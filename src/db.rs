use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use chrono::Utc;
use sqlx::{sqlite::SqliteConnectOptions, sqlite::SqlitePoolOptions, Row, SqlitePool};
use std::str::FromStr;
use uuid::Uuid;

use crate::model::{ClaimedMessage, ConsumerSnapshot, TopicSnapshot};

#[derive(Clone)]
pub struct Database(pub SqlitePool);

impl Database {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        let options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await?;

        sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS messages (
                id TEXT PRIMARY KEY NOT NULL,
                idempotency_key TEXT UNIQUE,
                topic TEXT NOT NULL,
                payload BLOB NOT NULL,
                attributes TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                next_attempt_at TEXT NOT NULL,
                last_error TEXT,
                created_at TEXT NOT NULL,
                delivered_at TEXT,
                lease TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_messages_ready
                ON messages(status, next_attempt_at, created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_created_at
                ON messages(created_at);
            CREATE INDEX IF NOT EXISTS idx_messages_topic_ready
                ON messages(topic, status, next_attempt_at, created_at);",
        )
            .execute(&pool)
            .await
            .context("creating SQLite queue schema")?;
        let _ = sqlx::query("ALTER TABLE messages ADD COLUMN lease TEXT")
            .execute(&pool)
            .await;
        sqlx::query("UPDATE messages SET status = 'pending', lease = NULL WHERE status = 'processing'")
            .execute(&pool)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS consumers (
                id TEXT PRIMARY KEY NOT NULL,
                topic TEXT NOT NULL,
                name TEXT NOT NULL DEFAULT '',
                attributes TEXT NOT NULL DEFAULT '{}',
                last_seen_at TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_consumers_topic_seen
                ON consumers(topic, last_seen_at);",
        )
            .execute(&pool)
            .await
            .context("creating SQLite consumer schema")?;
        migrate_consumers(&pool).await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS topic_stats (
                topic TEXT PRIMARY KEY NOT NULL,
                accepted INTEGER NOT NULL DEFAULT 0,
                duplicates INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0,
                failed INTEGER NOT NULL DEFAULT 0
            )",
        )
            .execute(&pool)
            .await
            .context("creating SQLite topic stats schema")?;
        sqlx::query(
            "INSERT OR IGNORE INTO topic_stats (topic, accepted, delivered, failed)
             SELECT topic,
                    COUNT(*),
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END)
             FROM messages
             GROUP BY topic",
        )
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO topic_stats (topic) SELECT DISTINCT topic FROM consumers",
        )
            .execute(&pool)
            .await?;
        Ok(Self(pool))
    }

    async fn bump_topic_stat(
        &self,
        topic: &str,
        accepted: i64,
        duplicates: i64,
        delivered: i64,
        failed: i64,
    ) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO topic_stats (topic, accepted, duplicates, delivered, failed)
             VALUES (?, ?, ?, ?, ?)
             ON CONFLICT(topic) DO UPDATE SET
                accepted = topic_stats.accepted + excluded.accepted,
                duplicates = topic_stats.duplicates + excluded.duplicates,
                delivered = topic_stats.delivered + excluded.delivered,
                failed = topic_stats.failed + excluded.failed",
        )
            .bind(topic)
            .bind(accepted)
            .bind(duplicates)
            .bind(delivered)
            .bind(failed)
            .execute(&self.0)
            .await?;
        Ok(())
    }

    pub async fn status_snapshot(&self, consumer_seen_after: Option<&str>) -> anyhow::Result<Vec<TopicSnapshot>> {
        let mut topics: HashMap<String, TopicSnapshot> = HashMap::new();

        for row in sqlx::query("SELECT topic, accepted, duplicates, delivered, failed FROM topic_stats")
            .fetch_all(&self.0)
            .await?
        {
            let name: String = row.get("topic");
            let topic = topics.entry(name.clone()).or_insert_with(|| TopicSnapshot {
                name,
                ..TopicSnapshot::default()
            });
            topic.accepted = row.get("accepted");
            topic.duplicates = row.get("duplicates");
            topic.delivered = row.get("delivered");
            topic.failed = row.get("failed");
        }

        for row in sqlx::query("SELECT topic, status, COUNT(*) AS n FROM messages GROUP BY topic, status")
            .fetch_all(&self.0)
            .await?
        {
            let name: String = row.get("topic");
            let status: String = row.get("status");
            let count: i64 = row.get("n");
            let topic = topics.entry(name.clone()).or_insert_with(|| TopicSnapshot {
                name,
                ..TopicSnapshot::default()
            });
            match status.as_str() {
                "pending" => topic.pending = count,
                "processing" => topic.processing = count,
                _ => {}
            }
        }

        for row in sqlx::query("SELECT id, topic, name, last_seen_at FROM consumers ORDER BY created_at")
            .fetch_all(&self.0)
            .await?
        {
            let topic_name: String = row.get("topic");
            let last_seen_at: String = row.get("last_seen_at");
            let live = consumer_seen_after
                .map(|cutoff| last_seen_at.as_str() >= cutoff)
                .unwrap_or(true);
            topics
                .entry(topic_name.clone())
                .or_insert_with(|| TopicSnapshot {
                    name: topic_name,
                    ..TopicSnapshot::default()
                })
                .consumers
                .push(ConsumerSnapshot {
                    id: row.get("id"),
                    name: row.get("name"),
                    last_seen_at,
                    live,
                });
        }

        let mut topics: Vec<TopicSnapshot> = topics.into_values().collect();
        topics.sort_by(|left, right| left.name.cmp(&right.name));
        Ok(topics)
    }

    pub async fn register_consumer(
        &self,
        consumer_id: Option<&str>,
        topic: &str,
        name: &str,
        attributes: &HashMap<String, String>,
    ) -> anyhow::Result<String> {
        let now = Utc::now().to_rfc3339();
        let attributes = serde_json::to_string(attributes)?;
        if let Some(id) = consumer_id.filter(|value| !value.is_empty()) {
            let updated = sqlx::query(
                "UPDATE consumers SET topic = ?, name = ?, attributes = ?, last_seen_at = ? WHERE id = ?",
            )
                .bind(topic)
                .bind(name)
                .bind(&attributes)
                .bind(&now)
                .bind(id)
                .execute(&self.0)
                .await?;
            if updated.rows_affected() > 0 {
                return Ok(id.to_string());
            }
            sqlx::query(
                "INSERT INTO consumers (id, topic, name, attributes, last_seen_at, created_at)
                 VALUES (?, ?, ?, ?, ?, ?)",
            )
                .bind(id)
                .bind(topic)
                .bind(name)
                .bind(&attributes)
                .bind(&now)
                .bind(&now)
                .execute(&self.0)
                .await?;
            return Ok(id.to_string());
        }
        let id = Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO consumers (id, topic, name, attributes, last_seen_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?)",
        )
            .bind(&id)
            .bind(topic)
            .bind(name)
            .bind(attributes)
            .bind(&now)
            .bind(&now)
            .execute(&self.0)
            .await?;
        Ok(id)
    }

    pub async fn touch_consumer(&self, consumer_id: &str) -> anyhow::Result<bool> {
        let result = sqlx::query("UPDATE consumers SET last_seen_at = ? WHERE id = ?")
            .bind(Utc::now().to_rfc3339())
            .bind(consumer_id)
            .execute(&self.0)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn unregister_consumer(&self, consumer_id: &str) -> anyhow::Result<bool> {
        let result = sqlx::query("DELETE FROM consumers WHERE id = ?")
            .bind(consumer_id)
            .execute(&self.0)
            .await?;
        Ok(result.rows_affected() > 0)
    }

    pub async fn purge_stale_consumers(&self, cutoff: &str) -> anyhow::Result<u64> {
        let result = sqlx::query("DELETE FROM consumers WHERE last_seen_at < ?")
            .bind(cutoff)
            .execute(&self.0)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn enqueue(
        &self,
        idempotency_key: Option<&str>,
        topic: &str,
        payload: &[u8],
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<(String, bool)> {
        if let Some(key) = idempotency_key.filter(|value| !value.is_empty()) {
            if let Some(row) = sqlx::query("SELECT id FROM messages WHERE idempotency_key = ?")
                .bind(key)
                .fetch_optional(&self.0)
                .await?
            {
                self.bump_topic_stat(topic, 0, 1, 0, 0).await?;
                return Ok((row.get("id"), true));
            }
        }
        let id = Uuid::new_v4().to_string();
        let now = Utc::now().to_rfc3339();
        let attributes = serde_json::to_string(&attributes)?;
        let result = sqlx::query(
            "INSERT OR IGNORE INTO messages
             (id, idempotency_key, topic, payload, attributes, next_attempt_at, created_at)
             VALUES (?, ?, ?, ?, ?, ?, ?)",
        )
            .bind(&id)
            .bind(idempotency_key.filter(|value| !value.is_empty()))
            .bind(topic)
            .bind(payload)
            .bind(attributes)
            .bind(&now)
            .bind(&now)
            .execute(&self.0)
            .await?;
        if result.rows_affected() == 0 {
            let key = idempotency_key.expect("conflict only possible with key");
            let row = sqlx::query("SELECT id FROM messages WHERE idempotency_key = ?")
                .bind(key)
                .fetch_one(&self.0)
                .await?;
            self.bump_topic_stat(topic, 0, 1, 0, 0).await?;
            return Ok((row.get("id"), true));
        }
        self.bump_topic_stat(topic, 1, 0, 0, 0).await?;
        Ok((id, false))
    }

    pub async fn reclaim_stale(&self) -> anyhow::Result<u64> {
        let result = sqlx::query(
            "UPDATE messages SET status = 'pending', lease = NULL
             WHERE status = 'processing' AND next_attempt_at <= ?",
        )
            .bind(Utc::now().to_rfc3339())
            .execute(&self.0)
            .await?;
        Ok(result.rows_affected())
    }

    pub async fn claim_next_for_topic(
        &self,
        topic: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        self.claim_ready(
            visibility,
            "SELECT id, topic, payload, attributes, created_at FROM messages
             WHERE status = 'pending' AND next_attempt_at <= ? AND topic = ?
             ORDER BY created_at LIMIT 1",
            Some(topic),
        )
        .await
    }

    pub async fn claim_next_excluding(
        &self,
        visibility: Duration,
        skip_topics: &[String],
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        if skip_topics.is_empty() {
            return self
                .claim_ready(
                    visibility,
                    "SELECT id, topic, payload, attributes, created_at FROM messages
                     WHERE status = 'pending' AND next_attempt_at <= ?
                     ORDER BY created_at LIMIT 1",
                    None,
                )
                .await;
        }
        let placeholders = std::iter::repeat_n("?", skip_topics.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT id, topic, payload, attributes, created_at FROM messages
             WHERE status = 'pending' AND next_attempt_at <= ?
               AND topic NOT IN ({placeholders})
             ORDER BY created_at LIMIT 1"
        );
        let _ = self.reclaim_stale().await;
        let mut tx = self.0.begin().await?;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let mut query = sqlx::query(&sql).bind(&now_text);
        for topic in skip_topics {
            query = query.bind(topic);
        }
        let row = query.fetch_optional(&mut *tx).await?;
        Self::finish_claim(tx, now, visibility, row).await
    }

    async fn claim_ready(
        &self,
        visibility: Duration,
        sql: &str,
        topic: Option<&str>,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let _ = self.reclaim_stale().await;
        let mut tx = self.0.begin().await?;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let mut query = sqlx::query(sql).bind(&now_text);
        if let Some(topic) = topic {
            query = query.bind(topic);
        }
        let row = query.fetch_optional(&mut *tx).await?;
        Self::finish_claim(tx, now, visibility, row).await
    }

    async fn finish_claim(
        mut tx: sqlx::Transaction<'_, sqlx::Sqlite>,
        now: chrono::DateTime<Utc>,
        visibility: Duration,
        row: Option<sqlx::sqlite::SqliteRow>,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let Some(row) = row else {
            tx.commit().await?;
            return Ok(None);
        };
        let id: String = row.get("id");
        let lease = Uuid::new_v4().to_string();
        let visible_again = (now
            + chrono::Duration::from_std(visibility).unwrap_or(chrono::Duration::seconds(30)))
        .to_rfc3339();
        let changed = sqlx::query(
            "UPDATE messages SET status = 'processing', attempts = attempts + 1, lease = ?, next_attempt_at = ?
             WHERE id = ? AND status = 'pending'",
        )
            .bind(&lease)
            .bind(&visible_again)
            .bind(&id)
            .execute(&mut *tx)
            .await?;
        if changed.rows_affected() == 0 {
            tx.commit().await?;
            return Ok(None);
        }
        tx.commit().await?;
        Ok(Some(ClaimedMessage {
            id,
            topic: row.get("topic"),
            payload: row.get("payload"),
            attributes: serde_json::from_str(&row.get::<String, _>("attributes"))?,
            created_at: row.get("created_at"),
            lease,
        }))
    }

    pub async fn delivered(&self, id: &str, lease: &str) -> anyhow::Result<bool> {
        let row = sqlx::query(
            "SELECT topic FROM messages WHERE id = ? AND lease = ? AND status = 'processing'",
        )
            .bind(id)
            .bind(lease)
            .fetch_optional(&self.0)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let topic: String = row.get("topic");
        let result = sqlx::query(
            "UPDATE messages SET status = 'delivered', delivered_at = ?, last_error = NULL, lease = NULL
             WHERE id = ? AND lease = ? AND status = 'processing'",
        )
            .bind(Utc::now().to_rfc3339())
            .bind(id)
            .bind(lease)
            .execute(&self.0)
            .await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        self.bump_topic_stat(&topic, 0, 0, 1, 0).await?;
        Ok(true)
    }

    pub async fn failed(&self, id: &str, lease: &str, error: &str, max_attempts: i64) -> anyhow::Result<bool> {
        let row = sqlx::query(
            "SELECT topic, attempts FROM messages WHERE id = ? AND lease = ? AND status = 'processing'",
        )
            .bind(id)
            .bind(lease)
            .fetch_optional(&self.0)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let topic: String = row.get("topic");
        let attempts: i64 = row.get("attempts");
        let status = if attempts >= max_attempts { "failed" } else { "pending" };
        let delay_secs = 2_i64.saturating_pow(attempts.min(8) as u32).min(300);
        let next_attempt_at = (Utc::now() + chrono::Duration::seconds(delay_secs)).to_rfc3339();
        let result = sqlx::query(
            "UPDATE messages SET status = ?, next_attempt_at = ?, last_error = ?, lease = NULL
             WHERE id = ? AND lease = ? AND status = 'processing'",
        )
            .bind(status)
            .bind(next_attempt_at)
            .bind(error)
            .bind(id)
            .bind(lease)
            .execute(&self.0)
            .await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        if status == "failed" {
            self.bump_topic_stat(&topic, 0, 0, 0, 1).await?;
        }
        Ok(true)
    }

    pub async fn purge_older_than(&self, cutoff: &str) -> anyhow::Result<u64> {
        let result = sqlx::query("DELETE FROM messages WHERE created_at < ?")
            .bind(cutoff)
            .execute(&self.0)
            .await?;
        Ok(result.rows_affected())
    }
}

async fn migrate_consumers(pool: &SqlitePool) -> anyhow::Result<()> {
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(consumers)")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    if columns.iter().any(|name| name == "name") && !columns.iter().any(|name| name == "downstream_url") {
        return Ok(());
    }
    if !columns.iter().any(|name| name == "downstream_url") {
        return Ok(());
    }
    sqlx::query(
        "CREATE TABLE consumers_new (
            id TEXT PRIMARY KEY NOT NULL,
            topic TEXT NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            attributes TEXT NOT NULL DEFAULT '{}',
            last_seen_at TEXT NOT NULL,
            created_at TEXT NOT NULL
        )",
    )
    .execute(pool)
    .await?;
    sqlx::query(
        "INSERT INTO consumers_new (id, topic, name, attributes, last_seen_at, created_at)
         SELECT id, topic, '', '{}', last_seen_at, created_at FROM consumers",
    )
    .execute(pool)
    .await?;
    sqlx::query("DROP TABLE consumers").execute(pool).await?;
    sqlx::query("ALTER TABLE consumers_new RENAME TO consumers")
        .execute(pool)
        .await?;
    sqlx::query("CREATE INDEX IF NOT EXISTS idx_consumers_topic_seen ON consumers(topic, last_seen_at)")
        .execute(pool)
        .await?;
    Ok(())
}