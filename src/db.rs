use std::{collections::HashMap, path::PathBuf, time::Duration};

use anyhow::Context;
use chrono::Utc;
use sqlx::{
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions, SqliteSynchronous},
    Row, SqlitePool,
};
use std::str::FromStr;
use uuid::Uuid;

use crate::model::{
    ClaimedMessage, ConsumerSnapshot, DeliveryMode, PersistenceMode, TopicConfig, TopicSnapshot,
};

#[derive(Clone)]
pub struct Database(pub SqlitePool);

#[derive(Debug, Clone)]
pub struct StoredMessage {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub attributes: serde_json::Value,
    pub status: String,
    pub attempts: i64,
    pub created_at: String,
    pub delivered_at: Option<String>,
    pub last_error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct TopicMessagePage {
    pub offset: i64,
    pub total: i64,
    pub message: Option<StoredMessage>,
}

impl Database {
    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        if let Some(path) = sqlite_file_path(url) {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).with_context(|| {
                        format!("create sqlite directory {}", parent.display())
                    })?;
                }
            }
        }

        let options = SqliteConnectOptions::from_str(url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));

        let pool = SqlitePoolOptions::new()
            .max_connections(10)
            .connect_with(options)
            .await
            .with_context(|| {
                format!(
                    "open sqlite {url} (leftover .db-wal/.db-shm is normal after a crash; \
                     this fails if another process still holds the file or the volume is not writable)"
                )
            })?;

        sqlx::query("PRAGMA journal_mode = WAL").execute(&pool).await?;
        let _ = sqlx::query("PRAGMA wal_checkpoint(RESTART)").execute(&pool).await;
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
                failed INTEGER NOT NULL DEFAULT 0,
                delivery TEXT NOT NULL DEFAULT 'broadcast',
                persistence TEXT NOT NULL DEFAULT 'ephemeral',
                dropped INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT,
                configured INTEGER NOT NULL DEFAULT 0
            )",
        )
            .execute(&pool)
            .await
            .context("creating SQLite topic stats schema")?;
        let _ = sqlx::query(
            "ALTER TABLE topic_stats ADD COLUMN delivery TEXT NOT NULL DEFAULT 'single'",
        )
        .execute(&pool)
        .await;
        let _ = sqlx::query(
            "ALTER TABLE topic_stats ADD COLUMN persistence TEXT NOT NULL DEFAULT 'persistent'",
        )
        .execute(&pool)
        .await;
        let _ = sqlx::query(
            "ALTER TABLE topic_stats ADD COLUMN dropped INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&pool)
        .await;
        let _ = sqlx::query("ALTER TABLE topic_stats ADD COLUMN last_seen_at TEXT")
            .execute(&pool)
            .await;
        let _ = sqlx::query(
            "ALTER TABLE topic_stats ADD COLUMN configured INTEGER NOT NULL DEFAULT 0",
        )
        .execute(&pool)
        .await;
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE topic_stats SET last_seen_at = ? WHERE last_seen_at IS NULL")
            .bind(&now)
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO topic_stats (topic, accepted, delivered, failed, delivery, persistence)
             SELECT topic,
                    COUNT(*),
                    SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END),
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END),
                    'broadcast',
                    'ephemeral'
             FROM messages
             GROUP BY topic",
        )
            .execute(&pool)
            .await?;
        sqlx::query(
            "INSERT OR IGNORE INTO topic_stats (topic, delivery, persistence)
             SELECT DISTINCT topic, 'broadcast', 'ephemeral' FROM consumers",
        )
            .execute(&pool)
            .await?;
        Ok(Self(pool))
    }

    pub async fn topic_persistence(&self, topic: &str) -> anyhow::Result<PersistenceMode> {
        Ok(self.topic_config(topic).await?.persistence)
    }

    pub async fn topic_config(&self, topic: &str) -> anyhow::Result<TopicConfig> {
        let row = sqlx::query(
            "SELECT delivery, persistence FROM topic_stats WHERE topic = ?",
        )
        .bind(topic)
        .fetch_optional(&self.0)
        .await?;
        Ok(row
            .map(|row| TopicConfig {
                topic: topic.to_string(),
                delivery: DeliveryMode::from_stored(&row.get::<String, _>("delivery")),
                persistence: PersistenceMode::from_stored(&row.get::<String, _>("persistence")),
            })
            .unwrap_or_else(|| TopicConfig::implicit(topic)))
    }

    /// Record an MQTT subscribe filter so idle purge will not delete it.
    /// Child publish topics stay implicit/ephemeral. Explicit ConfigureTopics wins.
    pub async fn note_subscribed_topic(&self, topic: &str) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO topic_stats (topic, delivery, persistence, configured, last_seen_at)
             VALUES (?, 'broadcast', 'persistent', 0, ?)
             ON CONFLICT(topic) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                persistence = CASE
                    WHEN IFNULL(topic_stats.configured, 0) = 0 THEN 'persistent'
                    ELSE topic_stats.persistence
                END",
        )
        .bind(topic)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.0)
        .await?;
        Ok(())
    }

    pub async fn ephemeral_topics(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query("SELECT topic FROM topic_stats WHERE persistence = 'ephemeral'")
            .fetch_all(&self.0)
            .await?;
        Ok(rows.into_iter().map(|row| row.get("topic")).collect())
    }

    pub async fn configure_topics(&self, configs: &[TopicConfig]) -> anyhow::Result<Vec<TopicConfig>> {
        for config in configs {
            sqlx::query(
                "INSERT INTO topic_stats (topic, delivery, persistence, configured, last_seen_at)
                 VALUES (?, ?, ?, 1, ?)
                 ON CONFLICT(topic) DO UPDATE SET
                    delivery = excluded.delivery,
                    persistence = excluded.persistence,
                    configured = 1",
            )
            .bind(&config.topic)
            .bind(config.delivery.as_str())
            .bind(config.persistence.as_str())
            .bind(Utc::now().to_rfc3339())
            .execute(&self.0)
            .await?;
        }
        self.list_topic_configs().await
    }

    pub async fn list_topic_configs(&self) -> anyhow::Result<Vec<TopicConfig>> {
        let rows = sqlx::query("SELECT topic, delivery, persistence FROM topic_stats ORDER BY topic")
            .fetch_all(&self.0)
            .await?;
        Ok(rows
            .into_iter()
            .map(|row| TopicConfig {
                topic: row.get("topic"),
                delivery: DeliveryMode::from_stored(&row.get::<String, _>("delivery")),
                persistence: PersistenceMode::from_stored(&row.get::<String, _>("persistence")),
            })
            .collect())
    }

    pub async fn close(self) {
        let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.0)
            .await;
        self.0.close().await;
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
            "INSERT INTO topic_stats (topic, accepted, duplicates, delivered, failed, delivery, persistence, last_seen_at)
             VALUES (?, ?, ?, ?, ?, 'broadcast', 'ephemeral', ?)
             ON CONFLICT(topic) DO UPDATE SET
                accepted = topic_stats.accepted + excluded.accepted,
                duplicates = topic_stats.duplicates + excluded.duplicates,
                delivered = topic_stats.delivered + excluded.delivered,
                failed = topic_stats.failed + excluded.failed,
                last_seen_at = excluded.last_seen_at",
        )
            .bind(topic)
            .bind(accepted)
            .bind(duplicates)
            .bind(delivered)
            .bind(failed)
            .bind(Utc::now().to_rfc3339())
            .execute(&self.0)
            .await?;
        Ok(())
    }

    pub async fn status_snapshot(&self, consumer_seen_after: Option<&str>) -> anyhow::Result<Vec<TopicSnapshot>> {
        let mut topics: HashMap<String, TopicSnapshot> = HashMap::new();

        for row in sqlx::query(
            "SELECT topic, accepted, duplicates, delivered, failed, dropped, delivery, persistence
             FROM topic_stats",
        )
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
            topic.dropped = row.get("dropped");
            let delivery: String = row.get("delivery");
            topic.delivery = DeliveryMode::from_stored(&delivery).as_str().to_string();
            let persistence: String = row.get("persistence");
            topic.persistence = PersistenceMode::from_stored(&persistence)
                .as_str()
                .to_string();
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

        for row in sqlx::query(
            "SELECT id, topic, name, attributes, last_seen_at FROM consumers ORDER BY created_at",
        )
        .fetch_all(&self.0)
        .await?
        {
            let topic_name: String = row.get("topic");
            let last_seen_at: String = row.get("last_seen_at");
            let live = consumer_seen_after
                .map(|cutoff| last_seen_at.as_str() >= cutoff)
                .unwrap_or(true);
            let attributes = parse_consumer_attributes(row.get("attributes"));
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
                    attributes,
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

    pub async fn consumer_attribute(
        &self,
        consumer_id: &str,
        key: &str,
    ) -> anyhow::Result<Option<String>> {
        let Some(row) = sqlx::query("SELECT attributes FROM consumers WHERE id = ?")
            .bind(consumer_id)
            .fetch_optional(&self.0)
            .await?
        else {
            return Ok(None);
        };
        Ok(parse_consumer_attributes(row.get("attributes"))
            .get(key)
            .cloned()
            .filter(|value| !value.is_empty()))
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

    pub async fn topic_message_page(
        &self,
        filter: &str,
        offset: i64,
    ) -> anyhow::Result<TopicMessagePage> {
        let mut offset = offset.max(0);
        let (total, id) = self.topic_message_id(filter, offset).await?;
        let (total, id) = if id.is_none() && total > 0 && offset >= total {
            offset = total - 1;
            self.topic_message_id(filter, offset).await?
        } else {
            (total, id)
        };
        let Some(id) = id else {
            return Ok(TopicMessagePage {
                offset,
                total,
                message: None,
            });
        };
        let Some(row) = sqlx::query(
            "SELECT id, topic, payload, attributes, status, attempts, created_at, delivered_at, last_error
             FROM messages WHERE id = ?",
        )
        .bind(&id)
        .fetch_optional(&self.0)
        .await?
        else {
            return Ok(TopicMessagePage {
                offset,
                total,
                message: None,
            });
        };
        let attributes_raw: String = row.get("attributes");
        let attributes = serde_json::from_str(&attributes_raw)
            .unwrap_or_else(|_| serde_json::json!({}));
        Ok(TopicMessagePage {
            offset,
            total,
            message: Some(StoredMessage {
                id: row.get("id"),
                topic: row.get("topic"),
                payload: row.get("payload"),
                attributes,
                status: row.get("status"),
                attempts: row.get("attempts"),
                created_at: row.get("created_at"),
                delivered_at: row.get("delivered_at"),
                last_error: row.get("last_error"),
            }),
        })
    }

    async fn topic_message_id(
        &self,
        filter: &str,
        offset: i64,
    ) -> anyhow::Result<(i64, Option<String>)> {
        if !crate::topic::is_wildcard_filter(filter) {
            let total: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE topic = ?")
                .bind(filter)
                .fetch_one(&self.0)
                .await?;
            if total == 0 || offset >= total {
                return Ok((total, None));
            }
            let id: Option<String> = sqlx::query_scalar(
                "SELECT id FROM messages WHERE topic = ?
                 ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?",
            )
            .bind(filter)
            .bind(offset)
            .fetch_optional(&self.0)
            .await?;
            return Ok((total, id));
        }
        if let Some(prefix) = crate::topic::multi_level_prefix(filter) {
            let like = format!("{}/%", escape_like(prefix));
            let total: i64 = if prefix.is_empty() {
                sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE topic NOT LIKE '$%'")
                    .fetch_one(&self.0)
                    .await?
            } else {
                sqlx::query_scalar(
                    "SELECT COUNT(*) FROM messages
                     WHERE topic = ? OR topic LIKE ? ESCAPE '\\'",
                )
                .bind(prefix)
                .bind(&like)
                .fetch_one(&self.0)
                .await?
            };
            if total == 0 || offset >= total {
                return Ok((total, None));
            }
            let id: Option<String> = if prefix.is_empty() {
                sqlx::query_scalar(
                    "SELECT id FROM messages WHERE topic NOT LIKE '$%'
                     ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?",
                )
                .bind(offset)
                .fetch_optional(&self.0)
                .await?
            } else {
                sqlx::query_scalar(
                    "SELECT id FROM messages
                     WHERE topic = ? OR topic LIKE ? ESCAPE '\\'
                     ORDER BY created_at DESC, id DESC LIMIT 1 OFFSET ?",
                )
                .bind(prefix)
                .bind(&like)
                .bind(offset)
                .fetch_optional(&self.0)
                .await?
            };
            return Ok((total, id));
        }
        let rows = sqlx::query("SELECT id, topic FROM messages ORDER BY created_at DESC, id DESC")
            .fetch_all(&self.0)
            .await?;
        let matched: Vec<String> = rows
            .into_iter()
            .filter_map(|row| {
                let topic: String = row.get("topic");
                crate::topic::filter_matches(filter, &topic).then(|| row.get("id"))
            })
            .collect();
        let total = matched.len() as i64;
        let id = matched.get(offset as usize).cloned();
        Ok((total, id))
    }

    pub async fn accept_dropped(&self, topic: &str) -> anyhow::Result<String> {
        let id = Uuid::new_v4().to_string();
        self.bump_topic_stat(topic, 1, 0, 0, 0).await?;
        self.bump_dropped(topic, 1).await?;
        Ok(id)
    }

    pub async fn drop_pending(&self, topic: &str) -> anyhow::Result<u64> {
        let result = sqlx::query(
            "UPDATE messages SET status = 'dropped', last_error = 'no live subscriber', lease = NULL
             WHERE topic = ? AND status = 'pending'",
        )
        .bind(topic)
        .execute(&self.0)
        .await?;
        let dropped = result.rows_affected();
        if dropped > 0 {
            self.bump_dropped(topic, dropped as i64).await?;
        }
        Ok(dropped)
    }

    pub async fn drop_claimed(&self, id: &str, lease: &str, error: &str) -> anyhow::Result<bool> {
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
            "UPDATE messages SET status = 'dropped', last_error = ?, lease = NULL
             WHERE id = ? AND lease = ? AND status = 'processing'",
        )
        .bind(error)
        .bind(id)
        .bind(lease)
        .execute(&self.0)
        .await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        self.bump_dropped(&topic, 1).await?;
        Ok(true)
    }

    async fn bump_dropped(&self, topic: &str, dropped: i64) -> anyhow::Result<()> {
        sqlx::query(
            "INSERT INTO topic_stats (topic, dropped, delivery, persistence, last_seen_at)
             VALUES (?, ?, 'broadcast', 'ephemeral', ?)
             ON CONFLICT(topic) DO UPDATE SET
                dropped = topic_stats.dropped + excluded.dropped,
                last_seen_at = excluded.last_seen_at",
        )
        .bind(topic)
        .bind(dropped)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.0)
        .await?;
        Ok(())
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

    pub async fn claim_next_for_filter(
        &self,
        filter: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        if !crate::topic::is_wildcard_filter(filter) {
            return self.claim_next_for_topic(filter, visibility).await;
        }
        if let Some(prefix) = crate::topic::multi_level_prefix(filter) {
            return self.claim_next_for_hash(prefix, visibility).await;
        }
        self.claim_next_matching(filter, visibility).await
    }

    pub async fn release(&self, id: &str, lease: &str) -> anyhow::Result<bool> {
        let result = sqlx::query(
            "UPDATE messages SET status = 'pending', lease = NULL
             WHERE id = ? AND lease = ? AND status = 'processing'",
        )
        .bind(id)
        .bind(lease)
        .execute(&self.0)
        .await?;
        Ok(result.rows_affected() > 0)
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

    async fn claim_next_for_hash(
        &self,
        prefix: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let _ = self.reclaim_stale().await;
        let mut tx = self.0.begin().await?;
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let row = if prefix.is_empty() {
            sqlx::query(
                "SELECT id, topic, payload, attributes, created_at FROM messages
                 WHERE status = 'pending' AND next_attempt_at <= ? AND topic NOT LIKE '$%'
                 ORDER BY created_at LIMIT 1",
            )
            .bind(&now_text)
            .fetch_optional(&mut *tx)
            .await?
        } else {
            let like = format!("{}/%", escape_like(prefix));
            sqlx::query(
                "SELECT id, topic, payload, attributes, created_at FROM messages
                 WHERE status = 'pending' AND next_attempt_at <= ?
                   AND (topic = ? OR topic LIKE ? ESCAPE '\\')
                 ORDER BY created_at LIMIT 1",
            )
            .bind(&now_text)
            .bind(prefix)
            .bind(&like)
            .fetch_optional(&mut *tx)
            .await?
        };
        Self::finish_claim(tx, now, visibility, row).await
    }

    async fn claim_next_matching(
        &self,
        filter: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let _ = self.reclaim_stale().await;
        let now_text = Utc::now().to_rfc3339();
        let rows = sqlx::query(
            "SELECT id, topic FROM messages
             WHERE status = 'pending' AND next_attempt_at <= ?
             ORDER BY created_at LIMIT 64",
        )
        .bind(&now_text)
        .fetch_all(&self.0)
        .await?;
        let Some(id) = rows.into_iter().find_map(|row| {
            let topic: String = row.get("topic");
            crate::topic::filter_matches(filter, &topic).then(|| row.get::<String, _>("id"))
        }) else {
            return Ok(None);
        };
        let mut tx = self.0.begin().await?;
        let now = Utc::now();
        let row = sqlx::query(
            "SELECT id, topic, payload, attributes, created_at FROM messages
             WHERE id = ? AND status = 'pending'",
        )
        .bind(&id)
        .fetch_optional(&mut *tx)
        .await?;
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

    pub async fn record_live_fanout(&self, topic: &str) -> anyhow::Result<()> {
        self.bump_topic_stat(topic, 1, 0, 1, 0).await
    }

    /// On-disk size of the main SQLite file plus `-wal` / `-shm` sidecars.
    pub async fn sqlite_size_bytes(&self) -> anyhow::Result<u64> {
        let row = sqlx::query("SELECT file FROM pragma_database_list WHERE name = 'main'")
            .fetch_optional(&self.0)
            .await?;
        let path = row
            .map(|row| row.get::<String, _>("file"))
            .unwrap_or_default();
        if path.is_empty() {
            return Ok(0);
        }
        Ok(file_len(&path)
            + file_len(&format!("{path}-wal"))
            + file_len(&format!("{path}-shm")))
    }

    pub async fn purge_older_than(&self, cutoff: &str) -> anyhow::Result<u64> {
        let result = sqlx::query("DELETE FROM messages WHERE created_at < ?")
            .bind(cutoff)
            .execute(&self.0)
            .await?;
        Ok(result.rows_affected())
    }

    /// Remove implicit ephemeral topics that have been idle since `cutoff`.
    /// Caller passes `now - ephemeral_idle_hours` and schedules on `purge_interval_hours`
    /// (both default to 1 hour). Live subscriber topics in `keep` and ConfigureTopics stay.
    pub async fn purge_idle_ephemeral(
        &self,
        cutoff: &str,
        keep: &[String],
    ) -> anyhow::Result<u64> {
        let rows = sqlx::query(
            "SELECT topic FROM topic_stats
             WHERE persistence = 'ephemeral'
               AND IFNULL(configured, 0) = 0
               AND last_seen_at IS NOT NULL
               AND last_seen_at < ?
               AND topic NOT IN (SELECT DISTINCT topic FROM consumers)",
        )
        .bind(cutoff)
        .fetch_all(&self.0)
        .await?;
        let stale: Vec<String> = rows
            .into_iter()
            .map(|row| row.get::<String, _>("topic"))
            .filter(|topic| !keep.iter().any(|live| live == topic))
            .collect();
        if stale.is_empty() {
            return Ok(0);
        }
        let mut tx = self.0.begin().await?;
        let mut deleted = 0u64;
        for topic in &stale {
            sqlx::query("DELETE FROM messages WHERE topic = ?")
                .bind(topic)
                .execute(&mut *tx)
                .await?;
            let result = sqlx::query(
                "DELETE FROM topic_stats
                 WHERE topic = ? AND persistence = 'ephemeral' AND IFNULL(configured, 0) = 0",
            )
            .bind(topic)
            .execute(&mut *tx)
            .await?;
            deleted += result.rows_affected();
        }
        tx.commit().await?;
        Ok(deleted)
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

fn parse_consumer_attributes(raw: String) -> HashMap<String, String> {
    serde_json::from_str(&raw).unwrap_or_default()
}

fn file_len(path: &str) -> u64 {
    std::fs::metadata(path).map(|meta| meta.len()).unwrap_or(0)
}

fn sqlite_file_path(url: &str) -> Option<PathBuf> {
    let rest = url.strip_prefix("sqlite:")?;
    if let Some(path) = rest.strip_prefix("///") {
        return Some(PathBuf::from(format!("/{path}")));
    }
    if rest.starts_with("//") {
        return None;
    }
    Some(PathBuf::from(rest))
}

fn escape_like(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if matches!(ch, '\\' | '%' | '_') {
            out.push('\\');
        }
        out.push(ch);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    async fn temp_db() -> (Database, PathBuf) {
        let dir = std::env::temp_dir().join(format!("cangling-broker-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let url = format!("sqlite:{}/queue.db", dir.display());
        let db = Database::connect(&url).await.unwrap();
        (db, dir)
    }

    #[tokio::test]
    async fn configure_persistence_and_drop_without_subscribers() {
        let (db, dir) = temp_db().await;
        db.configure_topics(&[TopicConfig {
            topic: "live-events".into(),
            delivery: DeliveryMode::Broadcast,
            persistence: PersistenceMode::Ephemeral,
        }])
        .await
        .unwrap();

        let listed = db.list_topic_configs().await.unwrap();
        let topic = listed.iter().find(|item| item.topic == "live-events").unwrap();
        assert_eq!(topic.delivery, DeliveryMode::Broadcast);
        assert_eq!(topic.persistence, PersistenceMode::Ephemeral);
        assert_eq!(
            db.topic_persistence("live-events").await.unwrap(),
            PersistenceMode::Ephemeral
        );
        assert_eq!(db.ephemeral_topics().await.unwrap(), vec!["live-events"]);

        let id = db.accept_dropped("live-events").await.unwrap();
        assert!(!id.is_empty());
        let snapshot = db.status_snapshot(None).await.unwrap();
        let stats = snapshot.iter().find(|item| item.name == "live-events").unwrap();
        assert_eq!(stats.accepted, 1);
        assert_eq!(stats.dropped, 1);
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.persistence, "ephemeral");

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn drop_pending_marks_queued_messages() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "live-events", b"hello", HashMap::new())
            .await
            .unwrap();
        assert_eq!(db.drop_pending("live-events").await.unwrap(), 1);
        assert_eq!(db.drop_pending("live-events").await.unwrap(), 0);
        let snapshot = db.status_snapshot(None).await.unwrap();
        let stats = snapshot.iter().find(|item| item.name == "live-events").unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.dropped, 1);

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn topic_message_page_walks_latest_first() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "jobs", b"one", HashMap::new()).await.unwrap();
        db.enqueue(None, "jobs", b"two", HashMap::new()).await.unwrap();
        db.enqueue(None, "other", b"skip", HashMap::new()).await.unwrap();
        let latest = db.topic_message_page("jobs", 0).await.unwrap();
        assert_eq!(latest.total, 2);
        assert_eq!(latest.message.as_ref().unwrap().payload, b"two");
        let older = db.topic_message_page("jobs", 1).await.unwrap();
        assert_eq!(older.message.as_ref().unwrap().payload, b"one");
        let past_end = db.topic_message_page("jobs", 5).await.unwrap();
        assert_eq!(past_end.offset, 1);
        assert_eq!(past_end.total, 2);
        assert_eq!(past_end.message.as_ref().unwrap().payload, b"one");
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn topic_message_page_matches_hash_filter() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "building/a", b"a", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "other", b"no", HashMap::new()).await.unwrap();
        let page = db.topic_message_page("building/#", 0).await.unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.message.as_ref().unwrap().topic, "building/a");
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn snapshot_includes_consumer_attributes() {
        let (db, dir) = temp_db().await;
        let mut attrs = HashMap::new();
        attrs.insert("host".into(), "worker-1".into());
        attrs.insert("version".into(), "java/0.1.29".into());
        let consumer_id = db
            .register_consumer(None, "jobs", "java-s0", &attrs)
            .await
            .unwrap();
        let snapshot = db.status_snapshot(None).await.unwrap();
        let topic = snapshot.iter().find(|item| item.name == "jobs").unwrap();
        assert_eq!(topic.consumers.len(), 1);
        assert_eq!(topic.consumers[0].name, "java-s0");
        assert_eq!(
            topic.consumers[0].attributes.get("host").map(String::as_str),
            Some("worker-1")
        );
        assert_eq!(
            db.consumer_attribute(&consumer_id, "version")
                .await
                .unwrap()
                .as_deref(),
            Some("java/0.1.29")
        );

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn sqlite_size_bytes_includes_main_file() {
        let (db, dir) = temp_db().await;
        let empty = db.sqlite_size_bytes().await.unwrap();
        assert!(empty > 0, "fresh sqlite file should have a header");
        db.enqueue(None, "jobs", &vec![0u8; 64 * 1024], HashMap::new())
            .await
            .unwrap();
        let grown = db.sqlite_size_bytes().await.unwrap();
        assert!(grown >= empty);

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn hash_filter_claims_child_and_parent_topics() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "building/floor1/temp", b"23", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "other", b"x", HashMap::new())
            .await
            .unwrap();
        let claimed = db
            .claim_next_for_filter("building/#", Duration::from_secs(5))
            .await
            .unwrap()
            .expect("child topic");
        assert_eq!(claimed.topic, "building/floor1/temp");
        db.delivered(&claimed.id, &claimed.lease).await.unwrap();

        db.enqueue(None, "building", b"root", HashMap::new())
            .await
            .unwrap();
        let parent = db
            .claim_next_for_filter("building/#", Duration::from_secs(5))
            .await
            .unwrap()
            .expect("parent topic");
        assert_eq!(parent.topic, "building");

        let skipped = db
            .claim_next_for_filter("building/#", Duration::from_secs(5))
            .await
            .unwrap();
        assert!(skipped.is_none());

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn mqtt_subscribed_topic_is_not_ephemeral() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "jobs", b"early", HashMap::new())
            .await
            .unwrap();
        assert_eq!(
            db.topic_config("jobs").await.unwrap().persistence,
            PersistenceMode::Ephemeral
        );

        db.note_subscribed_topic("jobs").await.unwrap();
        assert_eq!(
            db.topic_config("jobs").await.unwrap().persistence,
            PersistenceMode::Persistent
        );

        db.note_subscribed_topic("building/#").await.unwrap();
        assert_eq!(
            db.topic_config("building/#").await.unwrap().persistence,
            PersistenceMode::Persistent
        );
        assert_eq!(
            db.topic_config("building/floor1/temp")
                .await
                .unwrap()
                .persistence,
            PersistenceMode::Ephemeral
        );

        db.enqueue(None, "building/floor1/temp", b"23", HashMap::new())
            .await
            .unwrap();
        let snapshot = db.status_snapshot(None).await.unwrap();
        let child = snapshot
            .iter()
            .find(|item| item.name == "building/floor1/temp")
            .unwrap();
        assert_eq!(child.persistence, "ephemeral");
        let filter = snapshot
            .iter()
            .find(|item| item.name == "building/#")
            .unwrap();
        assert_eq!(filter.persistence, "persistent");

        let future = (Utc::now() + chrono::Duration::hours(8)).to_rfc3339();
        db.purge_idle_ephemeral(&future, &[]).await.unwrap();
        let listed = db.list_topic_configs().await.unwrap();
        let names: Vec<_> = listed.iter().map(|item| item.topic.as_str()).collect();
        assert!(names.contains(&"building/#"));
        assert!(!names.contains(&"building/floor1/temp"));
        assert!(names.contains(&"jobs"));

        db.configure_topics(&[TopicConfig {
            topic: "jobs".into(),
            delivery: DeliveryMode::Broadcast,
            persistence: PersistenceMode::Ephemeral,
        }])
        .await
        .unwrap();
        db.note_subscribed_topic("jobs").await.unwrap();
        assert_eq!(
            db.topic_config("jobs").await.unwrap().persistence,
            PersistenceMode::Ephemeral
        );

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn unconfigured_topic_defaults_to_broadcast_ephemeral() {
        let (db, dir) = temp_db().await;
        let missing = db.topic_config("fresh").await.unwrap();
        assert_eq!(missing.delivery, DeliveryMode::Broadcast);
        assert_eq!(missing.persistence, PersistenceMode::Ephemeral);

        db.enqueue(None, "fresh", b"x", HashMap::new())
            .await
            .unwrap();
        let stored = db.topic_config("fresh").await.unwrap();
        assert_eq!(stored.delivery, DeliveryMode::Broadcast);
        assert_eq!(stored.persistence, PersistenceMode::Ephemeral);

        db.configure_topics(&[TopicConfig {
            topic: "fresh".into(),
            delivery: DeliveryMode::Single,
            persistence: PersistenceMode::Persistent,
        }])
        .await
        .unwrap();
        db.enqueue(None, "fresh", b"y", HashMap::new())
            .await
            .unwrap();
        let configured = db.topic_config("fresh").await.unwrap();
        assert_eq!(configured.delivery, DeliveryMode::Single);
        assert_eq!(configured.persistence, PersistenceMode::Persistent);

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn purge_idle_ephemeral_keeps_configured_and_live() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "stale", b"old", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "live", b"now", HashMap::new())
            .await
            .unwrap();
        db.configure_topics(&[TopicConfig {
            topic: "kept".into(),
            delivery: DeliveryMode::Broadcast,
            persistence: PersistenceMode::Ephemeral,
        }])
        .await
        .unwrap();
        db.enqueue(None, "kept", b"cfg", HashMap::new())
            .await
            .unwrap();

        let future = (Utc::now() + chrono::Duration::hours(8)).to_rfc3339();
        let deleted = db
            .purge_idle_ephemeral(&future, &["live".into()])
            .await
            .unwrap();
        assert_eq!(deleted, 1);
        let listed = db.list_topic_configs().await.unwrap();
        let names: Vec<_> = listed.iter().map(|item| item.topic.as_str()).collect();
        assert!(!names.contains(&"stale"));
        assert!(names.contains(&"live"));
        assert!(names.contains(&"kept"));

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}