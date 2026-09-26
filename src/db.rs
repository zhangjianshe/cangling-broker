use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc, Mutex, RwLock,
    },
    time::Duration,
};

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
pub struct Database(
    pub SqlitePool,
    Arc<RwLock<HashMap<String, TopicConfig>>>,
    Arc<RwLock<HashMap<String, (i64, i64)>>>,
    Arc<Vec<SqlitePool>>,
    Arc<AtomicUsize>,
    Arc<Mutex<HashMap<String, TopicStatsDelta>>>,
    Arc<Mutex<HashMap<String, MessageTrendDelta>>>,
);

const MESSAGE_SHARDS: usize = 16;

#[derive(Clone, Copy, Debug, Default)]
struct TopicStatsDelta {
    accepted: i64,
    duplicates: i64,
    delivered: i64,
}

#[derive(Clone, Copy, Debug, Default)]
struct MessageTrendDelta {
    accepted: i64,
    delivered: i64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct MessageTrendPoint {
    pub minute: String,
    pub accepted: i64,
    pub delivered: i64,
}

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

#[derive(Debug, Clone)]
pub struct EnqueueInput {
    pub id: String,
    pub idempotency_key: Option<String>,
    pub topic: String,
    pub payload: Vec<u8>,
    pub attributes: HashMap<String, String>,
}

impl Database {
    pub async fn recent_idempotency_keys(
        &self,
        limit_per_shard: usize,
    ) -> anyhow::Result<Vec<(usize, String, String)>> {
        let mut entries = Vec::new();
        for (shard, pool) in self.3.iter().enumerate() {
            let rows = sqlx::query(
                "SELECT idempotency_key, id FROM (
                    SELECT idempotency_key, id, created_at
                    FROM messages
                    WHERE idempotency_key IS NOT NULL AND idempotency_key != ''
                    ORDER BY created_at DESC
                    LIMIT ?
                 ) ORDER BY created_at ASC",
            )
            .bind(i64::try_from(limit_per_shard).unwrap_or(i64::MAX))
            .fetch_all(pool)
            .await?;
            entries.extend(rows.into_iter().map(|row| {
                (
                    shard,
                    row.get::<String, _>("idempotency_key"),
                    row.get::<String, _>("id"),
                )
            }));
        }
        Ok(entries)
    }

    pub async fn connect(url: &str) -> anyhow::Result<Self> {
        if let Some(path) = sqlite_file_path(url) {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent)
                        .with_context(|| format!("create sqlite directory {}", parent.display()))?;
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

        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await?;
        let _ = sqlx::query("PRAGMA wal_checkpoint(RESTART)")
            .execute(&pool)
            .await;
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
            CREATE INDEX IF NOT EXISTS idx_messages_delivered
                ON messages(status, delivered_at);
            CREATE INDEX IF NOT EXISTS idx_messages_topic_ready
                ON messages(topic, status, next_attempt_at, created_at);",
        )
        .execute(&pool)
        .await
        .context("creating SQLite queue schema")?;
        let _ = sqlx::query("ALTER TABLE messages ADD COLUMN lease TEXT")
            .execute(&pool)
            .await;
        sqlx::query(
            "UPDATE messages SET status = 'pending', lease = NULL WHERE status = 'processing'",
        )
        .execute(&pool)
        .await?;
        let shards = open_message_shards(url).await?;
        migrate_legacy_messages(&pool, &shards).await?;
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
                pending INTEGER NOT NULL DEFAULT 0,
                processing INTEGER NOT NULL DEFAULT 0,
                last_seen_at TEXT,
                configured INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await
        .context("creating SQLite topic stats schema")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS message_trend_minute (
                minute TEXT PRIMARY KEY NOT NULL,
                accepted INTEGER NOT NULL DEFAULT 0,
                delivered INTEGER NOT NULL DEFAULT 0
            ) WITHOUT ROWID",
        )
        .execute(&pool)
        .await
        .context("creating minute message trend schema")?;
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
        let _ =
            sqlx::query("ALTER TABLE topic_stats ADD COLUMN dropped INTEGER NOT NULL DEFAULT 0")
                .execute(&pool)
                .await;
        let _ = sqlx::query("ALTER TABLE topic_stats ADD COLUMN last_seen_at TEXT")
            .execute(&pool)
            .await;
        let _ =
            sqlx::query("ALTER TABLE topic_stats ADD COLUMN configured INTEGER NOT NULL DEFAULT 0")
                .execute(&pool)
                .await;
        let _ =
            sqlx::query("ALTER TABLE topic_stats ADD COLUMN pending INTEGER NOT NULL DEFAULT 0")
                .execute(&pool)
                .await;
        let _ =
            sqlx::query("ALTER TABLE topic_stats ADD COLUMN processing INTEGER NOT NULL DEFAULT 0")
                .execute(&pool)
                .await;
        sqlx::query(
            "DROP TRIGGER IF EXISTS trg_messages_insert_depth;
             DROP TRIGGER IF EXISTS trg_messages_update_depth;
             DROP TRIGGER IF EXISTS trg_messages_delete_depth;",
        )
        .execute(&pool)
        .await?;
        let now = Utc::now().to_rfc3339();
        sqlx::query("UPDATE topic_stats SET last_seen_at = ? WHERE last_seen_at IS NULL")
            .bind(&now)
            .execute(&pool)
            .await?;
        // Older releases promoted every MQTT subscription, including exact topic names,
        // to persistent. Only wildcard filters need a durable catalog row; exact,
        // unconfigured topics must keep the normal implicit/ephemeral policy.
        sqlx::query(
            "UPDATE topic_stats
             SET persistence = 'ephemeral'
             WHERE configured = 0
               AND persistence = 'persistent'
               AND instr(topic, '#') = 0
               AND instr(topic, '+') = 0",
        )
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
        // Queue depth is maintained incrementally during normal operation. Reconcile it once
        // on startup so upgrades and crash recovery begin from authoritative message rows.
        let queue_depths = load_queue_depths(&shards).await?;
        sqlx::query("UPDATE topic_stats SET pending = 0, processing = 0")
            .execute(&pool)
            .await?;
        for (topic, (pending, processing)) in &queue_depths {
            sqlx::query(
                "INSERT INTO topic_stats
                    (topic, pending, processing, delivery, persistence, last_seen_at)
                 VALUES (?, ?, ?, 'broadcast', 'ephemeral', ?)
                 ON CONFLICT(topic) DO UPDATE SET
                    pending = excluded.pending, processing = excluded.processing",
            )
            .bind(topic)
            .bind(pending)
            .bind(processing)
            .bind(&now)
            .execute(&pool)
            .await?;
        }
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sys_kv (
                key TEXT PRIMARY KEY NOT NULL,
                value BLOB NOT NULL,
                value_type TEXT NOT NULL DEFAULT 'string',
                expire_at TEXT,
                update_time TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sys_kv_expire ON sys_kv(expire_at);",
        )
        .execute(&pool)
        .await
        .context("creating SQLite kv cache schema")?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS sys_lock (
                lock_key TEXT PRIMARY KEY NOT NULL,
                owner TEXT NOT NULL,
                expire_at TEXT NOT NULL,
                create_time TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_sys_lock_expire ON sys_lock(expire_at);",
        )
        .execute(&pool)
        .await
        .context("creating SQLite lock schema")?;
        let rows = sqlx::query("SELECT topic, delivery, persistence FROM topic_stats")
            .fetch_all(&pool)
            .await?;
        let topics = rows
            .into_iter()
            .map(|row| {
                let topic: String = row.get("topic");
                let delivery: String = row.get("delivery");
                let persistence: String = row.get("persistence");
                (
                    topic.clone(),
                    TopicConfig {
                        topic,
                        delivery: DeliveryMode::from_stored(&delivery),
                        persistence: PersistenceMode::from_stored(&persistence),
                    },
                )
            })
            .collect();
        let depths = queue_depths;
        Ok(Self(
            pool,
            Arc::new(RwLock::new(topics)),
            Arc::new(RwLock::new(depths)),
            Arc::new(shards),
            Arc::new(AtomicUsize::new(0)),
            Arc::new(Mutex::new(HashMap::new())),
            Arc::new(Mutex::new(HashMap::new())),
        ))
    }

    pub async fn topic_persistence(&self, topic: &str) -> anyhow::Result<PersistenceMode> {
        Ok(self.topic_config(topic).await?.persistence)
    }

    pub async fn topic_config(&self, topic: &str) -> anyhow::Result<TopicConfig> {
        if let Some(config) = self.1.read().expect("topic cache").get(topic).cloned() {
            return Ok(config);
        }
        let row = sqlx::query("SELECT delivery, persistence FROM topic_stats WHERE topic = ?")
            .bind(topic)
            .fetch_optional(&self.0)
            .await?;
        let config = row
            .map(|row| TopicConfig {
                topic: topic.to_string(),
                delivery: DeliveryMode::from_stored(&row.get::<String, _>("delivery")),
                persistence: PersistenceMode::from_stored(&row.get::<String, _>("persistence")),
            })
            .unwrap_or_else(|| TopicConfig::implicit(topic));
        self.1
            .write()
            .expect("topic cache")
            .insert(topic.to_string(), config.clone());
        Ok(config)
    }

    /// Record an MQTT subscription. Wildcard filters remain in the catalog so idle purge
    /// cannot remove them; exact, unconfigured topics retain the implicit ephemeral policy.
    /// Explicit ConfigureTopics always wins.
    pub async fn note_subscribed_topic(&self, topic: &str) -> anyhow::Result<()> {
        let persistence = if topic.contains('#') || topic.contains('+') {
            "persistent"
        } else {
            "ephemeral"
        };
        sqlx::query(
            "INSERT INTO topic_stats (topic, delivery, persistence, configured, last_seen_at)
             VALUES (?, 'broadcast', ?, 0, ?)
             ON CONFLICT(topic) DO UPDATE SET
                last_seen_at = excluded.last_seen_at,
                persistence = CASE
                    WHEN IFNULL(topic_stats.configured, 0) = 0 THEN excluded.persistence
                    ELSE topic_stats.persistence
                END",
        )
        .bind(topic)
        .bind(persistence)
        .bind(Utc::now().to_rfc3339())
        .execute(&self.0)
        .await?;
        self.1.write().expect("topic cache").remove(topic);
        Ok(())
    }

    pub async fn ephemeral_topics(&self) -> anyhow::Result<Vec<String>> {
        let rows = sqlx::query("SELECT topic FROM topic_stats WHERE persistence = 'ephemeral'")
            .fetch_all(&self.0)
            .await?;
        Ok(rows.into_iter().map(|row| row.get("topic")).collect())
    }

    pub async fn configure_topics(
        &self,
        configs: &[TopicConfig],
    ) -> anyhow::Result<Vec<TopicConfig>> {
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
            self.1
                .write()
                .expect("topic cache")
                .insert(config.topic.clone(), config.clone());
        }
        self.list_topic_configs().await
    }

    pub async fn list_topic_configs(&self) -> anyhow::Result<Vec<TopicConfig>> {
        let rows =
            sqlx::query("SELECT topic, delivery, persistence FROM topic_stats ORDER BY topic")
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
        if let Err(error) = self.flush_topic_stats().await {
            tracing::warn!(%error, "topic stats flush failed during shutdown");
        }
        if let Err(error) = self.flush_message_trends().await {
            tracing::warn!(%error, "message trends flush failed during shutdown");
        }
        if let Err(error) = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
            .execute(&self.0)
            .await
        {
            tracing::warn!(%error, "sqlite wal_checkpoint failed during shutdown");
        }
        for shard in self.3.iter() {
            let _ = sqlx::query("PRAGMA wal_checkpoint(TRUNCATE)")
                .execute(shard)
                .await;
            shard.close().await;
        }
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
        self.record_message_trend(accepted, delivered);
        Ok(())
    }

    fn record_topic_stats(&self, topic: &str, delta: TopicStatsDelta) {
        let mut pending = self.5.lock().expect("topic stats delta cache");
        let entry = pending.entry(topic.to_string()).or_default();
        entry.accepted += delta.accepted;
        entry.duplicates += delta.duplicates;
        entry.delivered += delta.delivered;
        self.record_message_trend(delta.accepted, delta.delivered);
    }

    fn record_message_trend(&self, accepted: i64, delivered: i64) {
        if accepted == 0 && delivered == 0 {
            return;
        }
        let minute = Utc::now().format("%Y-%m-%dT%H:%M:00Z").to_string();
        let mut trends = self.6.lock().expect("message trend delta cache");
        let trend = trends.entry(minute).or_default();
        trend.accepted += accepted;
        trend.delivered += delivered;
    }

    pub(crate) async fn flush_message_trends(&self) -> anyhow::Result<usize> {
        let pending = {
            let mut guard = self.6.lock().expect("message trend delta cache");
            std::mem::take(&mut *guard)
        };
        if pending.is_empty() {
            return Ok(0);
        }
        let result = async {
            let mut tx = self.0.begin().await?;
            for (minute, delta) in &pending {
                sqlx::query(
                    "INSERT INTO message_trend_minute (minute, accepted, delivered)
                     VALUES (?, ?, ?)
                     ON CONFLICT(minute) DO UPDATE SET
                        accepted = message_trend_minute.accepted + excluded.accepted,
                        delivered = message_trend_minute.delivered + excluded.delivered",
                )
                .bind(minute)
                .bind(delta.accepted)
                .bind(delta.delivered)
                .execute(&mut *tx)
                .await?;
            }
            let cutoff = (Utc::now() - chrono::Duration::days(7))
                .format("%Y-%m-%dT%H:%M:00Z")
                .to_string();
            sqlx::query("DELETE FROM message_trend_minute WHERE minute < ?")
                .bind(cutoff)
                .execute(&mut *tx)
                .await?;
            tx.commit().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;
        if let Err(error) = result {
            let mut guard = self.6.lock().expect("message trend delta cache");
            for (minute, delta) in pending {
                let entry = guard.entry(minute).or_default();
                entry.accepted += delta.accepted;
                entry.delivered += delta.delivered;
            }
            return Err(error);
        }
        Ok(pending.len())
    }

    pub async fn message_trends(&self, minutes: u32) -> anyhow::Result<Vec<MessageTrendPoint>> {
        let minutes = minutes.clamp(10, 1440);
        let cutoff = (Utc::now() - chrono::Duration::minutes(i64::from(minutes - 1)))
            .format("%Y-%m-%dT%H:%M:00Z")
            .to_string();
        let rows = sqlx::query(
            "SELECT minute, accepted, delivered FROM message_trend_minute
             WHERE minute >= ? ORDER BY minute",
        )
        .bind(&cutoff)
        .fetch_all(&self.0)
        .await?;
        let mut points: HashMap<String, MessageTrendPoint> = rows
            .into_iter()
            .map(|row| {
                let minute: String = row.get("minute");
                (
                    minute.clone(),
                    MessageTrendPoint {
                        minute,
                        accepted: row.get("accepted"),
                        delivered: row.get("delivered"),
                    },
                )
            })
            .collect();
        let pending = self.6.lock().expect("message trend delta cache").clone();
        for (minute, delta) in pending {
            if minute < cutoff {
                continue;
            }
            let point = points.entry(minute.clone()).or_insert(MessageTrendPoint {
                minute,
                accepted: 0,
                delivered: 0,
            });
            point.accepted += delta.accepted;
            point.delivered += delta.delivered;
        }
        let mut points: Vec<_> = points.into_values().collect();
        points.sort_unstable_by(|left, right| left.minute.cmp(&right.minute));
        Ok(points)
    }

    pub(crate) async fn flush_topic_stats(&self) -> anyhow::Result<usize> {
        let pending = {
            let mut guard = self.5.lock().expect("topic stats delta cache");
            std::mem::take(&mut *guard)
        };
        if pending.is_empty() {
            return Ok(0);
        }

        let result = async {
            let mut tx = self.0.begin().await?;
            let now = Utc::now().to_rfc3339();
            for (topic, delta) in &pending {
                sqlx::query(
                    "INSERT INTO topic_stats
                        (topic, accepted, duplicates, delivered, delivery, persistence, last_seen_at)
                     VALUES (?, ?, ?, ?, 'broadcast', 'ephemeral', ?)
                     ON CONFLICT(topic) DO UPDATE SET
                        accepted = topic_stats.accepted + excluded.accepted,
                        duplicates = topic_stats.duplicates + excluded.duplicates,
                        delivered = topic_stats.delivered + excluded.delivered,
                        last_seen_at = excluded.last_seen_at",
                )
                .bind(topic)
                .bind(delta.accepted)
                .bind(delta.duplicates)
                .bind(delta.delivered)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        if let Err(error) = result {
            let mut guard = self.5.lock().expect("topic stats delta cache");
            for (topic, delta) in pending {
                let entry = guard.entry(topic).or_default();
                entry.accepted += delta.accepted;
                entry.duplicates += delta.duplicates;
                entry.delivered += delta.delivered;
            }
            return Err(error);
        }
        Ok(pending.len())
    }

    fn bump_queue_depth(&self, topic: &str, pending: i64, processing: i64) {
        let mut depths = self.2.write().expect("queue depth cache");
        let depth = depths.entry(topic.to_string()).or_default();
        depth.0 = (depth.0 + pending).max(0);
        depth.1 = (depth.1 + processing).max(0);
    }

    async fn shard_index_for_id(&self, id: &str) -> anyhow::Result<Option<usize>> {
        if let Some(index) = message_id_shard(id) {
            return Ok(Some(index));
        }
        for (index, shard) in self.3.iter().enumerate() {
            let found: Option<i64> = sqlx::query_scalar("SELECT 1 FROM messages WHERE id = ?")
                .bind(id)
                .fetch_optional(shard)
                .await?;
            if found.is_some() {
                return Ok(Some(index));
            }
        }
        Ok(None)
    }

    async fn refresh_queue_depths(&self) -> anyhow::Result<()> {
        let fresh = load_queue_depths(&self.3).await?;
        let mut depths = self.2.write().expect("queue depth cache");
        *depths = fresh;
        Ok(())
    }

    pub async fn status_snapshot(
        &self,
        consumer_seen_after: Option<&str>,
    ) -> anyhow::Result<Vec<TopicSnapshot>> {
        self.flush_topic_stats().await?;
        let mut topics: HashMap<String, TopicSnapshot> = HashMap::new();

        for row in sqlx::query(
            "SELECT topic, accepted, duplicates, delivered, failed, dropped, pending, processing, delivery, persistence
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
            topic.pending = row.get("pending");
            topic.processing = row.get("processing");
            let delivery: String = row.get("delivery");
            topic.delivery = DeliveryMode::from_stored(&delivery).as_str().to_string();
            let persistence: String = row.get("persistence");
            topic.persistence = PersistenceMode::from_stored(&persistence)
                .as_str()
                .to_string();
        }
        for (name, (pending, processing)) in self.2.read().expect("queue depth cache").iter() {
            let topic = topics.entry(name.clone()).or_insert_with(|| TopicSnapshot {
                name: name.clone(),
                ..TopicSnapshot::default()
            });
            topic.pending = *pending;
            topic.processing = *processing;
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

    #[cfg_attr(not(test), allow(dead_code))]
    pub async fn enqueue(
        &self,
        idempotency_key: Option<&str>,
        topic: &str,
        payload: &[u8],
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<(String, bool)> {
        let mut results = self
            .enqueue_batch(vec![EnqueueInput {
                id: new_message_id(topic),
                idempotency_key: idempotency_key
                    .filter(|value| !value.is_empty())
                    .map(ToOwned::to_owned),
                topic: topic.to_string(),
                payload: payload.to_vec(),
                attributes,
            }])
            .await?;
        Ok(results.pop().expect("one enqueue result"))
    }

    pub async fn enqueue_batch(
        &self,
        items: Vec<EnqueueInput>,
    ) -> anyhow::Result<Vec<(String, bool)>> {
        let count = items.len();
        let mut groups: Vec<Vec<(usize, EnqueueInput)>> =
            (0..MESSAGE_SHARDS).map(|_| Vec::new()).collect();
        for (index, item) in items.into_iter().enumerate() {
            groups[topic_shard(&item.topic)].push((index, item));
        }
        let mut results: Vec<Option<(String, bool)>> = vec![None; count];
        let mut stats: HashMap<String, (i64, i64)> = HashMap::new();
        for (shard_index, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut tx = self.3[shard_index].begin().await?;
            for (index, item) in group {
                let id = item.id;
                let now = Utc::now().to_rfc3339();
                let attributes = serde_json::to_string(&item.attributes)?;
                let result = sqlx::query(
                    "INSERT OR IGNORE INTO messages
                     (id, idempotency_key, topic, payload, attributes, next_attempt_at, created_at)
                     VALUES (?, ?, ?, ?, ?, ?, ?)",
                )
                .bind(&id)
                .bind(item.idempotency_key.as_deref())
                .bind(&item.topic)
                .bind(&item.payload)
                .bind(attributes)
                .bind(&now)
                .bind(&now)
                .execute(&mut *tx)
                .await?;
                let duplicate = result.rows_affected() == 0;
                let message_id = if duplicate {
                    let key = item
                        .idempotency_key
                        .as_deref()
                        .context("enqueue conflict without idempotency key")?;
                    sqlx::query_scalar("SELECT id FROM messages WHERE idempotency_key = ?")
                        .bind(key)
                        .fetch_one(&mut *tx)
                        .await?
                } else {
                    id
                };
                let entry = stats.entry(item.topic).or_default();
                if duplicate {
                    entry.1 += 1;
                } else {
                    entry.0 += 1;
                }
                results[index] = Some((message_id, duplicate));
            }
            tx.commit().await?;
        }

        for (topic, (accepted, duplicates)) in stats {
            self.record_topic_stats(
                &topic,
                TopicStatsDelta {
                    accepted,
                    duplicates,
                    ..TopicStatsDelta::default()
                },
            );
            self.bump_queue_depth(&topic, accepted, 0);
        }
        Ok(results
            .into_iter()
            .map(|result| result.expect("each enqueue has a shard result"))
            .collect())
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
        let Some(shard_index) = self.shard_index_for_id(&id).await? else {
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
        .fetch_optional(&self.3[shard_index])
        .await?
        else {
            return Ok(TopicMessagePage {
                offset,
                total,
                message: None,
            });
        };
        let attributes_raw: String = row.get("attributes");
        let attributes =
            serde_json::from_str(&attributes_raw).unwrap_or_else(|_| serde_json::json!({}));
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
                .fetch_one(&self.3[topic_shard(filter)])
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
            .fetch_optional(&self.3[topic_shard(filter)])
            .await?;
            return Ok((total, id));
        }
        let mut matched: Vec<(String, String)> = Vec::new();
        for shard in self.3.iter() {
            for row in sqlx::query("SELECT id, topic, created_at FROM messages")
                .fetch_all(shard)
                .await?
            {
                let topic: String = row.get("topic");
                if crate::topic::filter_matches(filter, &topic) {
                    matched.push((row.get("created_at"), row.get("id")));
                }
            }
        }
        matched.sort_unstable_by(|left, right| right.cmp(left));
        let total = matched.len() as i64;
        let id = matched.get(offset as usize).map(|(_, id)| id.clone());
        Ok((total, id))
    }

    pub async fn clear_topic_messages(&self, filter: &str) -> anyhow::Result<u64> {
        self.flush_topic_stats().await?;
        let mut deleted = 0u64;
        if !crate::topic::is_wildcard_filter(filter) {
            deleted = sqlx::query("DELETE FROM messages WHERE topic = ?")
                .bind(filter)
                .execute(&self.3[topic_shard(filter)])
                .await?
                .rows_affected();
        } else {
            for shard in self.3.iter() {
                let rows = sqlx::query("SELECT id, topic FROM messages")
                    .fetch_all(shard)
                    .await?;
                for row in rows {
                    let topic: String = row.get("topic");
                    if crate::topic::filter_matches(filter, &topic) {
                        deleted += sqlx::query("DELETE FROM messages WHERE id = ?")
                            .bind(row.get::<String, _>("id"))
                            .execute(shard)
                            .await?
                            .rows_affected();
                    }
                }
            }
        }
        self.reset_topic_stats(filter).await?;
        self.refresh_queue_depths().await?;
        Ok(deleted)
    }

    async fn reset_topic_stats(&self, filter: &str) -> anyhow::Result<()> {
        let names: Vec<String> = sqlx::query_scalar("SELECT topic FROM topic_stats")
            .fetch_all(&self.0)
            .await?;
        for name in names {
            let hit = name == filter || crate::topic::filter_matches(filter, &name);
            if !hit {
                continue;
            }
            sqlx::query(
                "UPDATE topic_stats
                 SET accepted = 0, duplicates = 0, delivered = 0, failed = 0, dropped = 0,
                     pending = 0, processing = 0
                 WHERE topic = ?",
            )
            .bind(name)
            .execute(&self.0)
            .await?;
        }
        Ok(())
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
        .execute(&self.3[topic_shard(topic)])
        .await?;
        let dropped = result.rows_affected();
        if dropped > 0 {
            self.bump_dropped(topic, dropped as i64).await?;
            self.bump_queue_depth(topic, -(dropped as i64), 0);
        }
        Ok(dropped)
    }

    pub async fn drop_claimed(&self, id: &str, lease: &str, error: &str) -> anyhow::Result<bool> {
        let Some(shard_index) = self.shard_index_for_id(id).await? else {
            return Ok(false);
        };
        let shard = &self.3[shard_index];
        let row = sqlx::query(
            "SELECT topic FROM messages WHERE id = ? AND lease = ? AND status = 'processing'",
        )
        .bind(id)
        .bind(lease)
        .fetch_optional(shard)
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
        .execute(shard)
        .await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        self.bump_dropped(&topic, 1).await?;
        self.bump_queue_depth(&topic, 0, -1);
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
        let now = Utc::now().to_rfc3339();
        let mut affected = 0;
        for shard in self.3.iter() {
            affected += sqlx::query(
                "UPDATE messages SET status = 'pending', lease = NULL
                 WHERE status = 'processing' AND next_attempt_at <= ?",
            )
            .bind(&now)
            .execute(shard)
            .await?
            .rows_affected();
        }
        if affected > 0 {
            self.refresh_queue_depths().await?;
        }
        Ok(affected)
    }

    pub async fn claim_next_for_topic(
        &self,
        topic: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let now = Utc::now();
        let now_text = now.to_rfc3339();
        let lease = Uuid::new_v4().to_string();
        let visible_again = (now
            + chrono::Duration::from_std(visibility).unwrap_or(chrono::Duration::seconds(30)))
        .to_rfc3339();
        let row = sqlx::query(
            "UPDATE messages
             SET status = 'processing', attempts = attempts + 1, lease = ?, next_attempt_at = ?
             WHERE id = (
                 SELECT id FROM messages
                 WHERE status = 'pending' AND next_attempt_at <= ? AND topic = ?
                 ORDER BY created_at LIMIT 1
             ) AND status = 'pending'
             RETURNING id, topic, payload, attributes, created_at",
        )
        .bind(&lease)
        .bind(&visible_again)
        .bind(&now_text)
        .bind(topic)
        .fetch_optional(&self.3[topic_shard(topic)])
        .await?;
        let Some(row) = row else {
            return Ok(None);
        };
        let topic: String = row.get("topic");
        self.bump_queue_depth(&topic, -1, 1);
        Ok(Some(ClaimedMessage {
            id: row.get("id"),
            topic,
            payload: row.get("payload"),
            attributes: serde_json::from_str(&row.get::<String, _>("attributes"))?,
            created_at: row.get("created_at"),
            lease,
        }))
    }

    pub async fn claim_next_for_filter(
        &self,
        filter: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        if !crate::topic::is_wildcard_filter(filter) {
            return self.claim_next_for_topic(filter, visibility).await;
        }
        let start = self.4.fetch_add(1, Ordering::Relaxed) % MESSAGE_SHARDS;
        for offset in 0..MESSAGE_SHARDS {
            let shard_index = (start + offset) % MESSAGE_SHARDS;
            if let Some(message) = self
                .claim_next_matching_on_shard(shard_index, filter, visibility)
                .await?
            {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }

    pub async fn release(&self, id: &str, lease: &str) -> anyhow::Result<bool> {
        let Some(shard_index) = self.shard_index_for_id(id).await? else {
            return Ok(false);
        };
        let topic: Option<String> = sqlx::query_scalar(
            "UPDATE messages SET status = 'pending', lease = NULL
             WHERE id = ? AND lease = ? AND status = 'processing' RETURNING topic",
        )
        .bind(id)
        .bind(lease)
        .fetch_optional(&self.3[shard_index])
        .await?;
        if let Some(topic) = topic {
            self.bump_queue_depth(&topic, 1, -1);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub async fn claim_next_excluding(
        &self,
        visibility: Duration,
        skip_topics: &[String],
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let start = self.4.fetch_add(1, Ordering::Relaxed) % MESSAGE_SHARDS;
        for offset in 0..MESSAGE_SHARDS {
            let shard_index = (start + offset) % MESSAGE_SHARDS;
            let placeholders = std::iter::repeat_n("?", skip_topics.len())
                .collect::<Vec<_>>()
                .join(",");
            let exclusion = if skip_topics.is_empty() {
                String::new()
            } else {
                format!(" AND topic NOT IN ({placeholders})")
            };
            let sql = format!(
                "SELECT id, topic, payload, attributes, created_at FROM messages
                 WHERE status = 'pending' AND next_attempt_at <= ?{exclusion}
                 ORDER BY created_at LIMIT 1"
            );
            let now = Utc::now();
            let mut query = sqlx::query(&sql).bind(now.to_rfc3339());
            for topic in skip_topics {
                query = query.bind(topic);
            }
            let row = query.fetch_optional(&self.3[shard_index]).await?;
            if let Some(message) = self.finish_claim(shard_index, now, visibility, row).await? {
                return Ok(Some(message));
            }
        }
        Ok(None)
    }

    async fn claim_next_matching_on_shard(
        &self,
        shard_index: usize,
        filter: &str,
        visibility: Duration,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let now_text = Utc::now().to_rfc3339();
        let rows = sqlx::query(
            "SELECT id, topic FROM messages
             WHERE status = 'pending' AND next_attempt_at <= ?
             ORDER BY created_at LIMIT 64",
        )
        .bind(&now_text)
        .fetch_all(&self.3[shard_index])
        .await?;
        let Some(id) = rows.into_iter().find_map(|row| {
            let topic: String = row.get("topic");
            crate::topic::filter_matches(filter, &topic).then(|| row.get::<String, _>("id"))
        }) else {
            return Ok(None);
        };
        let now = Utc::now();
        let row = sqlx::query(
            "SELECT id, topic, payload, attributes, created_at FROM messages
             WHERE id = ? AND status = 'pending'",
        )
        .bind(&id)
        .fetch_optional(&self.3[shard_index])
        .await?;
        self.finish_claim(shard_index, now, visibility, row).await
    }

    async fn finish_claim(
        &self,
        shard_index: usize,
        now: chrono::DateTime<Utc>,
        visibility: Duration,
        row: Option<sqlx::sqlite::SqliteRow>,
    ) -> anyhow::Result<Option<ClaimedMessage>> {
        let Some(row) = row else {
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
            .execute(&self.3[shard_index])
            .await?;
        if changed.rows_affected() == 0 {
            return Ok(None);
        }
        let topic: String = row.get("topic");
        self.bump_queue_depth(&topic, -1, 1);
        Ok(Some(ClaimedMessage {
            id,
            topic,
            payload: row.get("payload"),
            attributes: serde_json::from_str(&row.get::<String, _>("attributes"))?,
            created_at: row.get("created_at"),
            lease,
        }))
    }

    pub async fn delivered(&self, id: &str, lease: &str) -> anyhow::Result<bool> {
        let mut results = self
            .delivered_batch(vec![(id.to_string(), lease.to_string())])
            .await?;
        Ok(results.pop().unwrap_or(false))
    }

    pub async fn delivered_batch(
        &self,
        messages: Vec<(String, String)>,
    ) -> anyhow::Result<Vec<bool>> {
        let count = messages.len();
        let mut groups: Vec<Vec<(usize, String, String)>> =
            (0..MESSAGE_SHARDS).map(|_| Vec::new()).collect();
        let mut results = vec![false; count];
        for (index, (id, lease)) in messages.into_iter().enumerate() {
            if let Some(shard_index) = self.shard_index_for_id(&id).await? {
                groups[shard_index].push((index, id, lease));
            }
        }
        let now = Utc::now().to_rfc3339();
        let mut stats: HashMap<String, i64> = HashMap::new();
        for (shard_index, group) in groups.into_iter().enumerate() {
            if group.is_empty() {
                continue;
            }
            let mut tx = self.3[shard_index].begin().await?;
            for (index, id, lease) in group {
                let topic: Option<String> = sqlx::query_scalar(
                    "UPDATE messages
                     SET status = 'delivered', delivered_at = ?, last_error = NULL, lease = NULL
                     WHERE id = ? AND lease = ? AND status = 'processing'
                     RETURNING topic",
                )
                .bind(&now)
                .bind(id)
                .bind(lease)
                .fetch_optional(&mut *tx)
                .await?;
                if let Some(topic) = topic {
                    *stats.entry(topic).or_default() += 1;
                    results[index] = true;
                }
            }
            tx.commit().await?;
        }
        for (topic, delivered) in stats {
            self.record_topic_stats(
                &topic,
                TopicStatsDelta {
                    delivered,
                    ..TopicStatsDelta::default()
                },
            );
            self.bump_queue_depth(&topic, 0, -delivered);
        }
        Ok(results)
    }

    pub async fn failed(
        &self,
        id: &str,
        lease: &str,
        error: &str,
        max_attempts: i64,
    ) -> anyhow::Result<bool> {
        let Some(shard_index) = self.shard_index_for_id(id).await? else {
            return Ok(false);
        };
        let shard = &self.3[shard_index];
        let row = sqlx::query(
            "SELECT topic, attempts FROM messages WHERE id = ? AND lease = ? AND status = 'processing'",
        )
            .bind(id)
            .bind(lease)
            .fetch_optional(shard)
            .await?;
        let Some(row) = row else {
            return Ok(false);
        };
        let topic: String = row.get("topic");
        let attempts: i64 = row.get("attempts");
        let status = if attempts >= max_attempts {
            "failed"
        } else {
            "pending"
        };
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
        .execute(shard)
        .await?;
        if result.rows_affected() == 0 {
            return Ok(false);
        }
        if status == "failed" {
            self.bump_topic_stat(&topic, 0, 0, 0, 1).await?;
            self.bump_queue_depth(&topic, 0, -1);
        } else {
            self.bump_queue_depth(&topic, 1, -1);
        }
        Ok(true)
    }

    pub async fn record_live_fanout(&self, topic: &str) -> anyhow::Result<()> {
        self.bump_topic_stat(topic, 1, 0, 1, 0).await
    }

    /// On-disk size of the control database and all message shards, including sidecars.
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
        let mut total =
            file_len(&path) + file_len(&format!("{path}-wal")) + file_len(&format!("{path}-shm"));
        for shard in self.3.iter() {
            let shard_path: String =
                sqlx::query_scalar("SELECT file FROM pragma_database_list WHERE name = 'main'")
                    .fetch_one(shard)
                    .await?;
            total += file_len(&shard_path)
                + file_len(&format!("{shard_path}-wal"))
                + file_len(&format!("{shard_path}-shm"));
        }
        Ok(total)
    }

    pub async fn purge_older_than(&self, cutoff: &str) -> anyhow::Result<u64> {
        let mut affected = 0;
        for shard in self.3.iter() {
            affected += sqlx::query("DELETE FROM messages WHERE created_at < ?")
                .bind(cutoff)
                .execute(shard)
                .await?
                .rows_affected();
        }
        if affected > 0 {
            self.refresh_queue_depths().await?;
        }
        Ok(affected)
    }

    /// Remove delivered rows whose `delivered_at` is older than `cutoff`.
    /// Caller passes `now - delivered_retention_hours`. Other statuses stay.
    pub async fn purge_delivered_older_than(&self, cutoff: &str) -> anyhow::Result<u64> {
        let mut affected = 0;
        for shard in self.3.iter() {
            affected += sqlx::query(
                "DELETE FROM messages
                 WHERE status = 'delivered' AND delivered_at IS NOT NULL AND delivered_at < ?",
            )
            .bind(cutoff)
            .execute(shard)
            .await?
            .rows_affected();
        }
        Ok(affected)
    }

    /// Remove implicit ephemeral topics that have been idle since `cutoff`.
    /// Caller passes `now - ephemeral_idle_hours` and schedules on `purge_interval_hours`
    /// (both default to 1 hour). Live subscriber topics in `keep` and ConfigureTopics stay.
    pub async fn purge_idle_ephemeral(&self, cutoff: &str, keep: &[String]) -> anyhow::Result<u64> {
        self.flush_topic_stats().await?;
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
                .execute(&self.3[topic_shard(topic)])
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
        {
            let mut depths = self.2.write().expect("queue depth cache");
            let mut configs = self.1.write().expect("topic cache");
            for topic in &stale {
                depths.remove(topic);
                configs.remove(topic);
            }
        }
        Ok(deleted)
    }
}

pub(crate) fn topic_shard(topic: &str) -> usize {
    let mut hash = 0xcbf29ce484222325u64;
    for byte in topic.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    (hash as usize) & (MESSAGE_SHARDS - 1)
}

fn prefixed_message_id(shard: usize) -> String {
    format!("s{shard:02}-{}", Uuid::new_v4())
}

pub(crate) fn new_message_id(topic: &str) -> String {
    prefixed_message_id(topic_shard(topic))
}

pub(crate) fn message_id_shard(id: &str) -> Option<usize> {
    let shard = id.strip_prefix('s')?.get(..2)?.parse::<usize>().ok()?;
    (shard < MESSAGE_SHARDS && id.as_bytes().get(3) == Some(&b'-')).then_some(shard)
}

async fn open_message_shards(url: &str) -> anyhow::Result<Vec<SqlitePool>> {
    let base = sqlite_file_path(url).context("message sharding requires a file SQLite URL")?;
    let parent = base.parent().unwrap_or_else(|| std::path::Path::new("."));
    let stem = base
        .file_stem()
        .and_then(|value| value.to_str())
        .unwrap_or("queue");
    let mut shards = Vec::with_capacity(MESSAGE_SHARDS);
    for index in 0..MESSAGE_SHARDS {
        let path = parent.join(format!("{stem}-{index:02}.db"));
        let shard_url = format!("sqlite:{}", path.display());
        let options = SqliteConnectOptions::from_str(&shard_url)?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .synchronous(SqliteSynchronous::Normal)
            .busy_timeout(Duration::from_secs(5));
        let shard = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(options)
            .await?;
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
            CREATE INDEX IF NOT EXISTS idx_messages_delivered
                ON messages(status, delivered_at);
            CREATE INDEX IF NOT EXISTS idx_messages_topic_ready
                ON messages(topic, status, next_attempt_at, created_at);",
        )
        .execute(&shard)
        .await?;
        sqlx::query(
            "UPDATE messages SET status = 'pending', lease = NULL WHERE status = 'processing'",
        )
        .execute(&shard)
        .await?;
        shards.push(shard);
    }
    Ok(shards)
}

async fn migrate_legacy_messages(
    control: &SqlitePool,
    shards: &[SqlitePool],
) -> anyhow::Result<()> {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS queue_migrations (
            name TEXT PRIMARY KEY NOT NULL, completed_at TEXT NOT NULL, rows_copied INTEGER NOT NULL
         )",
    )
    .execute(control)
    .await?;
    let complete: Option<i64> = sqlx::query_scalar(
        "SELECT rows_copied FROM queue_migrations WHERE name = 'message-shards-v1'",
    )
    .fetch_optional(control)
    .await?;
    if complete.is_some() {
        return Ok(());
    }
    let rows = sqlx::query(
        "SELECT id, idempotency_key, topic, payload, attributes, status, attempts,
                next_attempt_at, last_error, created_at, delivered_at, lease FROM messages",
    )
    .fetch_all(control)
    .await?;
    for row in &rows {
        let topic: String = row.get("topic");
        let shard_index = topic_shard(&topic);
        let legacy_id: String = row.get("id");
        let migrated_id = if message_id_shard(&legacy_id).is_some() {
            legacy_id
        } else {
            format!("s{shard_index:02}-{legacy_id}")
        };
        let shard = &shards[shard_index];
        sqlx::query(
            "INSERT OR IGNORE INTO messages
             (id, idempotency_key, topic, payload, attributes, status, attempts,
              next_attempt_at, last_error, created_at, delivered_at, lease)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(&migrated_id)
        .bind(row.get::<Option<String>, _>("idempotency_key"))
        .bind(&topic)
        .bind(row.get::<Vec<u8>, _>("payload"))
        .bind(row.get::<String, _>("attributes"))
        .bind(row.get::<String, _>("status"))
        .bind(row.get::<i64, _>("attempts"))
        .bind(row.get::<String, _>("next_attempt_at"))
        .bind(row.get::<Option<String>, _>("last_error"))
        .bind(row.get::<String, _>("created_at"))
        .bind(row.get::<Option<String>, _>("delivered_at"))
        .bind(row.get::<Option<String>, _>("lease"))
        .execute(shard)
        .await?;
        let copied: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE id = ?")
            .bind(&migrated_id)
            .fetch_one(shard)
            .await?;
        anyhow::ensure!(copied == 1, "legacy message {migrated_id} was not copied");
    }
    sqlx::query(
        "INSERT INTO queue_migrations (name, completed_at, rows_copied) VALUES ('message-shards-v1', ?, ?)",
    )
    .bind(Utc::now().to_rfc3339())
    .bind(rows.len() as i64)
    .execute(control)
    .await?;
    tracing::info!(
        messages = rows.len(),
        shards = MESSAGE_SHARDS,
        "copied legacy queue messages into shards"
    );
    Ok(())
}

async fn load_queue_depths(shards: &[SqlitePool]) -> anyhow::Result<HashMap<String, (i64, i64)>> {
    let mut depths = HashMap::new();
    for shard in shards {
        for row in sqlx::query(
            "SELECT topic,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) AS pending,
                    SUM(CASE WHEN status = 'processing' THEN 1 ELSE 0 END) AS processing
             FROM messages GROUP BY topic",
        )
        .fetch_all(shard)
        .await?
        {
            let entry = depths
                .entry(row.get::<String, _>("topic"))
                .or_insert((0, 0));
            entry.0 += row.get::<i64, _>("pending");
            entry.1 += row.get::<i64, _>("processing");
        }
    }
    Ok(depths)
}

async fn migrate_consumers(pool: &SqlitePool) -> anyhow::Result<()> {
    let columns: Vec<String> = sqlx::query("PRAGMA table_info(consumers)")
        .fetch_all(pool)
        .await?
        .into_iter()
        .map(|row| row.get::<String, _>("name"))
        .collect();
    if columns.iter().any(|name| name == "name")
        && !columns.iter().any(|name| name == "downstream_url")
    {
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
    sqlx::query(
        "CREATE INDEX IF NOT EXISTS idx_consumers_topic_seen ON consumers(topic, last_seen_at)",
    )
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
    async fn creates_sixteen_stable_message_shards() {
        let (db, dir) = temp_db().await;
        for index in 0..MESSAGE_SHARDS {
            assert!(dir.join(format!("queue-{index:02}.db")).is_file());
        }
        for topic in ["jobs/a", "jobs/b", "images/seed", "events/archive"] {
            let (id, duplicate) = db
                .enqueue(None, topic, b"payload", HashMap::new())
                .await
                .unwrap();
            let shard = topic_shard(topic);
            assert!(!duplicate);
            assert!(id.starts_with(&format!("s{shard:02}-")));
            let stored: i64 =
                sqlx::query_scalar("SELECT COUNT(*) FROM messages WHERE id = ? AND topic = ?")
                    .bind(&id)
                    .bind(topic)
                    .fetch_one(&db.3[shard])
                    .await
                    .unwrap();
            assert_eq!(stored, 1);
        }
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn safely_copies_legacy_messages_and_keeps_rollback_rows() {
        let dir = std::env::temp_dir().join(format!("cangling-legacy-test-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let url = format!("sqlite:{}/queue.db", dir.display());
        let legacy = SqlitePool::connect_with(
            SqliteConnectOptions::from_str(&url)
                .unwrap()
                .create_if_missing(true),
        )
        .await
        .unwrap();
        sqlx::query(
            "CREATE TABLE messages (
                id TEXT PRIMARY KEY NOT NULL, idempotency_key TEXT UNIQUE, topic TEXT NOT NULL,
                payload BLOB NOT NULL, attributes TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0, next_attempt_at TEXT NOT NULL, last_error TEXT,
                created_at TEXT NOT NULL, delivered_at TEXT, lease TEXT)",
        )
        .execute(&legacy)
        .await
        .unwrap();
        let now = Utc::now().to_rfc3339();
        sqlx::query(
            "INSERT INTO messages
             (id, idempotency_key, topic, payload, attributes, next_attempt_at, created_at)
             VALUES ('legacy-id', 'legacy-key', 'legacy/jobs', X'01', '{}', ?, ?)",
        )
        .bind(&now)
        .bind(&now)
        .execute(&legacy)
        .await
        .unwrap();
        legacy.close().await;

        let db = Database::connect(&url).await.unwrap();
        let shard = topic_shard("legacy/jobs");
        let migrated: String =
            sqlx::query_scalar("SELECT id FROM messages WHERE idempotency_key = 'legacy-key'")
                .fetch_one(&db.3[shard])
                .await
                .unwrap();
        assert_eq!(migrated, format!("s{shard:02}-legacy-id"));
        let rollback_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM messages")
            .fetch_one(&db.0)
            .await
            .unwrap();
        assert_eq!(rollback_rows, 1);
        let copied: i64 = sqlx::query_scalar(
            "SELECT rows_copied FROM queue_migrations WHERE name = 'message-shards-v1'",
        )
        .fetch_one(&db.0)
        .await
        .unwrap();
        assert_eq!(copied, 1);
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
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
        let topic = listed
            .iter()
            .find(|item| item.topic == "live-events")
            .unwrap();
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
        let stats = snapshot
            .iter()
            .find(|item| item.name == "live-events")
            .unwrap();
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
        let stats = snapshot
            .iter()
            .find(|item| item.name == "live-events")
            .unwrap();
        assert_eq!(stats.pending, 0);
        assert_eq!(stats.dropped, 1);

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn topic_message_page_walks_latest_first() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "jobs", b"one", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "jobs", b"two", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "other", b"skip", HashMap::new())
            .await
            .unwrap();
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
        db.enqueue(None, "other", b"no", HashMap::new())
            .await
            .unwrap();
        let page = db.topic_message_page("building/#", 0).await.unwrap();
        assert_eq!(page.total, 1);
        assert_eq!(page.message.as_ref().unwrap().topic, "building/a");
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn clear_topic_messages_deletes_only_that_topic() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "jobs", b"a", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "jobs", b"b", HashMap::new())
            .await
            .unwrap();
        db.enqueue(None, "other", b"c", HashMap::new())
            .await
            .unwrap();
        assert_eq!(db.clear_topic_messages("jobs").await.unwrap(), 2);
        assert!(db
            .topic_message_page("jobs", 0)
            .await
            .unwrap()
            .message
            .is_none());
        assert_eq!(db.topic_message_page("other", 0).await.unwrap().total, 1);
        let snapshot = db.status_snapshot(None).await.unwrap();
        let jobs = snapshot.iter().find(|item| item.name == "jobs").unwrap();
        assert_eq!(jobs.accepted, 0);
        assert_eq!(jobs.pending, 0);
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
            topic.consumers[0]
                .attributes
                .get("host")
                .map(String::as_str),
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
    async fn concurrent_claims_deliver_a_message_to_only_one_worker() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "jobs", b"once", HashMap::new())
            .await
            .unwrap();

        let mut workers = Vec::new();
        for _ in 0..16 {
            let worker_db = db.clone();
            workers.push(tokio::spawn(async move {
                worker_db
                    .claim_next_for_topic("jobs", Duration::from_secs(30))
                    .await
            }));
        }

        let mut claimed = Vec::new();
        for worker in workers {
            if let Some(message) = worker.await.unwrap().unwrap() {
                claimed.push(message);
            }
        }
        assert_eq!(claimed.len(), 1);
        db.delivered(&claimed[0].id, &claimed[0].lease)
            .await
            .unwrap();

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn mqtt_subscription_only_persists_wildcard_filters() {
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
            PersistenceMode::Ephemeral
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
        assert!(!names.contains(&"jobs"));

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
    async fn message_trends_aggregate_accepts_and_deliveries_by_minute() {
        let (db, dir) = temp_db().await;
        db.enqueue(None, "jobs", b"payload", HashMap::new())
            .await
            .unwrap();
        let claimed = db
            .claim_next_for_topic("jobs", Duration::from_secs(30))
            .await
            .unwrap()
            .unwrap();
        db.delivered(&claimed.id, &claimed.lease).await.unwrap();
        db.record_live_fanout("live").await.unwrap();

        let live = db.message_trends(60).await.unwrap();
        assert_eq!(live.iter().map(|point| point.accepted).sum::<i64>(), 2);
        assert_eq!(live.iter().map(|point| point.delivered).sum::<i64>(), 2);

        db.flush_message_trends().await.unwrap();
        let persisted = db.message_trends(60).await.unwrap();
        assert_eq!(persisted.iter().map(|point| point.accepted).sum::<i64>(), 2);
        assert_eq!(
            persisted.iter().map(|point| point.delivered).sum::<i64>(),
            2
        );

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn purge_delivered_older_than_keeps_pending_and_recent() {
        let (db, dir) = temp_db().await;
        let visibility = Duration::from_secs(5);
        let (old_id, _) = db
            .enqueue(None, "jobs", b"old-delivered", HashMap::new())
            .await
            .unwrap();
        let old = db
            .claim_next_for_topic("jobs", visibility)
            .await
            .unwrap()
            .expect("old claim");
        assert_eq!(old.id, old_id);
        db.delivered(&old.id, &old.lease).await.unwrap();

        let (recent_id, _) = db
            .enqueue(None, "jobs", b"recent-delivered", HashMap::new())
            .await
            .unwrap();
        let recent = db
            .claim_next_for_topic("jobs", visibility)
            .await
            .unwrap()
            .expect("recent claim");
        assert_eq!(recent.id, recent_id);
        db.delivered(&recent.id, &recent.lease).await.unwrap();

        let (pending_id, _) = db
            .enqueue(None, "jobs", b"still-pending", HashMap::new())
            .await
            .unwrap();

        let stale = (Utc::now() - chrono::Duration::hours(25)).to_rfc3339();
        let old_shard = topic_shard("jobs");
        sqlx::query("UPDATE messages SET delivered_at = ? WHERE id = ?")
            .bind(&stale)
            .bind(&old_id)
            .execute(&db.3[old_shard])
            .await
            .unwrap();

        let cutoff = (Utc::now() - chrono::Duration::hours(24)).to_rfc3339();
        let deleted = db.purge_delivered_older_than(&cutoff).await.unwrap();
        assert_eq!(deleted, 1);

        let remaining: Vec<String> = sqlx::query("SELECT id FROM messages ORDER BY created_at")
            .fetch_all(&db.3[old_shard])
            .await
            .unwrap()
            .into_iter()
            .map(|row| row.get::<String, _>("id"))
            .collect();
        assert_eq!(remaining, vec![recent_id, pending_id]);

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
