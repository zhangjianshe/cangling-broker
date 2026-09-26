use std::{
    collections::{HashMap, VecDeque},
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, watch};

use crate::db::{message_id_shard, new_message_id, topic_shard, Database, EnqueueInput};

type DeliveredResult = Result<bool, String>;

struct EnqueueCommand {
    input: EnqueueInput,
}

struct DeliveredCommand {
    id: String,
    lease: String,
    response: oneshot::Sender<DeliveredResult>,
}

enum WriteCommand {
    Enqueue(EnqueueCommand),
    Delivered(DeliveredCommand),
    Shutdown(oneshot::Sender<()>),
}

/// Accepts persistent publishes into bounded memory queues and commits them to SQLite in batches.
/// A caller is acknowledged after queue admission; graceful shutdown drains every admitted command.
#[derive(Clone)]
pub struct QueueWriter {
    tx: Arc<Vec<mpsc::Sender<WriteCommand>>>,
    recent_ids: Arc<Vec<Mutex<RecentIds>>>,
    notifications: QueueNotifications,
    stats_shutdown: watch::Sender<bool>,
}

struct RecentIds {
    values: HashMap<String, String>,
    order: VecDeque<String>,
    capacity: usize,
}

impl RecentIds {
    fn new(capacity: usize) -> Self {
        Self {
            values: HashMap::new(),
            order: VecDeque::new(),
            capacity,
        }
    }

    fn get(&self, key: &str) -> Option<String> {
        self.values.get(key).cloned()
    }

    fn insert(&mut self, key: String, id: String) {
        if let Some(existing) = self.values.get_mut(&key) {
            *existing = id;
            return;
        }
        while self.values.len() >= self.capacity {
            if let Some(oldest) = self.order.pop_front() {
                self.values.remove(&oldest);
            }
        }
        self.order.push_back(key.clone());
        self.values.insert(key, id);
    }

    fn remove(&mut self, key: &str) {
        self.values.remove(key);
        self.order.retain(|candidate| candidate != key);
    }
}

#[derive(Clone, Default)]
struct QueueNotifications {
    topics: Arc<Mutex<HashMap<String, watch::Sender<u64>>>>,
    global: Arc<Mutex<Option<watch::Sender<u64>>>>,
}

impl QueueNotifications {
    fn subscribe(&self, topic: &str) -> watch::Receiver<u64> {
        let slot = if crate::topic::is_wildcard_filter(topic) {
            &self.global
        } else {
            let mut topics = self.topics.lock().expect("topic notification lock");
            return topics
                .entry(topic.to_string())
                .or_insert_with(|| watch::channel(0).0)
                .subscribe();
        };
        let mut sender = slot.lock().expect("global notification lock");
        sender
            .get_or_insert_with(|| watch::channel(0).0)
            .subscribe()
    }

    fn notify(&self, topic: &str) {
        if let Some(sender) = self
            .topics
            .lock()
            .expect("topic notification lock")
            .get(topic)
        {
            sender.send_modify(|generation| *generation = generation.wrapping_add(1));
        }
        if let Some(sender) = self
            .global
            .lock()
            .expect("global notification lock")
            .as_ref()
        {
            sender.send_modify(|generation| *generation = generation.wrapping_add(1));
        }
    }
}

impl QueueWriter {
    pub async fn start(
        db: Database,
        queue_size: usize,
        batch_size: usize,
        batch_wait: Duration,
    ) -> anyhow::Result<(Self, tokio::task::JoinHandle<()>)> {
        let notifications = QueueNotifications::default();
        let (stats_shutdown, mut stats_shutdown_rx) = watch::channel(false);
        let mut senders = Vec::with_capacity(16);
        let mut workers = Vec::with_capacity(16);
        let per_shard_queue = queue_size.div_ceil(16).max(1);
        let recent_ids_per_shard = per_shard_queue.saturating_mul(4).max(64);
        let recent_ids: Vec<_> = (0..16)
            .map(|_| Mutex::new(RecentIds::new(recent_ids_per_shard)))
            .collect();
        for (shard, key, id) in db.recent_idempotency_keys(recent_ids_per_shard).await? {
            recent_ids[shard]
                .lock()
                .expect("recent id lock")
                .insert(key, id);
        }
        for _ in 0..16 {
            let (tx, rx) = mpsc::channel(per_shard_queue);
            senders.push(tx);
            workers.push(tokio::spawn(run_writer(
                db.clone(),
                rx,
                batch_size.max(1),
                batch_wait,
                notifications.clone(),
            )));
        }
        let stats_db = db.clone();
        let stats_worker = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_millis(100));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    _ = interval.tick() => {
                        if let Err(error) = stats_db.flush_topic_stats().await {
                            tracing::warn!(%error, "periodic topic stats flush failed");
                        }
                    }
                    changed = stats_shutdown_rx.changed() => {
                        if changed.is_err() || *stats_shutdown_rx.borrow() {
                            break;
                        }
                    }
                }
            }
            if let Err(error) = stats_db.flush_topic_stats().await {
                tracing::warn!(%error, "final topic stats flush failed");
            }
        });
        let handle = tokio::spawn(async move {
            for worker in workers {
                let _ = worker.await;
            }
            let _ = stats_worker.await;
        });
        Ok((
            Self {
                tx: Arc::new(senders),
                recent_ids: Arc::new(recent_ids),
                notifications,
                stats_shutdown,
            },
            handle,
        ))
    }

    pub async fn enqueue(
        &self,
        idempotency_key: Option<&str>,
        topic: &str,
        payload: &[u8],
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<(String, bool)> {
        let shard = topic_shard(topic);
        let key = idempotency_key.filter(|value| !value.is_empty());
        let id = new_message_id(topic);
        if let Some(key) = key {
            let mut recent = self.recent_ids[shard].lock().expect("recent id lock");
            if let Some(id) = recent.get(key) {
                return Ok((id, true));
            }
            recent.insert(key.to_string(), id.clone());
        }
        let command = WriteCommand::Enqueue(EnqueueCommand {
            input: EnqueueInput {
                id: id.clone(),
                idempotency_key: key.map(ToOwned::to_owned),
                topic: topic.to_string(),
                payload: payload.to_vec(),
                attributes,
            },
        });
        if self.tx[shard].send(command).await.is_err() {
            if let Some(key) = key {
                self.recent_ids[shard]
                    .lock()
                    .expect("recent id lock")
                    .remove(key);
            }
            anyhow::bail!("queue writer stopped");
        }
        Ok((id, false))
    }

    pub fn subscribe(&self, topic: &str) -> watch::Receiver<u64> {
        self.notifications.subscribe(topic)
    }

    pub async fn shutdown(&self) {
        let mut waits = Vec::with_capacity(self.tx.len());
        for tx in self.tx.iter() {
            let (response, receive) = oneshot::channel();
            if tx.send(WriteCommand::Shutdown(response)).await.is_ok() {
                waits.push(receive);
            }
        }
        for receive in waits {
            let _ = receive.await;
        }
        let _ = self.stats_shutdown.send(true);
    }

    pub async fn delivered(&self, id: &str, lease: &str) -> anyhow::Result<bool> {
        let (response, receive) = oneshot::channel();
        let shard = message_id_shard(id).unwrap_or(0);
        self.tx[shard]
            .send(WriteCommand::Delivered(DeliveredCommand {
                id: id.to_string(),
                lease: lease.to_string(),
                response,
            }))
            .await
            .map_err(|_| anyhow::anyhow!("queue writer stopped"))?;
        receive
            .await
            .map_err(|_| anyhow::anyhow!("queue writer stopped"))?
            .map_err(anyhow::Error::msg)
    }
}

async fn run_writer(
    db: Database,
    mut rx: mpsc::Receiver<WriteCommand>,
    batch_size: usize,
    batch_wait: Duration,
    notifications: QueueNotifications,
) {
    while let Some(command) = rx.recv().await {
        if let WriteCommand::Shutdown(response) = command {
            let _ = response.send(());
            break;
        }
        let mut batch = vec![command];
        let mut shutdown = None;
        let deadline = tokio::time::sleep(batch_wait);
        tokio::pin!(deadline);
        while batch.len() < batch_size {
            tokio::select! {
                _ = &mut deadline => break,
                command = rx.recv() => match command {
                    Some(WriteCommand::Shutdown(response)) => {
                        shutdown = Some(response);
                        break;
                    }
                    Some(command) => batch.push(command),
                    None => break,
                }
            }
        }
        let mut enqueues = Vec::new();
        let mut delivered = Vec::new();
        for command in batch {
            match command {
                WriteCommand::Enqueue(command) => enqueues.push(command),
                WriteCommand::Delivered(command) => delivered.push(command),
                WriteCommand::Shutdown(_) => unreachable!(),
            }
        }
        if !enqueues.is_empty() {
            let topics: Vec<String> = enqueues
                .iter()
                .map(|command| command.input.topic.clone())
                .collect();
            let inputs: Vec<EnqueueInput> = enqueues
                .iter_mut()
                .map(|command| {
                    std::mem::replace(
                        &mut command.input,
                        EnqueueInput {
                            id: String::new(),
                            idempotency_key: None,
                            topic: String::new(),
                            payload: Vec::new(),
                            attributes: HashMap::new(),
                        },
                    )
                })
                .collect();
            let mut retry_wait = Duration::from_millis(10);
            loop {
                match db.enqueue_batch(inputs.clone()).await {
                    Ok(results) => {
                        for (topic, result) in topics.iter().zip(results) {
                            if !result.1 {
                                notifications.notify(topic);
                            }
                        }
                        break;
                    }
                    Err(error) => {
                        tracing::error!(
                            %error,
                            count = inputs.len(),
                            retry_ms = retry_wait.as_millis(),
                            "asynchronous queue persistence failed; retrying"
                        );
                        tokio::time::sleep(retry_wait).await;
                        retry_wait = retry_wait.saturating_mul(2).min(Duration::from_secs(1));
                    }
                }
            }
        }
        if !delivered.is_empty() {
            let updates = delivered
                .iter()
                .map(|command| (command.id.clone(), command.lease.clone()))
                .collect();
            match db.delivered_batch(updates).await {
                Ok(results) => {
                    for (command, result) in delivered.into_iter().zip(results) {
                        let _ = command.response.send(Ok(result));
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for command in delivered {
                        let _ = command.response.send(Err(error.clone()));
                    }
                }
            }
        }
        if let Some(response) = shutdown {
            let _ = response.send(());
            break;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{DeliveryMode, PersistenceMode, TopicConfig};
    use futures_util::future::join_all;
    use uuid::Uuid;

    #[tokio::test]
    async fn batches_enqueue_and_delivery_completion() {
        let dir = std::env::temp_dir().join(format!("cangling-writer-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::connect(&format!("sqlite:{}/queue.db", dir.display()))
            .await
            .unwrap();
        db.configure_topics(&[TopicConfig {
            topic: "jobs".into(),
            delivery: DeliveryMode::Single,
            persistence: PersistenceMode::Persistent,
        }])
        .await
        .unwrap();
        let (writer, task) = QueueWriter::start(db.clone(), 256, 64, Duration::from_millis(2))
            .await
            .unwrap();
        let mut notification = writer.subscribe("jobs");
        let sends = (0..64).map(|index| {
            let writer = writer.clone();
            async move {
                writer
                    .enqueue(
                        Some(&format!("key-{index}")),
                        "jobs",
                        b"payload",
                        HashMap::new(),
                    )
                    .await
                    .unwrap()
            }
        });
        let results = join_all(sends).await;
        assert_eq!(results.len(), 64);
        assert!(results.iter().all(|(_, duplicate)| !duplicate));
        tokio::time::timeout(Duration::from_millis(100), notification.changed())
            .await
            .expect("committed publish should wake subscribers")
            .unwrap();

        let snapshot = db.status_snapshot(None).await.unwrap();
        let jobs = snapshot.iter().find(|topic| topic.name == "jobs").unwrap();
        assert_eq!(jobs.pending, 64);
        assert_eq!(jobs.processing, 0);

        let message = db
            .claim_next_for_topic("jobs", Duration::from_secs(10))
            .await
            .unwrap()
            .unwrap();
        assert!(writer.delivered(&message.id, &message.lease).await.unwrap());
        let snapshot = db.status_snapshot(None).await.unwrap();
        let jobs = snapshot.iter().find(|topic| topic.name == "jobs").unwrap();
        assert_eq!(jobs.pending, 63);
        assert_eq!(jobs.processing, 0);
        writer.shutdown().await;
        task.await.unwrap();
        let accepted: i64 =
            sqlx::query_scalar("SELECT accepted FROM topic_stats WHERE topic = 'jobs'")
                .fetch_one(&db.0)
                .await
                .unwrap();
        let delivered: i64 =
            sqlx::query_scalar("SELECT delivered FROM topic_stats WHERE topic = 'jobs'")
                .fetch_one(&db.0)
                .await
                .unwrap();
        assert_eq!(accepted, 64);
        assert_eq!(delivered, 1);
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn concurrent_idempotency_is_resolved_before_sqlite_commit() {
        let dir = std::env::temp_dir().join(format!("cangling-writer-id-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::connect(&format!("sqlite:{}/queue.db", dir.display()))
            .await
            .unwrap();
        let (writer, task) = QueueWriter::start(db.clone(), 64, 64, Duration::from_millis(20))
            .await
            .unwrap();
        let left = writer.enqueue(Some("same-key"), "jobs", b"one", HashMap::new());
        let right = writer.enqueue(Some("same-key"), "jobs", b"two", HashMap::new());
        let (left, right) = tokio::join!(left, right);
        let left = left.unwrap();
        let right = right.unwrap();
        assert_eq!(left.0, right.0);
        assert_ne!(left.1, right.1);

        writer.shutdown().await;
        task.await.unwrap();
        let (restarted, restarted_task) =
            QueueWriter::start(db.clone(), 64, 64, Duration::from_millis(20))
                .await
                .unwrap();
        let retry = restarted
            .enqueue(Some("same-key"), "jobs", b"retry", HashMap::new())
            .await
            .unwrap();
        assert_eq!(retry.0, left.0);
        assert!(retry.1);
        restarted.shutdown().await;
        restarted_task.await.unwrap();
        let snapshot = db.status_snapshot(None).await.unwrap();
        let jobs = snapshot.iter().find(|topic| topic.name == "jobs").unwrap();
        assert_eq!(jobs.pending, 1);
        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}
