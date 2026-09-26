use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};

use tokio::sync::{mpsc, oneshot, watch};

use crate::db::{message_id_shard, topic_shard, Database, EnqueueInput};

type EnqueueResult = Result<(String, bool), String>;
type DeliveredResult = Result<bool, String>;

struct EnqueueCommand {
    input: EnqueueInput,
    response: oneshot::Sender<EnqueueResult>,
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

/// Serializes SQLite queue writes and groups publishes into short transactions.
/// A caller receives success only after the transaction containing its message commits.
#[derive(Clone)]
pub struct QueueWriter {
    tx: Arc<Vec<mpsc::Sender<WriteCommand>>>,
    notifications: QueueNotifications,
    stats_shutdown: watch::Sender<bool>,
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
    pub fn start(
        db: Database,
        queue_size: usize,
        batch_size: usize,
        batch_wait: Duration,
    ) -> (Self, tokio::task::JoinHandle<()>) {
        let notifications = QueueNotifications::default();
        let (stats_shutdown, mut stats_shutdown_rx) = watch::channel(false);
        let mut senders = Vec::with_capacity(16);
        let mut workers = Vec::with_capacity(16);
        let per_shard_queue = queue_size.div_ceil(16).max(1);
        for _ in 0..16 {
            let (tx, rx) = mpsc::channel(per_shard_queue);
            senders.push(tx);
            workers.push(tokio::spawn(run_writer(
                db.clone(),
                rx,
                batch_size.max(1),
                batch_wait,
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
        (
            Self {
                tx: Arc::new(senders),
                notifications,
                stats_shutdown,
            },
            handle,
        )
    }

    pub async fn enqueue(
        &self,
        idempotency_key: Option<&str>,
        topic: &str,
        payload: &[u8],
        attributes: HashMap<String, String>,
    ) -> anyhow::Result<(String, bool)> {
        let (response, receive) = oneshot::channel();
        self.tx[topic_shard(topic)]
            .send(WriteCommand::Enqueue(EnqueueCommand {
                input: EnqueueInput {
                    idempotency_key: idempotency_key
                        .filter(|value| !value.is_empty())
                        .map(ToOwned::to_owned),
                    topic: topic.to_string(),
                    payload: payload.to_vec(),
                    attributes,
                },
                response,
            }))
            .await
            .map_err(|_| anyhow::anyhow!("queue writer stopped"))?;
        let result = receive
            .await
            .map_err(|_| anyhow::anyhow!("queue writer stopped"))?
            .map_err(anyhow::Error::msg)?;
        if !result.1 {
            self.notifications.notify(topic);
        }
        Ok(result)
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
            let inputs = enqueues
                .iter_mut()
                .map(|command| {
                    std::mem::replace(
                        &mut command.input,
                        EnqueueInput {
                            idempotency_key: None,
                            topic: String::new(),
                            payload: Vec::new(),
                            attributes: HashMap::new(),
                        },
                    )
                })
                .collect();
            match db.enqueue_batch(inputs).await {
                Ok(results) => {
                    for (command, result) in enqueues.into_iter().zip(results) {
                        let _ = command.response.send(Ok(result));
                    }
                }
                Err(error) => {
                    let error = error.to_string();
                    for command in enqueues {
                        let _ = command.response.send(Err(error.clone()));
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
        let (writer, task) = QueueWriter::start(db.clone(), 256, 64, Duration::from_millis(2));
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
}
