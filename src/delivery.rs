use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
    time::Duration,
};

use chrono::Utc;
use tokio::sync::{mpsc, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    config::Config,
    db::Database,
    model::{DeliveryMode, PersistenceMode},
    proto::SatwayMessage,
    subscribers::{InflightAcks, StreamSender, SubscriptionGuard, TopicSubscribers},
    writer::QueueWriter,
};

pub const PROTOCOL_GRPC: &str = "grpc";

fn payload_preview(payload: &[u8]) -> String {
    const MAX: usize = 4096;
    let text = String::from_utf8_lossy(payload);
    if text.len() <= MAX {
        return text.into_owned();
    }
    let mut end = MAX;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({} bytes)", &text[..end], payload.len())
}

pub enum Ingested {
    Queued { message_id: String, duplicate: bool },
    Dropped { message_id: String },
}

#[allow(clippy::too_many_arguments)]
pub async fn ingest(
    db: &Database,
    writer: &QueueWriter,
    subscribers: &TopicSubscribers,
    topic: &str,
    payload: &[u8],
    attributes: HashMap<String, String>,
    idempotency_key: Option<&str>,
    log_messages: bool,
) -> anyhow::Result<Ingested> {
    if log_messages {
        info!(topic, payload = %payload_preview(payload), bytes = payload.len(), "received message");
    }
    let settings = db
        .topic_config(topic)
        .await
        .unwrap_or_else(|_| crate::model::TopicConfig::implicit(topic));
    if settings.persistence == PersistenceMode::Ephemeral {
        return fanout_ephemeral(
            db,
            subscribers,
            topic,
            payload,
            attributes,
            settings.delivery,
        )
        .await;
    }
    let (message_id, duplicate) = writer
        .enqueue(idempotency_key, topic, payload, attributes)
        .await?;
    Ok(Ingested::Queued {
        message_id,
        duplicate,
    })
}

async fn fanout_ephemeral(
    db: &Database,
    subscribers: &TopicSubscribers,
    topic: &str,
    payload: &[u8],
    attributes: HashMap<String, String>,
    delivery: DeliveryMode,
) -> anyhow::Result<Ingested> {
    let mut senders = subscribers.matching_senders(topic);
    if delivery == DeliveryMode::Single {
        senders.truncate(1);
    }
    if senders.is_empty() {
        let message_id = db.accept_dropped(topic).await?;
        return Ok(Ingested::Dropped { message_id });
    }
    let message_id = Uuid::new_v4().to_string();
    let outgoing = SatwayMessage {
        message_id: message_id.clone(),
        topic: topic.to_string(),
        payload: payload.to_vec(),
        attributes,
        created_at: Utc::now().to_rfc3339(),
        lease: String::new(),
    };
    let mut sent = 0usize;
    for (_, sender) in senders {
        if let Ok(Ok(())) = tokio::time::timeout(
            Duration::from_millis(200),
            sender.send(Ok(outgoing.clone())),
        )
        .await
        {
            sent += 1;
        }
    }
    if sent == 0 {
        let message_id = db.accept_dropped(topic).await?;
        return Ok(Ingested::Dropped { message_id });
    }
    db.record_live_fanout(topic).await?;
    Ok(Ingested::Queued {
        message_id,
        duplicate: false,
    })
}

pub struct SubscribeLoop {
    pub db: Database,
    pub writer: QueueWriter,
    pub config: Arc<Config>,
    pub subscribers: TopicSubscribers,
    pub inflight: InflightAcks,
    pub shutdown: CancellationToken,
    pub topic: String,
    pub session: String,
    pub consumer_id: String,
    pub tx: StreamSender,
    pub peer: String,
    pub protocol: &'static str,
    pub version: String,
    pub host: String,
}

pub fn spawn_subscribe_loop(loop_args: SubscribeLoop) {
    tokio::spawn(run_subscribe_loop(loop_args));
}

pub async fn run_subscribe_loop(args: SubscribeLoop) {
    let SubscribeLoop {
        db,
        writer,
        config,
        subscribers,
        inflight,
        shutdown,
        topic,
        session,
        consumer_id,
        tx,
        peer,
        protocol,
        version,
        host,
    } = args;
    let guard = SubscriptionGuard::new(
        subscribers.clone(),
        topic.clone(),
        session.clone(),
        tx.clone(),
        peer,
        protocol,
        version,
        host,
    );
    info!(
        topic = %topic,
        consumer_id = %consumer_id,
        protocol,
        "subscriber connected"
    );
    let visibility = Duration::from_secs(config.ack_timeout_secs.max(1));
    // Consumer liveness does not need to be persisted on every queue poll. With many topics,
    // doing so turns idle subscribers into a continuous stream of SQLite writers.
    let heartbeat_every = Duration::from_secs(if config.consumer_ttl_secs == 0 {
        30
    } else {
        config.consumer_ttl_secs.saturating_div(3).clamp(1, 30)
    });
    let mut last_heartbeat = tokio::time::Instant::now();
    let mut notification = writer.subscribe(&topic);
    let permits = Arc::new(Semaphore::new(config.consumer_prefetch.max(1)));
    let mut deliveries = tokio::task::JoinSet::new();
    let mut ready = VecDeque::new();
    loop {
        while deliveries.try_join_next().is_some() {}
        if shutdown.is_cancelled() || tx.is_closed() {
            break;
        }
        if ready.is_empty() {
            let permit = tokio::select! {
                _ = shutdown.cancelled() => break,
                permit = permits.clone().acquire_owned() => match permit {
                    Ok(permit) => permit,
                    Err(_) => break,
                }
            };
            let mut reserved = vec![permit];
            while reserved.len() < config.claim_batch_size.clamp(1, 128) {
                match permits.clone().try_acquire_owned() {
                    Ok(permit) => reserved.push(permit),
                    Err(_) => break,
                }
            }
            let claimed_at = tokio::time::Instant::now();
            let claimed = match db
                .claim_batch_for_filter(&topic, visibility, reserved.len())
                .await
            {
                Ok(messages) => messages,
                Err(error) => {
                    error!(%error, topic = %topic, "unable to claim queue batch for subscriber");
                    if !sleep_or_shutdown(&shutdown, config.worker_poll_ms).await {
                        break;
                    }
                    continue;
                }
            };
            for (message, permit) in claimed.into_iter().zip(reserved) {
                ready.push_back((message, permit, claimed_at));
            }
            if ready.is_empty() {
                if !consumer_id.is_empty() && last_heartbeat.elapsed() >= heartbeat_every {
                    let _ = db.touch_consumer(&consumer_id).await;
                    last_heartbeat = tokio::time::Instant::now();
                }
                tokio::select! {
                    _ = shutdown.cancelled() => break,
                    changed = notification.changed() => {
                        if changed.is_err() { break; }
                    }
                    _ = tokio::time::sleep(Duration::from_secs(30)) => {}
                }
                continue;
            }
        }
        let (message, permit, claimed_at) = ready.pop_front().expect("nonempty claim batch");
        // A blocked stream can outlive the remaining batch's leases. Return buffered
        // messages before sending rather than dispatching an already expired lease.
        if claimed_at.elapsed() >= visibility / 2 {
            if db
                .release(&message.id, &message.lease)
                .await
                .unwrap_or(false)
            {
                writer.notify_ready(&message.topic);
            }
            continue;
        }
        let send_started = std::time::Instant::now();
        if let Ok(created) = chrono::DateTime::parse_from_rfc3339(&message.created_at) {
            crate::metrics::QUEUE_WAIT.record(
                (Utc::now() - created.with_timezone(&Utc))
                    .to_std()
                    .unwrap_or_default(),
            );
            tracing::debug!(topic = %message.topic,
                queue_wait_ms = (Utc::now() - created.with_timezone(&Utc)).num_milliseconds().max(0),
                "queue message dispatch");
        }
        let settings = db.topic_config(&message.topic).await.ok();
        let broadcast = settings
            .as_ref()
            .is_some_and(|config| config.delivery == DeliveryMode::Broadcast);
        let ephemeral = settings
            .as_ref()
            .is_some_and(|config| config.persistence == PersistenceMode::Ephemeral);
        let targets = if broadcast {
            let mut senders = subscribers.matching_senders(&message.topic);
            if senders.is_empty() {
                senders.push((session.clone(), tx.clone()));
            }
            senders
        } else {
            vec![(session.clone(), tx.clone())]
        };
        let mut waits = Vec::new();
        let mut leases = Vec::new();
        for (_id, sender) in targets {
            let lease = Uuid::new_v4().to_string();
            let ack = inflight.register(message.id.clone(), lease.clone());
            let outgoing = SatwayMessage {
                message_id: message.id.clone(),
                topic: message.topic.clone(),
                payload: message.payload.clone(),
                attributes: attrs_to_map(&message.attributes),
                created_at: message.created_at.clone(),
                lease: lease.clone(),
            };
            match tokio::time::timeout_at(
                (tokio::time::Instant::now() + Duration::from_secs(2)).min(claimed_at + visibility),
                sender.send(Ok(outgoing)),
            )
            .await
            {
                Ok(Ok(())) => {
                    leases.push(lease);
                    waits.push(ack);
                }
                _ => {
                    inflight.cancel(&lease);
                }
            }
        }
        crate::metrics::SEND.record(send_started.elapsed());
        tracing::debug!(topic = %message.topic, elapsed_us = send_started.elapsed().as_micros() as u64,
            targets = waits.len(), "queue message send");
        if ephemeral && !waits.is_empty() {
            for lease in &leases {
                inflight.cancel(lease);
            }
            if let Err(error) = db.delivered(&message.id, &message.lease).await {
                error!(%error, "could not mark delivery");
            }
            drop(permit);
            continue;
        }
        if waits.is_empty() {
            if ephemeral {
                let _ = db
                    .drop_claimed(
                        &message.id,
                        &message.lease,
                        "no live subscriber accepted the message",
                    )
                    .await;
            } else {
                let _ = db
                    .failed(
                        &message.id,
                        &message.lease,
                        "no live subscriber accepted the message",
                        config.max_delivery_attempts,
                    )
                    .await;
            }
            if tx.is_closed() {
                break;
            }
            drop(permit);
            continue;
        }
        let delivery_db = db.clone();
        let delivery_writer = writer.clone();
        let delivery_inflight = inflight.clone();
        let delivery_shutdown = shutdown.clone();
        let max_attempts = config.max_delivery_attempts;
        let ack_started = std::time::Instant::now();
        deliveries.spawn(async move {
            let _permit = permit;
            let mut all_ok = true;
            let mut last_error = "ack timeout or subscriber gone".to_string();
            for ack in waits {
                tokio::select! {
                    _ = delivery_shutdown.cancelled() => {
                        all_ok = false;
                        last_error = "broker shutting down".into();
                    }
                    timed = tokio::time::timeout_at(claimed_at + visibility, ack) => {
                        match timed {
                            Ok(Ok(decision)) if decision.success => {}
                            Ok(Ok(decision)) => {
                                all_ok = false;
                                last_error = if decision.error.is_empty() {
                                    "nack".into()
                                } else {
                                    decision.error
                                };
                            }
                            Ok(Err(_)) | Err(_) => all_ok = false,
                        }
                    }
                }
                if delivery_shutdown.is_cancelled() {
                    break;
                }
            }
            crate::metrics::ACK_WAIT.record(ack_started.elapsed());
            for lease in &leases {
                delivery_inflight.cancel(lease);
            }
            if delivery_shutdown.is_cancelled() {
                return;
            }
            if all_ok {
                if let Err(error) = delivery_writer.delivered(&message.id, &message.lease).await {
                    error!(%error, "could not mark delivery");
                }
            } else {
                let _ = delivery_db
                    .failed(&message.id, &message.lease, &last_error, max_attempts)
                    .await;
            }
        });
    }
    for (message, _permit, _) in ready {
        if db
            .release(&message.id, &message.lease)
            .await
            .unwrap_or(false)
        {
            writer.notify_ready(&message.topic);
        }
    }
    while deliveries.join_next().await.is_some() {}
    drop(guard);
    if !crate::topic::is_wildcard_filter(&topic)
        && !subscribers.covers(&topic)
        && db
            .topic_persistence(&topic)
            .await
            .ok()
            .is_some_and(|mode| mode == PersistenceMode::Ephemeral)
    {
        match db.drop_pending(&topic).await {
            Ok(0) => {}
            Ok(dropped) => info!(
                topic = %topic,
                dropped,
                "dropped ephemeral messages after last subscriber left"
            ),
            Err(error) => error!(%error, topic = %topic, "could not drop ephemeral messages"),
        }
    }
    info!(topic = %topic, protocol, "subscriber disconnected");
}

pub async fn sleep_or_shutdown(shutdown: &CancellationToken, poll_ms: u64) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = tokio::time::sleep(Duration::from_millis(poll_ms)) => true,
    }
}

pub fn attrs_to_map(value: &serde_json::Value) -> HashMap<String, String> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter_map(|(key, item)| item.as_str().map(|text| (key.clone(), text.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

/// Keep the outgoing channel open for the life of an MQTT connection.
pub fn outgoing_channel() -> (
    StreamSender,
    tokio::sync::mpsc::Receiver<Result<SatwayMessage, tonic::Status>>,
) {
    mpsc::channel(1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use std::time::Duration;

    #[test]
    fn payload_preview_keeps_short_text() {
        assert_eq!(payload_preview(b"hello"), "hello");
    }

    #[test]
    fn payload_preview_truncates_long_text() {
        let body = "x".repeat(5000);
        let preview = payload_preview(body.as_bytes());
        assert!(preview.ends_with("… (5000 bytes)"));
        assert!(preview.starts_with("xxxx"));
        assert!(preview.len() < body.len());
    }

    #[tokio::test]
    async fn closed_stream_returns_unsent_batch_to_queue() {
        let dir = tempfile::tempdir().unwrap();
        let db = Database::connect(&format!("sqlite:{}/queue.db", dir.path().display()))
            .await
            .unwrap();
        db.configure_topics(&[crate::model::TopicConfig {
            topic: "jobs".into(),
            delivery: DeliveryMode::Single,
            persistence: PersistenceMode::Persistent,
        }])
        .await
        .unwrap();
        for _ in 0..4 {
            db.enqueue(None, "jobs", b"payload", HashMap::new())
                .await
                .unwrap();
        }
        let (writer, writer_task) =
            QueueWriter::start(db.clone(), 64, 32, Duration::from_millis(1))
                .await
                .unwrap();
        let mut config = Config::test_default();
        config.consumer_prefetch = 4;
        config.claim_batch_size = 4;
        config.ack_timeout_secs = 30;
        let shutdown = CancellationToken::new();
        let (tx, mut rx) = mpsc::channel(1);
        let task = tokio::spawn(run_subscribe_loop(SubscribeLoop {
            db: db.clone(),
            writer: writer.clone(),
            config: Arc::new(config),
            subscribers: TopicSubscribers::default(),
            inflight: InflightAcks::default(),
            shutdown: shutdown.clone(),
            topic: "jobs".into(),
            session: "test".into(),
            consumer_id: String::new(),
            tx,
            peer: String::new(),
            protocol: PROTOCOL_GRPC,
            version: String::new(),
            host: String::new(),
        }));
        tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        rx.close();
        shutdown.cancel();
        tokio::time::timeout(Duration::from_secs(5), task)
            .await
            .unwrap()
            .unwrap();
        let snapshot = db.status_snapshot(None).await.unwrap();
        let jobs = snapshot.iter().find(|t| t.name == "jobs").unwrap();
        assert!(
            jobs.pending >= 1,
            "unsent buffered leases must be returned on disconnect"
        );
        assert_eq!(jobs.pending + jobs.processing, 4);
        writer.shutdown().await;
        writer_task.await.unwrap();
        db.close().await;
    }

    #[tokio::test]
    async fn ephemeral_hash_fanout_does_not_queue() {
        let dir = std::env::temp_dir().join(format!("cangling-fanout-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::connect(&format!("sqlite:{}/queue.db", dir.display()))
            .await
            .unwrap();
        let subscribers = TopicSubscribers::default();
        let (tx, mut rx) = outgoing_channel();
        subscribers.add(
            "/ibuser/1/#",
            "mqtt:web-1",
            tx,
            "127.0.0.1:1",
            "mqtt-ws",
            "3.1.1",
            "",
        );

        let (writer, _writer_task) =
            QueueWriter::start(db.clone(), 256, 32, Duration::from_millis(1))
                .await
                .unwrap();
        let ingested = ingest(
            &db,
            &writer,
            &subscribers,
            "/ibuser/1/dRueErAe",
            b"hello",
            HashMap::new(),
            None,
            false,
        )
        .await
        .unwrap();
        assert!(matches!(
            ingested,
            Ingested::Queued {
                duplicate: false,
                ..
            }
        ));

        let message = tokio::time::timeout(Duration::from_secs(1), rx.recv())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(message.topic, "/ibuser/1/dRueErAe");
        assert_eq!(message.payload, b"hello");

        let leftover = db
            .claim_next_for_topic("/ibuser/1/dRueErAe", Duration::from_secs(1))
            .await
            .unwrap();
        assert!(leftover.is_none(), "ephemeral live fanout must not persist");

        db.close().await;
        let _ = std::fs::remove_dir_all(dir);
    }
}
