use std::{collections::HashMap, sync::Arc, time::Duration};

use chrono::Utc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};
use uuid::Uuid;

use crate::{
    config::Config,
    db::Database,
    model::{DeliveryMode, PersistenceMode},
    proto::SatwayMessage,
    subscribers::{InflightAcks, StreamSender, SubscriptionGuard, TopicSubscribers},
};

pub const PROTOCOL_GRPC: &str = "grpc";

pub enum Ingested {
    Queued { message_id: String, duplicate: bool },
    Dropped { message_id: String },
}

pub async fn ingest(
    db: &Database,
    subscribers: &TopicSubscribers,
    topic: &str,
    payload: &[u8],
    attributes: HashMap<String, String>,
    idempotency_key: Option<&str>,
) -> anyhow::Result<Ingested> {
    let settings = db
        .topic_config(topic)
        .await
        .unwrap_or_else(|_| crate::model::TopicConfig::implicit(topic));
    if settings.persistence == PersistenceMode::Ephemeral {
        return fanout_ephemeral(db, subscribers, topic, payload, attributes, settings.delivery)
            .await;
    }
    let (message_id, duplicate) = db
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
        match tokio::time::timeout(Duration::from_millis(200), sender.send(Ok(outgoing.clone())))
            .await
        {
            Ok(Ok(())) => sent += 1,
            _ => {}
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
}

pub fn spawn_subscribe_loop(loop_args: SubscribeLoop) {
    tokio::spawn(run_subscribe_loop(loop_args));
}

pub async fn run_subscribe_loop(args: SubscribeLoop) {
    let SubscribeLoop {
        db,
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
    } = args;
    let guard = SubscriptionGuard::new(
        subscribers.clone(),
        topic.clone(),
        session.clone(),
        tx.clone(),
        peer,
        protocol,
    );
    info!(
        topic = %topic,
        consumer_id = %consumer_id,
        protocol,
        "subscriber connected"
    );
    let visibility = Duration::from_secs(config.ack_timeout_secs.max(1));
    loop {
        if shutdown.is_cancelled() || tx.is_closed() {
            break;
        }
        let claimed = match db.claim_next_for_filter(&topic, visibility).await {
            Ok(claimed) => claimed,
            Err(error) => {
                error!(%error, topic = %topic, "unable to claim queue message for subscriber");
                if !sleep_or_shutdown(&shutdown, config.worker_poll_ms).await {
                    break;
                }
                continue;
            }
        };
        let Some(message) = claimed else {
            if !consumer_id.is_empty() {
                let _ = db.touch_consumer(&consumer_id).await;
            }
            if !sleep_or_shutdown(&shutdown, config.worker_poll_ms).await {
                break;
            }
            continue;
        };
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
            match tokio::time::timeout(Duration::from_secs(2), sender.send(Ok(outgoing))).await {
                Ok(Ok(())) => {
                    leases.push(lease);
                    waits.push(ack);
                }
                _ => {
                    inflight.cancel(&lease);
                }
            }
        }
        if ephemeral && !waits.is_empty() {
            for lease in &leases {
                inflight.cancel(lease);
            }
            if let Err(error) = db.delivered(&message.id, &message.lease).await {
                error!(%error, "could not mark delivery");
            }
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
            continue;
        }
        let mut all_ok = true;
        let mut last_error = "ack timeout or subscriber gone".to_string();
        for ack in waits {
            tokio::select! {
                _ = shutdown.cancelled() => {
                    all_ok = false;
                    last_error = "broker shutting down".into();
                }
                timed = tokio::time::timeout(visibility, ack) => {
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
                        Ok(Err(_)) | Err(_) => {
                            all_ok = false;
                        }
                    }
                }
            }
            if shutdown.is_cancelled() {
                break;
            }
        }
        for lease in &leases {
            inflight.cancel(lease);
        }
        if shutdown.is_cancelled() {
            break;
        }
        if all_ok {
            if let Err(error) = db.delivered(&message.id, &message.lease).await {
                error!(%error, "could not mark delivery");
            } else {
                info!(id = %message.id, topic = %topic, "message delivered");
            }
        } else {
            let _ = db
                .failed(
                    &message.id,
                    &message.lease,
                    &last_error,
                    config.max_delivery_attempts,
                )
                .await;
        }
    }
    drop(guard);
    if !crate::topic::is_wildcard_filter(&topic) && !subscribers.covers(&topic) {
        if db
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
pub fn outgoing_channel() -> (StreamSender, tokio::sync::mpsc::Receiver<Result<SatwayMessage, tonic::Status>>) {
    mpsc::channel(1024)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use std::time::Duration;

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
        );

        let ingested = ingest(
            &db,
            &subscribers,
            "/ibuser/1/dRueErAe",
            b"hello",
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
        assert!(matches!(ingested, Ingested::Queued { duplicate: false, .. }));

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
