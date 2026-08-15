use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::proto::SatwayMessage;

pub type StreamSender = mpsc::Sender<Result<SatwayMessage, Status>>;

#[derive(Clone, Default)]
pub struct TopicSubscribers(Arc<Mutex<HashMap<String, HashMap<String, StreamSender>>>>);

impl TopicSubscribers {
    pub fn add(&self, topic: &str, session: &str, tx: StreamSender) {
        self.0
            .lock()
            .expect("subscriber map")
            .entry(topic.to_string())
            .or_default()
            .insert(session.to_string(), tx);
    }

    pub fn remove(&self, topic: &str, session: &str) {
        let mut topics = self.0.lock().expect("subscriber map");
        if let Some(sessions) = topics.get_mut(topic) {
            sessions.remove(session);
            if sessions.is_empty() {
                topics.remove(topic);
            }
        }
    }

    pub fn count(&self, topic: &str) -> usize {
        self.0
            .lock()
            .expect("subscriber map")
            .get(topic)
            .map(HashMap::len)
            .unwrap_or(0)
    }

    pub fn is_live(&self, topic: &str, consumer_id: &str) -> bool {
        self.0
            .lock()
            .expect("subscriber map")
            .get(topic)
            .is_some_and(|sessions| sessions.contains_key(consumer_id))
    }

    pub fn topics(&self) -> Vec<String> {
        self.0.lock().expect("subscriber map").keys().cloned().collect()
    }

    pub fn senders(&self, topic: &str) -> Vec<(String, StreamSender)> {
        self.0
            .lock()
            .expect("subscriber map")
            .get(topic)
            .map(|sessions| {
                sessions
                    .iter()
                    .map(|(id, tx)| (id.clone(), tx.clone()))
                    .collect()
            })
            .unwrap_or_default()
    }
}

pub struct AckDecision {
    pub success: bool,
    pub error: String,
}

struct PendingAck {
    message_id: String,
    tx: oneshot::Sender<AckDecision>,
}

#[derive(Clone, Default)]
pub struct InflightAcks(Arc<Mutex<HashMap<String, PendingAck>>>);

impl InflightAcks {
    pub fn register(&self, message_id: String, lease: String) -> oneshot::Receiver<AckDecision> {
        let (tx, rx) = oneshot::channel();
        self.0.lock().expect("inflight map").insert(
            lease,
            PendingAck {
                message_id,
                tx,
            },
        );
        rx
    }

    pub fn complete(&self, message_id: &str, lease: &str, success: bool, error: String) -> bool {
        let mut inflight = self.0.lock().expect("inflight map");
        match inflight.get(lease) {
            Some(pending) if pending.message_id == message_id => {
                let pending = inflight.remove(lease).expect("checked");
                pending.tx.send(AckDecision { success, error }).is_ok()
            }
            _ => false,
        }
    }

    pub fn cancel(&self, lease: &str) {
        self.0.lock().expect("inflight map").remove(lease);
    }
}

pub struct SubscriptionGuard {
    subscribers: TopicSubscribers,
    topic: String,
    session: String,
}

impl SubscriptionGuard {
    pub fn new(
        subscribers: TopicSubscribers,
        topic: String,
        session: String,
        tx: StreamSender,
    ) -> Self {
        subscribers.add(&topic, &session, tx);
        Self {
            subscribers,
            topic,
            session,
        }
    }
}

impl Drop for SubscriptionGuard {
    fn drop(&mut self) {
        self.subscribers.remove(&self.topic, &self.session);
    }
}
