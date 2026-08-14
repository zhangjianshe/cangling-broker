use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

use tokio::sync::oneshot;

#[derive(Clone, Default)]
pub struct TopicSubscribers(Arc<Mutex<HashMap<String, HashSet<String>>>>);

impl TopicSubscribers {
    pub fn add(&self, topic: &str, session: &str) {
        self.0
            .lock()
            .expect("subscriber map")
            .entry(topic.to_string())
            .or_default()
            .insert(session.to_string());
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
            .map(HashSet::len)
            .unwrap_or(0)
    }

    pub fn is_live(&self, topic: &str, consumer_id: &str) -> bool {
        self.0
            .lock()
            .expect("subscriber map")
            .get(topic)
            .is_some_and(|sessions| sessions.contains(consumer_id))
    }

    pub fn topics(&self) -> Vec<String> {
        self.0.lock().expect("subscriber map").keys().cloned().collect()
    }
}

pub struct AckDecision {
    pub success: bool,
    pub error: String,
}

struct PendingAck {
    lease: String,
    tx: oneshot::Sender<AckDecision>,
}

#[derive(Clone, Default)]
pub struct InflightAcks(Arc<Mutex<HashMap<String, PendingAck>>>);

impl InflightAcks {
    pub fn register(&self, message_id: String, lease: String) -> oneshot::Receiver<AckDecision> {
        let (tx, rx) = oneshot::channel();
        self.0
            .lock()
            .expect("inflight map")
            .insert(message_id, PendingAck { lease, tx });
        rx
    }

    pub fn complete(&self, message_id: &str, lease: &str, success: bool, error: String) -> bool {
        let mut inflight = self.0.lock().expect("inflight map");
        match inflight.get(message_id) {
            Some(pending) if pending.lease == lease => {
                let pending = inflight.remove(message_id).expect("checked");
                pending.tx.send(AckDecision { success, error }).is_ok()
            }
            _ => false,
        }
    }

    pub fn cancel(&self, message_id: &str) {
        self.0.lock().expect("inflight map").remove(message_id);
    }
}

pub struct SubscriptionGuard {
    subscribers: TopicSubscribers,
    topic: String,
    session: String,
}

impl SubscriptionGuard {
    pub fn new(subscribers: TopicSubscribers, topic: String, session: String) -> Self {
        subscribers.add(&topic, &session);
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
