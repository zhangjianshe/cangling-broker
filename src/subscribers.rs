use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use tokio::sync::{mpsc, oneshot};
use tonic::Status;

use crate::{proto::SatwayMessage, topic};

pub type StreamSender = mpsc::Sender<Result<SatwayMessage, Status>>;

#[derive(Clone)]
pub struct SessionInfo {
    pub id: String,
    pub topic: String,
    pub peer: String,
    pub connected_at: String,
    pub protocol: &'static str,
}

struct LiveEntry {
    info: SessionInfo,
    tx: StreamSender,
}

#[derive(Clone, Default)]
pub struct TopicSubscribers(Arc<Mutex<HashMap<String, HashMap<String, LiveEntry>>>>);

impl TopicSubscribers {
    pub fn add(
        &self,
        topic: &str,
        session: &str,
        tx: StreamSender,
        peer: &str,
        protocol: &'static str,
    ) {
        self.0
            .lock()
            .expect("subscriber map")
            .entry(topic.to_string())
            .or_default()
            .insert(
                session.to_string(),
                LiveEntry {
                    info: SessionInfo {
                        id: session.to_string(),
                        topic: topic.to_string(),
                        peer: peer.to_string(),
                        connected_at: chrono::Utc::now().to_rfc3339(),
                        protocol,
                    },
                    tx,
                },
            );
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

    #[cfg(test)]
    pub fn count(&self, topic: &str) -> usize {
        self.0
            .lock()
            .expect("subscriber map")
            .get(topic)
            .map(HashMap::len)
            .unwrap_or(0)
    }

    pub fn covers(&self, published_topic: &str) -> bool {
        self.0
            .lock()
            .expect("subscriber map")
            .iter()
            .any(|(filter, sessions)| {
                !sessions.is_empty() && topic::filter_matches(filter, published_topic)
            })
    }

    pub fn matching_count(&self, published_topic: &str) -> usize {
        self.matching_senders(published_topic).len()
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

    pub fn sessions(&self) -> Vec<SessionInfo> {
        let mut sessions: Vec<SessionInfo> = self
            .0
            .lock()
            .expect("subscriber map")
            .values()
            .flat_map(|topic| topic.values().map(|entry| entry.info.clone()))
            .collect();
        sessions.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.id.cmp(&right.id))
        });
        sessions
    }

    /// Unique sessions whose filter matches the published topic (exact, `+`, or `#`).
    pub fn matching_senders(&self, published_topic: &str) -> Vec<(String, StreamSender)> {
        let topics = self.0.lock().expect("subscriber map");
        let mut seen = HashMap::new();
        for (filter, sessions) in topics.iter() {
            if !topic::filter_matches(filter, published_topic) {
                continue;
            }
            for (id, entry) in sessions {
                seen.entry(id.clone())
                    .or_insert_with(|| entry.tx.clone());
            }
        }
        seen.into_iter().collect()
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
        peer: String,
        protocol: &'static str,
    ) -> Self {
        subscribers.add(&topic, &session, tx, &peer, protocol);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracks_live_session_metadata() {
        let subscribers = TopicSubscribers::default();
        let (tx, _rx) = mpsc::channel(1);
        subscribers.add("jobs", "c1", tx, "127.0.0.1:4321", "grpc");
        assert_eq!(subscribers.count("jobs"), 1);
        assert!(subscribers.is_live("jobs", "c1"));
        let sessions = subscribers.sessions();
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].id, "c1");
        assert_eq!(sessions[0].topic, "jobs");
        assert_eq!(sessions[0].peer, "127.0.0.1:4321");
        assert_eq!(sessions[0].protocol, "grpc");
        assert!(!sessions[0].connected_at.is_empty());
        subscribers.remove("jobs", "c1");
        assert!(subscribers.sessions().is_empty());
    }

    #[test]
    fn hash_filter_covers_child_topics() {
        let subscribers = TopicSubscribers::default();
        let (tx, _rx) = mpsc::channel(1);
        subscribers.add("sensor/#", "mqtt:c1", tx, "127.0.0.1:1", "mqtt");
        assert!(subscribers.covers("sensor"));
        assert!(subscribers.covers("sensor/temp"));
        assert!(subscribers.covers("sensor/a/b"));
        assert!(!subscribers.covers("other"));
        assert_eq!(subscribers.matching_count("sensor/temp"), 1);
        assert_eq!(subscribers.count("sensor/temp"), 0);
        assert_eq!(subscribers.count("sensor/#"), 1);
    }
}
