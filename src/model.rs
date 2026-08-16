use std::collections::HashMap;

use serde::Serialize;

#[derive(Debug)]
pub struct ClaimedMessage {
    pub id: String,
    pub topic: String,
    pub payload: Vec<u8>,
    pub attributes: serde_json::Value,
    pub created_at: String,
    pub lease: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ConsumerSnapshot {
    pub id: String,
    pub name: String,
    pub last_seen_at: String,
    pub live: bool,
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub attributes: HashMap<String, String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeliveryMode {
    Single,
    Broadcast,
}

impl DeliveryMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Single => "single",
            Self::Broadcast => "broadcast",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "single" | "queue" | "competing" => Some(Self::Single),
            "broadcast" | "fanout" | "pubsub" => Some(Self::Broadcast),
            _ => None,
        }
    }

    pub fn from_stored(value: &str) -> Self {
        Self::parse(value).unwrap_or(Self::Broadcast)
    }
}

impl Default for DeliveryMode {
    fn default() -> Self {
        Self::Broadcast
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistenceMode {
    Persistent,
    Ephemeral,
}

impl PersistenceMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Persistent => "persistent",
            Self::Ephemeral => "ephemeral",
        }
    }

    pub fn parse(value: &str) -> Option<Self> {
        match value.trim().to_ascii_lowercase().as_str() {
            "" | "persistent" | "durable" | "store" => Some(Self::Persistent),
            "ephemeral" | "transient" | "none" | "drop" => Some(Self::Ephemeral),
            _ => None,
        }
    }

    pub fn from_stored(value: &str) -> Self {
        let trimmed = value.trim();
        if trimmed.is_empty() {
            return Self::Ephemeral;
        }
        Self::parse(trimmed).unwrap_or(Self::Ephemeral)
    }
}

impl Default for PersistenceMode {
    fn default() -> Self {
        Self::Ephemeral
    }
}

#[derive(Debug, Clone)]
pub struct TopicConfig {
    pub topic: String,
    pub delivery: DeliveryMode,
    pub persistence: PersistenceMode,
}

impl TopicConfig {
    pub fn implicit(topic: impl Into<String>) -> Self {
        Self {
            topic: topic.into(),
            delivery: DeliveryMode::Broadcast,
            persistence: PersistenceMode::Ephemeral,
        }
    }
}

#[derive(Debug, Serialize, Clone, Default)]
pub struct TopicSnapshot {
    pub name: String,
    pub accepted: i64,
    pub duplicates: i64,
    pub pending: i64,
    pub processing: i64,
    pub delivered: i64,
    pub failed: i64,
    #[serde(default)]
    pub streams: usize,
    #[serde(default)]
    pub delivery: String,
    #[serde(default)]
    pub persistence: String,
    #[serde(default)]
    pub dropped: i64,
    pub consumers: Vec<ConsumerSnapshot>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn persistence_parse_defaults_empty_to_persistent() {
        assert_eq!(PersistenceMode::parse(""), Some(PersistenceMode::Persistent));
        assert_eq!(
            PersistenceMode::parse("DURABLE"),
            Some(PersistenceMode::Persistent)
        );
        assert_eq!(
            PersistenceMode::parse("drop"),
            Some(PersistenceMode::Ephemeral)
        );
        assert_eq!(PersistenceMode::parse("maybe"), None);
    }

    #[test]
    fn implicit_topic_is_broadcast_ephemeral() {
        let config = TopicConfig::implicit("demo");
        assert_eq!(config.delivery, DeliveryMode::Broadcast);
        assert_eq!(config.persistence, PersistenceMode::Ephemeral);
        assert_eq!(DeliveryMode::from_stored(""), DeliveryMode::Broadcast);
        assert_eq!(PersistenceMode::from_stored(""), PersistenceMode::Ephemeral);
    }
}

impl ClaimedMessage {
    pub fn to_downstream(&self) -> DownstreamMessage {
        let (payload, payload_encoding) = match String::from_utf8(self.payload.clone()) {
            Ok(text) => (text, "utf-8"),
            Err(_) => (base64_encode(&self.payload), "base64"),
        };
        DownstreamMessage {
            id: self.id.clone(),
            topic: self.topic.clone(),
            payload,
            payload_encoding,
            attributes: self.attributes.clone(),
            created_at: self.created_at.clone(),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DownstreamMessage {
    pub id: String,
    pub topic: String,
    /// UTF-8 payloads are sent as text; arbitrary bytes are base64 encoded.
    pub payload: String,
    pub payload_encoding: &'static str,
    pub attributes: serde_json::Value,
    pub created_at: String,
}

fn base64_encode(bytes: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut output = String::with_capacity((bytes.len() + 2) / 3 * 4);
    for chunk in bytes.chunks(3) {
        let n = (u32::from(chunk[0]) << 16)
            | (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
            | u32::from(*chunk.get(2).unwrap_or(&0));
        output.push(TABLE[((n >> 18) & 63) as usize] as char);
        output.push(TABLE[((n >> 12) & 63) as usize] as char);
        output.push(if chunk.len() > 1 {
            TABLE[((n >> 6) & 63) as usize] as char
        } else {
            '='
        });
        output.push(if chunk.len() > 2 {
            TABLE[(n & 63) as usize] as char
        } else {
            '='
        });
    }
    output
}
