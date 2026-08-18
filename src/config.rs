use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(about = "Durable gRPC message dispatcher")]
pub struct Config {
    /// gRPC listen port (`0.0.0.0:<port>`).
    #[arg(long, env = "CL_BROKER_PORT", default_value_t = 7500)]
    pub port: u16,

    /// HTTP status listen port (`0.0.0.0:<port>`).
    #[arg(long, env = "CL_BROKER_WEBPORT", default_value_t = 7501)]
    pub web_port: u16,

    /// Optional HTTP path prefix when a reverse proxy forwards `/msg/...`
    /// without stripping it. Root routes stay registered. Example: `/msg`.
    #[arg(long, env = "CL_BROKER_WEB_BASE", default_value = "")]
    pub web_base: String,

    /// Accept MQTT 3.1.1 clients over TCP and WebSocket.
    #[arg(long, env = "CL_BROKER_MQTT_ENABLED", default_value_t = true, action = clap::ArgAction::Set)]
    pub mqtt_enabled: bool,

    /// MQTT TCP port (`0.0.0.0:<port>`). 0 disables the TCP listener.
    /// Default is 7883 so an unprivileged process can bind (map 1883:7883 in Docker).
    #[arg(long, env = "CL_BROKER_MQTT_PORT", default_value_t = 7883)]
    pub mqtt_port: u16,

    /// MQTT WebSocket port (`0.0.0.0:<port>`). 0 attaches `GET /mqtt` to the status server.
    #[arg(long, env = "CL_BROKER_MQTT_WSPORT", default_value_t = 8083)]
    pub mqtt_ws_port: u16,

    /// Shared secret. When set, every gRPC call must send it
    /// (`authorization: Bearer <token>` or `x-auth-token`). Empty disables auth.
    #[arg(long, env = "CL_BROKER_AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// Data directory. SQLite is `<dir>/queue.db`, logs are `<dir>/logs`.
    #[arg(long, env = "CL_BROKER_DATA")]
    pub data_dir: Option<PathBuf>,

    /// Optional HTTP fallback used only when a topic has no live gRPC Subscribe stream.
    #[arg(long, env = "DOWNSTREAM_URL")]
    pub downstream_url: Option<String>,

    #[arg(long, env = "WORKER_POLL_MS", default_value_t = 500)]
    pub worker_poll_ms: u64,

    #[arg(long, env = "MAX_DELIVERY_ATTEMPTS", default_value_t = 10)]
    pub max_delivery_attempts: i64,

    /// How many days to keep a message in SQLite. Older rows are deleted. 0 disables purge.
    #[arg(long, env = "MESSAGE_RETENTION_DAYS", default_value_t = 10)]
    pub message_retention_days: i64,

    /// Delete unconfigured ephemeral topic rows that have not received a
    /// message for this many hours. 0 disables. Explicit ConfigureTopics stay.
    #[arg(long, env = "CL_BROKER_EPHEMERAL_IDLE_HOURS", default_value_t = 1)]
    pub ephemeral_idle_hours: u64,

    /// How often to run idle-topic purge, in hours. Default 1. 0 runs it on every sweep.
    #[arg(long, env = "CL_BROKER_PURGE_INTERVAL_HOURS", default_value_t = 1)]
    pub purge_interval_hours: u64,

    /// How long a worker may hold a claimed message before it is retried.
    #[arg(long, env = "ACK_TIMEOUT_SECS", default_value_t = 30)]
    pub ack_timeout_secs: u64,

    /// Drop a registered consumer if it does not Register again within this many seconds. 0 keeps it until Unregister.
    #[arg(long, env = "CONSUMER_TTL_SECS", default_value_t = 60)]
    pub consumer_ttl_secs: u64,

    /// Rotate the log file after this many bytes. Default 100 MiB.
    #[arg(long, env = "LOG_MAX_BYTES", default_value_t = 100 * 1024 * 1024)]
    pub log_max_bytes: usize,

    /// How many log files to keep, including the current one.
    #[arg(long, env = "LOG_KEEP_FILES", default_value_t = 3)]
    pub log_keep_files: usize,
}

impl Config {
    pub fn grpc_listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.port))
    }

    pub fn status_listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.web_port))
    }

    pub fn mqtt_listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.mqtt_port))
    }

    pub fn mqtt_ws_listen_addr(&self) -> SocketAddr {
        SocketAddr::from(([0, 0, 0, 0], self.mqtt_ws_port))
    }

    pub fn database_url(&self) -> String {
        match self.data_dir.as_ref() {
            Some(dir) => sqlite_url(&dir.to_string_lossy()),
            None => "sqlite:./queue.db".into(),
        }
    }

    pub fn log_dir(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("logs"))
    }

    /// `/msg` from `msg`, `/msg/`, or `/msg`. Empty or `/` is `None`.
    pub fn status_web_base(&self) -> Option<String> {
        normalize_web_base(&self.web_base)
    }
}

pub fn normalize_web_base(raw: &str) -> Option<String> {
    let trimmed = raw.trim().trim_matches('/');
    if trimmed.is_empty() {
        None
    } else {
        Some(format!("/{trimmed}"))
    }
}

fn sqlite_url(dir: &str) -> String {
    let dir = dir.trim().trim_end_matches('/');
    if dir.starts_with('/') {
        format!("sqlite://{dir}/queue.db")
    } else {
        format!("sqlite:{dir}/queue.db")
    }
}

#[cfg(test)]
impl Config {
    pub fn test_default() -> Self {
        Self {
            port: 7500,
            web_port: 7501,
            mqtt_enabled: true,
            mqtt_port: 7883,
            mqtt_ws_port: 8083,
            web_base: String::new(),
            auth_token: None,
            data_dir: None,
            downstream_url: None,
            worker_poll_ms: 20,
            max_delivery_attempts: 10,
            message_retention_days: 0,
            ephemeral_idle_hours: 0,
            purge_interval_hours: 1,
            ack_timeout_secs: 3,
            consumer_ttl_secs: 0,
            log_max_bytes: 1024,
            log_keep_files: 1,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_web_base;

    #[test]
    fn normalize_web_base_empty_or_root() {
        assert_eq!(normalize_web_base(""), None);
        assert_eq!(normalize_web_base("   "), None);
        assert_eq!(normalize_web_base("/"), None);
        assert_eq!(normalize_web_base("//"), None);
    }

    #[test]
    fn normalize_web_base_prefix() {
        assert_eq!(normalize_web_base("msg").as_deref(), Some("/msg"));
        assert_eq!(normalize_web_base("/msg").as_deref(), Some("/msg"));
        assert_eq!(normalize_web_base("/msg/").as_deref(), Some("/msg"));
        assert_eq!(normalize_web_base("  /msg/  ").as_deref(), Some("/msg"));
    }
}
