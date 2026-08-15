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

    pub fn database_url(&self) -> String {
        match self.data_dir.as_ref() {
            Some(dir) => sqlite_url(&dir.to_string_lossy()),
            None => "sqlite:./queue.db".into(),
        }
    }

    pub fn log_dir(&self) -> Option<PathBuf> {
        self.data_dir.as_ref().map(|dir| dir.join("logs"))
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
