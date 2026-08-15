use std::{net::SocketAddr, path::PathBuf};

use clap::Parser;

#[derive(Debug, Clone, Parser)]
#[command(about = "Durable gRPC message dispatcher")]
pub struct Config {
    /// Address for the gRPC intake service.
    #[arg(long, env = "GRPC_LISTEN_ADDR", default_value = "0.0.0.0:7500")]
    pub grpc_listen_addr: SocketAddr,

    /// Address for the HTTP status endpoint (`GET /status`, `GET /health`).
    #[arg(long, env = "STATUS_LISTEN_ADDR", default_value = "0.0.0.0:7501")]
    pub status_listen_addr: SocketAddr,

    /// Shared secret. When set, every gRPC call must send it
    /// (`authorization: Bearer <token>` or `x-auth-token`). Empty disables auth.
    #[arg(long, env = "AUTH_TOKEN")]
    pub auth_token: Option<String>,

    /// SQLite database URL. Use sqlite:./queue.db to place it beside the binary.
    #[arg(long, env = "DATABASE_URL", default_value = "sqlite:./queue.db")]
    pub database_url: String,

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

    /// Directory for rotating log files. Unset logs only to stdout.
    #[arg(long, env = "LOG_DIR")]
    pub log_dir: Option<PathBuf>,

    /// Rotate the log file after this many bytes. Default 100 MiB.
    #[arg(long, env = "LOG_MAX_BYTES", default_value_t = 100 * 1024 * 1024)]
    pub log_max_bytes: usize,

    /// How many log files to keep, including the current one.
    #[arg(long, env = "LOG_KEEP_FILES", default_value_t = 3)]
    pub log_keep_files: usize,
}
