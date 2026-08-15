use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{fmt, layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};

use crate::config::Config;

pub fn init(config: &Config) -> anyhow::Result<Option<WorkerGuard>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout = fmt::layer().with_writer(std::io::stdout);

    let Some(dir) = config.log_dir() else {
        tracing_subscriber::registry().with(filter).with(stdout).init();
        return Ok(None);
    };

    std::fs::create_dir_all(&dir)?;
    let path = dir.join("cangling-message.log");
    let archives = config.log_keep_files.saturating_sub(1).max(1);
    let rotate = FileRotate::new(
        path,
        AppendCount::new(archives),
        ContentLimit::Bytes(config.log_max_bytes),
        Compression::None,
        None,
    );
    let (writer, guard) = tracing_appender::non_blocking(rotate);
    let file = fmt::layer().with_ansi(false).with_writer(writer);
    tracing_subscriber::registry()
        .with(filter)
        .with(stdout)
        .with(file)
        .init();
    Ok(Some(guard))
}
