use file_rotate::{compression::Compression, suffix::AppendCount, ContentLimit, FileRotate};
use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::{
    fmt::{self, format::Writer, time::FormatTime},
    layer::SubscriberExt,
    util::SubscriberInitExt,
    EnvFilter,
};

use crate::config::Config;

const WALL_TIME: &str = "%Y-%m-%d %H:%M:%S";

struct CompactUtc;

impl FormatTime for CompactUtc {
    fn format_time(&self, w: &mut Writer<'_>) -> std::fmt::Result {
        write!(w, "{}", chrono::Utc::now().format(WALL_TIME))
    }
}

pub fn format_wall_time(value: &str) -> String {
    if let Ok(parsed) = chrono::DateTime::parse_from_rfc3339(value) {
        return parsed
            .with_timezone(&chrono::Utc)
            .format(WALL_TIME)
            .to_string();
    }
    if chrono::NaiveDateTime::parse_from_str(value, WALL_TIME).is_ok() {
        return value.to_string();
    }
    value.to_string()
}

pub fn init(config: &Config) -> anyhow::Result<Option<WorkerGuard>> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let stdout = fmt::layer().with_timer(CompactUtc).with_writer(std::io::stdout);

    let Some(dir) = config.log_dir() else {
        tracing_subscriber::registry().with(filter).with(stdout).init();
        return Ok(None);
    };

    std::fs::create_dir_all(&dir)?;
    let path = dir.join("cangling-broker.log");
    let archives = config.log_keep_files.saturating_sub(1).max(1);
    let rotate = FileRotate::new(
        path,
        AppendCount::new(archives),
        ContentLimit::Bytes(config.log_max_bytes),
        Compression::None,
        None,
    );
    let (writer, guard) = tracing_appender::non_blocking(rotate);
    let file = fmt::layer()
        .with_timer(CompactUtc)
        .with_ansi(false)
        .with_writer(writer);
    tracing_subscriber::registry()
        .with(filter)
        .with(stdout)
        .with(file)
        .init();
    Ok(Some(guard))
}

#[cfg(test)]
mod tests {
    use super::format_wall_time;

    #[test]
    fn formats_rfc3339_and_keeps_wall_time() {
        assert_eq!(
            format_wall_time("2026-08-15T09:37:27Z"),
            "2026-08-15 09:37:27"
        );
        assert_eq!(
            format_wall_time("2026-08-15 09:37:27"),
            "2026-08-15 09:37:27"
        );
    }
}
