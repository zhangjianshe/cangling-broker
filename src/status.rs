use std::time::Instant;

use axum::{extract::State, http::StatusCode, response::Html, routing::get, Json, Router};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::{config::Config, db::Database, model::TopicSnapshot, subscribers::TopicSubscribers};

#[derive(Clone)]
struct StatusState {
    db: Database,
    subscribers: TopicSubscribers,
    consumer_ttl_secs: u64,
    started: Instant,
}

#[derive(Debug, Serialize)]
struct Health {
    ok: bool,
}

#[derive(Debug, Serialize)]
struct BrokerStatus {
    uptime_secs: u64,
    topics: usize,
    consumers: usize,
    accepted: i64,
    duplicates: i64,
    pending: i64,
    processing: i64,
    delivered: i64,
    failed: i64,
    topics_detail: Vec<TopicSnapshot>,
}

fn consumer_cutoff(ttl_secs: u64) -> Option<String> {
    if ttl_secs == 0 {
        None
    } else {
        Some((chrono::Utc::now() - chrono::Duration::seconds(ttl_secs as i64)).to_rfc3339())
    }
}

pub async fn serve(
    listener: tokio::net::TcpListener,
    db: Database,
    config: std::sync::Arc<Config>,
    subscribers: TopicSubscribers,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let state = StatusState {
        db,
        subscribers,
        consumer_ttl_secs: config.consumer_ttl_secs,
        started: Instant::now(),
    };
    let app = Router::new()
        .route("/", get(page))
        .route("/health", get(health))
        .route("/status", get(status))
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn page() -> Html<&'static str> {
    Html(include_str!("status.html"))
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

async fn status(State(state): State<StatusState>) -> Result<Json<BrokerStatus>, StatusCode> {
    let cutoff = consumer_cutoff(state.consumer_ttl_secs);
    let mut topics_detail = state
        .db
        .status_snapshot(cutoff.as_deref())
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    for topic in &mut topics_detail {
        topic.streams = state.subscribers.count(&topic.name);
        for consumer in &mut topic.consumers {
            if state.subscribers.is_live(&topic.name, &consumer.id) {
                consumer.live = true;
            }
        }
    }
    let consumers = topics_detail.iter().map(|topic| topic.streams).sum();
    Ok(Json(BrokerStatus {
        uptime_secs: state.started.elapsed().as_secs(),
        topics: topics_detail.len(),
        consumers,
        accepted: topics_detail.iter().map(|topic| topic.accepted).sum(),
        duplicates: topics_detail.iter().map(|topic| topic.duplicates).sum(),
        pending: topics_detail.iter().map(|topic| topic.pending).sum(),
        processing: topics_detail.iter().map(|topic| topic.processing).sum(),
        delivered: topics_detail.iter().map(|topic| topic.delivered).sum(),
        failed: topics_detail.iter().map(|topic| topic.failed).sum(),
        topics_detail,
    }))
}
