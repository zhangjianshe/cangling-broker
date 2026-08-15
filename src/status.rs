use std::time::Instant;

use axum::{
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, Response},
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use tokio_util::sync::CancellationToken;

use crate::{
    auth,
    config::Config,
    db::Database,
    model::{DeliveryMode, PersistenceMode, TopicConfig, TopicSnapshot},
    subscribers::TopicSubscribers,
};

#[derive(Clone)]
struct StatusState {
    db: Database,
    subscribers: TopicSubscribers,
    consumer_ttl_secs: u64,
    started: Instant,
    auth_token: Option<String>,
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
    dropped: i64,
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
        auth_token: auth::normalize(config.auth_token.as_deref()),
    };
    let app = Router::new()
        .route("/", get(page))
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/topics", get(list_topics).post(configure_topics))
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .with_state(state);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            shutdown.cancelled().await;
        })
        .await?;
    Ok(())
}

async fn require_token(
    State(state): State<StatusState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if request.uri().path() == "/health" {
        return Ok(next.run(request).await);
    }
    let Some(expected) = state.auth_token.as_deref() else {
        return Ok(next.run(request).await);
    };
    let authorization = request
        .headers()
        .get(AUTHORIZATION)
        .and_then(|value| value.to_str().ok());
    if auth::tokens_match(expected, auth::http_token(authorization, request.uri().query()).as_deref())
    {
        return Ok(next.run(request).await);
    }
    Err(StatusCode::UNAUTHORIZED)
}

async fn page() -> Html<&'static str> {
    Html(include_str!("status.html"))
}

async fn health() -> Json<Health> {
    Json(Health { ok: true })
}

#[derive(Debug, Deserialize)]
struct ConfigureTopicsBody {
    topics: Vec<TopicConfigBody>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TopicConfigBody {
    topic: String,
    delivery: String,
    #[serde(default)]
    persistence: String,
}

#[derive(Debug, Serialize)]
struct TopicsBody {
    topics: Vec<TopicConfigBody>,
}

async fn list_topics(State(state): State<StatusState>) -> Result<Json<TopicsBody>, StatusCode> {
    let topics = state
        .db
        .list_topic_configs()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(TopicsBody {
        topics: topics.into_iter().map(to_body).collect(),
    }))
}

async fn configure_topics(
    State(state): State<StatusState>,
    Json(body): Json<ConfigureTopicsBody>,
) -> Result<Json<TopicsBody>, StatusCode> {
    if body.topics.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut configs = Vec::new();
    for item in body.topics {
        let topic = item.topic.trim();
        if topic.is_empty() {
            return Err(StatusCode::BAD_REQUEST);
        }
        let Some(delivery) = DeliveryMode::parse(&item.delivery) else {
            return Err(StatusCode::BAD_REQUEST);
        };
        let Some(persistence) = PersistenceMode::parse(&item.persistence) else {
            return Err(StatusCode::BAD_REQUEST);
        };
        configs.push(TopicConfig {
            topic: topic.to_string(),
            delivery,
            persistence,
        });
    }
    let topics = state
        .db
        .configure_topics(&configs)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(TopicsBody {
        topics: topics.into_iter().map(to_body).collect(),
    }))
}

fn to_body(config: TopicConfig) -> TopicConfigBody {
    TopicConfigBody {
        topic: config.topic,
        delivery: config.delivery.as_str().to_string(),
        persistence: config.persistence.as_str().to_string(),
    }
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
        dropped: topics_detail.iter().map(|topic| topic.dropped).sum(),
        topics_detail,
    }))
}
