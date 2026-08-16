use std::{collections::HashMap, time::Instant};

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
    model::{ConsumerSnapshot, DeliveryMode, PersistenceMode, TopicConfig, TopicSnapshot},
    subscribers::{SessionInfo, TopicSubscribers},
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
    version: &'static str,
    git: &'static str,
    built: String,
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
    clients: Vec<ClientInfo>,
    topics_detail: Vec<TopicSnapshot>,
}

#[derive(Debug, Serialize)]
struct ClientInfo {
    id: String,
    name: String,
    topic: String,
    attributes: HashMap<String, String>,
    peer: String,
    connected_at: String,
    last_seen_at: String,
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
    let live_sessions = state.subscribers.sessions();
    for topic in &mut topics_detail {
        topic.streams = state.subscribers.count(&topic.name);
        for consumer in &mut topic.consumers {
            if state.subscribers.is_live(&topic.name, &consumer.id) {
                consumer.live = true;
            }
        }
    }
    let clients = connected_clients(&live_sessions, &topics_detail);
    let consumers = topics_detail.iter().map(|topic| topic.streams).sum();
    Ok(Json(BrokerStatus {
        version: env!("CARGO_PKG_VERSION"),
        git: env!("GIT_HASH"),
        built: crate::logging::format_wall_time(env!("BUILD_TIME")),
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
        clients,
        topics_detail,
    }))
}

fn connected_clients(sessions: &[SessionInfo], topics: &[TopicSnapshot]) -> Vec<ClientInfo> {
    let registered: HashMap<(&str, &str), &ConsumerSnapshot> = topics
        .iter()
        .flat_map(|topic| {
            topic
                .consumers
                .iter()
                .map(move |consumer| ((topic.name.as_str(), consumer.id.as_str()), consumer))
        })
        .collect();
    sessions
        .iter()
        .map(|session| {
            let meta = registered.get(&(session.topic.as_str(), session.id.as_str()));
            ClientInfo {
                id: session.id.clone(),
                name: meta.map(|consumer| consumer.name.clone()).unwrap_or_default(),
                topic: session.topic.clone(),
                attributes: meta
                    .map(|consumer| consumer.attributes.clone())
                    .unwrap_or_default(),
                peer: session.peer.clone(),
                connected_at: session.connected_at.clone(),
                last_seen_at: meta
                    .map(|consumer| consumer.last_seen_at.clone())
                    .unwrap_or_else(|| session.connected_at.clone()),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connected_clients_merge_register_metadata() {
        let sessions = vec![SessionInfo {
            id: "c1".into(),
            topic: "jobs".into(),
            peer: "127.0.0.1:9".into(),
            connected_at: "2026-08-16T00:00:00Z".into(),
        }];
        let topics = vec![TopicSnapshot {
            name: "jobs".into(),
            consumers: vec![ConsumerSnapshot {
                id: "c1".into(),
                name: "java-s0".into(),
                last_seen_at: "2026-08-16T00:01:00Z".into(),
                live: true,
                attributes: HashMap::from([("host".into(), "worker-1".into())]),
            }],
            ..TopicSnapshot::default()
        }];
        let clients = connected_clients(&sessions, &topics);
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].name, "java-s0");
        assert_eq!(clients[0].peer, "127.0.0.1:9");
        assert_eq!(clients[0].last_seen_at, "2026-08-16T00:01:00Z");
        assert_eq!(clients[0].attributes.get("host").unwrap(), "worker-1");
    }
}
