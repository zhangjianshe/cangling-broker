use std::{collections::HashMap, time::Instant};

use axum::{
    extract::State,
    http::{header::AUTHORIZATION, Request, StatusCode},
    middleware::{self, Next},
    response::{Html, Response},
    routing::get,
    Json, Router,
};
use std::net::SocketAddr;
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
    mqtt_clients: crate::mqtt::ClientRegistry,
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
    peer: String,
    protocol: &'static str,
    #[serde(skip_serializing_if = "String::is_empty")]
    client_id: String,
    streams: usize,
    connected_at: String,
    last_seen_at: String,
    subscriptions: Vec<ClientSubscription>,
}

#[derive(Debug, Serialize)]
struct ClientSubscription {
    id: String,
    name: String,
    topic: String,
    protocol: &'static str,
    attributes: HashMap<String, String>,
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
    mqtt: Option<crate::mqtt::MqttCtx>,
    mqtt_clients: crate::mqtt::ClientRegistry,
) -> anyhow::Result<()> {
    let state = StatusState {
        db,
        subscribers,
        mqtt_clients,
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
    let app = match mqtt {
        Some(ctx) => app.merge(crate::mqtt::ws_status_router(ctx)),
        None => app,
    };
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<SocketAddr>(),
    )
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
    merge_live_sessions(&mut topics_detail, &live_sessions);
    for topic in &mut topics_detail {
        topic.streams = state.subscribers.matching_count(&topic.name);
        for consumer in &mut topic.consumers {
            if state.subscribers.is_live(&topic.name, &consumer.id) {
                consumer.live = true;
            }
        }
    }
    topics_detail.sort_by(|left, right| {
        right
            .streams
            .cmp(&left.streams)
            .then(left.name.cmp(&right.name))
    });
    let clients = connected_clients(
        &live_sessions,
        &state.mqtt_clients.clients(),
        &topics_detail,
    );
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

fn merge_live_sessions(topics: &mut Vec<TopicSnapshot>, sessions: &[SessionInfo]) {
    let mut index: HashMap<String, usize> = topics
        .iter()
        .enumerate()
        .map(|(i, topic)| (topic.name.clone(), i))
        .collect();
    for session in sessions {
        let idx = match index.get(&session.topic) {
            Some(idx) => *idx,
            None => {
                let idx = topics.len();
                index.insert(session.topic.clone(), idx);
                topics.push(TopicSnapshot {
                    name: session.topic.clone(),
                    delivery: "broadcast".into(),
                    persistence: "ephemeral".into(),
                    ..TopicSnapshot::default()
                });
                idx
            }
        };
        let topic = &mut topics[idx];
        if let Some(consumer) = topic
            .consumers
            .iter_mut()
            .find(|consumer| consumer.id == session.id)
        {
            consumer.live = true;
            continue;
        }
        topic.consumers.push(ConsumerSnapshot {
            id: session.id.clone(),
            name: session
                .id
                .strip_prefix("mqtt:")
                .unwrap_or(session.id.as_str())
                .to_string(),
            last_seen_at: session.connected_at.clone(),
            live: true,
            attributes: HashMap::from([("protocol".into(), session.protocol.to_string())]),
        });
    }
}

fn connected_clients(
    sessions: &[SessionInfo],
    mqtt_clients: &[crate::mqtt::MqttClientInfo],
    topics: &[TopicSnapshot],
) -> Vec<ClientInfo> {
    let registered: HashMap<(&str, &str), &ConsumerSnapshot> = topics
        .iter()
        .flat_map(|topic| {
            topic
                .consumers
                .iter()
                .map(move |consumer| ((topic.name.as_str(), consumer.id.as_str()), consumer))
        })
        .collect();
    let mut claimed = std::collections::HashSet::new();
    let mut clients = Vec::new();
    for mqtt in mqtt_clients {
        let session_id = format!("mqtt:{}", mqtt.client_id);
        let mut subscriptions = Vec::new();
        for session in sessions {
            if session.id != session_id {
                continue;
            }
            claimed.insert((session.id.as_str(), session.topic.as_str()));
            subscriptions.push(to_subscription(session, &registered));
        }
        subscriptions.sort_by(|left, right| {
            left.topic
                .cmp(&right.topic)
                .then(left.id.cmp(&right.id))
        });
        let last_seen_at = subscriptions
            .iter()
            .map(|item| item.last_seen_at.as_str())
            .max()
            .unwrap_or(mqtt.connected_at.as_str())
            .to_string();
        clients.push(ClientInfo {
            peer: mqtt.peer.clone(),
            protocol: mqtt.transport,
            client_id: mqtt.client_id.clone(),
            streams: subscriptions.len(),
            connected_at: mqtt.connected_at.clone(),
            last_seen_at,
            subscriptions,
        });
    }
    let mut by_peer: HashMap<String, Vec<ClientSubscription>> = HashMap::new();
    for session in sessions {
        if claimed.contains(&(session.id.as_str(), session.topic.as_str())) {
            continue;
        }
        by_peer
            .entry(session.peer.clone())
            .or_default()
            .push(to_subscription(session, &registered));
    }
    clients.extend(by_peer.into_iter().map(|(peer, mut subscriptions)| {
            subscriptions.sort_by(|left, right| {
                left.topic
                    .cmp(&right.topic)
                    .then(left.name.cmp(&right.name))
                    .then(left.id.cmp(&right.id))
            });
            let connected_at = subscriptions
                .iter()
                .map(|item| item.connected_at.as_str())
                .min()
                .unwrap_or("")
                .to_string();
            let last_seen_at = subscriptions
                .iter()
                .map(|item| item.last_seen_at.as_str())
                .max()
                .unwrap_or("")
                .to_string();
            let protocol = subscriptions
                .first()
                .map(|item| item.protocol)
                .unwrap_or("grpc");
            ClientInfo {
                peer,
                protocol,
                client_id: String::new(),
                streams: subscriptions.len(),
                connected_at,
                last_seen_at,
                subscriptions,
            }
        }));
    clients.sort_by(|left, right| {
        left.peer
            .cmp(&right.peer)
            .then(left.client_id.cmp(&right.client_id))
    });
    clients
}

fn to_subscription(
    session: &SessionInfo,
    registered: &HashMap<(&str, &str), &ConsumerSnapshot>,
) -> ClientSubscription {
    let meta = registered.get(&(session.topic.as_str(), session.id.as_str()));
    ClientSubscription {
        id: session.id.clone(),
        name: meta
            .map(|consumer| consumer.name.clone())
            .filter(|name| !name.is_empty())
            .or_else(|| session.id.strip_prefix("mqtt:").map(ToOwned::to_owned))
            .unwrap_or_default(),
        topic: session.topic.clone(),
        protocol: session.protocol,
        attributes: meta
            .map(|consumer| consumer.attributes.clone())
            .unwrap_or_default(),
        connected_at: session.connected_at.clone(),
        last_seen_at: meta
            .map(|consumer| consumer.last_seen_at.clone())
            .unwrap_or_else(|| session.connected_at.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn connected_clients_group_streams_by_peer() {
        let sessions = vec![
            SessionInfo {
                id: "c1".into(),
                topic: "jobs".into(),
                peer: "127.0.0.1:9".into(),
                connected_at: "2026-08-16T00:00:02Z".into(),
                protocol: "grpc",
            },
            SessionInfo {
                id: "c2".into(),
                topic: "logs".into(),
                peer: "127.0.0.1:9".into(),
                connected_at: "2026-08-16T00:00:01Z".into(),
                protocol: "grpc",
            },
            SessionInfo {
                id: "c3".into(),
                topic: "alerts".into(),
                peer: "10.0.0.2:8".into(),
                connected_at: "2026-08-16T00:00:03Z".into(),
                protocol: "mqtt",
            },
        ];
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
        let clients = connected_clients(&sessions, &[], &topics);
        assert_eq!(clients.len(), 2);
        assert_eq!(clients[0].peer, "10.0.0.2:8");
        assert_eq!(clients[0].protocol, "mqtt");
        assert_eq!(clients[0].streams, 1);
        assert_eq!(clients[1].peer, "127.0.0.1:9");
        assert_eq!(clients[1].streams, 2);
        assert_eq!(clients[1].connected_at, "2026-08-16T00:00:01Z");
        assert_eq!(clients[1].last_seen_at, "2026-08-16T00:01:00Z");
        assert_eq!(clients[1].subscriptions[0].name, "java-s0");
        assert_eq!(
            clients[1].subscriptions[0]
                .attributes
                .get("host")
                .unwrap(),
            "worker-1"
        );
        assert_eq!(clients[1].subscriptions[1].topic, "logs");
    }

    #[test]
    fn websocket_subscribe_adds_topic_to_list() {
        let mut topics = vec![TopicSnapshot {
            name: "jobs".into(),
            accepted: 3,
            ..TopicSnapshot::default()
        }];
        let sessions = vec![SessionInfo {
            id: "mqtt:browser-1".into(),
            topic: "building/#".into(),
            peer: "10.0.0.8:1".into(),
            connected_at: "2026-08-16T00:00:04Z".into(),
            protocol: "mqtt-ws",
        }];
        merge_live_sessions(&mut topics, &sessions);
        assert_eq!(topics.len(), 2);
        let live = topics.iter().find(|topic| topic.name == "building/#").unwrap();
        assert_eq!(live.delivery, "broadcast");
        assert_eq!(live.persistence, "ephemeral");
        assert_eq!(live.consumers.len(), 1);
        assert!(live.consumers[0].live);
        assert_eq!(live.consumers[0].name, "browser-1");
        assert_eq!(
            live.consumers[0].attributes.get("protocol").map(String::as_str),
            Some("mqtt-ws")
        );
        assert_eq!(topics.iter().find(|topic| topic.name == "jobs").unwrap().accepted, 3);
    }

    #[test]
    fn mqtt_websocket_connect_appears_without_subscribe() {
        let mqtt_clients = vec![crate::mqtt::MqttClientInfo {
            client_id: "browser-1".into(),
            peer: "10.0.0.8:44321".into(),
            transport: "mqtt-ws",
            connected_at: "2026-08-16T00:00:04Z".into(),
        }];
        let clients = connected_clients(&[], &mqtt_clients, &[]);
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].protocol, "mqtt-ws");
        assert_eq!(clients[0].client_id, "browser-1");
        assert_eq!(clients[0].peer, "10.0.0.8:44321");
        assert_eq!(clients[0].streams, 0);
        assert!(clients[0].subscriptions.is_empty());
    }
}
