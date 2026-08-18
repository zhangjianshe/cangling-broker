use std::{collections::HashMap, time::Instant};

use axum::{
    extract::{Query, State},
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
    grpc_conn::{GrpcClientInfo, GrpcClientRegistry},
    model::{ConsumerSnapshot, DeliveryMode, PersistenceMode, TopicConfig, TopicSnapshot},
    subscribers::{SessionInfo, TopicSubscribers},
};

#[derive(Clone)]
struct StatusState {
    db: Database,
    subscribers: TopicSubscribers,
    mqtt_clients: crate::mqtt::ClientRegistry,
    grpc_clients: GrpcClientRegistry,
    consumer_ttl_secs: u64,
    started: Instant,
    auth_token: Option<String>,
    web_base: Option<String>,
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
    db_bytes: u64,
    clients: Vec<ClientInfo>,
    topics_detail: Vec<TopicSnapshot>,
}

#[derive(Debug, Serialize)]
struct ClientInfo {
    peer: String,
    protocol: &'static str,
    #[serde(skip_serializing_if = "String::is_empty")]
    client_id: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    host: String,
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
    #[serde(skip_serializing_if = "String::is_empty")]
    version: String,
    #[serde(skip_serializing_if = "String::is_empty")]
    host: String,
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
    grpc_clients: GrpcClientRegistry,
) -> anyhow::Result<()> {
    let state = StatusState {
        db,
        subscribers,
        mqtt_clients,
        grpc_clients,
        consumer_ttl_secs: config.consumer_ttl_secs,
        started: Instant::now(),
        auth_token: auth::normalize(config.auth_token.as_deref()),
        web_base: config.status_web_base(),
    };
    let app = status_app(state, mqtt);
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

fn status_app(state: StatusState, mqtt: Option<crate::mqtt::MqttCtx>) -> Router {
    let web_base = state.web_base.clone();
    let mut app = status_routes(state.clone());
    if let Some(ctx) = mqtt {
        app = app.merge(crate::mqtt::ws_status_router(ctx));
    }
    match web_base {
        Some(base) => Router::new().merge(app.clone()).nest(&base, app),
        None => app,
    }
}

fn status_routes(state: StatusState) -> Router {
    Router::new()
        .route("/", get(page))
        .route("/health", get(health))
        .route("/status", get(status))
        .route("/topics", get(list_topics).post(configure_topics))
        .route("/messages", get(topic_message).delete(clear_topic_messages))
        .layer(middleware::from_fn_with_state(state.clone(), require_token))
        .with_state(state)
}

fn is_open_health_path(path: &str, web_base: Option<&str>) -> bool {
    if path == "/health" {
        return true;
    }
    web_base.is_some_and(|base| path == format!("{base}/health"))
}

fn dashboard_html(web_base: Option<&str>) -> String {
    let html = include_str!("status.html");
    match web_base {
        Some(base) => {
            let tag = format!(r#"<base href="{}/">"#, base.trim_end_matches('/'));
            html.replacen("<head>", &format!("<head>\n  {tag}"), 1)
        }
        None => html.to_string(),
    }
}

async fn require_token(
    State(state): State<StatusState>,
    request: Request<axum::body::Body>,
    next: Next,
) -> Result<Response, StatusCode> {
    if is_open_health_path(request.uri().path(), state.web_base.as_deref()) {
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

async fn page(State(state): State<StatusState>) -> Html<String> {
    Html(dashboard_html(state.web_base.as_deref()))
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

#[derive(Debug, Deserialize)]
struct MessagesQuery {
    topic: String,
    #[serde(default)]
    offset: i64,
}

#[derive(Debug, Serialize)]
struct MessagePageBody {
    topic: String,
    offset: i64,
    total: i64,
    message: Option<MessageBody>,
}

#[derive(Debug, Serialize)]
struct MessageBody {
    id: String,
    topic: String,
    payload: String,
    payload_encoding: &'static str,
    truncated: bool,
    attributes: serde_json::Value,
    status: String,
    attempts: i64,
    created_at: String,
    delivered_at: Option<String>,
    last_error: Option<String>,
}

const MESSAGE_PREVIEW_BYTES: usize = 64 * 1024;

async fn topic_message(
    State(state): State<StatusState>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<MessagePageBody>, StatusCode> {
    let topic = query.topic.trim();
    if topic.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let page = state
        .db
        .topic_message_page(topic, query.offset)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(MessagePageBody {
        topic: topic.to_string(),
        offset: page.offset,
        total: page.total,
        message: page.message.map(to_message_body),
    }))
}

#[derive(Debug, Serialize)]
struct ClearMessagesBody {
    topic: String,
    deleted: u64,
}

async fn clear_topic_messages(
    State(state): State<StatusState>,
    Query(query): Query<MessagesQuery>,
) -> Result<Json<ClearMessagesBody>, StatusCode> {
    let topic = query.topic.trim();
    if topic.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let deleted = state
        .db
        .clear_topic_messages(topic)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(ClearMessagesBody {
        topic: topic.to_string(),
        deleted,
    }))
}

fn to_message_body(message: crate::db::StoredMessage) -> MessageBody {
    let (payload, payload_encoding, truncated) = match String::from_utf8(message.payload) {
        Ok(text) => {
            if text.len() > MESSAGE_PREVIEW_BYTES {
                let mut end = MESSAGE_PREVIEW_BYTES;
                while end > 0 && !text.is_char_boundary(end) {
                    end -= 1;
                }
                (text[..end].to_string(), "utf-8", true)
            } else {
                (text, "utf-8", false)
            }
        }
        Err(error) => {
            let bytes = error.into_bytes();
            let truncated = bytes.len() > MESSAGE_PREVIEW_BYTES;
            let slice = if truncated {
                &bytes[..MESSAGE_PREVIEW_BYTES]
            } else {
                &bytes
            };
            (encode_base64(slice), "base64", truncated)
        }
    };
    MessageBody {
        id: message.id,
        topic: message.topic,
        payload,
        payload_encoding,
        truncated,
        attributes: message.attributes,
        status: message.status,
        attempts: message.attempts,
        created_at: message.created_at,
        delivered_at: message.delivered_at,
        last_error: message.last_error,
    }
}

fn encode_base64(bytes: &[u8]) -> String {
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
        &state.grpc_clients.clients(),
        &topics_detail,
    );
    let consumers = topics_detail.iter().map(|topic| topic.streams).sum();
    let db_bytes = state.db.sqlite_size_bytes().await.unwrap_or(0);
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
        db_bytes,
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
        if !index.contains_key(&session.topic) {
            let idx = topics.len();
            index.insert(session.topic.clone(), idx);
            topics.push(TopicSnapshot {
                name: session.topic.clone(),
                delivery: "broadcast".into(),
                persistence: "persistent".into(),
                ..TopicSnapshot::default()
            });
        }
    }
    for topic in topics.iter_mut() {
        for session in sessions {
            if !crate::topic::filter_matches(&session.topic, &topic.name) {
                continue;
            }
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
}

fn connected_clients(
    sessions: &[SessionInfo],
    mqtt_clients: &[crate::mqtt::MqttClientInfo],
    grpc_clients: &[GrpcClientInfo],
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
            version: first_value(&mqtt.version, &subscriptions, |item| item.version.as_str()),
            host: first_value("", &subscriptions, |item| item.host.as_str()),
            streams: subscriptions.len(),
            connected_at: mqtt.connected_at.clone(),
            last_seen_at,
            subscriptions,
        });
    }
    for grpc in grpc_clients {
        let mut subscriptions = Vec::new();
        for session in sessions {
            if session.peer != grpc.peer {
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
            .unwrap_or(grpc.connected_at.as_str())
            .to_string();
        clients.push(ClientInfo {
            peer: grpc.peer.clone(),
            protocol: "grpc",
            client_id: String::new(),
            version: first_value(&grpc.version, &subscriptions, |item| item.version.as_str()),
            host: first_value(&grpc.host, &subscriptions, |item| item.host.as_str()),
            streams: subscriptions.len(),
            connected_at: grpc.connected_at.clone(),
            last_seen_at,
            subscriptions,
        });
    }
    let mut by_identity: HashMap<String, (String, Vec<ClientSubscription>)> = HashMap::new();
    for session in sessions {
        if claimed.contains(&(session.id.as_str(), session.topic.as_str())) {
            continue;
        }
        let subscription = to_subscription(session, &registered);
        let key = grpc_group_key(session);
        by_identity
            .entry(key)
            .or_insert_with(|| (session.peer.clone(), Vec::new()))
            .1
            .push(subscription);
    }
    clients.extend(by_identity.into_iter().map(|(_, (peer, mut subscriptions))| {
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
                version: first_value("", &subscriptions, |item| item.version.as_str()),
                host: first_value("", &subscriptions, |item| item.host.as_str()),
                streams: subscriptions.len(),
                connected_at,
                last_seen_at,
                subscriptions,
            }
        }));
    clients.sort_by(|left, right| {
        left.host
            .cmp(&right.host)
            .then(left.peer.cmp(&right.peer))
            .then(left.client_id.cmp(&right.client_id))
    });
    clients
}

fn grpc_group_key(session: &SessionInfo) -> String {
    if !session.host.is_empty() {
        format!("h:{}", session.host)
    } else {
        format!("p:{}", session.peer)
    }
}

fn to_subscription(
    session: &SessionInfo,
    registered: &HashMap<(&str, &str), &ConsumerSnapshot>,
) -> ClientSubscription {
    let meta = registered.get(&(session.topic.as_str(), session.id.as_str()));
    let attributes = meta
        .map(|consumer| consumer.attributes.clone())
        .unwrap_or_default();
    let version = if !session.version.is_empty() {
        session.version.clone()
    } else {
        attributes
            .get("version")
            .cloned()
            .unwrap_or_default()
    };
    let host = if !session.host.is_empty() {
        session.host.clone()
    } else {
        attributes.get("host").cloned().unwrap_or_default()
    };
    ClientSubscription {
        id: session.id.clone(),
        name: meta
            .map(|consumer| consumer.name.clone())
            .filter(|name| !name.is_empty())
            .or_else(|| session.id.strip_prefix("mqtt:").map(ToOwned::to_owned))
            .unwrap_or_default(),
        topic: session.topic.clone(),
        protocol: session.protocol,
        version,
        host,
        attributes,
        connected_at: session.connected_at.clone(),
        last_seen_at: meta
            .map(|consumer| consumer.last_seen_at.clone())
            .unwrap_or_else(|| session.connected_at.clone()),
    }
}

fn first_value(
    primary: &str,
    subscriptions: &[ClientSubscription],
    pick: impl Fn(&ClientSubscription) -> &str,
) -> String {
    if !primary.is_empty() {
        return primary.to_string();
    }
    subscriptions
        .iter()
        .map(pick)
        .find(|value| !value.is_empty())
        .unwrap_or("")
        .to_string()
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
                version: "java/0.1.29".into(),
                host: String::new(),
            },
            SessionInfo {
                id: "c2".into(),
                topic: "logs".into(),
                peer: "127.0.0.1:9".into(),
                connected_at: "2026-08-16T00:00:01Z".into(),
                protocol: "grpc",
                version: "java/0.1.29".into(),
                host: String::new(),
            },
            SessionInfo {
                id: "c3".into(),
                topic: "alerts".into(),
                peer: "10.0.0.2:8".into(),
                connected_at: "2026-08-16T00:00:03Z".into(),
                protocol: "mqtt",
                version: "3.1.1".into(),
                host: String::new(),
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
        let clients = connected_clients(&sessions, &[], &[], &topics);
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
        assert_eq!(clients[0].version, "3.1.1");
        assert_eq!(clients[1].version, "java/0.1.29");
        assert_eq!(clients[1].host, "worker-1");
    }

    #[test]
    fn docker_nat_peers_split_by_host() {
        let sessions = vec![
            SessionInfo {
                id: "c1".into(),
                topic: "jobs".into(),
                peer: "172.21.0.1:41001".into(),
                connected_at: "2026-08-16T00:00:02Z".into(),
                protocol: "grpc",
                version: "java/0.1.29".into(),
                host: "api".into(),
            },
            SessionInfo {
                id: "c2".into(),
                topic: "jobs".into(),
                peer: "172.21.0.1:41002".into(),
                connected_at: "2026-08-16T00:00:01Z".into(),
                protocol: "grpc",
                version: "java/0.1.29".into(),
                host: "worker".into(),
            },
            SessionInfo {
                id: "c3".into(),
                topic: "logs".into(),
                peer: "172.21.0.1:41003".into(),
                connected_at: "2026-08-16T00:00:03Z".into(),
                protocol: "grpc",
                version: "java/0.1.29".into(),
                host: "worker".into(),
            },
        ];
        let clients = connected_clients(&sessions, &[], &[], &[]);
        assert_eq!(clients.len(), 2);
        assert_eq!(clients[0].host, "api");
        assert_eq!(clients[0].streams, 1);
        assert_eq!(clients[1].host, "worker");
        assert_eq!(clients[1].streams, 2);
    }

    #[test]
    fn client_version_falls_back_to_register_attribute() {
        let sessions = vec![SessionInfo {
            id: "c1".into(),
            topic: "jobs".into(),
            peer: "127.0.0.1:9".into(),
            connected_at: "2026-08-16T00:00:02Z".into(),
            protocol: "grpc",
            version: String::new(),
            host: String::new(),
        }];
        let topics = vec![TopicSnapshot {
            name: "jobs".into(),
            consumers: vec![ConsumerSnapshot {
                id: "c1".into(),
                name: "java-s0".into(),
                last_seen_at: "2026-08-16T00:01:00Z".into(),
                live: true,
                attributes: HashMap::from([("version".into(), "python/0.1.29".into())]),
            }],
            ..TopicSnapshot::default()
        }];
        let clients = connected_clients(&sessions, &[], &[], &topics);
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].version, "python/0.1.29");
        assert_eq!(clients[0].subscriptions[0].version, "python/0.1.29");
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
            version: "3.1.1".into(),
            host: String::new(),
        }];
        merge_live_sessions(&mut topics, &sessions);
        assert_eq!(topics.len(), 2);
        let live = topics.iter().find(|topic| topic.name == "building/#").unwrap();
        assert_eq!(live.delivery, "broadcast");
        assert_eq!(live.persistence, "persistent");
        assert_eq!(live.consumers.len(), 1);
        assert!(live.consumers[0].live);
        assert_eq!(live.consumers[0].name, "browser-1");
        assert_eq!(
            live.consumers[0].attributes.get("protocol").map(String::as_str),
            Some("mqtt-ws")
        );
        assert_eq!(topics.iter().find(|topic| topic.name == "jobs").unwrap().accepted, 3);

        let mut published = vec![TopicSnapshot {
            name: "/ibuser/1/dRueErAe".into(),
            accepted: 10,
            ..TopicSnapshot::default()
        }];
        let hash_sessions = vec![SessionInfo {
            id: "mqtt:browser-1".into(),
            topic: "/ibuser/1/#".into(),
            peer: "10.0.0.8:1".into(),
            connected_at: "2026-08-16T00:00:04Z".into(),
            protocol: "mqtt-ws",
            version: "3.1.1".into(),
            host: String::new(),
        }];
        merge_live_sessions(&mut published, &hash_sessions);
        let row = published
            .iter()
            .find(|topic| topic.name == "/ibuser/1/dRueErAe")
            .unwrap();
        assert_eq!(row.consumers.len(), 1);
        assert_eq!(row.consumers[0].name, "browser-1");
    }

    #[test]
    fn mqtt_websocket_connect_appears_without_subscribe() {
        let mqtt_clients = vec![crate::mqtt::MqttClientInfo {
            client_id: "browser-1".into(),
            peer: "10.0.0.8:44321".into(),
            transport: "mqtt-ws",
            connected_at: "2026-08-16T00:00:04Z".into(),
            version: "3.1.1".into(),
        }];
        let clients = connected_clients(&[], &mqtt_clients, &[], &[]);
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].protocol, "mqtt-ws");
        assert_eq!(clients[0].client_id, "browser-1");
        assert_eq!(clients[0].peer, "10.0.0.8:44321");
        assert_eq!(clients[0].version, "3.1.1");
        assert_eq!(clients[0].streams, 0);
        assert!(clients[0].subscriptions.is_empty());
    }

    #[test]
    fn grpc_tcp_connect_appears_without_subscribe() {
        let grpc_clients = vec![GrpcClientInfo {
            peer: "172.21.0.1:41001".into(),
            connected_at: "2026-08-16T00:00:04Z".into(),
            version: "python/0.1.34".into(),
            host: "api".into(),
        }];
        let clients = connected_clients(&[], &[], &grpc_clients, &[]);
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].protocol, "grpc");
        assert_eq!(clients[0].peer, "172.21.0.1:41001");
        assert_eq!(clients[0].host, "api");
        assert_eq!(clients[0].version, "python/0.1.34");
        assert_eq!(clients[0].streams, 0);
        assert!(clients[0].subscriptions.is_empty());
    }

    #[test]
    fn dashboard_html_injects_base_href() {
        let html = dashboard_html(Some("/msg"));
        assert!(html.contains(r#"<base href="/msg/">"#), "{html}");
        assert!(html.contains("apiUrl(\"status\")"), "{html}");
        assert!(!html.contains(r#"fetch("/status""#), "{html}");
        assert!(html.contains("data-tab=\"clients\""), "{html}");
        assert!(html.contains("data-tab=\"topics\""), "{html}");
        assert!(html.contains("withPagers("), "{html}");
        assert!(html.contains("client.version"), "{html}");
        assert!(html.contains("client.host"), "{html}");
        assert!(html.contains("序号"), "{html}");
        assert!(html.contains("data-topic-name"), "{html}");
        assert!(html.contains("topic-modal"), "{html}");
        assert!(html.contains("consumerSummary("), "{html}");
        assert!(html.contains("id=\"client-type-filter\""), "{html}");
        assert!(html.contains("id=\"topic-name-filter\""), "{html}");
        assert!(html.contains("function fuzzyMatch("), "{html}");
        assert!(html.contains("连接时长"), "{html}");
        assert!(html.contains("fmtSince("), "{html}");
        assert!(html.contains("apiUrl(\"messages\")"), "{html}");
        assert!(html.contains("data-msg-nav"), "{html}");
        assert!(html.contains("function numCell("), "{html}");
        assert!(html.contains("td.zero"), "{html}");
        assert!(html.contains("data-clear-topic"), "{html}");
        assert!(html.contains("id=\"client-page-size\""), "{html}");
        assert!(html.contains("id=\"topic-page-size\""), "{html}");
        assert!(!html.contains("连接时间"), "{html}");
        assert!(html.contains("pad2(date.getMonth() + 1)"), "{html}");
        assert!(!html.contains("toLocaleString"), "{html}");
        assert!(!html.contains("toLocaleTimeString"), "{html}");
    }

    #[test]
    fn dashboard_html_root_has_no_base_tag() {
        let html = dashboard_html(None);
        assert!(!html.contains("<base "));
        assert!(html.contains("apiUrl(\"status\")"));
        assert!(html.contains("role=\"tablist\""));
        assert!(html.contains("pager.top"));
        assert!(html.contains("pager.bottom"));
    }

    #[test]
    fn health_stays_open_under_web_base() {
        assert!(is_open_health_path("/health", None));
        assert!(is_open_health_path("/health", Some("/msg")));
        assert!(is_open_health_path("/msg/health", Some("/msg")));
        assert!(!is_open_health_path("/msg/status", Some("/msg")));
        assert!(!is_open_health_path("/status", Some("/msg")));
    }
}
