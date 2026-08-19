mod auth;
mod config;
mod db;
mod delivery;
mod grpc_conn;
mod logging;
mod model;
mod mqtt;
mod status;
mod subscribers;
mod topic;

use std::{pin::Pin, sync::Arc, time::Duration};

use clap::Parser;
use auth::AuthInterceptor;
use config::Config;
use db::Database;
use delivery::{Ingested, PROTOCOL_GRPC};
use grpc_conn::{GrpcClientRegistry, TrackingIncoming};
use subscribers::{InflightAcks, TopicSubscribers};
use tokio::sync::mpsc;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status, Streaming};
use tracing::{error, info, warn};
use uuid::Uuid;

pub mod proto {
    tonic::include_proto!("dispatcher.v1");
}
use proto::{
    message_queue_server::{MessageQueue, MessageQueueServer},
    AcceptMessageRequest, AcceptMessageResponse, AckMessageRequest, AckMessageResponse,
    ConfigureTopicsRequest, ConfigureTopicsResponse, ListTopicsRequest, ListTopicsResponse,
    RegisterRequest, RegisterResponse, SatwayMessage, SubscribeRequest, TopicConfig as ProtoTopicConfig,
    UnregisterRequest, UnregisterResponse,
};
use crate::model::{DeliveryMode, PersistenceMode, TopicConfig};

#[derive(Clone)]
struct QueueService {
    db: Database,
    config: Arc<Config>,
    subscribers: TopicSubscribers,
    inflight: InflightAcks,
    grpc_clients: GrpcClientRegistry,
    shutdown: CancellationToken,
}

type ResponseStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

fn note_grpc_identity<T>(
    registry: &GrpcClientRegistry,
    request: &Request<T>,
    version: &str,
    host: &str,
) {
    if let Some(addr) = request.remote_addr() {
        registry.touch(&addr.to_string(), version, host);
    }
}

fn to_proto_topic(config: TopicConfig) -> ProtoTopicConfig {
    ProtoTopicConfig {
        topic: config.topic,
        delivery: config.delivery.as_str().to_string(),
        persistence: config.persistence.as_str().to_string(),
    }
}

#[tonic::async_trait]
impl MessageQueue for QueueService {
    type AcceptMessagesStream = ResponseStream<AcceptMessageResponse>;
    type SubscribeStream = ResponseStream<SatwayMessage>;

    async fn accept_messages(
        &self,
        request: Request<Streaming<AcceptMessageRequest>>,
    ) -> Result<Response<Self::AcceptMessagesStream>, Status> {
        note_grpc_identity(
            &self.grpc_clients,
            &request,
            &auth::metadata_client_version(request.metadata()),
            &auth::metadata_client_host(request.metadata()),
        );
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let db = self.db.clone();
        let subscribers = self.subscribers.clone();
        let log_messages = self.config.log_messages;
        tokio::spawn(async move {
            while let Some(message) = inbound.next().await {
                let message = match message {
                    Ok(message) => message,
                    Err(error) => {
                        let _ = tx.send(Err(error)).await;
                        break;
                    }
                };
                if message.topic.trim().is_empty() {
                    let _ = tx
                        .send(Err(Status::invalid_argument("topic is required")))
                        .await;
                    continue;
                }
                if message.payload.is_empty() {
                    let _ = tx
                        .send(Err(Status::invalid_argument("payload is required")))
                        .await;
                    continue;
                }
                let topic = message.topic.trim();
                match crate::delivery::ingest(
                    &db,
                    &subscribers,
                    topic,
                    &message.payload,
                    message.attributes,
                    Some(&message.idempotency_key),
                    log_messages,
                )
                .await
                {
                    Ok(Ingested::Dropped { message_id }) => {
                        info!(topic, id = %message_id, "ephemeral message dropped: no live subscriber");
                        if tx
                            .send(Ok(AcceptMessageResponse {
                                message_id,
                                duplicate: false,
                            }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Ok(Ingested::Queued {
                        message_id,
                        duplicate,
                    }) => {
                        if tx
                            .send(Ok(AcceptMessageResponse { message_id, duplicate }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                    Err(error) => {
                        error!(%error, "queue write failed");
                        let _ = tx
                            .send(Err(Status::internal("could not persist message")))
                            .await;
                        break;
                    }
                }
            }
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let metadata = request.metadata().clone();
        note_grpc_identity(
            &self.grpc_clients,
            &request,
            &auth::metadata_client_version(&metadata),
            &auth::metadata_client_host(&metadata),
        );
        let mut request = request.into_inner();
        if request.topic.trim().is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        auth::apply_client_metadata(&metadata, &mut request.attributes);
        let consumer_id = self
            .db
            .register_consumer(
                Some(&request.consumer_id),
                request.topic.trim(),
                request.name.trim(),
                &request.attributes,
            )
            .await
            .map_err(|error| {
                error!(%error, "consumer register failed");
                Status::internal("could not register consumer")
            })?;
        info!(%consumer_id, topic = %request.topic, name = %request.name, "consumer metadata registered");
        Ok(Response::new(RegisterResponse { consumer_id }))
    }

    async fn unregister(
        &self,
        request: Request<UnregisterRequest>,
    ) -> Result<Response<UnregisterResponse>, Status> {
        let request = request.into_inner();
        if request.consumer_id.trim().is_empty() {
            return Err(Status::invalid_argument("consumer_id is required"));
        }
        let removed = self
            .db
            .unregister_consumer(request.consumer_id.trim())
            .await
            .map_err(|error| {
                error!(%error, "consumer unregister failed");
                Status::internal("could not unregister consumer")
            })?;
        if removed {
            info!(consumer_id = %request.consumer_id, "consumer unregistered");
        }
        Ok(Response::new(UnregisterResponse {}))
    }

    async fn subscribe(
        &self,
        request: Request<SubscribeRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        let peer = request
            .remote_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_default();
        let mut version = auth::metadata_client_version(request.metadata());
        let mut host = auth::metadata_client_host(request.metadata());
        note_grpc_identity(&self.grpc_clients, &request, &version, &host);
        let request = request.into_inner();
        if request.topic.trim().is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        let topic = request.topic.trim().to_string();
        let consumer_id = request.consumer_id.trim().to_string();
        if !consumer_id.is_empty() {
            let _ = self.db.touch_consumer(&consumer_id).await;
            if version.is_empty() {
                if let Ok(Some(stored)) = self.db.consumer_attribute(&consumer_id, "version").await {
                    version = stored;
                }
            }
            if host.is_empty() {
                if let Ok(Some(stored)) = self.db.consumer_attribute(&consumer_id, "host").await {
                    host = stored;
                }
            }
        }
        let session = if consumer_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            consumer_id.clone()
        };
        let (tx, rx) = mpsc::channel(16);
        crate::delivery::spawn_subscribe_loop(crate::delivery::SubscribeLoop {
            db: self.db.clone(),
            config: self.config.clone(),
            subscribers: self.subscribers.clone(),
            inflight: self.inflight.clone(),
            shutdown: self.shutdown.clone(),
            topic,
            session,
            consumer_id,
            tx,
            peer,
            protocol: PROTOCOL_GRPC,
            version,
            host,
        });
        Ok(Response::new(Box::pin(ReceiverStream::new(rx))))
    }

    async fn configure_topics(
        &self,
        request: Request<ConfigureTopicsRequest>,
    ) -> Result<Response<ConfigureTopicsResponse>, Status> {
        let request = request.into_inner();
        if request.topics.is_empty() {
            return Err(Status::invalid_argument("topics is required"));
        }
        let mut configs = Vec::with_capacity(request.topics.len());
        for item in request.topics {
            let topic = item.topic.trim();
            if topic.is_empty() {
                return Err(Status::invalid_argument("topic is required"));
            }
            let Some(delivery) = DeliveryMode::parse(&item.delivery) else {
                return Err(Status::invalid_argument(
                    "delivery must be single or broadcast",
                ));
            };
            let Some(persistence) = PersistenceMode::parse(&item.persistence) else {
                return Err(Status::invalid_argument(
                    "persistence must be persistent or ephemeral",
                ));
            };
            configs.push(TopicConfig {
                topic: topic.to_string(),
                delivery,
                persistence,
            });
        }
        let stored = self.db.configure_topics(&configs).await.map_err(|error| {
            error!(%error, "configure topics failed");
            Status::internal("could not configure topics")
        })?;
        info!(count = configs.len(), "topic delivery configured");
        Ok(Response::new(ConfigureTopicsResponse {
            topics: stored.into_iter().map(to_proto_topic).collect(),
        }))
    }

    async fn list_topics(
        &self,
        _request: Request<ListTopicsRequest>,
    ) -> Result<Response<ListTopicsResponse>, Status> {
        let stored = self.db.list_topic_configs().await.map_err(|error| {
            error!(%error, "list topics failed");
            Status::internal("could not list topics")
        })?;
        Ok(Response::new(ListTopicsResponse {
            topics: stored.into_iter().map(to_proto_topic).collect(),
        }))
    }

    async fn ack_message(
        &self,
        request: Request<AckMessageRequest>,
    ) -> Result<Response<AckMessageResponse>, Status> {
        let ack = request.into_inner();
        if ack.message_id.is_empty() || ack.lease.is_empty() {
            return Err(Status::invalid_argument("message_id and lease are required"));
        }
        let accepted = self
            .inflight
            .complete(&ack.message_id, &ack.lease, ack.success, ack.error);
        Ok(Response::new(AckMessageResponse { accepted }))
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let config = Arc::new(Config::parse());
    let _log_guard = logging::init(&config)?;
    info!(
        version = env!("CARGO_PKG_VERSION"),
        git = env!("GIT_HASH"),
        built = crate::logging::format_wall_time(env!("BUILD_TIME")),
        "cangling-broker starting"
    );
    if let Some(dir) = config.log_dir() {
        info!(
            dir = %dir.display(),
            max_bytes = config.log_max_bytes,
            keep_files = config.log_keep_files,
            "file logging enabled"
        );
    }
    let db = Database::connect(&config.database_url()).await?;
    let db_for_shutdown = db.clone();
    let shutdown = CancellationToken::new();
    let subscribers = TopicSubscribers::default();
    let inflight = InflightAcks::default();
    let grpc_clients = GrpcClientRegistry::default();
    let status_addr = config.status_listen_addr();
    let status_listener = tokio::net::TcpListener::bind(status_addr).await?;
    info!(address = %status_addr, "HTTP status listening");
    let mqtt_ctx = mqtt::MqttCtx {
        db: db.clone(),
        config: config.clone(),
        subscribers: subscribers.clone(),
        inflight: inflight.clone(),
        shutdown: shutdown.clone(),
        registry: mqtt::ClientRegistry::default(),
    };
    let mqtt_on_status = config.mqtt_enabled && config.mqtt_ws_port == 0;
    let mqtt_tcp = if config.mqtt_enabled && config.mqtt_port != 0 {
        let address = config.mqtt_listen_addr();
        let listener = tokio::net::TcpListener::bind(address).await?;
        info!(%address, "MQTT TCP listening (MQTT 3.1.1, QoS 0/1)");
        Some(tokio::spawn(mqtt::serve_tcp(listener, mqtt_ctx.clone())))
    } else {
        None
    };
    if mqtt_on_status {
        info!(
            address = %status_addr,
            "MQTT WebSocket attached to status port (/mqtt)"
        );
    }
    let mqtt_ws = if config.mqtt_enabled && config.mqtt_ws_port != 0 {
        let address = config.mqtt_ws_listen_addr();
        let listener = tokio::net::TcpListener::bind(address).await?;
        info!(%address, "MQTT WebSocket listening (/mqtt)");
        Some(tokio::spawn(mqtt::serve_ws(listener, mqtt_ctx.clone())))
    } else {
        None
    };
    let status = tokio::spawn(status::serve(
        status_listener,
        db.clone(),
        config.clone(),
        subscribers.clone(),
        shutdown.clone(),
        mqtt_on_status.then(|| mqtt_ctx.clone()),
        mqtt_ctx.registry.clone(),
        grpc_clients.clone(),
    ));
    let worker = tokio::spawn(dispatch_loop(
        db.clone(),
        config.clone(),
        subscribers.clone(),
        shutdown.clone(),
    ));
    let cleaner = tokio::spawn(retention_loop(
        db.clone(),
        config.clone(),
        subscribers.clone(),
        shutdown.clone(),
    ));
    let address = config.grpc_listen_addr();
    let interceptor = AuthInterceptor::new(config.auth_token.clone());
    if interceptor.enabled() {
        info!(%address, "gRPC intake service listening (CL_BROKER_AUTH_TOKEN required)");
    } else {
        info!(%address, "gRPC intake service listening (CL_BROKER_AUTH_TOKEN unset, open)");
    }
    let grpc_listener = tokio::net::TcpListener::bind(address).await?;
    Server::builder()
        .add_service(MessageQueueServer::with_interceptor(
            QueueService {
                db,
                config,
                subscribers,
                inflight,
                grpc_clients: grpc_clients.clone(),
                shutdown: shutdown.clone(),
            },
            interceptor,
        ))
        .serve_with_incoming_shutdown(
            TrackingIncoming::new(grpc_listener, grpc_clients),
            async move {
                wait_for_shutdown().await;
                info!("shutdown signal received");
                shutdown.cancel();
            },
        )
        .await?;
    status.await??;
    worker.await?;
    cleaner.await?;
    if let Some(handle) = mqtt_tcp {
        handle.await??;
    }
    if let Some(handle) = mqtt_ws {
        handle.await??;
    }
    db_for_shutdown.close().await;
    info!("broker stopped");
    Ok(())
}

async fn wait_for_shutdown() {
    let ctrl_c = tokio::signal::ctrl_c();
    #[cfg(unix)]
    {
        let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("listen for SIGTERM");
        tokio::select! {
            _ = ctrl_c => {}
            _ = sigterm.recv() => {}
        }
        return;
    }
    #[cfg(not(unix))]
    {
        let _ = ctrl_c.await;
    }
}

fn consumer_cutoff(ttl_secs: u64) -> Option<String> {
    if ttl_secs == 0 {
        None
    } else {
        Some((chrono::Utc::now() - chrono::Duration::seconds(ttl_secs as i64)).to_rfc3339())
    }
}

async fn dispatch_loop(
    db: Database,
    config: Arc<Config>,
    subscribers: TopicSubscribers,
    shutdown: CancellationToken,
) {
    let Some(fallback) = config.downstream_url.clone() else {
        shutdown.cancelled().await;
        return;
    };
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.ack_timeout_secs.max(1)))
        .build()
        .expect("http client");
    let visibility = Duration::from_secs(config.ack_timeout_secs.max(1));
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_millis(config.worker_poll_ms)) => {}
        }
        let mut skip = subscribers.topics();
        if let Ok(ephemeral) = db.ephemeral_topics().await {
            for topic in ephemeral {
                if !skip.iter().any(|live| live == &topic) {
                    skip.push(topic);
                }
            }
        }
        let Some(message) = (match db.claim_next_excluding(visibility, &skip).await {
            Ok(message) => message,
            Err(error) => {
                error!(%error, "unable to claim queue message");
                continue;
            }
        }) else {
            continue;
        };
        if subscribers.covers(&message.topic) {
            let _ = db.release(&message.id, &message.lease).await;
            continue;
        }
        let response = tokio::select! {
            _ = shutdown.cancelled() => break,
            response = client.post(&fallback).json(&message.to_downstream()).send() => response,
        };
        match response.and_then(|response| response.error_for_status()) {
            Ok(_) => {
                info!(id = %message.id, topic = %message.topic, url = %fallback, "message delivered");
                if let Err(error) = db.delivered(&message.id, &message.lease).await {
                    error!(%error, "could not mark delivery");
                }
            }
            Err(error) => {
                warn!(id = %message.id, url = %fallback, %error, "message delivery failed");
                let _ = db
                    .failed(
                        &message.id,
                        &message.lease,
                        &error.to_string(),
                        config.max_delivery_attempts,
                    )
                    .await;
            }
        }
    }
}

async fn retention_loop(
    db: Database,
    config: Arc<Config>,
    subscribers: TopicSubscribers,
    shutdown: CancellationToken,
) {
    const SWEEP_SECS: u64 = 60;
    let purge_every = (config.purge_interval_hours > 0)
        .then(|| Duration::from_secs(config.purge_interval_hours.saturating_mul(3600)));
    let mut last_idle_purge: Option<tokio::time::Instant> = None;
    loop {
        match db.ephemeral_topics().await {
            Ok(topics) => {
                for topic in topics {
                    if subscribers.covers(&topic) {
                        continue;
                    }
                    match db.drop_pending(&topic).await {
                        Ok(0) => {}
                        Ok(dropped) => info!(
                            topic = %topic,
                            dropped,
                            "dropped ephemeral messages with no live subscriber"
                        ),
                        Err(error) => {
                            error!(%error, topic = %topic, "unable to drop ephemeral messages")
                        }
                    }
                }
            }
            Err(error) => error!(%error, "unable to list ephemeral topics"),
        }
        if config.message_retention_days > 0 {
            let cutoff = chrono::Utc::now() - chrono::Duration::days(config.message_retention_days);
            match db.purge_older_than(&cutoff.to_rfc3339()).await {
                Ok(0) => {}
                Ok(deleted) => info!(
                    deleted,
                    days = config.message_retention_days,
                    "purged messages older than retention"
                ),
                Err(error) => error!(%error, "unable to purge expired messages"),
            }
        }
        if config.delivered_retention_hours > 0 {
            let cutoff = chrono::Utc::now()
                - chrono::Duration::hours(config.delivered_retention_hours as i64);
            match db.purge_delivered_older_than(&cutoff.to_rfc3339()).await {
                Ok(0) => {}
                Ok(deleted) => info!(
                    deleted,
                    hours = config.delivered_retention_hours,
                    "purged delivered messages older than retention"
                ),
                Err(error) => error!(%error, "unable to purge delivered messages"),
            }
        }
        let due_idle_purge = config.ephemeral_idle_hours > 0
            && last_idle_purge
                .map(|started| {
                    purge_every
                        .map(|every| started.elapsed() >= every)
                        .unwrap_or(true)
                })
                .unwrap_or(true);
        if due_idle_purge {
            let cutoff = chrono::Utc::now()
                - chrono::Duration::hours(config.ephemeral_idle_hours as i64);
            let keep = subscribers.topics();
            match db.purge_idle_ephemeral(&cutoff.to_rfc3339(), &keep).await {
                Ok(0) => {}
                Ok(deleted) => info!(
                    deleted,
                    hours = config.ephemeral_idle_hours,
                    "removed idle ephemeral topics"
                ),
                Err(error) => error!(%error, "unable to purge idle ephemeral topics"),
            }
            last_idle_purge = Some(tokio::time::Instant::now());
        }
        if config.consumer_ttl_secs > 0 {
            if let Some(cutoff) = consumer_cutoff(config.consumer_ttl_secs) {
                match db.purge_stale_consumers(&cutoff).await {
                    Ok(0) => {}
                    Ok(deleted) => info!(deleted, "removed stale consumers"),
                    Err(error) => error!(%error, "unable to purge stale consumers"),
                }
            }
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(Duration::from_secs(SWEEP_SECS)) => {}
        }
    }
}
