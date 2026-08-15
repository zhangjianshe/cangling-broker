mod auth;
mod config;
mod db;
mod logging;
mod model;
mod status;
mod subscribers;

use std::{collections::HashMap, pin::Pin, sync::Arc, time::Duration};

use clap::Parser;
use auth::AuthInterceptor;
use config::Config;
use db::Database;
use subscribers::{InflightAcks, SubscriptionGuard, TopicSubscribers};
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
use crate::model::{DeliveryMode, TopicConfig};

#[derive(Clone)]
struct QueueService {
    db: Database,
    config: Arc<Config>,
    subscribers: TopicSubscribers,
    inflight: InflightAcks,
    shutdown: CancellationToken,
}

type ResponseStream<T> = Pin<Box<dyn tokio_stream::Stream<Item = Result<T, Status>> + Send>>;

fn to_proto_topic(config: TopicConfig) -> ProtoTopicConfig {
    ProtoTopicConfig {
        topic: config.topic,
        delivery: config.delivery.as_str().to_string(),
    }
}

fn attrs_to_map(value: &serde_json::Value) -> HashMap<String, String> {
    value
        .as_object()
        .map(|object| {
            object
                .iter()
                .filter_map(|(key, item)| item.as_str().map(|text| (key.clone(), text.to_string())))
                .collect()
        })
        .unwrap_or_default()
}

#[tonic::async_trait]
impl MessageQueue for QueueService {
    type AcceptMessagesStream = ResponseStream<AcceptMessageResponse>;
    type SubscribeStream = ResponseStream<SatwayMessage>;

    async fn accept_messages(
        &self,
        request: Request<Streaming<AcceptMessageRequest>>,
    ) -> Result<Response<Self::AcceptMessagesStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel(16);
        let db = self.db.clone();
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
                match db
                    .enqueue(
                        Some(&message.idempotency_key),
                        &message.topic,
                        &message.payload,
                        message.attributes,
                    )
                    .await
                {
                    Ok((message_id, duplicate)) => {
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
        let request = request.into_inner();
        if request.topic.trim().is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
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
        let request = request.into_inner();
        if request.topic.trim().is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        let topic = request.topic.trim().to_string();
        let consumer_id = request.consumer_id.trim().to_string();
        if !consumer_id.is_empty() {
            let _ = self.db.touch_consumer(&consumer_id).await;
        }
        let session = if consumer_id.is_empty() {
            Uuid::new_v4().to_string()
        } else {
            consumer_id.clone()
        };
        let (tx, rx) = mpsc::channel(16);
        let db = self.db.clone();
        let inflight = self.inflight.clone();
        let subscribers = self.subscribers.clone();
        let config = self.config.clone();
        let shutdown = self.shutdown.clone();
        tokio::spawn(async move {
            let _guard = SubscriptionGuard::new(
                subscribers.clone(),
                topic.clone(),
                session.clone(),
                tx.clone(),
            );
            info!(topic = %topic, consumer_id = %consumer_id, "subscriber connected");
            let visibility = Duration::from_secs(config.ack_timeout_secs.max(1));
            loop {
                if shutdown.is_cancelled() {
                    break;
                }
                let claimed = match db.claim_next_for_topic(&topic, visibility).await {
                    Ok(claimed) => claimed,
                    Err(error) => {
                        error!(%error, topic = %topic, "unable to claim queue message for subscriber");
                        if !sleep_or_shutdown(&shutdown, config.worker_poll_ms).await {
                            break;
                        }
                        continue;
                    }
                };
                let Some(message) = claimed else {
                    if !consumer_id.is_empty() {
                        let _ = db.touch_consumer(&consumer_id).await;
                    }
                    if !sleep_or_shutdown(&shutdown, config.worker_poll_ms).await {
                        break;
                    }
                    continue;
                };
                let broadcast = db
                    .topic_delivery(&topic)
                    .await
                    .ok()
                    .is_some_and(|mode| mode == DeliveryMode::Broadcast);
                let targets = if broadcast {
                    let mut senders = subscribers.senders(&topic);
                    if senders.is_empty() {
                        senders.push((session.clone(), tx.clone()));
                    }
                    senders
                } else {
                    vec![(session.clone(), tx.clone())]
                };
                let mut waits = Vec::new();
                let mut leases = Vec::new();
                for (_id, sender) in targets {
                    let lease = Uuid::new_v4().to_string();
                    let ack = inflight.register(message.id.clone(), lease.clone());
                    let outgoing = SatwayMessage {
                        message_id: message.id.clone(),
                        topic: message.topic.clone(),
                        payload: message.payload.clone(),
                        attributes: attrs_to_map(&message.attributes),
                        created_at: message.created_at.clone(),
                        lease: lease.clone(),
                    };
                    match tokio::time::timeout(Duration::from_secs(2), sender.send(Ok(outgoing))).await
                    {
                        Ok(Ok(())) => {
                            leases.push(lease);
                            waits.push(ack);
                        }
                        _ => {
                            inflight.cancel(&lease);
                        }
                    }
                }
                if waits.is_empty() {
                    let _ = db
                        .failed(
                            &message.id,
                            &message.lease,
                            "no live subscriber accepted the message",
                            config.max_delivery_attempts,
                        )
                        .await;
                    continue;
                }
                let mut all_ok = true;
                let mut last_error = "ack timeout or subscriber gone".to_string();
                for ack in waits {
                    tokio::select! {
                        _ = shutdown.cancelled() => {
                            all_ok = false;
                            last_error = "broker shutting down".into();
                        }
                        timed = tokio::time::timeout(visibility, ack) => {
                            match timed {
                                Ok(Ok(decision)) if decision.success => {}
                                Ok(Ok(decision)) => {
                                    all_ok = false;
                                    last_error = if decision.error.is_empty() {
                                        "nack".into()
                                    } else {
                                        decision.error
                                    };
                                }
                                Ok(Err(_)) | Err(_) => {
                                    all_ok = false;
                                }
                            }
                        }
                    }
                    if shutdown.is_cancelled() {
                        break;
                    }
                }
                for lease in &leases {
                    inflight.cancel(lease);
                }
                if shutdown.is_cancelled() {
                    break;
                }
                if all_ok {
                    if let Err(error) = db.delivered(&message.id, &message.lease).await {
                        error!(%error, "could not mark delivery");
                    } else {
                        info!(id = %message.id, topic = %topic, "message delivered");
                    }
                } else {
                    let _ = db
                        .failed(
                            &message.id,
                            &message.lease,
                            &last_error,
                            config.max_delivery_attempts,
                        )
                        .await;
                }
            }
            info!(topic = %topic, "subscriber disconnected");
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
            configs.push(TopicConfig {
                topic: topic.to_string(),
                delivery,
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
        built = env!("BUILD_TIME"),
        "cangling-message starting"
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
    let status_addr = config.status_listen_addr();
    let status_listener = tokio::net::TcpListener::bind(status_addr).await?;
    info!(address = %status_addr, "HTTP status listening");
    let status = tokio::spawn(status::serve(
        status_listener,
        db.clone(),
        config.clone(),
        subscribers.clone(),
        shutdown.clone(),
    ));
    let worker = tokio::spawn(dispatch_loop(
        db.clone(),
        config.clone(),
        subscribers.clone(),
        shutdown.clone(),
    ));
    let cleaner = tokio::spawn(retention_loop(db.clone(), config.clone(), shutdown.clone()));
    let address = config.grpc_listen_addr();
    let interceptor = AuthInterceptor::new(config.auth_token.clone());
    if interceptor.enabled() {
        info!(%address, "gRPC intake service listening (CL_MESSAGE_AUTH_TOKEN required)");
    } else {
        info!(%address, "gRPC intake service listening (CL_MESSAGE_AUTH_TOKEN unset, open)");
    }
    Server::builder()
        .add_service(MessageQueueServer::with_interceptor(
            QueueService {
                db,
                config,
                subscribers,
                inflight,
                shutdown: shutdown.clone(),
            },
            interceptor,
        ))
        .serve_with_shutdown(address, async move {
            wait_for_shutdown().await;
            info!("shutdown signal received");
            shutdown.cancel();
        })
        .await?;
    status.await??;
    worker.await?;
    cleaner.await?;
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

async fn sleep_or_shutdown(shutdown: &CancellationToken, poll_ms: u64) -> bool {
    tokio::select! {
        _ = shutdown.cancelled() => false,
        _ = tokio::time::sleep(Duration::from_millis(poll_ms)) => true,
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
        let skip = subscribers.topics();
        let Some(message) = (match db.claim_next_excluding(visibility, &skip).await {
            Ok(message) => message,
            Err(error) => {
                error!(%error, "unable to claim queue message");
                continue;
            }
        }) else {
            continue;
        };
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

async fn retention_loop(db: Database, config: Arc<Config>, shutdown: CancellationToken) {
    const SWEEP_SECS: u64 = 60;
    loop {
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
