mod config;
mod db;
mod model;
mod status;

use std::{sync::Arc, time::Duration};

use clap::Parser;
use config::Config;
use db::Database;
use tokio_util::sync::CancellationToken;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, warn};

pub mod proto {
    tonic::include_proto!("dispatcher.v1");
}
use proto::{
    message_queue_server::{MessageQueue, MessageQueueServer},
    AcceptMessageRequest, AcceptMessageResponse, RegisterRequest, RegisterResponse, UnregisterRequest,
    UnregisterResponse,
};

#[derive(Clone)]
struct QueueService {
    db: Database,
}

fn valid_downstream_url(url: &str) -> bool {
    reqwest::Url::parse(url)
        .ok()
        .is_some_and(|parsed| matches!(parsed.scheme(), "http" | "https") && parsed.host_str().is_some())
}

#[tonic::async_trait]
impl MessageQueue for QueueService {
    async fn accept_message(
        &self,
        request: Request<AcceptMessageRequest>,
    ) -> Result<Response<AcceptMessageResponse>, Status> {
        let message = request.into_inner();
        if message.topic.trim().is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        if message.payload.is_empty() {
            return Err(Status::invalid_argument("payload is required"));
        }
        let (message_id, duplicate) = self
            .db
            .enqueue(
                Some(&message.idempotency_key),
                &message.topic,
                &message.payload,
                message.attributes,
            )
            .await
            .map_err(|error| {
                error!(%error, "queue write failed");
                Status::internal("could not persist message")
            })?;
        Ok(Response::new(AcceptMessageResponse { message_id, duplicate }))
    }

    async fn register(
        &self,
        request: Request<RegisterRequest>,
    ) -> Result<Response<RegisterResponse>, Status> {
        let request = request.into_inner();
        if request.topic.trim().is_empty() {
            return Err(Status::invalid_argument("topic is required"));
        }
        if !valid_downstream_url(&request.downstream_url) {
            return Err(Status::invalid_argument("downstream_url must be an http(s) URL"));
        }
        let consumer_id = self
            .db
            .register_consumer(
                Some(&request.consumer_id),
                request.topic.trim(),
                request.downstream_url.trim(),
            )
            .await
            .map_err(|error| {
                error!(%error, "consumer register failed");
                Status::internal("could not register consumer")
            })?;
        info!(%consumer_id, topic = %request.topic, url = %request.downstream_url, "consumer registered");
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();
    let config = Arc::new(Config::parse());
    let db = Database::connect(&config.database_url).await?;
    let shutdown = CancellationToken::new();
    let status_listener = tokio::net::TcpListener::bind(config.status_listen_addr).await?;
    info!(address = %config.status_listen_addr, "HTTP status listening");
    let status = tokio::spawn(status::serve(
        status_listener,
        db.clone(),
        config.clone(),
        shutdown.clone(),
    ));
    let worker = tokio::spawn(dispatch_loop(db.clone(), config.clone(), shutdown.clone()));
    let cleaner = tokio::spawn(retention_loop(db.clone(), config.clone(), shutdown.clone()));
    let address = config.grpc_listen_addr;
    info!(%address, "gRPC intake service listening");
    Server::builder()
        .add_service(MessageQueueServer::new(QueueService { db }))
        .serve_with_shutdown(address, async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        })
        .await?;
    status.await??;
    worker.await?;
    cleaner.await?;
    Ok(())
}

fn consumer_cutoff(ttl_secs: u64) -> Option<String> {
    if ttl_secs == 0 {
        None
    } else {
        Some((chrono::Utc::now() - chrono::Duration::seconds(ttl_secs as i64)).to_rfc3339())
    }
}

async fn dispatch_loop(db: Database, config: Arc<Config>, shutdown: CancellationToken) {
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
        let cutoff = consumer_cutoff(config.consumer_ttl_secs);
        let allow_without_consumer = config.downstream_url.is_some();
        let Some(message) = (match db
            .claim_next(visibility, cutoff.as_deref(), allow_without_consumer)
            .await
        {
            Ok(message) => message,
            Err(error) => {
                error!(%error, "unable to claim queue message");
                continue;
            }
        }) else {
            continue;
        };
        let destination = match db.pick_consumer(&message.topic, cutoff.as_deref()).await {
            Ok(Some(consumer)) => consumer.downstream_url,
            Ok(None) => match &config.downstream_url {
                Some(url) => url.clone(),
                None => {
                    warn!(id = %message.id, topic = %message.topic, "no consumer registered; retrying later");
                    let _ = db
                        .failed(
                            &message.id,
                            &message.lease,
                            "no consumer registered",
                            config.max_delivery_attempts,
                        )
                        .await;
                    continue;
                }
            },
            Err(error) => {
                error!(%error, id = %message.id, "unable to pick consumer");
                let _ = db
                    .failed(
                        &message.id,
                        &message.lease,
                        "unable to pick consumer",
                        config.max_delivery_attempts,
                    )
                    .await;
                continue;
            }
        };
        match client
            .post(&destination)
            .json(&message.to_downstream())
            .send()
            .await
            .and_then(|response| response.error_for_status())
        {
            Ok(_) => {
                info!(id = %message.id, topic = %message.topic, url = %destination, "message delivered");
                if let Err(error) = db.delivered(&message.id, &message.lease).await {
                    error!(%error, "could not mark delivery");
                }
            }
            Err(error) => {
                warn!(id = %message.id, url = %destination, %error, "message delivery failed");
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
