use std::{net::SocketAddr, time::Duration};

use axum::{extract::State, http::StatusCode, routing::post, Json, Router};
use clap::Parser;
use serde_json::Value;
use tokio_util::sync::CancellationToken;

pub mod proto {
    tonic::include_proto!("dispatcher.v1");
}
use proto::{message_queue_client::MessageQueueClient, RegisterRequest, UnregisterRequest};

#[derive(Parser, Debug)]
#[command(about = "HTTP receiver that registers itself with the broker")]
struct Args {
    #[arg(long, env = "LISTEN_ADDR", default_value = "127.0.0.1:8080")]
    listen_addr: SocketAddr,

    #[arg(long, env = "BROKER_ADDR", default_value = "http://127.0.0.1:7500")]
    broker_addr: String,

    #[arg(long, env = "TOPIC", default_value = "cangling-test")]
    topic: String,

    /// URL the broker should POST to. Defaults to http://<listen-addr>/messages
    #[arg(long, env = "CALLBACK_URL")]
    callback_url: Option<String>,

    #[arg(long, env = "HEARTBEAT_SECS", default_value_t = 15)]
    heartbeat_secs: u64,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let callback_url = args
        .callback_url
        .unwrap_or_else(|| format!("http://{}/messages", args.listen_addr));
    let shutdown = CancellationToken::new();
    let register = tokio::spawn(register_loop(
        args.broker_addr,
        args.topic.clone(),
        callback_url.clone(),
        Duration::from_secs(args.heartbeat_secs.max(1)),
        shutdown.clone(),
    ));

    let app = Router::new()
        .route("/messages", post(receive))
        .with_state(());
    let listener = tokio::net::TcpListener::bind(args.listen_addr).await.unwrap();
    println!(
        "Receiver listening at {callback_url} (topic {})",
        args.topic
    );
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        })
        .await
        .unwrap();
    let _ = register.await;
}

async fn receive(State(()): State<()>, Json(message): Json<Value>) -> StatusCode {
    println!("received: {message}");
    StatusCode::ACCEPTED
}

async fn register_loop(
    broker_addr: String,
    topic: String,
    callback_url: String,
    heartbeat: Duration,
    shutdown: CancellationToken,
) {
    let mut consumer_id = String::new();
    loop {
        match MessageQueueClient::connect(broker_addr.clone()).await {
            Ok(mut client) => match client
                .register(RegisterRequest {
                    topic: topic.clone(),
                    downstream_url: callback_url.clone(),
                    consumer_id: consumer_id.clone(),
                })
                .await
            {
                Ok(response) => {
                    consumer_id = response.into_inner().consumer_id;
                    println!("registered consumer_id={consumer_id}");
                }
                Err(error) => eprintln!("register failed: {error}"),
            },
            Err(error) => eprintln!("broker connect failed: {error}"),
        }
        tokio::select! {
            _ = shutdown.cancelled() => break,
            _ = tokio::time::sleep(heartbeat) => {}
        }
    }
    if consumer_id.is_empty() {
        return;
    }
    if let Ok(mut client) = MessageQueueClient::connect(broker_addr).await {
        let _ = client
            .unregister(UnregisterRequest { consumer_id })
            .await;
    }
}
