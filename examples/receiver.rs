use clap::Parser;
use tokio_stream::StreamExt;

pub mod proto {
    tonic::include_proto!("dispatcher.v1");
}
use proto::{
    message_queue_client::MessageQueueClient, AckMessageRequest, RegisterRequest, SubscribeRequest,
    UnregisterRequest,
};

#[derive(Parser, Debug)]
#[command(about = "gRPC stream consumer")]
struct Args {
    #[arg(long, env = "BROKER_ADDR", default_value = "http://127.0.0.1:7500")]
    broker_addr: String,

    #[arg(long, env = "TOPIC", default_value = "cangling-test")]
    topic: String,

    #[arg(long, env = "CONSUMER_NAME", default_value = "receiver")]
    name: String,
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let mut client = MessageQueueClient::connect(args.broker_addr.clone())
        .await
        .expect("broker");
    let registered = client
        .register(RegisterRequest {
            topic: args.topic.clone(),
            consumer_id: String::new(),
            name: args.name.clone(),
            attributes: Default::default(),
        })
        .await
        .expect("register")
        .into_inner();
    let consumer_id = registered.consumer_id;
    println!("registered consumer_id={consumer_id} name={}", args.name);

    let mut stream = client
        .subscribe(SubscribeRequest {
            topic: args.topic.clone(),
            consumer_id: consumer_id.clone(),
        })
        .await
        .expect("subscribe")
        .into_inner();

    while let Some(item) = stream.next().await {
        match item {
            Ok(message) => {
                let payload = String::from_utf8_lossy(&message.payload);
                println!("received {} | {}", message.message_id, payload);
                let _ = client
                    .ack_message(AckMessageRequest {
                        message_id: message.message_id,
                        lease: message.lease,
                        success: true,
                        error: String::new(),
                    })
                    .await;
            }
            Err(error) => {
                eprintln!("stream error: {error}");
                break;
            }
        }
    }

    let _ = client
        .unregister(UnregisterRequest { consumer_id })
        .await;
}
