use clap::Parser;
use tokio_stream::StreamExt;
use tonic::metadata::MetadataValue;
use tonic::service::Interceptor;
use tonic::transport::Channel;
use tonic::{Request, Status};

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

    #[arg(long, env = "AUTH_TOKEN")]
    token: Option<String>,
}

#[derive(Clone)]
struct TokenInterceptor(Option<MetadataValue<tonic::metadata::Ascii>>);

impl Interceptor for TokenInterceptor {
    fn call(&mut self, mut request: Request<()>) -> Result<Request<()>, Status> {
        if let Some(token) = &self.0 {
            request.metadata_mut().insert("authorization", token.clone());
        }
        Ok(request)
    }
}

#[tokio::main]
async fn main() {
    let args = Args::parse();
    let header = args
        .token
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|token| {
            let value = if token.to_ascii_lowercase().starts_with("bearer ") {
                token.to_string()
            } else {
                format!("Bearer {token}")
            };
            value.parse().expect("AUTH_TOKEN must be ASCII")
        });
    let channel = Channel::from_shared(args.broker_addr.clone())
        .expect("broker url")
        .connect()
        .await
        .expect("broker");
    let mut client = MessageQueueClient::with_interceptor(channel, TokenInterceptor(header));
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
