mod codec;
mod session;

use std::{collections::HashMap, sync::{Arc, Mutex}};

use axum::{
    extract::{ws::WebSocketUpgrade, ConnectInfo, State},
    response::IntoResponse,
    routing::get,
    Router,
};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};

use crate::{
    config::Config,
    db::Database,
    subscribers::{InflightAcks, TopicSubscribers},
};

#[derive(Clone)]
pub struct MqttCtx {
    pub db: Database,
    pub config: Arc<Config>,
    pub subscribers: TopicSubscribers,
    pub inflight: InflightAcks,
    pub shutdown: CancellationToken,
    pub registry: ClientRegistry,
}

#[derive(Clone, Default)]
pub struct ClientRegistry(Arc<Mutex<HashMap<String, CancellationToken>>>);

impl ClientRegistry {
    pub fn insert(&self, client_id: String, token: CancellationToken) -> Option<CancellationToken> {
        self.0.lock().expect("mqtt registry").insert(client_id, token)
    }

    pub fn remove_if(&self, client_id: &str, token: &CancellationToken) {
        let mut registry = self.0.lock().expect("mqtt registry");
        if registry.get(client_id).is_some_and(|stored| stored == token) {
            registry.remove(client_id);
        }
    }
}

pub async fn serve_tcp(listener: TcpListener, ctx: MqttCtx) -> anyhow::Result<()> {
    let shutdown = ctx.shutdown.clone();
    loop {
        tokio::select! {
            _ = shutdown.cancelled() => break,
            accepted = listener.accept() => {
                let (stream, peer) = accepted?;
                let ctx = ctx.clone();
                tokio::spawn(async move {
                    if let Err(error) = session::run_tcp(stream, peer.to_string(), ctx).await {
                        warn!(%error, %peer, "mqtt tcp session ended");
                    }
                });
            }
        }
    }
    info!("mqtt tcp listener stopped");
    Ok(())
}

pub fn ws_router(ctx: MqttCtx) -> Router {
    Router::new()
        .route("/mqtt", get(ws_handler))
        .route("/", get(ws_handler))
        .with_state(ctx)
}

pub async fn serve_ws(listener: TcpListener, ctx: MqttCtx) -> anyhow::Result<()> {
    let shutdown = ctx.shutdown.clone();
    let app = ws_router(ctx);
    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(async move {
        shutdown.cancelled().await;
    })
    .await?;
    info!("mqtt websocket listener stopped");
    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    peer: Option<ConnectInfo<std::net::SocketAddr>>,
    State(ctx): State<MqttCtx>,
) -> impl IntoResponse {
    let peer = peer
        .map(|ConnectInfo(addr)| addr.to_string())
        .unwrap_or_else(|| "mqtt-ws".into());
    ws.protocols(["mqtt", "mqttv3.1"])
        .on_upgrade(move |socket| async move {
            if let Err(error) = session::run_ws(socket, peer, ctx).await {
                warn!(%error, "mqtt websocket session ended");
            }
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        config::Config,
        delivery::Ingested,
        mqtt::codec::{
            self, encode, ConnAck, Connect, Packet, Publish, SubAck, Subscribe, SubscribeFilter,
            Unsubscribe, CONNACK_ACCEPT, CONNACK_NOT_AUTHORIZED, SUBACK_FAILURE,
        },
    };
    use std::time::Duration;
    use tokio::{io::{AsyncReadExt, AsyncWriteExt}, net::TcpStream};
    use uuid::Uuid;

    async fn temp_ctx() -> (MqttCtx, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!("cangling-broker-mqtt-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let db = Database::connect(&format!("sqlite:{}/queue.db", dir.display()))
            .await
            .unwrap();
        let ctx = MqttCtx {
            db,
            config: Arc::new(Config::test_default()),
            subscribers: TopicSubscribers::default(),
            inflight: InflightAcks::default(),
            shutdown: CancellationToken::new(),
            registry: ClientRegistry::default(),
        };
        (ctx, dir)
    }

    async fn start_broker(ctx: MqttCtx) -> std::net::SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = serve_tcp(listener, ctx).await;
        });
        addr
    }

    async fn write_packet(stream: &mut TcpStream, packet: &Packet) {
        stream.write_all(&encode(packet)).await.unwrap();
        stream.flush().await.unwrap();
    }

    async fn read_packet(stream: &mut TcpStream) -> Packet {
        let mut buf = Vec::new();
        let mut tmp = [0u8; 2048];
        loop {
            if let Some(packet) = codec::decode_one(&mut buf).unwrap() {
                return packet;
            }
            let n = tokio::time::timeout(Duration::from_secs(3), stream.read(&mut tmp))
                .await
                .expect("mqtt test read timeout")
                .unwrap();
            assert!(n > 0, "mqtt test connection closed");
            buf.extend_from_slice(&tmp[..n]);
        }
    }

    async fn connect_ok(stream: &mut TcpStream, client_id: &str, password: Option<&str>) {
        write_packet(
            stream,
            &Packet::Connect(Connect {
                protocol_level: 4,
                clean_session: true,
                keep_alive: 30,
                client_id: client_id.into(),
                username: None,
                password: password.map(|value| value.as_bytes().to_vec()),
            }),
        )
        .await;
        match read_packet(stream).await {
            Packet::ConnAck(ConnAck { code, .. }) => assert_eq!(code, CONNACK_ACCEPT),
            other => panic!("expected CONNACK, got {other:?}"),
        }
    }

    #[test]
    fn password_or_username_matches_token() {
        assert!(session::authorized_for_test(None, None, None));
        assert!(session::authorized_for_test(Some("tok"), None, Some("tok")));
        assert!(session::authorized_for_test(Some("tok"), Some("tok"), None));
        assert!(!session::authorized_for_test(Some("tok"), Some("no"), Some("no")));
        assert!(!session::authorized_for_test(Some("tok"), None, None));
    }

    #[tokio::test]
    async fn rejects_missing_token() {
        let (mut ctx, dir) = temp_ctx().await;
        ctx.config = {
            let mut config = Config::test_default();
            config.auth_token = Some("change-me".into());
            Arc::new(config)
        };
        let addr = start_broker(ctx).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        write_packet(
            &mut stream,
            &Packet::Connect(Connect {
                protocol_level: 4,
                clean_session: true,
                keep_alive: 10,
                client_id: "no-auth".into(),
                username: None,
                password: None,
            }),
        )
        .await;
        match read_packet(&mut stream).await {
            Packet::ConnAck(ConnAck { code, .. }) => assert_eq!(code, CONNACK_NOT_AUTHORIZED),
            other => panic!("{other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn malformed_hash_subscribe_is_rejected() {
        let (ctx, dir) = temp_ctx().await;
        let addr = start_broker(ctx).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut stream, "wild", None).await;
        write_packet(
            &mut stream,
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                filters: vec![
                    SubscribeFilter {
                        topic: "sensor#".into(),
                        qos: 1,
                    },
                    SubscribeFilter {
                        topic: "a/#/b".into(),
                        qos: 1,
                    },
                ],
            }),
        )
        .await;
        match read_packet(&mut stream).await {
            Packet::SubAck(SubAck { packet_id, codes }) => {
                assert_eq!(packet_id, 1);
                assert_eq!(codes, vec![SUBACK_FAILURE, SUBACK_FAILURE]);
            }
            other => panic!("{other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn hash_subscribe_receives_child_topics() {
        let (ctx, dir) = temp_ctx().await;
        let addr = start_broker(ctx).await;

        let mut sub = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut sub, "hash-sub", None).await;
        write_packet(
            &mut sub,
            &Packet::Subscribe(Subscribe {
                packet_id: 2,
                filters: vec![SubscribeFilter {
                    topic: "building/#".into(),
                    qos: 1,
                }],
            }),
        )
        .await;
        match read_packet(&mut sub).await {
            Packet::SubAck(SubAck { codes, .. }) => assert_eq!(codes, vec![1]),
            other => panic!("{other:?}"),
        }

        let mut publisher = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut publisher, "hash-pub", None).await;
        write_packet(
            &mut publisher,
            &Packet::Publish(Publish {
                dup: false,
                qos: 1,
                retain: false,
                topic: "building/floor1/temp".into(),
                packet_id: Some(11),
                payload: b"23.5".to_vec(),
            }),
        )
        .await;
        match read_packet(&mut publisher).await {
            Packet::PubAck { packet_id } => assert_eq!(packet_id, 11),
            other => panic!("{other:?}"),
        }

        match read_packet(&mut sub).await {
            Packet::Publish(publish) => {
                assert_eq!(publish.topic, "building/floor1/temp");
                assert_eq!(publish.payload, b"23.5");
                assert_eq!(publish.qos, 1);
                write_packet(
                    &mut sub,
                    &Packet::PubAck {
                        packet_id: publish.packet_id.expect("qos1"),
                    },
                )
                .await;
            }
            other => panic!("{other:?}"),
        }

        write_packet(
            &mut publisher,
            &Packet::Publish(Publish {
                dup: false,
                qos: 1,
                retain: false,
                topic: "other".into(),
                packet_id: Some(12),
                payload: b"nope".to_vec(),
            }),
        )
        .await;
        match read_packet(&mut publisher).await {
            Packet::PubAck { packet_id } => assert_eq!(packet_id, 12),
            other => panic!("{other:?}"),
        }
        let late = tokio::time::timeout(Duration::from_millis(150), read_packet(&mut sub)).await;
        assert!(late.is_err(), "hash filter must not receive unrelated topics");
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn publish_subscribe_qos1_roundtrip() {
        let (ctx, dir) = temp_ctx().await;
        let addr = start_broker(ctx).await;

        let mut sub = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut sub, "sub-1", None).await;
        write_packet(
            &mut sub,
            &Packet::Subscribe(Subscribe {
                packet_id: 2,
                filters: vec![SubscribeFilter {
                    topic: "demo".into(),
                    qos: 1,
                }],
            }),
        )
        .await;
        match read_packet(&mut sub).await {
            Packet::SubAck(SubAck { codes, .. }) => assert_eq!(codes, vec![1]),
            other => panic!("{other:?}"),
        }

        let mut publisher = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut publisher, "pub-1", None).await;
        write_packet(
            &mut publisher,
            &Packet::Publish(Publish {
                dup: false,
                qos: 1,
                retain: false,
                topic: "demo".into(),
                packet_id: Some(9),
                payload: b"hello-mqtt".to_vec(),
            }),
        )
        .await;
        match read_packet(&mut publisher).await {
            Packet::PubAck { packet_id } => assert_eq!(packet_id, 9),
            other => panic!("{other:?}"),
        }

        match read_packet(&mut sub).await {
            Packet::Publish(publish) => {
                assert_eq!(publish.topic, "demo");
                assert_eq!(publish.payload, b"hello-mqtt");
                assert_eq!(publish.qos, 1);
                let packet_id = publish.packet_id.expect("qos1 packet id");
                write_packet(&mut sub, &Packet::PubAck { packet_id }).await;
            }
            other => panic!("{other:?}"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn unsubscribe_stops_delivery() {
        let (ctx, dir) = temp_ctx().await;
        let addr = start_broker(ctx.clone()).await;
        let mut sub = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut sub, "sub-un", None).await;
        write_packet(
            &mut sub,
            &Packet::Subscribe(Subscribe {
                packet_id: 1,
                filters: vec![SubscribeFilter {
                    topic: "once".into(),
                    qos: 0,
                }],
            }),
        )
        .await;
        let _ = read_packet(&mut sub).await;
        write_packet(
            &mut sub,
            &Packet::Unsubscribe(Unsubscribe {
                packet_id: 2,
                filters: vec!["once".into()],
            }),
        )
        .await;
        match read_packet(&mut sub).await {
            Packet::UnsubAck { packet_id } => assert_eq!(packet_id, 2),
            other => panic!("{other:?}"),
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(ctx.subscribers.count("once"), 0);
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn ping_gets_pong() {
        let (ctx, dir) = temp_ctx().await;
        let addr = start_broker(ctx).await;
        let mut stream = TcpStream::connect(addr).await.unwrap();
        connect_ok(&mut stream, "pinger", None).await;
        write_packet(&mut stream, &Packet::PingReq).await;
        assert!(matches!(read_packet(&mut stream).await, Packet::PingResp));
        let _ = std::fs::remove_dir_all(dir);
    }

    #[tokio::test]
    async fn ingest_queues_for_live_mqtt_topic() {
        let (ctx, dir) = temp_ctx().await;
        ctx.subscribers.add(
            "jobs",
            "mqtt:c1",
            delivery_channel().0,
            "127.0.0.1:1",
            "mqtt",
        );
        let ingested = crate::delivery::ingest(
            &ctx.db,
            &ctx.subscribers,
            "jobs",
            b"x",
            HashMap::new(),
            None,
        )
        .await
        .unwrap();
        match ingested {
            Ingested::Queued { duplicate, .. } => assert!(!duplicate),
            Ingested::Dropped { .. } => panic!("should queue when a subscriber is live"),
        }
        let _ = std::fs::remove_dir_all(dir);
    }

    fn delivery_channel() -> (
        crate::subscribers::StreamSender,
        tokio::sync::mpsc::Receiver<Result<crate::proto::SatwayMessage, tonic::Status>>,
    ) {
        crate::delivery::outgoing_channel()
    }
}
