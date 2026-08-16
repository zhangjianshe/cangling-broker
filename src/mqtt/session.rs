use std::{collections::HashMap, time::Duration};

use anyhow::Context;
use futures_util::{SinkExt, StreamExt};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::mpsc,
};
use tokio_util::sync::CancellationToken;
use tracing::{info, warn};
use uuid::Uuid;

use super::{
    codec::{
        self, Packet, Publish, SubAck, SubscribeFilter, CONNACK_ACCEPT, CONNACK_BAD_AUTH,
        CONNACK_IDENTIFIER, CONNACK_NOT_AUTHORIZED, CONNACK_PROTOCOL, MAX_PACKET_SIZE,
        SUBACK_FAILURE,
    },
    MqttCtx,
};
use crate::{
    auth,
    delivery::{self, Ingested, SubscribeLoop, PROTOCOL_MQTT},
    proto::SatwayMessage,
};

enum Outgoing {
    Tcp(OwnedWriteHalf),
    Ws(futures_util::stream::SplitSink<axum::extract::ws::WebSocket, axum::extract::ws::Message>),
}

impl Outgoing {
    async fn send(&mut self, packet: &Packet) -> anyhow::Result<()> {
        let bytes = codec::encode(packet);
        match self {
            Self::Tcp(writer) => {
                writer.write_all(&bytes).await?;
                writer.flush().await?;
            }
            Self::Ws(sink) => {
                sink.send(axum::extract::ws::Message::Binary(bytes))
                    .await
                    .map_err(|error| anyhow::anyhow!("websocket send: {error}"))?;
            }
        }
        Ok(())
    }
}

pub async fn run_tcp(stream: TcpStream, peer: String, ctx: MqttCtx) -> anyhow::Result<()> {
    let _ = stream.set_nodelay(true);
    let (reader, writer) = stream.into_split();
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(tcp_read_loop(reader, tx));
    run_session(Outgoing::Tcp(writer), rx, peer, ctx).await
}

pub async fn run_ws(socket: axum::extract::ws::WebSocket, peer: String, ctx: MqttCtx) -> anyhow::Result<()> {
    let (sink, stream) = socket.split();
    let (tx, rx) = mpsc::channel(32);
    tokio::spawn(ws_read_loop(stream, tx));
    run_session(Outgoing::Ws(sink), rx, peer, ctx).await
}

async fn tcp_read_loop(mut reader: OwnedReadHalf, tx: mpsc::Sender<Packet>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        match codec::decode_one(&mut buf) {
            Ok(Some(packet)) => {
                if tx.send(packet).await.is_err() {
                    break;
                }
                continue;
            }
            Ok(None) => {}
            Err(_) => break,
        }
        if buf.len() > MAX_PACKET_SIZE + 5 {
            break;
        }
        match reader.read(&mut tmp).await {
            Ok(0) | Err(_) => break,
            Ok(n) => buf.extend_from_slice(&tmp[..n]),
        }
    }
}

async fn ws_read_loop(
    mut stream: futures_util::stream::SplitStream<axum::extract::ws::WebSocket>,
    tx: mpsc::Sender<Packet>,
) {
    let mut buf = Vec::new();
    loop {
        match codec::decode_one(&mut buf) {
            Ok(Some(packet)) => {
                if tx.send(packet).await.is_err() {
                    break;
                }
                continue;
            }
            Ok(None) => {}
            Err(_) => break,
        }
        if buf.len() > MAX_PACKET_SIZE + 5 {
            break;
        }
        match stream.next().await {
            Some(Ok(axum::extract::ws::Message::Binary(data))) => buf.extend_from_slice(&data),
            Some(Ok(axum::extract::ws::Message::Text(text))) => buf.extend_from_slice(text.as_bytes()),
            Some(Ok(axum::extract::ws::Message::Ping(_) | axum::extract::ws::Message::Pong(_))) => {}
            Some(Ok(axum::extract::ws::Message::Close(_))) | None | Some(Err(_)) => break,
        }
    }
}

async fn run_session(
    mut out: Outgoing,
    mut packets: mpsc::Receiver<Packet>,
    peer: String,
    ctx: MqttCtx,
) -> anyhow::Result<()> {
    let first = tokio::time::timeout(Duration::from_secs(15), packets.recv())
        .await
        .context("mqtt CONNECT timeout")?
        .context("mqtt connection closed before CONNECT")?;
    let Packet::Connect(connect) = first else {
        anyhow::bail!("first mqtt packet was not CONNECT");
    };

    if connect.protocol_level != 3 && connect.protocol_level != 4 {
        let _ = out
            .send(&Packet::ConnAck(codec::ConnAck {
                session_present: false,
                code: CONNACK_PROTOCOL,
            }))
            .await;
        anyhow::bail!("unsupported mqtt protocol level {}", connect.protocol_level);
    }

    let client_id = if connect.client_id.trim().is_empty() {
        if !connect.clean_session {
            let _ = out
                .send(&Packet::ConnAck(codec::ConnAck {
                    session_present: false,
                    code: CONNACK_IDENTIFIER,
                }))
                .await;
            anyhow::bail!("empty client id requires clean session");
        }
        format!("mqtt-{}", Uuid::new_v4())
    } else {
        connect.client_id.trim().to_string()
    };

    let expected = auth::normalize(ctx.config.auth_token.as_deref());
    if !mqtt_authorized(expected.as_deref(), &connect) {
        let code = if connect.username.is_none() && connect.password.is_none() {
            CONNACK_NOT_AUTHORIZED
        } else {
            CONNACK_BAD_AUTH
        };
        let _ = out
            .send(&Packet::ConnAck(codec::ConnAck {
                session_present: false,
                code,
            }))
            .await;
        anyhow::bail!("mqtt auth failed for {client_id}");
    }

    out.send(&Packet::ConnAck(codec::ConnAck {
        session_present: false,
        code: CONNACK_ACCEPT,
    }))
    .await?;

    let session_cancel = ctx.shutdown.child_token();
    let previous = ctx.registry.insert(client_id.clone(), session_cancel.clone());
    if let Some(old) = previous {
        old.cancel();
    }

    info!(client_id = %client_id, %peer, "mqtt connected");
    let result = session_loop(
        &mut out,
        &mut packets,
        SessionState {
            client_id: client_id.clone(),
            peer: peer.clone(),
            keep_alive: connect.keep_alive,
            qos_by_topic: HashMap::new(),
            inflight_pub: HashMap::new(),
            next_packet_id: 0,
            subscriptions: HashMap::new(),
        },
        ctx.clone(),
        session_cancel.clone(),
    )
    .await;
    ctx.registry.remove_if(&client_id, &session_cancel);
    session_cancel.cancel();
    info!(client_id = %client_id, %peer, "mqtt disconnected");
    result
}

struct SessionState {
    client_id: String,
    peer: String,
    keep_alive: u16,
    qos_by_topic: HashMap<String, u8>,
    inflight_pub: HashMap<u16, (String, String)>,
    next_packet_id: u16,
    subscriptions: HashMap<String, CancellationToken>,
}

async fn session_loop(
    out: &mut Outgoing,
    packets: &mut mpsc::Receiver<Packet>,
    mut state: SessionState,
    ctx: MqttCtx,
    session_cancel: CancellationToken,
) -> anyhow::Result<()> {
    let (out_tx, mut out_rx) = delivery::outgoing_channel();
    let idle_after = keepalive_idle(state.keep_alive);
    let mut idle = Box::pin(sleep_or_pending(idle_after));

    loop {
        tokio::select! {
            _ = ctx.shutdown.cancelled() => break,
            _ = session_cancel.cancelled() => break,
            incoming = packets.recv() => {
                idle = Box::pin(sleep_or_pending(idle_after));
                let Some(packet) = incoming else {
                    break;
                };
                if handle_packet(out, packet, &mut state, &ctx, &session_cancel, &out_tx).await? {
                    break;
                }
            }
            outgoing = out_rx.recv() => {
                let Some(Ok(message)) = outgoing else {
                    if outgoing.is_none() {
                        break;
                    }
                    continue;
                };
                send_outgoing(out, &mut state, &ctx, message).await?;
            }
            _ = &mut idle => {
                warn!(client_id = %state.client_id, "mqtt keepalive timeout");
                break;
            }
        }
    }
    for token in state.subscriptions.into_values() {
        token.cancel();
    }
    Ok(())
}

async fn handle_packet(
    out: &mut Outgoing,
    packet: Packet,
    state: &mut SessionState,
    ctx: &MqttCtx,
    session_cancel: &CancellationToken,
    out_tx: &tokio::sync::mpsc::Sender<Result<SatwayMessage, tonic::Status>>,
) -> anyhow::Result<bool> {
    match packet {
        Packet::Publish(publish) => {
            accept_publish(out, state, ctx, publish).await?;
        }
        Packet::PubAck { packet_id } => {
            if let Some((message_id, lease)) = state.inflight_pub.remove(&packet_id) {
                let _ = ctx.inflight.complete(&message_id, &lease, true, String::new());
            }
        }
        Packet::PubRel { packet_id } => {
            out.send(&Packet::PubComp { packet_id }).await?;
        }
        Packet::Subscribe(subscribe) => {
            let mut codes = Vec::with_capacity(subscribe.filters.len());
            for filter in subscribe.filters {
                codes.push(subscribe_topic(state, ctx, session_cancel, out_tx, filter).await);
            }
            out.send(&Packet::SubAck(SubAck {
                packet_id: subscribe.packet_id,
                codes,
            }))
            .await?;
        }
        Packet::Unsubscribe(unsubscribe) => {
            for topic in unsubscribe.filters {
                if let Some(token) = state.subscriptions.remove(&topic) {
                    token.cancel();
                }
                state.qos_by_topic.remove(&topic);
            }
            out.send(&Packet::UnsubAck {
                packet_id: unsubscribe.packet_id,
            })
            .await?;
        }
        Packet::PingReq => {
            out.send(&Packet::PingResp).await?;
        }
        Packet::Disconnect => return Ok(true),
        Packet::Connect(_) => anyhow::bail!("duplicate CONNECT"),
        Packet::PubRec { .. } | Packet::PubComp { .. } | Packet::ConnAck(_) | Packet::SubAck(_)
        | Packet::UnsubAck { .. } | Packet::PingResp => {}
    }
    Ok(false)
}

async fn accept_publish(
    out: &mut Outgoing,
    state: &SessionState,
    ctx: &MqttCtx,
    publish: Publish,
) -> anyhow::Result<()> {
    let topic = publish.topic.trim();
    if !crate::topic::is_valid_publish_topic(topic) {
        anyhow::bail!("invalid mqtt publish topic");
    }
    let qos = publish.qos.min(2);
    let packet_id = publish.packet_id;
    let mut attributes = HashMap::new();
    attributes.insert("mqtt_client_id".into(), state.client_id.clone());
    match delivery::ingest(
        &ctx.db,
        &ctx.subscribers,
        topic,
        &publish.payload,
        attributes,
        None,
    )
    .await
    {
        Ok(Ingested::Dropped { message_id }) => {
            info!(
                topic,
                id = %message_id,
                "ephemeral message dropped: no live subscriber"
            );
        }
        Ok(Ingested::Queued { .. }) => {}
        Err(error) => {
            warn!(%error, topic, "mqtt publish enqueue failed");
            anyhow::bail!(error);
        }
    }
    if qos == 2 {
        if let Some(packet_id) = packet_id {
            out.send(&Packet::PubRec { packet_id }).await?;
        }
    } else if qos == 1 {
        if let Some(packet_id) = packet_id {
            out.send(&Packet::PubAck { packet_id }).await?;
        }
    }
    Ok(())
}

async fn subscribe_topic(
    state: &mut SessionState,
    ctx: &MqttCtx,
    session_cancel: &CancellationToken,
    out_tx: &tokio::sync::mpsc::Sender<Result<SatwayMessage, tonic::Status>>,
    filter: SubscribeFilter,
) -> u8 {
    let topic = filter.topic.trim();
    if !crate::topic::is_valid_subscribe_filter(topic) {
        return SUBACK_FAILURE;
    }
    let granted = filter.qos.min(1);
    if let Some(previous) = state.subscriptions.remove(topic) {
        previous.cancel();
    }
    let topic_cancel = session_cancel.child_token();
    state.qos_by_topic.insert(topic.to_string(), granted);
    state
        .subscriptions
        .insert(topic.to_string(), topic_cancel.clone());
    delivery::spawn_subscribe_loop(SubscribeLoop {
        db: ctx.db.clone(),
        config: ctx.config.clone(),
        subscribers: ctx.subscribers.clone(),
        inflight: ctx.inflight.clone(),
        shutdown: topic_cancel,
        topic: topic.to_string(),
        session: format!("mqtt:{}", state.client_id),
        consumer_id: String::new(),
        tx: out_tx.clone(),
        peer: state.peer.clone(),
        protocol: PROTOCOL_MQTT,
    });
    info!(client_id = %state.client_id, topic, qos = granted, "mqtt subscribed");
    granted
}

async fn send_outgoing(
    out: &mut Outgoing,
    state: &mut SessionState,
    ctx: &MqttCtx,
    message: SatwayMessage,
) -> anyhow::Result<()> {
    let qos = granted_qos(&state.qos_by_topic, &message.topic);
    if qos == 0 {
        out.send(&Packet::Publish(Publish {
            dup: false,
            qos: 0,
            retain: false,
            topic: message.topic,
            packet_id: None,
            payload: message.payload,
        }))
        .await?;
        let _ = ctx
            .inflight
            .complete(&message.message_id, &message.lease, true, String::new());
        return Ok(());
    }
    let packet_id = alloc_packet_id(&mut state.next_packet_id, &state.inflight_pub);
    state
        .inflight_pub
        .insert(packet_id, (message.message_id.clone(), message.lease.clone()));
    out.send(&Packet::Publish(Publish {
        dup: false,
        qos: 1,
        retain: false,
        topic: message.topic,
        packet_id: Some(packet_id),
        payload: message.payload,
    }))
    .await
}

fn granted_qos(qos_by_topic: &HashMap<String, u8>, published_topic: &str) -> u8 {
    qos_by_topic
        .iter()
        .filter(|(filter, _)| crate::topic::filter_matches(filter, published_topic))
        .map(|(_, qos)| *qos)
        .max()
        .unwrap_or(0)
        .min(1)
}

fn alloc_packet_id(next: &mut u16, used: &HashMap<u16, (String, String)>) -> u16 {
    for _ in 0..u16::MAX {
        *next = if *next == u16::MAX { 1 } else { *next + 1 };
        if !used.contains_key(next) {
            return *next;
        }
    }
    *next
}

fn mqtt_authorized(expected: Option<&str>, connect: &codec::Connect) -> bool {
    let Some(expected) = expected else {
        return true;
    };
    if let Some(password) = connect.password.as_deref() {
        if let Ok(text) = std::str::from_utf8(password) {
            if auth::tokens_match(expected, Some(text.trim())) {
                return true;
            }
        }
    }
    auth::tokens_match(expected, connect.username.as_deref())
}

fn keepalive_idle(keep_alive: u16) -> Option<Duration> {
    if keep_alive == 0 {
        None
    } else {
        Some(Duration::from_millis(u64::from(keep_alive) * 1500))
    }
}

async fn sleep_or_pending(duration: Option<Duration>) {
    match duration {
        Some(duration) => tokio::time::sleep(duration).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
pub fn authorized_for_test(expected: Option<&str>, username: Option<&str>, password: Option<&str>) -> bool {
    mqtt_authorized(
        expected,
        &codec::Connect {
            protocol_level: 4,
            clean_session: true,
            keep_alive: 0,
            client_id: "t".into(),
            username: username.map(ToOwned::to_owned),
            password: password.map(|value| value.as_bytes().to_vec()),
        },
    )
}
