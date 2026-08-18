use std::fmt;

pub const MAX_PACKET_SIZE: usize = 1024 * 1024;
pub const CONNACK_ACCEPT: u8 = 0;
pub const CONNACK_PROTOCOL: u8 = 1;
pub const CONNACK_IDENTIFIER: u8 = 2;
pub const CONNACK_BAD_AUTH: u8 = 4;
pub const CONNACK_NOT_AUTHORIZED: u8 = 5;
pub const SUBACK_FAILURE: u8 = 0x80;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Packet {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck { packet_id: u16 },
    PubRec { packet_id: u16 },
    PubRel { packet_id: u16 },
    PubComp { packet_id: u16 },
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck { packet_id: u16 },
    PingReq,
    PingResp,
    Disconnect,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub protocol_level: u8,
    pub clean_session: bool,
    pub keep_alive: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

impl Connect {
    pub fn protocol_version(&self) -> &'static str {
        match self.protocol_level {
            3 => "3.1",
            4 => "3.1.1",
            _ => "",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub code: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>,
    pub payload: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub packet_id: u16,
    pub filters: Vec<SubscribeFilter>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscribeFilter {
    pub topic: String,
    pub qos: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    pub packet_id: u16,
    pub codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub filters: Vec<String>,
}

#[derive(Debug)]
pub enum CodecError {
    Malformed(&'static str),
    TooLarge,
}

impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Malformed(reason) => write!(f, "malformed mqtt packet: {reason}"),
            Self::TooLarge => write!(f, "mqtt packet exceeds {MAX_PACKET_SIZE} bytes"),
        }
    }
}

impl std::error::Error for CodecError {}

/// Pull one packet from the front of `buf`. Leaves unread bytes in place.
/// `Ok(None)` means more bytes are needed.
pub fn decode_one(buf: &mut Vec<u8>) -> Result<Option<Packet>, CodecError> {
    if buf.is_empty() {
        return Ok(None);
    }
    let first = buf[0];
    let (remaining, header_len) = match decode_remaining_length(&buf[1..])? {
        Some(parsed) => parsed,
        None => return Ok(None),
    };
    if remaining > MAX_PACKET_SIZE {
        return Err(CodecError::TooLarge);
    }
    let total = header_len + remaining;
    if buf.len() < total {
        return Ok(None);
    }
    let packet_type = first >> 4;
    let flags = first & 0x0f;
    let payload = buf[header_len..total].to_vec();
    buf.drain(..total);
    Ok(Some(decode_payload(packet_type, flags, &payload)?))
}

pub fn encode(packet: &Packet) -> Vec<u8> {
    match packet {
        Packet::Connect(connect) => encode_connect(connect),
        Packet::ConnAck(ack) => wrap(2, 0, &[u8::from(ack.session_present), ack.code]),
        Packet::Publish(publish) => encode_publish(publish),
        Packet::PubAck { packet_id } => wrap(4, 0, &u16_bytes(*packet_id)),
        Packet::PubRec { packet_id } => wrap(5, 0, &u16_bytes(*packet_id)),
        Packet::PubRel { packet_id } => wrap(6, 0x02, &u16_bytes(*packet_id)),
        Packet::PubComp { packet_id } => wrap(7, 0, &u16_bytes(*packet_id)),
        Packet::Subscribe(sub) => encode_subscribe(sub),
        Packet::SubAck(ack) => {
            let mut body = u16_bytes(ack.packet_id).to_vec();
            body.extend_from_slice(&ack.codes);
            wrap(9, 0, &body)
        }
        Packet::Unsubscribe(unsub) => encode_unsubscribe(unsub),
        Packet::UnsubAck { packet_id } => wrap(11, 0, &u16_bytes(*packet_id)),
        Packet::PingReq => wrap(12, 0, &[]),
        Packet::PingResp => wrap(13, 0, &[]),
        Packet::Disconnect => wrap(14, 0, &[]),
    }
}

fn decode_payload(packet_type: u8, flags: u8, payload: &[u8]) -> Result<Packet, CodecError> {
    match packet_type {
        1 => {
            if flags != 0 {
                return Err(CodecError::Malformed("CONNECT flags"));
            }
            Ok(Packet::Connect(decode_connect(payload)?))
        }
        2 => {
            if payload.len() != 2 {
                return Err(CodecError::Malformed("CONNACK length"));
            }
            Ok(Packet::ConnAck(ConnAck {
                session_present: payload[0] & 0x01 != 0,
                code: payload[1],
            }))
        }
        3 => Ok(Packet::Publish(decode_publish(flags, payload)?)),
        4 => Ok(Packet::PubAck {
            packet_id: require_packet_id(payload)?,
        }),
        5 => Ok(Packet::PubRec {
            packet_id: require_packet_id(payload)?,
        }),
        6 => {
            if flags != 0x02 {
                return Err(CodecError::Malformed("PUBREL flags"));
            }
            Ok(Packet::PubRel {
                packet_id: require_packet_id(payload)?,
            })
        }
        7 => Ok(Packet::PubComp {
            packet_id: require_packet_id(payload)?,
        }),
        8 => {
            if flags != 0x02 {
                return Err(CodecError::Malformed("SUBSCRIBE flags"));
            }
            Ok(Packet::Subscribe(decode_subscribe(payload)?))
        }
        9 => {
            if payload.len() < 2 {
                return Err(CodecError::Malformed("SUBACK"));
            }
            Ok(Packet::SubAck(SubAck {
                packet_id: u16::from_be_bytes([payload[0], payload[1]]),
                codes: payload[2..].to_vec(),
            }))
        }
        10 => {
            if flags != 0x02 {
                return Err(CodecError::Malformed("UNSUBSCRIBE flags"));
            }
            Ok(Packet::Unsubscribe(decode_unsubscribe(payload)?))
        }
        11 => Ok(Packet::UnsubAck {
            packet_id: require_packet_id(payload)?,
        }),
        12 => {
            if !payload.is_empty() {
                return Err(CodecError::Malformed("PINGREQ"));
            }
            Ok(Packet::PingReq)
        }
        13 => {
            if !payload.is_empty() {
                return Err(CodecError::Malformed("PINGRESP"));
            }
            Ok(Packet::PingResp)
        }
        14 => {
            if !payload.is_empty() {
                return Err(CodecError::Malformed("DISCONNECT"));
            }
            Ok(Packet::Disconnect)
        }
        _ => Err(CodecError::Malformed("unknown packet type")),
    }
}

fn decode_connect(payload: &[u8]) -> Result<Connect, CodecError> {
    let mut offset = 0;
    let protocol = read_mqtt_string(payload, &mut offset)?;
    let level = read_u8(payload, &mut offset)?;
    match (protocol.as_str(), level) {
        ("MQTT", 4) | ("MQIsdp", 3) => {}
        _ => return Err(CodecError::Malformed("unsupported protocol")),
    }
    let flags = read_u8(payload, &mut offset)?;
    if flags & 0x01 != 0 {
        return Err(CodecError::Malformed("CONNECT reserved flag"));
    }
    let keep_alive = read_u16(payload, &mut offset)?;
    let client_id = read_mqtt_string(payload, &mut offset)?;
    if flags & 0x04 != 0 {
        let _will_topic = read_mqtt_string(payload, &mut offset)?;
        let _will_payload = read_mqtt_bytes(payload, &mut offset)?;
    }
    let username = if flags & 0x80 != 0 {
        Some(read_mqtt_string(payload, &mut offset)?)
    } else {
        None
    };
    let password = if flags & 0x40 != 0 {
        Some(read_mqtt_bytes(payload, &mut offset)?)
    } else {
        None
    };
    Ok(Connect {
        protocol_level: level,
        clean_session: flags & 0x02 != 0,
        keep_alive,
        client_id,
        username,
        password,
    })
}

fn decode_publish(flags: u8, payload: &[u8]) -> Result<Publish, CodecError> {
    let qos = (flags >> 1) & 0x03;
    if qos > 2 {
        return Err(CodecError::Malformed("PUBLISH QoS"));
    }
    let mut offset = 0;
    let topic = read_mqtt_string(payload, &mut offset)?;
    let packet_id = if qos > 0 {
        Some(read_u16(payload, &mut offset)?)
    } else {
        None
    };
    Ok(Publish {
        dup: flags & 0x08 != 0,
        qos,
        retain: flags & 0x01 != 0,
        topic,
        packet_id,
        payload: payload[offset..].to_vec(),
    })
}

fn decode_subscribe(payload: &[u8]) -> Result<Subscribe, CodecError> {
    if payload.len() < 2 {
        return Err(CodecError::Malformed("SUBSCRIBE"));
    }
    let mut offset = 0;
    let packet_id = read_u16(payload, &mut offset)?;
    let mut filters = Vec::new();
    while offset < payload.len() {
        let topic = read_mqtt_string(payload, &mut offset)?;
        let qos = read_u8(payload, &mut offset)?;
        if qos > 2 {
            return Err(CodecError::Malformed("SUBSCRIBE QoS"));
        }
        filters.push(SubscribeFilter { topic, qos });
    }
    if filters.is_empty() {
        return Err(CodecError::Malformed("SUBSCRIBE has no filters"));
    }
    Ok(Subscribe { packet_id, filters })
}

fn decode_unsubscribe(payload: &[u8]) -> Result<Unsubscribe, CodecError> {
    if payload.len() < 2 {
        return Err(CodecError::Malformed("UNSUBSCRIBE"));
    }
    let mut offset = 0;
    let packet_id = read_u16(payload, &mut offset)?;
    let mut filters = Vec::new();
    while offset < payload.len() {
        filters.push(read_mqtt_string(payload, &mut offset)?);
    }
    if filters.is_empty() {
        return Err(CodecError::Malformed("UNSUBSCRIBE has no filters"));
    }
    Ok(Unsubscribe { packet_id, filters })
}

fn encode_connect(connect: &Connect) -> Vec<u8> {
    let mut body = Vec::new();
    if connect.protocol_level == 3 {
        push_mqtt_string(&mut body, "MQIsdp");
        body.push(3);
    } else {
        push_mqtt_string(&mut body, "MQTT");
        body.push(4);
    }
    let mut flags = 0u8;
    if connect.clean_session {
        flags |= 0x02;
    }
    if connect.username.is_some() {
        flags |= 0x80;
    }
    if connect.password.is_some() {
        flags |= 0x40;
    }
    body.push(flags);
    body.extend_from_slice(&u16_bytes(connect.keep_alive));
    push_mqtt_string(&mut body, &connect.client_id);
    if let Some(username) = &connect.username {
        push_mqtt_string(&mut body, username);
    }
    if let Some(password) = &connect.password {
        push_mqtt_bytes(&mut body, password);
    }
    wrap(1, 0, &body)
}

fn encode_publish(publish: &Publish) -> Vec<u8> {
    let qos = publish.qos.min(2);
    let mut flags = 0u8;
    if publish.dup {
        flags |= 0x08;
    }
    flags |= qos << 1;
    if publish.retain {
        flags |= 0x01;
    }
    let mut body = Vec::new();
    push_mqtt_string(&mut body, &publish.topic);
    if qos > 0 {
        let packet_id = publish.packet_id.unwrap_or(1);
        body.extend_from_slice(&u16_bytes(packet_id));
    }
    body.extend_from_slice(&publish.payload);
    wrap(3, flags, &body)
}

fn encode_subscribe(sub: &Subscribe) -> Vec<u8> {
    let mut body = u16_bytes(sub.packet_id).to_vec();
    for filter in &sub.filters {
        push_mqtt_string(&mut body, &filter.topic);
        body.push(filter.qos.min(2));
    }
    wrap(8, 0x02, &body)
}

fn encode_unsubscribe(unsub: &Unsubscribe) -> Vec<u8> {
    let mut body = u16_bytes(unsub.packet_id).to_vec();
    for filter in &unsub.filters {
        push_mqtt_string(&mut body, filter);
    }
    wrap(10, 0x02, &body)
}

fn wrap(packet_type: u8, flags: u8, payload: &[u8]) -> Vec<u8> {
    let mut out = vec![(packet_type << 4) | (flags & 0x0f)];
    encode_remaining_length(payload.len(), &mut out);
    out.extend_from_slice(payload);
    out
}

fn decode_remaining_length(buf: &[u8]) -> Result<Option<(usize, usize)>, CodecError> {
    let mut value = 0usize;
    let mut multiplier = 1usize;
    for (index, byte) in buf.iter().copied().enumerate() {
        if index >= 4 {
            return Err(CodecError::Malformed("remaining length"));
        }
        value += (byte & 0x7f) as usize * multiplier;
        if byte & 0x80 == 0 {
            return Ok(Some((value, index + 1 + 1)));
        }
        multiplier = multiplier
            .checked_mul(128)
            .ok_or(CodecError::Malformed("remaining length"))?;
    }
    Ok(None)
}

fn encode_remaining_length(mut len: usize, out: &mut Vec<u8>) {
    if len == 0 {
        out.push(0);
        return;
    }
    while len > 0 {
        let mut byte = (len % 128) as u8;
        len /= 128;
        if len > 0 {
            byte |= 0x80;
        }
        out.push(byte);
    }
}

fn require_packet_id(payload: &[u8]) -> Result<u16, CodecError> {
    if payload.len() != 2 {
        return Err(CodecError::Malformed("packet id"));
    }
    Ok(u16::from_be_bytes([payload[0], payload[1]]))
}

fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, CodecError> {
    let value = *data
        .get(*offset)
        .ok_or(CodecError::Malformed("truncated"))?;
    *offset += 1;
    Ok(value)
}

fn read_u16(data: &[u8], offset: &mut usize) -> Result<u16, CodecError> {
    if *offset + 2 > data.len() {
        return Err(CodecError::Malformed("truncated u16"));
    }
    let value = u16::from_be_bytes([data[*offset], data[*offset + 1]]);
    *offset += 2;
    Ok(value)
}

fn read_mqtt_bytes(data: &[u8], offset: &mut usize) -> Result<Vec<u8>, CodecError> {
    let len = read_u16(data, offset)? as usize;
    if *offset + len > data.len() {
        return Err(CodecError::Malformed("truncated bytes"));
    }
    let bytes = data[*offset..*offset + len].to_vec();
    *offset += len;
    Ok(bytes)
}

fn read_mqtt_string(data: &[u8], offset: &mut usize) -> Result<String, CodecError> {
    let bytes = read_mqtt_bytes(data, offset)?;
    String::from_utf8(bytes).map_err(|_| CodecError::Malformed("utf-8"))
}

fn push_mqtt_string(buf: &mut Vec<u8>, value: &str) {
    push_mqtt_bytes(buf, value.as_bytes());
}

fn push_mqtt_bytes(buf: &mut Vec<u8>, value: &[u8]) {
    let len = u16::try_from(value.len()).unwrap_or(u16::MAX);
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(&value[..len as usize]);
}

fn u16_bytes(value: u16) -> [u8; 2] {
    value.to_be_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(packet: Packet) -> Packet {
        let encoded = encode(&packet);
        let mut buf = encoded;
        decode_one(&mut buf).unwrap().unwrap()
    }

    #[test]
    fn remaining_length_boundaries() {
        let mut out = Vec::new();
        encode_remaining_length(127, &mut out);
        assert_eq!(out, vec![127]);
        out.clear();
        encode_remaining_length(128, &mut out);
        assert_eq!(out, vec![0x80, 1]);
        out.clear();
        encode_remaining_length(16383, &mut out);
        assert_eq!(out, vec![0xff, 0x7f]);
    }

    #[test]
    fn decode_needs_more_bytes() {
        let mut buf = vec![0x30, 0x05, b'h'];
        assert!(decode_one(&mut buf).unwrap().is_none());
        assert_eq!(buf.len(), 3);
    }

    #[test]
    fn connect_roundtrip() {
        let packet = Packet::Connect(Connect {
            protocol_level: 4,
            clean_session: true,
            keep_alive: 30,
            client_id: "dev-1".into(),
            username: Some("user".into()),
            password: Some(b"secret".to_vec()),
        });
        match roundtrip(packet) {
            Packet::Connect(connect) => {
                assert_eq!(connect.client_id, "dev-1");
                assert_eq!(connect.protocol_version(), "3.1.1");
                assert_eq!(connect.keep_alive, 30);
                assert_eq!(connect.username.as_deref(), Some("user"));
                assert_eq!(connect.password.as_deref(), Some(b"secret".as_slice()));
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn publish_qos1_roundtrip() {
        let packet = Packet::Publish(Publish {
            dup: false,
            qos: 1,
            retain: false,
            topic: "sensor/temp".into(),
            packet_id: Some(7),
            payload: b"22.5".to_vec(),
        });
        match roundtrip(packet) {
            Packet::Publish(publish) => {
                assert_eq!(publish.topic, "sensor/temp");
                assert_eq!(publish.qos, 1);
                assert_eq!(publish.packet_id, Some(7));
                assert_eq!(publish.payload, b"22.5");
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn subscribe_roundtrip() {
        let packet = Packet::Subscribe(Subscribe {
            packet_id: 9,
            filters: vec![SubscribeFilter {
                topic: "jobs".into(),
                qos: 1,
            }],
        });
        match roundtrip(packet) {
            Packet::Subscribe(sub) => {
                assert_eq!(sub.packet_id, 9);
                assert_eq!(sub.filters[0].topic, "jobs");
                assert_eq!(sub.filters[0].qos, 1);
            }
            other => panic!("{other:?}"),
        }
    }

    #[test]
    fn ping_and_disconnect_roundtrip() {
        assert!(matches!(roundtrip(Packet::PingReq), Packet::PingReq));
        assert!(matches!(roundtrip(Packet::PingResp), Packet::PingResp));
        assert!(matches!(roundtrip(Packet::Disconnect), Packet::Disconnect));
    }

    #[test]
    fn two_packets_in_one_buffer() {
        let mut buf = encode(&Packet::PingReq);
        buf.extend_from_slice(&encode(&Packet::PingResp));
        assert!(matches!(decode_one(&mut buf).unwrap(), Some(Packet::PingReq)));
        assert!(matches!(decode_one(&mut buf).unwrap(), Some(Packet::PingResp)));
        assert!(buf.is_empty());
    }
}
