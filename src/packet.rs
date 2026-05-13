use bytes::Bytes;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProtocolLevel {
    V311 = 4,
    V5 = 5,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Property {
    PayloadFormatIndicator(u8),
    MessageExpiryInterval(u32),
    TopicAlias(u16),
    ResponseTopic(String),
    CorrelationData(Vec<u8>),
    UserProperty(String, String),
    SubscriptionIdentifier(u32),
    ContentType(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MqttPacket {
    Connect(Connect),
    ConnAck(ConnAck),
    Publish(Publish),
    PubAck(PubAck),
    PubRec(PubRec),
    PubRel(PubRel),
    PubComp(PubComp),
    Subscribe(Subscribe),
    SubAck(SubAck),
    Unsubscribe(Unsubscribe),
    UnsubAck(UnsubAck),
    PingReq,
    PingResp,
    Disconnect,
}

impl MqttPacket {
    pub fn packet_type(&self) -> u8 {
        match self {
            MqttPacket::Connect(_) => 1,
            MqttPacket::ConnAck(_) => 2,
            MqttPacket::Publish(_) => 3,
            MqttPacket::PubAck(_) => 4,
            MqttPacket::PubRec(_) => 5,
            MqttPacket::PubRel(_) => 6,
            MqttPacket::PubComp(_) => 7,
            MqttPacket::Subscribe(_) => 8,
            MqttPacket::SubAck(_) => 9,
            MqttPacket::Unsubscribe(_) => 10,
            MqttPacket::UnsubAck(_) => 11,
            MqttPacket::PingReq => 12,
            MqttPacket::PingResp => 13,
            MqttPacket::Disconnect => 14,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Connect {
    pub protocol_level: ProtocolLevel,
    pub client_id: String,
    pub clean_session: bool,
    pub keep_alive: u16,
    // Note: We ignore Will, Username, and Password for now in this MVP edge ingest
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnAck {
    pub session_present: bool,
    pub return_code: u8,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Publish {
    pub dup: bool,
    pub qos: u8,
    pub retain: bool,
    pub topic: String,
    pub packet_id: Option<u16>, // Only present if QoS > 0
    pub properties: Vec<Property>,
    pub payload: Bytes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubAck {
    pub packet_id: u16,
    /// MQTT v5 reason code. Ignored for v3.1.1.
    /// - `None` or `Some(0x00)` = Success
    /// - `Some(0x80)` = Unspecified error
    /// - `Some(0x83)` = Implementation specific error
    /// - `Some(0x97)` = Quota exceeded
    /// - `Some(0x99)` = Payload format invalid
    pub reason_code: Option<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRec {
    pub packet_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubRel {
    pub packet_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PubComp {
    pub packet_id: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Subscribe {
    pub packet_id: u16,
    pub filters: Vec<(String, u8)>, // (topic filter, max qos)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubAck {
    pub packet_id: u16,
    pub return_codes: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unsubscribe {
    pub packet_id: u16,
    pub filters: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsubAck {
    pub packet_id: u16,
}
