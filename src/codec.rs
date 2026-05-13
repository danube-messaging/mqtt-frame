use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::MqttError;
use crate::packet::{
    Connect, MqttPacket, Property, ProtocolLevel, PubAck, PubComp, PubRec, PubRel, Publish, SubAck,
    Subscribe, UnsubAck, Unsubscribe,
};
use crate::utils::read_var_int;

pub struct MqttCodec {
    /// Tracks the protocol level negotiated during CONNECT.
    /// Defaults to V311 for the first packet.
    pub protocol_level: ProtocolLevel,
}

impl Default for MqttCodec {
    fn default() -> Self {
        Self::new()
    }
}

impl MqttCodec {
    pub fn new() -> Self {
        Self {
            protocol_level: ProtocolLevel::V311,
        }
    }
}

impl Decoder for MqttCodec {
    type Item = MqttPacket;
    type Error = MqttError;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if src.is_empty() {
            return Ok(None);
        }

        let mut cursor = Cursor::new(&src[..]);
        let fixed_header = cursor.get_u8();
        let packet_type = fixed_header >> 4;
        let flags = fixed_header & 0x0F;

        let var_int_result = read_var_int(&mut cursor)?;
        let remaining_length = match var_int_result {
            Some((len, _)) => len as usize,
            None => return Ok(None), // Not enough data for length
        };

        let header_len = cursor.position() as usize;
        let total_len = header_len + remaining_length;

        if src.len() < total_len {
            src.reserve(total_len - src.len());
            return Ok(None); // Wait for more data
        }

        // We have the full packet. Let's slice it out using zero-copy.
        let packet_bytes = src.split_to(total_len).freeze();

        let mut payload_cursor = Cursor::new(&packet_bytes[header_len..]);

        let packet = match packet_type {
            1 => {
                // CONNECT
                let protocol_name_len = payload_cursor.get_u16() as usize;
                let mut protocol_name = vec![0; protocol_name_len];
                payload_cursor.copy_to_slice(&mut protocol_name);

                let protocol_level_byte = payload_cursor.get_u8();
                let protocol_level = match protocol_level_byte {
                    4 => ProtocolLevel::V311,
                    5 => ProtocolLevel::V5,
                    _ => return Err(MqttError::UnsupportedVersion),
                };

                let connect_flags = payload_cursor.get_u8();
                let clean_session = (connect_flags & 0x02) != 0;
                let keep_alive = payload_cursor.get_u16();

                // Properties (if v5)
                if protocol_level == ProtocolLevel::V5 {
                    if let Some((props_len, _)) = read_var_int(&mut payload_cursor)? {
                        payload_cursor.advance(props_len as usize); // Skip properties for now
                    } else {
                        return Err(MqttError::MalformedPacket("Incomplete v5 properties"));
                    }
                }

                // Client ID
                let client_id_len = payload_cursor.get_u16() as usize;
                let mut client_id_bytes = vec![0; client_id_len];
                payload_cursor.copy_to_slice(&mut client_id_bytes);
                let client_id = String::from_utf8_lossy(&client_id_bytes).to_string();

                // Self-update the codec's protocol level for subsequent packets!
                self.protocol_level = protocol_level;

                MqttPacket::Connect(Connect {
                    protocol_level,
                    client_id,
                    clean_session,
                    keep_alive,
                })
            }
            3 => {
                // PUBLISH
                let dup = (flags & 0x08) != 0;
                let qos = (flags & 0x06) >> 1;
                let retain = (flags & 0x01) != 0;

                let topic_len = payload_cursor.get_u16() as usize;
                let mut topic_bytes = vec![0; topic_len];
                payload_cursor.copy_to_slice(&mut topic_bytes);
                let topic = String::from_utf8_lossy(&topic_bytes).to_string();

                let packet_id = if qos > 0 {
                    Some(payload_cursor.get_u16())
                } else {
                    None
                };

                let mut properties = Vec::new();

                // If V5, parse properties before extracting payload
                if self.protocol_level == ProtocolLevel::V5 {
                    if let Some((props_len, _)) = read_var_int(&mut payload_cursor)? {
                        let props_end = payload_cursor.position() as usize + props_len as usize;
                        if total_len < header_len + props_end {
                            return Err(MqttError::MalformedPacket(
                                "Properties length exceeds packet",
                            ));
                        }
                        properties = parse_properties(&mut payload_cursor, props_len as usize)?;
                    } else {
                        return Err(MqttError::MalformedPacket(
                            "Incomplete v5 properties in PUBLISH",
                        ));
                    }
                }

                // Payload is the rest of the packet
                let payload_start = header_len + payload_cursor.position() as usize;
                let payload = packet_bytes.slice(payload_start..total_len);

                MqttPacket::Publish(Publish {
                    dup,
                    qos,
                    retain,
                    topic,
                    packet_id,
                    properties,
                    payload,
                })
            }
            4 => {
                let packet_id = payload_cursor.get_u16();
                // MQTT v5: reason code follows packet_id if remaining_length > 2
                let reason_code =
                    if self.protocol_level == ProtocolLevel::V5 && remaining_length > 2 {
                        Some(payload_cursor.get_u8())
                    } else {
                        None
                    };
                MqttPacket::PubAck(PubAck {
                    packet_id,
                    reason_code,
                })
            }
            5 => MqttPacket::PubRec(PubRec {
                packet_id: payload_cursor.get_u16(),
            }),
            6 => MqttPacket::PubRel(PubRel {
                packet_id: payload_cursor.get_u16(),
            }),
            7 => MqttPacket::PubComp(PubComp {
                packet_id: payload_cursor.get_u16(),
            }),
            8 => {
                // SUBSCRIBE
                let packet_id = payload_cursor.get_u16();
                let mut filters = Vec::new();
                while payload_cursor.has_remaining() {
                    let topic_len = payload_cursor.get_u16() as usize;
                    let mut topic_bytes = vec![0; topic_len];
                    payload_cursor.copy_to_slice(&mut topic_bytes);
                    let topic = String::from_utf8_lossy(&topic_bytes).to_string();
                    let qos = payload_cursor.get_u8();
                    filters.push((topic, qos));
                }
                MqttPacket::Subscribe(Subscribe { packet_id, filters })
            }
            9 => {
                // SUBACK
                let packet_id = payload_cursor.get_u16();
                let mut return_codes = Vec::new();
                while payload_cursor.has_remaining() {
                    return_codes.push(payload_cursor.get_u8());
                }
                MqttPacket::SubAck(SubAck {
                    packet_id,
                    return_codes,
                })
            }
            10 => {
                // UNSUBSCRIBE
                let packet_id = payload_cursor.get_u16();
                let mut filters = Vec::new();
                while payload_cursor.has_remaining() {
                    let topic_len = payload_cursor.get_u16() as usize;
                    let mut topic_bytes = vec![0; topic_len];
                    payload_cursor.copy_to_slice(&mut topic_bytes);
                    filters.push(String::from_utf8_lossy(&topic_bytes).to_string());
                }
                MqttPacket::Unsubscribe(Unsubscribe { packet_id, filters })
            }
            11 => MqttPacket::UnsubAck(UnsubAck {
                packet_id: payload_cursor.get_u16(),
            }),
            12 => MqttPacket::PingReq,
            13 => MqttPacket::PingResp,
            14 => MqttPacket::Disconnect,
            _ => {
                return Err(MqttError::ProtocolError(format!(
                    "Unsupported packet type: {}",
                    packet_type
                )))
            }
        };

        Ok(Some(packet))
    }
}

impl Encoder<MqttPacket> for MqttCodec {
    type Error = MqttError;

    fn encode(&mut self, item: MqttPacket, dst: &mut BytesMut) -> Result<(), Self::Error> {
        match item {
            MqttPacket::ConnAck(connack) => {
                dst.put_u8(0x20); // Type 2 (CONNACK)
                dst.put_u8(2); // Remaining length is always 2 for v3.1.1
                dst.put_u8(if connack.session_present { 1 } else { 0 });
                dst.put_u8(connack.return_code);
            }
            MqttPacket::PingResp => {
                dst.put_u8(0xD0); // Type 13 (PINGRESP)
                dst.put_u8(0); // Remaining length is 0
            }
            MqttPacket::PubAck(puback) => {
                dst.put_u8(0x40);
                if self.protocol_level == ProtocolLevel::V5 {
                    let reason = puback.reason_code.unwrap_or(0x00);
                    if reason == 0x00 {
                        // MQTT v5 §3.4.2.1: if Reason Code is 0x00 and there
                        // are no Properties, the Reason Code and Property
                        // Length can be omitted (short form).
                        dst.put_u8(2); // Remaining length: 2 (packet_id only)
                        dst.put_u16(puback.packet_id);
                    } else {
                        // Non-success: must include reason code + empty properties
                        dst.put_u8(4); // Remaining length: 2 (ID) + 1 (reason) + 1 (props=0)
                        dst.put_u16(puback.packet_id);
                        dst.put_u8(reason);
                        dst.put_u8(0); // 0 Properties
                    }
                } else {
                    // MQTT v3.1.1: no reason code
                    dst.put_u8(2); // Remaining length: 2 (ID)
                    dst.put_u16(puback.packet_id);
                }
            }
            MqttPacket::PubRec(pubrec) => {
                dst.put_u8(0x50);
                dst.put_u8(2);
                dst.put_u16(pubrec.packet_id);
            }
            MqttPacket::PubRel(pubrel) => {
                dst.put_u8(0x62);
                dst.put_u8(2);
                dst.put_u16(pubrel.packet_id);
            }
            MqttPacket::PubComp(pubcomp) => {
                dst.put_u8(0x70);
                dst.put_u8(2);
                dst.put_u16(pubcomp.packet_id);
            }
            MqttPacket::SubAck(suback) => {
                dst.put_u8(0x90);
                // Length is 2 (packet id) + number of return codes + property length (if v5)
                let props_len = if self.protocol_level == ProtocolLevel::V5 {
                    1
                } else {
                    0
                };
                let remaining_len = 2 + suback.return_codes.len() as u32 + props_len;
                crate::utils::write_var_int(remaining_len, dst)?;
                dst.put_u16(suback.packet_id);

                if self.protocol_level == ProtocolLevel::V5 {
                    dst.put_u8(0); // 0 Properties
                }

                for rc in suback.return_codes {
                    dst.put_u8(rc);
                }
            }
            MqttPacket::UnsubAck(unsuback) => {
                dst.put_u8(0xB0);
                dst.put_u8(2);
                dst.put_u16(unsuback.packet_id);
            }
            MqttPacket::PingReq => {
                dst.put_u8(0xC0);
                dst.put_u8(0);
            }

            MqttPacket::Disconnect => {
                dst.put_u8(0xE0);
                dst.put_u8(0);
            }
            _ => {
                return Err(MqttError::ProtocolError(
                    "Packet encoding not implemented for this type".into(),
                ))
            }
        }
        Ok(())
    }
}

pub fn parse_properties(
    cursor: &mut Cursor<&[u8]>,
    length: usize,
) -> Result<Vec<Property>, MqttError> {
    let mut properties = Vec::new();
    let start_pos = cursor.position() as usize;

    while (cursor.position() as usize - start_pos) < length {
        if let Some((identifier, _)) = read_var_int(cursor)? {
            match identifier {
                0x01 => properties.push(Property::PayloadFormatIndicator(cursor.get_u8())),
                0x02 => properties.push(Property::MessageExpiryInterval(cursor.get_u32())),
                0x03 => {
                    let str_len = cursor.get_u16() as usize;
                    let mut str_bytes = vec![0; str_len];
                    cursor.copy_to_slice(&mut str_bytes);
                    properties.push(Property::ContentType(
                        String::from_utf8_lossy(&str_bytes).to_string(),
                    ));
                }
                0x08 => {
                    let str_len = cursor.get_u16() as usize;
                    let mut str_bytes = vec![0; str_len];
                    cursor.copy_to_slice(&mut str_bytes);
                    properties.push(Property::ResponseTopic(
                        String::from_utf8_lossy(&str_bytes).to_string(),
                    ));
                }
                0x09 => {
                    let bin_len = cursor.get_u16() as usize;
                    let mut bin_bytes = vec![0; bin_len];
                    cursor.copy_to_slice(&mut bin_bytes);
                    properties.push(Property::CorrelationData(bin_bytes));
                }
                0x0B => {
                    if let Some((sub_id, _)) = read_var_int(cursor)? {
                        properties.push(Property::SubscriptionIdentifier(sub_id));
                    }
                }
                0x23 => properties.push(Property::TopicAlias(cursor.get_u16())),
                0x26 => {
                    let k_len = cursor.get_u16() as usize;
                    let mut k_bytes = vec![0; k_len];
                    cursor.copy_to_slice(&mut k_bytes);
                    let v_len = cursor.get_u16() as usize;
                    let mut v_bytes = vec![0; v_len];
                    cursor.copy_to_slice(&mut v_bytes);
                    properties.push(Property::UserProperty(
                        String::from_utf8_lossy(&k_bytes).to_string(),
                        String::from_utf8_lossy(&v_bytes).to_string(),
                    ));
                }
                _ => return Err(MqttError::MalformedPacket("Unknown property identifier")),
            }
        } else {
            break;
        }
    }

    Ok(properties)
}
