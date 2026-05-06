use bytes::{Buf, BufMut, BytesMut};
use std::io::Cursor;
use tokio_util::codec::{Decoder, Encoder};

use crate::error::MqttError;
use crate::packet::{Connect, MqttPacket, ProtocolLevel, Publish};
use crate::utils::read_var_int;

pub struct MqttCodec;

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

                // Payload is the rest of the packet
                let payload_start = header_len + payload_cursor.position() as usize;
                let payload = packet_bytes.slice(payload_start..total_len);

                MqttPacket::Publish(Publish {
                    dup,
                    qos,
                    retain,
                    topic,
                    packet_id,
                    payload,
                })
            }
            12 => MqttPacket::PingReq,
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
                dst.put_u8(0x40); // Type 4 (PUBACK)
                dst.put_u8(2); // Remaining length is 2
                dst.put_u16(puback.packet_id);
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
