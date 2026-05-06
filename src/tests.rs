#[cfg(test)]
mod tests {
    use crate::codec::MqttCodec;
    use crate::packet::{Connect, MqttPacket, ProtocolLevel, Publish};
    use bytes::{Bytes, BytesMut};
    use tokio_util::codec::Decoder;

    #[test]
    fn test_decode_connect_v311() {
        let mut codec = MqttCodec;
        let mut buf = BytesMut::new();
        
        // CONNECT Fixed Header
        buf.extend_from_slice(&[0x10, 0x10]); // Type 1, remaining length 16
        
        // Protocol Name "MQTT"
        buf.extend_from_slice(&[0x00, 0x04, b'M', b'Q', b'T', b'T']);
        
        // Protocol Level 4 (v3.1.1)
        buf.extend_from_slice(&[0x04]);
        
        // Connect Flags (Clean Session = 1)
        buf.extend_from_slice(&[0x02]);
        
        // Keep Alive (60 seconds)
        buf.extend_from_slice(&[0x00, 0x3C]);
        
        // Client ID "test"
        buf.extend_from_slice(&[0x00, 0x04, b't', b'e', b's', b't']);

        let result = codec.decode(&mut buf).unwrap();
        
        match result {
            Some(MqttPacket::Connect(connect)) => {
                assert_eq!(connect.protocol_level, ProtocolLevel::V311);
                assert_eq!(connect.client_id, "test");
                assert_eq!(connect.clean_session, true);
                assert_eq!(connect.keep_alive, 60);
            }
            _ => panic!("Failed to decode CONNECT packet"),
        }
    }

    #[test]
    fn test_decode_publish_qos1_zero_copy() {
        let mut codec = MqttCodec;
        let mut buf = BytesMut::new();
        
        // PUBLISH Fixed Header: Type 3, DUP 0, QoS 1 (bit 1 and 2), RETAIN 0 => 0x32
        // Remaining length: 2 (topic len) + 4 ("test") + 2 (packet ID) + 5 ("hello") = 13
        buf.extend_from_slice(&[0x32, 0x0D]);
        
        // Topic "test"
        buf.extend_from_slice(&[0x00, 0x04, b't', b'e', b's', b't']);
        
        // Packet ID 10
        buf.extend_from_slice(&[0x00, 0x0A]);
        
        // Payload "hello"
        buf.extend_from_slice(b"hello");

        let result = codec.decode(&mut buf).unwrap();
        
        match result {
            Some(MqttPacket::Publish(publish)) => {
                assert_eq!(publish.topic, "test");
                assert_eq!(publish.qos, 1);
                assert_eq!(publish.packet_id, Some(10));
                assert_eq!(publish.payload.as_ref(), b"hello");
            }
            _ => panic!("Failed to decode PUBLISH packet"),
        }
    }
}
