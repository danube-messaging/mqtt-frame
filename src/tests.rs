#[cfg(test)]
mod tests {
    use crate::codec::MqttCodec;
    use crate::packet::{MqttPacket, Property, ProtocolLevel};
    use bytes::BytesMut;
    use tokio_util::codec::Decoder;

    #[test]
    fn test_decode_connect_v311() {
        let mut codec = MqttCodec::new();
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
        let mut codec = MqttCodec::new();
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
                assert!(publish.properties.is_empty());
                assert_eq!(publish.payload.as_ref(), b"hello");
            }
            _ => panic!("Failed to decode PUBLISH packet"),
        }
    }

    #[test]
    fn test_decode_publish_v5_with_properties() {
        let mut codec = MqttCodec::new();
        codec.protocol_level = ProtocolLevel::V5; // Simulate a V5 connection
        let mut buf = BytesMut::new();

        // PUBLISH Fixed Header: Type 3, DUP 0, QoS 0, RETAIN 0 => 0x30
        // Remaining length: 2 (topic len) + 4 ("test") + 1 (prop len) + 2 (prop 1) + 7 (prop 2) + 5 ("hello") = 21
        buf.extend_from_slice(&[0x30, 21]);

        // Topic "test"
        buf.extend_from_slice(&[0x00, 0x04, b't', b'e', b's', b't']);

        // No Packet ID (since QoS is 0)

        // Properties Length: 9
        buf.extend_from_slice(&[9]);

        // Property 1: Payload Format Indicator (0x01) -> 1 (UTF-8)
        buf.extend_from_slice(&[0x01, 0x01]);

        // Property 2: Content Type (0x03) -> "JSON" (len 4)
        buf.extend_from_slice(&[0x03, 0x00, 0x04, b'J', b'S', b'O', b'N']);

        // Payload "hello"
        buf.extend_from_slice(b"hello");

        let result = codec.decode(&mut buf).unwrap();

        match result {
            Some(MqttPacket::Publish(publish)) => {
                assert_eq!(publish.topic, "test");
                assert_eq!(publish.properties.len(), 2);
                assert_eq!(publish.properties[0], Property::PayloadFormatIndicator(1));
                assert_eq!(
                    publish.properties[1],
                    Property::ContentType("JSON".to_string())
                );
                assert_eq!(publish.payload.as_ref(), b"hello");
            }
            _ => panic!("Failed to decode V5 PUBLISH packet"),
        }
    }
}
