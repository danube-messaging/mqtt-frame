use futures::{SinkExt, StreamExt};
use mqtt_frame::{MqttCodec, MqttPacket, ProtocolLevel};
use rumqttc::{AsyncClient, MqttOptions, QoS};
use std::time::Duration;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;

async fn spawn_mock_broker(version: ProtocolLevel) -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();

    tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut framed = Framed::new(socket, MqttCodec::new());

        // 1. Expect CONNECT
        let connect_packet = framed.next().await.unwrap().unwrap();
        match connect_packet {
            MqttPacket::Connect(connect) => {
                assert_eq!(connect.protocol_level, version);
                assert_eq!(connect.client_id, "integration-test-client");
                assert!(connect.clean_session);
            }
            _ => panic!("Expected CONNECT packet"),
        }

        // 2. Send CONNACK (Raw Bytes to trick rumqttc)
        framed
            .send(MqttPacket::ConnAck(mqtt_frame::packet::ConnAck {
                session_present: false,
                return_code: 0,
            }))
            .await
            .unwrap();

        // 3. Expect PUBLISH
        let publish_packet = framed.next().await.unwrap().unwrap();
        match publish_packet {
            MqttPacket::Publish(publish) => {
                assert_eq!(publish.topic, "test/topic");
                assert_eq!(publish.qos, 1);
                assert_eq!(publish.payload.as_ref(), b"hello integration");

                if version == ProtocolLevel::V5 {
                    // rumqttc v5 publisher might send properties if configured, but default is empty
                    assert!(publish.properties.is_empty());
                } else {
                    assert!(publish.properties.is_empty());
                }
            }
            _ => panic!("Expected PUBLISH packet"),
        }

        // 4. Send PUBACK
        framed
            .send(MqttPacket::PubAck(mqtt_frame::packet::PubAck {
                packet_id: 1, // rumqttc will use packet ID 1 for the first message
            }))
            .await
            .unwrap();
    });

    port
}

#[tokio::test]
async fn test_rumqttc_integration_v311() {
    let port = spawn_mock_broker(ProtocolLevel::V311).await;

    let mut mqttoptions = MqttOptions::new("integration-test-client", "127.0.0.1", port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClient::new(mqttoptions, 10);

    tokio::spawn(async move {
        loop {
            if let Ok(_) = eventloop.poll().await {
            } else {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish(
            "test/topic",
            QoS::AtLeastOnce,
            false,
            b"hello integration".to_vec(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
}

#[tokio::test]
async fn test_rumqttc_integration_v5() {
    let port = spawn_mock_broker(ProtocolLevel::V5).await;

    use rumqttc::v5::mqttbytes::QoS as QoSV5;
    use rumqttc::v5::AsyncClient as AsyncClientV5;
    use rumqttc::v5::MqttOptions as MqttOptionsV5;

    let mut mqttoptions = MqttOptionsV5::new("integration-test-client", "127.0.0.1", port);
    mqttoptions.set_keep_alive(Duration::from_secs(5));

    let (client, mut eventloop) = AsyncClientV5::new(mqttoptions, 10);

    tokio::spawn(async move {
        loop {
            if let Ok(_) = eventloop.poll().await {
            } else {
                break;
            }
        }
    });

    tokio::time::sleep(Duration::from_millis(100)).await;

    client
        .publish(
            "test/topic",
            QoSV5::AtLeastOnce,
            false,
            b"hello integration".to_vec(),
        )
        .await
        .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
}
