# MQTT Frame

`mqtt-frame` is a lightweight, zero-copy, "Sans-I/O" MQTT v3.1.1 and v5.0 protocol codec and parser.

## Features

- **Sans-I/O Architecture**: The library focuses entirely on framing and parsing MQTT packets over a byte stream.
- **Tokio & Bytes Native**: Built directly on top of `tokio_util::codec::Decoder` and the `bytes` crate. It heavily utilizes `BytesMut::split_to().freeze()` to guarantee **zero-copy slicing** of payloads directly from the TCP network buffer.
- **Dual Protocol Support**: Natively supports both **MQTT 3.1.1** and **MQTT 5.0**. 
- **V5 Properties Extraction**: Securely extracts MQTT v5.0 Properties (like `MessageExpiryInterval`, `ContentType`, `UserProperty`) into a strongly typed `Property` enum.

## Usage

Because `mqtt-frame` is built on `tokio_util::codec`, using it with Tokio's TCP streams is straightforward:

```rust
use mqtt_frame::MqttCodec;
use tokio::net::TcpListener;
use tokio_util::codec::Framed;
use futures::StreamExt;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:1883").await?;
    println!("MQTT Broker listening on port 1883...");

    loop {
        let (socket, _) = listener.accept().await?;
        
        tokio::spawn(async move {
            // Wrap the raw TCP socket in our Codec
            // The codec defaults to V311, and will self-update to V5 
            // if the client connects using the MQTT 5.0 protocol level.
            let mut framed = Framed::new(socket, MqttCodec::new());

            // Process packets synchronously as they arrive
            while let Some(result) = framed.next().await {
                match result {
                    Ok(packet) => {
                        println!("Received packet: {:?}", packet);
                        // Implement your protocol bridge and session logic here
                    }
                    Err(e) => {
                        eprintln!("Protocol Error: {:?}", e);
                        break;
                    }
                }
            }
        });
    }
}
```

## Supported Packets

The parser fully implements the 14 standard packets required by the OASIS MQTT standard:

- `CONNECT` / `CONNACK`
- `PUBLISH` / `PUBACK` / `PUBREC` / `PUBREL` / `PUBCOMP`
- `SUBSCRIBE` / `SUBACK`
- `UNSUBSCRIBE` / `UNSUBACK`
- `PINGREQ` / `PINGRESP`
- `DISCONNECT`

## License

This project is licensed under the Apache License, Version 2.0.
