use std::io;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum MqttError {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),
    
    #[error("Malformed packet: {0}")]
    MalformedPacket(&'static str),
    
    #[error("Protocol error: {0}")]
    ProtocolError(String),
    
    #[error("Unsupported protocol version")]
    UnsupportedVersion,

    #[error("Payload too large")]
    PayloadTooLarge,
}

impl From<MqttError> for io::Error {
    fn from(err: MqttError) -> Self {
        match err {
            MqttError::Io(e) => e,
            other => io::Error::new(io::ErrorKind::InvalidData, other.to_string()),
        }
    }
}
