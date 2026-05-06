use bytes::{Buf, BufMut};
use crate::error::MqttError;

/// Reads an MQTT variable length integer from a buffer.
/// Returns a tuple of `(value, bytes_read)` if successful, or an error.
/// If there are not enough bytes to read the full integer, returns `Ok(None)`.
pub fn read_var_int(src: &mut std::io::Cursor<&[u8]>) -> Result<Option<(u32, usize)>, MqttError> {
    let mut multiplier: u32 = 1;
    let mut value: u32 = 0;
    let mut bytes_read: usize = 0;
    
    let chunk = src.chunk();
    let mut iter = chunk.iter();

    loop {
        if let Some(&encoded_byte) = iter.next() {
            bytes_read += 1;
            value += (encoded_byte & 127) as u32 * multiplier;
            if multiplier > 128 * 128 * 128 {
                return Err(MqttError::MalformedPacket("Malformed Variable Byte Integer"));
            }
            multiplier *= 128;
            if (encoded_byte & 128) == 0 {
                // Advance the cursor by the number of bytes read
                src.advance(bytes_read);
                return Ok(Some((value, bytes_read)));
            }
        } else {
            // Reached end of buffer before completing the varint
            return Ok(None);
        }
    }
}

/// Writes an MQTT variable length integer to a buffer.
pub fn write_var_int<B: BufMut>(mut value: u32, dst: &mut B) -> Result<(), MqttError> {
    loop {
        let mut encoded_byte = (value % 128) as u8;
        value /= 128;
        if value > 0 {
            encoded_byte |= 128;
        }
        dst.put_u8(encoded_byte);
        if value == 0 {
            break;
        }
    }
    Ok(())
}
