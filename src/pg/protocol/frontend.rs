use phf::phf_map;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};

use super::{PgConnectionState, PgMessage, PgMessageType, ProtocolError};

#[derive(Debug, Clone, Copy)]
pub enum PgFrontendMessageType {
    Startup,
    CancelRequest,
    SslRequest,
    PasswordMessageFamily,

    Query,

    FunctionCall,

    Parse,
    Close,
    Bind,
    Describe,
    Execute,
    Flush,
    Sync,

    Terminate,

    CopyData,
    CopyFail,
    CopyDone,
}

impl PgMessageType for PgFrontendMessageType {}

pub type PgFrontendMessage = PgMessage<PgFrontendMessageType>;

const FRONTEND_MESSAGE_TYPE_MAP: phf::Map<u8, PgFrontendMessageType> = phf_map! {
    b'B' => PgFrontendMessageType::Bind,
    b'C' => PgFrontendMessageType::Close,
    b'd' => PgFrontendMessageType::CopyData,
    b'c' => PgFrontendMessageType::CopyDone,
    b'f' => PgFrontendMessageType::CopyFail,
    b'D' => PgFrontendMessageType::Describe,
    b'E' => PgFrontendMessageType::Execute,
    b'H' => PgFrontendMessageType::Flush,
    b'F' => PgFrontendMessageType::FunctionCall,
    b'p' => PgFrontendMessageType::PasswordMessageFamily,
    b'P' => PgFrontendMessageType::Parse,
    b'Q' => PgFrontendMessageType::Query,
    b'S' => PgFrontendMessageType::Sync,
    b'X' => PgFrontendMessageType::Terminate,

};

#[derive(Debug, Default)]
pub struct PgFrontendMessageCodec {
    pub state: PgConnectionState,
}

/// Extract a parameter value from a startup message by key.
///
/// The startup message format is:
/// - 4 bytes: message length
/// - 4 bytes: protocol version (3.0)
/// - Null-terminated key-value pairs until empty key
///
/// Returns `Some(&str)` if the key is found, `None` otherwise.
pub fn startup_message_parameter<'a>(data: &'a [u8], key: &str) -> Option<&'a str> {
    // Skip length (4 bytes) and protocol version (4 bytes)
    let params = data.get(8..)?;

    let mut parts = params.split(|&b| b == 0);

    loop {
        let k = parts.next()?;
        if k.is_empty() {
            return None; // End of parameters
        }
        let v = parts.next()?;

        if k == key.as_bytes() {
            return std::str::from_utf8(v).ok();
        }
    }
}

/// Build a simple query message for sending to the backend.
///
/// Message format: 'Q' | int32 len | string query (null-terminated)
pub fn simple_query_message_build(query: &str) -> BytesMut {
    let query_bytes = query.as_bytes();
    // Length includes: 4 bytes for length field + query bytes + 1 null terminator
    let len = i32::try_from(4 + query_bytes.len() + 1).expect("query message fits in i32");
    let mut buf = BytesMut::with_capacity(1 + 4 + query_bytes.len() + 1);
    buf.extend_from_slice(b"Q");
    buf.extend_from_slice(&len.to_be_bytes());
    buf.extend_from_slice(query_bytes);
    buf.extend_from_slice(&[0]);
    buf
}

impl Decoder for PgFrontendMessageCodec {
    type Item = PgFrontendMessage;
    type Error = ProtocolError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        match self.state {
            PgConnectionState::Startup => {
                // multiple messages types can be received:
                // * SSLRequest
                // * StartupMessage
                // * CancelRequest
                // * ssl handshake
                //
                // they all start with a 4 byte message length (except ssl handshake)

                const CODE_STARTUP: [i16; 2] = [3, 0];
                const CODE_CANCEL_REQUEST: [i16; 2] = [1234, 5678];
                const CODE_SSL_REQUEST: [i16; 2] = [1234, 5679];

                const MIN_MESSAGE_LEN: usize = 8;
                if buf.remaining() < MIN_MESSAGE_LEN {
                    return Ok(None);
                }
                let msg_len = usize::try_from(buf.as_ref().get_i32())
                    .map_err(|_| ProtocolError::InvalidStartupFrame)?;
                if buf.remaining() < msg_len {
                    return Ok(None);
                }

                let (_, mut code_slice) = buf.split_at(4);
                let (_, mut minor_slice) = code_slice.split_at(2);
                let code = [code_slice.get_i16(), minor_slice.get_i16()];

                match code {
                    CODE_STARTUP => {
                        //validate message ends with double nulls
                        let (_, mut end_slice) = buf.split_at(msg_len - 2);
                        let end = end_slice.get_i16();
                        if end != 0 {
                            return Err(ProtocolError::InvalidStartupFrame);
                        }

                        self.state = PgConnectionState::Query;
                        Ok(Some(PgFrontendMessage {
                            message_type: PgFrontendMessageType::Startup,
                            data: buf.split_to(msg_len),
                        }))
                    }
                    CODE_CANCEL_REQUEST => Ok(Some(PgFrontendMessage {
                        message_type: PgFrontendMessageType::CancelRequest,
                        data: buf.split_to(msg_len),
                    })),
                    CODE_SSL_REQUEST => Ok(Some(PgFrontendMessage {
                        message_type: PgFrontendMessageType::SslRequest,
                        data: buf.split_to(msg_len),
                    })),
                    _ => Err(ProtocolError::InvalidProtocolVersion {
                        major: code[0],
                        minor: code[1],
                    }),
                }
            }
            PgConnectionState::Authentication | PgConnectionState::Query => {
                let Some(&first_byte) = buf.first() else {
                    return Ok(None);
                };

                let Some(msg_type) = FRONTEND_MESSAGE_TYPE_MAP.get(&first_byte) else {
                    return Err(ProtocolError::UnrecognizedMessageType {
                        tag: first_byte.escape_ascii().to_string(),
                    });
                };

                const MIN_MESSAGE_LEN: usize = 5;
                if buf.remaining() < MIN_MESSAGE_LEN {
                    return Ok(None);
                }

                let (_, mut len_slice) = buf.split_at(1);
                let msg_len = usize::try_from(len_slice.get_i32()).map_err(|_| {
                    ProtocolError::IoError(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "negative frontend message length",
                    ))
                })? + 1;
                if buf.remaining() < msg_len {
                    return Ok(None);
                }

                Ok(Some(PgFrontendMessage {
                    message_type: *msg_type,
                    data: buf.split_to(msg_len),
                }))
            }
        }
    }
}
