use phf::phf_map;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};

use super::*;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PgBackendMessageType {
    // startup
    SslRequestResponse,
    Authentication,
    ParameterStatus,
    BackendKeyData,
    NegotiateProtocolVersion,

    // extended query
    ParseComplete,
    CloseComplete,
    BindComplete,
    PortalSuspended,

    // command response
    CommandComplete,
    EmptyQueryResponse,
    ReadyForQuery,
    ErrorResponse,
    NoticeResponse,
    NotificationResponse,
    FunctionCallResponse,

    // data
    ParameterDescription,
    RowDescription,
    DataRows, //represent one or more data rows
    NoData,

    // copy
    CopyData,
    CopyDone,
    CopyInResponse,
    CopyOutResponse,
    CopyBothResponse,
}

impl PgMessageType for PgBackendMessageType {}

pub(crate) type PgBackendMessage = PgMessage<PgBackendMessageType>;

pub const AUTHENTICATION_TAG: u8 = b'R'; // => PgBackendMessageType::Authentication,
pub const BACKEND_KEY_DATA_TAG: u8 = b'K'; // => PgBackendMessageType::BackendKeyData,
pub const BIND_COMPLETE_TAG: u8 = b'2'; // => PgBackendMessageType::BindComplete,
pub const CLOSE_COMPLETE_TAG: u8 = b'3'; // => PgBackendMessageType::CloseComplete,
pub const COMMAND_COMPLETE_TAG: u8 = b'C'; // => PgBackendMessageType::CommandComplete,
pub const COPY_DATA_TAG: u8 = b'd'; // => PgBackendMessageType::CopyData,
pub const COPY_DONE_TAG: u8 = b'c'; // => PgBackendMessageType::CopyDone,
pub const COPY_IN_RESPONSE_TAG: u8 = b'G'; // => PgBackendMessageType::CopyInResponse,
pub const COPY_OUT_RESPONSE_TAG: u8 = b'H'; // => PgBackendMessageType::CopyOutResponse,
pub const COPY_BOHT_RESPONSE_TAG: u8 = b'W'; // => PgBackendMessageType::CopyBothResponse,
pub const DATA_ROW_TAG: u8 = b'D'; // => PgBackendMessageType::DataRow,
pub const EMPTY_QUERY_RESPONSE_TAG: u8 = b'I'; // => PgBackendMessageType::EmptyQueryResponse,
pub const ERROR_RESPONSE_TAG: u8 = b'E'; // => PgBackendMessageType::ErrorResponse,
pub const FUNCTION_CALL_RESPONSE_TAG: u8 = b'V'; // => PgBackendMessageType::FunctionCallResponse,
pub const NEGOTIATE_PROTOCOL_VERSION_TAG: u8 = b'v'; // => PgBackendMessageType::NegotiateProtocolVersion,
pub const NO_DATA_TAG: u8 = b'n'; // => PgBackendMessageType::NoData,
pub const NOTICE_RESPONSE_TAG: u8 = b'N'; // => PgBackendMessageType::NoticeResponse,
pub const NOTIFICATION_RESONPSE_TAG: u8 = b'A'; // => PgBackendMessageType::NotificationResponse,
pub const PARAMETER_DESCRIPTION_TAG: u8 = b't'; // => PgBackendMessageType::ParameterDescription,
pub const PARAMETER_STATUS_TAG: u8 = b'S'; // => PgBackendMessageType::ParameterStatus,
pub const PARSE_COMPLETE_TAG: u8 = b'1'; // => PgBackendMessageType::ParseComplete,
pub const PORTAL_SUSPENDED_TAG: u8 = b's'; // => PgBackendMessageType::PortalSuspended,
pub const READY_FOR_QUERY_TAG: u8 = b'Z'; // => PgBackendMessageType::ReadyForQuery,
pub const ROW_DESCRIPTION_TAG: u8 = b'T'; // => PgBackendMessageType::RowDescription,

const BACKEND_MESSAGE_TYPE_MAP: phf::Map<u8, PgBackendMessageType> = phf_map! {
    b'R' => PgBackendMessageType::Authentication,
    b'K' => PgBackendMessageType::BackendKeyData,
    b'2' => PgBackendMessageType::BindComplete,
    b'3' => PgBackendMessageType::CloseComplete,
    b'C' => PgBackendMessageType::CommandComplete,
    b'd' => PgBackendMessageType::CopyData,
    b'c' => PgBackendMessageType::CopyDone,
    b'G' => PgBackendMessageType::CopyInResponse,
    b'H' => PgBackendMessageType::CopyOutResponse,
    b'W' => PgBackendMessageType::CopyBothResponse,
    b'D' => PgBackendMessageType::DataRows,
    b'I' => PgBackendMessageType::EmptyQueryResponse,
    b'E' => PgBackendMessageType::ErrorResponse,
    b'V' => PgBackendMessageType::FunctionCallResponse,
    b'v' => PgBackendMessageType::NegotiateProtocolVersion,
    b'n' => PgBackendMessageType::NoData,
    b'N' => PgBackendMessageType::NoticeResponse,
    b'A' => PgBackendMessageType::NotificationResponse,
    b't' => PgBackendMessageType::ParameterDescription,
    b'S' => PgBackendMessageType::ParameterStatus,
    b'1' => PgBackendMessageType::ParseComplete,
    b's' => PgBackendMessageType::PortalSuspended,
    b'Z' => PgBackendMessageType::ReadyForQuery,
    b'T' => PgBackendMessageType::RowDescription,
};

pub const AUTHENTICATION_OK: i32 = 0;
pub const AUTHENTICATION_KERBEROSV5: i32 = 2;
pub const AUTHENTICATION_CLEARTEXT_PASSWORD: i32 = 3;
pub const AUTHENTICATION_MD5_PASSWORD: i32 = 5;
pub const AUTHENTICATION_GSS: i32 = 7;
pub const AUTHENTICATION_GSS_CONTINUE: i32 = 8;
pub const AUTHENTICATION_SSPI: i32 = 9;
pub const AUTHENTICATION_SASL: i32 = 10;
pub const AUTHENTICATION_SASL_CONTINUE: i32 = 11;
pub const AUTHENTICATION_SASL_FINAL: i32 = 12;

#[derive(Debug, Default)]
pub struct PgBackendMessageCodec {
    pub state: PgConnectionState,
}

impl PgBackendMessageCodec {
    fn handle_authentication_message(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<PgMessage<PgBackendMessageType>>, ProtocolError> {
        const MIN_AUTHENTICATION_LEN: usize = 9;
        if buf.remaining() < MIN_AUTHENTICATION_LEN {
            return Ok(None);
        }

        let (_, mut rest) = buf.split_at(1);
        let msg_len =
            usize::try_from(rest.get_i32()).map_err(|_| ProtocolError::InvalidStartupFrame)? + 1;
        if buf.remaining() < msg_len {
            return Ok(None);
        }

        let (_, mut auth_code_slice) = buf.split_at(5);
        self.state = if auth_code_slice.get_i32() == AUTHENTICATION_OK {
            PgConnectionState::Query
        } else {
            PgConnectionState::Authentication
        };

        Ok(Some(PgBackendMessage {
            message_type: PgBackendMessageType::Authentication,
            data: buf.split_to(msg_len),
        }))
    }
}

impl Decoder for PgBackendMessageCodec {
    type Item = PgMessage<PgBackendMessageType>;
    type Error = ProtocolError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if !buf.has_remaining() {
            return Ok(None);
        }

        match self.state {
            PgConnectionState::Startup => {
                let Some(&first_byte) = buf.first() else {
                    return Ok(None);
                };

                match first_byte {
                    b'S' | b'N' => Ok(Some(PgBackendMessage {
                        message_type: PgBackendMessageType::SslRequestResponse,
                        data: buf.split_to(1),
                    })),
                    b'R' => self.handle_authentication_message(buf),
                    _ => Err(ProtocolError::InvalidStartupFrame),
                }
            }
            PgConnectionState::Authentication => self.handle_authentication_message(buf),
            _ => {
                let Some(&first_byte) = buf.first() else {
                    return Ok(None);
                };

                let Some(msg_type) = BACKEND_MESSAGE_TYPE_MAP.get(&first_byte) else {
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
                        "negative backend message length",
                    ))
                })? + 1;
                if buf.remaining() < msg_len {
                    return Ok(None);
                }

                if *msg_type == PgBackendMessageType::DataRows {
                    const MAX_BATCH_SIZE: usize = 64 * 1024;

                    // Start with the first DataRow message
                    let mut total_bytes = msg_len;
                    let mut position = msg_len;

                    // Look ahead for more consecutive DataRow messages
                    while position + 5 <= buf.remaining() {
                        let Some(&next_tag) = buf.get(position) else {
                            break;
                        };

                        // Stop if not a DataRow message
                        if next_tag != DATA_ROW_TAG {
                            break;
                        }

                        // Read the next message length
                        let (_, mut next_len_slice) = buf.split_at(position + 1);
                        let Ok(next_msg_len) =
                            usize::try_from(next_len_slice.get_i32()).map(|n| n + 1)
                        else {
                            break;
                        };

                        // Stop if message is incomplete in buffer
                        if position + next_msg_len > buf.remaining() {
                            break;
                        }

                        // Stop if we would exceed the 64KB limit
                        if total_bytes + next_msg_len > MAX_BATCH_SIZE {
                            break;
                        }

                        // Accumulate this message
                        total_bytes += next_msg_len;
                        position += next_msg_len;
                    }

                    // Return all accumulated DataRow messages
                    Ok(Some(PgBackendMessage {
                        message_type: PgBackendMessageType::DataRows,
                        data: buf.split_to(total_bytes),
                    }))
                } else {
                    Ok(Some(PgBackendMessage {
                        message_type: *msg_type,
                        data: buf.split_to(msg_len),
                    }))
                }
            }
        }
    }
}

/// Parse a ParameterStatus message to extract name and value.
///
/// Message format: 'S' | int32 len | string name (null-terminated) | string value (null-terminated)
///
/// Returns `None` if the message is malformed.
pub fn parameter_status_parse(data: &[u8]) -> Option<(&str, &str)> {
    // Skip tag ('S') and length (4 bytes)
    let payload = data.get(5..)?;

    // Split on null bytes: [name, value, ""]
    let mut parts = payload.split(|&b| b == 0);

    let name = std::str::from_utf8(parts.next()?).ok()?;
    let value = std::str::from_utf8(parts.next()?).ok()?;

    Some((name, value))
}

/// Extract the authentication type from an Authentication message.
///
/// Message format: 'R' | int32 len | int32 auth_type | ...
///
/// Returns `None` if the message is too short.
pub fn authentication_type(data: &BytesMut) -> Option<i32> {
    let auth_type_bytes = data.get(5..9)?;
    Some(i32::from_be_bytes(auth_type_bytes.try_into().ok()?))
}

/// Extract the first column value from a DataRow message as a string.
///
/// Message format: 'D' | int32 len | int16 column_count | (int32 col_len | bytes col_data)*
///
/// Returns `None` if the message is malformed or the column is NULL.
pub fn data_row_first_column(data: &[u8]) -> Option<&str> {
    // Skip tag ('D') and length (4 bytes) and column count (2 bytes)
    let payload = data.get(7..)?;

    // First 4 bytes are the column length (-1 means NULL)
    let col_len = i32::from_be_bytes(payload.get(..4)?.try_into().ok()?);
    if col_len < 0 {
        return None; // NULL value
    }

    let col_len = usize::try_from(col_len).ok()?;
    let col_data = payload.get(4..4 + col_len)?;
    std::str::from_utf8(col_data).ok()
}

/// Extract the first column of *every* DataRow in a (possibly batched) `DataRows`
/// frame, appending each non-NULL value to `out`. [`decode`] coalesces
/// consecutive DataRow messages into one frame, so a consumer that reads rows
/// individually (rather than relaying the frame verbatim) must walk them all —
/// reading only the first would silently drop the rest (e.g. all but the top
/// line of a multi-row `EXPLAIN` plan).
pub fn data_rows_first_columns(data: &[u8], out: &mut Vec<String>) {
    let mut pos = 0;
    while let Some(&tag) = data.get(pos) {
        if tag != DATA_ROW_TAG {
            break;
        }
        let Some(len_bytes) = data
            .get(pos + 1..pos + 5)
            .and_then(|s| <[u8; 4]>::try_from(s).ok())
        else {
            break;
        };
        // The length field counts itself and the body, but not the tag byte.
        let Ok(body_len) = usize::try_from(i32::from_be_bytes(len_bytes)) else {
            break;
        };
        let msg_end = pos + 1 + body_len;
        let Some(message) = data.get(pos..msg_end) else {
            break;
        };
        if let Some(column) = data_row_first_column(message) {
            out.push(column.to_owned());
        }
        pos = msg_end;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Encode one `DataRow` ('D') with a single text column.
    fn data_row(value: &str) -> Vec<u8> {
        let mut frame = vec![DATA_ROW_TAG];
        let body_len = 2 + 4 + value.len(); // column count + column length + value
        frame.extend_from_slice(&i32::try_from(4 + body_len).unwrap().to_be_bytes());
        frame.extend_from_slice(&1i16.to_be_bytes()); // one column
        frame.extend_from_slice(&i32::try_from(value.len()).unwrap().to_be_bytes());
        frame.extend_from_slice(value.as_bytes());
        frame
    }

    #[test]
    fn test_data_rows_first_columns_walks_every_batched_row() {
        // The codec coalesces consecutive DataRow messages into one frame; the
        // walker must return all of them, not just the first (PGC-345).
        let mut blob = Vec::new();
        for line in [
            "Seq Scan on t",
            "  Filter: (a = 1)",
            "Planning Time: 0.1 ms",
        ] {
            blob.extend_from_slice(&data_row(line));
        }
        let mut out = Vec::new();
        data_rows_first_columns(&blob, &mut out);
        assert_eq!(
            out,
            vec![
                "Seq Scan on t".to_owned(),
                "  Filter: (a = 1)".to_owned(),
                "Planning Time: 0.1 ms".to_owned(),
            ]
        );
    }

    #[test]
    fn test_data_rows_first_columns_stops_at_non_datarow() {
        // A trailing non-'D' byte (e.g. the start of CommandComplete) ends the walk.
        let mut blob = data_row("only line");
        blob.push(b'C'); // start of a CommandComplete frame — not a DataRow
        let mut out = Vec::new();
        data_rows_first_columns(&blob, &mut out);
        assert_eq!(out, vec!["only line".to_owned()]);
    }
}
