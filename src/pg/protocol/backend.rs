use std::hash::Hash;

use phf::phf_map;
use tokio_util::{
    bytes::{Buf, BytesMut},
    codec::Decoder,
};

use super::{PgConnectionState, PgMessage, PgMessageType, ProtocolError, encode};
use encode::{
    READY_FOR_QUERY_FAILED_MSG, READY_FOR_QUERY_IDLE_MSG, READY_FOR_QUERY_IN_TRANSACTION_MSG,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PgBackendMessageType {
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

pub(crate) const COMMAND_COMPLETE_TAG: u8 = b'C'; // => PgBackendMessageType::CommandComplete,
pub(crate) const DATA_ROW_TAG: u8 = b'D'; // => PgBackendMessageType::DataRow,
pub(crate) const ROW_DESCRIPTION_TAG: u8 = b'T'; // => PgBackendMessageType::RowDescription,

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

pub(crate) const AUTHENTICATION_OK: i32 = 0;
pub(crate) const AUTHENTICATION_SASL: i32 = 10;

#[derive(Debug, Default)]
pub(crate) struct PgBackendMessageCodec {
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
            PgConnectionState::Startup => self.startup_decode(buf),
            PgConnectionState::Authentication => self.handle_authentication_message(buf),
            _ => message_decode(buf),
        }
    }
}

impl PgBackendMessageCodec {
    /// The server's first bytes: a one-byte SSL response, or an Authentication
    /// message.
    fn startup_decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<PgBackendMessage>, ProtocolError> {
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
}

/// Decode one tagged backend message once it is complete in `buf`. Consecutive
/// DataRows come back as one batched message.
fn message_decode(buf: &mut BytesMut) -> Result<Option<PgBackendMessage>, ProtocolError> {
    const MIN_MESSAGE_LEN: usize = 5;

    let Some(&first_byte) = buf.first() else {
        return Ok(None);
    };
    let Some(&message_type) = BACKEND_MESSAGE_TYPE_MAP.get(&first_byte) else {
        return Err(ProtocolError::UnrecognizedMessageType {
            tag: first_byte.escape_ascii().to_string(),
        });
    };
    if buf.remaining() < MIN_MESSAGE_LEN {
        return Ok(None);
    }
    let msg_len = frame_len_at(buf, 0).ok_or_else(|| {
        ProtocolError::IoError(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "negative backend message length",
        ))
    })?;
    if buf.remaining() < msg_len {
        return Ok(None);
    }

    let len = if message_type == PgBackendMessageType::DataRows {
        data_rows_batch_len(buf, msg_len)
    } else {
        msg_len
    };
    Ok(Some(PgBackendMessage {
        message_type,
        data: buf.split_to(len),
    }))
}

/// Total length (tag included) of the message starting at `position`, from its
/// length word; `None` if the word is not in `buf` or is negative.
fn frame_len_at(buf: &[u8], position: usize) -> Option<usize> {
    let word = buf.get(position + 1..position + 5)?;
    let len = i32::from_be_bytes(word.try_into().ok()?);
    usize::try_from(len).ok().map(|n| n + 1)
}

/// How many bytes of `buf` to return as one DataRows batch, given the first
/// row's length: following DataRows are taken while each is complete in the
/// buffer and the batch stays within 64KB.
fn data_rows_batch_len(buf: &[u8], first_len: usize) -> usize {
    const MAX_BATCH_SIZE: usize = 64 * 1024;

    let mut total = first_len;
    while let Some(next_len) = next_data_row_len(buf, total) {
        if total + next_len > MAX_BATCH_SIZE {
            break;
        }
        total += next_len;
    }
    total
}

/// Length of the complete DataRow at `position`, or `None` if the next message
/// is not a DataRow, has a negative length, or is not yet fully buffered.
fn next_data_row_len(buf: &[u8], position: usize) -> Option<usize> {
    if *buf.get(position)? != DATA_ROW_TAG {
        return None;
    }
    let len = frame_len_at(buf, position)?;
    (position + len <= buf.len()).then_some(len)
}

/// The backend transaction status carried by every `ReadyForQuery`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum TransactionStatus {
    /// `'I'`: not in a transaction block.
    #[default]
    Idle,
    /// `'T'`: in a transaction block.
    InTransaction,
    /// `'E'`: in a failed transaction block — origin rejects every statement
    /// until the block ends.
    Failed,
}

impl TransactionStatus {
    /// Parse the status byte of a `ReadyForQuery` frame (byte 5). A malformed
    /// or unknown byte reads as `Failed`: the most conservative state (never
    /// cache-served), and origin will report the real state on the next RFQ.
    pub fn from_ready_for_query(data: &[u8]) -> Self {
        match data.get(5) {
            Some(b'I') => Self::Idle,
            Some(b'T') => Self::InTransaction,
            _ => Self::Failed,
        }
    }

    /// The complete `ReadyForQuery` frame announcing this status.
    pub fn ready_for_query_message(self) -> &'static [u8] {
        match self {
            Self::Idle => READY_FOR_QUERY_IDLE_MSG,
            Self::InTransaction => READY_FOR_QUERY_IN_TRANSACTION_MSG,
            Self::Failed => READY_FOR_QUERY_FAILED_MSG,
        }
    }
}

/// Parse a ParameterStatus message to extract name and value.
///
/// Message format: 'S' | int32 len | string name (null-terminated) | string value (null-terminated)
///
/// Returns `None` if the message is malformed.
pub(crate) fn parameter_status_parse(data: &[u8]) -> Option<(&str, &str)> {
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
pub(crate) fn authentication_type(data: &BytesMut) -> Option<i32> {
    let auth_type_bytes = data.get(5..9)?;
    Some(i32::from_be_bytes(auth_type_bytes.try_into().ok()?))
}

/// Extract the first column value from a DataRow message as a string.
///
/// Message format: 'D' | int32 len | int16 column_count | (int32 col_len | bytes col_data)*
///
/// Returns `None` if the message is malformed or the column is NULL.
pub(crate) fn data_row_first_column(data: &[u8]) -> Option<&str> {
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
pub(crate) fn data_rows_first_columns(data: &[u8], out: &mut Vec<String>) {
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

    fn rows(values: &[&str]) -> Vec<u8> {
        values.iter().flat_map(|v| data_row(v)).collect()
    }

    #[test]
    fn test_data_rows_batch_len_takes_consecutive_complete_rows() {
        let buf = rows(&["a", "bb", "ccc"]);
        let first = data_row("a").len();
        assert_eq!(data_rows_batch_len(&buf, first), buf.len());
    }

    #[test]
    fn test_data_rows_batch_len_stops_at_non_data_row() {
        let mut buf = rows(&["a", "bb"]);
        let rows_len = buf.len();
        buf.extend_from_slice(&[COMMAND_COMPLETE_TAG, 0, 0, 0, 4]);
        assert_eq!(data_rows_batch_len(&buf, data_row("a").len()), rows_len);
    }

    #[test]
    fn test_data_rows_batch_len_stops_at_incomplete_or_negative_row() {
        let complete = rows(&["a", "bb"]);
        let mut truncated = complete.clone();
        truncated.extend_from_slice(&data_row("ccc")[..6]);
        assert_eq!(
            data_rows_batch_len(&truncated, data_row("a").len()),
            complete.len()
        );

        let mut negative = complete.clone();
        negative.extend_from_slice(&[DATA_ROW_TAG, 0xff, 0xff, 0xff, 0xff]);
        assert_eq!(
            data_rows_batch_len(&negative, data_row("a").len()),
            complete.len()
        );
    }

    #[test]
    fn test_data_rows_batch_len_caps_the_batch_at_64k() {
        let big = "x".repeat(30_000);
        let buf = rows(&[&big, &big, &big]);
        let row_len = data_row(&big).len();
        assert_eq!(data_rows_batch_len(&buf, row_len), 2 * row_len);
    }

    #[test]
    fn test_message_decode_batches_data_rows_then_returns_the_next_message() {
        let mut buf = BytesMut::from(&rows(&["a", "bb"])[..]);
        buf.extend_from_slice(&[COMMAND_COMPLETE_TAG, 0, 0, 0, 4]);
        let batch = message_decode(&mut buf)
            .expect("decode data rows")
            .expect("complete batch");
        assert_eq!(batch.message_type, PgBackendMessageType::DataRows);
        assert_eq!(batch.data.len(), rows(&["a", "bb"]).len());
        let next = message_decode(&mut buf)
            .expect("decode command complete")
            .expect("complete message");
        assert_eq!(next.message_type, PgBackendMessageType::CommandComplete);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_message_decode_waits_for_a_complete_frame() {
        let row = data_row("abc");
        let mut buf = BytesMut::from(&row[..row.len() - 1]);
        assert!(message_decode(&mut buf).expect("decode partial").is_none());
        assert_eq!(buf.len(), row.len() - 1);
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
