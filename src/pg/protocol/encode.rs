use tokio_util::bytes::{BufMut, BytesMut};

use crate::pg::protocol::backend::{COMMAND_COMPLETE_TAG, DATA_ROW_TAG, ROW_DESCRIPTION_TAG};
use crate::pg::protocol::{ProtocolResult, message_length};

/// Fixed protocol messages as static byte slices — no heap allocation.
pub(crate) const PARSE_COMPLETE_MSG: &[u8] = &[b'1', 0, 0, 0, 4];
pub(crate) const BIND_COMPLETE_MSG: &[u8] = &[b'2', 0, 0, 0, 4];
pub(crate) const CLOSE_COMPLETE_MSG: &[u8] = &[b'3', 0, 0, 0, 4];
pub(crate) const NO_DATA_MSG: &[u8] = &[b'n', 0, 0, 0, 4];
pub(crate) const READY_FOR_QUERY_IDLE_MSG: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];
pub(crate) const READY_FOR_QUERY_IN_TRANSACTION_MSG: &[u8] = &[b'Z', 0, 0, 0, 5, b'T'];
pub(crate) const READY_FOR_QUERY_FAILED_MSG: &[u8] = &[b'Z', 0, 0, 0, 5, b'E'];

/// Fixed `ErrorResponse` for a cache serve that already streamed bytes to the
/// client and so cannot be transparently forwarded to origin (PGC-291). Fields:
/// Severity=ERROR, SQLSTATE=58000 (system_error), generic message (no SQL
/// leaked). Static bytes — no allocation on the serve path. Layout is validated
/// against `error_response_frame` in the serve tests.
pub(crate) const SERVE_ERROR_MSG: &[u8] =
    b"E\x00\x00\x00\x30SERROR\x00C58000\x00Mpgcache: cache serve failed\x00\x00";

/// `text` type OID — the column type for a synthesized single-column result.
const TEXT_TYPE_OID: u32 = 25;

/// Encode a `NoticeResponse` ('N') carrying a single message at NOTICE severity
/// (SQLSTATE `00000`, successful_completion). Used to attach human-readable
/// diagnostics to a synthesized response without polluting the result set.
/// Fields: `S`=severity, `C`=SQLSTATE, `M`=message, each a null-terminated
/// string, then a final field-list terminator.
pub(crate) fn notice_response_encode(message: &str, buf: &mut BytesMut) -> ProtocolResult<()> {
    let body_len = 1 + b"NOTICE".len() + 1   // 'S' + "NOTICE" + \0
        + 1 + b"00000".len() + 1              // 'C' + "00000" + \0
        + 1 + message.len() + 1               // 'M' + message + \0
        + 1; // field-list terminator
    let msg_len = message_length(4 + body_len)?;

    buf.put_u8(b'N');
    buf.put_i32(msg_len);
    buf.put_u8(b'S');
    buf.put_slice(b"NOTICE");
    buf.put_u8(0);
    buf.put_u8(b'C');
    buf.put_slice(b"00000");
    buf.put_u8(0);
    buf.put_u8(b'M');
    buf.put_slice(message.as_bytes());
    buf.put_u8(0);
    buf.put_u8(0);
    Ok(())
}

/// Encode a `RowDescription` for a single `text` column with the given name.
/// Layout matches [`row_description_encode`] for one field; type is `text`
/// (OID 25), variable width (size -1, modifier -1), text format.
pub(crate) fn row_description_text_encode(
    column_name: &str,
    buf: &mut BytesMut,
) -> ProtocolResult<()> {
    let msg_len = message_length(6 + 18 + column_name.len() + 1)?;
    buf.put_u8(ROW_DESCRIPTION_TAG);
    buf.put_i32(msg_len);
    buf.put_i16(1); // one field
    buf.put_slice(column_name.as_bytes());
    buf.put_u8(0);
    buf.put_i32(0); // table oid
    buf.put_i16(0); // column num
    buf.put_u32(TEXT_TYPE_OID);
    buf.put_i16(-1); // text is variable width
    buf.put_i32(-1); // no type modifier
    buf.put_i16(0); // text format
    Ok(())
}

/// Encode a `DataRow` for a single `text` column. `None` encodes a SQL NULL
/// (length -1); `Some(value)` encodes its bytes.
pub(crate) fn data_row_text_encode(value: Option<&str>, buf: &mut BytesMut) -> ProtocolResult<()> {
    let value_len = value.map_or(0, str::len);
    let msg_len = message_length(6 + 4 + value_len)?;
    buf.put_u8(DATA_ROW_TAG);
    buf.put_i32(msg_len);
    buf.put_i16(1); // one column
    match value {
        Some(value) => {
            buf.put_i32(message_length(value.len())?);
            buf.put_slice(value.as_bytes());
        }
        None => buf.put_i32(-1),
    }
    Ok(())
}

/// Encode a `CommandComplete` carrying an arbitrary command tag (e.g. `EXPLAIN`),
/// unlike [`command_complete_encode`] which always reports `SELECT <count>`.
pub(crate) fn command_complete_tag_encode(tag: &str, buf: &mut BytesMut) -> ProtocolResult<()> {
    let msg_len = message_length(4 + tag.len() + 1)?;
    buf.put_u8(COMMAND_COMPLETE_TAG);
    buf.put_i32(msg_len);
    buf.put_slice(tag.as_bytes());
    buf.put_u8(0);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A backend frame's declared length must equal the byte count after the
    /// 1-byte tag (the length field counts itself plus the body, not the tag).
    fn frame_length_field(buf: &[u8]) -> usize {
        usize::try_from(i32::from_be_bytes(
            buf[1..5].try_into().expect("length field present"),
        ))
        .expect("length field non-negative")
    }

    #[test]
    fn test_notice_response_encode_layout() {
        let mut buf = BytesMut::new();
        notice_response_encode("hello", &mut buf).expect("encode notice response");
        assert_eq!(buf[0], b'N');
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        // Fields and terminator are present in order.
        assert!(buf.windows(7).any(|w| w == b"SNOTICE"));
        assert!(buf.windows(6).any(|w| w == b"C00000"));
        assert!(buf.windows(6).any(|w| w == b"Mhello"));
        assert_eq!(*buf.last().expect("non-empty"), 0);
    }

    #[test]
    fn test_row_description_text_encode_layout() {
        let mut buf = BytesMut::new();
        row_description_text_encode("QUERY PLAN", &mut buf).expect("encode row description text");
        assert_eq!(buf[0], ROW_DESCRIPTION_TAG);
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        // One field.
        assert_eq!(i16::from_be_bytes(buf[5..7].try_into().unwrap()), 1);
        assert!(buf.windows(b"QUERY PLAN".len()).any(|w| w == b"QUERY PLAN"));
    }

    #[test]
    fn test_data_row_text_encode_value_and_null() {
        let mut buf = BytesMut::new();
        data_row_text_encode(Some("abc"), &mut buf).expect("encode data row text");
        assert_eq!(buf[0], DATA_ROW_TAG);
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        assert_eq!(i16::from_be_bytes(buf[5..7].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(buf[7..11].try_into().unwrap()), 3);
        assert_eq!(&buf[11..14], b"abc");

        let mut null_buf = BytesMut::new();
        data_row_text_encode(None, &mut null_buf).expect("encode data row text");
        // NULL column length is -1.
        assert_eq!(i32::from_be_bytes(null_buf[7..11].try_into().unwrap()), -1);
    }

    #[test]
    fn test_command_complete_tag_encode_layout() {
        let mut buf = BytesMut::new();
        command_complete_tag_encode("EXPLAIN", &mut buf).expect("encode command complete tag");
        assert_eq!(buf[0], COMMAND_COMPLETE_TAG);
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        assert_eq!(&buf[5..12], b"EXPLAIN");
        assert_eq!(*buf.last().expect("non-empty"), 0);
    }
}
