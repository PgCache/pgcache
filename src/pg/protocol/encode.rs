use std::sync::Arc;

use tokio_postgres::{SimpleColumn, SimpleQueryRow};
use tokio_util::bytes::{BufMut, BytesMut};
use tracing::instrument;

use crate::pg::protocol::backend::{
    BIND_COMPLETE_TAG, COMMAND_COMPLETE_TAG, DATA_ROW_TAG, PARSE_COMPLETE_TAG, READY_FOR_QUERY_TAG,
    ROW_DESCRIPTION_TAG,
};

#[instrument(skip_all)]
pub fn row_description_encode(desc: &Arc<[SimpleColumn]>, buf: &mut BytesMut) {
    // PostgreSQL caps columns per relation at 1664, so the count always fits in i16.
    let field_cnt = i16::try_from(desc.len()).expect("column count fits in i16");
    let string_len: usize = desc.iter().map(|col| col.name().len() + 1).sum();
    let msg_len =
        i32::try_from(6 + 18 * desc.len() + string_len).expect("RowDescription size fits in i32");

    buf.put_u8(ROW_DESCRIPTION_TAG);
    buf.put_i32(msg_len);
    buf.put_i16(field_cnt);
    for col in desc.iter() {
        buf.put_slice(col.name().as_bytes());
        buf.put_u8(0); // string terminator
        buf.put_i32(0); // table oid
        buf.put_i16(0); // column num
        buf.put_u32(col.type_oid());
        buf.put_i16(col.type_size());
        buf.put_i32(col.type_modifier());
        buf.put_i16(0); // format code
    }
}

#[instrument(skip_all)]
pub fn simple_query_row_encode(row: &SimpleQueryRow, buf: &mut BytesMut) {
    let field_cnt = i16::try_from(row.len()).expect("column count fits in i16");
    let value_len: usize = (0..row.len())
        .map(|i| row.get(i).unwrap_or_default().len())
        .sum();
    let msg_len = i32::try_from(6 + 4 * row.len() + value_len).expect("DataRow size fits in i32");

    buf.put_u8(DATA_ROW_TAG);
    buf.put_i32(msg_len);
    buf.put_i16(field_cnt);
    for i in 0..row.len() {
        let data = row.get(i).unwrap_or_default().as_bytes();
        let data_len = i32::try_from(data.len()).expect("column value fits in i32");
        buf.put_i32(data_len);
        buf.put_slice(data);
    }
}

#[instrument(skip_all)]
pub fn command_complete_encode(cnt: u64, buf: &mut BytesMut) {
    let msg = format!("SELECT {cnt}");
    let msg_len = i32::try_from(4 + msg.len() + 1).expect("CommandComplete fits in i32");

    buf.put_u8(COMMAND_COMPLETE_TAG);
    buf.put_i32(msg_len);
    buf.put_slice(msg.as_bytes());
    buf.put_u8(0);
}

/// Fixed protocol messages as static byte slices — no heap allocation.
pub const PARSE_COMPLETE_MSG: &[u8] = &[b'1', 0, 0, 0, 4];
pub const BIND_COMPLETE_MSG: &[u8] = &[b'2', 0, 0, 0, 4];
pub const CLOSE_COMPLETE_MSG: &[u8] = &[b'3', 0, 0, 0, 4];
pub const NO_DATA_MSG: &[u8] = &[b'n', 0, 0, 0, 4];
pub const READY_FOR_QUERY_IDLE_MSG: &[u8] = &[b'Z', 0, 0, 0, 5, b'I'];

/// Fixed `ErrorResponse` for a cache serve that already streamed bytes to the
/// client and so cannot be transparently forwarded to origin (PGC-291). Fields:
/// Severity=ERROR, SQLSTATE=58000 (system_error), generic message (no SQL
/// leaked). Static bytes — no allocation on the serve path. Layout is validated
/// against `error_response_frame` in the serve tests.
pub const SERVE_ERROR_MSG: &[u8] =
    b"E\x00\x00\x00\x30SERROR\x00C58000\x00Mpgcache: cache serve failed\x00\x00";

#[instrument(skip_all)]
pub fn ready_for_query_encode(buf: &mut BytesMut) {
    buf.put_u8(READY_FOR_QUERY_TAG);
    buf.put_i32(5);
    buf.put_u8(b'I');
}

/// Encodes a ParseComplete message (tag '1', 5 bytes total, no payload).
pub fn parse_complete_encode(buf: &mut BytesMut) {
    buf.put_u8(PARSE_COMPLETE_TAG);
    buf.put_i32(4);
}

/// Encodes a BindComplete message (tag '2', 5 bytes total, no payload).
pub fn bind_complete_encode(buf: &mut BytesMut) {
    buf.put_u8(BIND_COMPLETE_TAG);
    buf.put_i32(4);
}

/// `text` type OID — the column type for a synthesized single-column result.
const TEXT_TYPE_OID: u32 = 25;

/// Encode a `NoticeResponse` ('N') carrying a single message at NOTICE severity
/// (SQLSTATE `00000`, successful_completion). Used to attach human-readable
/// diagnostics to a synthesized response without polluting the result set.
/// Fields: `S`=severity, `C`=SQLSTATE, `M`=message, each a null-terminated
/// string, then a final field-list terminator.
pub fn notice_response_encode(message: &str, buf: &mut BytesMut) {
    let body_len = 1 + b"NOTICE".len() + 1   // 'S' + "NOTICE" + \0
        + 1 + b"00000".len() + 1              // 'C' + "00000" + \0
        + 1 + message.len() + 1               // 'M' + message + \0
        + 1; // field-list terminator
    let msg_len = i32::try_from(4 + body_len).expect("NoticeResponse fits in i32");

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
}

/// Encode a `RowDescription` for a single `text` column with the given name.
/// Layout matches [`row_description_encode`] for one field; type is `text`
/// (OID 25), variable width (size -1, modifier -1), text format.
pub fn row_description_text_encode(column_name: &str, buf: &mut BytesMut) {
    let msg_len =
        i32::try_from(6 + 18 + column_name.len() + 1).expect("RowDescription fits in i32");
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
}

/// Encode a `DataRow` for a single `text` column. `None` encodes a SQL NULL
/// (length -1); `Some(value)` encodes its bytes.
pub fn data_row_text_encode(value: Option<&str>, buf: &mut BytesMut) {
    let value_len = value.map_or(0, str::len);
    let msg_len = i32::try_from(6 + 4 + value_len).expect("DataRow fits in i32");
    buf.put_u8(DATA_ROW_TAG);
    buf.put_i32(msg_len);
    buf.put_i16(1); // one column
    match value {
        Some(value) => {
            buf.put_i32(i32::try_from(value.len()).expect("column value fits in i32"));
            buf.put_slice(value.as_bytes());
        }
        None => buf.put_i32(-1),
    }
}

/// Encode a `CommandComplete` carrying an arbitrary command tag (e.g. `EXPLAIN`),
/// unlike [`command_complete_encode`] which always reports `SELECT <count>`.
pub fn command_complete_tag_encode(tag: &str, buf: &mut BytesMut) {
    let msg_len = i32::try_from(4 + tag.len() + 1).expect("CommandComplete fits in i32");
    buf.put_u8(COMMAND_COMPLETE_TAG);
    buf.put_i32(msg_len);
    buf.put_slice(tag.as_bytes());
    buf.put_u8(0);
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
        notice_response_encode("hello", &mut buf);
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
        row_description_text_encode("QUERY PLAN", &mut buf);
        assert_eq!(buf[0], ROW_DESCRIPTION_TAG);
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        // One field.
        assert_eq!(i16::from_be_bytes(buf[5..7].try_into().unwrap()), 1);
        assert!(buf.windows(b"QUERY PLAN".len()).any(|w| w == b"QUERY PLAN"));
    }

    #[test]
    fn test_data_row_text_encode_value_and_null() {
        let mut buf = BytesMut::new();
        data_row_text_encode(Some("abc"), &mut buf);
        assert_eq!(buf[0], DATA_ROW_TAG);
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        assert_eq!(i16::from_be_bytes(buf[5..7].try_into().unwrap()), 1);
        assert_eq!(i32::from_be_bytes(buf[7..11].try_into().unwrap()), 3);
        assert_eq!(&buf[11..14], b"abc");

        let mut null_buf = BytesMut::new();
        data_row_text_encode(None, &mut null_buf);
        // NULL column length is -1.
        assert_eq!(i32::from_be_bytes(null_buf[7..11].try_into().unwrap()), -1);
    }

    #[test]
    fn test_command_complete_tag_encode_layout() {
        let mut buf = BytesMut::new();
        command_complete_tag_encode("EXPLAIN", &mut buf);
        assert_eq!(buf[0], COMMAND_COMPLETE_TAG);
        assert_eq!(frame_length_field(&buf) + 1, buf.len());
        assert_eq!(&buf[5..12], b"EXPLAIN");
        assert_eq!(*buf.last().expect("non-empty"), 0);
    }
}
