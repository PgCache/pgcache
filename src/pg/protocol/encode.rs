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
