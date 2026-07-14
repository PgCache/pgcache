//! Build the *frontend* messages pgcache sends to a Postgres server (Startup,
//! Parse/Bind/Describe/Execute/Sync).
//!
//! The mirror of [`encode`](super::encode), which builds the *backend* messages
//! pgcache sends to its own clients.

use std::fmt::Write as _;

use ecow::EcoString;
use tokio_util::bytes::{BufMut, BytesMut};

use crate::cache::{CacheError, CacheResult};
use crate::query::ShapeKey;
use crate::query::ast::LiteralValue;

/// Postgres `int8` (bigint) type OID, declared for the parameterized
/// `LIMIT $1 OFFSET $2` placeholders so the planner doesn't have to infer it.
pub(crate) const INT8_OID: u32 = 20;
/// Postgres `text` type OID, declared for the `set_config` value parameter.
pub(crate) const TEXT_OID: u32 = 25;

/// Prepared statement (one per connection) that stamps the query generation
/// before a serve. `set_config(...)` takes a bound parameter (a bare `SET`
/// can't), so this is parsed once and Bind+Execute'd per serve (PGC-235).
/// `$1` is the generation as text; the GUC is integer-typed and coerces it.
pub(crate) const SETGEN_STATEMENT_NAME: &[u8] = b"pgc_setgen";
pub(crate) const SETGEN_SQL: &str = "SELECT set_config('mem.query_generation', $1, false)";

/// Build a PG startup message (protocol v3.0).
///
/// Format: int32 len | int32 protocol_version(196608) | key\0value\0 pairs | \0
pub(crate) fn startup_message_build(user: &str, database: &str) -> BytesMut {
    // Calculate total length
    let body_len = 4 // protocol version
        + 5 + user.len() + 1      // "user\0" + user + \0
        + 9 + database.len() + 1   // "database\0" + database + \0
        + 1; // final \0 terminator
    let total_len = 4 + body_len; // 4 for the length field itself
    let total_len_i32 = i32::try_from(total_len).expect("startup message fits in i32");

    let mut buf = BytesMut::with_capacity(total_len);
    buf.put_i32(total_len_i32);
    buf.put_i32(196608); // Protocol 3.0
    buf.put_slice(b"user\0");
    buf.put_slice(user.as_bytes());
    buf.put_u8(0);
    buf.put_slice(b"database\0");
    buf.put_slice(database.as_bytes());
    buf.put_u8(0);
    buf.put_u8(0); // terminator
    buf
}

/// Append a frontend protocol message: the tag byte, a 4-byte length backfilled
/// to cover the length field plus `body`, and the body itself. Errors if the
/// message exceeds the protocol's i32 length field (a query too large to wire).
pub(crate) fn frontend_msg_append(
    buf: &mut BytesMut,
    tag: u8,
    body: impl FnOnce(&mut BytesMut) -> CacheResult<()>,
) -> CacheResult<()> {
    buf.put_u8(tag);
    let len_pos = buf.len();
    buf.put_i32(0); // placeholder
    body(buf)?;
    let len = i32::try_from(buf.len() - len_pos).map_err(|_| CacheError::InvalidMessage)?;
    if let Some(slot) = buf.get_mut(len_pos..len_pos + 4) {
        slot.copy_from_slice(&len.to_be_bytes());
    }
    Ok(())
}

/// Append a text-format Bind parameter (4-byte length + raw bytes) to `buf`.
pub(crate) fn bind_text_write(buf: &mut BytesMut, bytes: &[u8]) -> CacheResult<()> {
    let len = i32::try_from(bytes.len()).map_err(|_| CacheError::InvalidMessage)?;
    buf.put_i32(len);
    buf.put_slice(bytes);
    Ok(())
}

/// Append a shape literal as a text-format Bind parameter — the raw value, not a
/// SQL literal (no quoting). Renders directly into `buf` to keep the serve hot
/// path allocation-free (int/bool/string render without a heap value; only the
/// rare `Float` path uses an inline `EcoString`). Only the four forms
/// `literal_is_parameterizable` admits reach a shape's literals; any other is a
/// logic error and binds empty under a debug assertion.
pub(crate) fn bind_value_write(buf: &mut BytesMut, literal: &LiteralValue) -> CacheResult<()> {
    match literal {
        LiteralValue::String(s) => bind_text_write(buf, s.as_bytes()),
        LiteralValue::Integer(i) => {
            let mut itoa_buf = itoa::Buffer::new();
            bind_text_write(buf, itoa_buf.format(*i).as_bytes())
        }
        LiteralValue::Boolean(v) => bind_text_write(buf, if *v { b"t" } else { b"f" }),
        LiteralValue::Float(f) => {
            let mut s = EcoString::new();
            let _ = write!(s, "{}", f.into_inner());
            bind_text_write(buf, s.as_bytes())
        }
        LiteralValue::StringWithCast(..)
        | LiteralValue::Array(..)
        | LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_) => {
            debug_assert!(false, "non-parameterizable literal in shape binds");
            bind_text_write(buf, b"")
        }
    }
}

/// Build a Parse + Bind + [Describe('P')] + Execute + Sync message group into
/// `buf`. `name` is the prepared-statement name (empty slice = unnamed). When
/// `send_parse` is true a Parse is emitted (first use of a named statement, or
/// every time for an unnamed one); otherwise only Bind/Execute are sent, reusing
/// the existing named statement.
///
/// Two param groups, bound in order: `literal_params` (the shape's `$1..$k`,
/// rendered inline as text with type OID 0 — inferred from context) followed by
/// `tail_params` (text, None = NULL) typed by `tail_param_oids` — kept as
/// borrowed slices so the serve hot path allocates nothing per hit. The result
/// format is binary when `binary_results`, else all-text.
#[allow(clippy::too_many_arguments, clippy::fn_params_excessive_bools)]
pub(crate) fn extended_query_build(
    buf: &mut BytesMut,
    name: &[u8],
    sql: &str,
    send_parse: bool,
    literal_params: &[LiteralValue],
    tail_param_oids: &[u32],
    tail_params: &[Option<&str>],
    include_describe: bool,
    binary_results: bool,
    include_sync: bool,
) -> CacheResult<()> {
    let param_count = literal_params.len() + tail_params.len();
    if send_parse {
        frontend_msg_append(buf, b'P', |b| {
            b.put_slice(name);
            b.put_u8(0); // statement name terminator
            b.put_slice(sql.as_bytes());
            b.put_u8(0); // SQL terminator
            b.put_i16(i16::try_from(param_count).map_err(|_| CacheError::InvalidMessage)?);
            for _ in 0..literal_params.len() {
                b.put_u32(0); // inferred from context
            }
            for &oid in tail_param_oids {
                b.put_u32(oid);
            }
            Ok(())
        })?;
    }

    frontend_msg_append(buf, b'B', |b| {
        b.put_u8(0); // unnamed portal
        b.put_slice(name);
        b.put_u8(0); // statement name terminator
        b.put_i16(0); // zero param format codes → all params text
        b.put_i16(i16::try_from(param_count).map_err(|_| CacheError::InvalidMessage)?);
        for literal in literal_params {
            bind_value_write(b, literal)?;
        }
        for value in tail_params {
            match *value {
                Some(s) => bind_text_write(b, s.as_bytes())?,
                None => b.put_i32(-1), // NULL
            }
        }
        if binary_results {
            b.put_i16(1); // one result format code
            b.put_i16(1); // binary
        } else {
            b.put_i16(0); // zero result format codes → all columns text
        }
        Ok(())
    })?;

    if include_describe {
        frontend_msg_append(buf, b'D', |b| {
            b.put_u8(b'P'); // describe portal
            b.put_u8(0); // unnamed portal
            Ok(())
        })?;
    }

    frontend_msg_append(buf, b'E', |b| {
        b.put_u8(0); // unnamed portal
        b.put_i32(0); // no row limit
        Ok(())
    })?;

    if include_sync {
        frontend_msg_append(buf, b'S', |_| Ok(()))?; // Sync
    }

    Ok(())
}

/// Length of a prepared-statement name: `pgc_` + 16 hex digits.
pub(crate) const STATEMENT_NAME_LEN: usize = 20;

/// Deterministic prepared-statement name for a query shape, formatted into a
/// fixed stack buffer to avoid a per-hit heap allocation on the serve path.
/// Equivalent to `format!("pgc_{:016x}", shape_key.raw())`. The shape key uniquely
/// determines the parameterized SQL, so the name is a stable key shared by every
/// query of that shape.
pub(crate) fn statement_name_bytes(shape_key: ShapeKey) -> [u8; STATEMENT_NAME_LEN] {
    let key = shape_key.raw();
    let mut name = [0u8; STATEMENT_NAME_LEN];
    let (prefix, hex) = name.split_at_mut(4);
    prefix.copy_from_slice(b"pgc_");
    for (i, slot) in hex.iter_mut().enumerate() {
        let nibble = (key >> ((15 - i) * 4)) & 0xf;
        *slot = char::from_digit(nibble as u32, 16).unwrap_or('0') as u8;
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn statement_name_bytes_matches_format() {
        for key in [0u64, 1, 0xdead_beef, 0x0123_4567_89ab_cdef, u64::MAX] {
            let expected = format!("pgc_{key:016x}");
            let got = statement_name_bytes(ShapeKey::from_raw(key));
            assert_eq!(
                std::str::from_utf8(&got).expect("ascii name"),
                expected,
                "shape key {key:#x}"
            );
        }
    }
}
