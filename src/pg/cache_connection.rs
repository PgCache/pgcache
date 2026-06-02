use std::collections::{HashSet, VecDeque};
use std::io;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::Decoder;
use tracing::debug;

use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::settings::PgSettings;

use super::protocol::PgMessage;
use super::protocol::backend::{AUTHENTICATION_OK, PgBackendMessageCodec, PgBackendMessageType};
use super::protocol::frontend::simple_query_message_build;

/// Postgres `int8` (bigint) type OID, declared for the parameterized
/// `LIMIT $1 OFFSET $2` placeholders so the planner doesn't have to infer it.
const INT8_OID: u32 = 20;

/// Per-connection registry of named prepared statements. PG prepared statements
/// are session-local, so each connection tracks its own. Statement lifecycle is
/// decoupled from cache eviction and self-tuning to the workload: there is no
/// cap. Each serve runs one step of round-robin reconciliation against the live
/// cache (see [`PreparedStatements::reconcile_one`]) — statements whose query is
/// still cached are kept (however many thousands), and statements whose query
/// was evicted are closed. A re-registered identical query reuses its statement
/// while present.
pub(crate) struct PreparedStatements {
    /// Prepared fingerprints; doubles as the round-robin reconciliation cursor
    /// (front = next to check).
    order: VecDeque<u64>,
    /// Membership set for O(1) lookup; mirrors `order`.
    live: HashSet<u64>,
}

impl PreparedStatements {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            live: HashSet::new(),
        }
    }

    fn contains(&self, fingerprint: u64) -> bool {
        self.live.contains(&fingerprint)
    }

    /// Record `fingerprint` as newly prepared on this connection.
    fn insert(&mut self, fingerprint: u64) {
        self.order.push_back(fingerprint);
        self.live.insert(fingerprint);
    }

    /// One bounded step of round-robin reconciliation: check the oldest tracked
    /// statement against `is_live`. If still live, rotate it to the back and
    /// return `None`; if its query was evicted, drop it and return its
    /// fingerprint so the caller closes the statement on the cache DB.
    pub(crate) fn reconcile_one(&mut self, is_live: impl Fn(u64) -> bool) -> Option<u64> {
        let &fingerprint = self.order.front()?;
        if is_live(fingerprint) {
            self.order.rotate_left(1);
            None
        } else {
            self.order.pop_front();
            self.live.remove(&fingerprint);
            Some(fingerprint)
        }
    }
}

/// What `pipelined_named_query_send` put on the wire, so the caller's response
/// state machine knows which completion messages to expect.
pub struct PrepareOutcome {
    /// A Parse was sent (expect a ParseComplete).
    pub sent_parse: bool,
    /// A Close for an evicted statement was sent ahead of the Parse (expect a
    /// CloseComplete before the ParseComplete).
    pub sent_close: bool,
}

/// Raw TCP connection to the cache database with PG protocol framing.
///
/// Avoids per-row overhead of tokio-postgres by providing direct access
/// to the underlying stream and codec for zero-copy frame forwarding.
pub struct CacheConnection {
    pub stream: TcpStream,
    pub read_buf: BytesMut,
    pub codec: PgBackendMessageCodec,
    /// Recycled SQL assembly buffer. The worker clears and rewrites this on every
    /// cache hit (SET generation prefix + precomputed body + optional LIMIT),
    /// avoiding per-request String allocations.
    pub sql_buf: String,
    /// Named prepared statements (`pgc_<fp>`) live on this connection, FIFO-capped.
    pub(crate) prepared: PreparedStatements,
}

impl CacheConnection {
    /// Connect to the cache database and complete the PG startup handshake.
    /// Assumes trust authentication (no password exchange).
    pub async fn connect(settings: &PgSettings) -> CacheResult<Self> {
        let addr = format!("{}:{}", settings.host, settings.port);
        let stream = TcpStream::connect(&addr)
            .await
            .map_into_report::<CacheError>()?;
        let _ = stream.set_nodelay(true);

        let mut conn = Self {
            stream,
            read_buf: BytesMut::with_capacity(64 * 1024),
            codec: PgBackendMessageCodec::default(),
            sql_buf: String::with_capacity(1024),
            prepared: PreparedStatements::new(),
        };

        // Send startup message
        let startup = startup_message_build(&settings.user, &settings.database);
        conn.stream
            .write_all(&startup)
            .await
            .map_into_report::<CacheError>()?;

        // Read until ReadyForQuery — trust auth sends:
        // AuthenticationOk → ParameterStatus* → BackendKeyData → ReadyForQuery
        conn.startup_handshake().await?;

        debug!(
            "cache connection established to {}:{}",
            settings.host, settings.port
        );
        Ok(conn)
    }

    /// Read one framed backend message, awaiting more bytes as needed. Errors on
    /// EOF (connection closed mid-stream).
    async fn frame_next(&mut self) -> CacheResult<PgMessage<PgBackendMessageType>> {
        loop {
            if let Some(msg) = self
                .codec
                .decode(&mut self.read_buf)
                .map_err(|_| CacheError::InvalidMessage)?
            {
                return Ok(msg);
            }
            let n = self
                .stream
                .read_buf(&mut self.read_buf)
                .await
                .map_into_report::<CacheError>()?;
            if n == 0 {
                return Err(CacheError::IoError(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "cache connection closed mid-stream",
                ))
                .into());
            }
        }
    }

    /// Read startup responses until ReadyForQuery is received.
    async fn startup_handshake(&mut self) -> CacheResult<()> {
        loop {
            let msg = self.frame_next().await?;
            #[allow(clippy::wildcard_enum_match_arm)]
            match msg.message_type {
                PgBackendMessageType::Authentication => {
                    // Verify it's AuthenticationOk (auth type at bytes 5..9)
                    let auth_type = msg
                        .data
                        .get(5..9)
                        .and_then(|b| b.try_into().ok())
                        .map(i32::from_be_bytes)
                        .unwrap_or(-1);
                    if auth_type != AUTHENTICATION_OK {
                        return Err(CacheError::InvalidMessage.into());
                    }
                }
                PgBackendMessageType::ReadyForQuery => return Ok(()),
                PgBackendMessageType::ErrorResponse => {
                    return Err(CacheError::InvalidMessage.into());
                }
                // Skip ParameterStatus, BackendKeyData, NegotiateProtocolVersion, etc.
                _ => {}
            }
        }
    }

    /// Send a pipelined SET (simple query) + a *named* prepared-statement
    /// Bind/Execute for the SELECT in `self.sql_buf` (which must already carry
    /// the trailing `LIMIT $1 OFFSET $2` placeholders). A Parse is emitted only
    /// the first time `fingerprint`'s statement is used on this connection;
    /// later hits reuse the prepared statement, so PG skips parse/plan.
    ///
    /// `limit_text`/`offset_text` bind `$1`/`$2` as text (None → NULL, which PG
    /// treats as no limit / offset 0).
    ///
    /// `close_victim`, when set, is a statement whose query was evicted (found by
    /// the caller's reconciliation pass); a `Close` for it is pipelined ahead of
    /// the Parse/Bind so its CloseComplete precedes the rest of the response.
    /// Returns a [`PrepareOutcome`] so the caller's response state machine knows
    /// which completion messages to expect.
    #[allow(clippy::too_many_arguments)]
    pub async fn pipelined_named_query_send(
        &mut self,
        fingerprint: u64,
        set_sql: &str,
        limit_text: Option<&str>,
        offset_text: Option<&str>,
        include_describe: bool,
        binary_results: bool,
        close_victim: Option<u64>,
    ) -> CacheResult<PrepareOutcome> {
        let send_parse = !self.prepared.contains(fingerprint);
        let name = statement_name_bytes(fingerprint);
        if send_parse {
            self.prepared.insert(fingerprint);
        }

        let set_msg = simple_query_message_build(set_sql);
        let mut buf = BytesMut::with_capacity(set_msg.len() + self.sql_buf.len() + 96);
        buf.extend_from_slice(&set_msg);

        // Close the reconciled (evicted) statement ahead of the Parse/Bind so
        // its CloseComplete precedes the rest of the response.
        if let Some(victim_fp) = close_victim {
            let victim_name = statement_name_bytes(victim_fp);
            frontend_msg_append(&mut buf, b'C', |b| {
                b.put_u8(b'S'); // close a prepared statement
                b.put_slice(&victim_name);
                b.put_u8(0);
                Ok(())
            })?;
        }

        extended_query_build(
            &mut buf,
            &name,
            &self.sql_buf,
            send_parse,
            &[INT8_OID, INT8_OID],
            &[limit_text, offset_text],
            include_describe,
            binary_results,
        )?;

        self.stream
            .write_all(&buf)
            .await
            .map_into_report::<CacheError>()?;

        Ok(PrepareOutcome {
            sent_parse: send_parse,
            sent_close: close_victim.is_some(),
        })
    }

    /// Extended-protocol serve with an *unnamed* statement and no parameters (MV
    /// reads: no generation SET — MV tables aren't `pgcache_pgrx`-tracked — and
    /// the LIMIT is baked into `select_sql`).
    pub async fn extended_query_unnamed_send(
        &mut self,
        select_sql: &str,
        include_describe: bool,
        binary_results: bool,
    ) -> CacheResult<()> {
        let mut buf = BytesMut::with_capacity(select_sql.len() + 64);
        extended_query_build(
            &mut buf,
            b"",
            select_sql,
            true,
            &[],
            &[],
            include_describe,
            binary_results,
        )?;
        self.stream
            .write_all(&buf)
            .await
            .map_into_report::<CacheError>()
    }
}

/// Build a PG startup message (protocol v3.0).
///
/// Format: int32 len | int32 protocol_version(196608) | key\0value\0 pairs | \0
fn startup_message_build(user: &str, database: &str) -> BytesMut {
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
fn frontend_msg_append(
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

/// Build a Parse + Bind + [Describe('P')] + Execute + Sync message group into
/// `buf`. `name` is the prepared-statement name (empty slice = unnamed). When
/// `send_parse` is true a Parse declaring `param_oids` is emitted (first use of a
/// named statement, or every time for an unnamed one); otherwise only
/// Bind/Execute are sent, reusing the existing named statement. Bind sends
/// `params` in text format (None = NULL) and selects the result format via
/// `binary_results` (binary, vs all-text when false).
#[allow(clippy::too_many_arguments)]
fn extended_query_build(
    buf: &mut BytesMut,
    name: &[u8],
    sql: &str,
    send_parse: bool,
    param_oids: &[u32],
    params: &[Option<&str>],
    include_describe: bool,
    binary_results: bool,
) -> CacheResult<()> {
    if send_parse {
        frontend_msg_append(buf, b'P', |b| {
            b.put_slice(name);
            b.put_u8(0); // statement name terminator
            b.put_slice(sql.as_bytes());
            b.put_u8(0); // SQL terminator
            b.put_i16(i16::try_from(param_oids.len()).map_err(|_| CacheError::InvalidMessage)?);
            for &oid in param_oids {
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
        b.put_i16(i16::try_from(params.len()).map_err(|_| CacheError::InvalidMessage)?);
        for value in params {
            match *value {
                Some(s) => {
                    let len = i32::try_from(s.len()).map_err(|_| CacheError::InvalidMessage)?;
                    b.put_i32(len);
                    b.put_slice(s.as_bytes());
                }
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

    frontend_msg_append(buf, b'S', |_| Ok(()))?; // Sync

    Ok(())
}

/// Length of a prepared-statement name: `pgc_` + 16 hex digits.
const STATEMENT_NAME_LEN: usize = 20;

/// Deterministic prepared-statement name for a query fingerprint, formatted into
/// a fixed stack buffer to avoid a per-hit heap allocation on the serve path.
/// Equivalent to `format!("pgc_{fingerprint:016x}")`. The fingerprint uniquely
/// determines the SQL, so the name is a stable key and a re-registered identical
/// query safely reuses any surviving statement.
fn statement_name_bytes(fingerprint: u64) -> [u8; STATEMENT_NAME_LEN] {
    let mut name = [0u8; STATEMENT_NAME_LEN];
    let (prefix, hex) = name.split_at_mut(4);
    prefix.copy_from_slice(b"pgc_");
    for (i, slot) in hex.iter_mut().enumerate() {
        let nibble = (fingerprint >> ((15 - i) * 4)) & 0xf;
        *slot = char::from_digit(nibble as u32, 16).unwrap_or('0') as u8;
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prepared_statements_reconcile_keeps_live_closes_evicted() {
        let mut p = PreparedStatements::new();
        p.insert(10);
        p.insert(20);
        p.insert(30); // order [10,20,30]
        assert!(p.contains(10) && p.contains(20) && p.contains(30));

        // All live → rotate, never close. order: [10,20,30] → [20,30,10] → [30,10,20].
        assert_eq!(p.reconcile_one(|_| true), None);
        assert_eq!(p.reconcile_one(|_| true), None);
        assert!(p.contains(10) && p.contains(20) && p.contains(30));

        // Front is now 30; mark it evicted → reconcile closes and drops it.
        assert_eq!(p.reconcile_one(|fp| fp != 30), Some(30));
        assert!(!p.contains(30));
        assert!(p.contains(10) && p.contains(20));

        // Remaining are live → no more closes.
        assert_eq!(p.reconcile_one(|_| true), None);
        assert_eq!(p.reconcile_one(|_| true), None);
        assert!(p.contains(10) && p.contains(20));
    }

    #[test]
    fn statement_name_bytes_matches_format() {
        for fp in [0u64, 1, 0xdead_beef, 0x0123_4567_89ab_cdef, u64::MAX] {
            let expected = format!("pgc_{fp:016x}");
            let got = statement_name_bytes(fp);
            assert_eq!(
                std::str::from_utf8(&got).expect("ascii name"),
                expected,
                "fingerprint {fp:#x}"
            );
        }
    }
}
