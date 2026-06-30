use crate::query::ShapeKey;
use crate::query::ast::LiteralValue;
use std::collections::{HashSet, VecDeque};
use std::fmt::Write as _;
use std::io;

use ecow::EcoString;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio_util::bytes::{BufMut, BytesMut};
use tokio_util::codec::{Decoder, FramedRead};
use tracing::debug;

use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::settings::PgSettings;

use super::protocol::PgMessage;
use super::protocol::backend::{
    AUTHENTICATION_OK, PgBackendMessageCodec, PgBackendMessageType, data_rows_first_columns,
};

/// Postgres `int8` (bigint) type OID, declared for the parameterized
/// `LIMIT $1 OFFSET $2` placeholders so the planner doesn't have to infer it.
const INT8_OID: u32 = 20;
/// Postgres `text` type OID, declared for the `set_config` value parameter.
const TEXT_OID: u32 = 25;

/// Prepared statement (one per connection) that stamps the query generation
/// before a serve. `set_config(...)` takes a bound parameter (a bare `SET`
/// can't), so this is parsed once and Bind+Execute'd per serve (PGC-235).
/// `$1` is the generation as text; the GUC is integer-typed and coerces it.
const SETGEN_STATEMENT_NAME: &[u8] = b"pgc_setgen";
const SETGEN_SQL: &str = "SELECT set_config('mem.query_generation', $1, false)";

/// FIFO cap on named prepared statements per connection. Statements key by query
/// *shape* (PGC-294), so the working set is bounded by query-shape diversity —
/// tens to low hundreds, regardless of literal cardinality — not by the per-
/// literal fingerprint count (which the old per-fingerprint registry had to
/// actively reconcile to bound). The cap never trips in practice; it is a
/// backstop for a pathologically long-lived connection that serves an unbounded
/// stream of distinct shapes.
const PREPARED_STATEMENT_CAP: usize = 512;

/// Per-connection registry of named prepared statements, keyed by [`ShapeKey`].
/// PG prepared statements are session-local, so each connection tracks its own.
/// A shape statement queries the shared per-relation cache tables and stays valid
/// while the relation is cached; query eviction does not invalidate it, and a
/// schema change evicts every query on the relation (so the statement is simply
/// never executed again and ages out via the FIFO cap). Lifecycle is therefore
/// decoupled from cache eviction — no per-serve reconciliation against the cache.
pub(crate) struct PreparedStatements {
    /// Prepared shapes in insertion order; front = oldest (first to evict).
    order: VecDeque<ShapeKey>,
    /// Membership set for O(1) lookup; mirrors `order`.
    live: HashSet<ShapeKey>,
}

impl PreparedStatements {
    fn new() -> Self {
        Self {
            order: VecDeque::new(),
            live: HashSet::default(),
        }
    }

    fn contains(&self, key: ShapeKey) -> bool {
        self.live.contains(&key)
    }

    /// Record `key` as newly prepared on this connection. If that pushes the
    /// registry past [`PREPARED_STATEMENT_CAP`], evict the oldest shape and return
    /// its key so the caller closes that statement on the cache DB.
    fn insert(&mut self, key: ShapeKey) -> Option<ShapeKey> {
        self.order.push_back(key);
        self.live.insert(key);
        if self.order.len() > PREPARED_STATEMENT_CAP {
            let evicted = self.order.pop_front();
            if let Some(evicted) = evicted {
                self.live.remove(&evicted);
            }
            evicted
        } else {
            None
        }
    }
}

/// What `pipelined_named_query_send` put on the wire, so the caller's response
/// state machine knows which completion messages to expect.
pub struct PrepareOutcome {
    /// A Parse for the `set_config` generation-stamp statement was sent (expect a
    /// ParseComplete before its BindComplete). First serve per connection only.
    pub sent_setgen_parse: bool,
    /// A Parse for the SELECT was sent (expect a ParseComplete).
    pub sent_parse: bool,
    /// A Close for a FIFO-evicted shape statement was sent ahead of the SELECT
    /// (expect a CloseComplete before the SELECT's ParseComplete).
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
    /// cache hit (the SELECT body + optional `LIMIT $1 OFFSET $2`), avoiding
    /// per-request String allocations.
    pub sql_buf: String,
    /// Recycled wire-encode buffer. Every serve clears and rebuilds the pipelined
    /// message group (set_config + optional Close + Parse/Bind/Execute + Sync)
    /// here, so the per-hit allocation is amortized to zero at steady state.
    pub write_buf: BytesMut,
    /// Named prepared statements (`pgc_<fp>`) live on this connection, FIFO-capped.
    pub(crate) prepared: PreparedStatements,
    /// Whether the `pgc_setgen` generation-stamp statement has been Parsed on this
    /// connection yet (parsed once, then Bind+Execute'd per serve — PGC-235).
    pub(crate) setgen_parsed: bool,
}

/// The non-read-half state of a [`CacheConnection`], held aside while its read
/// half is wrapped in a `FramedRead` for a serve and restored by
/// [`CacheConnection::from_framed`]. Opaque to callers — they only carry it
/// between [`CacheConnection::into_framed`] and `from_framed`.
pub(crate) struct ParkedConnection {
    sql_buf: String,
    write_buf: BytesMut,
    prepared: PreparedStatements,
    setgen_parsed: bool,
}

impl CacheConnection {
    /// Move the read half (`stream` + `codec`) into a `FramedRead`, reusing the
    /// recycled `read_buf`, and return it alongside the parked rest of the
    /// connection. `with_capacity(.., 0)` so `FramedRead` doesn't allocate its
    /// default 8 KiB read buffer — we immediately swap in `read_buf`, which would
    /// otherwise drop that fresh allocation every serve.
    pub(crate) fn into_framed(
        self,
    ) -> (
        FramedRead<TcpStream, PgBackendMessageCodec>,
        ParkedConnection,
    ) {
        let mut framed = FramedRead::with_capacity(self.stream, self.codec, 0);
        *framed.read_buffer_mut() = self.read_buf;
        (
            framed,
            ParkedConnection {
                sql_buf: self.sql_buf,
                write_buf: self.write_buf,
                prepared: self.prepared,
                setgen_parsed: self.setgen_parsed,
            },
        )
    }

    /// Reassemble a `CacheConnection` from a `FramedRead` and the parked state
    /// returned by [`into_framed`](Self::into_framed).
    pub(crate) fn from_framed(
        framed: FramedRead<TcpStream, PgBackendMessageCodec>,
        parked: ParkedConnection,
    ) -> Self {
        let parts = framed.into_parts();
        Self {
            stream: parts.io,
            read_buf: parts.read_buf,
            codec: parts.codec,
            sql_buf: parked.sql_buf,
            write_buf: parked.write_buf,
            prepared: parked.prepared,
            setgen_parsed: parked.setgen_parsed,
        }
    }

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
            write_buf: BytesMut::with_capacity(4096),
            prepared: PreparedStatements::new(),
            setgen_parsed: false,
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

    /// Send a pipelined generation-stamp + a *named* prepared-statement
    /// Bind/Execute for the SELECT in `self.sql_buf` (which must already carry
    /// the trailing `LIMIT $1 OFFSET $2` placeholders), all under a single Sync.
    ///
    /// The generation is set via a prepared `SELECT set_config('mem.query_generation',
    /// $1, false)` (PGC-235) rather than a per-hit simple-query `SET`: the
    /// statement is parsed once per connection, and folding it into the same
    /// extended pipeline as the SELECT removes the SET's per-hit parse/plan *and*
    /// its separate implicit-transaction boundary. `pgcache_pgrx`'s CustomScan
    /// reads the GUC at scan-begin to record scanned rows under the generation, so
    /// this must run before the SELECT — pipeline order guarantees that.
    ///
    /// The SELECT in `self.sql_buf` is the shape SQL, carrying `$1..$k` for the
    /// shape's literals followed by `$(k+1)`/`$(k+2)` for `LIMIT`/`OFFSET`.
    /// `literal_params` binds `$1..$k` (text format, all non-NULL by construction);
    /// `limit_text`/`offset_text` bind the trailing two (None → NULL = no limit /
    /// offset 0). The literal params are Parsed with type OID 0 (inferred from
    /// context); the LIMIT/OFFSET pair is typed `int8`.
    ///
    /// A Parse is emitted for the SELECT only the first time `shape_key`'s statement
    /// is used on this connection; the set_config Parse only the first time anything
    /// is served on this connection.
    ///
    /// If preparing this shape evicts the oldest shape from the FIFO cap, a `Close`
    /// for the evicted statement is pipelined so its CloseComplete precedes the
    /// SELECT response. Returns a [`PrepareOutcome`] so the caller's response state
    /// machine knows which completion messages to expect. Built into the recycled
    /// `write_buf`, sent in one write.
    #[allow(clippy::too_many_arguments)]
    pub async fn pipelined_named_query_send(
        &mut self,
        shape_key: ShapeKey,
        generation: u64,
        literals: &[LiteralValue],
        limit_text: Option<&str>,
        offset_text: Option<&str>,
        include_describe: bool,
        binary_results: bool,
    ) -> CacheResult<PrepareOutcome> {
        let send_parse = !self.prepared.contains(shape_key);
        let name = statement_name_bytes(shape_key);
        let close_victim = if send_parse {
            self.prepared.insert(shape_key)
        } else {
            None
        };
        let send_setgen_parse = !self.setgen_parsed;
        self.setgen_parsed = true;

        self.write_buf.clear();

        // Generation stamp: prepared `set_config(...)` (parse-on-first-use),
        // bound to the generation as text, no Describe — its one-row result is
        // consumed by the caller's state machine. No trailing Sync (shared).
        let mut gen_buf = itoa::Buffer::new();
        let gen_text = gen_buf.format(generation);
        extended_query_build(
            &mut self.write_buf,
            SETGEN_STATEMENT_NAME,
            SETGEN_SQL,
            send_setgen_parse,
            &[],
            &[TEXT_OID],
            &[Some(gen_text)],
            false, // no Describe
            false, // text result (consumed)
            false, // no Sync — shared with the SELECT below
        )?;

        // Close the FIFO-evicted shape statement ahead of the SELECT so its
        // CloseComplete precedes the SELECT response.
        if let Some(victim_key) = close_victim {
            let victim_name = statement_name_bytes(victim_key);
            frontend_msg_append(&mut self.write_buf, b'C', |b| {
                b.put_u8(b'S'); // close a prepared statement
                b.put_slice(&victim_name);
                b.put_u8(0);
                Ok(())
            })?;
        }

        // Params: the shape's `$1..$k` literals (rendered inline, OID 0 inferred)
        // then `LIMIT`/`OFFSET` typed `int8`. Borrowed slices only — no per-hit
        // allocation.
        extended_query_build(
            &mut self.write_buf,
            &name,
            &self.sql_buf,
            send_parse,
            literals,
            &[INT8_OID, INT8_OID],
            &[limit_text, offset_text],
            include_describe,
            binary_results,
            true, // single trailing Sync for the whole pipeline
        )?;

        self.stream
            .write_all(&self.write_buf)
            .await
            .map_into_report::<CacheError>()?;

        Ok(PrepareOutcome {
            sent_setgen_parse: send_setgen_parse,
            sent_parse: send_parse,
            sent_close: close_victim.is_some(),
        })
    }

    /// Extended-protocol serve with an *unnamed* statement and no parameters for
    /// the SELECT in `self.sql_buf` (MV reads: no generation SET — MV tables
    /// aren't `pgcache_pgrx`-tracked — and the LIMIT is baked into the SQL).
    /// Built into the recycled `write_buf`.
    pub async fn extended_query_unnamed_send(
        &mut self,
        include_describe: bool,
        binary_results: bool,
    ) -> CacheResult<()> {
        self.write_buf.clear();
        extended_query_build(
            &mut self.write_buf,
            b"",
            &self.sql_buf,
            true,
            &[],
            &[],
            &[],
            include_describe,
            binary_results,
            true, // MV path is standalone — terminate with its own Sync
        )?;
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_into_report::<CacheError>()
    }

    /// Reset the session `mem.query_generation` GUC to 0 (simple query, drained
    /// through `ReadyForQuery`). The serve path sets it with session scope
    /// (`SETGEN_SQL`, `is_local=false`), so a pooled connection carries the last
    /// serve's generation; without this reset an `EXPLAIN ANALYZE` would execute
    /// the pgcache_pgrx custom scan at that generation and stamp the GC tracker
    /// (PGC-345).
    async fn generation_reset(&mut self) -> CacheResult<()> {
        self.write_buf.clear();
        frontend_msg_append(&mut self.write_buf, b'Q', |b| {
            b.put_slice(b"SET mem.query_generation TO 0");
            b.put_u8(0);
            Ok(())
        })?;
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_into_report::<CacheError>()?;
        loop {
            if self.frame_next().await?.message_type == PgBackendMessageType::ReadyForQuery {
                return Ok(());
            }
        }
    }

    /// Run an already-`EXPLAIN`-wrapped statement via an unnamed extended-protocol
    /// query, binding `literals` as `$1..$k`, and collect the `QUERY PLAN` rows
    /// (PGC-345). Resets `mem.query_generation` to 0 first (see
    /// [`generation_reset`](Self::generation_reset)) so an `EXPLAIN ANALYZE`
    /// cannot stamp the pgcache_pgrx generation tracker. The response is read
    /// through `ReadyForQuery`, leaving the connection protocol-clean for reuse;
    /// a cache-DB `ErrorResponse` is captured rather than failing the connection.
    /// Not a hot path — clarity over zero-copy.
    pub async fn explain_collect(
        &mut self,
        explain_sql: &str,
        literals: &[LiteralValue],
    ) -> CacheResult<ExplainOutcome> {
        self.generation_reset().await?;

        self.write_buf.clear();
        extended_query_build(
            &mut self.write_buf,
            b"",
            explain_sql,
            true,
            literals,
            &[],
            &[],
            true,  // Describe('P'): the plan's RowDescription is returned, then consumed here
            false, // text results
            true,  // standalone Sync
        )?;
        self.stream
            .write_all(&self.write_buf)
            .await
            .map_into_report::<CacheError>()?;

        let mut plan = Vec::new();
        let mut cache_error: Option<String> = None;
        loop {
            let frame = self.frame_next().await?;
            #[allow(clippy::wildcard_enum_match_arm)]
            match frame.message_type {
                // The codec batches consecutive DataRow messages into one frame,
                // so extract every row's plan line, not just the first.
                PgBackendMessageType::DataRows => {
                    data_rows_first_columns(&frame.data, &mut plan);
                }
                PgBackendMessageType::ErrorResponse => {
                    cache_error = Some(error_response_display(&frame.data));
                }
                PgBackendMessageType::ReadyForQuery => break,
                _ => {}
            }
        }

        Ok(match cache_error {
            Some(message) => ExplainOutcome::CacheError(message),
            None => ExplainOutcome::Plan(plan),
        })
    }
}

/// Outcome of [`CacheConnection::explain_collect`].
pub enum ExplainOutcome {
    /// Plan text — one entry per `QUERY PLAN` row from the cache DB.
    Plan(Vec<String>),
    /// The cache DB rejected the statement (bad EXPLAIN options, or the cache
    /// table was dropped during an eviction race). Carries a display message.
    CacheError(String),
}

/// Render a backend `ErrorResponse` frame to `[<sqlstate>] <message>` for display.
/// Fields are `code (1 byte) | value (null-terminated)`, list terminated by a 0
/// code; only `C` (SQLSTATE) and `M` (message) are read.
fn error_response_display(data: &[u8]) -> String {
    let mut sqlstate: Option<&str> = None;
    let mut message: Option<&str> = None;
    let mut payload = data.get(5..).unwrap_or_default();
    while let Some((&code, rest)) = payload.split_first() {
        if code == 0 {
            break;
        }
        let end = rest.iter().position(|&b| b == 0).unwrap_or(rest.len());
        let value = std::str::from_utf8(rest.get(..end).unwrap_or_default()).unwrap_or("");
        match code {
            b'C' => sqlstate = Some(value),
            b'M' => message = Some(value),
            _ => {}
        }
        payload = rest.get(end + 1..).unwrap_or_default();
    }
    match (sqlstate, message) {
        (Some(code), Some(text)) => format!("[{code}] {text}"),
        (None, Some(text)) => text.to_owned(),
        (Some(code), None) => format!("[{code}]"),
        (None, None) => "cache DB error".to_owned(),
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

/// Append a text-format Bind parameter (4-byte length + raw bytes) to `buf`.
fn bind_text_write(buf: &mut BytesMut, bytes: &[u8]) -> CacheResult<()> {
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
fn bind_value_write(buf: &mut BytesMut, literal: &LiteralValue) -> CacheResult<()> {
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
fn extended_query_build(
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
const STATEMENT_NAME_LEN: usize = 20;

/// Deterministic prepared-statement name for a query shape, formatted into a
/// fixed stack buffer to avoid a per-hit heap allocation on the serve path.
/// Equivalent to `format!("pgc_{:016x}", shape_key.raw())`. The shape key uniquely
/// determines the parameterized SQL, so the name is a stable key shared by every
/// query of that shape.
fn statement_name_bytes(shape_key: ShapeKey) -> [u8; STATEMENT_NAME_LEN] {
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
    fn prepared_statements_insert_tracks_membership_no_eviction_under_cap() {
        let mut p = PreparedStatements::new();
        assert_eq!(p.insert(ShapeKey::from_raw(10)), None);
        assert_eq!(p.insert(ShapeKey::from_raw(20)), None);
        assert_eq!(p.insert(ShapeKey::from_raw(30)), None);
        assert!(
            p.contains(ShapeKey::from_raw(10))
                && p.contains(ShapeKey::from_raw(20))
                && p.contains(ShapeKey::from_raw(30))
        );
        assert!(!p.contains(ShapeKey::from_raw(40)));
    }

    #[test]
    fn prepared_statements_evicts_oldest_at_cap() {
        let mut p = PreparedStatements::new();
        for i in 0..PREPARED_STATEMENT_CAP as u64 {
            assert_eq!(p.insert(ShapeKey::from_raw(i)), None);
        }
        // One past the cap evicts the oldest (shape 0), returned for the caller to
        // close; everything else stays.
        assert_eq!(
            p.insert(ShapeKey::from_raw(PREPARED_STATEMENT_CAP as u64)),
            Some(ShapeKey::from_raw(0))
        );
        assert!(!p.contains(ShapeKey::from_raw(0)));
        assert!(p.contains(ShapeKey::from_raw(1)));
        assert!(p.contains(ShapeKey::from_raw(PREPARED_STATEMENT_CAP as u64)));
    }

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
