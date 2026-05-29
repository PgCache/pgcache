use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    time::Instant,
};

use lru::LruCache;

use ecow::EcoString;

use crate::catalog::FunctionVolatility;

use rootcause::Report;
use tokio::{
    io::AsyncWriteExt,
    net::{TcpStream, lookup_host},
    runtime::Builder,
    select,
    sync::{mpsc::UnboundedReceiver, oneshot},
    task::{LocalSet, spawn_local},
};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument, trace, warn};

use crate::{
    cache::{
        CacheMessage, CacheReply, ProxyMessage, QueryParameters,
        messages::{PipelineContext, PipelineDescribe},
        query::CacheableQuery,
    },
    metrics::names,
    pg::protocol::{
        ProtocolError,
        backend::{
            AUTHENTICATION_SASL, PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType,
            authentication_type, data_row_first_column, parameter_status_parse,
        },
        extended::{
            ParsedBindMessage, ParsedParseMessage, Portal, PreparedStatement, StatementType,
            parse_bind_message, parse_close_message, parse_describe_message, parse_execute_message,
            parse_parameter_description, parse_parse_message,
        },
        frontend::{
            PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType,
            simple_query_message_build, startup_message_parameter,
        },
    },
    proxy::egress::EgressQueue,
    query::ast::{query_expr_convert, query_expr_fingerprint},
    settings::{Settings, SslMode},
    telemetry::pg_version_set,
    timing::{QueryId, QueryTiming, timing_record},
    tls::{self},
};

use super::client_stream::{ClientReadHalf, ClientSocketSource, ClientStream, ClientWriteHalf};
use super::query::{Action, ForwardReason, handle_query};
use super::search_path::{
    SearchPath, search_path_mutates_any, search_path_mutates_single_piggybackable,
};
use super::tls_stream::{TlsReadHalf, TlsStream, TlsWriteHalf};
use super::{CacheSender, ConnectionError, ConnectionResult, ProxyMode, ProxyStatus};
use crate::result::{MapIntoReport, ReportExt};

/// Guard that decrements active connections gauge when dropped.
struct ActiveConnectionGuard;

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        metrics::gauge!(names::CONNECTIONS_ACTIVE).decrement(1.0);
    }
}

// ============================================================================
// OriginStream - type aliases using generic TLS stream types
// ============================================================================

/// Origin database connection stream, either plain TCP or TLS-encrypted.
pub type OriginStream = TlsStream<rustls::ClientConnection>;

/// Borrowed read half of an OriginStream.
pub type OriginReadHalf<'a> = TlsReadHalf<'a, rustls::ClientConnection>;

/// Borrowed write half of an OriginStream.
pub type OriginWriteHalf<'a> = TlsWriteHalf<'a, rustls::ClientConnection>;

/// Create an OriginStream from a tokio-rustls TlsStream.
///
/// Decomposes the TlsStream to allow borrowed splits with `.writable()`.
fn origin_stream_from_tls(tls_stream: tokio_rustls::client::TlsStream<TcpStream>) -> OriginStream {
    let (tcp, client_connection) = tls_stream.into_inner();
    TlsStream::Tls {
        tcp,
        tls_state: Arc::new(std::sync::Mutex::new(client_connection)),
    }
}

/// A given SQL can have different `ParameterDescription` responses depending
/// on the OID hints the client supplied in its `Parse` message, so both go
/// into the key.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
struct DescribeKey {
    sql: String,
    parameter_oids: Vec<u32>,
}

/// `row_description` is `None` when origin returned `NoData`.
#[derive(Debug, Clone)]
struct DescribeCacheEntry {
    parameter_description: BytesMut,
    row_description: Option<BytesMut>,
}

/// Bounded per connection so dynamic-SQL workloads can't grow it unbounded.
const DESCRIBE_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();

/// State machine for intercepting origin responses that shouldn't reach the client.
/// Only one intercept can be active at a time.
enum OriginIntercept {
    /// No intercept active — origin messages forwarded normally.
    None,
    /// Intercepting SHOW search_path response (pre-PG18 fallback).
    SearchPath,
    /// pgcache prepended a `Parse` ahead of the client's `Bind+Execute+Sync`
    /// because origin didn't know the statement name. Swallow the resulting
    /// `ParseComplete` (the client didn't ask for it) and let `BindComplete`
    /// onward flow through unchanged.
    LazyParseInline { statement_name: String },
    /// Piggyback: the client's Query was rewritten to append `; SHOW search_path`.
    /// Responses for the original statement are forwarded; the SHOW's response
    /// is stripped and parsed into `search_path_state`.
    TrailingShowSearchPath(TrailingShowState),
}

/// Sub-state for `OriginIntercept::TrailingShowSearchPath`.
#[derive(Debug, Clone, Copy)]
enum TrailingShowState {
    /// Before the first `CommandComplete` or `ErrorResponse` — forwarding
    /// responses for the original (client-written) statement.
    PreShow,
    /// After the original statement's `CommandComplete` — intercepting the
    /// injected SHOW's `RowDescription`, `DataRow`, `CommandComplete`.
    InShow,
    /// Original statement errored; PostgreSQL skips subsequent statements in
    /// the simple-query batch, so no SHOW response will arrive. Forward
    /// everything through to the final `ReadyForQuery`.
    Error,
}

/// Extract the SQL text (without the trailing null) from a simple-query
/// `'Q'` message body. Returns `None` if the frame is malformed or the text
/// is not valid UTF-8.
fn query_message_sql(data: &BytesMut) -> Option<&str> {
    // Frame layout: tag(1) | len(4) | sql(N) | nul(1); len counts itself+body,
    // so the SQL text (excluding the nul) lies at bytes 5..len_field.
    let len_bytes: [u8; 4] = data.get(1..5)?.try_into().ok()?;
    let msg_len = u32::from_be_bytes(len_bytes) as usize;
    str::from_utf8(data.get(5..msg_len)?).ok()
}

/// Append `; SHOW search_path` to a simple-query message, returning a new
/// frame. Caller must have verified the original message parses as exactly
/// one statement with a detected mutation.
fn query_message_append_show_search_path(data: &BytesMut) -> Option<BytesMut> {
    let sql = query_message_sql(data)?;
    Some(simple_query_message_build(&format!(
        "{sql}; SHOW search_path"
    )))
}

/// Search path discovery state machine.
///
/// pgcache needs the session's search_path for table resolution. PG 18+ sends
/// it via ParameterStatus at startup and again on every change. Older versions
/// send it only via an explicit `SHOW search_path` query, so we discover and
/// re-discover it around detected mutations (see `search_path_mutates_*`) and
/// transaction boundaries.
enum SearchPathState {
    /// No authoritative value: either before the first ReadyForQuery, or after
    /// a detected mutation (SET/RESET search_path, DISCARD ALL, COMMIT/ROLLBACK).
    /// Cacheable queries are forwarded until the next SHOW response (or
    /// ParameterStatus on PG18+) resolves the value.
    Unknown,

    /// search_path has been resolved (either from ParameterStatus or SHOW query)
    Resolved(SearchPath),
}

impl SearchPathState {
    /// Resolve the search_path if available, expanding $user to session_user.
    fn resolve(&self, session_user: Option<&str>) -> Option<Vec<String>> {
        match self {
            Self::Unknown => None,
            Self::Resolved(sp) => Some(
                sp.resolve(session_user)
                    .into_iter()
                    .map(String::from)
                    .collect(),
            ),
        }
    }
}

/// Timing instrumentation for the current query in flight.
/// Tracks timestamps for metrics recording across the origin and cache paths.
struct QueryTelemetry {
    /// When the client message arrived — measures end-to-end latency for both
    /// cache hits (CACHE_QUERY_LATENCY) and origin queries (ORIGIN_QUERY_LATENCY)
    client_received_at: Option<Instant>,

    /// When the query was forwarded to origin — measures origin-only execution
    /// time (ORIGIN_EXECUTION), excluding parse and cacheability-check overhead
    origin_sent_at: Option<Instant>,

    /// Per-stage timing breakdown that travels with the query through the cache
    /// pipeline (coordinator → worker) and back, only set for cache-path queries
    cache_timing: Option<QueryTiming>,
}

impl QueryTelemetry {
    fn new() -> Self {
        Self {
            client_received_at: None,
            origin_sent_at: None,
            cache_timing: None,
        }
    }

    /// Record that a client message was received.
    fn query_receive(&mut self) {
        self.client_received_at = Some(Instant::now());
    }

    /// Record that the query was forwarded to origin. Pass the QueryTiming
    /// returned by the cache thread to also stamp `forwarded_at` and retain
    /// it for per-stage histogram emission on completion.
    fn origin_forward(&mut self, timing: Option<QueryTiming>) {
        let now = Instant::now();
        self.origin_sent_at = Some(now);
        if let Some(mut t) = timing {
            t.forwarded_at = Some(now);
            self.cache_timing = Some(t);
        }
    }

    /// Create cache timing for a cacheable query.
    fn cache_timing_start(&mut self, fingerprint: u64) {
        let query_id = QueryId::new(fingerprint);
        let received_at = self.client_received_at.unwrap_or_else(Instant::now);
        let mut timing = QueryTiming::new(query_id, received_at);
        timing.parsed_at = Some(Instant::now());
        self.cache_timing = Some(timing);
    }

    /// Record origin query completion. Records ORIGIN_EXECUTION_SECONDS and
    /// ORIGIN_QUERY_LATENCY_SECONDS, and — when forward-path timing was
    /// threaded back from the cache thread — records the per-stage breakdown
    /// via `timing_record`.
    fn origin_complete(&mut self) {
        let now = Instant::now();
        if let Some(start) = self.origin_sent_at.take() {
            metrics::histogram!(names::ORIGIN_EXECUTION_SECONDS)
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(start) = self.client_received_at.take() {
            metrics::histogram!(names::ORIGIN_QUERY_LATENCY_SECONDS)
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(mut timing) = self.cache_timing.take() {
            // Forward path: `response_written_at` is intentionally left None.
            // The actual client write happens later in the event loop; stamping
            // here would record a near-zero diff that would pollute the
            // `response_write_seconds` histogram with cache-hit values.
            // `total_ns` falls back to `origin_response_at` (see timing.rs).
            timing.origin_response_at = Some(now);
            timing_record(&timing);
        }
    }

    /// Record cache query completion. Records CACHE_QUERY_LATENCY_SECONDS
    /// and per-stage timing breakdown.
    fn cache_complete(&mut self, reply_timing: Option<QueryTiming>) {
        if let Some(start) = self.client_received_at.take() {
            metrics::histogram!(names::CACHE_QUERY_LATENCY_SECONDS)
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(timing) = reply_timing {
            timing_record(&timing);
        }
    }

    /// Take the cache timing for dispatch to the cache pipeline.
    /// Sets dispatched_at before returning.
    fn cache_timing_dispatch(&mut self) -> QueryTiming {
        let mut t = self
            .cache_timing
            .take()
            .unwrap_or_else(|| QueryTiming::new(QueryId::new(0), Instant::now()));
        t.dispatched_at = Some(Instant::now());
        t
    }
}

/// Manages state for a single client connection.
/// Encapsulates transaction state, query fingerprint cache, and protocol state.
pub(super) struct ConnectionState {
    /// data waiting to be written to origin
    origin_write_buf: VecDeque<BytesMut>,

    /// Ordered queue of pending client-bound responses (origin relay, synth,
    /// cache). The single source of truth for client response ordering; see
    /// [`EgressQueue`]. Replaces the former `client_write_buf` + `pending_synth`
    /// + `origin_inflight_syncs` coordination.
    egress: EgressQueue<CacheMessage>,

    /// A `Flush` forwarded a Parse/Bind/Describe sub-request to origin (JDBC
    /// pattern), opening an `Origin` egress slot whose describe response carries
    /// no `ReadyForQuery` to seal it. The client reads that response before
    /// sending its next message, so the next client message seals the slot.
    flush_describe_pending: bool,

    /// Cache of query fingerprints to cacheability decisions
    fingerprint_cache: HashMap<u64, Result<Arc<CacheableQuery>, ForwardReason>>,

    /// Whether the connection is currently in a transaction
    in_transaction: bool,

    /// Current proxy mode (reading, writing to client/origin/cache)
    proxy_mode: ProxyMode,

    /// Proxy status (normal or degraded if cache is unavailable)
    proxy_status: ProxyStatus,

    /// Source for creating ClientSocket instances for cache queries.
    /// The connection handler writes directly to the client stream, but when
    /// sending queries to the cache, we create a ClientSocket from this source.
    client_socket_source: ClientSocketSource,

    /// Extended protocol: prepared statements by name
    prepared_statements: HashMap<String, PreparedStatement>,

    /// Extended protocol: portals (bound statements) by name
    portals: HashMap<String, Portal>,

    /// PostgreSQL session user from startup message
    /// TODO: Track SET ROLE queries to update effective user for permission checks
    session_user: Option<String>,

    /// Intercepts origin responses that shouldn't reach the client (e.g., SHOW
    /// search_path or proactive Parse+Sync). Only one intercept active at a time.
    origin_intercept: OriginIntercept,

    /// Search path discovery state
    search_path_state: SearchPathState,

    /// Set when the TrailingShowSearchPath piggyback intercept resolves
    /// search_path within the current origin message batch. Cleared when the
    /// RFQ for that batch is processed. Used to suppress the txn-end dirty
    /// marker so piggyback on COMMIT/ROLLBACK doesn't immediately clobber the
    /// freshly-resolved value.
    search_path_just_piggyback_resolved: bool,

    /// Set on the first `ParameterStatus("search_path", ...)` message we
    /// receive. This signals that the origin treats search_path as a
    /// GUC_REPORT parameter and will emit ParameterStatus on every change
    /// (PG18+ behavior). Once known, the proxy skips its defensive SHOW
    /// machinery — mutation detection, piggyback rewrite, and txn-end dirty
    /// marking — since ParameterStatus keeps state in sync automatically and
    /// the redundant SHOW would just burn a round trip.
    search_path_auto_reported: bool,

    /// Query timing instrumentation
    telemetry: QueryTelemetry,

    /// Function volatility map for cacheability checks
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,

    /// Extended query protocol pipeline state
    extended: ExtendedPending,

    /// Configured origin database name for client database validation
    origin_database: EcoString,

    /// Caching disabled for this connection (e.g., client targets a different database)
    cache_disabled: bool,

    /// Describe-response cache keyed by `(sql, parameter_oids)`; populated on
    /// each forwarded Parse+Describe, consulted by the Parse-only synthesize
    /// path so repeat prepares skip the origin round-trip.
    describe_cache: LruCache<DescribeKey, DescribeCacheEntry>,
}

/// Buffered extended protocol messages, accumulated until Sync/Flush.
/// All decision-making (cache vs. forward) is deferred to Sync time.
struct ExtendedBuffer {
    /// Concatenated raw bytes of all buffered messages
    bytes: BytesMut,
    /// Whether a Parse was buffered
    has_parse: bool,
    /// Whether a Bind was buffered
    has_bind: bool,
    /// Whether/what Describe was buffered
    describe: PipelineDescribe,
    /// Portal name from Execute (None if no Execute yet)
    execute_portal: Option<String>,
    /// True if more than one Execute was buffered (forces forward-to-origin)
    multiple_executes: bool,
    /// Statement name from Parse (for pending_parse_statement on forward)
    parse_statement_name: Option<String>,
    /// Statement name from Describe('S') (for pending_describe_statement on forward)
    describe_statement_name: Option<String>,
}

impl Default for ExtendedBuffer {
    fn default() -> Self {
        Self {
            bytes: BytesMut::new(),
            has_parse: false,
            has_bind: false,
            describe: PipelineDescribe::None,
            execute_portal: None,
            multiple_executes: false,
            parse_statement_name: None,
            describe_statement_name: None,
        }
    }
}

/// State for the extended query protocol pipeline.
/// Accumulates messages until Sync/Flush, then tracks pending origin responses
/// and pipeline context for cache dispatch.
struct ExtendedPending {
    /// Statement name whose ParseComplete we're waiting for from origin.
    /// Set when Parse is buffered in the pipeline; consumed when origin responds.
    pending_parse_statement: Option<String>,

    /// Name of statement most recently described (awaiting ParameterDescription)
    pending_describe_statement: Option<String>,

    /// Statement name to lazily Parse on the next origin forward. Set at Sync
    /// time for Bind-without-Parse batches against statements origin doesn't
    /// know; consumed by the forward paths in `handle_cache_reply`. Cleared on
    /// every Sync so stale state from a prior cache hit doesn't leak.
    pending_lazy_parse: Option<String>,

    /// Buffered extended protocol messages accumulated until Sync/Flush.
    /// Decision-making deferred to Sync time.
    buffer: Option<ExtendedBuffer>,

    /// Pipeline context ready for cache dispatch.
    /// Built at Sync time from ExtendedBuffer, consumed by ProxyMessage.
    pipeline_context: Option<PipelineContext>,
}

impl ExtendedPending {
    fn new() -> Self {
        Self {
            pending_parse_statement: None,
            pending_describe_statement: None,
            pending_lazy_parse: None,
            buffer: None,
            pipeline_context: None,
        }
    }

    /// Get or create the ExtendedBuffer for accumulating messages.
    fn buffer_get_or_create(&mut self) -> &mut ExtendedBuffer {
        self.buffer.get_or_insert_with(ExtendedBuffer::default)
    }

    /// Take the buffer contents. Returns None if no buffer was active.
    fn buffer_take(&mut self) -> Option<ExtendedBuffer> {
        self.buffer.take()
    }

    /// Flush any buffered extended protocol messages.
    /// Extracts pending statement names from buffer metadata.
    /// Returns the buffer's bytes for the caller to push to origin.
    fn buffer_flush(&mut self) -> Option<BytesMut> {
        let buffer = self.buffer.take()?;
        if buffer.has_parse {
            self.pending_parse_statement = buffer.parse_statement_name;
        }
        if buffer.describe_statement_name.is_some() {
            self.pending_describe_statement = buffer.describe_statement_name;
        }
        Some(buffer.bytes)
    }

    /// Forward buffer to origin with trailing bytes (Sync or Flush).
    /// Extracts pending statement names from buffer metadata.
    /// Returns bytes to push to origin.
    fn buffer_forward(&mut self, mut buffer: ExtendedBuffer, trailing_bytes: &[u8]) -> BytesMut {
        if buffer.has_parse {
            self.pending_parse_statement = buffer.parse_statement_name;
        }
        if buffer.describe_statement_name.is_some() {
            self.pending_describe_statement = buffer.describe_statement_name;
        }
        buffer.bytes.extend_from_slice(trailing_bytes);
        buffer.bytes
    }

    /// Handle ParseComplete from origin: mark statement as origin_prepared.
    fn parse_complete(&mut self, prepared_statements: &mut HashMap<String, PreparedStatement>) {
        if let Some(stmt_name) = self.pending_parse_statement.take()
            && let Some(stmt) = prepared_statements.get_mut(&stmt_name)
        {
            stmt.origin_prepared = true;
            trace!("origin_prepared set for statement '{}'", stmt_name);
        }
    }

    /// Update the pending statement's parameter OIDs. Does not clear
    /// `pending_describe_statement`; the following `RowDescription` or `NoData`
    /// consumes it.
    fn parameter_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<String, PreparedStatement>,
    ) {
        if let Some(stmt_name) = self.pending_describe_statement.as_ref()
            && let Ok(parsed) = parse_parameter_description(msg_data)
            && let Some(stmt) = prepared_statements.get_mut(stmt_name)
        {
            debug!(
                "updated statement '{}' with parameter OIDs {:?}",
                stmt_name, parsed.parameter_oids
            );
            stmt.parameter_oids = parsed.parameter_oids;
            stmt.parameter_description = Some(msg_data.clone());
        }
    }

    /// Store the raw RowDescription on the pending statement and clear
    /// `pending_describe_statement`. Returns the statement name so the caller
    /// can populate the per-connection describe cache.
    fn row_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<String, PreparedStatement>,
    ) -> Option<String> {
        let stmt_name = self.pending_describe_statement.take()?;
        let stmt = prepared_statements.get_mut(&stmt_name)?;
        stmt.row_description = Some(msg_data.clone());
        stmt.describe_no_data = false;
        Some(stmt_name)
    }

    /// Record NoData (statement has no result columns, e.g. INSERT without
    /// RETURNING) on the pending statement and clear `pending_describe_statement`.
    /// Returns the statement name so the caller can populate the per-connection
    /// describe cache.
    fn no_data_received(
        &mut self,
        prepared_statements: &mut HashMap<String, PreparedStatement>,
    ) -> Option<String> {
        let stmt_name = self.pending_describe_statement.take()?;
        let stmt = prepared_statements.get_mut(&stmt_name)?;
        stmt.row_description = None;
        stmt.describe_no_data = true;
        Some(stmt_name)
    }

    /// Take pipeline context (for origin fallback or cache dispatch).
    fn pipeline_take(&mut self) -> Option<PipelineContext> {
        self.pipeline_context.take()
    }
}

impl ConnectionState {
    fn new(
        client_socket_source: ClientSocketSource,
        func_volatility: Arc<HashMap<String, FunctionVolatility>>,
        origin_database: EcoString,
    ) -> Self {
        Self {
            origin_write_buf: VecDeque::new(),
            egress: EgressQueue::new(),
            flush_describe_pending: false,
            fingerprint_cache: HashMap::new(),
            in_transaction: false,
            proxy_mode: ProxyMode::Read,
            proxy_status: ProxyStatus::Normal,
            client_socket_source,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            session_user: None,
            origin_intercept: OriginIntercept::None,
            search_path_state: SearchPathState::Unknown,
            search_path_just_piggyback_resolved: false,
            search_path_auto_reported: false,
            telemetry: QueryTelemetry::new(),
            func_volatility,
            extended: ExtendedPending::new(),
            origin_database,
            cache_disabled: false,
            describe_cache: LruCache::new(DESCRIBE_CACHE_CAPACITY),
        }
    }

    /// Mark search_path as needing rediscovery and clear the describe-cache:
    /// RowDescription column type_oids depend on the resolved search_path,
    /// so cached entries could carry stale column metadata.
    fn search_path_mark_unknown(&mut self) {
        self.search_path_state = SearchPathState::Unknown;
        if !self.describe_cache.is_empty() {
            let n = self.describe_cache.len();
            self.describe_cache.clear();
            metrics::counter!(names::PROTOCOL_DESCRIBE_CACHE_INVALIDATIONS).increment(n as u64);
        }
    }

    /// Populate `describe_cache` from a freshly-Described statement. No-op for
    /// non-cacheable statements and for statements where origin errored before
    /// returning a parameter description.
    fn describe_cache_populate(&mut self, stmt_name: &str) {
        let Some(stmt) = self.prepared_statements.get(stmt_name) else {
            return;
        };
        if !matches!(stmt.sql_type, StatementType::Cacheable(_)) {
            return;
        }
        let Some(parameter_description) = stmt.parameter_description.clone() else {
            return;
        };
        let key = DescribeKey {
            sql: stmt.sql.clone(),
            parameter_oids: stmt.client_parameter_oids.clone(),
        };
        let entry = DescribeCacheEntry {
            parameter_description,
            row_description: stmt.row_description.clone(),
        };
        let was_at_capacity = self.describe_cache.len() == DESCRIBE_CACHE_CAPACITY.get();
        let replaced = self.describe_cache.put(key, entry).is_some();
        if !replaced && was_at_capacity {
            metrics::counter!(names::PROTOCOL_DESCRIBE_CACHE_EVICTIONS).increment(1);
        }
    }
}

impl ConnectionState {
    /// Push a Sync-terminated batch to origin, recording the forward in
    /// telemetry and reserving an ordered client-response slot so locally
    /// produced responses (synth, cache) can't jump ahead of this one.
    fn origin_dispatch(&mut self, bytes: BytesMut, timing: Option<QueryTiming>) {
        self.telemetry.origin_forward(timing);
        self.egress.origin_open();
        self.origin_write_buf.push_back(bytes);
    }

    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    #[expect(clippy::wildcard_enum_match_arm)]
    async fn handle_client_message(&mut self, mut msg: PgFrontendMessage) {
        trace!("net: client→proxy {:?}", msg.message_type);

        // A prior Flush forwarded a Describe sub-request whose response carries
        // no ReadyForQuery. The client only sends this next message after
        // reading that response, so the describe egress slot is now complete:
        // seal it so a following cache/forward response stays correctly ordered
        // behind it instead of being blocked by the unsealed slot.
        if self.flush_describe_pending {
            self.flush_describe_pending = false;
            self.egress.origin_seal();
        }
        match msg.message_type {
            PgFrontendMessageType::Query => {
                metrics::counter!(names::QUERIES_TOTAL).increment(1);
                metrics::counter!(names::PROTOCOL_SIMPLE_QUERIES).increment(1);
                self.telemetry.query_receive();

                self.search_path_inspect_query(&mut msg);

                if !self.in_transaction && !self.cache_disabled {
                    self.proxy_mode = match handle_query(
                        &msg.data,
                        &mut self.fingerprint_cache,
                        &self.func_volatility,
                    )
                    .await
                    {
                        Ok(Action::Forward(reason)) => {
                            match reason {
                                ForwardReason::UnsupportedStatement => {
                                    metrics::counter!(names::QUERIES_UNSUPPORTED).increment(1);
                                }
                                ForwardReason::UncacheableSelect => {
                                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                                }
                                ForwardReason::Invalid => {
                                    metrics::counter!(names::QUERIES_INVALID).increment(1);
                                }
                            }
                            self.origin_dispatch(msg.data, None);
                            ProxyMode::Read
                        }
                        Ok(Action::CacheCheck(ast)) => {
                            let fingerprint = query_expr_fingerprint(&ast.query);
                            self.telemetry.cache_timing_start(fingerprint);
                            self.egress.cache_push(CacheMessage::Query(msg.data, ast));
                            ProxyMode::OriginDrain
                        }
                        Err(e) => {
                            metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                            metrics::counter!(names::QUERIES_INVALID).increment(1);
                            error!("handle_query {}", e);
                            self.origin_dispatch(msg.data, None);
                            ProxyMode::Read
                        }
                    };
                } else {
                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                    self.origin_dispatch(msg.data, None);
                }
            }
            PgFrontendMessageType::Parse => {
                self.handle_parse_message(msg);
            }
            PgFrontendMessageType::Bind => {
                self.handle_bind_message(msg);
            }
            PgFrontendMessageType::Execute => {
                self.handle_execute_message(msg);
            }
            PgFrontendMessageType::Describe => {
                self.handle_describe_message(msg);
            }
            PgFrontendMessageType::Close => {
                self.handle_close_message(msg);
            }
            PgFrontendMessageType::Sync => {
                self.handle_sync_message(msg);
            }
            PgFrontendMessageType::Flush => {
                self.handle_flush_message(msg);
            }
            PgFrontendMessageType::Startup => {
                self.session_user = startup_message_parameter(&msg.data, "user").map(String::from);

                // The cache is connected to a specific database and cannot serve queries
                // for other databases.
                if let Some(client_db) = startup_message_parameter(&msg.data, "database")
                    && client_db != self.origin_database.as_str()
                {
                    warn!(
                        "client database '{}' does not match cache database '{}', caching disabled for this connection",
                        client_db, self.origin_database
                    );
                    self.cache_disabled = true;
                }

                self.origin_write_buf.push_back(msg.data);
            }
            PgFrontendMessageType::SslRequest => {
                // SSLRequest should be handled during connection setup before framing begins.
                // If we receive it here, something unexpected happened - log a warning.
                // Respond with 'N' to allow the connection to continue.
                debug!("unexpected SslRequest after TLS negotiation phase, responding 'N'");
                self.egress.synth_push(Bytes::from_static(b"N"));
            }
            PgFrontendMessageType::PasswordMessageFamily => {
                // Forward password/SASL messages to origin.
                // Note: Channel binding cannot be modified in transit because SCRAM includes
                // the gs2-header in the cryptographic proof. Clients connecting via TLS must
                // use channel_binding=disable in their connection string.
                self.origin_write_buf.push_back(msg.data);
            }
            _ => {
                // All other message types - forward to origin
                self.origin_write_buf.push_back(msg.data);
            }
        }
    }

    /// Handle an origin message during an active intercept.
    /// Returns true if the message was consumed (caller should not forward).
    #[expect(clippy::wildcard_enum_match_arm)]
    fn origin_intercept_handle(&mut self, msg: &PgBackendMessage) -> bool {
        match &self.origin_intercept {
            OriginIntercept::None => false,

            OriginIntercept::SearchPath => {
                match msg.message_type {
                    PgBackendMessageType::DataRows => {
                        if let Some(value) = data_row_first_column(&msg.data) {
                            debug!("received search_path from SHOW query: {}", value);
                            self.search_path_state =
                                SearchPathState::Resolved(SearchPath::parse(value));
                        }
                    }
                    PgBackendMessageType::ReadyForQuery => {
                        debug!("search_path query complete");
                        self.origin_intercept = OriginIntercept::None;
                    }
                    _ => {}
                }
                true
            }

            OriginIntercept::LazyParseInline { statement_name } => {
                let stmt_name = statement_name.clone();
                match msg.message_type {
                    PgBackendMessageType::ParseComplete => {
                        // Swallow ParseComplete (client didn't ask for it),
                        // mark origin-prepared, let the rest flow through.
                        if let Some(stmt) = self.prepared_statements.get_mut(&stmt_name) {
                            stmt.origin_prepared = true;
                            trace!("origin_prepared set for '{}' (lazy parse)", stmt_name);
                        }
                        self.origin_intercept = OriginIntercept::None;
                        true
                    }
                    _ => {
                        // Parse failed (or unexpected response): drop the
                        // intercept and let ErrorResponse + RFQ reach the client.
                        self.origin_intercept = OriginIntercept::None;
                        false
                    }
                }
            }

            &OriginIntercept::TrailingShowSearchPath(state) => {
                self.trailing_show_search_path_handle(state, msg)
            }
        }
    }

    /// Process one origin message under the piggyback intercept.
    ///
    /// Response layout for the rewritten `<stmt>; SHOW search_path`:
    /// - `<stmt>` responses up to its `CommandComplete` or `ErrorResponse`
    /// - on success: SHOW's `RowDescription`, one `DataRow`, `CommandComplete`
    /// - final `ReadyForQuery`
    ///
    /// The original-statement responses are forwarded to the client; the
    /// SHOW portion is consumed and its DataRow is parsed into
    /// `search_path_state`. If the original statement errored, the SHOW is
    /// skipped by PostgreSQL and everything forwards through to the RFQ.
    #[expect(clippy::wildcard_enum_match_arm)]
    fn trailing_show_search_path_handle(
        &mut self,
        state: TrailingShowState,
        msg: &PgBackendMessage,
    ) -> bool {
        match state {
            TrailingShowState::PreShow => {
                match msg.message_type {
                    PgBackendMessageType::CommandComplete => {
                        self.origin_intercept =
                            OriginIntercept::TrailingShowSearchPath(TrailingShowState::InShow);
                    }
                    PgBackendMessageType::ErrorResponse => {
                        debug!("piggyback: original statement errored, SHOW skipped");
                        self.origin_intercept =
                            OriginIntercept::TrailingShowSearchPath(TrailingShowState::Error);
                    }
                    _ => {}
                }
                false
            }
            TrailingShowState::InShow => match msg.message_type {
                PgBackendMessageType::DataRows => {
                    if let Some(value) = data_row_first_column(&msg.data) {
                        debug!("piggyback: received search_path from SHOW: {}", value);
                        self.search_path_state =
                            SearchPathState::Resolved(SearchPath::parse(value));
                        self.search_path_just_piggyback_resolved = true;
                    }
                    true
                }
                PgBackendMessageType::ReadyForQuery => {
                    debug!("piggyback: complete");
                    self.origin_intercept = OriginIntercept::None;
                    false
                }
                // RowDescription and SHOW's CommandComplete are for the client's
                // eyes: strip them.
                PgBackendMessageType::RowDescription | PgBackendMessageType::CommandComplete => {
                    true
                }
                // Anything else at this phase is unexpected; pass through to
                // avoid stalling the protocol.
                _ => false,
            },
            TrailingShowState::Error => {
                if matches!(msg.message_type, PgBackendMessageType::ReadyForQuery) {
                    self.origin_intercept = OriginIntercept::None;
                }
                false
            }
        }
    }

    /// Handle a message from the origin database (backend).
    /// Updates transaction state, captures parameter OIDs, and forwards to client.
    #[expect(clippy::wildcard_enum_match_arm)]
    fn handle_origin_message(&mut self, mut msg: PgBackendMessage) {
        trace!("net: origin→proxy {:?}", msg.message_type);

        if self.origin_intercept_handle(&msg) {
            // Swallowed by an intercept (injected SHOW, lazy ParseComplete, …):
            // these have no client egress slot, so an RFQ here seals nothing.
            return;
        }

        match msg.message_type {
            PgBackendMessageType::ParameterStatus => {
                if let Some((name, value)) = parameter_status_parse(&msg.data) {
                    match name {
                        "search_path" => {
                            // PG18+ reports search_path as a GUC_REPORT parameter,
                            // emitting this message on every change (including
                            // startup, SET, DISCARD ALL, and SET LOCAL reverts at
                            // transaction end). First arrival tells us we can
                            // skip the defensive SHOW machinery.
                            debug!("received search_path from ParameterStatus: {}", value);
                            self.search_path_state =
                                SearchPathState::Resolved(SearchPath::parse(value));
                            self.search_path_auto_reported = true;
                        }
                        "server_version" => {
                            pg_version_set(value.to_owned());
                        }
                        _ => {}
                    }
                }
            }
            PgBackendMessageType::ParameterDescription => {
                self.extended
                    .parameter_description_received(&msg.data, &mut self.prepared_statements);
            }
            PgBackendMessageType::RowDescription => {
                if let Some(name) = self
                    .extended
                    .row_description_received(&msg.data, &mut self.prepared_statements)
                {
                    self.describe_cache_populate(&name);
                }
            }
            PgBackendMessageType::NoData => {
                if let Some(name) = self
                    .extended
                    .no_data_received(&mut self.prepared_statements)
                {
                    self.describe_cache_populate(&name);
                }
            }
            PgBackendMessageType::ParseComplete => {
                self.extended.parse_complete(&mut self.prepared_statements);
            }
            PgBackendMessageType::Authentication => {
                if authentication_type(&msg.data).is_some_and(|v| v == AUTHENTICATION_SASL) {
                    // Strip SCRAM-SHA-256-PLUS from SASL authentication options.
                    // Channel binding cannot be supported because the proxy terminates TLS.
                    let needle = b"SCRAM-SHA-256-PLUS\0";
                    if let Some(pos) = msg
                        .data
                        .windows(needle.len())
                        .position(|window| window == needle)
                    {
                        // Remove needle in place using split/unsplit
                        let mut tail = msg.data.split_off(pos);
                        let after_needle = tail.split_off(needle.len());
                        msg.data.unsplit(after_needle);

                        // Update the length field (bytes 1-4, big-endian i32, excludes tag byte)
                        // Safety: Message format guarantees at least 5 bytes (1 tag + 4 length)
                        let new_len =
                            i32::try_from(msg.data.len() - 1).expect("PG message size fits in i32");
                        #[expect(
                            clippy::indexing_slicing,
                            reason = "PostgreSQL message format guarantees 5+ bytes"
                        )]
                        msg.data[1..5].copy_from_slice(&new_len.to_be_bytes());
                    }
                }
            }
            PgBackendMessageType::ReadyForQuery => {
                // ReadyForQuery message contains transaction status at byte 5
                // 'I' = idle (not in transaction)
                // 'T' = in transaction block
                // 'E' = in failed transaction block
                let was_in_transaction = self.in_transaction;
                self.in_transaction = msg.data.get(5).is_some_and(|&b| b == b'T' || b == b'E');

                self.telemetry.origin_complete();

                // Clean up unnamed portals when transaction ends
                if !self.in_transaction {
                    self.portals.retain(|name, _| !name.is_empty());
                }

                // Transaction ended: any SET (including SET LOCAL) within the
                // txn reverts, so the cached search_path may no longer match.
                // Skipped on PG18+: ParameterStatus already arrived earlier in
                // this same batch with the post-txn value. Also skipped if a
                // piggyback intercept in this batch has already resolved
                // search_path — marking unknown would clobber that fresh
                // value.
                if was_in_transaction
                    && !self.in_transaction
                    && !self.search_path_just_piggyback_resolved
                    && !self.search_path_auto_reported
                {
                    debug!("txn ended, marking search_path unknown");
                    self.search_path_mark_unknown();
                }
                self.search_path_just_piggyback_resolved = false;

                // If search_path is unknown (initial discovery on pre-PG18, or
                // after a detected mutation / txn-end), inject a SHOW query to
                // re-sync. Skipped if another intercept is active — another
                // RFQ will follow.
                if let SearchPathState::Unknown = self.search_path_state
                    && matches!(self.origin_intercept, OriginIntercept::None)
                {
                    debug!("search_path unknown, sending SHOW search_path query");
                    self.origin_intercept = OriginIntercept::SearchPath;
                    let query_msg = simple_query_message_build("SHOW search_path;");
                    // Injected query: its response is fully swallowed by the
                    // SearchPath intercept, so it gets no client egress slot.
                    self.origin_write_buf.push_back(query_msg);
                }
            }
            _ => {}
        }

        trace!(
            "net: origin→client {:?} ({} bytes)",
            msg.message_type,
            msg.data.len()
        );
        let was_rfq = matches!(msg.message_type, PgBackendMessageType::ReadyForQuery);
        self.egress.origin_append(msg.data.freeze());
        if was_rfq {
            // ReadyForQuery ends this request's response: seal its slot so the
            // next slot (and any locally-produced response behind it) can flush.
            self.egress.origin_seal();
        }
    }

    /// Flush any buffered extended protocol messages to origin.
    fn extended_buffer_flush_to_origin(&mut self) {
        if let Some(bytes) = self.extended.buffer_flush() {
            self.origin_write_buf.push_back(bytes);
        }
    }

    /// Forward an extended buffer to origin, appending the trailing message bytes (Sync or Flush).
    /// Records metrics for any Execute in the buffer.
    fn extended_buffer_forward_to_origin(&mut self, buffer: ExtendedBuffer, trailing_bytes: &[u8]) {
        let mut lazy_parse_stmt: Option<String> = None;
        if buffer.execute_portal.is_some() {
            metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);

            if let Some(portal_name) = &buffer.execute_portal
                && let Some(portal) = self.portals.get(portal_name)
                && let Some(stmt) = self.prepared_statements.get(&portal.statement_name)
            {
                match &stmt.sql_type {
                    StatementType::NonSelect => {
                        metrics::counter!(names::QUERIES_UNSUPPORTED).increment(1);
                    }
                    StatementType::ParseError => {
                        metrics::counter!(names::QUERIES_INVALID).increment(1);
                    }
                    StatementType::Cacheable(_) | StatementType::UncacheableSelect => {}
                }
                if !buffer.has_parse && !stmt.origin_prepared {
                    lazy_parse_stmt = Some(portal.statement_name.clone());
                }
            }
        }

        if let Some(stmt_name) = lazy_parse_stmt {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.extended,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }

        let bytes = self.extended.buffer_forward(buffer, trailing_bytes);
        self.origin_dispatch(bytes, None);
    }

    /// Handle a reply from the cache.
    /// If cache indicates error or needs forwarding, send query to origin instead.
    fn handle_cache_reply(&mut self, reply: CacheReply) {
        trace!(
            "net: cache→proxy reply={}",
            match &reply {
                CacheReply::Complete(_) => "Complete",
                CacheReply::Forward(_, _) => "Forward",
                CacheReply::Error(_) => "Error",
            }
        );
        match reply {
            CacheReply::Complete(timing) => {
                metrics::counter!(names::QUERIES_CACHE_HIT).increment(1);
                self.telemetry.cache_complete(timing);

                // Cache hit: the worker wrote the full response directly to the
                // client socket. Pop the serving cache slot so the next slot can
                // flush. Origin saw nothing; lazy-Parse-on-forward catches up the
                // next time we actually have to forward.
                self.egress.cache_done();
                self.extended.pending_parse_statement.take();
                self.extended.pending_lazy_parse.take();

                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Error(buf) => {
                metrics::counter!(names::QUERIES_CACHE_ERROR).increment(1);
                debug!("forwarding to origin");
                if let Some(stmt_name) = self.extended.pending_lazy_parse.take() {
                    forward_lazy_parse_install(
                        &stmt_name,
                        &self.prepared_statements,
                        &mut self.extended,
                        &mut self.origin_write_buf,
                        &mut self.origin_intercept,
                    );
                }
                self.cache_reply_forward(buf, None);
                self.proxy_mode = ProxyMode::Read;
            }
            CacheReply::Forward(buf, timing) => {
                metrics::counter!(names::QUERIES_CACHE_MISS).increment(1);
                debug!("forwarding to origin");
                if let Some(stmt_name) = self.extended.pending_lazy_parse.take() {
                    forward_lazy_parse_install(
                        &stmt_name,
                        &self.prepared_statements,
                        &mut self.extended,
                        &mut self.origin_write_buf,
                        &mut self.origin_intercept,
                    );
                }
                self.cache_reply_forward(buf, Some(timing));
                self.proxy_mode = ProxyMode::Read;
            }
        }
    }

    /// Forward a cache miss/error to origin: replace the serving cache slot with
    /// an origin slot in place (keeping response order) and queue the bytes. The
    /// slot already exists, so this does not open a new one.
    fn cache_reply_forward(&mut self, buf: BytesMut, timing: Option<QueryTiming>) {
        self.telemetry.origin_forward(timing);
        self.egress.cache_to_origin();
        self.origin_write_buf.push_back(buf);
    }

    /// Fall back to forwarding a cacheable query to origin after it was taken
    /// from its egress slot (search_path unknown or client-socket creation
    /// failed): convert the now-serving `Cache` slot back to an `Origin` slot in
    /// place, install a lazy `Parse` if origin doesn't yet know the statement,
    /// forward the query bytes, and return to `Read`.
    fn cache_slot_forward_to_origin(&mut self, msg: CacheMessage) {
        self.egress.cache_to_origin();
        if let Some(stmt_name) = self.extended.pending_lazy_parse.take() {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.extended,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }
        cache_forward_to_origin(&mut self.extended, &mut self.origin_write_buf, msg);
        self.proxy_mode = ProxyMode::Read;
    }

    /// Inspect an outgoing simple-query `Query` message for search_path
    /// mutations. Marks the cached search_path stale on any detected mutation
    /// (SET/RESET search_path, DISCARD ALL, COMMIT/ROLLBACK). When the message
    /// is a single such statement and no other intercept is active, rewrites
    /// the message to append `; SHOW search_path` and installs the
    /// `TrailingShowSearchPath` intercept so the SHOW's response is captured
    /// and stripped before reaching the client — avoiding the extra round
    /// trip a lazy SHOW would cost.
    fn search_path_inspect_query(&mut self, msg: &mut PgFrontendMessage) {
        // Fast path: PG18+ auto-reports search_path via ParameterStatus, so
        // the origin will push every change before the next RFQ. Defensive
        // marking and the piggyback SHOW just waste cycles.
        if self.search_path_auto_reported {
            return;
        }

        let Some(sql) = query_message_sql(&msg.data) else {
            return;
        };
        let Ok(ast) = pg_query::parse(sql) else {
            return;
        };

        if search_path_mutates_any(&ast) {
            debug!("search_path mutation detected in Query");
            self.search_path_mark_unknown();
        }

        // Piggyback only on single-statement, piggyback-safe mutations, and
        // only when no other intercept is active (otherwise the inline SHOW
        // response would collide with the existing intercept's state machine).
        if search_path_mutates_single_piggybackable(&ast).is_some()
            && matches!(self.origin_intercept, OriginIntercept::None)
            && let Some(rewritten) = query_message_append_show_search_path(&msg.data)
        {
            debug!("piggybacking SHOW search_path onto mutation query");
            msg.data = rewritten;
            self.origin_intercept =
                OriginIntercept::TrailingShowSearchPath(TrailingShowState::PreShow);
        }
    }

    /// Handle Parse message — analyze cacheability, store statement, buffer bytes.
    fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_parse_message(&msg.data) {
            let ast_result = pg_query::parse(&parsed.sql);

            // Detect search_path mutation at Parse time (pessimistic: the
            // client may Execute the statement or not). Piggyback isn't
            // attempted for extended protocol — a standalone SHOW will be
            // issued via the lazy path on the next RFQ once state is Unknown.
            // Skipped on PG18+ where ParameterStatus keeps us in sync.
            if !self.search_path_auto_reported
                && let Ok(ast) = &ast_result
                && search_path_mutates_any(ast)
            {
                debug!("search_path mutation detected in Parse");
                self.search_path_mark_unknown();
            }

            let sql_type = match ast_result {
                Ok(ast) => match query_expr_convert(&ast) {
                    Ok(query) => match CacheableQuery::try_new(&query, &self.func_volatility) {
                        Ok(cacheable_query) => StatementType::Cacheable(Arc::new(cacheable_query)),
                        Err(_) => StatementType::UncacheableSelect,
                    },
                    Err(_) => StatementType::NonSelect,
                },
                Err(_) => StatementType::ParseError,
            };

            let parse_bytes = msg.data.clone();
            let statement_name = parsed.statement_name.clone();
            self.statement_store(parsed, sql_type, parse_bytes);

            let buffer = self.extended.buffer_get_or_create();
            buffer.has_parse = true;
            buffer.parse_statement_name = Some(statement_name);
            buffer.bytes.extend_from_slice(&msg.data);
            trace!("net: Parse buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Bind message — store portal, buffer bytes.
    fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_bind_message(&msg.data) {
            self.portal_store(parsed);

            let buffer = self.extended.buffer_get_or_create();
            buffer.has_bind = true;
            buffer.bytes.extend_from_slice(&msg.data);
            trace!("net: Bind buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Execute message — record metrics, parse portal name, buffer bytes.
    /// Decision-making deferred to Sync.
    fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
        metrics::counter!(names::QUERIES_TOTAL).increment(1);
        metrics::counter!(names::PROTOCOL_EXTENDED_QUERIES).increment(1);
        self.telemetry.query_receive();

        let portal_name = parse_execute_message(&msg.data).ok().map(|p| p.portal_name);

        let buffer = self.extended.buffer_get_or_create();

        if buffer.execute_portal.is_some() {
            buffer.multiple_executes = true;
        } else {
            buffer.execute_portal = portal_name;
        }

        buffer.bytes.extend_from_slice(&msg.data);
        trace!("net: Execute buffered");
    }

    /// Attempt to create a cache message from the extended buffer at Sync time.
    /// Returns None if caching is not possible.
    fn buffer_try_cache(&self, buffer: &ExtendedBuffer) -> Option<CacheMessage> {
        if self.in_transaction || self.cache_disabled {
            return None;
        }
        if self.proxy_status != ProxyStatus::Normal {
            return None;
        }

        let portal_name = buffer.execute_portal.as_ref()?;
        let portal = self.portals.get(portal_name)?;

        // Only handle implicit or uniform result formats
        if portal.result_formats.len() > 1
            && !portal
                .result_formats
                .windows(2)
                .all(|w| matches!(w, [a, b] if a == b))
        {
            trace!("result format is not implicit or uniform");
            return None;
        }

        let stmt = self.prepared_statements.get(&portal.statement_name)?;

        let cacheable_query = match &stmt.sql_type {
            StatementType::Cacheable(query) => Arc::clone(query),
            StatementType::NonSelect
            | StatementType::UncacheableSelect
            | StatementType::ParseError => return None,
        };

        // Bind-without-Parse against an unknown statement is still cacheable:
        // `forward_lazy_parse_install` prepends the Parse on miss/forward.

        // Describe('S') in buffer: require cached parameter_description
        if buffer.describe == PipelineDescribe::Statement && stmt.parameter_description.is_none() {
            return None;
        }

        Some(CacheMessage::QueryParameterized(
            // Execute bytes are already in buffer.bytes; pass empty data since
            // the worker uses pipeline context's buffered_bytes for extended queries
            BytesMut::new(),
            cacheable_query,
            QueryParameters {
                values: portal.parameter_values.clone(),
                formats: portal.parameter_formats.clone(),
                oids: stmt.parameter_oids.clone(),
            },
            portal.result_formats.clone(),
        ))
    }

    /// Handle Describe message — buffer bytes and track describe metadata.
    fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_describe_message(&msg.data) {
            let buffer = self.extended.buffer_get_or_create();

            match parsed.describe_type {
                b'S' => {
                    buffer.describe = PipelineDescribe::Statement;
                    buffer.describe_statement_name = Some(parsed.name);
                }
                b'P' => {
                    buffer.describe = PipelineDescribe::Portal;
                }
                _ => {}
            }

            buffer.bytes.extend_from_slice(&msg.data);
            trace!("net: Describe buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Close message — flush buffer to origin, clean up state, forward Close.
    fn handle_close_message(&mut self, msg: PgFrontendMessage) {
        self.extended_buffer_flush_to_origin();
        if let Ok(parsed) = parse_close_message(&msg.data) {
            match parsed.close_type {
                b'S' => self.statement_close(&parsed.name),
                b'P' => self.portal_close(&parsed.name),
                _ => {}
            }
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Sync message — all cache vs. forward decision-making happens here.
    ///
    /// If the buffer contains exactly one cacheable Execute, dispatch to cache.
    /// Otherwise, forward the whole batch to origin.
    fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
        let Some(mut buffer) = self.extended.buffer_take() else {
            // Bare Sync; origin replies with one RFQ.
            trace!("net: proxy→origin Sync (no buffer)");
            self.egress.origin_open();
            self.origin_write_buf.push_back(msg.data);
            return;
        };

        // Try cache path: single Execute that is cacheable
        if !buffer.multiple_executes
            && buffer.execute_portal.is_some()
            && let Some(cache_msg) = self.buffer_try_cache(&buffer)
        {
            // Build PipelineContext from buffer for the cache/forward path
            let parameter_description = if buffer.describe == PipelineDescribe::Statement {
                buffer
                    .execute_portal
                    .as_ref()
                    .and_then(|p| self.portals.get(p))
                    .and_then(|portal| self.prepared_statements.get(&portal.statement_name))
                    .and_then(|stmt| stmt.parameter_description.clone())
            } else {
                None
            };

            // Append Sync bytes to buffer
            buffer.bytes.extend_from_slice(&msg.data);

            self.extended.pipeline_context = Some(PipelineContext {
                buffered_bytes: buffer.bytes,
                describe: buffer.describe,
                parameter_description,
                has_parse: buffer.has_parse,
                has_bind: buffer.has_bind,
            });

            // Track pending statement names for origin fallback path
            if buffer.has_parse {
                self.extended.pending_parse_statement = buffer.parse_statement_name.clone();
            }
            if buffer.describe_statement_name.is_some() {
                self.extended.pending_describe_statement = buffer.describe_statement_name;
            }
            // Bind-without-Parse against an origin-unknown statement: record
            // the name so the forward path can lazy-Parse on its way out.
            // Reset first so stale state from a prior cache hit can't leak.
            self.extended.pending_lazy_parse = None;
            if !buffer.has_parse
                && let Some(portal_name) = &buffer.execute_portal
                && let Some(portal) = self.portals.get(portal_name)
                && let Some(stmt) = self.prepared_statements.get(&portal.statement_name)
                && !stmt.origin_prepared
                && !portal.statement_name.is_empty()
            {
                self.extended.pending_lazy_parse = Some(portal.statement_name.clone());
            }

            // Create timing with fingerprint from the cacheable query
            let fingerprint = match &cache_msg {
                CacheMessage::Query(_, ast) | CacheMessage::QueryParameterized(_, ast, _, _) => {
                    query_expr_fingerprint(&ast.query)
                }
            };
            self.telemetry.cache_timing_start(fingerprint);

            trace!("net: Sync → cache dispatch");
            self.egress.cache_push(cache_msg);
            self.proxy_mode = ProxyMode::OriginDrain;
        } else if self.try_synthesize_parse_describe_response(&buffer) {
            trace!("net: Sync → synthesized ParseComplete+Describe response");
        } else {
            self.extended_buffer_forward_to_origin(buffer, &msg.data);
            trace!("net: Sync → origin (forwarded buffer)");
        }
    }

    /// Return the named statement targeted by a Parse-only / Parse+Describe('S')
    /// Sync batch that's eligible for synthesize. `None` if the batch shape,
    /// statement state, or session state disqualifies it.
    ///
    /// In-transaction is excluded because a statement Parsed mid-txn would
    /// resolve against the txn's snapshot. Portal Describe is excluded
    /// because no portal exists without a Bind. Unnamed statements are
    /// excluded because origin's unnamed slot is one-shot per Sync.
    fn synth_eligible<'a>(&self, buffer: &'a ExtendedBuffer) -> Option<&'a str> {
        if !buffer.has_parse || buffer.has_bind || buffer.execute_portal.is_some() {
            return None;
        }
        if buffer.describe == PipelineDescribe::Portal {
            return None;
        }
        if self.in_transaction {
            return None;
        }
        let stmt_name = buffer.parse_statement_name.as_deref()?;
        if stmt_name.is_empty() {
            return None;
        }
        let stmt = self.prepared_statements.get(stmt_name)?;
        if !matches!(stmt.sql_type, StatementType::Cacheable(_)) {
            return None;
        }
        Some(stmt_name)
    }

    /// Attempt to serve a `Parse+Describe('S')+Sync` (or `Parse+Sync`) batch
    /// from the per-connection describe-response cache. Returns `true` on
    /// hit, in which case the synthesized response was pushed (or deferred)
    /// and the caller must not forward to origin. Returns `false` on miss
    /// or ineligible batch — caller falls through to the normal forward.
    fn try_synthesize_parse_describe_response(&mut self, buffer: &ExtendedBuffer) -> bool {
        let Some(stmt_name) = self.synth_eligible(buffer) else {
            return false;
        };
        // synth_eligible already verified the statement exists.
        let Some(stmt) = self.prepared_statements.get(stmt_name) else {
            return false;
        };
        let key = DescribeKey {
            sql: stmt.sql.clone(),
            parameter_oids: stmt.client_parameter_oids.clone(),
        };
        let Some(entry) = self.describe_cache.get(&key) else {
            metrics::counter!(names::PROTOCOL_DESCRIBE_CACHE_MISSES).increment(1);
            return false;
        };
        let parameter_description = entry.parameter_description.clone();
        let row_description = entry.row_description.clone();
        let stmt_name = stmt_name.to_owned();

        // Populate the freshly-Parsed statement with the cached Describe
        // metadata so a subsequent Bind+Execute can build a parameterized
        // cache message without an origin round-trip.
        let parsed_param_oids = parse_parameter_description(&parameter_description)
            .ok()
            .map(|p| p.parameter_oids);
        if let Some(stmt_mut) = self.prepared_statements.get_mut(&stmt_name) {
            if let Some(oids) = parsed_param_oids {
                stmt_mut.parameter_oids = oids;
            }
            stmt_mut.parameter_description = Some(parameter_description.clone());
            stmt_mut.row_description = row_description.clone();
            stmt_mut.describe_no_data = row_description.is_none();
        }

        metrics::counter!(names::PROTOCOL_DESCRIBE_CACHE_HITS).increment(1);

        let mut out = BytesMut::with_capacity(
            5 + parameter_description.len()
                + row_description.as_ref().map(BytesMut::len).unwrap_or(5)
                + 6,
        );
        // ParseComplete
        out.put_slice(&[b'1', 0, 0, 0, 4]);
        if buffer.describe == PipelineDescribe::Statement {
            out.put_slice(&parameter_description);
            if let Some(row_desc) = row_description {
                out.put_slice(&row_desc);
            } else {
                // NoData: tag 'n', length 4 (length field only)
                out.put_slice(&[b'n', 0, 0, 0, 4]);
            }
        }
        // ReadyForQuery 'I' — synth_eligible excluded in_transaction.
        out.put_slice(&[b'Z', 0, 0, 0, 5, b'I']);

        // Enqueue as an ordered slot: the egress queue keeps it behind any
        // earlier in-flight origin response so the synth bytes can't jump ahead.
        self.egress.synth_push(out.freeze());

        true
    }

    /// Handle Flush message — forward buffer to origin, no cache attempt.
    /// Handles JDBC pattern: Parse/Bind/Describe/Flush then Execute/Sync.
    fn handle_flush_message(&mut self, msg: PgFrontendMessage) {
        let Some(buffer) = self.extended.buffer_take() else {
            self.origin_write_buf.push_back(msg.data);
            return;
        };

        self.extended_buffer_forward_to_origin(buffer, &msg.data);
        // The forwarded batch ends in Flush, not Sync — origin will send the
        // describe response with no ReadyForQuery. Mark the opened egress slot
        // so the next client message seals it (see `handle_client_message`).
        self.flush_describe_pending = true;
        trace!("net: Flush → origin (forwarded buffer)");
    }

    /// Store a prepared statement in connection state.
    ///
    /// For unnamed statements (empty name), always overwrite — the protocol allows reuse of
    /// the unnamed slot with a new Parse. For named statements, `or_insert` preserves existing
    /// metadata (parameter_description, origin_prepared) accumulated during the cold path.
    fn statement_store(
        &mut self,
        parsed: ParsedParseMessage,
        sql_type: StatementType,
        parse_bytes: BytesMut,
    ) {
        let client_parameter_oids = parsed.parameter_oids.clone();
        let stmt = PreparedStatement {
            name: parsed.statement_name.clone(),
            sql: parsed.sql,
            parameter_oids: parsed.parameter_oids,
            client_parameter_oids,
            sql_type,
            parameter_description: None,
            row_description: None,
            describe_no_data: false,
            origin_prepared: false,
            parse_bytes: Some(parse_bytes),
        };
        debug!("parsed statement insert {}", parsed.statement_name);

        if parsed.statement_name.is_empty() {
            // Unnamed statement: always overwrite per protocol spec
            self.prepared_statements.insert(parsed.statement_name, stmt);
        } else {
            // Named statement: preserve existing metadata from first cold path
            if !self
                .prepared_statements
                .contains_key(&parsed.statement_name)
            {
                metrics::gauge!(names::PROTOCOL_PREPARED_STATEMENTS).increment(1.0);
            }
            self.prepared_statements
                .entry(parsed.statement_name)
                .or_insert(stmt);
        }
    }

    /// Store a portal in connection state.
    fn portal_store(&mut self, parsed: ParsedBindMessage) {
        let portal = Portal {
            name: parsed.portal_name.clone(),
            statement_name: parsed.statement_name,
            parameter_values: parsed.parameter_values,
            parameter_formats: parsed.parameter_formats,
            result_formats: parsed.result_formats,
        };

        debug!("parsed portal insert {:?}", portal);
        self.portals.insert(parsed.portal_name, portal);
    }

    /// Remove a prepared statement from connection state.
    fn statement_close(&mut self, name: &str) {
        if self.prepared_statements.remove(name).is_some() {
            metrics::gauge!(names::PROTOCOL_PREPARED_STATEMENTS).decrement(1.0);
        }
    }

    /// Remove a portal from connection state.
    fn portal_close(&mut self, name: &str) {
        self.portals.remove(name);
    }

    /// Clear all prepared statements from connection state.
    #[expect(unused)]
    fn statements_clear(&mut self) {
        self.prepared_statements.clear();
    }

    /// Clear all portals from connection state.
    #[expect(unused)]
    fn portals_clear(&mut self) {
        self.portals.clear();
    }

    /// Dispatch the result of an origin-read poll: handle the message, or map a
    /// decode error / EOF to a connection error. Shared by the select loops.
    fn origin_read_dispatch(
        &mut self,
        res: Option<Result<PgBackendMessage, ProtocolError>>,
    ) -> ConnectionResult<()> {
        match res {
            Some(Ok(msg)) => {
                self.handle_origin_message(msg);
                Ok(())
            }
            Some(Err(err)) => {
                debug!("origin read error [{}]", err);
                Err(ConnectionError::ProtocolError(err).into())
            }
            None => {
                debug!("origin stream closed");
                Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "origin disconnected",
                ))
                .into())
            }
        }
    }

    /// Write one buffered chunk to origin, popping it once fully drained. Guard
    /// the call site with `!self.origin_write_buf.is_empty()`.
    #[expect(
        clippy::indexing_slicing,
        reason = "VecDeque access guarded by !is_empty() at the call site"
    )]
    async fn origin_write_flush(
        &mut self,
        origin_write: &mut Pin<&mut OriginWriteHalf<'_>>,
    ) -> ConnectionResult<()> {
        origin_write
            .write_buf(&mut self.origin_write_buf[0])
            .await
            .map_err(ConnectionError::IoError)?;
        if !self.origin_write_buf[0].has_remaining() {
            self.origin_write_buf.pop_front();
        }
        Ok(())
    }

    /// Write the next ready egress chunk to the client. Guard the call site with
    /// `self.egress.has_writable()`.
    async fn client_egress_flush(
        &mut self,
        client_write: &mut Pin<&mut ClientWriteHalf<'_>>,
    ) -> ConnectionResult<()> {
        let n = client_write
            .write(self.egress.write_chunk())
            .await
            .map_err(ConnectionError::IoError)?;
        self.egress.advance(n);
        Ok(())
    }

    async fn connection_select<'a, 'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        client_read: &mut Pin<&mut FramedRead<ClientReadHalf<'a>, PgFrontendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut Pin<&mut ClientWriteHalf<'a>>,
    ) -> ConnectionResult<()> {
        select! {
            res = client_read.next() => {
                match res {
                    Some(Ok(msg)) => {
                        self.handle_client_message(msg).await;
                    }
                    Some(Err(err)) => {
                        debug!("client read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err).into());
                    }
                    None => {
                        debug!("client stream closed");
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "client disconnected",
                        )).into());
                    }
                }
            }
            res = origin_read.next() => {
                self.origin_read_dispatch(res)?;
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                self.origin_write_flush(origin_write).await?;
            }
            _ = client_write.writable(), if self.egress.has_writable() => {
                self.client_egress_flush(client_write).await?;
            }
        };

        Ok(())
    }

    async fn connection_select_with_cache<'a, 'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut Pin<&mut ClientWriteHalf<'a>>,
    ) -> ConnectionResult<()> {
        // Extract cache_rx from self.proxy_mode
        let ProxyMode::CacheRead(ref mut cache_rx) = self.proxy_mode else {
            return Err(ConnectionError::IoError(io::Error::new(
                io::ErrorKind::InvalidData,
                "Expected CacheRead mode",
            ))
            .into());
        };

        select! {
            res = origin_read.next() => {
                self.origin_read_dispatch(res)?;
            }
            reply = &mut *cache_rx => {
                match reply {
                    Ok(reply) => {
                        self.handle_cache_reply(reply);
                    }
                    Err(_) => {
                        debug!("cache channel closed");
                        return Err(ConnectionError::CacheDead.into());
                    }
                }
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                self.origin_write_flush(origin_write).await?;
            }
            _ = client_write.writable(), if self.egress.has_writable() => {
                self.client_egress_flush(client_write).await?;
            }
        };

        Ok(())
    }

    /// Select loop for `OriginDrain`: a cacheable query is queued behind earlier
    /// responses. Drains origin and flushes the egress queue **without reading
    /// the client**, so no later request is processed before the queued cache
    /// query — preserving response order until the cache slot reaches the head.
    async fn connection_select_drain<'a, 'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut Pin<&mut ClientWriteHalf<'a>>,
    ) -> ConnectionResult<()> {
        select! {
            res = origin_read.next() => {
                self.origin_read_dispatch(res)?;
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                self.origin_write_flush(origin_write).await?;
            }
            _ = client_write.writable(), if self.egress.has_writable() => {
                self.client_egress_flush(client_write).await?;
            }
        };

        Ok(())
    }
}

/// Connect to the origin database server.
/// Tries each address in sequence until one succeeds.
/// If ssl_mode is Require, performs PostgreSQL SSL negotiation and TLS handshake.
async fn origin_connect(
    addrs: &[SocketAddr],
    ssl_mode: SslMode,
    server_name: &str,
) -> ConnectionResult<OriginStream> {
    for addr in addrs {
        if let Ok(stream) = TcpStream::connect(addr).await {
            let _ = stream.set_nodelay(true);
            return match ssl_mode {
                SslMode::Disable => Ok(TlsStream::plain(stream)),
                SslMode::Require | SslMode::VerifyFull => {
                    let tls_stream = tls::pg_tls_connect(stream, ssl_mode, server_name)
                        .await
                        .map_err(|e| {
                            Report::from(ConnectionError::TlsError(io::Error::other(
                                e.into_current_context(),
                            )))
                        })
                        .attach_loc("establishing TLS connection")?;
                    Ok(origin_stream_from_tls(tls_stream))
                }
            };
        }
    }
    Err(ConnectionError::NoConnection.into())
}

/// Forward a cacheable query's bytes to origin when cache dispatch isn't
/// possible (search_path unknown, socket creation failure). Sends either the
/// buffered pipeline bytes or the raw message data. The query's egress slot has
/// already been converted to an `Origin` slot by `cache_to_origin`, so this does
/// not open a new one. Free function to allow disjoint field borrows.
fn cache_forward_to_origin(
    extended: &mut ExtendedPending,
    origin_write_buf: &mut VecDeque<BytesMut>,
    msg: CacheMessage,
) {
    let bytes = extended
        .pipeline_take()
        .map(|p| p.buffered_bytes)
        .unwrap_or_else(|| msg.into_data());
    origin_write_buf.push_back(bytes);
}

/// Prepend a `Parse` to `origin_write_buf` when origin doesn't yet know the
/// statement. No-op if origin already knows it, if no `parse_bytes` were
/// captured, if the statement is unnamed (origin's unnamed slot doesn't
/// persist across Sync), or if another `OriginIntercept` is already active.
///
/// Free function (not a method) to allow disjoint field borrows from the
/// event-loop call sites that work with `state.*` after partial moves.
fn forward_lazy_parse_install(
    stmt_name: &str,
    prepared_statements: &HashMap<String, PreparedStatement>,
    extended: &mut ExtendedPending,
    origin_write_buf: &mut VecDeque<BytesMut>,
    origin_intercept: &mut OriginIntercept,
) {
    if stmt_name.is_empty() {
        return;
    }
    if !matches!(origin_intercept, OriginIntercept::None) {
        return;
    }
    let Some(stmt) = prepared_statements.get(stmt_name) else {
        return;
    };
    if stmt.origin_prepared {
        return;
    }
    let Some(parse_bytes) = stmt.parse_bytes.clone() else {
        return;
    };
    origin_write_buf.push_back(parse_bytes);
    *origin_intercept = OriginIntercept::LazyParseInline {
        statement_name: stmt_name.to_owned(),
    };
    extended.pending_parse_statement = Some(stmt_name.to_owned());
    metrics::counter!(names::PROTOCOL_LAZY_PARSE_FORWARDED).increment(1);
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_connection(
    mut client_stream: ClientStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: &str,
    cache_sender: CacheSender,
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,
    origin_database: EcoString,
) -> ConnectionResult<()> {
    // Track active connections - guard ensures decrement on any exit path
    metrics::gauge!(names::CONNECTIONS_ACTIVE).increment(1.0);
    let _connection_guard = ActiveConnectionGuard;

    // Create ClientSocketSource BEFORE splitting (captures raw fd and TLS state)
    let client_socket_source = client_stream.socket_source_create();

    // Connect to origin database (with TLS if required)
    let mut origin_stream = origin_connect(&addrs, ssl_mode, server_name)
        .await
        .attach_loc("connecting to origin")?;

    // Split origin stream (borrowed halves with .writable() support)
    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    // Split client stream in place (borrowed halves with .writable() support)
    let (client_read, client_write) = client_stream.split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    // Initialize connection state with socket source
    let mut state = ConnectionState::new(client_socket_source, func_volatility, origin_database);

    tokio::pin!(origin_framed_read);
    tokio::pin!(client_framed_read);
    tokio::pin!(origin_write);
    tokio::pin!(client_write);

    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                if let Err(err) = state
                    .connection_select(
                        &mut origin_framed_read,
                        &mut client_framed_read,
                        &mut origin_write,
                        &mut client_write,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
                    break;
                }
            }
            ProxyMode::CacheRead(_) => {
                if let Err(err) = state
                    .connection_select_with_cache(
                        &mut origin_framed_read,
                        &mut origin_write,
                        &mut client_write,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
                    // if matches!(err.current_context(), ConnectionError::CacheDead) {
                    //     state.proxy_status = ProxyStatus::Degraded;
                    // }
                    break;
                }
            }
            ProxyMode::OriginDrain => {
                // The cacheable query is queued in the egress queue. `cache_dispatch`
                // returns it (and marks its slot serving) only once the slot has
                // reached the head; until then we drain origin and flush earlier
                // responses without reading the client (no read-ahead), so the
                // cache response can't jump ahead of an in-flight origin response.
                let Some(msg) = state.egress.cache_dispatch() else {
                    if let Err(err) = state
                        .connection_select_drain(
                            &mut origin_framed_read,
                            &mut origin_write,
                            &mut client_write,
                        )
                        .await
                    {
                        debug!("read error [{}]", err);
                        break;
                    }
                    continue;
                };

                // The cache query is in hand and its slot is serving. Resolve
                // search_path; if unknown, forward to origin instead of caching.
                let Some(resolved_search_path) = state
                    .search_path_state
                    .resolve(state.session_user.as_deref())
                else {
                    debug!("search_path unknown, forwarding to origin");
                    metrics::counter!(names::QUERIES_UNCACHEABLE).increment(1);
                    state.cache_slot_forward_to_origin(msg);
                    continue;
                };

                metrics::counter!(names::QUERIES_CACHEABLE).increment(1);

                // Create ClientSocket for this query (dupes the fd)
                let client_socket = match state.client_socket_source.socket_create() {
                    Ok(s) => s,
                    Err(e) => {
                        error!("Failed to create client socket: {}", e);
                        state.cache_slot_forward_to_origin(msg);
                        continue;
                    }
                };

                let (reply_tx, reply_rx) = oneshot::channel();

                let timing = state.telemetry.cache_timing_dispatch();

                let proxy_msg = ProxyMessage {
                    message: msg,
                    client_socket,
                    reply_tx,
                    search_path: resolved_search_path,
                    timing,
                    pipeline: state.extended.pipeline_take(),
                };

                match cache_sender.send(proxy_msg).await {
                    Ok(()) => {
                        state.proxy_mode = ProxyMode::CacheRead(reply_rx);
                    }
                    Err(e) => {
                        // Cache is unavailable, fall back to proxying directly to origin.
                        debug!("cache unavailable");
                        state.proxy_status = ProxyStatus::Degraded;
                        let proxy_msg = e.into_message();
                        // The cache slot is now serving (message already taken);
                        // convert it back to an origin slot in place.
                        state.egress.cache_to_origin();
                        if let Some(pipeline) = proxy_msg.pipeline {
                            state.origin_write_buf.push_back(pipeline.buffered_bytes);
                        } else {
                            state
                                .origin_write_buf
                                .push_back(proxy_msg.message.into_data());
                        }
                        state.proxy_mode = ProxyMode::Read;
                    }
                }
            }
        }
    }

    // Clean up prepared statements gauge before connection state is dropped
    let remaining_stmts = state.prepared_statements.len();
    if remaining_stmts > 0 {
        // Gauge value; per-connection prepared statement count never approaches 2^53.
        #[allow(clippy::cast_precision_loss)]
        metrics::gauge!(names::PROTOCOL_PREPARED_STATEMENTS).decrement(remaining_stmts as f64);
    }

    match state.proxy_status {
        ProxyStatus::Degraded => Err(ConnectionError::CacheDead.into()),
        ProxyStatus::Normal => Ok(()),
    }
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn connection_run(
    worker_id: usize,
    settings: &Settings,
    mut rx: UnboundedReceiver<TcpStream>,
    cache_sender: CacheSender,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    func_volatility: Arc<HashMap<String, FunctionVolatility>>,
) -> ConnectionResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<ConnectionError>()
        .attach_loc("creating connection runtime")?;

    // Extract settings for the connection loop
    let ssl_mode = settings.origin.ssl_mode;
    let server_name = settings.origin.host.clone();
    let origin_database = EcoString::from(settings.origin.database.as_str());

    debug!("handle connection start");
    rt.block_on(async {
        let addrs: Vec<SocketAddr> =
            lookup_host((settings.origin.host.as_str(), settings.origin.port))
                .await
                .map_into_report::<ConnectionError>()
                .attach_loc("resolving origin host")?
                .collect();

        LocalSet::new()
            .run_until(async {
                while let Some(socket) = rx.recv().await {
                    // Channel depth gauge; queue length never approaches 2^53.
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!(names::PROXY_WORKER_QUEUE, "worker" => worker_id.to_string())
                        .set(rx.len() as f64);

                    let addrs = addrs.clone();
                    let server_name = server_name.clone();
                    let cache_sender = cache_sender.clone();
                    let tls_acceptor = tls_acceptor.clone();
                    let func_volatility = Arc::clone(&func_volatility);
                    let origin_database = origin_database.clone();
                    spawn_local(async move {
                        debug!("task spawn");

                        // Negotiate client TLS if configured
                        let client_stream = match tls::client_tls_negotiate(
                            socket,
                            tls_acceptor.as_deref(),
                        )
                        .await
                        {
                            Ok(tls::ClientTlsResult::Tls {
                                tcp_stream,
                                tls_state,
                            }) => ClientStream::tls(tcp_stream, tls_state),
                            Ok(tls::ClientTlsResult::Plain(stream)) => ClientStream::plain(stream),
                            Err(e) => {
                                metrics::counter!(names::CONNECTIONS_ERRORS).increment(1);
                                error!("TLS negotiation failed: {}", e);
                                return Ok(());
                            }
                        };

                        let res = handle_connection(
                            client_stream,
                            addrs,
                            ssl_mode,
                            &server_name,
                            cache_sender,
                            func_volatility,
                            origin_database,
                        )
                        .await;

                        if let Err(e) = res {
                            error!("{}", e);
                            metrics::counter!(names::CONNECTIONS_ERRORS).increment(1);
                            if matches!(e.current_context(), ConnectionError::CacheDead) {
                                debug!("connection closed in degraded mode");
                                return Err(io::Error::other("cache dead"));
                            }
                        }

                        debug!("task done");
                        Ok(())
                    });
                }

                Ok(())
            })
            .await
    })
}
