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
use tokio::{io::AsyncWriteExt, net::TcpStream, select, sync::oneshot};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, BufMut, Bytes, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument, trace, warn};

use crate::{
    cache::{
        CacheDispatchHandle, CacheMessage, CacheOutcome, CacheReply, ProxyMessage, QueryParameters,
        messages::{MessageSlices, PipelineContext, PipelineDescribe, slices_concat},
        query::CacheableQuery,
    },
    pg::protocol::{
        ProtocolError,
        backend::{
            AUTHENTICATION_SASL, PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType,
            authentication_type, data_row_first_column, parameter_status_parse,
        },
        encode::{CLOSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG},
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
    query::ast::{AstError, QueryExpr, query_expr_convert_raw, query_expr_fingerprint},
    settings::SslMode,
    telemetry::pg_version_set,
    timing::{QueryId, QueryTiming, timing_record},
    tls::{self},
};

use super::client_stream::{ClientSocket, ClientStream, OwnedClientReadHalf};
use super::query::{Action, ForwardReason, handle_query};
use super::search_path::{SearchPath, search_path_mutations_raw};
use super::tls_stream::{TlsReadHalf, TlsStream, TlsWriteHalf};
use super::{ConnectionError, ConnectionResult, ProxyMode, ProxyStatus};
use crate::result::ReportExt;

/// Guard that decrements active connections gauge when dropped.
struct ActiveConnectionGuard;

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        crate::metrics::handles().conn.active.decrement(1.0);
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
    parameter_description: Bytes,
    row_description: Option<Bytes>,
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
    LazyParseInline { statement_name: EcoString },
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
    fn resolve(&self, session_user: Option<&str>) -> Option<Vec<EcoString>> {
        match self {
            Self::Unknown => None,
            Self::Resolved(sp) => Some(
                sp.resolve(session_user)
                    .into_iter()
                    .map(EcoString::from)
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
    /// pipeline (dispatch → worker) and back, only set for cache-path queries
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
        let m = crate::metrics::handles();
        if let Some(start) = self.origin_sent_at.take() {
            m.query
                .origin_execution
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(start) = self.client_received_at.take() {
            m.query.origin_latency.record(start.elapsed().as_secs_f64());
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
            crate::metrics::handles()
                .query
                .cache_latency
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

    /// Extended protocol: prepared statements by name
    prepared_statements: HashMap<EcoString, PreparedStatement>,

    /// Extended protocol: portals (bound statements) by name
    portals: HashMap<EcoString, Portal>,

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
    func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,

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

/// Wire bytes of a bare `Sync` message (`'S'` + length 4). Synthesized to close
/// origin's implicit transaction when a multi-execute cache batch falls back to
/// forwarding (the client's own `Sync` isn't replayed per entry).
const SYNC_MESSAGE: [u8; 5] = [b'S', 0, 0, 0, 4];

/// A cacheable-query snapshot captured at Execute time. Taken eagerly (not at
/// Sync) because a multi-execute batch typically reuses the unnamed portal /
/// statement, so `self.portals` / `prepared_statements` reflect only the *last*
/// Bind/Parse by Sync time. Snapshotting per entry keeps each execute's own
/// parameters and query.
struct CacheCandidate {
    cacheable_query: Arc<CacheableQuery>,
    parameters: QueryParameters,
    result_formats: Vec<i16>,
    /// ParameterDescription bytes, present only for a Describe('S') entry.
    parameter_description: Option<Bytes>,
    /// Target statement name (for the lazy-Parse-on-forward decision).
    statement_name: EcoString,
    /// Whether origin already knows the statement (no lazy Parse needed).
    origin_prepared: bool,
}

impl CacheCandidate {
    /// Whether forwarding a Bind-without-Parse execute against this candidate
    /// requires prepending a lazy Parse (origin doesn't know the named
    /// statement). Combine with the entry's `has_parse` at the call site.
    fn lazy_parse_needed(&self) -> bool {
        !self.origin_prepared && !self.statement_name.is_empty()
    }
}

/// Parse/Bind/Describe messages accumulated toward the next Execute. Sealed
/// into an [`ExecuteEntry`] when an Execute arrives.
#[derive(Default)]
struct Segment {
    /// Raw bytes of the segment's messages, one refcounted slice per message in
    /// arrival order. Accumulating `Bytes` (zero-copy frozen from the codec
    /// split) avoids deep-copying every Parse/Bind/Describe into a contiguous
    /// buffer that is only ever needed on the (cold) forward path. Inline-stored
    /// (`MessageSlices`) so the common segment never heap-allocates.
    bytes: MessageSlices,
    /// Whether a Parse was buffered in this segment.
    has_parse: bool,
    /// Whether a Bind was buffered in this segment.
    has_bind: bool,
    /// Whether/what Describe was buffered in this segment.
    describe: PipelineDescribe,
    /// Statement name of each Parse in this segment, in order. One per Parse —
    /// a dirty segment has several. Drives `pending_parse_statements` on forward.
    parse_statement_names: Vec<EcoString>,
    /// Statement name of each Describe('S') in this segment, in order.
    describe_statement_names: Vec<EcoString>,
    /// True once the segment holds more than one of any Parse/Bind/Describe —
    /// i.e. more than one executable's worth of prep. Such a segment can't be
    /// served from cache (the worker synthesizes exactly one ParseComplete /
    /// BindComplete / Describe response), so it forces the forward path.
    dirty: bool,
}

/// One Execute plus the Parse/Bind/Describe messages that preceded it since the
/// previous Execute (or batch start). Sealed at Execute; carries its own bytes
/// so it can be dispatched independently (cached) or concatenated for forward.
struct ExecuteEntry {
    /// Raw bytes of this execute's Parse/Bind/Describe/Execute run, one
    /// refcounted slice per message in order.
    bytes: MessageSlices,
    /// Portal name from Execute (None if the Execute failed to parse).
    portal_name: Option<EcoString>,
    has_parse: bool,
    has_bind: bool,
    describe: PipelineDescribe,
    /// Statement name of each Parse / Describe('S') in this entry, in order.
    /// A clean (cacheable) entry has at most one of each.
    parse_statement_names: Vec<EcoString>,
    describe_statement_names: Vec<EcoString>,
    /// Carried from the segment: more than one P/B/D, so not cacheable.
    dirty: bool,
    /// Cacheable-query snapshot captured at Execute time, if this execute is a
    /// cacheable SELECT with a resolvable portal. `None` ⇒ not cacheable.
    candidate: Option<CacheCandidate>,
}

impl ExecuteEntry {
    /// Whether forwarding this entry to origin requires prepending a lazy Parse
    /// (Bind-without-Parse against a named statement origin doesn't yet know).
    fn needs_lazy_parse(&self) -> bool {
        !self.has_parse
            && self
                .candidate
                .as_ref()
                .is_some_and(CacheCandidate::lazy_parse_needed)
    }
}

/// Buffered extended protocol messages, accumulated until Sync/Flush.
/// All decision-making (cache vs. forward) is deferred to Sync/Flush time.
#[derive(Default)]
struct ExtendedBuffer {
    /// Sealed executes, in arrival order. One per Execute message.
    entries: Vec<ExecuteEntry>,
    /// Messages accumulated since the last Execute (or batch start).
    pending: Segment,
}

impl ExtendedBuffer {
    /// Seal the pending segment together with this Execute's bytes into an entry.
    fn pending_seal(
        &mut self,
        execute_bytes: &[u8],
        portal_name: Option<EcoString>,
        candidate: Option<CacheCandidate>,
    ) {
        let mut seg = std::mem::take(&mut self.pending);
        seg.bytes.push(Bytes::copy_from_slice(execute_bytes));
        self.entries.push(ExecuteEntry {
            bytes: seg.bytes,
            portal_name,
            has_parse: seg.has_parse,
            has_bind: seg.has_bind,
            describe: seg.describe,
            parse_statement_names: seg.parse_statement_names,
            describe_statement_names: seg.describe_statement_names,
            dirty: seg.dirty,
            candidate,
        });
    }

    /// Every Parse statement name across the window in wire order (entries then
    /// the trailing pending segment) — one per Parse. Drives the ordered
    /// `pending_parse_statements` queue so each origin ParseComplete marks the
    /// right statement `origin_prepared`.
    fn parse_statements_all(&self) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .flat_map(|e| e.parse_statement_names.iter())
            .chain(self.pending.parse_statement_names.iter())
            .map(EcoString::as_str)
    }

    /// Every Describe('S') statement name across the window in wire order.
    fn describe_statements_all(&self) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .flat_map(|e| e.describe_statement_names.iter())
            .chain(self.pending.describe_statement_names.iter())
            .map(EcoString::as_str)
    }

    /// Concatenate all buffered bytes (entries in order, then the trailing
    /// pending segment) into the wire stream as originally received. Only the
    /// (cold) forward path needs the contiguous form.
    fn bytes_concat(&self) -> BytesMut {
        let mut out = BytesMut::new();
        for entry in &self.entries {
            for slice in &entry.bytes {
                out.extend_from_slice(slice);
            }
        }
        for slice in &self.pending.bytes {
            out.extend_from_slice(slice);
        }
        out
    }

    /// Whether any Parse was buffered across the whole window.
    fn any_has_parse(&self) -> bool {
        self.pending.has_parse || self.entries.iter().any(|e| e.has_parse)
    }
}

/// State for the extended query protocol pipeline.
/// Accumulates messages until Sync/Flush, then tracks pending origin responses
/// and pipeline context for cache dispatch.
struct ExtendedPending {
    /// Statement names whose ParseCompletes we're awaiting from origin, in wire
    /// order — one per forwarded Parse. Each origin ParseComplete pops the front
    /// and marks that statement `origin_prepared`.
    pending_parse_statements: VecDeque<EcoString>,

    /// Statement names being described, in wire order — one per forwarded
    /// Describe('S'). ParameterDescription peeks the front; RowDescription/NoData
    /// pops it.
    pending_describe_statements: VecDeque<EcoString>,

    /// Statement name to lazily Parse on the next origin forward. Set at Sync
    /// time for Bind-without-Parse batches against statements origin doesn't
    /// know; consumed by the forward paths in `handle_cache_reply`. Cleared on
    /// every Sync so stale state from a prior cache hit doesn't leak.
    pending_lazy_parse: Option<EcoString>,

    /// Buffered extended protocol messages accumulated until Sync/Flush.
    /// Decision-making deferred to Sync time.
    buffer: Option<ExtendedBuffer>,

    /// Pipeline context ready for cache dispatch.
    /// Built at Sync time from ExtendedBuffer, consumed by ProxyMessage.
    pipeline_context: Option<PipelineContext>,

    /// Remaining cache slots of a multi-execute batch, queued in order. The
    /// current in-flight slot lives in `pipeline_context` (+ the egress Cache
    /// slot); each hit advances to the next here. On a miss the remainder is
    /// forwarded to origin as one run.
    batch: VecDeque<DispatchContext>,

    /// Whether the in-flight cache dispatch is an extended-protocol pipeline (vs
    /// a self-terminating simple `Query`). Gates the synthesized trailing `Sync`
    /// on the forward-fallback path: extended entries carry no `Sync`, a simple
    /// `Query` already triggers its own `ReadyForQuery`.
    dispatch_is_extended: bool,

    /// Count of `Close(statement)` messages handled locally (statement never
    /// `origin_prepared`, so the origin never knew it) whose `CloseComplete` is
    /// still owed to the client. Synthesized — and the counter reset — at the
    /// next Sync (or before any origin forward, to preserve response order).
    /// PGC-234: avoids forwarding useless Close+Sync round-trips to origin for
    /// cache-served statements.
    deferred_close_completes: u32,

    /// Whether anything was forwarded to origin in the current Sync group (a
    /// forwarded Close or a Flush). Gates the bare-Sync local-`ReadyForQuery`
    /// optimization: only synthesize the RFQ when the group is purely local.
    group_origin_forwarded: bool,
}

/// Everything needed to dispatch one execute as a cache slot, computed at Sync
/// from an [`ExecuteEntry`]'s snapshot. Held in `ExtendedPending::batch` until
/// its turn; on dispatch the pipeline/statement state is applied to the
/// connection and the message is leased to a worker.
struct DispatchContext {
    msg: CacheMessage,
    pipeline: PipelineContext,
    fingerprint: u64,
    lazy_parse: Option<EcoString>,
    parse_statement: Option<EcoString>,
    describe_statement: Option<EcoString>,
}

impl DispatchContext {
    /// Assemble a dispatch context from an entry and its cache candidate.
    /// `is_last` carries the single trailing `ReadyForQuery` for the batch.
    fn build(entry: ExecuteEntry, candidate: CacheCandidate, is_last: bool) -> Self {
        let fingerprint = query_expr_fingerprint(&candidate.cacheable_query.query);
        let lazy_parse = (!entry.has_parse && candidate.lazy_parse_needed())
            .then(|| candidate.statement_name.clone());
        let pipeline = PipelineContext {
            buffered_bytes: entry.bytes,
            describe: entry.describe,
            parameter_description: candidate.parameter_description,
            has_parse: entry.has_parse,
            has_bind: entry.has_bind,
            emit_rfq: is_last,
        };
        let msg = CacheMessage::QueryParameterized(
            BytesMut::new(),
            candidate.cacheable_query,
            candidate.parameters,
            candidate.result_formats,
        );
        Self {
            msg,
            pipeline,
            fingerprint,
            lazy_parse,
            // A cacheable (non-dirty) entry has at most one Parse / Describe('S').
            parse_statement: if entry.has_parse {
                entry.parse_statement_names.into_iter().next()
            } else {
                None
            },
            describe_statement: entry.describe_statement_names.into_iter().next(),
        }
    }
}

impl ExtendedPending {
    fn new() -> Self {
        Self {
            pending_parse_statements: VecDeque::new(),
            pending_describe_statements: VecDeque::new(),
            pending_lazy_parse: None,
            buffer: None,
            pipeline_context: None,
            batch: VecDeque::new(),
            dispatch_is_extended: false,
            deferred_close_completes: 0,
            group_origin_forwarded: false,
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

    /// Capture every forwarded Parse/Describe('S') statement name, in wire
    /// order, into the pending origin-response queues (shared by the flush and
    /// forward paths). Replaces any prior contents — this forwards a whole
    /// buffer, so the queues describe exactly its responses.
    fn pending_statements_capture(&mut self, buffer: &ExtendedBuffer) {
        self.pending_parse_statements =
            buffer.parse_statements_all().map(EcoString::from).collect();
        self.pending_describe_statements = buffer
            .describe_statements_all()
            .map(EcoString::from)
            .collect();
    }

    /// Flush any buffered extended protocol messages.
    /// Extracts pending statement names from buffer metadata.
    /// Returns the buffer's bytes for the caller to push to origin.
    fn buffer_flush(&mut self) -> Option<BytesMut> {
        let buffer = self.buffer.take()?;
        self.pending_statements_capture(&buffer);
        Some(buffer.bytes_concat())
    }

    /// Forward buffer to origin with trailing bytes (Sync or Flush).
    /// Extracts pending statement names from buffer metadata.
    /// Returns bytes to push to origin.
    fn buffer_forward(&mut self, buffer: ExtendedBuffer, trailing_bytes: &[u8]) -> BytesMut {
        self.pending_statements_capture(&buffer);
        let mut bytes = buffer.bytes_concat();
        bytes.extend_from_slice(trailing_bytes);
        bytes
    }

    /// Handle ParseComplete from origin: mark the next awaited statement as
    /// origin_prepared (one ParseComplete per forwarded Parse, in order).
    fn parse_complete(&mut self, prepared_statements: &mut HashMap<EcoString, PreparedStatement>) {
        if let Some(stmt_name) = self.pending_parse_statements.pop_front()
            && let Some(stmt) = prepared_statements.get_mut(stmt_name.as_str())
        {
            stmt.origin_prepared = true;
            trace!("origin_prepared set for statement '{}'", stmt_name);
        }
    }

    /// Update the front pending statement's parameter OIDs. Peeks (does not pop)
    /// the queue; the following `RowDescription` or `NoData` pops it.
    fn parameter_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) {
        if let Some(stmt_name) = self.pending_describe_statements.front()
            && let Ok(parsed) = parse_parameter_description(msg_data)
            && let Some(stmt) = prepared_statements.get_mut(stmt_name.as_str())
        {
            debug!(
                "updated statement '{}' with parameter OIDs {:?}",
                stmt_name, parsed.parameter_oids
            );
            stmt.parameter_oids = parsed.parameter_oids;
            stmt.parameter_description = Some(Bytes::copy_from_slice(msg_data));
        }
    }

    /// Store the raw RowDescription on the front pending statement and pop it.
    /// Returns the statement name so the caller can populate the per-connection
    /// describe cache.
    fn row_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) -> Option<EcoString> {
        let stmt_name = self.pending_describe_statements.pop_front()?;
        let stmt = prepared_statements.get_mut(stmt_name.as_str())?;
        stmt.row_description = Some(Bytes::copy_from_slice(msg_data));
        stmt.describe_no_data = false;
        Some(stmt_name)
    }

    /// Record NoData (statement has no result columns, e.g. INSERT without
    /// RETURNING) on the front pending statement and pop it. Returns the
    /// statement name so the caller can populate the per-connection describe cache.
    fn no_data_received(
        &mut self,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) -> Option<EcoString> {
        let stmt_name = self.pending_describe_statements.pop_front()?;
        let stmt = prepared_statements.get_mut(stmt_name.as_str())?;
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
        func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
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
            crate::metrics::handles()
                .conn
                .describe_invalidations
                .increment(n as u64);
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
            crate::metrics::handles()
                .conn
                .describe_evictions
                .increment(1);
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
                let m = crate::metrics::handles();
                m.query.total.increment(1);
                m.conn.simple_queries.increment(1);
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
                                    m.query.unsupported.increment(1);
                                }
                                ForwardReason::UncacheableSelect => {
                                    m.query.uncacheable.increment(1);
                                }
                                ForwardReason::Invalid => {
                                    m.query.invalid.increment(1);
                                }
                            }
                            self.origin_dispatch(msg.data, None);
                            ProxyMode::Read
                        }
                        Ok(Action::CacheCheck(ast)) => {
                            let fingerprint = query_expr_fingerprint(&ast.query);
                            self.telemetry.cache_timing_start(fingerprint);
                            self.extended.dispatch_is_extended = false;
                            self.egress.cache_push(CacheMessage::Query(msg.data, ast));
                            ProxyMode::OriginDrain
                        }
                        Err(e) => {
                            m.query.uncacheable.increment(1);
                            m.query.invalid.increment(1);
                            error!("handle_query {}", e);
                            self.origin_dispatch(msg.data, None);
                            ProxyMode::Read
                        }
                    };
                } else {
                    m.query.uncacheable.increment(1);
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
                        if let Some(stmt) = self.prepared_statements.get_mut(stmt_name.as_str()) {
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
                        // Post-strip length field (bytes 1-4, big-endian i32, excludes tag byte).
                        // Computed before mutating so an out-of-range length (unreachable: auth
                        // messages are tiny) degrades to forwarding the message unmodified rather
                        // than panicking — the strip is best-effort anyway.
                        if let Some(new_len) = msg
                            .data
                            .len()
                            .checked_sub(needle.len() + 1)
                            .and_then(|n| i32::try_from(n).ok())
                        {
                            // Remove needle in place using split/unsplit
                            let mut tail = msg.data.split_off(pos);
                            let after_needle = tail.split_off(needle.len());
                            msg.data.unsplit(after_needle);

                            #[expect(
                                clippy::indexing_slicing,
                                reason = "PostgreSQL message format guarantees 5+ bytes"
                            )]
                            msg.data[1..5].copy_from_slice(&new_len.to_be_bytes());
                        }
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
        let mut lazy_parse_stmt: Option<EcoString> = None;
        if let Some(first) = buffer.entries.first() {
            crate::metrics::handles().query.uncacheable.increment(1);

            if let Some(portal_name) = &first.portal_name
                && let Some(portal) = self.portals.get(portal_name.as_str())
                && let Some(stmt) = self.prepared_statements.get(&portal.statement_name)
            {
                match &stmt.sql_type {
                    StatementType::NonSelect => {
                        crate::metrics::handles().query.unsupported.increment(1);
                    }
                    StatementType::ParseError => {
                        crate::metrics::handles().query.invalid.increment(1);
                    }
                    StatementType::Cacheable(_) | StatementType::UncacheableSelect => {}
                }
                if !buffer.any_has_parse() && !stmt.origin_prepared {
                    lazy_parse_stmt = Some(portal.statement_name.clone());
                }
            }
        }

        if let Some(stmt_name) = lazy_parse_stmt {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }

        let bytes = self.extended.buffer_forward(buffer, trailing_bytes);
        self.origin_dispatch(bytes, None);
    }

    /// Handle the outcome of a cache reply (the leased socket has already been
    /// recovered by `cache_serve_wait`). If the cache indicates error or needs
    /// forwarding, send the query to origin instead.
    fn handle_cache_outcome(&mut self, outcome: CacheOutcome) {
        trace!(
            "net: cache→proxy reply={}",
            match &outcome {
                CacheOutcome::Complete(_) => "Complete",
                CacheOutcome::Forward(_, _) => "Forward",
                CacheOutcome::Error(_) => "Error",
            }
        );
        match outcome {
            CacheOutcome::Complete(timing) => {
                crate::metrics::handles().query.cache_hit.increment(1);
                self.telemetry.cache_complete(timing);

                // Cache hit: the worker wrote the full response directly to the
                // client socket. Pop the serving cache slot so the next slot can
                // flush. Origin saw nothing; lazy-Parse-on-forward catches up the
                // next time we actually have to forward.
                self.egress.cache_done();
                self.extended.pending_parse_statements.clear();
                self.extended.pending_describe_statements.clear();
                self.extended.pending_lazy_parse.take();

                // Advance to the next batched execute, or finish.
                self.cache_batch_advance();
            }
            CacheOutcome::Error(buf) => {
                crate::metrics::handles().query.cache_error.increment(1);
                debug!("forwarding to origin");
                self.cache_reply_forward(buf, None);
            }
            CacheOutcome::Forward(buf, timing) => {
                crate::metrics::handles().query.cache_miss.increment(1);
                debug!("forwarding to origin");
                self.cache_reply_forward(buf, Some(timing));
            }
        }
    }

    /// Convert the serving `Cache` slot to an `Origin` slot in place, lazy-Parse
    /// the current entry if origin doesn't yet know its statement, forward its
    /// bytes followed by the rest of the batch and a single trailing Sync, and
    /// return to `Read`. Shared by every cache→origin fallback (miss/error,
    /// search_path-unknown, cache-unavailable).
    fn forward_current_and_rest(&mut self, current_bytes: BytesMut) {
        self.egress.cache_to_origin();
        if let Some(stmt_name) = self.extended.pending_lazy_parse.take() {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }
        self.origin_write_buf.push_back(current_bytes);
        self.batch_remaining_forward();
        self.proxy_mode = ProxyMode::Read;
    }

    /// Forward a cache miss/error to origin: the worker returns the missed
    /// entry's bytes, which we forward along with the rest of the batch.
    fn cache_reply_forward(&mut self, buf: BytesMut, timing: Option<QueryTiming>) {
        self.telemetry.origin_forward(timing);
        self.forward_current_and_rest(buf);
    }

    /// Fall back to forwarding a cacheable query to origin after it was taken
    /// from its egress slot (search_path unknown or client-socket creation
    /// failed): forward the pipeline/raw bytes plus the rest of the batch.
    fn cache_slot_forward_to_origin(&mut self, msg: CacheMessage) {
        let bytes = self
            .extended
            .pipeline_take()
            .map_or_else(|| msg.into_data(), |p| slices_concat(&p.buffered_bytes));
        self.forward_current_and_rest(bytes);
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
        let Ok(mutations) =
            pg_query::parse_raw_scoped(sql, |tree| unsafe { search_path_mutations_raw(tree) })
        else {
            return;
        };

        if mutations.any {
            debug!("search_path mutation detected in Query");
            self.search_path_mark_unknown();
        }

        // Piggyback only on single-statement, piggyback-safe mutations, and
        // only when no other intercept is active (otherwise the inline SHOW
        // response would collide with the existing intercept's state machine).
        if mutations.single_piggybackable.is_some()
            && matches!(self.origin_intercept, OriginIntercept::None)
            && let Some(rewritten) = query_message_append_show_search_path(&msg.data)
        {
            debug!("piggybacking SHOW search_path onto mutation query");
            msg.data = rewritten;
            self.origin_intercept =
                OriginIntercept::TrailingShowSearchPath(TrailingShowState::PreShow);
        }
    }

    /// Map a parsed-and-converted query to its cacheability classification.
    fn statement_type_classify(&self, convert: Result<QueryExpr, AstError>) -> StatementType {
        match convert {
            Ok(query) => match CacheableQuery::try_new(query, &self.func_volatility) {
                Ok(cacheable_query) => StatementType::Cacheable(Arc::new(cacheable_query)),
                Err(_) => StatementType::UncacheableSelect,
            },
            Err(_) => StatementType::NonSelect,
        }
    }

    /// Handle Parse message — analyze cacheability, store statement, buffer bytes.
    fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_parse_message(&msg.data) {
            // Single raw-tree parse (PGC-192): build the QueryExpr and — on
            // pre-PG18, where search_path isn't auto-reported — detect a
            // search_path mutation in the same walk. No protobuf round-trip.
            // (Pessimistic: the client may or may not Execute the statement.
            // Piggyback isn't attempted for extended protocol; a standalone
            // SHOW is issued via the lazy path on the next RFQ.)
            let check_search_path = !self.search_path_auto_reported;
            let result = pg_query::parse_raw_scoped(&parsed.sql, |tree| unsafe {
                let convert = query_expr_convert_raw(tree);
                // search_path mutations are non-SELECT statements that the
                // converter rejects, so the second walk is only needed when
                // convert didn't yield a SELECT — the common cacheable-SELECT
                // case skips it.
                let mutates =
                    check_search_path && convert.is_err() && search_path_mutations_raw(tree).any;
                (convert, mutates)
            });

            let sql_type = match result {
                Ok((convert, mutates)) => {
                    if mutates {
                        debug!("search_path mutation detected in Parse");
                        self.search_path_mark_unknown();
                    }
                    self.statement_type_classify(convert)
                }
                Err(_) => StatementType::ParseError,
            };

            // Freeze the codec's zero-copy slice to `Bytes`; storing/buffering it
            // is then a refcount bump instead of two deep copies.
            let data = msg.data.freeze();
            let statement_name = parsed.statement_name.clone();
            self.statement_store(parsed, sql_type, data.clone());

            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.has_parse {
                seg.dirty = true;
            }
            seg.has_parse = true;
            seg.parse_statement_names.push(statement_name);
            seg.bytes.push(data);
            trace!("net: Parse buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Bind message — store portal, buffer bytes.
    fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_bind_message(&msg.data) {
            self.portal_store(parsed);

            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.has_bind {
                seg.dirty = true;
            }
            seg.has_bind = true;
            seg.bytes.push(msg.data.freeze());
            trace!("net: Bind buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Execute message — record metrics, parse portal name, buffer bytes.
    /// Decision-making deferred to Sync.
    fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
        let m = crate::metrics::handles();
        m.query.total.increment(1);
        m.conn.extended_queries.increment(1);
        self.telemetry.query_receive();

        let portal_name = parse_execute_message(&msg.data).ok().map(|p| p.portal_name);

        // Snapshot the cache candidate now, while the portal/statement still
        // reflect this execute's Bind/Parse (a later execute may rebind the
        // same — usually unnamed — portal). The current segment's Describe
        // governs the ParameterDescription requirement.
        let describe = self
            .extended
            .buffer
            .as_ref()
            .map_or(PipelineDescribe::None, |b| b.pending.describe);
        let candidate = self.execute_cache_candidate(portal_name.as_deref(), describe);

        self.extended
            .buffer_get_or_create()
            .pending_seal(&msg.data, portal_name, candidate);
        trace!("net: Execute buffered");
    }

    /// Snapshot a cacheable-query candidate for the portal an Execute targets.
    /// Returns None when the portal/statement doesn't resolve to a cacheable
    /// SELECT with uniform result formats (and, for Describe('S'), a cached
    /// ParameterDescription). Global cache gating is checked separately at Sync.
    fn execute_cache_candidate(
        &self,
        portal_name: Option<&str>,
        describe: PipelineDescribe,
    ) -> Option<CacheCandidate> {
        let portal = self.portals.get(portal_name?)?;

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

        // Describe('S'): require a cached parameter_description
        if describe == PipelineDescribe::Statement && stmt.parameter_description.is_none() {
            return None;
        }

        Some(CacheCandidate {
            cacheable_query,
            parameters: QueryParameters {
                values: portal.parameter_values.clone(),
                formats: portal.parameter_formats.clone(),
                oids: stmt.parameter_oids.clone(),
            },
            result_formats: portal.result_formats.clone(),
            parameter_description: if describe == PipelineDescribe::Statement {
                stmt.parameter_description.clone()
            } else {
                None
            },
            statement_name: portal.statement_name.clone(),
            origin_prepared: stmt.origin_prepared,
        })
    }

    /// Whether the connection's global state currently permits cache serving.
    fn cache_globally_enabled(&self) -> bool {
        !self.in_transaction && !self.cache_disabled && self.proxy_status == ProxyStatus::Normal
    }

    /// Handle Describe message — buffer bytes and track describe metadata.
    fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_describe_message(&msg.data) {
            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.describe != PipelineDescribe::None {
                seg.dirty = true;
            }

            match parsed.describe_type {
                b'S' => {
                    seg.describe = PipelineDescribe::Statement;
                    seg.describe_statement_names.push(parsed.name);
                }
                b'P' => {
                    seg.describe = PipelineDescribe::Portal;
                }
                _ => {}
            }

            seg.bytes.push(msg.data.freeze());
            trace!("net: Describe buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Emit any deferred `CloseComplete`s (PGC-234: locally-handled Closes of
    /// statements the origin never prepared) as one ordered synth slot, so they
    /// keep their place ahead of whatever origin/cache response follows. Returns
    /// the count flushed.
    fn deferred_close_completes_flush(&mut self) -> u32 {
        let n = self.extended.deferred_close_completes;
        if n > 0 {
            self.extended.deferred_close_completes = 0;
            let mut out = BytesMut::with_capacity(CLOSE_COMPLETE_MSG.len() * n as usize);
            for _ in 0..n {
                out.put_slice(CLOSE_COMPLETE_MSG);
            }
            self.egress.synth_push(out.freeze());
        }
        n
    }

    /// Handle Close message. A `Close(statement)` for a statement that was served
    /// from cache and never prepared on the origin (`origin_prepared == false`)
    /// is handled locally — the origin never knew it, so forwarding the Close (and
    /// its paired Sync) is a useless round-trip (PGC-234). We defer the
    /// `CloseComplete` (synthesized at the next Sync) and leave origin untouched.
    /// Everything else forwards as before: origin-prepared statements, portals, a
    /// Close mid-batch (`buffer` present), or once anything has already been
    /// forwarded this group (so deferred completions can't reorder ahead of it).
    fn handle_close_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_close_message(&msg.data) {
            if parsed.close_type == b'S'
                && self.extended.buffer.is_none()
                && !self.extended.group_origin_forwarded
                && self
                    .prepared_statements
                    .get(parsed.name.as_str())
                    .is_some_and(|s| !s.origin_prepared)
            {
                self.statement_close(&parsed.name);
                self.extended.deferred_close_completes += 1;
                crate::metrics::handles().conn.close_local.increment(1);
                return;
            }
            self.deferred_close_completes_flush();
            self.extended_buffer_flush_to_origin();
            match parsed.close_type {
                b'S' => self.statement_close(&parsed.name),
                b'P' => self.portal_close(&parsed.name),
                _ => {}
            }
        } else {
            self.deferred_close_completes_flush();
            self.extended_buffer_flush_to_origin();
        }
        self.extended.group_origin_forwarded = true;
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Sync message — all cache vs. forward decision-making happens here.
    ///
    /// If every Execute in the batch is an independently cacheable read, each is
    /// dispatched as its own cache slot (in order). Otherwise the batch is
    /// synthesized (Parse-only) or forwarded whole to origin.
    fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
        // Emit any deferred CloseCompletes (locally-handled Closes) as an ordered
        // synth slot before this Sync's responses (PGC-234).
        let local_closes = self.deferred_close_completes_flush();
        let group_origin_forwarded = self.extended.group_origin_forwarded;
        self.extended.group_origin_forwarded = false;

        let Some(buffer) = self.extended.buffer_take() else {
            // Bare Sync. If this group only handled Closes locally (nothing was
            // forwarded to origin), the origin has no pending work to ack —
            // synthesize the ReadyForQuery instead of a useless round-trip.
            if local_closes > 0 && !group_origin_forwarded {
                trace!("net: bare Sync → synth ReadyForQuery (local closes only)");
                self.egress
                    .synth_push(Bytes::from_static(READY_FOR_QUERY_IDLE_MSG));
            } else {
                trace!("net: proxy→origin Sync (no buffer)");
                self.egress.origin_open();
                self.origin_write_buf.push_back(msg.data);
            }
            return;
        };

        if self.cache_batch_eligible(&buffer) {
            let entries = buffer.entries;
            trace!("net: Sync → cache batch ({} executes)", entries.len());
            self.cache_batch_dispatch(entries);
        } else if self.try_synthesize_parse_describe_response(&buffer) {
            trace!("net: Sync → synthesized ParseComplete+Describe response");
        } else {
            self.extended_buffer_forward_to_origin(buffer, &msg.data);
            trace!("net: Sync → origin (forwarded buffer)");
        }
    }

    /// Whether every Execute in the batch is an independently cacheable read, so
    /// the whole batch can be served as a sequence of cache slots. Requires a
    /// clean `[P?][B?][D?] E` shape per entry, no trailing prep, global cache
    /// gating, and at most one entry needing a lazy Parse on forward (the
    /// single-intercept forward path can absorb only one).
    fn cache_batch_eligible(&self, buffer: &ExtendedBuffer) -> bool {
        !buffer.entries.is_empty()
            && buffer.pending.bytes.is_empty()
            && self.cache_globally_enabled()
            && buffer
                .entries
                .iter()
                .all(|e| !e.dirty && e.candidate.is_some())
            && buffer
                .entries
                .iter()
                .filter(|e| e.needs_lazy_parse())
                .count()
                <= 1
    }

    /// Build a dispatch context per entry, queue them, and begin the first slot.
    /// Caller guarantees [`Self::cache_batch_eligible`] (every entry has a
    /// candidate and the list is non-empty).
    fn cache_batch_dispatch(&mut self, entries: Vec<ExecuteEntry>) {
        let last = entries.len() - 1;
        let mut contexts = VecDeque::with_capacity(entries.len());
        for (i, mut entry) in entries.into_iter().enumerate() {
            // Eligibility guarantees a candidate; skip defensively rather than
            // panic if that invariant is ever violated.
            let Some(candidate) = entry.candidate.take() else {
                continue;
            };
            contexts.push_back(DispatchContext::build(entry, candidate, i == last));
        }
        if let Some(first) = contexts.pop_front() {
            self.extended.batch = contexts;
            self.cache_slot_begin(first);
            self.proxy_mode = ProxyMode::OriginDrain;
        }
    }

    /// Apply a dispatch context as the current in-flight cache slot: stamp
    /// timing, install pipeline + forward-fallback state, and push the egress
    /// Cache slot.
    fn cache_slot_begin(&mut self, ctx: DispatchContext) {
        self.telemetry.cache_timing_start(ctx.fingerprint);
        self.extended.dispatch_is_extended = true;
        self.extended.pipeline_context = Some(ctx.pipeline);
        // Reset the awaited-response queues to just this slot's statement(s);
        // on a miss `batch_remaining_forward` appends the rest in order.
        self.extended.pending_parse_statements.clear();
        self.extended
            .pending_parse_statements
            .extend(ctx.parse_statement);
        self.extended.pending_describe_statements.clear();
        self.extended
            .pending_describe_statements
            .extend(ctx.describe_statement);
        self.extended.pending_lazy_parse = ctx.lazy_parse;
        self.egress.cache_push(ctx.msg);
    }

    /// Advance the batch after a cache hit: begin the next queued slot (staying
    /// in `OriginDrain`) or, when the batch is exhausted, return to `Read`.
    fn cache_batch_advance(&mut self) {
        if let Some(next) = self.extended.batch.pop_front() {
            self.cache_slot_begin(next);
            self.proxy_mode = ProxyMode::OriginDrain;
        } else {
            self.proxy_mode = ProxyMode::Read;
        }
    }

    /// Forward the remaining batch entries (each without a Sync) followed by one
    /// synthesized `Sync`, so origin runs them in a single implicit transaction
    /// and emits exactly one ReadyForQuery. Installs a lazy Parse for any entry
    /// that needs one (eligibility bounds this to at most one across the batch).
    fn batch_remaining_forward(&mut self) {
        while let Some(next) = self.extended.batch.pop_front() {
            if let Some(stmt_name) = next.lazy_parse {
                forward_lazy_parse_install(
                    &stmt_name,
                    &self.prepared_statements,
                    &mut self.origin_write_buf,
                    &mut self.origin_intercept,
                );
            }
            // Track this entry's awaited ParseComplete / Describe responses so
            // they mark the right statement origin_prepared, in order.
            self.extended
                .pending_parse_statements
                .extend(next.parse_statement);
            self.extended
                .pending_describe_statements
                .extend(next.describe_statement);
            self.origin_write_buf
                .push_back(slices_concat(&next.pipeline.buffered_bytes));
        }
        // A simple `Query` is self-terminating (origin emits its own RFQ);
        // only extended-pipeline entries need a synthesized Sync to close the
        // implicit transaction and produce the single trailing RFQ.
        if self.extended.dispatch_is_extended {
            self.origin_write_buf
                .push_back(BytesMut::from(SYNC_MESSAGE.as_slice()));
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
        // Synthesize only applies to a Parse-only batch: no Execute (no entries),
        // a Parse but no Bind in the pending segment.
        if !buffer.entries.is_empty() {
            return None;
        }
        let seg = &buffer.pending;
        // A dirty segment holds more than one Parse/Describe — synth produces
        // exactly one response, so it must forward instead.
        if !seg.has_parse || seg.has_bind || seg.dirty {
            return None;
        }
        if seg.describe == PipelineDescribe::Portal {
            return None;
        }
        if self.in_transaction {
            return None;
        }
        let stmt_name = seg.parse_statement_names.first().map(EcoString::as_str)?;
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
            crate::metrics::handles().conn.describe_misses.increment(1);
            return false;
        };
        // Cheap (refcount) clones now that the describe metadata is `Bytes`.
        let parameter_description = entry.parameter_description.clone();
        let row_description = entry.row_description.clone();
        // `stmt_name` borrows `buffer` (aliases `self.extended`); detach it as an
        // EcoString (inline for the short statement names clients use) so the
        // `&mut self` populate below doesn't conflict with that borrow.
        let stmt_name = EcoString::from(stmt_name);

        // Populate the freshly-Parsed statement with the cached Describe
        // metadata so a subsequent Bind+Execute can build a parameterized
        // cache message without an origin round-trip.
        let parsed_param_oids = parse_parameter_description(&parameter_description)
            .ok()
            .map(|p| p.parameter_oids);
        if let Some(stmt_mut) = self.prepared_statements.get_mut(stmt_name.as_str()) {
            if let Some(oids) = parsed_param_oids {
                stmt_mut.parameter_oids = oids;
            }
            stmt_mut.parameter_description = Some(parameter_description.clone());
            stmt_mut.row_description = row_description.clone();
            stmt_mut.describe_no_data = row_description.is_none();
        }

        crate::metrics::handles().conn.describe_hits.increment(1);

        let mut out = BytesMut::with_capacity(
            5 + parameter_description.len()
                + row_description.as_ref().map(Bytes::len).unwrap_or(5)
                + 6,
        );
        // ParseComplete
        out.put_slice(&[b'1', 0, 0, 0, 4]);
        if buffer.pending.describe == PipelineDescribe::Statement {
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
        // Anything reaching origin must come after any deferred CloseCompletes,
        // and marks the group as having origin work (PGC-234).
        self.deferred_close_completes_flush();
        self.extended.group_origin_forwarded = true;
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
        parse_bytes: Bytes,
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
                crate::metrics::handles()
                    .conn
                    .prepared_statements
                    .increment(1.0);
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
            crate::metrics::handles()
                .conn
                .prepared_statements
                .decrement(1.0);
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
        client_write: &mut ClientSocket,
    ) -> ConnectionResult<()> {
        let n = client_write
            .write(self.egress.write_chunk())
            .await
            .map_err(ConnectionError::IoError)?;
        self.egress.advance(n);
        Ok(())
    }

    async fn connection_select<'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        client_read: &mut Pin<&mut FramedRead<OwnedClientReadHalf, PgFrontendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut ClientSocket,
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

    /// Await the cache worker's reply for the in-flight query, returning the
    /// leased client write half it hands back. The client write half is with the
    /// worker, so this does **not** write the client — origin messages that
    /// arrive meanwhile buffer in the egress queue and flush once the socket is
    /// restored and we are back in `Read`.
    async fn cache_serve_wait<'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        mut reply_rx: oneshot::Receiver<CacheReply>,
    ) -> ConnectionResult<ClientSocket> {
        loop {
            select! {
                res = origin_read.next() => {
                    self.origin_read_dispatch(res)?;
                }
                reply = &mut reply_rx => {
                    match reply {
                        Ok(reply) => {
                            self.handle_cache_outcome(reply.outcome);
                            return Ok(reply.socket);
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
            }
        }
    }

    /// Select loop for `OriginDrain`: a cacheable query is queued behind earlier
    /// responses. Drains origin and flushes the egress queue **without reading
    /// the client**, so no later request is processed before the queued cache
    /// query — preserving response order until the cache slot reaches the head.
    async fn connection_select_drain<'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut ClientSocket,
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

/// Prepend a `Parse` to `origin_write_buf` when origin doesn't yet know the
/// statement. No-op if origin already knows it, if no `parse_bytes` were
/// captured, if the statement is unnamed (origin's unnamed slot doesn't
/// persist across Sync), or if another `OriginIntercept` is already active.
///
/// Free function (not a method) to allow disjoint field borrows from the
/// event-loop call sites that work with `state.*` after partial moves.
fn forward_lazy_parse_install(
    stmt_name: &str,
    prepared_statements: &HashMap<EcoString, PreparedStatement>,
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
    // Cold lazy-Parse forward: materialize the refcounted slice into the
    // origin write queue (BytesMut).
    origin_write_buf.push_back(slices_concat(std::slice::from_ref(&parse_bytes)));
    // The prepended Parse's ParseComplete is swallowed by the LazyParseInline
    // intercept (which marks origin_prepared) — it is NOT a client-awaited
    // response, so it must not be queued in pending_parse_statements.
    *origin_intercept = OriginIntercept::LazyParseInline {
        statement_name: stmt_name.into(),
    };
    crate::metrics::handles()
        .conn
        .lazy_parse_forwarded
        .increment(1);
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    client_stream: ClientStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: &str,
    dispatch_handle: CacheDispatchHandle,
    func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
    origin_database: EcoString,
) -> ConnectionResult<()> {
    // Track active connections - guard ensures decrement on any exit path
    crate::metrics::handles().conn.active.increment(1.0);
    let _connection_guard = ActiveConnectionGuard;

    // Connect to origin database (with TLS if required)
    let mut origin_stream = origin_connect(&addrs, ssl_mode, server_name)
        .await
        .attach_loc("connecting to origin")?;

    // Split origin stream (borrowed halves with .writable() support)
    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    // Split the client stream into OWNED halves with one reactor registration.
    // The connection keeps the read half (framed) and owns the write half
    // (`socket`), which it leases to the cache worker per query — no per-query
    // `dup`. `socket` is a plain owned value; while it is lent to the worker
    // there is simply no `socket` binding to write through.
    let (client_read, mut socket) = client_stream.into_split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    let mut state = ConnectionState::new(func_volatility, origin_database);

    tokio::pin!(origin_framed_read);
    tokio::pin!(client_framed_read);
    tokio::pin!(origin_write);

    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                if let Err(err) = state
                    .connection_select(
                        &mut origin_framed_read,
                        &mut client_framed_read,
                        &mut origin_write,
                        &mut socket,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
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
                            &mut socket,
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
                    crate::metrics::handles().query.uncacheable.increment(1);
                    state.cache_slot_forward_to_origin(msg);
                    continue;
                };

                crate::metrics::handles().query.cacheable.increment(1);

                let (reply_tx, reply_rx) = oneshot::channel();
                let timing = state.telemetry.cache_timing_dispatch();

                // Lease the owned write half to the worker for this query.
                let proxy_msg = ProxyMessage {
                    message: msg,
                    client_socket: socket,
                    reply_tx,
                    search_path: resolved_search_path,
                    timing,
                    pipeline: state.extended.pipeline_take(),
                };

                // Inline dispatch: the connection dispatches against the shared
                // CacheDispatch directly (inline, no extra hop). A hit goes straight to
                // the worker, a miss/coalesce/registration is handled inline; the
                // reply (and the leased write half) comes back via `reply_rx`.
                match dispatch_handle.current() {
                    Some(mut dispatch) => {
                        // The write half is now leased into the dispatch. Await the
                        // reply, which returns it; origin messages buffer in egress
                        // meanwhile and flush once we are back in `Read`.
                        dispatch.dispatch_proxy(proxy_msg).await;
                        match state
                            .cache_serve_wait(&mut origin_framed_read, &mut origin_write, reply_rx)
                            .await
                        {
                            Ok(returned) => {
                                // `handle_cache_outcome` set the next mode:
                                // `OriginDrain` to serve the next batched slot,
                                // or `Read` when the batch is done / forwarded.
                                socket = returned;
                            }
                            Err(err) => {
                                debug!("cache serve error [{}]", err);
                                break;
                            }
                        }
                    }
                    None => {
                        // Cache is unavailable: recover the leased write half and
                        // fall back to proxying directly to origin.
                        debug!("cache unavailable");
                        state.proxy_status = ProxyStatus::Degraded;
                        socket = proxy_msg.client_socket;
                        // The cache slot is now serving (message already taken);
                        // forward this entry (lazy-Parsing if origin doesn't know
                        // its statement) plus the rest of the batch + one Sync.
                        let bytes = proxy_msg.pipeline.map_or_else(
                            || proxy_msg.message.into_data(),
                            |p| slices_concat(&p.buffered_bytes),
                        );
                        state.forward_current_and_rest(bytes);
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
        crate::metrics::handles()
            .conn
            .prepared_statements
            .decrement(remaining_stmts as f64);
    }

    match state.proxy_status {
        ProxyStatus::Degraded => Err(ConnectionError::CacheDead.into()),
        ProxyStatus::Normal => Ok(()),
    }
}

/// Handle one accepted client connection end-to-end (TLS negotiation + the
/// proxy loop). Spawned per accept onto the shared multi-thread runtime.
#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
#[allow(clippy::too_many_arguments)]
pub async fn connection_task(
    socket: TcpStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: EcoString,
    dispatch_handle: CacheDispatchHandle,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
    origin_database: EcoString,
) {
    debug!("task spawn");

    // Negotiate client TLS if configured
    let client_stream = match tls::client_tls_negotiate(socket, tls_acceptor.as_deref()).await {
        Ok(tls::ClientTlsResult::Tls {
            tcp_stream,
            tls_state,
        }) => ClientStream::tls(tcp_stream, tls_state),
        Ok(tls::ClientTlsResult::Plain(stream)) => ClientStream::plain(stream),
        Err(e) => {
            crate::metrics::handles().conn.errors.increment(1);
            error!("TLS negotiation failed: {}", e);
            return;
        }
    };

    let res = handle_connection(
        client_stream,
        addrs,
        ssl_mode,
        &server_name,
        dispatch_handle,
        func_volatility,
        origin_database,
    )
    .await;

    if let Err(e) = res {
        error!("{}", e);
        crate::metrics::handles().conn.errors.increment(1);
    }

    debug!("task done");
}
