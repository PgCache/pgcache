use std::{
    collections::{HashMap, VecDeque},
    num::NonZeroUsize,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use lru::LruCache;

use ecow::EcoString;

use crate::catalog::FunctionVolatility;

use tokio_util::bytes::BytesMut;

use crate::{
    cache::CacheMessage,
    pg::protocol::extended::{Portal, PreparedStatement},
    proxy::egress::EgressQueue,
};

use super::query::CacheabilityCache;
use super::{ProxyMode, ProxyStatus};

mod describe_cache;
mod extended;
mod relay;
mod search_path_intercept;
mod telemetry;

pub(in crate::proxy::connection) use super::origin_stream::{
    OriginReadHalf, OriginWriteHalf, origin_connect,
};
pub(in crate::proxy::connection) use describe_cache::{DescribeCacheEntry, DescribeKey};
pub(in crate::proxy::connection) use extended::ExtendedPending;
pub use relay::connection_task;
pub(in crate::proxy::connection) use relay::forward_lazy_parse_install;
pub(in crate::proxy::connection) use search_path_intercept::{OriginIntercept, SearchPathState};
pub(in crate::proxy::connection) use telemetry::QueryTelemetry;

/// Process-global "log at most once per window" gate, shared across all
/// connections. A flapping cache subsystem can drop one client per query; this
/// keeps the operator-facing warning to a single line per window per call site
/// rather than one per affected client. `last` is per-call-site state holding
/// the last emit time in whole seconds since first use (`u64::MAX` = never).
fn log_gate(last: &AtomicU64, window_secs: u64) -> bool {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    let now = START.elapsed().as_secs();
    let prev = last.load(Ordering::Relaxed);
    if prev != u64::MAX && now.wrapping_sub(prev) < window_secs {
        return false;
    }
    last.compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

/// Window for the cache-down operator warnings, in seconds.
const CACHE_DOWN_LOG_WINDOW_SECS: u64 = 5;

/// Gate state: cache died with a query in flight (client connection dropped).
static CACHE_DEAD_INFLIGHT_LOG: AtomicU64 = AtomicU64::new(u64::MAX);

/// Gate state: cache unavailable at dispatch (connections degraded to origin).
static CACHE_DEGRADED_LOG: AtomicU64 = AtomicU64::new(u64::MAX);

/// Guard that decrements active connections gauge when dropped.
struct ActiveConnectionGuard;

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        crate::metrics::handles().conn.active.decrement(1.0);
    }
}

/// Synth response for a Parse-only batch (no Describe): ParseComplete + RFQ('I').
const PARSE_COMPLETE_RFQ_IDLE: &[u8] = &[b'1', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'];

/// Bounded per connection so dynamic-SQL workloads can't grow it unbounded.
const DESCRIBE_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();

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

/// Manages state for a single client connection.
/// Encapsulates transaction state, query fingerprint cache, and protocol state.
pub(super) struct ConnectionState {
    /// data waiting to be written to origin
    pub(in crate::proxy::connection) origin_write_buf: VecDeque<BytesMut>,

    /// Ordered queue of pending client-bound responses (origin relay, synth,
    /// cache). The single source of truth for client response ordering; see
    /// [`EgressQueue`]. Replaces the former `client_write_buf` + `pending_synth`
    /// + `origin_inflight_syncs` coordination.
    pub(in crate::proxy::connection) egress: EgressQueue<CacheMessage>,

    /// A `Flush` forwarded a Parse/Bind/Describe sub-request to origin (JDBC
    /// pattern), opening an `Origin` egress slot whose describe response carries
    /// no `ReadyForQuery` to seal it. The client reads that response before
    /// sending its next message, so the next client message seals the slot.
    pub(in crate::proxy::connection) flush_describe_pending: bool,

    /// Cache of query fingerprints to cacheability decisions
    pub(in crate::proxy::connection) cacheability_cache: CacheabilityCache,

    /// Whether the connection is currently in a transaction
    pub(in crate::proxy::connection) in_transaction: bool,

    /// Current proxy mode (reading, writing to client/origin/cache)
    pub(in crate::proxy::connection) proxy_mode: ProxyMode,

    /// Proxy status (normal or degraded if cache is unavailable)
    pub(in crate::proxy::connection) proxy_status: ProxyStatus,

    /// Extended protocol: prepared statements by name
    pub(in crate::proxy::connection) prepared_statements: HashMap<EcoString, PreparedStatement>,

    /// Extended protocol: portals (bound statements) by name
    pub(in crate::proxy::connection) portals: HashMap<EcoString, Portal>,

    /// PostgreSQL session user from startup message
    /// TODO: Track SET ROLE queries to update effective user for permission checks
    pub(in crate::proxy::connection) session_user: Option<String>,

    /// Intercepts origin responses that shouldn't reach the client (e.g., SHOW
    /// search_path or proactive Parse+Sync). Only one intercept active at a time.
    pub(in crate::proxy::connection) origin_intercept: OriginIntercept,

    /// Search path discovery state
    pub(in crate::proxy::connection) search_path_state: SearchPathState,

    /// Set when the TrailingShowSearchPath piggyback intercept resolves
    /// search_path within the current origin message batch. Cleared when the
    /// RFQ for that batch is processed. Used to suppress the txn-end dirty
    /// marker so piggyback on COMMIT/ROLLBACK doesn't immediately clobber the
    /// freshly-resolved value.
    pub(in crate::proxy::connection) search_path_just_piggyback_resolved: bool,

    /// Set on the first `ParameterStatus("search_path", ...)` message we
    /// receive. This signals that the origin treats search_path as a
    /// GUC_REPORT parameter and will emit ParameterStatus on every change
    /// (PG18+ behavior). Once known, the proxy skips its defensive SHOW
    /// machinery — mutation detection, piggyback rewrite, and txn-end dirty
    /// marking — since ParameterStatus keeps state in sync automatically and
    /// the redundant SHOW would just burn a round trip.
    pub(in crate::proxy::connection) search_path_auto_reported: bool,

    /// Query timing instrumentation
    pub(in crate::proxy::connection) telemetry: QueryTelemetry,

    /// Function volatility map for cacheability checks
    pub(in crate::proxy::connection) func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,

    /// Extended query protocol pipeline state
    pub(in crate::proxy::connection) extended: ExtendedPending,

    /// Configured origin database name for client database validation
    pub(in crate::proxy::connection) origin_database: EcoString,

    /// Caching disabled for this connection (e.g., client targets a different database)
    pub(in crate::proxy::connection) cache_disabled: bool,

    /// Describe-response cache keyed by `(sql, parameter_oids)`; populated on
    /// each forwarded Parse+Describe, consulted by the Parse-only synthesize
    /// path so repeat prepares skip the origin round-trip.
    pub(in crate::proxy::connection) describe_cache: LruCache<DescribeKey, DescribeCacheEntry>,
}

/// Wire bytes of a bare `Sync` message (`'S'` + length 4). Synthesized to close
/// origin's implicit transaction when a multi-execute cache batch falls back to
/// forwarding (the client's own `Sync` isn't replayed per entry).
const SYNC_MESSAGE: [u8; 5] = [b'S', 0, 0, 0, 4];
