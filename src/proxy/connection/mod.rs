use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use ecow::EcoString;
use lru::LruCache;
use tokio_util::bytes::BytesMut;

use super::cacheability_memo::CacheabilityCache;
use super::{ProxyMode, ProxyStatus};
use crate::catalog::FunctionVolatility;
use crate::query::write::IsolationLevel;
use crate::{
    cache::{CacheDispatchHandle, CacheMessage},
    pg::protocol::{
        backend::TransactionStatus,
        session::{Portal, PreparedStatement},
    },
    proxy::egress::EgressQueue,
};

mod cache_batch;
mod describe_cache;
mod describe_synth;
mod extended;
mod extended_buffer;
mod origin_intercept;
mod probe;
mod relay;
mod telemetry;
mod transaction_isolation;
mod write_log;

use super::origin_stream::{OriginReadHalf, OriginWriteHalf, origin_connect};
use crate::proxy::session_settings::{IsolationState, SessionSettings};
use describe_cache::{DESCRIBE_CACHE_CAPACITY, DescribeCacheEntry, DescribeKey};
use extended_buffer::ExtendedPending;
use origin_intercept::OriginIntercept;
pub use relay::connection_task;
use relay::forward_lazy_parse_install;
use telemetry::QueryTelemetry;
use transaction_isolation::TransactionForwardReason;
use write_log::{RawDecision, RawForwardCause, RawForwardReason, WriteLog, forward_cause};

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

    /// Per-connection cacheability memo: bounded handles into the shared
    /// interning store.
    cacheability_cache: CacheabilityCache,

    /// Origin's transaction status as of the last ReadyForQuery. Cache serves
    /// echo it in their own ReadyForQuery, and only `InTransaction` (never
    /// `Failed`) may be cache-served inside a block (PGC-387).
    transaction_status: TransactionStatus,

    /// The open block's isolation level, fixed on the idle→in-block edge from
    /// `pending_block_isolation` or the session default; tightened by `SET
    /// TRANSACTION`. Meaningless outside a block.
    block_isolation: IsolationState,

    /// A `BEGIN ... ISOLATION LEVEL` clause forwarded but not yet acknowledged
    /// by the block's ReadyForQuery.
    pending_block_isolation: Option<IsolationLevel>,

    /// Forwarded `BEGIN`s whose idle→in-block ReadyForQuery has not arrived:
    /// a following pipelined statement already belongs to that block.
    begins_forwarded: u32,

    /// A cache serve was abandoned mid-response inside a block: the connection
    /// closes once the serve returns so origin rolls the block back.
    close_requested: bool,

    /// Current proxy mode (reading, writing to client/origin/cache)
    proxy_mode: ProxyMode,

    /// Proxy status (normal or degraded if cache is unavailable)
    proxy_status: ProxyStatus,

    /// Extended protocol: prepared statements by name
    prepared_statements: HashMap<EcoString, PreparedStatement>,

    /// Extended protocol: portals (bound statements) by name
    portals: HashMap<EcoString, Portal>,

    /// Strips proxy-added parts out of a client request's own origin response
    /// (a lazy `Parse`). Only one active at a time. Whole injected probes are
    /// ordered by `egress` instead.
    origin_intercept: OriginIntercept,

    /// The tracked session settings and the views dispatch reads from them:
    /// the resolved search_path and the isolation default (PGC-287).
    session_settings: SessionSettings,

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

    /// Per-connection read-after-write log (PGC-124): the writes this connection
    /// forwarded to origin, aggregated per table, so a subsequent cacheable read
    /// on this connection is forwarded rather than served stale while those
    /// writes are in the commit→CDC-apply window.
    write_log: WriteLog,

    /// Handle to the current cache generation, used to read the CDC apply
    /// watermark for draining `write_log`. Read through this (never a cached
    /// `Arc`) so a cache restart never surfaces a stale-high watermark (PGC-124).
    dispatch_handle: CacheDispatchHandle,
}
