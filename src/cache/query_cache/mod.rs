use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use ecow::EcoString;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::bytes::{Bytes, BytesMut};

use crate::pg::protocol::extended::ResultFormats;
use crate::proxy::ClientSocket;
use crate::query::ast::LimitClause;
use crate::query::{Fingerprint, QueryShape};
use crate::settings::DynamicConfigHandle;
use crate::timing::QueryTiming;

use super::coalesce_queue::CoalesceQueue;
use super::messages::{CacheReply, MessageSlices, PipelineContext, PipelineDescribe, QueryCommand};
use super::mv::MvServe;
use super::query::CacheableQuery;
use super::reg_bucket::RegRateBucket;
use super::reply::ReplySender;
use super::types::{CacheStateView, SharedResolved};

mod coalesce;
mod dispatch;
mod handle;
mod serve;

pub use handle::{CacheDispatchHandle, CacheDispatchPublisher, CacheDispatchUpdater};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum QueryType {
    Simple,
    Extended,
}

/// A client waiting to receive coalesced response bytes from a shared serve execution.
pub struct CoalescedClient {
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    pub timing: QueryTiming,
    /// Pre-computed origin fallback bytes (pipeline.buffered_bytes or raw data).
    pub data: BytesMut,
}

pub struct QueryRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub cacheable_query: Arc<CacheableQuery>,
    pub result_formats: ResultFormats,
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    /// Resolved search_path for schema resolution
    pub search_path: Arc<[EcoString]>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context from the proxy (None for simple queries and cold-path extended)
    pub pipeline: Option<PipelineContext>,
}

/// Request sent to cache serve for executing cached queries.
/// Contains the resolved AST with schema-qualified table names.
pub struct ServeRequest {
    pub fingerprint: Fingerprint,
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: SharedResolved,
    /// Precomputed deparsed SQL body of `resolved`. Spliced into the SET +
    /// body + LIMIT wire string the serve pool sends to the cache DB.
    pub deparsed_sql: EcoString,
    /// Parameterized serve shape (literal-free SQL + ordered literal values).
    /// `Some` for source-row serves (the shape-keyed prepared-statement path);
    /// `None` for MV-backed serves, which keep the `deparsed_sql` path (PGC-294).
    pub serve_shape: Option<QueryShape>,
    /// Generation number for row tracking in pgcache_pgrx extension
    pub generation: u64,
    /// Serve from the MV (carrying its aliased output column names) or
    /// from source rows. Decided on the dispatch path.
    pub mv: MvServe,
    pub result_formats: ResultFormats,
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Incoming query's LIMIT clause, appended to SQL at serve time
    pub limit: Option<LimitClause>,
    /// Whether the serve path should append ReadyForQuery after this execute's
    /// response (the trailing execute of a Sync-terminated dispatch).
    pub emit_rfq: bool,
    /// Whether Parse was buffered in the pipeline.
    /// False for Bind-only pipelines (named statement re-execution without Parse).
    pub has_parse: bool,
    /// Whether Bind was buffered in the pipeline.
    /// False when Bind was flushed separately (e.g., JDBC Parse/Bind/Describe/Flush then Execute/Sync).
    pub has_bind: bool,
    /// Whether the pipeline includes a Describe message and which type.
    pub pipeline_describe: PipelineDescribe,
    /// Stored ParameterDescription bytes for Describe('S') responses in the pipeline.
    pub parameter_description: Option<Bytes>,
    /// Buffered message slices for origin fallback on serve error. Concatenated
    /// only on that (cold) path; a successful serve drops them untouched.
    pub forward_bytes: Option<MessageSlices>,
    /// Additional clients to receive the same response bytes.
    /// Empty for non-coalesced requests.
    pub coalesced: Vec<CoalescedClient>,
}

/// Per-connection inline dispatch against the cache: routes queries and
/// delegates writes to the writer thread. `Send + Clone`; each connection holds
/// one (via the watch handle) and dispatches against it directly.
#[derive(Clone)]
pub struct CacheDispatch {
    query_tx: UnboundedSender<QueryCommand>,
    serve_tx: UnboundedSender<ServeRequest>,
    state_view: Arc<CacheStateView>,
    dynamic: DynamicConfigHandle,
    waiting: Arc<CoalesceQueue>,
    /// CDC-liveness flag (set by the CDC thread). While CDC is down, queries are
    /// forwarded to origin rather than served from cache, to avoid stale reads.
    cdc_connected: Arc<AtomicBool>,
    /// PGC-277 prototype: caps the new-registration admit rate.
    reg_bucket: Arc<RegRateBucket>,
}
