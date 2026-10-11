use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use tokio::sync::mpsc::UnboundedSender;
use tokio_util::bytes::{Bytes, BytesMut};

use super::coalesce_queue::CoalesceQueue;
use super::explain::ExplainJob;
use super::messages::{CacheReply, MessageSlices, PipelineContext, PipelineDescribe, QueryCommand};
use super::mv::MvServe;
use super::output_settings::OutputSettings;
use super::query::CacheableQuery;
use super::reg_bucket::RegRateBucket;
use super::reply::ReplySender;
use super::types::{CacheStateView, SharedQueryShape, SharedResolvedQuery};
use crate::cache::Generation;
use crate::pg::protocol::backend::TransactionStatus;
use crate::pg::protocol::session::ResultFormats;
use crate::proxy::ClientSocket;
use crate::query::ast::LimitClause;
use crate::query::keying_settings::KeyingSettings;
use crate::query::{Fingerprint, SessionKey};
use crate::settings::DynamicConfigHandle;
use crate::timing::QueryTiming;

mod coalesce;
mod dispatch;
mod explain;
mod handle;
mod register;
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
    /// What the read is keyed under: the resolved search_path and the
    /// literal settings at this read's scope.
    pub keying: Arc<KeyingSettings>,
    /// The read's session key when the proxy hashed it at the slot.
    pub session_key: Option<SessionKey>,
    /// The client's rendering settings at this read's scope (PGC-287).
    pub output_settings: Arc<OutputSettings>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context from the proxy (None for simple queries and cold-path extended)
    pub pipeline: Option<PipelineContext>,
    /// The client's transaction status for the trailing ReadyForQuery.
    pub transaction_status: TransactionStatus,
}

/// Request sent to cache serve for executing cached queries.
pub struct ServeRequest {
    pub fingerprint: Fingerprint,
    pub query_type: QueryType,
    pub data: BytesMut,
    /// The query as registered: what the serve reads, for the memo.
    pub bound: SharedResolvedQuery,
    /// Parameterized serve shape (literal-free SQL + ordered literal values):
    /// the shape-keyed prepared statement a source-row serve runs (PGC-294).
    pub serve_shape: SharedQueryShape,
    /// Generation number for row tracking in pgcache_pgrx extension
    pub generation: Generation,
    /// Serve from the MV (carrying its aliased output column names) or
    /// from source rows. Decided on the dispatch path.
    pub mv: MvServe,
    /// Applied on the serve connection before the SELECT so the cache DB
    /// renders as origin would for this client (PGC-287).
    pub output_settings: Arc<OutputSettings>,
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
    /// The client's transaction status for that ReadyForQuery.
    pub transaction_status: TransactionStatus,
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

impl ServeRequest {
    /// The trailing ReadyForQuery this serve appends: simple-query clients
    /// always expect one, extended clients when their trailing Execute carried
    /// the Sync. Its status byte echoes the client's transaction state.
    pub fn trailing_rfq(&self) -> Option<&'static [u8]> {
        (self.query_type == QueryType::Simple || self.emit_rfq)
            .then(|| self.transaction_status.ready_for_query_message())
    }
}

/// A unit of work for the serve pool: a normal cache serve, or a one-shot
/// `pgcache_explain` (PGC-345). Both borrow a pooled cache connection from the
/// same loop; the explain variant uses a dedicated handler.
// `ServeRequest` is large and travels the serve hot path by value (as it did
// before this enum existed); boxing it to shrink the variant gap would add a
// per-serve allocation, so the size difference is accepted here.
#[allow(clippy::large_enum_variant)]
pub enum ServeJob {
    Query(ServeRequest),
    Explain(ExplainJob),
}

impl ServeJob {
    /// The job's timing struct, for the serve loop's per-job stamps.
    pub fn timing_mut(&mut self) -> &mut QueryTiming {
        match self {
            ServeJob::Query(request) => &mut request.timing,
            ServeJob::Explain(job) => &mut job.timing,
        }
    }
}

/// Per-connection inline dispatch against the cache: routes queries and
/// delegates writes to the writer thread. `Send + Clone`; each connection holds
/// one (via the watch handle) and dispatches against it directly.
#[derive(Clone)]
pub struct CacheDispatch {
    query_tx: UnboundedSender<QueryCommand>,
    serve_tx: UnboundedSender<ServeJob>,
    state_view: Arc<CacheStateView>,
    dynamic: DynamicConfigHandle,
    waiting: Arc<CoalesceQueue>,
    /// CDC-liveness flag (set by the CDC thread). While CDC is down, queries are
    /// forwarded to origin rather than served from cache, to avoid stale reads.
    cdc_connected: Arc<AtomicBool>,
    /// PGC-277 prototype: caps the new-registration admit rate.
    reg_bucket: Arc<RegRateBucket>,
}
