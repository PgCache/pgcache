use crate::query::Fingerprint;
use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;
use tokio_util::bytes::{Bytes, BytesMut};

use super::query::CacheableQuery;
use super::reply::ReplySender;
use crate::pg::protocol::session::ResultFormats;
use crate::proxy::{ClientSocket, ExplainSpec};
use crate::timing::QueryTiming;

use super::types::SharedResolved;

mod cdc_command;
mod query_command;

pub use cdc_command::{CdcCommand, CdcValue, cdc_values_convert};
pub use query_command::{
    AdmitAction, MvBuildOutcome, PopulationMerge, QueryCommand, SubsumptionResult,
};

/// Notifications from writer to dispatch for coalescing queue drain.
pub enum WriterNotify {
    /// Population completed — query is Ready.
    Ready {
        fingerprint: Fingerprint,
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        max_limit: Option<u64>,
    },
    /// Population failed.
    Failed { fingerprint: Fingerprint },
}

/// Whether the pipeline includes a Describe and which type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PipelineDescribe {
    /// No Describe in the pipeline
    #[default]
    None,
    /// Describe('S') — serve path should include ParameterDescription + RowDescription
    Statement,
    /// Describe('P') — serve path should include RowDescription only
    Portal,
}

/// Buffered extended-protocol message slices (Parse/Bind/Describe/Execute), one
/// refcounted `Bytes` per message. Inline-stored for the common ≤4-message
/// segment (cacheable shape) so accumulation never touches the heap; spills only
/// for dirty multi-prep batches, which take the forward path anyway.
pub(crate) type MessageSlices = SmallVec<[Bytes; 4]>;

/// Concatenate refcounted message slices into one contiguous buffer, for the
/// (cold) forward-to-origin / error fallback paths. Cache hits drop the slices
/// without ever concatenating.
pub(crate) fn slices_concat(slices: &[Bytes]) -> BytesMut {
    let mut out = BytesMut::with_capacity(slices.iter().map(Bytes::len).sum());
    for s in slices {
        out.extend_from_slice(s);
    }
    out
}

/// Pipeline context for atomic extended query dispatch.
/// Contains the raw Parse/Bind/Describe bytes buffered by the proxy,
/// used for origin fallback on cache miss.
pub struct PipelineContext {
    /// All buffered messages (Parse + Bind + optional Describe), one refcounted
    /// slice per message in order. Concatenated and forwarded to origin only on
    /// cache miss (Forward reply); dropped untouched on a hit.
    pub buffered_bytes: MessageSlices,
    /// Whether the pipeline includes a Describe message.
    pub describe: PipelineDescribe,
    /// Stored ParameterDescription bytes for Describe('S') responses.
    pub parameter_description: Option<Bytes>,
    /// Whether Parse was buffered in this pipeline.
    /// False for Bind-only pipelines (named statement re-execution without Parse).
    pub has_parse: bool,
    /// Whether Bind was buffered in this pipeline.
    /// False when Bind was flushed separately (e.g., JDBC Parse/Bind/Describe/Flush then Execute/Sync).
    pub has_bind: bool,
    /// Whether the serve path should append ReadyForQuery after this execute's
    /// response. True for a Sync-terminated dispatch's trailing execute; false
    /// for non-trailing executes and Flush dispatches (one Sync ⇒ one RFQ).
    pub emit_rfq: bool,
}

/// Parameters passed into an extended query
#[derive(Debug)]
pub struct QueryParameters {
    pub values: Vec<Option<Bytes>>,
    pub formats: Vec<i16>,
    pub oids: Vec<u32>,
}

impl QueryParameters {
    pub fn get(&self, index: usize) -> Option<QueryParameter> {
        let value = self.values.get(index)?;

        // Per the extended query protocol, format codes and OIDs may have fewer
        // entries than there are parameters:
        //   0 entries  → apply the default (text format / unspecified OID) to all
        //   1 entry    → apply that single value to all parameters
        //   N entries  → one entry per parameter
        let format = match self.formats.as_slice() {
            [] => 0,
            [single] => *single,
            codes => *codes.get(index)?,
        };
        let oid = match self.oids.as_slice() {
            [] => 0,
            [single] => *single,
            oids => *oids.get(index)?,
        };

        Some(QueryParameter {
            value: value.clone(),
            format,
            oid,
        })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Debug)]
pub struct QueryParameter {
    pub value: Option<Bytes>,
    pub format: i16,
    pub oid: u32,
}

/// Message types for communication between proxy and cache
#[derive(Debug)]
pub enum CacheMessage {
    Query(BytesMut, Arc<CacheableQuery>),
    QueryParameterized(
        BytesMut,
        Arc<CacheableQuery>,
        QueryParameters,
        ResultFormats,
    ),
    /// `SELECT pgcache_explain(...)` — normally intercepted by the dispatch, but
    /// the second field carries the original simple-query bytes so a fallback
    /// path (cache unavailable) can still forward it to origin (which reports
    /// the unknown function) rather than send an empty frame (PGC-345).
    Explain(ExplainSpec, BytesMut),
}

impl CacheMessage {
    /// Extracts the raw query data buffer, discarding the parsed query information.
    pub fn into_data(self) -> BytesMut {
        match self {
            CacheMessage::Query(data, _) | CacheMessage::QueryParameterized(data, _, _, _) => data,
            CacheMessage::Explain(_, data) => data,
        }
    }
}

/// State of data stream processing
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DataStreamState {
    Incomplete,
    Complete,
}

/// Reply sent from cache back to the proxy. Always returns the leased client
/// write half (`socket`) so the connection can resume; `outcome` carries what
/// the serve pool did. The socket return is kept orthogonal to the outcome so no
/// path can forget it.
#[derive(Debug)]
pub struct CacheReply {
    /// The leased client write half, returned to the connection.
    pub socket: ClientSocket,
    pub outcome: CacheOutcome,
}

/// What the serve pool did with a dispatched query (see [`CacheReply`]).
#[derive(Debug)]
pub enum CacheOutcome {
    /// Query completed successfully. Worker wrote the full response to the client.
    Complete(Option<QueryTiming>),
    /// Query should be forwarded to origin (cache miss or not cacheable).
    /// Contains buffered bytes for origin (or just execute_data if no pipeline),
    /// plus the per-query timing struct so the proxy can continue stamping
    /// forward-path stages and record full per-stage histograms when the
    /// forward completes.
    Forward(BytesMut, QueryTiming),
    /// Query execution failed. Contains buffered bytes for origin fallback.
    Error(BytesMut),
}

/// Message from proxy containing query and connection details
pub struct ProxyMessage {
    pub message: CacheMessage,
    /// Socket for sending response data directly to the client
    pub client_socket: ClientSocket,
    pub reply_tx: ReplySender<CacheReply>,
    /// Resolved search_path for this connection (with $user expanded to session_user)
    pub search_path: Arc<[EcoString]>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context for atomic extended query dispatch.
    /// None for simple queries and cold-path extended queries (no pipeline active).
    pub pipeline: Option<PipelineContext>,
}
