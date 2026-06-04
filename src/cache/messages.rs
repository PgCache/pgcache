use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::oneshot;
use tokio_util::bytes::{Bytes, BytesMut};

use super::{CacheError, Report, query::CacheableQuery, query_cache::QueryType};
use crate::catalog::TableMetadata;
use crate::proxy::ClientSocket;
use crate::query::transform::query_expr_parameters_replace;
use crate::timing::QueryTiming;

use super::types::SharedResolved;

/// Result of a subsumption check, sent from the writer back to the coordinator
/// via a oneshot channel included in the Register command.
pub enum SubsumptionResult {
    /// Data already in cache. State is Ready, serve immediately.
    Subsumed {
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
    },
    /// Not subsumed. Forward to origin; population dispatched if admit_action was Admit.
    NotSubsumed,
}

/// Notifications from writer to coordinator for coalescing queue drain.
pub enum WriterNotify {
    /// Population completed — query is Ready.
    Ready {
        fingerprint: u64,
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        max_limit: Option<u64>,
    },
    /// Population failed.
    Failed { fingerprint: u64 },
}

/// Whether the pipeline includes a Describe and which type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PipelineDescribe {
    /// No Describe in the pipeline
    #[default]
    None,
    /// Describe('S') — worker should include ParameterDescription + RowDescription
    Statement,
    /// Describe('P') — worker should include RowDescription only
    Portal,
}

/// Pipeline context for atomic extended query dispatch.
/// Contains the raw Parse/Bind/Describe bytes buffered by the proxy,
/// used for origin fallback on cache miss.
pub struct PipelineContext {
    /// All buffered messages (Parse + Bind + optional Describe) concatenated.
    /// Forwarded to origin on cache miss (Forward reply).
    pub buffered_bytes: BytesMut,
    /// Whether the pipeline includes a Describe message.
    pub describe: PipelineDescribe,
    /// Stored ParameterDescription bytes for Describe('S') responses.
    pub parameter_description: Option<BytesMut>,
    /// Whether Parse was buffered in this pipeline.
    /// False for Bind-only pipelines (named statement re-execution without Parse).
    pub has_parse: bool,
    /// Whether Bind was buffered in this pipeline.
    /// False when Bind was flushed separately (e.g., JDBC Parse/Bind/Describe/Flush then Execute/Sync).
    pub has_bind: bool,
    /// Whether the worker should append ReadyForQuery after this execute's
    /// response. True for a Sync-terminated dispatch's trailing execute; false
    /// for non-trailing executes and Flush dispatches (one Sync ⇒ one RFQ).
    pub emit_rfq: bool,
}

/// Converted query data ready for processing
pub struct QueryData {
    pub data: BytesMut,
    pub cacheable_query: Arc<CacheableQuery>,
    pub query_type: QueryType,
    pub result_formats: Vec<i16>,
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
    QueryParameterized(BytesMut, Arc<CacheableQuery>, QueryParameters, Vec<i16>),
}

impl CacheMessage {
    /// Extracts the raw query data buffer, discarding the parsed query information.
    pub fn into_data(self) -> BytesMut {
        match self {
            CacheMessage::Query(data, _) | CacheMessage::QueryParameterized(data, _, _, _) => data,
        }
    }

    /// Converts the cache message into query data ready for processing.
    /// For parameterized queries, this performs parameter replacement in the AST.
    ///
    /// On error, returns the original data buffer so it can be forwarded to the origin.
    pub fn into_query_data(self) -> Result<QueryData, (Report<CacheError>, BytesMut)> {
        match self {
            CacheMessage::Query(data, cacheable_query) => Ok(QueryData {
                data,
                cacheable_query,
                query_type: QueryType::Simple,
                result_formats: Vec::new(),
            }),
            CacheMessage::QueryParameterized(data, cacheable_query, parameters, result_formats) => {
                // Replace parameters in AST, producing a new QueryExpr
                match query_expr_parameters_replace(&cacheable_query.query, &parameters) {
                    Ok(replaced_query) => Ok(QueryData {
                        data,
                        cacheable_query: Arc::new(CacheableQuery {
                            query: replaced_query,
                        }),
                        query_type: QueryType::Extended,
                        result_formats,
                    }),
                    Err(e) => Err((e.context_transform(CacheError::from), data)),
                }
            }
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
/// the worker did. The socket return is kept orthogonal to the outcome so no
/// path can forget it.
#[derive(Debug)]
pub struct CacheReply {
    /// The leased client write half, returned to the connection.
    pub socket: ClientSocket,
    pub outcome: CacheOutcome,
}

/// What the worker did with a dispatched query (see [`CacheReply`]).
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
    pub reply_tx: oneshot::Sender<CacheReply>,
    /// Resolved search_path for this connection (with $user expanded to session_user)
    pub search_path: Vec<EcoString>,
    /// Per-query timing data
    pub timing: QueryTiming,
    /// Pipeline context for atomic extended query dispatch.
    /// None for simple queries and cold-path extended queries (no pipeline active).
    pub pipeline: Option<PipelineContext>,
}

/// Commands for query registration lifecycle, sent to the writer thread
impl std::fmt::Debug for QueryCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Register { fingerprint, .. } => f
                .debug_struct("Register")
                .field("fingerprint", fingerprint)
                .finish_non_exhaustive(),
            Self::Ready {
                fingerprint,
                cached_bytes,
                row_count,
            } => f
                .debug_struct("Ready")
                .field("fingerprint", fingerprint)
                .field("cached_bytes", cached_bytes)
                .field("row_count", row_count)
                .finish(),
            Self::Failed { fingerprint } => f
                .debug_struct("Failed")
                .field("fingerprint", fingerprint)
                .finish(),
            Self::LimitBump {
                fingerprint,
                max_limit,
            } => f
                .debug_struct("LimitBump")
                .field("fingerprint", fingerprint)
                .field("max_limit", max_limit)
                .finish(),
            Self::Readmit { fingerprint } => f
                .debug_struct("Readmit")
                .field("fingerprint", fingerprint)
                .finish(),
            Self::MvBuild { fingerprint } => f
                .debug_struct("MvBuild")
                .field("fingerprint", fingerprint)
                .finish(),
        }
    }
}

/// Controls what the writer does when a query is not subsumed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmitAction {
    /// Register and populate when not subsumed (first miss, threshold reached, invalidated).
    Admit,
    /// Do nothing when not subsumed (pending below threshold).
    CheckOnly,
}

pub enum QueryCommand {
    /// Register a new query. The writer checks subsumption and responds
    /// via `subsumption_tx` before optionally dispatching population.
    Register {
        fingerprint: u64,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Vec<EcoString>,
        started_at: Instant,
        /// Writer sends subsumption result back so the coordinator can route the held request.
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        /// What to do when the query is not subsumed by existing cached data.
        admit_action: AdmitAction,
        /// Pinned queries are protected from eviction and auto-readmitted after invalidation.
        pinned: bool,
    },

    /// Query population completed successfully
    Ready {
        fingerprint: u64,
        cached_bytes: usize,
        row_count: u64,
    },

    /// Query population failed
    Failed { fingerprint: u64 },

    /// Bump the max_limit for a cached query and re-populate with higher limit.
    /// Sent when an incoming query needs more rows than currently cached.
    LimitBump {
        fingerprint: u64,
        /// New max_limit value (None = unlimited)
        max_limit: Option<u64>,
    },

    /// Readmit a pinned query after CDC invalidation.
    /// Deferred via the writer's internal channel to avoid inline population during CDC processing.
    Readmit { fingerprint: u64 },

    /// Build (or rebuild) the materialized result for a cached query. Sent by
    /// the coordinator when it observes `mv_state == Pending { .. }` on a cache
    /// hit and transitions to `Scheduled { .. }`. The writer's handler branches
    /// on `has_table` to choose between `CREATE TABLE AS` (first build, may
    /// run the Measure size gate) and `BEGIN; TRUNCATE; INSERT; COMMIT`
    /// (rebuild; gate is sticky).
    MvBuild { fingerprint: u64 },
}

/// Commands for CDC mutations and relation tracking, sent to the writer thread
#[derive(Debug)]
pub enum CdcCommand {
    /// Source-transaction begin marker. Emitted by the CDC processor for each
    /// pgoutput BEGIN, carrying the source transaction's `xid`. The explicit
    /// delimiter lets the writer enter a frame deterministically (rather than
    /// inferring it from the first mutation), so `FrameState::Idle` genuinely
    /// means "between source transactions".
    Begin { xid: u32 },

    /// Register table metadata from CDC
    TableRegister(TableMetadata),

    /// CDC Insert operation
    Insert {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },

    /// CDC Update operation
    Update {
        relation_oid: u32,
        key_data: Vec<Option<String>>,
        row_data: Vec<Option<String>>,
    },

    /// CDC Delete operation
    Delete {
        relation_oid: u32,
        row_data: Vec<Option<String>>,
    },

    /// CDC Truncate operation
    Truncate { relation_oids: Vec<u32> },

    /// Transaction commit marker. Emitted by the CDC processor after all
    /// mutations from a single transaction have been sent. Carries the
    /// `end_lsn` of the commit record. The writer advances its
    /// `last_applied_lsn` watermark when it processes this command —
    /// guaranteeing the watermark is transaction-aligned.
    CommitMark { lsn: u64 },

    /// Keep-alive marker. Emitted when the CDC processor receives a
    /// PrimaryKeepAlive whose `wal_end` advances past the previously
    /// observed position. Carries `wal_end`. Allows the writer's
    /// `last_applied_lsn` watermark to advance during idle periods
    /// (no published-table transactions) so the gauge remains current.
    KeepAliveMark { lsn: u64 },
}
