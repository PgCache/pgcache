use crate::catalog::Oid;
use crate::pg::Lsn;
use crate::query::Fingerprint;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use smallvec::SmallVec;
use tokio::sync::oneshot;
use tokio_util::bytes::{Bytes, BytesMut};

use super::reply::ReplySender;
use super::{CacheError, Report, query::CacheableQuery, query_cache::QueryType};
use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;
use crate::pg::protocol::extended::ResultFormats;
use crate::proxy::ClientSocket;
use crate::query::transform::query_expr_parameters_replace;
use crate::timing::QueryTiming;

use super::types::SharedResolved;

/// Result of a subsumption check, sent from the writer back to the dispatch
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

/// Converted query data ready for processing
pub struct QueryData {
    pub data: BytesMut,
    pub cacheable_query: Arc<CacheableQuery>,
    pub query_type: QueryType,
    pub result_formats: ResultFormats,
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
                result_formats: ResultFormats::Implicit,
            }),
            CacheMessage::QueryParameterized(data, cacheable_query, parameters, result_formats) => {
                if parameters.is_empty() {
                    // No bind parameters → nothing to substitute. Reuse the shared
                    // CacheableQuery (Arc clone) instead of cloning the whole AST.
                    // The convert-time constant fold already ran; the bind-time
                    // fold only matters once a parameter has been substituted.
                    return Ok(QueryData {
                        data,
                        cacheable_query,
                        query_type: QueryType::Extended,
                        result_formats,
                    });
                }
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

/// Commands for query registration lifecycle, sent to the writer thread
impl std::fmt::Debug for QueryCommand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Register { fingerprint, .. } => f
                .debug_struct("Register")
                .field("fingerprint", fingerprint)
                .finish_non_exhaustive(),
            Self::Failed {
                fingerprint,
                generation,
            } => f
                .debug_struct("Failed")
                .field("fingerprint", fingerprint)
                .field("generation", generation)
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
            Self::MvBuildComplete { fingerprint, .. } => f
                .debug_struct("MvBuildComplete")
                .field("fingerprint", fingerprint)
                .finish_non_exhaustive(),
            Self::Merge(m) => f
                .debug_struct("Merge")
                .field("fingerprint", &m.fingerprint)
                .field("generation", &m.generation)
                .field("relations", &m.staged.len())
                .finish_non_exhaustive(),
        }
    }
}

/// Payload for `QueryCommand::Merge`: a population staged its snapshot and the
/// writer must merge each relation's staging table into the shared cache table.
pub struct PopulationMerge {
    pub fingerprint: Fingerprint,
    pub generation: u64,
    /// `(relation_oid, staging table name in pgcache_stage)` per relation read.
    pub staged: Vec<(Oid, EcoString)>,
    pub cached_bytes: usize,
    pub row_count: u64,
    /// Origin WAL LSN captured after the population reads (upper bound on the
    /// snapshot). The merge itself is withheld until the CDC apply watermark
    /// reaches this (PGC-272, superseding the PGC-250 Slice B Ready-time
    /// gate): snapshot-state rows entering the shared table early would be
    /// served by already-Ready bystander queries as a torn mix of two origin
    /// points in time.
    pub snapshot_lsn: Lsn,
    /// When the staged population entered the merge pipeline (worker send time);
    /// drives the merge-wait histogram at apply (PGC-335).
    pub enqueued_at: std::time::Instant,
    /// Fetch+stage wall time for this population (origin read + cache staging,
    /// excluding queue wait). Feeds the per-query estimate that sets the
    /// re-population coalesce-forward deadline (PGC-335).
    pub fetch_stage_ms: f64,
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
        fingerprint: Fingerprint,
        cacheable_query: Arc<CacheableQuery>,
        search_path: Arc<[EcoString]>,
        started_at: Instant,
        /// Writer sends subsumption result back so the dispatch can route the held request.
        subsumption_tx: oneshot::Sender<SubsumptionResult>,
        /// What to do when the query is not subsumed by existing cached data.
        admit_action: AdmitAction,
        /// Pinned queries are protected from eviction and auto-readmitted after invalidation.
        pinned: bool,
    },

    /// Query population failed. `generation` identifies which population (a
    /// query can have a superseded generation still in flight) so the writer
    /// releases the right deleted-key tracking.
    Failed {
        fingerprint: Fingerprint,
        generation: u64,
    },

    /// Bump the max_limit for a cached query and re-populate with higher limit.
    /// Sent when an incoming query needs more rows than currently cached.
    LimitBump {
        fingerprint: Fingerprint,
        /// New max_limit value (None = unlimited)
        max_limit: Option<u64>,
    },

    /// Readmit a pinned query after CDC invalidation.
    /// Deferred via the writer's internal channel to avoid inline population during CDC processing.
    Readmit { fingerprint: Fingerprint },

    /// Population staged its origin snapshot into `pgcache_stage`. The writer
    /// merges it into the shared cache table(s) — filtering rows CDC removed
    /// during the population — when no CDC frame is open, then marks the query
    /// Ready (PGC-250).
    Merge(PopulationMerge),

    /// Build (or rebuild) the materialized result for a cached query. Sent by
    /// the dispatch when it observes `mv_state == Pending { .. }` on a cache
    /// hit and transitions to `Scheduled { .. }`. The writer's handler snapshots
    /// the build context, flips to `Building { has_table }`, and spawns the SQL
    /// onto the shared runtime; `has_table` chooses between `CREATE TABLE AS`
    /// (first build, may run the Measure size gate) and
    /// `BEGIN; TRUNCATE; INSERT; COMMIT` (rebuild; gate is sticky).
    MvBuild { fingerprint: Fingerprint },

    /// A spawned MV build task finished; the writer applies the state
    /// transition. Keeping the flip on the writer serializes it against CDC
    /// dirty-marking, so a build raced by a relevant change is always observed
    /// as `BuildingDirty` here and discarded.
    MvBuildComplete {
        fingerprint: Fingerprint,
        outcome: MvBuildOutcome,
    },
}

/// Result of an off-thread MV build task. The task runs SQL only; all
/// `MvState` transitions happen in the writer's `MvBuildComplete` handler.
pub enum MvBuildOutcome {
    /// Build batch committed; the MV table holds the result.
    Built {
        output_columns: Arc<[EcoString]>,
        /// Build path taken (false = `CREATE TABLE AS` first build). On
        /// success a table exists either way; this picks the metric label
        /// and the first-build source-row-state recheck.
        was_first_build: bool,
    },
    /// Measure size gate failed. Terminal for this cache entry.
    Ineligible,
    /// Build failed; the task already rolled back / dropped the partial
    /// table. `has_table` is what remains on disk for the `Pending` reset.
    Failed { has_table: bool },
}

/// A single column value decoded from a pgoutput tuple (PGC-264).
///
/// `Toasted` is pgoutput's "unchanged TOASTed value" marker: the origin elides
/// the value from UPDATE new-row images when the column didn't change, on the
/// contract that the consumer already holds it. It must never be conflated
/// with `Null` — doing so overwrites cached TOAST values with NULL.
// ByteString (ADR-032 boundary exception): each value is a zero-copy
// refcounted view into its replication frame, so decoding and cloning never
// copy the text. The view pins its frame, which is bounded by the row size.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CdcValue {
    Null,
    Text(ByteString),
    Toasted,
}

/// Convert decoded tuple values to the downstream row representation,
/// appending to `row_data` (callers pass a recycled Vec so steady-state
/// conversion allocates nothing) and reporting the column indexes that
/// carried the unchanged-toast marker (`Toasted` maps to `None` in the
/// output). Past the writer's repair step the indexes must be empty or
/// handled — this is the only path from `CdcValue` rows to
/// `Option<ByteString>` rows.
pub fn cdc_values_convert(
    values: Vec<CdcValue>,
    row_data: &mut Vec<Option<ByteString>>,
) -> Vec<usize> {
    let mut toasted = Vec::new();
    row_data.reserve(values.len());
    for (idx, value) in values.into_iter().enumerate() {
        row_data.push(match value {
            CdcValue::Null => None,
            CdcValue::Text(text) => Some(text),
            CdcValue::Toasted => {
                toasted.push(idx);
                None
            }
        });
    }
    toasted
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
        relation_oid: Oid,
        row_data: Vec<CdcValue>,
    },

    /// CDC Update operation
    Update {
        relation_oid: Oid,
        key_data: Vec<CdcValue>,
        row_data: Vec<CdcValue>,
    },

    /// CDC Delete operation
    Delete {
        relation_oid: Oid,
        row_data: Vec<CdcValue>,
    },

    /// CDC Truncate operation
    Truncate { relation_oids: Vec<Oid> },

    /// Transaction commit marker. Emitted by the CDC processor after all
    /// mutations from a single transaction have been sent. Carries the
    /// `end_lsn` of the commit record. The writer advances its
    /// `last_applied_lsn` watermark when it processes this command —
    /// guaranteeing the watermark is transaction-aligned.
    CommitMark { lsn: Lsn },

    /// Keep-alive marker. Emitted when the CDC processor receives a
    /// PrimaryKeepAlive whose `wal_end` advances past the previously
    /// observed position. Carries `wal_end`. Allows the writer's
    /// `last_applied_lsn` watermark to advance during idle periods
    /// (no published-table transactions) so the gauge remains current.
    KeepAliveMark { lsn: Lsn },
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cdc_values_convert_reports_toasted_indexes() {
        let mut row_data = Vec::new();
        let toasted = cdc_values_convert(
            vec![
                CdcValue::Text("a".into()),
                CdcValue::Toasted,
                CdcValue::Null,
                CdcValue::Toasted,
            ],
            &mut row_data,
        );
        assert_eq!(
            row_data,
            vec![Some("a".into()), None, None, None],
            "Toasted and Null both map to None in the row representation"
        );
        assert_eq!(toasted, vec![1, 3]);
    }

    #[test]
    fn test_cdc_values_convert_no_toast() {
        let mut row_data = Vec::new();
        let toasted = cdc_values_convert(
            vec![CdcValue::Null, CdcValue::Text("b".into())],
            &mut row_data,
        );
        assert_eq!(row_data, vec![None, Some("b".into())]);
        assert!(toasted.is_empty());
    }
}
