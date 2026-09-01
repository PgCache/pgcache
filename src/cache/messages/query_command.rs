//! Commands on the query-registration lifecycle, sent to the writer thread.

use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::oneshot;
use tokio_util::bytes::BytesMut;

use crate::oid::Oid;
use crate::pg::Lsn;
use crate::pg::protocol::session::ResultFormats;
use crate::query::Fingerprint;

use super::super::types::SharedResolved;
use super::super::{
    CacheError, CacheMessage, Report, query::CacheableQuery, query_cache::QueryType,
};

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

/// Converted query data ready for processing
/// Converted query data ready for processing
pub struct QueryData {
    pub data: BytesMut,
    pub cacheable_query: Arc<CacheableQuery>,
    pub query_type: QueryType,
    pub result_formats: ResultFormats,
}

impl CacheMessage {
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
                // Replace parameters in AST, producing the per-literal form.
                match cacheable_query.parameters_replace(&parameters) {
                    Ok(replaced) => Ok(QueryData {
                        data,
                        cacheable_query: Arc::new(replaced),
                        query_type: QueryType::Extended,
                        result_formats,
                    }),
                    Err(e) => Err((e.context_transform(CacheError::from), data)),
                }
            }
            // Explain is routed by `dispatch_proxy` before `into_query_data`; if
            // it ever reaches here, forward the original bytes to origin.
            CacheMessage::Explain(_, data) => Err((CacheError::InvalidMessage.into(), data)),
        }
    }
}

/// Commands for query registration lifecycle, sent to the writer thread
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
