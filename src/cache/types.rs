use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::AtomicU32;
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use ecow::EcoString;
use hdrhistogram::Histogram;

use iddqd::{BiHashItem, BiHashMap, IdHashItem, IdHashMap, bi_upcast, id_upcast};

use crate::{
    cache::{mv::MvMeta, query::CacheableQuery, subsumption_index::SubsumptionIndex},
    catalog::TableMetadata,
    query::{ast::QueryExpr, constraints::QueryConstraints, resolved::ResolvedQueryExpr},
    settings::{DynamicConfigHandle, Settings},
};

/// Shared resolved query expression, wrapped in Arc to avoid deep cloning
/// on every cache hit (the coordinator→worker path).
pub type SharedResolved = Arc<ResolvedQueryExpr>;

/// State of a cached query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedQueryState {
    /// Seen but not yet admitted to cache. `hit_count` promotes at
    /// `admission_threshold`; `credit` is the decay budget — see
    /// `QueryCache::pending_initial_credit`.
    Pending { hit_count: u32, credit: u32 },
    /// Admitted, population in progress
    Loading,
    /// Cached and serving hits
    Ready,
    /// CDC-invalidated, awaiting re-hit for fast readmission (clock policy only)
    Invalidated,
}

/// A cached query with its metadata and state
#[derive(Debug)]
pub struct CachedQuery {
    pub fingerprint: u64,
    /// Generation number assigned when query was registered (monotonically increasing)
    pub generation: u64,
    pub relation_oids: Vec<u32>,
    pub query: QueryExpr,
    pub resolved: SharedResolved,
    /// Deparsed SQL body of the resolved query. Computed once at registration
    /// and reused on every non-MV cache hit to avoid per-request AST traversal.
    /// Excludes LIMIT (which varies per request) and the `SET mem.query_generation`
    /// prefix.
    pub deparsed_sql: EcoString,
    /// Maximum rows cached for this fingerprint.
    /// `None` = all rows cached (query seen without LIMIT, or OFFSET-only).
    /// `Some(n)` = up to `n` rows cached (max LIMIT+OFFSET across all variants seen).
    pub max_limit: Option<u64>,
    /// Estimated size of cached data in bytes (sum of raw value bytes)
    pub cached_bytes: usize,
    /// Timestamp when registration started (for latency metrics)
    pub registration_started_at: Option<Instant>,
    /// True when in Invalidated state (kept in cached_queries for metadata reuse on readmission)
    pub invalidated: bool,
    /// Pinned queries are protected from eviction and auto-readmitted after CDC invalidation.
    pub pinned: bool,
}

impl BiHashItem for CachedQuery {
    type K1<'a> = u64;
    type K2<'b> = u64;

    fn key1(&self) -> Self::K1<'_> {
        self.fingerprint
    }

    fn key2(&self) -> Self::K2<'_> {
        self.generation
    }

    bi_upcast!();
}

// impl IdHashItem for CachedQuery {
//     type Key<'a> = u64;

//     fn key(&self) -> Self::Key<'_> {
//         self.fingerprint
//     }

//     id_upcast!();
// }

/// The kind of subquery context a table was found in.
/// Determines invalidation behavior based on whether the subquery's
/// result set growing or shrinking affects the outer query.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum SubqueryKind {
    /// IN, EXISTS, FROM subquery, CTE.
    /// Set growth → outer result grows → invalidate.
    /// Set shrink → outer result shrinks → skip.
    Inclusion,
    /// NOT IN (<> ALL), NOT EXISTS.
    /// Set growth → outer result shrinks → skip.
    /// Set shrink → outer result grows → invalidate.
    Exclusion,
    /// Scalar subquery returning a single value.
    /// Any change → invalidate.
    Scalar,
}

/// Strategy for evaluating an update query's WHERE clause during CDC handling.
///
/// Determined once at registration time based on the shape of the resolved query.
/// `LocalEval` rows skip per-query PG round-trips; `PgEval` rows fall through to
/// the `INSERT ... WHERE EXISTS` dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateEvalStrategy {
    /// WHERE clause is fully representable by the Rust evaluator and can be
    /// decided against a single CDC row without touching the cache DB.
    LocalEval,
    /// WHERE clause shape is not representable by the Rust evaluator (subqueries,
    /// GROUP BY/HAVING, unsupported expressions, multi-table, or non-FromClause
    /// source). CDC must use the per-query `INSERT ... WHERE EXISTS` path.
    PgEval,
}

/// Whether an update query was derived from a direct table or a subquery table
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UpdateQuerySource {
    /// Table appears in the FROM clause (inner join or single table)
    FromClause,
    /// Table appears inside a subquery, CTE, or derived table
    Subquery(SubqueryKind),
    /// Table is on the terminal optional side of an outer join.
    /// Its columns don't appear in WHERE or other join conditions, so
    /// CDC INSERT/DELETE can be handled in place without invalidation.
    /// The preserved side already has the row — changes here only affect
    /// which values fill the NULL-padded columns.
    OuterJoinTerminal,
    /// Table is on the non-terminal optional side of an outer join
    /// (its columns appear in WHERE or other join conditions).
    /// CDC events trigger full query invalidation rather than row-level updates.
    OuterJoinOptional,
}

/// Query used to update cached results when data changes
#[derive(Debug, Clone)]
pub struct UpdateQuery {
    /// Fingerprint of cached query that generated this update query
    pub fingerprint: u64,
    /// Resolved AST query
    pub resolved: ResolvedQueryExpr,
    /// Complexity score (lower = simpler = more likely to match = try first)
    pub complexity: usize,
    /// Whether this table was found directly in FROM or inside a subquery
    pub source: UpdateQuerySource,
    /// WHERE clause constraints for CDC invalidation filtering
    pub constraints: QueryConstraints,
    /// Whether the parent cached query has a LIMIT (max_limit.is_some()).
    /// Used by CDC to determine if DELETE should trigger invalidation.
    pub has_limit: bool,
    /// Eval strategy for this query's WHERE clause during CDC row matching.
    pub eval_strategy: UpdateEvalStrategy,
    /// Columns from ORDER BY / WHERE / HAVING that define the LIMIT window.
    /// Populated only when `has_limit`; consumed by row_cached_invalidation_check
    /// to detect updates that may demote a cached row out of the window (PGC-94).
    pub limit_window_columns: HashSet<EcoString>,
    /// Whether a CDC UPDATE for this query can ever invalidate. When `false`,
    /// `handle_update` skips the `query_row_changes` SELECT and the invalidation
    /// check entirely (PGC-227). Set at registration from
    /// `update_invalidation_possible` (the single source of truth — needs the
    /// relation's table name).
    pub change_dependent: bool,
}

impl UpdateQuery {
    /// Whether a CDC UPDATE (`operation = Upsert`) for this query could ever
    /// invalidate it: the static upper bound of `row_cached_invalidation_check`
    /// ∪ `row_uncached_invalidation_check` over all possible row values. This is
    /// the single source of truth for `change_dependent` (PGC-227); each clause
    /// mirrors a check branch and the two must stay in lockstep — the
    /// `debug_assert` in `update_queries_check_invalidate` enforces it at
    /// runtime by failing if a check ever invalidates while this returns false.
    pub fn update_invalidation_possible(&self, table_name: &str) -> bool {
        // Cached path: Subquery / non-terminal outer join always invalidate.
        matches!(
            self.source,
            UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional
        )
        // Cached path: a windowed FromClause query invalidates when a window
        // column changes (PGC-94).
        || (matches!(self.source, UpdateQuerySource::FromClause)
            && self.has_limit
            && !self.limit_window_columns.is_empty())
        // Cached path: a join column on this table changing can invalidate.
        || self.constraints.table_join_columns(table_name).next().is_some()
        // Uncached path: any multi-table FromClause query invalidates on a row
        // that newly satisfies the join — the partner side may not be cached,
        // so in-place maintenance can't materialize it. Independent of whether
        // the equivalence was recorded as `col = col`, so cross joins and
        // expression/cast equi-joins land here rather than the join-column clause.
        || (matches!(self.source, UpdateQuerySource::FromClause)
            && !self.resolved.is_single_table())
    }
}

/// Collection of update queries for a specific relation.
///
/// `queries` is keyed by fingerprint for O(1) lookup. `complexity_order`
/// holds the same fingerprints in complexity-ascending order so CDC can keep
/// the "try simplest first" iteration property. `subsumption` is a typed
/// index over the queries' WHERE-clause constraints used by
/// `subsumption_check` for sub-linear candidate lookup (see PGC-119).
#[derive(Debug)]
pub struct UpdateQueries {
    pub relation_oid: u32,
    pub queries: HashMap<u64, UpdateQuery>,
    pub complexity_order: Vec<u64>,
    pub subsumption: SubsumptionIndex,
    /// Count of queries with `change_dependent == true`. Maintained on
    /// insert/remove via `change_dependent_account` so `needs_change_eval` is
    /// O(1) on the CDC hot path. Derivable from `queries`; `needs_change_eval`
    /// debug-asserts it against a recompute so a missed account call can't
    /// silently desync (a missed increment risks stale reads, a missed
    /// decrement disables the PGC-227 skip).
    change_dependent_count: usize,
}

impl UpdateQueries {
    pub fn new(relation_oid: u32) -> Self {
        Self {
            relation_oid,
            queries: HashMap::new(),
            complexity_order: Vec::new(),
            subsumption: SubsumptionIndex::new(),
            change_dependent_count: 0,
        }
    }

    /// Whether any query over this relation needs `query_row_changes` to decide
    /// a CDC UPDATE's invalidation. When false, `handle_update` skips the
    /// SELECT and the invalidation check entirely (PGC-227).
    pub fn needs_change_eval(&self) -> bool {
        debug_assert_eq!(
            self.change_dependent_count,
            self.queries.values().filter(|q| q.change_dependent).count(),
            "change_dependent_count desynced from queries — insert/remove must go \
             through query_insert/query_remove"
        );
        self.change_dependent_count > 0
    }

    /// Insert (or replace) an update query, keeping `change_dependent_count` in
    /// sync with `queries`. Returns the replaced entry, if any. Replacement is
    /// real: the same fingerprint can be registered more than once for a
    /// relation (e.g. a self-correlated subquery decorrelates into multiple
    /// update queries over the same table), and the map is keyed by fingerprint.
    pub fn query_insert(&mut self, query: UpdateQuery) -> Option<UpdateQuery> {
        let change_dependent = query.change_dependent;
        let prev = self.queries.insert(query.fingerprint, query);
        if let Some(prev) = &prev {
            self.change_dependent_account(prev.change_dependent, false);
        }
        self.change_dependent_account(change_dependent, true);
        prev
    }

    /// Remove an update query by fingerprint, keeping `change_dependent_count`
    /// in sync with `queries`. Returns the removed entry, if any.
    pub fn query_remove(&mut self, fingerprint: u64) -> Option<UpdateQuery> {
        let removed = self.queries.remove(&fingerprint);
        if let Some(removed) = &removed {
            self.change_dependent_account(removed.change_dependent, false);
        }
        removed
    }

    /// Account for a query being added to / removed from the set. `added`
    /// increments on insert, decrements on remove. Private: callers mutate
    /// `queries` only via `query_insert`/`query_remove`, which keep the count
    /// in lockstep.
    fn change_dependent_account(&mut self, change_dependent: bool, added: bool) {
        if !change_dependent {
            return;
        }
        if added {
            self.change_dependent_count += 1;
        } else {
            self.change_dependent_count = self.change_dependent_count.saturating_sub(1);
        }
    }

    /// Iterate update queries in complexity-ascending order. CDC code paths
    /// rely on this ordering to try simpler queries first.
    pub fn iter_complexity_ordered(&self) -> impl Iterator<Item = &UpdateQuery> {
        self.complexity_order
            .iter()
            .filter_map(|fp| self.queries.get(fp))
    }
}

impl IdHashItem for UpdateQueries {
    type Key<'a> = u32;

    fn key(&self) -> Self::Key<'_> {
        self.relation_oid
    }

    id_upcast!();
}

/// Main cache data structure containing all cached state
#[derive(Debug)]
pub struct Cache {
    pub tables: BiHashMap<TableMetadata>,
    pub update_queries: IdHashMap<UpdateQueries>,
    pub cached_queries: BiHashMap<CachedQuery>,
    /// Monotonically increasing generation counter (starts at 1)
    pub generation_counter: u64,
    /// Generations of active cached queries (for efficient min-tracking)
    pub generations: BTreeSet<u64>,
    /// Size of currently cached data, updated after loading queries or purging data
    /// Actual size can drift from this value because of CDC traffic
    pub current_size: usize,
    /// Dynamic config handle for runtime-adjustable cache settings
    pub dynamic: DynamicConfigHandle,
}

impl Cache {
    pub fn new(settings: &Settings) -> Self {
        Self {
            tables: BiHashMap::new(),
            update_queries: IdHashMap::new(),
            cached_queries: BiHashMap::new(),
            generation_counter: 0,
            generations: BTreeSet::new(),
            current_size: 0,
            dynamic: settings.dynamic.clone(),
        }
    }

    /// Returns the minimum generation that can be safely purged.
    /// This is the highest generation that is less than all active generations
    /// or the current generation_counter if there are no active generations
    pub fn generation_purge_threshold(&self) -> u64 {
        self.generations
            .first()
            .map(|min| min.saturating_sub(1))
            .unwrap_or(self.generation_counter)
    }

    /// Drop `fingerprint`'s entries from `update_queries` for each given OID.
    /// No-op for OIDs without an `update_queries` entry. Also tears down the
    /// subsumption index entry so the lookup path doesn't return a stale
    /// candidate.
    pub fn update_queries_remove_fingerprint(&mut self, fingerprint: u64, oids: &[u32]) {
        for oid in oids {
            if let Some(mut queries) = self.update_queries.get_mut(oid) {
                queries.query_remove(fingerprint);
                queries.complexity_order.retain(|fp| *fp != fingerprint);
                queries.subsumption.remove(fingerprint);
            }
        }
    }
}

/// Shared set of relation OIDs that have active cached queries.
/// Written by the writer thread, read by the CDC processor.
pub type ActiveRelations = Arc<ArcSwap<HashSet<u32>>>;

/// Per-query operational metrics.
///
/// All writes go through `DashMap::get_mut()`, which holds an exclusive shard lock
/// for the duration of access. This means plain `u64` fields and the `Histogram`
/// need no additional synchronization — the shard lock provides mutual exclusion.
///
/// Only two threads write: coordinator (hit/miss/subsumption counts) and
/// writer (all other fields including histogram recording from worker channel).
pub struct QueryMetrics {
    pub hit_count: u64,
    pub miss_count: u64,
    /// Nanoseconds since `CacheStateView.started_at`
    pub last_hit_at_ns: Option<NonZeroU64>,
    /// Nanoseconds since `CacheStateView.started_at` when query was first seen. Set once, never cleared.
    pub registered_at_ns: Option<NonZeroU64>,
    /// Nanoseconds since `CacheStateView.started_at` when query last became Ready. Cleared on invalidation/eviction.
    pub cached_since_ns: Option<NonZeroU64>,
    pub invalidation_count: u64,
    pub readmission_count: u64,
    pub eviction_count: u64,
    pub subsumption_count: u64,
    pub population_count: u64,
    pub last_population_duration_us: Option<NonZeroU64>,
    pub total_bytes_served: u64,
    /// Physical rows inserted during last population (sum across all branch tables)
    pub population_row_count: u64,
    /// Cache-hit latency distribution (1us–60s, 2 significant figures)
    pub cache_hit_latency: Histogram<u64>,
}

impl QueryMetrics {
    pub fn new(registered_at_ns: Option<NonZeroU64>) -> Self {
        Self {
            hit_count: 0,
            miss_count: 0,
            last_hit_at_ns: None,
            registered_at_ns,
            cached_since_ns: None,
            invalidation_count: 0,
            readmission_count: 0,
            eviction_count: 0,
            subsumption_count: 0,
            population_count: 0,
            last_population_duration_us: None,
            total_bytes_served: 0,
            population_row_count: 0,
            #[allow(clippy::unwrap_used)]
            cache_hit_latency: Histogram::new_with_bounds(1, 60_000_000, 2).unwrap(),
        }
    }
}

/// Metrics sent from worker to coordinator after each cache hit.
pub struct WorkerMetrics {
    pub fingerprint: u64,
    pub latency_us: u64,
    pub bytes_served: u64,
}

/// Shared cache state for coordinator lookups and writer updates.
/// Uses DashMap for per-shard locking — reads to one shard don't block
/// writes to another, eliminating the global RwLock bottleneck.
pub struct CacheStateView {
    pub cached_queries: DashMap<u64, CachedQueryView>,
    pub metrics: DashMap<u64, QueryMetrics>,
    pub started_at: Instant,
    /// Cache hits observed during the current GC interval. Incremented by the
    /// coordinator on each Ready-state serve; snapshot-and-zeroed by the writer
    /// on the 1s GC tick. Drives the Pending-credit decay scheme.
    pub hits_since_gc: AtomicU32,
    /// Previous GC tick's hit count, used by the coordinator to size the
    /// initial credit stamped on new Pending entries (or on Pending re-hits).
    pub last_hits_per_gc: AtomicU32,
}

impl std::fmt::Debug for CacheStateView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStateView")
            .field("cached_queries", &self.cached_queries)
            .field("metrics_len", &self.metrics.len())
            .field("started_at", &self.started_at)
            .finish()
    }
}

impl CacheStateView {
    pub fn new() -> Self {
        Self {
            cached_queries: DashMap::new(),
            metrics: DashMap::new(),
            started_at: Instant::now(),
            hits_since_gc: AtomicU32::new(0),
            last_hits_per_gc: AtomicU32::new(0),
        }
    }
}

/// Lightweight view of a cached query for coordinator lookups.
#[derive(Debug, Clone)]
pub struct CachedQueryView {
    pub state: CachedQueryState,
    /// Generation number (0 for Loading placeholder before writer assigns real value)
    pub generation: u64,
    /// Resolved query (None for Loading placeholder before writer resolves)
    pub resolved: Option<SharedResolved>,
    /// Precomputed deparsed SQL body (mirrors `CachedQuery.deparsed_sql`).
    /// None while Loading; Some once the view transitions to Ready.
    pub deparsed_sql: Option<EcoString>,
    /// Maximum rows cached for this fingerprint (None = all rows)
    pub max_limit: Option<u64>,
    /// CLOCK reference bit — set by coordinator on cache hit, read/cleared by writer during eviction
    pub referenced: bool,
    /// MV shape classification, runtime state, and captured output column
    /// names — all written by the writer (registration / MV build).
    pub mv: MvMeta,
}

/// A pre-validated pinned query, ready for registration.
pub struct PinnedQuery {
    pub fingerprint: u64,
    pub cacheable_query: Arc<CacheableQuery>,
}
