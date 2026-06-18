use crate::catalog::Oid;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use ecow::EcoString;
use hdrhistogram::Histogram;

use iddqd::{BiHashItem, BiHashMap, IdHashItem, IdHashMap, bi_upcast, id_upcast};

use crate::{
    cache::{memo::ResultMemo, mv::MvMeta, query::CacheableQuery},
    catalog::TableMetadata,
    query::{
        Fingerprint, FingerprintDashMap, FingerprintMap, QueryShape, ast::QueryExpr,
        constraint_index::ConstraintIndex, constraints::QueryConstraints,
        resolved::ResolvedQueryExpr,
    },
    settings::{DynamicConfigHandle, Settings},
};

/// Shared resolved query expression, wrapped in Arc to avoid deep cloning
/// on every cache hit (the dispatch→serve path).
pub type SharedResolved = Arc<ResolvedQueryExpr>;

/// State of a cached query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CachedQueryState {
    /// Seen but not yet admitted to cache. `hit_count` promotes at
    /// `admission_threshold`; `credit` is the decay budget — see
    /// `CacheDispatch::pending_initial_credit`.
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
    pub fingerprint: Fingerprint,
    /// Generation number assigned when query was registered (monotonically increasing)
    pub generation: u64,
    pub relation_oids: Vec<Oid>,
    pub query: QueryExpr,
    pub resolved: SharedResolved,
    /// Deparsed SQL body of the resolved query. Computed once at registration
    /// and reused on every non-MV cache hit to avoid per-request AST traversal.
    /// Excludes LIMIT (which varies per request) and the `SET mem.query_generation`
    /// prefix.
    pub deparsed_sql: EcoString,
    /// Parameterized serve shape (PGC-294): `deparsed_sql` with literals replaced
    /// by `$N` placeholders, plus its key and the bound literals. Built alongside
    /// the (unchanged) fingerprint; lets the serve path share one prepared
    /// statement per shape across all fingerprints that differ only in literals.
    pub serve_shape: QueryShape,
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
    type K1<'a> = Fingerprint;
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
    pub fingerprint: Fingerprint,
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
    /// Whether this query's PgEval membership can be evaluated for many CDC
    /// rows in one multi-row VALUES statement (PGC-241). Requires per-row
    /// independence: no GROUP BY / HAVING and no aggregates in the SELECT list
    /// (those evaluate against the substituted rows *as a set*, so a multi-row
    /// VALUES would change the per-row answer). Set at registration; only
    /// meaningful for `eval_strategy == PgEval`.
    pub pg_batchable: bool,
    /// This relation's columns whose values CDC eval reads: WHERE, join
    /// predicates, GROUP BY, HAVING — not the outer SELECT list. An
    /// unchanged-toast UPDATE that elides one of these and can't be repaired
    /// makes every eval verdict for this query untrustworthy, forcing
    /// invalidation (PGC-264).
    pub predicate_columns: HashSet<EcoString>,
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
    pub relation_oid: Oid,
    pub queries: FingerprintMap<UpdateQuery>,
    pub complexity_order: Vec<Fingerprint>,
    pub subsumption: ConstraintIndex<Fingerprint>,
    /// Per-table constraint index over the *full* update-query population of
    /// this relation — LocalEval and PgEval alike (PGC-292). Probed point-wise
    /// (`candidates_point`) to narrow CDC per-row work: the upsert matcher
    /// (`update_queries_execute_batch`, which filters to LocalEval after the
    /// probe), the memo-eviction pass, and `mv_dirty_mark_removed_row`. Queries
    /// with no/partial extractable constraints land in the unconstrained class
    /// and are returned for every row, so narrowing never drops a true match
    /// (no stale reads).
    pub eval_index: ConstraintIndex<Fingerprint>,
    /// Count of queries with `change_dependent == true`. Maintained on
    /// insert/remove via `change_dependent_account` so `needs_change_eval` is
    /// O(1) on the CDC hot path. Derivable from `queries`; `needs_change_eval`
    /// debug-asserts it against a recompute so a missed account call can't
    /// silently desync (a missed increment risks stale reads, a missed
    /// decrement disables the PGC-227 skip).
    change_dependent_count: usize,
}

impl UpdateQueries {
    pub fn new(relation_oid: Oid) -> Self {
        Self {
            relation_oid,
            queries: HashMap::default(),
            complexity_order: Vec::new(),
            subsumption: ConstraintIndex::new(),
            eval_index: ConstraintIndex::new(),
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
    pub fn query_remove(&mut self, fingerprint: Fingerprint) -> Option<UpdateQuery> {
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
    type Key<'a> = Oid;

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
    pub fn update_queries_remove_fingerprint(&mut self, fingerprint: Fingerprint, oids: &[Oid]) {
        for oid in oids {
            if let Some(mut queries) = self.update_queries.get_mut(oid) {
                queries.query_remove(fingerprint);
                queries.complexity_order.retain(|fp| *fp != fingerprint);
                queries.subsumption.remove(fingerprint);
                queries.eval_index.remove(fingerprint);
            }
        }
    }
}

/// Shared set of relation OIDs that have active cached queries.
/// Written by the writer thread, read by the CDC processor.
pub type ActiveRelations = Arc<ArcSwap<HashSet<Oid>>>;

/// Per-query operational metrics.
///
/// All writes go through `DashMap::get_mut()`, which holds an exclusive shard lock
/// for the duration of access. This means plain `u64` fields and the `Histogram`
/// need no additional synchronization — the shard lock provides mutual exclusion.
///
/// Two writers: dispatch on connection tasks (hit/miss/subsumption counts) and
/// the writer (all other fields including histogram recording from serve channel).
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
    /// Cache-hit latency distribution (µs, 2 significant figures). Auto-resizing
    /// so it grows only as hits are recorded.
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
            // Auto-resizing (starts at a few hundred bytes, grows on record)
            // rather than fixed-range — a fixed 1µs–60s bound pre-allocates
            // ~20 KB per query, the dominant cost under high query cardinality.
            #[allow(clippy::unwrap_used)]
            cache_hit_latency: Histogram::new(2).unwrap(),
        }
    }
}

/// Shared cache state for dispatch lookups and writer updates.
/// Uses DashMap for per-shard locking — reads to one shard don't block
/// writes to another, eliminating the global RwLock bottleneck.
/// Shared state for the BBR-lite adaptive registration gate (PGC-277).
///
/// Model (à la TCP BBR): the writer's *drain rate* (registrations reaching Ready
/// per second) is the bottleneck "bandwidth"; the writer *backlog depth* is the
/// "queue". The controller max-filters the drain rate to estimate writer capacity
/// and paces the admit rate (`reg_rate`) at it, using the min-filtered backlog as
/// the standing-queue signal. There is deliberately no upper bound on `reg_rate`:
/// when the writer keeps up it rises freely (effectively "no limit").
///
/// - Writer publishes: `completed` (monotonic Ready count) and the per-iteration
///   backlog window via `queue_observe`.
/// - Controller writes: `reg_rate`.
/// - Dispatch token bucket reads: `reg_rate`.
pub struct RegGate {
    /// Admit rate (registrations/sec) the token bucket refills at — f64 in an
    /// AtomicU64. `INFINITY` means "no gate yet" (admit all); the controller
    /// replaces it with a finite paced rate once it has signal.
    reg_rate_bits: AtomicU64,
    /// Monotonic count of registrations that reached Ready. The controller's
    /// drain-rate (capacity) estimate is `Δcompleted / Δt`.
    completed: AtomicU64,
    /// Writer backlog window min since the last controller reset. `~0` ⇒ the
    /// writer drained (healthy); `> floor` ⇒ a standing queue (saturated).
    queue_min: AtomicUsize,
    /// Writer backlog window max since reset. `0` ⇒ no registration load this
    /// window (the controller holds `reg_rate` rather than probing into a void).
    queue_max: AtomicUsize,
    /// Population in-flight: queries in `Loading` (admitted, populating, not yet
    /// Ready). Published by the writer's gauge tick from the authoritative state
    /// scan (so it never drifts). The second congestion signal — the writer's
    /// command queue (`queue_min`) catches writer-stage congestion; this catches
    /// population-stage congestion (`spawn_local` origin SELECTs), which on a
    /// remote origin is the binding constraint the command queue is blind to.
    loading: AtomicUsize,
    /// Monotonic count of registrations the token bucket *denied* (shed to
    /// origin). The controller probes the rate up only when this advanced — i.e.
    /// demand is bumping against the rate — so a partly-warm cache (low miss/
    /// registration load) can't drift the rate up into a low-demand void.
    denied: AtomicU64,
}

impl RegGate {
    pub fn new() -> Self {
        Self {
            reg_rate_bits: AtomicU64::new(f64::INFINITY.to_bits()),
            completed: AtomicU64::new(0),
            queue_min: AtomicUsize::new(usize::MAX),
            queue_max: AtomicUsize::new(0),
            loading: AtomicUsize::new(0),
            denied: AtomicU64::new(0),
        }
    }

    /// Current admit rate (registrations/sec). `INFINITY` ⇒ ungated.
    pub fn rate(&self) -> f64 {
        f64::from_bits(self.reg_rate_bits.load(Ordering::Relaxed))
    }

    /// Controller: set the paced admit rate.
    pub fn rate_set(&self, rate: f64) {
        self.reg_rate_bits.store(rate.to_bits(), Ordering::Relaxed);
    }

    /// Writer: a registration reached Ready (one unit of drained work).
    pub fn completed_inc(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Controller: monotonic completed count, for the drain-rate delta.
    pub fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    /// Writer: fold the current backlog depth into this window's min/max.
    pub fn queue_observe(&self, depth: usize) {
        self.queue_min.fetch_min(depth, Ordering::Relaxed);
        self.queue_max.fetch_max(depth, Ordering::Relaxed);
    }

    /// Controller: read and reset the backlog window. Returns `(min, max)`;
    /// `min` is `0` when the window saw an empty backlog (or saw no samples).
    pub fn window_take(&self) -> (usize, usize) {
        let max = self.queue_max.swap(0, Ordering::Relaxed);
        let min = self.queue_min.swap(usize::MAX, Ordering::Relaxed);
        (if min == usize::MAX { 0 } else { min }, max)
    }

    /// Writer gauge tick: publish the authoritative `Loading` count (population
    /// in-flight) from the state scan.
    pub fn loading_set(&self, count: usize) {
        self.loading.store(count, Ordering::Relaxed);
    }

    /// Controller: current population in-flight (queries still populating).
    pub fn loading_get(&self) -> usize {
        self.loading.load(Ordering::Relaxed)
    }

    /// Dispatch: the token bucket denied a registration (shed to origin).
    pub fn denied_inc(&self) {
        self.denied.fetch_add(1, Ordering::Relaxed);
    }

    /// Controller: monotonic denied count, for the per-window shed delta.
    pub fn denied_count(&self) -> u64 {
        self.denied.load(Ordering::Relaxed)
    }
}

impl Default for RegGate {
    fn default() -> Self {
        Self::new()
    }
}

pub struct CacheStateView {
    pub cached_queries: FingerprintDashMap<CachedQueryView>,
    pub metrics: FingerprintDashMap<QueryMetrics>,
    pub started_at: Instant,
    /// Cache hits observed during the current GC interval. Incremented by the
    /// dispatch on each Ready-state serve; snapshot-and-zeroed by the writer
    /// on the 1s GC tick. Drives the Pending-credit decay scheme.
    pub hits_since_gc: AtomicU32,
    /// Previous GC tick's hit count, used by the dispatch to size the
    /// initial credit stamped on new Pending entries (or on Pending re-hits).
    pub last_hits_per_gc: AtomicU32,
    /// In-process hot-result cache (PGC-236). Captured by the serve pool, served by
    /// dispatch, evicted (slot-bumped) by the writer's CDC path.
    pub memo: ResultMemo,
    /// Set by the memory monitor when used memory crosses the registration
    /// budget high-water mark. While set, dispatch forwards brand-new (and
    /// in-flight Loading) queries to origin instead of registering them, and
    /// population workers skip the in-flight backlog — bounding memory growth.
    /// Already-cached queries are unaffected. `Arc` so population workers can
    /// share it; wait-free read on the hot path.
    pub registration_throttled: Arc<AtomicBool>,
    /// Set by the writer tick when the cache volume is under disk pressure
    /// (free space below the reserve). While set, dispatch forwards brand-new
    /// queries to origin instead of registering them — stopping cache growth
    /// while the tick's escalating reclaim frees disk (PGC-276). Separate from
    /// `registration_throttled` (memory) so the two pressures are independent;
    /// dispatch ORs them.
    pub disk_throttle: Arc<AtomicBool>,
    /// Authoritative registered (Ready) query count, published by the writer so
    /// the memory monitor can size the count cap from a real measurement (PGC-251).
    pub registered_count: Arc<AtomicUsize>,
    /// Max registered queries that fit the memory budget, published by the
    /// memory monitor and read by the writer's eviction loop. `usize::MAX` =
    /// no cap (insufficient signal, or memory not detectable) (PGC-251).
    pub query_count_cap: Arc<AtomicUsize>,
    /// Set by the memory monitor under memory pressure; read by the serve loop to
    /// gate connection recycling. Clear → no recycling (PGC-251 Slice 1d).
    pub recycle_wanted: Arc<AtomicBool>,
    /// Monotonic count of serve connections recycled, incremented by the serve
    /// loop and read by the monitor to reset the peak after a full pool cycle.
    pub recycle_count: Arc<AtomicUsize>,
    /// BBR-lite adaptive registration gate (PGC-277): writer-published drain +
    /// backlog signals and the controller's paced admit rate.
    pub reg_gate: Arc<RegGate>,
}

impl std::fmt::Debug for CacheStateView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStateView")
            .field("cached_queries", &self.cached_queries)
            .field("metrics_len", &self.metrics.len())
            .field("started_at", &self.started_at)
            .field("memo_entries", &self.memo.len())
            .finish()
    }
}

impl CacheStateView {
    pub fn new(dynamic: DynamicConfigHandle) -> Self {
        Self {
            cached_queries: DashMap::default(),
            metrics: DashMap::default(),
            started_at: Instant::now(),
            hits_since_gc: AtomicU32::new(0),
            last_hits_per_gc: AtomicU32::new(0),
            registration_throttled: Arc::new(AtomicBool::new(false)),
            disk_throttle: Arc::new(AtomicBool::new(false)),
            registered_count: Arc::new(AtomicUsize::new(0)),
            query_count_cap: Arc::new(AtomicUsize::new(usize::MAX)),
            recycle_wanted: Arc::new(AtomicBool::new(false)),
            recycle_count: Arc::new(AtomicUsize::new(0)),
            reg_gate: Arc::new(RegGate::new()),
            memo: ResultMemo::new(dynamic),
        }
    }

    /// Whether dispatch should forward new queries to origin instead of
    /// registering them — under memory pressure (`registration_throttled`) or
    /// disk pressure (`disk_throttle`). Wait-free; on the hot path.
    pub fn throttled(&self) -> bool {
        self.registration_throttled.load(Ordering::Relaxed)
            || self.disk_throttle.load(Ordering::Relaxed)
    }
}

/// Lightweight view of a cached query for dispatch lookups.
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
    /// Parameterized serve shape (mirrors `CachedQuery.serve_shape`, PGC-294).
    /// None while Loading; Some once the view transitions to Ready.
    pub serve_shape: Option<QueryShape>,
    /// Maximum rows cached for this fingerprint (None = all rows)
    pub max_limit: Option<u64>,
    /// CLOCK reference bit — set by dispatch on cache hit, read/cleared by writer during eviction
    pub referenced: bool,
    /// MV shape classification, runtime state, and captured output column
    /// names — all written by the writer (registration / MV build).
    pub mv: MvMeta,
}

/// A pre-validated pinned query, ready for registration.
pub struct PinnedQuery {
    pub fingerprint: Fingerprint,
    pub cacheable_query: Arc<CacheableQuery>,
}
