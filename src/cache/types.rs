use crate::catalog::Oid;
use std::collections::{BTreeSet, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize, Ordering};
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use ecow::EcoString;
use hdrhistogram::Histogram;

use iddqd::{BiHashItem, BiHashMap, IdHashMap, bi_upcast};

use crate::{
    cache::{memo::ResultMemo, mv::MvMeta, query::CacheableQuery},
    catalog::TableMetadata,
    query::{
        Fingerprint, FingerprintDashMap, QueryShape, ast::QueryExpr, resolved::ResolvedQueryExpr,
    },
    settings::{DynamicConfigHandle, Settings},
};

use super::reg_gate::RegGate;
use super::update_query::UpdateQueries;

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
