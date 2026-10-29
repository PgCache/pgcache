use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroU64;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use arc_swap::ArcSwap;
use dashmap::DashMap;
use ecow::EcoString;
use hdrhistogram::Histogram;
use iddqd::{BiHashItem, BiHashMap, IdHashMap, bi_upcast};
use rootcause::Report;

use super::generation::Generation;
use super::join_subsumers::JoinSubsumers;
use super::population_pool::PopulationPool;
use super::privileges::PrivilegeVerdicts;
use super::reg_gate::RegGate;
use super::serve_pool_state::ServePool;
use super::update_query::UpdateQueries;
use crate::oid::Oid;
use crate::query::update::ReadRelations;
use crate::{
    cache::{CacheError, CacheResult, memo::ResultMemo, mv::MvMeta, query::CacheableQuery},
    catalog::{Catalog, RelationName, TableMetadata},
    query::{
        Fingerprint, FingerprintDashMap, QueryShape, SessionKeyDashMap,
        resolve::{PrivilegeNeeds, ResolvedQuery},
    },
    settings::{DynamicConfigHandle, Settings},
};

/// An alias-map entry: the canonical fingerprint a session key resolved to,
/// stamped with the writer's sweep tick at insertion. The sweep spares an
/// alias inserted since the previous sweep: its dispatch may not have
/// inserted the entry under the fingerprint yet.
#[derive(Debug, Clone, Copy)]
pub struct Alias {
    pub fingerprint: Fingerprint,
    pub tick: u64,
    /// The key carries an RLS context (PGC-113): the fingerprint depends on
    /// the role's policies, so a role or policy change drops the alias.
    pub rls: bool,
}

/// The relations origin applies row-level security to, by bare name (any
/// schema), each with the custom settings its policies read (PGC-113). The
/// proxy keys a read naming one of them on the session's values of those
/// settings and its role; the writer resolves the name exactly and inlines.
#[derive(Debug, Default)]
pub struct RlsRelations {
    by_name: HashMap<EcoString, Vec<EcoString>>,
}

impl RlsRelations {
    /// From `(table name, settings its policies read)` entries; two
    /// relations of one name in different schemas pool their settings.
    pub fn new(entries: impl IntoIterator<Item = (EcoString, Vec<EcoString>)>) -> Self {
        let mut by_name: HashMap<EcoString, Vec<EcoString>> = HashMap::new();
        for (name, settings) in entries {
            let pooled = by_name.entry(name).or_default();
            pooled.extend(settings);
            pooled.sort();
            pooled.dedup();
        }
        Self { by_name }
    }

    pub fn is_empty(&self) -> bool {
        self.by_name.is_empty()
    }

    /// The settings the policies of any relation named `name` read, or
    /// `None` when no such relation has row-level security.
    pub fn settings_of(&self, name: &str) -> Option<&[EcoString]> {
        self.by_name.get(name).map(Vec::as_slice)
    }
}

/// The canonical plain tree with its bindings (ADR-057), shared so a cache
/// hit never deep-clones it (the dispatch→serve path).
pub(super) type SharedResolvedQuery = Arc<ResolvedQuery>;

/// The serve shape, shared between the cached query, its view and every
/// hit so serving allocates nothing for it.
pub(super) type SharedQueryShape = Arc<QueryShape>;

pub(super) use super::serve_decision::CachedQueryState;

/// A cached query with its metadata and state
#[derive(Debug)]
pub struct CachedQuery {
    pub fingerprint: Fingerprint,
    /// Generation number assigned when query was registered (monotonically increasing)
    pub generation: Generation,
    pub relation_oids: Vec<Oid>,
    pub bound: SharedResolvedQuery,
    /// What a hit on `bound` must prove, built once with it.
    pub(crate) read_facts: SharedReadFacts,
    /// The tree population reads: the decorrelated form of `bound`, or
    /// `bound` itself when nothing was correlated.
    pub population: SharedResolvedQuery,
    /// Parameterized serve shape (PGC-294): the serve SQL of `bound` with its
    /// predicate literals replaced by `$N` placeholders, plus its key and the
    /// literals to bind. Computed once at registration; lets the serve path
    /// share one prepared statement per shape across all fingerprints that
    /// differ only in literals.
    pub serve_shape: SharedQueryShape,
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
    /// Mirrored into the view's `QueryServing::reference_only`.
    pub reference_only: bool,
}

impl BiHashItem for CachedQuery {
    type K1<'a> = Fingerprint;
    type K2<'b> = Generation;

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
    pub tables: Catalog,
    pub update_queries: IdHashMap<UpdateQueries>,
    /// The same-shape join parents, by shape over their WHERE regions
    /// (PGC-417).
    pub join_subsumers: JoinSubsumers,
    pub cached_queries: BiHashMap<CachedQuery>,
    /// Monotonically increasing generation counter (starts at 1)
    pub generation_counter: Generation,
    /// Generations of active cached queries (for efficient min-tracking)
    pub generations: BTreeSet<Generation>,
    /// Dynamic config handle for runtime-adjustable cache settings
    pub dynamic: DynamicConfigHandle,
}

impl Cache {
    pub fn new(settings: &Settings) -> Self {
        Self {
            tables: BiHashMap::new(),
            update_queries: IdHashMap::new(),
            join_subsumers: JoinSubsumers::default(),
            cached_queries: BiHashMap::new(),
            generation_counter: Generation::ZERO,
            generations: BTreeSet::new(),
            dynamic: settings.dynamic.clone(),
        }
    }

    /// Allocate the next generation and register it as active.
    pub fn generation_allocate(&mut self) -> Generation {
        self.generation_counter = self.generation_counter.next();
        self.generations.insert(self.generation_counter);
        self.generation_counter
    }

    /// Returns the minimum generation that can be safely purged.
    /// This is the highest generation that is less than all active generations
    /// or the current generation_counter if there are no active generations
    pub fn generation_purge_threshold(&self) -> Generation {
        self.generations
            .first()
            .map(|min| min.previous())
            .unwrap_or(self.generation_counter)
    }

    /// Drop `fingerprint`'s entries from `update_queries` for each given OID
    /// and from the join parents. No-op for OIDs without an `update_queries`
    /// entry. Also tears down the subsumption index entries so the lookup
    /// path doesn't return a stale candidate.
    pub fn update_queries_remove_fingerprint(&mut self, fingerprint: Fingerprint, oids: &[Oid]) {
        for oid in oids {
            if let Some(mut queries) = self.update_queries.get_mut(*oid) {
                queries.fingerprint_remove(fingerprint);
            }
        }
        self.join_subsumers.fingerprint_remove(fingerprint);
    }
}

/// Shared set of relation OIDs that have active cached queries.
/// Written by the writer thread, read by the CDC processor.
pub(super) type ActiveRelations = Arc<ArcSwap<HashSet<Oid>>>;

/// A cached relation a forwarded write can land rows in, under one name it
/// can be written by: the table itself, or one of its write aliases
/// ([`TableMetadata::write_aliases`]: a partition above or below it, or a
/// view over any of those).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteTarget {
    /// The schema of the name written.
    pub schema: EcoString,
    /// The cached relation the rows land in.
    pub relation: Oid,
    pub table: RelationName,
    /// Record the write as opaque on the relation (a view alias) rather
    /// than carrying its rows over (the table itself or a partition alias).
    pub opaque: bool,
}

impl WriteTarget {
    /// Whether a write of the target's name under `schema`, or unqualified
    /// along `search_path`, can land here. Origin resolves a qualified name in
    /// its schema and an unqualified one in the first schema of the path
    /// holding the name among those the role has `USAGE` on, neither of which
    /// the proxy sees in full, so every schema on the path is a candidate; an
    /// unknown path admits every schema.
    pub fn lands_in(&self, schema: Option<&str>, search_path: Option<&[EcoString]>) -> bool {
        match (schema, search_path) {
            (Some(schema), _) => self.schema == schema,
            (None, Some(path)) => path.contains(&self.schema),
            (None, None) => true,
        }
    }
}

/// Every name a forwarded write can land rows in a cached relation under,
/// keyed by bare name, with the relations it lands in per schema. Published
/// by the writer whenever the cached tables or their alias lists change, read
/// by the read-after-write gate when it records a write, which resolves the
/// write's target through it the way origin resolves the name (ADR-048,
/// PGC-542).
#[derive(Debug, Default)]
pub struct WriteTargetMap(HashMap<EcoString, Vec<WriteTarget>>);

impl WriteTargetMap {
    pub fn from_tables<'a>(tables: impl Iterator<Item = &'a TableMetadata>) -> Self {
        let mut map: HashMap<EcoString, Vec<WriteTarget>> = HashMap::new();
        for table in tables {
            let name = RelationName {
                schema: table.schema.clone(),
                name: table.name.clone(),
            };
            map.entry(table.name.clone())
                .or_default()
                .push(WriteTarget {
                    schema: table.schema.clone(),
                    relation: table.relation_oid,
                    table: name.clone(),
                    opaque: false,
                });
            for alias in &table.write_aliases {
                map.entry(alias.relation.name.clone())
                    .or_default()
                    .push(WriteTarget {
                        schema: alias.relation.schema.clone(),
                        relation: table.relation_oid,
                        table: name.clone(),
                        opaque: alias.is_view,
                    });
            }
        }
        Self(map)
    }

    /// Every target a write to the bare `name` can land in, in any schema;
    /// [`WriteTarget::lands_in`] narrows them to the write's resolution.
    pub fn named(&self, name: &str) -> &[WriteTarget] {
        match self.0.get(name) {
            Some(targets) => targets,
            None => &[],
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

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
    /// EWMA of population fetch+stage time (ms), excluding queue wait. Sets the
    /// re-population coalesce-forward deadline (PGC-335). `None` until the first
    /// population completes, which also marks the query as no longer cold.
    pub population_fetch_stage_ewma_ms: Option<f64>,
    pub total_bytes_served: u64,
    /// Physical rows inserted during last population (sum across all branch tables)
    pub population_row_count: u64,
    /// Cache-hit latency distribution (µs, 2 significant figures). Auto-resizing
    /// so it grows only as hits are recorded.
    pub cache_hit_latency: Histogram<u64>,
}

impl QueryMetrics {
    /// `latency_template` is the empty histogram each query's
    /// `cache_hit_latency` starts as (see [`CacheStateView::latency_template`]).
    pub fn new(registered_at_ns: Option<NonZeroU64>, latency_template: &Histogram<u64>) -> Self {
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
            population_fetch_stage_ewma_ms: None,
            total_bytes_served: 0,
            population_row_count: 0,
            cache_hit_latency: latency_template.clone(),
        }
    }
}

/// Shared cache state for dispatch lookups and writer updates.
/// Uses DashMap for per-shard locking — reads to one shard don't block
/// writes to another, eliminating the global RwLock bottleneck.
pub struct CacheStateView {
    pub cached_queries: FingerprintDashMap<CachedQueryView>,
    /// Session key → canonical fingerprint for every query with an
    /// unqualified table name the writer has resolved under some search_path.
    /// Written only by the writer (on `QueryCommand::Resolve`, and swept with
    /// the stale-entry cleanup once the fingerprint is gone from both query
    /// maps); dispatch only reads it. An alias whose target is missing is
    /// dispatched as a cold miss under that fingerprint.
    pub aliases: SessionKeyDashMap<Alias>,
    pub metrics: FingerprintDashMap<QueryMetrics>,
    pub started_at: Instant,
    /// Cache hits observed during the current GC interval. Incremented by the
    /// dispatch on each Ready-state serve; snapshot-and-zeroed by the writer
    /// on the 1s GC tick. Drives the Pending-credit decay scheme.
    pub hits_since_gc: AtomicU32,
    /// Previous GC tick's hit count, used by the dispatch to size the
    /// initial credit stamped on new Pending entries (or on Pending re-hits).
    pub last_hits_per_gc: AtomicU32,
    /// Pending entries inserted during the current GC interval: the growth
    /// of the Pending set. Bumped by the dispatch on each cold claim that
    /// lands in Pending; snapshot-and-zeroed by the writer on the GC tick,
    /// where it bounds the set when hits alone would never drain it.
    pub pending_inserts_since_gc: AtomicU32,
    /// In-process hot-result cache (PGC-236). Captured by the serve pool, served by
    /// dispatch, evicted (slot-bumped) by the writer's CDC path.
    pub memo: ResultMemo,
    /// Per-(role, object) read privileges origin has confirmed (PGC-463).
    /// Looked up by the writer on dispatch's request, read by the hit path,
    /// flushed by the writer on every schema-change signal.
    pub privileges: PrivilegeVerdicts,
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
    pub(super) reg_gate: Arc<RegGate>,
    /// Elastic population worker pool (PGC-437): worker-published demand and
    /// service-time counters, and the controller's worker-count target.
    pub population_pool: Arc<PopulationPool>,
    /// Elastic cache serve pool (ADR-053): serve-loop-published demand and
    /// service-time counters, and the controller's connection-count target.
    pub(super) serve_pool: Arc<ServePool>,
    /// Settled watermark (raw `Lsn`) published by the writer so
    /// per-connection read-after-write logs can clear their pending writes once
    /// it passes each write's commit-LSN bound (PGC-124): every origin
    /// transaction committing at or below it has either been applied to the
    /// cache or produced no decodable output. Decoded *and* applied — not the
    /// decode-stage cursor, which runs ahead of apply under writer lag. Per-generation: a fresh
    /// `CacheStateView` starts at 0, so a restart never reports a stale-high
    /// watermark to a connection reading through the current dispatch.
    pub settled_lsn: Arc<AtomicU64>,
    /// Decode-stage receive cursor (raw `Lsn`) published by the CDC processor:
    /// the highest WAL position origin has delivered to pgcache. Runs ahead of
    /// `settled_lsn` under decode/apply lag. Used by the read-after-write
    /// gate's forward attribution (PGC-440): a forward whose blocking bound is
    /// at or below it is waiting on apply/settle, above it on origin delivery.
    pub received_lsn: Arc<AtomicU64>,
    /// The write-target map for the read-after-write gate (ADR-048), published
    /// whole by the writer on every change to the cached tables or their alias
    /// lists; a connection loads it wait-free when it records a write.
    pub write_targets: ArcSwap<WriteTargetMap>,
    /// The relations with row-level security and the custom settings their
    /// policies read (PGC-113), published whole by the writer; a connection
    /// loads it to decide what a read over such a relation is keyed under.
    pub rls_relations: ArcSwap<RlsRelations>,
    /// Empty cache-hit latency histogram cloned into each query's metrics, so
    /// the one fallible construction happens at startup. Auto-resizing (starts
    /// at a few hundred bytes, grows on record) rather than fixed-range — a
    /// fixed 1µs–60s bound pre-allocates ~20 KB per query, the dominant cost
    /// under high query cardinality.
    pub latency_template: Histogram<u64>,
}

impl std::fmt::Debug for CacheStateView {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CacheStateView")
            .field("cached_queries", &self.cached_queries)
            .field("aliases_len", &self.aliases.len())
            .field("metrics_len", &self.metrics.len())
            .field("started_at", &self.started_at)
            .field("memo_entries", &self.memo.len())
            .finish()
    }
}

impl CacheStateView {
    pub fn new(dynamic: DynamicConfigHandle) -> CacheResult<Self> {
        let latency_template =
            Histogram::new(2).map_err(|_| Report::from(CacheError::LatencyHistogram))?;
        Ok(Self {
            cached_queries: DashMap::default(),
            aliases: DashMap::default(),
            metrics: DashMap::default(),
            started_at: Instant::now(),
            hits_since_gc: AtomicU32::new(0),
            last_hits_per_gc: AtomicU32::new(0),
            pending_inserts_since_gc: AtomicU32::new(0),
            registration_throttled: Arc::new(AtomicBool::new(false)),
            disk_throttle: Arc::new(AtomicBool::new(false)),
            registered_count: Arc::new(AtomicUsize::new(0)),
            query_count_cap: Arc::new(AtomicUsize::new(usize::MAX)),
            recycle_wanted: Arc::new(AtomicBool::new(false)),
            recycle_count: Arc::new(AtomicUsize::new(0)),
            reg_gate: Arc::new(RegGate::new()),
            population_pool: Arc::new(PopulationPool::new(0)),
            serve_pool: Arc::new(ServePool::default()),
            settled_lsn: Arc::new(AtomicU64::new(0)),
            received_lsn: Arc::new(AtomicU64::new(0)),
            write_targets: ArcSwap::from_pointee(WriteTargetMap::default()),
            rls_relations: ArcSwap::from_pointee(RlsRelations::default()),
            memo: ResultMemo::new(dynamic),
            privileges: PrivilegeVerdicts::default(),
            latency_template,
        })
    }

    /// Whether dispatch should forward new queries to origin instead of
    /// registering them — under memory pressure (`registration_throttled`) or
    /// disk pressure (`disk_throttle`). Wait-free; on the hot path.
    pub fn throttled(&self) -> bool {
        self.registration_throttled.load(Ordering::Relaxed)
            || self.disk_throttle.load(Ordering::Relaxed)
    }
}

/// What a hit on a cached query must prove, derived once from its bound
/// query at registration and shared by the entry, its view and every
/// serving handoff (PGC-543).
#[derive(Debug)]
pub(crate) struct ReadFacts {
    /// What a role must be allowed to read for a hit (PGC-463).
    pub(crate) privilege_needs: PrivilegeNeeds,
    /// The relations the read touches, with the columns it references and
    /// its per-reference ranges on each, for the read-after-write gate
    /// (PGC-512, PGC-542).
    pub(crate) relations: ReadRelations,
}

impl ReadFacts {
    pub(crate) fn build(bound: &ResolvedQuery) -> Self {
        let privilege_needs = bound.privilege_needs();
        let relations = ReadRelations::build(bound, &privilege_needs.relations);
        Self {
            privilege_needs,
            relations,
        }
    }
}

pub(super) type SharedReadFacts = Arc<ReadFacts>;

/// What a cached query serves: the facts its resolution produced, written
/// to the view as one value when the writer marks Ready (and Loading, for a
/// readmit or limit bump) and sent with the Ready notify, so a hit or a
/// coalesce drain reads them as a unit.
/// The query's generation travels beside it, not in it: CLOCK second chance
/// re-stamps the generation without changing what is served.
#[derive(Debug)]
pub(crate) struct QueryServing {
    pub(crate) bound: SharedResolvedQuery,
    /// What a hit on `bound` must prove.
    pub(crate) read_facts: SharedReadFacts,
    /// Parameterized serve shape (mirrors `CachedQuery.serve_shape`, PGC-294).
    pub(crate) serve_shape: SharedQueryShape,
    /// Maximum rows cached for this fingerprint (None = all rows).
    pub(crate) max_limit: Option<u64>,
    /// A date-time literal was left as written at registration, so the
    /// entry holds what the reference settings make of it; serve only to a
    /// session on the same settings (PGC-287).
    pub(crate) reference_only: bool,
}

pub(crate) type SharedQueryServing = Arc<QueryServing>;

/// A serving at the query's generation: what a writer handoff carries. The
/// generation stays out of the shared payload and travels beside it here.
#[derive(Debug, Clone)]
pub(crate) struct ServingAt {
    pub(crate) generation: Generation,
    pub(crate) serving: SharedQueryServing,
}

/// Lightweight view of a cached query for dispatch lookups.
#[derive(Debug, Clone)]
pub struct CachedQueryView {
    pub state: CachedQueryState,
    /// The query's current generation; 0 for a placeholder the dispatch
    /// inserted before the writer assigned one.
    pub generation: Generation,
    /// What the entry serves; `None` until the writer first writes the
    /// entry: Pending, and the Loading placeholder of a fresh admission
    /// until it is Ready.
    pub(crate) serving: Option<SharedQueryServing>,
    /// CLOCK reference bit — set by dispatch on cache hit, read/cleared by writer during eviction
    pub referenced: bool,
    /// MV shape classification, runtime state, and captured output column
    /// names — all written by the writer (registration / MV build).
    pub mv: MvMeta,
}

/// A pre-validated pinned query, ready for registration.
pub struct PinnedQuery {
    pub cacheable_query: Arc<CacheableQuery>,
}
