# Architecture Decision Records

This directory records the significant architectural decisions behind pgcache. Each ADR captures the **context, decision, and consequences** of one choice — the *why*, where the code only shows the *how*. Start here to find which ADR covers a subsystem, then read that ADR before changing it.

Conventions: files are `ADR-XXX-brief-title.md` with the next sequential number; status is `Proposed | Accepted | Deprecated | Superseded`. New ADRs follow the `adr` skill template (Status / Context / Decision / Rationale / Consequences). All ADRs below are **Accepted** unless noted. (ADR-007 is unassigned.)

## Runtime & threading
- [ADR-001](ADR-001-multi-threaded-architecture.md) — Multi-threaded architecture with scoped threading *(runtime topology largely superseded by ADR-033)*
- [ADR-033](ADR-033-unified-serving-runtime.md) — Unified serving runtime: collapse multi-executor design onto one shared tokio runtime
- [ADR-034](ADR-034-cache-restart-supervisor.md) — Cache restart supervisor: generation-based auto-restart with backoff *(depends on ADR-033)*
- [ADR-004](ADR-004-tokio-select-connection-multiplexing.md) — `tokio::select!` for client/origin/cache I/O multiplexing
- [ADR-028](ADR-028-dynamic-runtime-configuration.md) — Dynamic vs static settings; lock-free `ArcSwap` readers, HTTP-mutable subset

## Proxy & PostgreSQL protocol
- [ADR-005](ADR-005-extended-query-protocol-support.md) — Extended query protocol (Parse/Bind/Execute) with parse-time cacheability
- [ADR-006](ADR-006-search-path-support.md) — Per-connection `search_path` tracking for schema resolution
- [ADR-008](ADR-008-origin-tls-support.md) — Origin TLS (rustls + SSLRequest negotiation)
- [ADR-009](ADR-009-client-tls-support.md) — Client TLS (shared `ServerConnection` across threads)
- [ADR-025](ADR-025-cache-connection-hot-path.md) — CacheConnection hot path: zero-copy frame forwarding, no `tokio_postgres`
- [ADR-042](ADR-042-hot-path-allocation-elimination.md) — Hot-path allocation elimination across serve encoding + CDC apply *(extends ADR-025)*

## Query parsing, analysis & transformation
- [ADR-002](ADR-002-resolved-ast.md) — Resolved AST: fully schema-qualified query form for analysis
- [ADR-003](ADR-003-constant-propagation.md) — Constant propagation through column equivalences for constraint analysis
- [ADR-011](ADR-011-set-operation-support.md) — Set operations (UNION/INTERSECT/EXCEPT): per-branch populate, whole-query invalidate
- [ADR-012](ADR-012-predicate-pushdown.md) — Predicate pushdown into FROM subqueries
- [ADR-013](ADR-013-ecostring-identifier-types.md) — EcoString for resolved-AST identifiers *(amended by ADR-032)*
- [ADR-032](ADR-032-ecostring-by-default.md) — EcoString as the default for owned strings *(amends ADR-013)*
- [ADR-014](ADR-014-exists-not-exists-decorrelation.md) — EXISTS/NOT EXISTS decorrelation (semi/anti-join)
- [ADR-015](ADR-015-scalar-subquery-decorrelation.md) — Scalar correlated subquery decorrelation (LEFT JOIN + GROUP BY)
- [ADR-016](ADR-016-in-any-decorrelation.md) — IN/ANY decorrelation (reuses EXISTS infrastructure)
- [ADR-017](ADR-017-not-in-all-decorrelation.md) — NOT IN/ALL decorrelation (reuses NOT EXISTS infrastructure)
- [ADR-024](ADR-024-predicate-subsumption.md) — Predicate subsumption: skip population when a cached query contains a new one
- [ADR-029](ADR-029-subsumption-complex-bucket-index.md) — Subsumption complex-bucket per-column index
- [ADR-030](ADR-030-subsumption-two-sided-range-index.md) — Subsumption two-sided (BETWEEN-style) range index
- [ADR-037](ADR-037-constraint-containment-index.md) — Generalized constraint-containment index *(generalizes ADR-024/029/030; shared with the CDC matcher)*

## Caching tiers & serving
- [ADR-010](ADR-010-population-worker-pool.md) — Persistent population worker pool
- [ADR-020](ADR-020-table-allowlist.md) — Optional table allowlist restricting what is cached
- [ADR-023](ADR-023-pinned-queries.md) — Pinned queries: pre-populate and protect from eviction
- [ADR-026](ADR-026-request-coalescing.md) — Request coalescing for Loading-state queries
- [ADR-031](ADR-031-materialized-query-results.md) — Materialized query results (MV tier) *(amended: off-thread builds)*
- [ADR-036](ADR-036-response-memoization.md) — In-process response memoization (third tier; serves client-ready wire bytes)
- [ADR-040](ADR-040-shape-keyed-serving.md) — Shape-keyed prepared-statement serving (collapse per-literal plan churn)

## CDC & cache consistency
- [ADR-018](ADR-018-dynamic-publication-management.md) — Dynamic publication management (filter CDC at source via ALTER PUBLICATION)
- [ADR-027](ADR-027-cdc-local-eval-fast-path.md) — CDC local-eval fast path (LocalEval in Rust vs PgEval)
- [ADR-035](ADR-035-population-cdc-merge-consistency.md) — Population/CDC merge consistency (staging, merge watermark gate, deleted-key set)
- [ADR-039](ADR-039-cdc-apply-batching.md) — CDC apply batching + prepared membership-eval
- [ADR-045](ADR-045-candidate-narrowed-cdc-bookkeeping.md) — Candidate-narrowed CDC invalidation + memo eviction *(extends ADR-037's shared probe to the invalidation/memo passes)*

## Resource governance
- [ADR-038](ADR-038-memory-pressure-governance.md) — Memory-pressure governance: count cap from measured marginal cost
- [ADR-043](ADR-043-disk-pressure-governance.md) — Disk-pressure governance: statvfs + escalating reclaim ladder
- [ADR-041](ADR-041-registration-admission-control.md) — Adaptive registration admission control (BBR-lite backpressure)
- [ADR-044](ADR-044-generation-tracking-garbage-collection.md) — Generation tracking as garbage collection (not visibility); eviction by lowest active generation

## Observability & operations
- [ADR-021](ADR-021-observability-admin-api.md) — Observability admin API (health/readiness/metrics/status HTTP endpoints)
- [ADR-022](ADR-022-per-query-metrics.md) — Per-query metrics (hit/miss/invalidation/latency)
- [ADR-046](ADR-046-cache-side-explain-pseudo-function.md) — Cache-side EXPLAIN via an intercepted `pgcache_explain(...)` pseudo-function (no stamping, no new unsafe)
- [ADR-019](ADR-019-anonymous-telemetry.md) — Anonymous telemetry *(Proposed)*

## Cross-cutting notes
- **Runtime evolution:** ADR-001 → ADR-033 (single shared runtime) → ADR-034 (supervised restart).
- **Subsumption / constraint index:** ADR-024 → ADR-029/030 → ADR-037 generalizes the index and shares it with the CDC matcher (ADR-039) and memo eviction (ADR-036).
- **Owned strings:** ADR-013 → ADR-032 (EcoString by default).
- **Resource governance** (ADR-038 memory, ADR-043 disk, ADR-041 admission) was split from a single bundled decision; the three controllers are independent but cooperate through shared dispatch throttles.
- **Reclamation:** ADR-044 (generation = GC, eviction by lowest active generation) underpins the row-level reclamation that ADR-038's count-cap eviction drives.
