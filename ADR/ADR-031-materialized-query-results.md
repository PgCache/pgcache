# ADR-031: Materialized Query Results

## Status
Accepted

## Context
pgcache caches source rows and re-evaluates queries against those cached rows on every serve. For queries that reduce or transform a large input into a small output — aggregates, window functions, GROUP BY, DISTINCT — the source-row cache stores far more data than the result needs and pays the re-evaluation cost on every hit (`SELECT count(*) FROM large_table` caches every row to return one integer per serve).

We want a second cache tier — a materialized result — for queries that benefit from storing the answer rather than recomputing it, while never serving stale data and never regressing latency for queries that don't benefit. The materialized result must stay consistent with the existing CDC-driven invalidation model, so PostgreSQL's `MATERIALIZED VIEW` / `REFRESH` (an origin-side operation that doesn't compose with our CDC pipeline) is not usable.

## Decision
Maintain a derived materialized-view (MV) tier alongside the source-row cache, owned end-to-end by pgcache's writer:

- **Storage**: pgcache-managed UNLOGGED tables in a dedicated `pgcache_mv` schema, named `q_<fingerprint>`. No PK/indexes/generation triggers (written once per build, read whole). Not `pgcache_pgrx`-tracked — consistency comes from MV state transitions, not per-row generation stamps.
- **Coexistence, not replacement**: an eligible query has *both* a source-row cache (the always-true invariant layer) and an MV (a derived projection). When the MV isn't fresh, serving falls through to source-row evaluation — no regression versus pre-MV behavior.
- **Eligibility via a shape gate** set once at registration against the decorrelated/resolved form: `Materialize` (window functions — justified on compute grounds regardless of size), `Measure` (aggregates/GROUP BY/HAVING/DISTINCT/reducing set-ops — apply a size gate `result_rows × mv_size_ratio ≤ source_rows` at first build), or `Skip` (plain filter/projection, `UNION ALL`, bare ORDER BY+LIMIT — MV would duplicate what the source-row cache already holds). Classification is **sticky**: it never re-evaluates on invalidation, only on eviction + re-registration.
- **Lazy, coordinator-driven builds**: builds are triggered by a cache hit that observes a non-fresh MV state, never on the request-critical path — the triggering hit falls through to source-row eval while the writer builds behind it. One uniform flow across first-build and rebuild; pinned queries self-trigger to stay warm.
- **Invalidation is an in-memory state flip** (`Fresh → Pending`), fired by both CDC invalidation paths (in-place-maintained and full-invalidation) and by LimitBump. No TRUNCATE or DROP at invalidation time — destructive work is deferred into the rebuild transaction.
- **Serve path** reuses the existing worker pool: fresh MVs are served as `SELECT * FROM mv [ORDER BY <positional>] [LIMIT user_limit]` with no generation SET; everything else dispatches as today.

## Rationale
- **Result tables over PG materialized views**: integrates with the existing population + CDC invalidation infrastructure and gives full control over population, stamping, and drop/truncate semantics; `REFRESH` does not compose with CDC.
- **Lazy rebuild self-tunes**: write-heavy/rarely-read queries simply stay dirty and cost only dead bytes on disk; read-heavy/stable queries stay warm. No explicit activation heuristic needed.
- **Deferring TRUNCATE into the rebuild transaction is a correctness requirement, not an optimization**: truncating at invalidation time opens a window where a worker dispatched as `mv_source = true` races the TRUNCATE and returns an empty result. Folding `BEGIN; TRUNCATE; INSERT; COMMIT` makes every serve MVCC-atomic — a SELECT sees either the pre-rebuild or post-rebuild rows, never an empty intermediate.
- **Sticky classification matches operator expectations**: tuning `mv_size_ratio` shouldn't churn existing storage; the gate is re-evaluated only on re-registration.
- **`has_table` bit on the Pending/Scheduled states** distinguishes first-build (`CREATE TABLE AS`) from rebuild (`TRUNCATE + INSERT`) without a separate state, and lets the `Pending → Scheduled` transition deduplicate concurrent build dispatches under the DashMap write guard.

## Consequences

### Positive
- High cache-hit value for reducing queries — aggregates/window functions serve from a small table instead of re-scanning the full source-row cache.
- No latency regression: the first hit after registration/invalidation sees exactly what it would have without the feature; the fast path only kicks in once a build completes.
- Cheap, lock-free invalidation (an in-memory flip), so CDC hooks can fire freely with no cache-DB round-trips.
- MV bytes count toward the cache size limit (via `pgcache_total_size()`), so they participate in eviction; a stale-MV pre-sweep reclaims dead bytes before evicting live entries.

### Negative
- v1 runs builds **synchronously in the writer task**, so a long MV build blocks CDC processing and grows replication-slot lag for its duration (mitigated by the `Measure` size gate and a build-duration histogram; off-thread builds with race detection are the planned follow-up).
- Storage amplification: an eligible query holds both a source-row cache and an MV; stale MVs occupy disk until rebuild or eviction.
- MV correctness is bounded by the source-row layer it derives from: the MV is only served when `Fresh`, and any source-row invalidation dirties it, so it can never be more stale than the source-row cache — but it also can't improve on the worst case (the first hit after invalidation always falls through to source-row eval).

## Implementation Notes
State lives on `CachedQueryView` as `ShapeGate` + `MvState` (`Skipped | Ineligible | Pending{has_table} | Scheduled{has_table} | Fresh`) in `src/cache/mv.rs`; MV table names are derived from the fingerprint, not stored. `mv_size_ratio` is a dynamic (`DynamicConfig`) setting. CDC wiring hooks both invalidation paths in `src/cache/writer/cdc.rs`; build/drop logic is in `src/cache/writer/mv.rs`. Out of scope for v1: off-thread builds, incremental view maintenance, MV-level generation filtering, and ORDER-BY-alias resolution.
