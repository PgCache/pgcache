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
- v1 runs builds **synchronously in the writer task**, so a long MV build blocks CDC processing and grows replication-slot lag for its duration (mitigated by the `Measure` size gate and a build-duration histogram; off-thread builds with race detection are the planned follow-up). *(Superseded — see Amendment: Off-thread MV builds. Builds now run off the writer thread, removing this blocking/slot-lag consequence.)*
- Storage amplification: an eligible query holds both a source-row cache and an MV; stale MVs occupy disk until rebuild or eviction.
- MV correctness is bounded by the source-row layer it derives from: the MV is only served when `Fresh`, and any source-row invalidation dirties it, so it can never be more stale than the source-row cache — but it also can't improve on the worst case (the first hit after invalidation always falls through to source-row eval).

## Implementation Notes
State lives on `CachedQueryView` as `ShapeGate` + `MvState` (`Skipped | Ineligible | Pending{has_table} | Scheduled{has_table} | Fresh`) in `src/cache/mv.rs`; MV table names are derived from the fingerprint, not stored. `mv_size_ratio` is a dynamic (`DynamicConfig`) setting. CDC wiring hooks both invalidation paths in `src/cache/writer/cdc.rs`; build/drop logic is in `src/cache/writer/mv.rs`. Out of scope for v1: off-thread builds, incremental view maintenance, MV-level generation filtering, and ORDER-BY-alias resolution.

## Amendment: Off-thread MV builds

### Status
Accepted

### Context
v1 ran MV build SQL — the `Measure` size gate, the output-column describe, and the build batch (`CREATE UNLOGGED TABLE AS` or `BEGIN; TRUNCATE; INSERT; COMMIT;`) — inline on the single-threaded writer task. Because that same task drains the CDC apply loop, a build of non-trivial duration stalled CDC processing for its entire run, and the resulting apply pause grew replication-slot lag on the origin. The `Measure` size gate bounded how large an MV's result could be, but the gate's denominator and the build itself still scan predicate-scoped source rows, so build cost is not bounded to a constant — the writer-blocking exposure scaled with input size. This was the off-thread-builds follow-up that v1 explicitly deferred.

### Decision
- **Build SQL runs on the shared multi-thread runtime, not the writer.** The writer's `MvBuild` handler snapshots everything the build needs from the (writer-only) catalog and the `state_view` entry, flips the entry to `Building`, and spawns the build task onto the shared runtime handle. The writer's event loop never blocks on build SQL; CDC apply continues during a build.
- **The writer still owns every `MvState` transition.** The spawned task performs no state transitions — it executes SQL only and reports its result back through the writer's internal command channel (`MvBuildComplete`). The terminal `Fresh` flip is therefore applied on the writer, serialized against CDC dirty-marking on the same task: a build raced by a relevant CDC change is observed at completion as `BuildingDirty` and its result discarded (the table is reset to `Pending`, leaving its on-disk rows for the next rebuild). The data a build reads is snapshot-consistent regardless; the serialization only governs whether the freshly built table is allowed to claim it is current.
- **A small dedicated connection pool, separate from the serve pool.** Build tasks check out cache-DB connections from a pool sized to `MV_BUILD_CONNECTIONS` (2), which doubles as the build-concurrency limit. It reuses the codebase's bounded-mpsc pool pattern, adapted so independent build tasks (no single dispatcher) share the receiver behind a `Mutex`, and so slots open their connections lazily on the shared runtime rather than eagerly on the writer's runtime.
- **At most one build per fingerprint is ever in flight.** All builds for a fingerprint write the same MV table, so a second concurrent task would race the first at the SQL level. The writer tracks in-flight fingerprints; a dispatch that finds one in flight leaves the entry `Scheduled` and the completion handler re-dispatches it.
- **Completion always reaches the writer.** A guard on the build task sends `MvBuildComplete` even on panic or cancellation, and the connection slot is returned on every exit path — a lost completion would wedge the fingerprint's MV behind the in-flight guard permanently.

### Rationale
- **Reporting completion back to the writer rather than letting the task flip state** is what keeps the v1 consistency model intact off-thread: with all transitions serialized on the writer, the `Fresh` flip and CDC dirty-marking can never interleave, so the `BuildingDirty` discard remains a simple in-memory check rather than a lock against concurrent CDC. The state flag is the *entire* race-detection mechanism — no generation or LSN comparison is needed, and it effectively subsumes one: any CDC event touching the build's data flips the entry to `BuildingDirty`, so a build that completes still in `Building` is itself proof that no relevant change committed during its window. Running completion on the writer is also structurally necessary, not only a serialization choice: the handler mutates writer-exclusive state — the in-flight fingerprint set, the writer's own cache connection (used to drop stale/orphan MV tables), and build re-dispatch — and reads the writer-only catalog. The serialization against CDC dirty-marking falls out of that single-threaded ownership rather than being a separate requirement.
- **A pool separate from the serve pool** keeps a backlog of builds from consuming serve capacity, and a build holds an exclusive lock on its MV table for a multi-statement transaction, so build connections have a different lifetime profile than serve connections. The pool size of 2 is a deliberately conservative cap to limit concurrent build load on the cache DB — a starting point pending evidence, not a measured optimum.
- **Off-thread builds remove the writer-blocking / slot-lag negative consequence v1 listed** without weakening consistency, because no state authority moved off the writer.

### Consequences

#### Positive
- A long MV build no longer stalls CDC apply or grows replication-slot lag; the writer's event loop stays responsive during builds.
- Build cost is decoupled from CDC throughput, so the `Measure` size gate is no longer load-bearing as a writer-protection mechanism (it remains a storage/eligibility decision).
- Concurrency is bounded and isolated from serving by the dedicated pool.

#### Negative
- Added moving parts: an in-flight fingerprint set, a build-completion command path, a panic/cancellation completion guard, and a second connection pool with lazy slot reconnection.
- Build concurrency is capped by the pool size, so a burst of distinct eligible queries queues for build connections (surfaced via a build-queue gauge).
- The off-thread window between dispatch and completion is exactly the window in which a CDC change can dirty an in-flight build and force a discard + rebuild — more likely under heavy write load than a synchronous build would have been.

### Implementation Notes
Build execution, the dedicated pool (`MvBuildPool` / `SlotGuard`), the completion guard, and the build/gate/describe SQL live in `src/cache/writer/mv_build.rs`; build dispatch (`MvBuild`), completion handling (`MvBuildComplete`), the in-flight fingerprint set, and all `MvState` transitions stay in `src/cache/writer/mv.rs`. The `MvState` machine gains `Building { has_table }` and `BuildingDirty { has_table }`; completion outcomes are carried by `MvBuildOutcome` (`Built | Ineligible | Failed { has_table }`). A fault-injection hold (`PGCACHE_FAULT_MV_BUILD_HOLD_MS`) widens the in-flight window so tests can deterministically land a CDC change mid-build and assert the `BuildingDirty` discard.
