# ADR-047: MV Rebuild Discard-Backoff

## Status
Accepted

## Context
MV rebuild scheduling was memoryless: every cache hit observing `MvState::Pending` scheduled a build, unconditionally. Each build executes the query body on the cache PG. Under sustained writes to a query's tables, builds are discarded in flight (`BuildingDirty`) or go `Fresh` only to be dirtied moments later — expected payoff zero, cost paid every cycle. PGC-335 measured ~600 hot queries in this loop saturating the cache PG. ADR-031's "lazy rebuild self-tunes" rationale holds only for rarely-*read* queries; hot-read + hot-write queries thrash. Correctness was never at stake — serving falls through to source-row evaluation — but nothing bounded the wasted rebuild work.

Alternatives considered: hit-count-gated backoff (retry after 2^n fall-through hits — demand-adaptive but requires per-query MV-serve counting on the shared-lock serve path and bounds rebuilds per hit, not per second); payoff-ratio EMA (serves-per-build below threshold — most precise, most state and tuning, detects the same failure); global token bucket on build dispatch (aggregate cap that cannot distinguish thrashing from productive MVs).

## Decision
Writer-owned, per-query, time-based exponential backoff (PGC-364):

- A build counts as **wasted** when it is discarded at completion (`BuildingDirty`), fails, or its `Fresh` is dirtied before `MV_PAYOFF_WINDOW` (5 s) elapses. A `Fresh` that outlives the window resets the counter.
- Backoff engages from the **second consecutive** wasted build (one wasted build is normal — any write to a cached table causes one): `retry_after = now + min(1 s × 2^(wasted−2), 300 s)`.
- The cooldown gates only the `Pending → Scheduled` transition (dispatch serve decision and pinned bootstrap); suppressed hits serve from source rows. Anything already `Scheduled` proceeds.
- All bookkeeping lives on `MvMeta` and is written writer-side (`dirty_apply` folds the dirty transition and its payoff accounting into one call); the dispatch hot path only compares an `Instant` under its existing shared guard, and only in the `Pending` arm.

## Rationale
- **Both waste signals are required.** In the measured storm the fast-build regime dominates (builds 5–20 ms vs dirty interval 100–200 ms), so only ~5–15% of thrash cycles end as `BuildingDirty` discards; short-lived `Fresh` is the dominant waste path. Discard-only detection would engage an order of magnitude slower and can miss entirely as builds get faster.
- **Wall-time retry bounds the collapsing resource directly**: worst-case aggregate rebuild load is N-thrashers / cap (600 → 2 builds/s), independent of hit rate.
- **Safe in both error directions**: over-eager backoff costs only source-row serving (the pre-MV baseline); under-eager probing costs one wasted build per cooldown. No path to a wrong result.
- **Self-healing**: when write pressure ends, the next probe build survives the payoff window and resets the counter.
- Counting `Failed` builds as waste also stops a persistently failing build from being retried on every hit.

## Consequences

### Positive
- Thrashing MVs converge to a bounded probe rate instead of unbounded rebuild work; the PGC-335 storm shape can no longer saturate the cache PG through rebuilds.
- No hot-path cost for healthy MVs: the `Fresh` fast path is untouched; the check runs only in the already-slow `Pending` arm.
- Observability: `mv_builds_suppressed` counter, per-query `mv_wasted_builds` / `mv_backoff_remaining_ms` in `/status`.

### Negative
- A query dirtied at just above the payoff window never backs off and rebuilds once per window — acceptable per query, unbounded only in contrived fleet-wide boundary cases (a global dispatch cap remains a follow-up lever if ever observed).
- A `Fresh` with heavy hit traffic that is dirtied just inside the window is misclassified as waste (lifetime is a proxy for serves); the cost is temporary source-row serving.
- Backoff state resets on eviction + re-registration, so a thrashing query that churns through eviction re-learns from zero.

## Implementation Notes
Constants (`MV_PAYOFF_WINDOW`, `MV_BACKOFF_BASE`, `MV_BACKOFF_CAP`) are code constants in `src/cache/mv.rs`, promotable to settings if tuning proves necessary. `tests/mv_build_race_test.rs` pins the suppress-then-release behavior end-to-end via the fault-injection build hold.
