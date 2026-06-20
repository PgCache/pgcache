# ADR-043: Disk-pressure governance

## Status
Accepted

## Context
pgcache writes its cache to a Postgres it co-locates with: source-row cache tables, materialized-view tables (ADR-031), and population staging tables all live on the cache-PG data volume. As registered queries accumulate, that volume fills. Unlike memory (sticky, OOM the failure mode — governed separately in ADR-038), disk is cheap and plentiful but *catastrophic* to fill: a full volume stalls the cache Postgres and can wedge the whole proxy. The original bound was a single flat size cap enforced by summing cache-table bytes (`pgcache_total_size()`).

Summing tracked cache tables is the wrong measurement for disk on two counts. It costs a query per check, and it accounts only for pgcache's own tables — missing WAL, temp files, and anything else sharing the volume — so the number it bounds is not the number that actually fills the disk. And because filling the volume is an emergency rather than a gradual cost, the response needs to be more aggressive than the soft, gradual eviction appropriate for memory.

The forcing constraints mirror ADR-038's: never serve stale data (shedding load forwards to origin), self-tune rather than require a guessed byte constant, and degrade safely where the volume can't be introspected.

## Decision
Replace the disk portion of the flat size cap with a `statvfs`-based pressure controller and an escalating reclaim ladder (PGC-276).

- **statvfs pressure against an auto-derived limit.** The writer caches a `statvfs` reading of the cache-PG data directory each tick and compares used bytes against an effective `disk_limit` (explicit config, else auto-derived to keep a reserve free — 5% of total, clamped to `[1 GiB, 10 GiB]`).

- **An escalating reclaim ladder, one rung per tick.** Disk pressure is treated as an emergency: crossing the limit sets a separate `disk_throttle` flag and then takes one escalating reclaim step per tick — rung 1 purges dead-generation rows, rung 2 sweeps Dirty MV tables, rung 3+ drops the source table referenced by the fewest queries (least collateral). After a drop the controller backs off one tick so asynchronously-freed space lands in `statvfs` before the next decision.

- **A disk throttle flag, OR'd at dispatch with the memory throttle.** `disk_throttle` (disk) and `registration_throttled` (memory, ADR-038) are distinct atomics; dispatch forwards a new query to origin if *either* is set. Already-registered queries continue to serve from cache.

- **Best-effort, fail-open.** Where the data directory can't be stat'd, disk reclaim is disabled. The old `cache_size` config is deprecated and ignored.

## Rationale
- **Escalating ladder over a tight reclaim loop, because `DROP TABLE` frees space asynchronously.** `statvfs` won't reflect a drop until a later read, so a tight loop would over-drop. Pacing one rung per tick (cheap reclaim first, table drops last, with a post-drop backoff) reclaims gradually and re-evaluates against fresh readings.

- **Cheap reclaim before destructive reclaim.** Purging dead rows and sweeping Dirty MVs reclaims space with no loss of live cache; dropping a source table is the last resort because it invalidates every query over that table. Dropping the *fewest-queries* table minimizes that collateral.

- **The reserve is a fixed safety minimum, not really a percentage.** Its only job is to stop the cache filling the volume and crashing the machine, so what matters is leaving a fixed amount of free space rather than a fraction of disk. The 5% is clamped hard at both ends and the clamps do the real work: the 10 GiB ceiling is grounded in an observed case — a ~1 TiB dev disk reserved 50 GiB at 5% and left the cache permanently empty — while the 1 GiB floor is just an ungrounded sane minimum. The reserve is a backstop against accidental disk exhaustion, not a substitute for operators watching the volume.

- **statvfs over `pgcache_total_size()`.** Reading live filesystem usage is one syscall and accounts for everything on the volume, where summing tracked cache tables both costs a query and misses non-table space (WAL, temp, staging).

- **Emergency semantics, distinct from memory.** Disk is plentiful until it isn't; the controller does nothing until near the limit, then acts decisively. This is the opposite shape from ADR-038's continuous soft count cap, which is why the two resources get separate controllers rather than one shared cap.

## Consequences

### Positive
- The cache volume is bounded by live filesystem usage including non-table space, not by a static guess that misses WAL/temp/staging.
- Reclaim is graduated: it costs no live cache until the cheap rungs are exhausted, and only drops tables as a last resort.
- Memory and disk shed load independently; disk pressure throttles new registration without affecting memory governance or stopping already-cached serves.
- No stale reads under pressure: throttling forwards to origin and reclaim removes entries; neither returns superseded data.

### Negative
- Disk reclaim is coarse at its top rung — dropping a whole source table invalidates all of its queries — accepted because filling the volume is the worse outcome.
- The per-tick, one-rung pacing means reclaim under a fast fill lags the fill rate by several ticks (bounded by the reserve headroom).
- Like ADR-038, introspection is platform-dependent; where `statvfs` is unavailable the bound is absent.

## Implementation Notes
- Disk pressure + escalating reclaim: `disk_stats_refresh`, `disk_pressure`, `disk_pressure_handle`, `disk_reclaim_drop_smallest` in `src/cache/writer/eviction.rs`, run on the writer's 1 s tick. The disk-reserve fraction/floor/cap and `disk_*` readers live in `src/memory.rs`.
- Shared signal on `CacheStateView` (`src/cache/types.rs`): `disk_throttle` (dispatch OR's it with `registration_throttled` via `throttled()`).
- Config (`src/settings/dynamic.rs`): `disk_limit` is dynamic (`DynamicConfig`), optional, and able only to *lower* the auto-derived ceiling; `cache_size` is parsed-but-ignored with a deprecation warning.
