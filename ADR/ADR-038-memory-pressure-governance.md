# ADR-038: Memory-pressure governance

## Status
Accepted

## Context
pgcache's memory footprint grows with the number of registered queries: each holds source-row cache tables, optionally a materialized-view table (ADR-031), and in-process state, while the cache Postgres it co-locates with grows its plan cache as more query shapes are served. Left unbounded, that growth eventually OOM-kills the process. The original bound was a single flat size cap enforced by summing cache-table bytes (`pgcache_total_size()`) and evicting above it — the same cap also stood in for disk (now governed separately, ADR-043).

A flat byte cap is the wrong control variable for memory on two counts. Memory is *sticky* — eviction does not return private RSS to the OS — and the failure mode is an OOM kill, so the bound must track live used memory rather than a static figure. And summing cache-table bytes misattributes the budget entirely: it counts only on-disk tables and misses the dominant memory consumers — the cache-PG plan cache and pgcache's own in-process state. Operators also reason in "how many queries does this box hold," which depends on the running workload's per-query cost, not on a guessed byte number.

The forcing constraints: the bound must hold against the *whole* co-located footprint (pgcache plus the cache Postgres), not just pgcache's RSS; it must self-tune to the live workload; and it must never serve stale data — shedding load means forwarding to origin, never returning a wrong answer. Detection is Linux-first (the production/Docker target) and must degrade to "no bound" where it can't introspect (non-Linux dev/test). Disk has the opposite economics — cheap and plentiful but catastrophic to fill — and is governed by an independent controller in ADR-043.

## Decision
Replace the memory portion of the flat size cap with a query-count cap derived from *measured marginal cost*, governed by live used memory rather than a static byte budget (PGC-251).

- **A monitor task derives a query-count cap from measured marginal cost.** A `memory_monitor` task samples whole-system used memory (cgroup `memory.current` when limited, else host `MemTotal − MemAvailable`) against a ceiling of 80% of detected RAM, lowered (never raised) by an optional `memory_limit`. Crossing the ceiling sets `registration_throttled`; a 2%-of-ceiling hysteresis band on the relieve side stops the flag flapping. Separately, the monitor measures the **private** (non-shared) per-query memory cost as an incremental derivative `Δprivate / Δcount`, sampled only while the count is *growing* past its high-water mark, smoothed into an EWMA, and tracked as a decaying peak. From that it publishes `query_count_cap = count + (budget − private) / peak`, anchored on the live private reading.

- **One eviction loop, driven by the count cap.** The writer's eviction loop (`eviction.rs`) evicts down to `query_count_cap`, always from the lowest active generation upward (ADR-044); pinned queries are never evicted. `cache_policy` decides whether a referenced query earns a second chance: under CLOCK it does — implemented by reassigning it a new (higher) generation, re-admitting it so it is no longer the eviction candidate — and under FIFO it does not. Either way the eviction order stays consistent with the generation reclamation watermark.

- **Connection recycling to reclaim sticky memory.** Because eviction alone doesn't return private RSS to the OS, under memory pressure the monitor sets `recycle_wanted` so the serve pool rolls connections one at a time; a full pool cycle resets the marginal-cost peak so the cap can re-learn a lighter workload.

- **A memory throttle flag, OR'd at dispatch with the disk throttle.** `registration_throttled` (memory) and `disk_throttle` (disk, ADR-043) are distinct atomics; dispatch forwards a new query to origin if *either* is set. Already-registered queries continue to serve from cache.

- **Best-effort, fail-open.** Where memory can't be detected the count cap is `usize::MAX` (unbounded) and the memory throttle stays clear. The old `cache_size` config is deprecated and ignored.

- **Signals consumed elsewhere.** The memory budget and pressure this controller publishes also feed the RAM-relative memo byte budget (ADR-036) and the adaptive registration admission gate (ADR-041); those mechanisms are documented in their own ADRs and are out of scope here.

## Rationale
- **Live pressure over a flat cap, because memory is sticky and the risk is OOM.** A static byte number can't track the live footprint of a co-located process pair whose plan caches grow with the workload; a soft count cap that throttles growth and evicts gradually matches memory's failure mode.

- **Count cap over a byte budget, because operators reason in queries, not bytes.** The system measures the workload's actual per-query cost and converts the memory budget into a query count, so the bound self-tunes instead of requiring a guessed constant.

- **The incremental derivative `Δprivate/Δcount`, not the ratio `private/count`.** Private RSS is sticky — eviction doesn't return it — so dividing sticky private by a shrinking post-eviction count inflates the estimate and collapses the cap toward its floor in a closed-loop death spiral. The derivative is immune: when the count is held at the cap there is simply no sample, and it excludes one-time fixed costs that don't recur per query. Sampling only on new-high-water growth keeps eviction churn from producing spurious samples.

- **Whole-system used memory, not pgcache RSS, because the cache Postgres shares the box.** The binding figure is the combined footprint; budgeting against pgcache's RSS alone would miss the cache-PG plan cache, the dominant grower. Targeting *private* memory for the per-query cost excludes `shared_buffers` (shared memory, reserved separately against the ceiling), isolating the actually-growing pool.

- **An 80% ceiling reserves headroom for the OS and spike absorption.** The remaining 20% is deliberately left for OS functionality, page cache, and transient memory spikes rather than handed to the cache — the same headroom principle the RAM-relative memo budget (ADR-036) follows.

- **The 2%-of-ceiling hysteresis band is a provisional default.** A band is needed so the throttle flag does not flap as usage hovers at the ceiling; the specific 2% width is a reasonable-looking default and has not been stress-tested for the right damping.

- **Estimator constants, mixed provenance.** The smoothing alpha, peak-decay half-life, and per-tick cap-increase fraction were settled empirically — they behaved well in the stress-test situations they were run against, though not claimed optimal. The two floors are structural rather than tuned: `MIN_GROWTH` requires a large enough count increase to give the `Δprivate/Δcount` derivative a reasonable sample size before it is trusted, and `MIN_QUERIES` keeps the cap math from collapsing when the registered count falls too far.

- **EWMA + decaying peak, anchored on the live reading.** The per-query cost rises as a plan propagates across the serve pool, so the peak captures the fully-propagated worst case while the EWMA stops a single noisy chunk from spiking it. Anchoring the cap on the live private reading (rather than a fixed cold-start baseline) makes it self-correct: as private approaches the budget the cap converges to the current count, and past the budget it drops below it, forcing eviction until recycling reclaims memory.

## Consequences

### Positive
- The memory footprint is bounded by real, live pressure — including the co-located cache Postgres — not by a static guess that ages out as the workload changes.
- The bound self-tunes to the running workload's measured per-query cost and recovers automatically when a lighter workload (via connection recycling) reclaims memory.
- No stale reads under pressure: throttling forwards to origin and eviction removes entries; neither returns superseded data.

### Negative
- Memory governance is Linux-first; on non-Linux dev/test the count cap is unbounded and memory throttling is disabled, so the bound is exercised only in production-like environments.
- The derivative estimator needs a growth window (a minimum count increase) before it has any signal, so the cap is uncapped during early warm-up and re-learns slowly after a workload-cost change (mitigated by a re-measurement probe and increase rate-limiting).
- Connection recycling adds churn to the serve pool under sustained memory pressure.
- The peak's per-tick decay is a slow backstop; the primary memory-recovery path is recycling, so a workload that never recycles relies on decay to lower the cap.

## Implementation Notes
- Memory monitor: `src/cache/runtime/memory_monitor.rs` (the 500 ms sampling task and shared-buffers query); pure decision logic and system introspection in `src/memory.rs` (`throttle_evaluate`, `count_cap_evaluate`, `cap_rate_limit`, and the cgroup/`/proc` readers, all `None` off-Linux). Tunables in `src/memory.rs`: `COUNT_CAP_EWMA_ALPHA`, `COUNT_CAP_PEAK_DECAY`, `COUNT_CAP_MIN_QUERIES`, `COUNT_CAP_MIN_GROWTH`, `CAP_MAX_INCREASE_FRACTION`.
- Eviction loop: `eviction_run` in `src/cache/writer/eviction.rs` (CLOCK/FIFO per `cache_policy`, bounded second-chance bumps, pinned-skip), driven to `query_count_cap`.
- Shared signals on `CacheStateView` (`src/cache/types.rs`): `registration_throttled` (dispatch OR's it with `disk_throttle` via `throttled()`), `query_count_cap`, `registered_count`, `recycle_wanted`, `recycle_count`.
- Config (`src/settings/dynamic.rs`): `memory_limit` is dynamic (`DynamicConfig`), optional, and able only to *lower* the auto-derived ceiling; `cache_size` is parsed-but-ignored with a deprecation warning.
- Gauges in `src/metrics.rs`: memory used/budget/RSS, `registration_throttled`(+total), marginal bytes-per-query, and `query_count_cap` (reported 0 when uncapped).
