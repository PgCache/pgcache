# ADR-053: Elastic cache serve pool

## Status

Accepted

## Context

The cache-PG serve pool was fixed at `num_workers × 2` with a floor of 4 — a
guess at right-sizing. The PGC-440 gap decomposition showed the fixed size
binding under write churn: at dba write_rate 0.2, write-driven memo eviction
pushes ~26% of hits onto the pool, which saturates and queues serves for
~48ms — the dominant term of page latency. A probe raising the size to 12 on
the same box lifted serve throughput 39% before converging at the box's CPU
capacity point (per-exec time inflating as queue time fell). The right size
is therefore real, box- and load-dependent, and discoverable — the same
conclusion that produced ADR-052 for the population pool.

## Decision

1. **Reuse the ADR-052 probe-and-verify controller**, extracted behind a
   `PoolControllerConfig` (`runtime/pool_controller.rs`) shared by both
   pools. The serve pool's config differs mainly in the wait target (25ms —
   serves are millisecond-scale; initial value, to be revisited against
   bench evidence).
2. **Bounds are hardcoded**: min `num_workers × 2` (the old floor of 4 was a
   sizing guess; sizing is now dynamic), max `num_workers × 8` — the
   cache-PG protection bound. The memory monitor receives the max as its
   PGC-251 full-pool-recycled threshold (its only use of pool size — a
   re-measurement trigger, not an RSS reservation); the sole consequence is
   a slower count-cap re-probe under sustained memory pressure, and max is
   the conservative-correct value for that trigger's semantics.
3. **The replenish task becomes a reconciler** maintaining `live == desired`:
   each existing loss signal (poison discard PGC-238, mid-flight loss
   PGC-278, memory-pressure recycle PGC-251) decrements `live` and the same
   connect-with-backoff loop restores it; controller growth is picked up on
   a short tick. Shrink retires only idle connections (never interrupts an
   in-flight serve), applied lazily in the serve loop. The pool channel is
   allocated at the elastic maximum so reconciliation never blocks on
   capacity.

## Rationale

- The wr0.2 evidence shows both failure directions of a fixed size: 4 leaves
  measurable throughput behind, while any larger fixed choice would hold
  idle cache-PG backends (each with plan-cache RSS) on quiet workloads.
  Probe-and-verify finds the operating point and tracks it as load shifts.
- The controller's refute branch is precisely the CPU-capacity detector the
  pool probe hit by hand: when another connection stops raising serve
  throughput, growth stops below the max.
- Serve completions run in the thousands per second, so the verify signal is
  essentially never starved — the backstop branch is expected to be dead
  code here (its counter firing would itself be a finding).

## Consequences

### Positive

- Under memo-eviction churn the pool grows to the box's measured capacity
  instead of queueing serves behind 4 connections; on quiet workloads it
  returns to the floor.
- One controller implementation and test surface for both elastic pools.

### Negative

- The memory monitor's full-pool-recycled re-measurement trigger fires
  slower (it counts to the elastic max), delaying count-cap re-learning
  under sustained memory pressure.
- Serve latency now depends on a feedback loop; a mis-tuned wait target
  moves where queueing sits rather than eliminating it (the underlying
  wr0.2 capacity problem is the box CPU — PGC-441 attacks the demand side).

## Implementation Notes

- `cache/runtime/pool_controller.rs` — shared controller + policy tests;
  `cache/serve_pool_state.rs` — shared counters and desired/live sizing;
  `cache/runtime/serve_pool.rs` — bounds, config, reconciler, lazy shrink,
  controller task (spawned from `runtime/setup.rs`).
- Metrics: `serve_pool_size`, `serve_pool_scale_up/down`,
  `serve_pool_backstop_grows`, alongside the existing pool
  liveness/replenish/recycle series.
- One surplus connection parks (with expiry, and released immediately under
  memory pressure) instead of dropping, so the controller's probe cycle at a
  capacity ceiling reuses a backend rather than recreating one; paired with
  the escalating refute hold (ADR-052 amendment) steady state at a stable
  ceiling is an occasional unpark/repark with zero connection churn.
- Amends ADR-052 (controller shared, config-parameterized). Bench validation
  under PGC-442.
