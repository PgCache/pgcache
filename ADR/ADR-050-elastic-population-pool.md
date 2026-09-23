# ADR-050: Elastic population worker pool

## Status

Accepted

## Context

Population tasks are origin-I/O-bound (~tens of ms each, dominated by origin
round trips), but the worker pool was sized from `num_workers` — a CPU count —
with a floor of 2 and blind round-robin onto per-worker unbounded channels.
Demand is bursty: registration waves and invalidation churn reach 100–175
populations/s, while two workers offer ~80–100/s. Past that point the
unbounded queues grow without limit; on the AWS bench (PGC-437) mean
population waits reached 15–35s and individual invalidated queries forwarded
to origin for 87–125s while "populating" — the work itself averaged 18ms.
Round-robin also binds each item to a worker at enqueue time, so one slow item
can strand its lane while the other worker idles.

Any fix must not turn pgcache into an origin-DoS amplifier: churn storms are
exactly when the origin is busiest, so concurrency against it must be bounded
and must back off on evidence of origin congestion.

## Decision

1. **Shared queue, idle-worker rendezvous.** One work queue; a dispatcher task
   pairs each item with the next idle worker (workers register a one-shot slot
   when free). No item is ever bound to a busy worker.
2. **Elastic worker set** between `population_workers_min` (default
   `max(num_workers, 2)`) and `population_workers_max` (default
   `num_workers × 8`) — new settings, decoupled from CPU sizing. The max is
   the origin-protection policy bound on concurrent population SELECTs.
3. **BBR-lite controller** (sibling of the PGC-277 registration gate): sizes
   the pool by Little's law, `N = λ·S_min / 0.7`, from runtime-measured
   demand (enqueue rate) and the windowed *minimum* task time (the
   uncongested baseline, à la BBR min_rtt). It grows only when mean queue
   wait breaches 250ms, never grows while current task time exceeds
   2×`S_min` (origin already queueing), and shrinks one worker per 30s of
   sustained surplus. The writer applies the target on its 1s tick: spawn
   with fresh connection pairs, surplus workers retire themselves between
   work items.

## Rationale

- Static formulas need demand and service time, which only exist at runtime;
  both are already observable at the queue. A boot-time origin-latency probe
  captures neither the query mix nor drift.
- The min-filtered task time separates "origin is slow" (baseline high →
  more workers needed for the same throughput) from "origin is congested"
  (current time inflated over baseline → adding workers amplifies harm).
- Scale-up needs wait evidence so an over-provisioned idle pool never holds
  connections it doesn't use; each worker is an origin + cache connection
  pair, and idle origin connections have measurable cost.

## Consequences

### Positive

- Storm-level demand (150/s) is absorbed with ~10ms queue waits instead of
  multi-minute per-query population outages (bench: pool of 8 vs 2).
- Normal-churn throughput is unchanged; the pool returns to its floor when
  demand ebbs.
- No head-of-line blocking as task-time variance grows.

### Negative

- Under a sustained storm the pool now executes the full demand, spending
  origin and proxy capacity on populations that churn may soon invalidate;
  queueing previously acted as accidental load-shedding. Demand-side control
  is deliberately out of scope here: per-query repopulation backoff (PGC-438)
  is the targeted mechanism.
- Two more tuning surfaces (`population_workers_min/max`), though defaults
  are derived and the controller's internals are not settings.

## Implementation Notes

- `cache/population_pool.rs` — shared counters (demand, task/wait time), the
  desired/live worker bookkeeping, id reuse (bounds the `worker` metric
  label), spawn-failure backoff flag.
- `cache/runtime/population_pool.rs` — the controller (`PoolController::step`
  is a pure function with unit tests); spawned from `runtime/setup.rs`.
- `cache/writer/population.rs` — dispatcher + worker rendezvous, worker
  self-retire; `cache/writer/registration.rs` — spawn context, reconcile on
  the writer's gauge tick, 5s connect-failure cooldown.
- Idle workers re-evaluate retirement on a bounded (30s) wait for work, so a
  quiet pool drains to its floor instead of holding surplus connection pairs
  indefinitely (PGC-455); the controller's live sample excludes in-flight
  connects (PGC-456).
- Metrics: `population_queue`, `population_workers`,
  `population_scale_up/down`, `population_task_floor_seconds`, alongside the
  existing `population.wait_seconds` / `population.task_seconds`.
- Validated on the AWS bench harness against the PGC-437 storm scenarios; see
  the ticket for the pool-2 vs pool-8 A/B data.
