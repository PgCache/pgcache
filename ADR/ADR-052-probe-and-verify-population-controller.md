# ADR-052: Probe-and-verify population pool controller

## Status

Accepted

## Context

ADR-050's controller sized the elastic population pool by Little's law from
`s_min`, a 30-tick windowed minimum of task time, and vetoed growth whenever
current task time exceeded 2×`s_min` (origin-congestion guard). On the AWS
bench (PGC-437 comment, 2026-09-21) a calm→storm regime change defeated both
uses at once: a read-only warm phase pinned `s_min` at 2.6ms, so when the
first invalidation wave arrived with legitimately bigger ~81ms populations,
the guard read the shift as congestion *and* the formula target computed to
under one worker. The pool sat at ~4 workers against ~115 populations/s;
waits reached 24.5s mean; the window lost 24% throughput. No window length
fixes this class of failure — any stale floor loses a race with some regime
change — and sizing from current task time instead is a runaway loop under
real congestion (congestion inflates service time, inflating the target).

## Decision

Replace the model with two feedback loops; no service-time baseline at all.

1. **Growth is a measured experiment.** While mean queue wait breaches 250ms:
   step the pool up one worker, then verify that completion throughput rose
   by at least half a healthy worker's rate (`ΔX ≥ 0.5/s_now`, this tick's
   mean). Three verdicts: *confirmed* → step again; *refuted* → step back one
   and hold briefly (origin is the bottleneck; oscillate at the capacity
   point); *indeterminate* (too few completions to judge) → grow anyway on a
   slow bounded cadence — the backstop — with its own metric
   (`population_backstop_grows`). Verification waits, bounded, for the
   stepped worker to actually spawn, and never steps while `desired` is
   already ahead of `live`.
2. **Shrink on observed surplus.** Below the wait target, a sustained (30s)
   tick streak with utilization `busy/(live × tick)` under 0.6 releases one
   worker, floored at `population_workers_min`. Any breach resets the streak.

## Rationale

- A backlogged pool's completion rate *is* its capacity, so the verify signal
  is valid exactly when it is consulted; workload shifts and true congestion
  are distinguished by their actual signature (throughput response) instead
  of a level compared against a stale floor.
- The indeterminate branch is unavoidable — long-task storms and origin
  saturation both starve the completion signal, and congestion darkens it
  precisely when the guard matters. Stalling is the field-proven failure;
  growing freely amplifies congestion; slow bounded growth has a harm
  ceiling equal to a static pool of `population_workers_max`, which is the
  origin-protection policy bound anyway. Precedence keeps the loops from
  fighting: the backstop acts only on indeterminate, step-back only on a
  measured refute.
- Shrink never needed a congestion-corrected baseline: inflated task times
  only raise measured utilization, which errs toward keeping workers.
  Utilization comes from existing counters and observes the surplus directly.

## Consequences

### Positive

- Immune to regime-change staleness (regression-tested against the field
  scenario); ramps at the speed the evidence supports (~1 worker per 2 ticks
  confirmed) instead of a formula's guess.
- Under true congestion the pool oscillates ±1 at the measured capacity
  point rather than running to max or refusing to move.
- `population_backstop_grows` makes "the controller was flying blind"
  directly observable.

### Negative

- An idle pool still shrinks to the floor, so the first burst after a quiet
  period pays a few probe-ramp ticks (ADR-050's don't-hold-idle-connections
  trade, carried forward).
- Verify verdicts are per-tick measurements and inherit their noise; β = 0.5
  and the hold/backstop cadences bound the cost of a wrong verdict to ±1
  worker for a few seconds.

## Implementation Notes

- `cache/runtime/population_pool.rs` — `PoolController::step` remains a pure
  function (`ProbeState`: Idle / AwaitVerify / Hold); thresholds are internal
  constants, not settings (per ADR-050's stance). The writer-side reconcile,
  worker self-retire, and spawn-failure cooldown (ADR-050) are unchanged.
- `population_task_floor_seconds` was removed with its only consumer;
  `population_backstop_grows` added.
- Amends ADR-050 (controller core only; pool mechanics stand).
