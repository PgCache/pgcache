# ADR-063: Box-level CPU pressure feedback

## Status
Accepted (amended 2026-10-30: the pool controllers no longer act on pressure)

## Context
Three controllers size pgcache's demand on the box it shares with its cache PostgreSQL: the probe-and-verify pool controllers for population workers (ADR-052) and serve connections (ADR-053), and the registration admission gate (ADR-041). Each judges itself by its own queue. On a 2-vCPU bench box under a 25 % write mix all three ran to their ceilings and stayed there: a probe "confirms" when the pool's own throughput rises, and on a shared CPU a new backend wins its throughput by taking cycles from the writer, the fold bgworker, autovacuum and the other pool. The gate backed off to its 20/s floor on the writer queue and the Loading count, both congested, and 20 populations a second on two cores kept the box there. Load sat at 34; the writer's merge slot ran at a fifth of its pace, population wait went from 6 s to 177 s, and hit rate halved. Nothing in the loop had the box as an input, and readmissions, most of the population demand under a write mix, bypassed the gate entirely.

Operators who act on pressure elsewhere (systemd-oomd, Android's lmkd, Kubernetes' PSI node conditions) publish no CPU threshold; what they share is the shape of the rule: a level, sustained over a longer window and still present in a shorter one, with a kill or a taint as the action. CPU is the compressible resource, so for us the action must be graduated.

## Decision
- **Two readings, one signal.** Linux PSI `/proc/pressure/cpu` `some` (the larger of the system-wide file and the process's own cgroup `cpu.pressure`): the fraction of wall time at least one runnable task waited for a CPU, over the kernel's 10 s and 60 s windows. It says *whether* the box is short of CPU and saturates near 1 once it is: the bench box read 0.95 at load 9 and at load 34. Beside it, the scheduler's run-queue wait time from `/proc/schedstat`, turned into the average number of tasks waiting for a CPU and smoothed over a few seconds: the *magnitude*, which keeps moving where PSI saturates. Published once a second on the shared state view, with gauges. Zero where the files are unavailable, which leaves every controller exactly as it was.
- **Two-window rule.** A controller treats the box as congested when both `avg60` and `avg10` are at or above its level, and clear when `avg60` falls below. The 10 s window alone at the level blocks growth without triggering a shed.
- **Pool controllers do not act on pressure** (amended 2026-10-30). The first version shed one member per cadence under congestion and judged every step on the run queue: a probe refuted when the waiting-task count rose, a shed kept when it fell. On the bench box the rule never released. At 80 concurrency a 2-vCPU box reads PSI 0.93–0.97 whether it is congested or simply serving flat out, every backend released does take a task off the run queue so each shed was confirmed, and the pools walked to their floors: population 2–4 and serve 4 against 16 and 16. Interleaved A/B on a rebooted box, same day, reap commit against the full stack: 821 vs 585 pages/s (29 % behind), p99 543 → 840 ms, warm-up 350 → 850 s. The pools keep ADR-052/053 unchanged, judged by their own queue wait only; the signal and its gauges stay for the gate and for diagnosis.
- **The gate gets a third congestion signal.** Box pressure standing on both PSI windows with the waiting-task count grown by a member's worth since the last window backs the admit rate off like a standing writer or population backlog; a saturated box whose run queue is not growing does not block the additive probe. Readmissions take a token from the same bucket; a denied readmission forwards and the entry stays Invalidated for the next read. Pinned queries readmit inside the writer and are unaffected.
- **Levels are internal constants**, in the ADR-050 tradition, to be set against bench evidence, not settings.

## Rationale
- PSI is congestion by definition: it measures waiting, not utilisation, is smoothed by the kernel, costs one file read, and is cgroup-aware in containers. The alternatives were the instantaneous host-wide `/proc/stat` run queue and pgcache-internal victim timings (writer loop latency, bgworker schedstat), the latter the service-time class ADR-052 already rejected.
- The two-window rule is what oomd's `pressure_rising_beyond` and Kubernetes' PSI node conditions converged on independently; it gives hysteresis from the kernel's own smoothing instead of a tick counter.
- The differential verify did what it was designed to do, and that was the defect: on a box saturated by its own serving, every member released eases the run queue, so a verify that keeps a shed when waiting falls confirms shedding useful work. PSI cannot carry a level that separates congestion from full utilisation (0.95 at load 9 and at load 34), and the run queue cannot carry a verdict that separates "this pool is part of the pressure" from "this pool is the work". The pool's own queue wait, which the shed overrode, is the signal that distinguishes them, which is why ADR-052/053 stand.
- A hard CPU throttle OR'd into `throttled()` was rejected for now: it forwards everything and fails in-flight populations at one level, the memory monitor's semantics, where a graduated shed is wanted.

## Consequences

### Positive
- The gate has the box as an input and backs admission off before the writer and the fold bgworker starve; the pools' own queue waits remain their only input.
- Under pressure pgcache's admission demand on a shared box is bounded by the box's own signal; its pool sizes are not.
- Readmission demand is paced for the first time; the memory, disk and heap-head shed paths now have a CPU counterpart.

### Negative
- Under pressure more readmissions forward to origin. The pools are never shed, so a box saturated by serving stays saturated; the bet is that the gate's pacing and smaller populations (PGC-570) recover more than a shed ever did.
- PSI `some` reads high on a box with a few busy cores even when throughput is fine, which is why the levels are deliberately high and the serve level highest; tuning is bench-driven.
- No signal on macOS (dev builds read 0) and none for a remote cache PostgreSQL's box; a cache-side signal is future work.

## Implementation Notes
The readmission horizon (ADR-035) includes the pool's expected queue wait, and staging is checked out by the worker that picks an item up, not at dispatch, so a long queue holds names rather than tables. `cache::pressure` holds the reading, the PSI parser and the published cell; `runtime::cpu_monitor` samples it (a fault-injection file stands in for the kernel in tests). The gate's AIMD step is a pure `GateAimd` with the pressure signal. Amends ADR-041; the amendments to ADR-052 and ADR-053 were withdrawn on 2026-10-30 after the A/B above.
