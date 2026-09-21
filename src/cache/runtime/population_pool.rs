use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::cache::types::CacheStateView;

/// Controller tick.
const TICK: Duration = Duration::from_secs(1);
/// Queue-wait mean above which the pool probes upward. Below it, waits are
/// already harmless (the whole point is avoiding multi-second population
/// outages), so an idle or keeping-up pool never grows.
const WAIT_TARGET: Duration = Duration::from_millis(250);
/// Fraction of one healthy worker's completion rate (`1/s_now`) an upward
/// probe must add to count as confirmed — slack for scheduling noise.
const VERIFY_BETA: f64 = 0.5;
/// Completions a verify tick needs before its throughput delta is judged at
/// all; below this the verdict is indeterminate (the backstop path).
const VERIFY_MIN_COMPLETIONS: u64 = 5;
/// Ticks a refuted probe holds before probing again while the breach lasts.
const PROBE_HOLD_TICKS: u32 = 5;
/// Cadence of backstop growth while verify is indeterminate and the wait
/// breach persists: slow enough to bound congestion amplification, fast
/// enough that a signal-starved storm still reaches `max_workers` in tens of
/// seconds. The harm ceiling is a static pool of `max_workers` — the origin
/// protection policy bound.
const BACKSTOP_TICKS: u32 = 5;
/// Ticks to wait for a stepped-up worker to actually spawn (writer reconcile
/// plus connect time) before abandoning the probe and re-evaluating.
const SPAWN_WAIT_TICKS: u32 = 5;
/// Utilization (busy time / worker time) below which a tick counts toward
/// shrinking. Congestion-inflated task times only raise utilization, which
/// errs toward keeping workers — safely conservative for shrink.
const RHO_SHRINK: f64 = 0.6;
/// Consecutive low-utilization ticks before shrinking by one worker — scale
/// up fast, down slowly.
const DOWN_TICKS: u32 = 30;

/// One tick's worth of pool activity, as deltas of the shared monotonic
/// counters, plus the observed live worker count.
#[derive(Debug, Clone, Copy)]
pub(super) struct TickSample {
    pub task_us: u64,
    pub task_count: u64,
    pub wait_us: u64,
    pub wait_count: u64,
    pub live_workers: usize,
    pub tick_seconds: f64,
}

/// What the controller did this tick, for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum StepKind {
    Hold,
    Grow,
    /// Growth on the backstop cadence: the wait breach persisted while the
    /// verify signal was indeterminate (too few completions to judge).
    GrowBackstop,
    Shrink,
}

/// Upward-probe state: growth is a measured experiment, not a model.
#[derive(Debug, Clone, Copy)]
enum ProbeState {
    Idle,
    /// A step to `target` workers is in flight; judge the throughput delta
    /// against `baseline_x` once the worker is live.
    AwaitVerify {
        baseline_x: f64,
        target: usize,
        ticks_waited: u32,
    },
    /// A refuted probe backs off and holds before re-probing.
    Hold {
        ticks_left: u32,
    },
}

/// Probe-and-verify pool sizing (ADR-052): while queue wait is breached, step
/// the pool up one worker and verify the completion rate actually rose; back
/// off on a measured refute, fall back to a slow bounded cadence when the
/// signal is too thin to judge. Shrink on sustained low utilization. Pure
/// state machine so the policy is unit-testable without a runtime.
pub(super) struct PoolController {
    min_workers: usize,
    max_workers: usize,
    probe: ProbeState,
    backstop_ticks: u32,
    shrink_streak: u32,
}

impl PoolController {
    pub(super) fn new(min_workers: usize, max_workers: usize) -> Self {
        Self {
            min_workers,
            max_workers,
            probe: ProbeState::Idle,
            backstop_ticks: 0,
            shrink_streak: 0,
        }
    }

    /// Advance one tick and return the new desired worker count.
    // Per-tick counts are small (bounded by demand per second), far below
    // f64's 52-bit mantissa.
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn step(&mut self, sample: TickSample, desired: usize) -> (usize, StepKind) {
        let clamp = |n: usize| n.clamp(self.min_workers, self.max_workers);
        let x_now = sample.task_count as f64 / sample.tick_seconds;
        let wait_now = sample
            .wait_us
            .checked_div(sample.wait_count)
            .map_or(Duration::ZERO, Duration::from_micros);

        if wait_now <= WAIT_TARGET {
            // Healthy queue: re-arm the probe and consider shrinking on
            // sustained low utilization of the live workers.
            self.probe = ProbeState::Idle;
            self.backstop_ticks = 0;
            let busy = sample.task_us as f64 / 1e6;
            let capacity = sample.live_workers as f64 * sample.tick_seconds;
            if sample.live_workers > 0 && desired > self.min_workers && busy < RHO_SHRINK * capacity
            {
                self.shrink_streak += 1;
                if self.shrink_streak >= DOWN_TICKS {
                    self.shrink_streak = 0;
                    return (clamp(desired - 1), StepKind::Shrink);
                }
            } else {
                self.shrink_streak = 0;
            }
            return (clamp(desired), StepKind::Hold);
        }

        // Wait breached: probe upward.
        self.shrink_streak = 0;
        match self.probe {
            ProbeState::Hold { ticks_left } => {
                self.probe = if ticks_left > 1 {
                    ProbeState::Hold {
                        ticks_left: ticks_left - 1,
                    }
                } else {
                    ProbeState::Idle
                };
                (clamp(desired), StepKind::Hold)
            }
            ProbeState::Idle => {
                // Step only when prior steps have materialized: if spawn is
                // stuck (connect failures under their own cooldown), running
                // `desired` further ahead of `live` helps nothing.
                if sample.live_workers >= desired {
                    self.probe_step(x_now, desired, StepKind::Grow)
                } else {
                    (clamp(desired), StepKind::Hold)
                }
            }
            ProbeState::AwaitVerify {
                baseline_x,
                target,
                ticks_waited,
            } => {
                if sample.live_workers < target {
                    // The stepped worker hasn't spawned yet; judging now would
                    // refute a worker that never ran. Bounded wait, then
                    // re-evaluate from scratch (spawn failures have their own
                    // cooldown in the writer's reconcile).
                    if ticks_waited >= SPAWN_WAIT_TICKS {
                        self.probe = ProbeState::Idle;
                    } else {
                        self.probe = ProbeState::AwaitVerify {
                            baseline_x,
                            target,
                            ticks_waited: ticks_waited + 1,
                        };
                    }
                    return (clamp(desired), StepKind::Hold);
                }
                if sample.task_count < VERIFY_MIN_COMPLETIONS {
                    // Indeterminate: too few completions to judge (long tasks,
                    // or origin so slow nothing finishes — indistinguishable).
                    // Grow on the slow bounded cadence rather than stall
                    // (field-proven bad) or grow freely (amplifies the one
                    // regime the verify exists for).
                    self.backstop_ticks += 1;
                    if self.backstop_ticks >= BACKSTOP_TICKS {
                        self.backstop_ticks = 0;
                        return self.probe_step(x_now, desired, StepKind::GrowBackstop);
                    }
                    return (clamp(desired), StepKind::Hold);
                }
                self.backstop_ticks = 0;
                let s_now = sample.task_us as f64 / sample.task_count as f64 / 1e6;
                let expected = VERIFY_BETA / s_now.max(1e-6);
                if x_now - baseline_x >= expected {
                    // Confirmed: the added worker delivered; keep stepping.
                    self.probe_step(x_now, desired, StepKind::Grow)
                } else {
                    // Refuted with a valid measurement: adding a worker did
                    // not raise throughput — origin is the bottleneck. Back
                    // off and hold.
                    self.probe = ProbeState::Hold {
                        ticks_left: PROBE_HOLD_TICKS,
                    };
                    (clamp(desired.saturating_sub(1)), StepKind::Shrink)
                }
            }
        }
    }

    fn probe_step(&mut self, x_now: f64, desired: usize, kind: StepKind) -> (usize, StepKind) {
        if desired >= self.max_workers {
            self.probe = ProbeState::Idle;
            return (self.max_workers, StepKind::Hold);
        }
        let target = desired + 1;
        self.probe = ProbeState::AwaitVerify {
            baseline_x: x_now,
            target,
            ticks_waited: 0,
        };
        (target.clamp(self.min_workers, self.max_workers), kind)
    }
}

/// Elastic population pool controller (PGC-437, ADR-052). Probes the pool
/// upward while queue wait is breached — verifying each step against measured
/// completion throughput — and shrinks on sustained low utilization, bounded
/// by `population_workers_min/max`; the writer's reconcile tick applies the
/// target.
pub(super) async fn population_pool_controller(
    state_view: Arc<CacheStateView>,
    min_workers: usize,
    max_workers: usize,
    cancel: CancellationToken,
) {
    let pool = &state_view.population_pool;
    let mut controller = PoolController::new(min_workers, max_workers);
    let mut prev = pool.counters();
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let handles = &crate::metrics::handles().reg;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {}
        }
        let now = pool.counters();
        let sample = TickSample {
            task_us: now.1 - prev.1,
            task_count: now.2 - prev.2,
            wait_us: now.3 - prev.3,
            wait_count: now.4 - prev.4,
            live_workers: pool.live_workers(),
            tick_seconds: TICK.as_secs_f64(),
        };
        prev = now;

        let desired = pool.desired_workers();
        let (next, kind) = controller.step(sample, desired);
        if next != desired {
            pool.desired_workers_set(next);
            match kind {
                StepKind::Grow => handles.population_scale_up.increment(1),
                StepKind::GrowBackstop => {
                    handles.population_scale_up.increment(1);
                    handles.population_backstop_grows.increment(1);
                }
                StepKind::Shrink => handles.population_scale_down.increment(1),
                StepKind::Hold => {}
            }
            tracing::debug!(
                "population pool target {desired} -> {next} ({kind:?}, tasks/tick={})",
                sample.task_count,
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A breached-wait tick where `workers` live workers each complete
    /// `per_worker` tasks of `task_ms`.
    fn storm(workers: usize, per_worker: u64, task_ms: u64) -> TickSample {
        let count = workers as u64 * per_worker;
        TickSample {
            task_us: task_ms * 1000 * count,
            task_count: count,
            wait_us: 2_000_000 * count.max(1),
            wait_count: count.max(1),
            live_workers: workers,
            tick_seconds: 1.0,
        }
    }

    fn calm(workers: usize, count: u64, task_ms: u64, wait_ms: u64) -> TickSample {
        TickSample {
            task_us: task_ms * 1000 * count,
            task_count: count,
            wait_us: wait_ms * 1000 * count.max(1),
            wait_count: count.max(1),
            live_workers: workers,
            tick_seconds: 1.0,
        }
    }

    #[test]
    fn test_storm_with_confirming_throughput_ramps() {
        // Each added worker delivers its share: confirmed every verify tick,
        // so the pool steps monotonically toward the demand point.
        let mut c = PoolController::new(2, 16);
        let mut desired = 2;
        let mut live = 2;
        for _ in 0..10 {
            let (next, kind) = c.step(storm(live, 12, 80), desired);
            assert!(matches!(kind, StepKind::Grow | StepKind::Hold));
            assert!(next >= desired, "must never shrink during a confirmed ramp");
            desired = next;
            live = desired; // spawns keep up in this scenario
        }
        assert!(desired >= 8, "confirmed ramp too slow: reached {desired}");
    }

    #[test]
    fn test_regime_change_after_calm_floor_still_ramps() {
        // The field failure (PGC-437 comment 2026-09-21): a long calm phase
        // with tiny tasks must not leave any floor that vetoes growth when a
        // storm of legitimately bigger tasks arrives.
        let mut c = PoolController::new(2, 16);
        let mut desired = 2;
        for _ in 0..60 {
            let (next, _) = c.step(calm(2, 20, 3, 1), desired);
            desired = next;
        }
        assert_eq!(desired, 2);
        // Storm with 80ms tasks: first breached tick must already step.
        let (next, kind) = c.step(storm(2, 12, 80), desired);
        assert_eq!(next, 3);
        assert_eq!(kind, StepKind::Grow);
    }

    #[test]
    fn test_refuted_probe_backs_off_and_holds() {
        let mut c = PoolController::new(2, 16);
        // Step 2 -> 3.
        let (next, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(next, 3);
        // Worker is live but throughput did not move: refute -> back to 2.
        let (next, kind) = c.step(storm(3, 16, 20), next);
        assert_eq!((next, kind), (2, StepKind::Shrink));
        // Held: no growth while the hold lasts, breach or not.
        for _ in 0..PROBE_HOLD_TICKS {
            let (held, kind) = c.step(storm(2, 25, 20), next);
            assert_eq!((held, kind), (2, StepKind::Hold));
        }
        // Hold expired: probes again while still breached.
        let (next, kind) = c.step(storm(2, 25, 20), 2);
        assert_eq!((next, kind), (3, StepKind::Grow));
    }

    #[test]
    fn test_indeterminate_verify_grows_on_backstop_cadence() {
        // Long tasks: nothing completes, so verify can never judge. Growth
        // must continue on the slow cadence instead of stalling.
        let mut c = PoolController::new(2, 16);
        let (mut desired, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(desired, 3);
        let mut backstop_grows = 0;
        for _ in 0..(BACKSTOP_TICKS * 3) {
            let (next, kind) = c.step(storm(desired, 0, 0), desired);
            if kind == StepKind::GrowBackstop {
                backstop_grows += 1;
                assert_eq!(next, desired + 1);
            } else {
                assert_eq!((next, kind), (desired, StepKind::Hold));
            }
            desired = next;
        }
        assert_eq!(backstop_grows, 3, "one backstop step per cadence window");
    }

    /// A breached tick where total completions stay fixed no matter how many
    /// workers are live — the congestion signature.
    fn congested(workers: usize, count: u64, task_ms: u64) -> TickSample {
        TickSample {
            task_us: task_ms * 1000 * count,
            task_count: count,
            wait_us: 2_000_000 * count.max(1),
            wait_count: count.max(1),
            live_workers: workers,
            tick_seconds: 1.0,
        }
    }

    #[test]
    fn test_persistent_refutes_oscillate_at_capacity_not_max() {
        // True congestion: added workers never raise throughput. The pool
        // must oscillate around the capacity point, not walk to max.
        let mut c = PoolController::new(2, 16);
        let mut desired = 4;
        let mut peak = desired;
        for _ in 0..40 {
            let (next, _) = c.step(congested(desired, 40, 90), desired);
            desired = next;
            peak = peak.max(desired);
        }
        assert!(peak <= 6, "refuted probes must not run away: peak {peak}");
    }

    #[test]
    fn test_spawn_lag_defers_judgment() {
        let mut c = PoolController::new(2, 16);
        let (next, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(next, 3);
        // Worker not live yet: no verdict, no further growth — including
        // after the timeout re-arms the probe (live still lags desired).
        for _ in 0..=SPAWN_WAIT_TICKS {
            let (held, kind) = c.step(storm(2, 25, 20), next);
            assert_eq!((held, kind), (3, StepKind::Hold));
        }
        // The worker finally spawns: the re-armed probe steps again.
        let (again, kind) = c.step(storm(3, 25, 20), next);
        assert_eq!((again, kind), (4, StepKind::Grow));
    }

    #[test]
    fn test_shrink_needs_sustained_low_utilization() {
        let mut c = PoolController::new(2, 16);
        let mut desired = 6;
        // rho = 6 tasks x 20ms / 6 workers = 0.02: far under RHO_SHRINK.
        for _ in 0..DOWN_TICKS - 1 {
            let (next, _) = c.step(calm(6, 6, 20, 1), desired);
            assert_eq!(next, 6);
            desired = next;
        }
        let (next, kind) = c.step(calm(6, 6, 20, 1), desired);
        assert_eq!((next, kind), (5, StepKind::Shrink));
        // Busy pool never shrinks: rho = 25 x 180ms / 5 worker-seconds = 0.9.
        let mut desired = next;
        for _ in 0..DOWN_TICKS + 1 {
            let (next, kind) = c.step(calm(5, 25, 180, 1), desired);
            assert_eq!((next, kind), (5, StepKind::Hold));
            desired = next;
        }
    }

    #[test]
    fn test_breach_resets_shrink_streak() {
        let mut c = PoolController::new(2, 16);
        let mut desired = 6;
        for _ in 0..DOWN_TICKS - 1 {
            let (next, _) = c.step(calm(6, 6, 20, 1), desired);
            desired = next;
        }
        // Breach wipes the streak (and the tick's step is a grow probe).
        let (next, _) = c.step(storm(6, 12, 80), desired);
        desired = next;
        let (next, _) = c.step(calm(desired, 6, 20, 1), desired);
        assert_eq!(next, desired, "streak must restart after a breach");
    }

    #[test]
    fn test_growth_clamped_to_max() {
        let mut c = PoolController::new(2, 3);
        let (next, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(next, 3);
        let (next, kind) = c.step(storm(3, 40, 20), 3);
        assert_eq!((next, kind), (3, StepKind::Hold));
    }

    #[test]
    fn test_cold_pool_holds_within_bounds() {
        let mut c = PoolController::new(2, 16);
        let (next, kind) = c.step(calm(2, 0, 0, 0), 2);
        assert_eq!((next, kind), (2, StepKind::Hold));
    }
}
