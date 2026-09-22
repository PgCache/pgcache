//! Probe-and-verify pool sizing (ADR-052): the pure controller shared by the
//! population worker pool and the cache serve pool. Growth is a measured
//! experiment — step up one while queue wait is breached, verify completion
//! throughput actually rose — and shrink follows sustained low utilization.
//! Thresholds vary per pool through [`PoolControllerConfig`].

use std::time::Duration;

/// Per-pool thresholds for the probe-and-verify policy.
#[derive(Debug, Clone, Copy)]
pub(super) struct PoolControllerConfig {
    /// Queue-wait mean above which the pool probes upward. Below it, waits
    /// are harmless, so an idle or keeping-up pool never grows.
    pub wait_target: Duration,
    /// Fraction of one healthy worker's completion rate (`1/s_now`) an upward
    /// probe must add to count as confirmed — slack for scheduling noise.
    pub verify_beta: f64,
    /// Completions a verify tick needs before its throughput delta is judged
    /// at all; below this the verdict is indeterminate (the backstop path).
    pub verify_min_completions: u64,
    /// Ticks a refuted probe holds before probing again while the breach
    /// lasts. Doubles per consecutive refute up to [`Self::probe_hold_max_ticks`]:
    /// a stable ceiling is probed at a decaying cadence instead of a fixed
    /// churn cycle.
    pub probe_hold_ticks: u32,
    /// Cap on the escalating refute hold.
    pub probe_hold_max_ticks: u32,
    /// Cadence of backstop growth while verify is indeterminate and the wait
    /// breach persists: slow enough to bound congestion amplification, fast
    /// enough that a signal-starved storm still reaches the max in tens of
    /// seconds. The harm ceiling is a static pool of `max` — the protection
    /// policy bound.
    pub backstop_ticks: u32,
    /// Ticks to wait for a stepped-up member to actually materialize before
    /// abandoning the probe and re-evaluating.
    pub spawn_wait_ticks: u32,
    /// Utilization (busy time / member time) below which a tick counts toward
    /// shrinking. Congestion-inflated task times only raise utilization,
    /// which errs toward keeping members — safely conservative for shrink.
    pub rho_shrink: f64,
    /// Consecutive low-utilization ticks before shrinking by one — scale up
    /// fast, down slowly.
    pub down_ticks: u32,
}

/// One tick's worth of pool activity, as deltas of the shared monotonic
/// counters, plus the observed live member count.
#[derive(Debug, Clone, Copy)]
pub(super) struct TickSample {
    pub task_us: u64,
    pub task_count: u64,
    pub wait_us: u64,
    pub wait_count: u64,
    pub live: usize,
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
    /// A step to `target` members is in flight; judge the throughput delta
    /// against `baseline_x` once the member is live.
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

/// Consecutive healthy (sub-target wait) ticks before the probe state and the
/// refute-hold ladder reset. At a hovering ceiling, single noisy healthy
/// ticks must not restart eager probing.
const HEALTHY_RESET_TICKS: u32 = 5;

/// Pure state machine so the policy is unit-testable without a runtime.
pub(super) struct PoolController {
    min: usize,
    max: usize,
    config: PoolControllerConfig,
    probe: ProbeState,
    backstop_ticks: u32,
    shrink_streak: u32,
    /// Refutes since the last confirmed grow or sustained-healthy reset;
    /// drives the escalating hold.
    consecutive_refutes: u32,
    healthy_streak: u32,
}

impl PoolController {
    pub(super) fn new(min: usize, max: usize, config: PoolControllerConfig) -> Self {
        Self {
            min,
            max,
            config,
            probe: ProbeState::Idle,
            backstop_ticks: 0,
            shrink_streak: 0,
            consecutive_refutes: 0,
            healthy_streak: 0,
        }
    }

    /// Advance one tick and return the new desired member count.
    // Per-tick counts are small (bounded by demand per second), far below
    // f64's 52-bit mantissa.
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn step(&mut self, sample: TickSample, desired: usize) -> (usize, StepKind) {
        let clamp = |n: usize| n.clamp(self.min, self.max);
        let x_now = sample.task_count as f64 / sample.tick_seconds;
        let wait_now = sample
            .wait_us
            .checked_div(sample.wait_count)
            .map_or(Duration::ZERO, Duration::from_micros);

        if wait_now <= self.config.wait_target {
            // Healthy queue: after a sustained streak (not one noisy tick at
            // a hovering ceiling), re-arm the probe and the hold ladder; and
            // consider shrinking on sustained low utilization of the live
            // members.
            self.healthy_streak += 1;
            if self.healthy_streak >= HEALTHY_RESET_TICKS {
                self.probe = ProbeState::Idle;
                self.backstop_ticks = 0;
                self.consecutive_refutes = 0;
            }
            let busy = sample.task_us as f64 / 1e6;
            let capacity = sample.live as f64 * sample.tick_seconds;
            if sample.live > 0 && desired > self.min && busy < self.config.rho_shrink * capacity {
                self.shrink_streak += 1;
                if self.shrink_streak >= self.config.down_ticks {
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
        self.healthy_streak = 0;
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
                // stuck, running `desired` further ahead of `live` helps
                // nothing.
                if sample.live >= desired {
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
                if sample.live < target {
                    // The stepped member hasn't materialized yet; judging now
                    // would refute a member that never ran. Bounded wait, then
                    // re-evaluate from scratch.
                    if ticks_waited >= self.config.spawn_wait_ticks {
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
                if sample.task_count < self.config.verify_min_completions {
                    // Indeterminate: too few completions to judge (long tasks,
                    // or the backend so slow nothing finishes —
                    // indistinguishable). Grow on the slow bounded cadence
                    // rather than stall (field-proven bad) or grow freely
                    // (amplifies the one regime the verify exists for).
                    self.backstop_ticks += 1;
                    if self.backstop_ticks >= self.config.backstop_ticks {
                        self.backstop_ticks = 0;
                        return self.probe_step(x_now, desired, StepKind::GrowBackstop);
                    }
                    return (clamp(desired), StepKind::Hold);
                }
                self.backstop_ticks = 0;
                let s_now = sample.task_us as f64 / sample.task_count as f64 / 1e6;
                let expected = self.config.verify_beta / s_now.max(1e-6);
                if x_now - baseline_x >= expected {
                    // Confirmed: the added member delivered; keep stepping,
                    // and reset the hold ladder — the ceiling moved.
                    self.consecutive_refutes = 0;
                    self.probe_step(x_now, desired, StepKind::Grow)
                } else {
                    // Refuted with a valid measurement: adding a member did
                    // not raise throughput — the backend is the bottleneck.
                    // Back off and hold, doubling the hold per consecutive
                    // refute so a stable ceiling converges to a slow cadence.
                    let hold = self
                        .config
                        .probe_hold_ticks
                        .saturating_mul(1 << self.consecutive_refutes.min(6))
                        .min(self.config.probe_hold_max_ticks);
                    self.consecutive_refutes = self.consecutive_refutes.saturating_add(1);
                    self.probe = ProbeState::Hold { ticks_left: hold };
                    (clamp(desired.saturating_sub(1)), StepKind::Shrink)
                }
            }
        }
    }

    fn probe_step(&mut self, x_now: f64, desired: usize, kind: StepKind) -> (usize, StepKind) {
        if desired >= self.max {
            self.probe = ProbeState::Idle;
            return (self.max, StepKind::Hold);
        }
        let target = desired + 1;
        self.probe = ProbeState::AwaitVerify {
            baseline_x: x_now,
            target,
            ticks_waited: 0,
        };
        (target.clamp(self.min, self.max), kind)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The population pool's thresholds (also the test reference config).
    pub(super) const CFG: PoolControllerConfig = PoolControllerConfig {
        wait_target: Duration::from_millis(250),
        verify_beta: 0.5,
        verify_min_completions: 5,
        probe_hold_ticks: 5,
        probe_hold_max_ticks: 60,
        backstop_ticks: 5,
        spawn_wait_ticks: 5,
        rho_shrink: 0.6,
        down_ticks: 30,
    };

    /// A breached-wait tick where `live` members each complete `per_member`
    /// tasks of `task_ms`.
    pub(super) fn storm(live: usize, per_member: u64, task_ms: u64) -> TickSample {
        let count = live as u64 * per_member;
        TickSample {
            task_us: task_ms * 1000 * count,
            task_count: count,
            wait_us: 2_000_000 * count.max(1),
            wait_count: count.max(1),
            live,
            tick_seconds: 1.0,
        }
    }

    pub(super) fn calm(live: usize, count: u64, task_ms: u64, wait_ms: u64) -> TickSample {
        TickSample {
            task_us: task_ms * 1000 * count,
            task_count: count,
            wait_us: wait_ms * 1000 * count.max(1),
            wait_count: count.max(1),
            live,
            tick_seconds: 1.0,
        }
    }

    /// A breached tick where total completions stay fixed no matter how many
    /// members are live — the congestion signature.
    pub(super) fn congested(live: usize, count: u64, task_ms: u64) -> TickSample {
        TickSample {
            task_us: task_ms * 1000 * count,
            task_count: count,
            wait_us: 2_000_000 * count.max(1),
            wait_count: count.max(1),
            live,
            tick_seconds: 1.0,
        }
    }

    #[test]
    fn test_storm_with_confirming_throughput_ramps() {
        // Each added member delivers its share: confirmed every verify tick,
        // so the pool steps monotonically toward the demand point.
        let mut c = PoolController::new(2, 16, CFG);
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
        let mut c = PoolController::new(2, 16, CFG);
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
        let mut c = PoolController::new(2, 16, CFG);
        // Step 2 -> 3.
        let (next, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(next, 3);
        // Member is live but throughput did not move: refute -> back to 2.
        let (next, kind) = c.step(storm(3, 16, 20), next);
        assert_eq!((next, kind), (2, StepKind::Shrink));
        // Held: no growth while the hold lasts, breach or not.
        for _ in 0..CFG.probe_hold_ticks {
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
        let mut c = PoolController::new(2, 16, CFG);
        let (mut desired, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(desired, 3);
        let mut backstop_grows = 0;
        for _ in 0..(CFG.backstop_ticks * 3) {
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

    #[test]
    fn test_persistent_refutes_oscillate_at_capacity_not_max() {
        // True congestion: added members never raise throughput. The pool
        // must oscillate around the capacity point, not walk to max.
        let mut c = PoolController::new(2, 16, CFG);
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
        let mut c = PoolController::new(2, 16, CFG);
        let (next, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(next, 3);
        // Member not live yet: no verdict, no further growth — including
        // after the timeout re-arms the probe (live still lags desired).
        for _ in 0..=CFG.spawn_wait_ticks {
            let (held, kind) = c.step(storm(2, 25, 20), next);
            assert_eq!((held, kind), (3, StepKind::Hold));
        }
        // The member finally materializes: the re-armed probe steps again.
        let (again, kind) = c.step(storm(3, 25, 20), next);
        assert_eq!((again, kind), (4, StepKind::Grow));
    }

    #[test]
    fn test_shrink_needs_sustained_low_utilization() {
        let mut c = PoolController::new(2, 16, CFG);
        let mut desired = 6;
        // rho = 6 tasks x 20ms / 6 members = 0.02: far under rho_shrink.
        for _ in 0..CFG.down_ticks - 1 {
            let (next, _) = c.step(calm(6, 6, 20, 1), desired);
            assert_eq!(next, 6);
            desired = next;
        }
        let (next, kind) = c.step(calm(6, 6, 20, 1), desired);
        assert_eq!((next, kind), (5, StepKind::Shrink));
        // Busy pool never shrinks: rho = 25 x 180ms / 5 member-seconds = 0.9.
        let mut desired = next;
        for _ in 0..CFG.down_ticks + 1 {
            let (next, kind) = c.step(calm(5, 25, 180, 1), desired);
            assert_eq!((next, kind), (5, StepKind::Hold));
            desired = next;
        }
    }

    #[test]
    fn test_breach_resets_shrink_streak() {
        let mut c = PoolController::new(2, 16, CFG);
        let mut desired = 6;
        for _ in 0..CFG.down_ticks - 1 {
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
        let mut c = PoolController::new(2, 3, CFG);
        let (next, _) = c.step(storm(2, 25, 20), 2);
        assert_eq!(next, 3);
        let (next, kind) = c.step(storm(3, 40, 20), 3);
        assert_eq!((next, kind), (3, StepKind::Hold));
    }

    #[test]
    fn test_cold_pool_holds_within_bounds() {
        let mut c = PoolController::new(2, 16, CFG);
        let (next, kind) = c.step(calm(2, 0, 0, 0), 2);
        assert_eq!((next, kind), (2, StepKind::Hold));
    }
}

#[cfg(test)]
mod ladder_tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use super::tests::congested;
    use super::tests::{CFG, calm, storm};
    use super::*;

    /// Run refute cycles at a fixed ceiling and return the hold length (ticks
    /// between a refute and the next probe) of each cycle.
    fn hold_gaps(cycles: usize) -> Vec<u32> {
        let mut c = PoolController::new(2, 16, CFG);
        let mut desired = 4;
        let mut gaps = Vec::new();
        let mut hold_run: u32 = 0;
        let mut seen_refute = false;
        while gaps.len() < cycles {
            let (next, kind) = c.step(congested(desired, 40, 90), desired);
            match kind {
                StepKind::Shrink => {
                    seen_refute = true;
                    hold_run = 0;
                }
                StepKind::Hold if seen_refute => hold_run += 1,
                StepKind::Grow if seen_refute => {
                    gaps.push(hold_run);
                    hold_run = 0;
                }
                _ => {}
            }
            desired = next;
        }
        gaps
    }

    #[test]
    fn test_refute_hold_escalates_and_caps() {
        assert_eq!(hold_gaps(6), vec![5, 10, 20, 40, 60, 60]);
    }

    #[test]
    fn test_confirmed_grow_resets_ladder() {
        let mut c = PoolController::new(2, 16, CFG);
        let mut desired = 4;
        // Two refuted cycles escalate the ladder (next hold would be 20).
        for _ in 0..40 {
            let (next, _) = c.step(congested(desired, 40, 90), desired);
            desired = next;
        }
        // The ceiling lifts: the first Grow is only the probe step — the
        // ladder resets when the *next* tick confirms it (the second Grow).
        let mut grows = 0;
        while grows < 2 {
            let (next, kind) = c.step(storm(desired, 12, 80), desired);
            desired = next;
            if kind == StepKind::Grow {
                grows += 1;
            }
        }
        // ...so after the next refute, the hold is back to the base length.
        let mut hold_run = 0;
        let mut refuted = false;
        loop {
            let (next, kind) = c.step(congested(desired, 40, 90), desired);
            desired = next;
            match kind {
                StepKind::Shrink => {
                    refuted = true;
                    hold_run = 0;
                }
                StepKind::Hold if refuted => hold_run += 1,
                StepKind::Grow if refuted => break,
                _ => {}
            }
        }
        assert_eq!(hold_run, 5, "confirm must reset the hold ladder");
    }

    #[test]
    fn test_sustained_healthy_resets_ladder() {
        let mut c = PoolController::new(2, 16, CFG);
        let mut desired = 4;
        for _ in 0..40 {
            let (next, _) = c.step(congested(desired, 40, 90), desired);
            desired = next;
        }
        // Breach resolves for a sustained streak: ladder re-arms.
        for _ in 0..HEALTHY_RESET_TICKS {
            let (next, _) = c.step(calm(desired, 40, 10, 1), desired);
            desired = next;
        }
        let mut hold_run = 0;
        let mut refuted = false;
        loop {
            let (next, kind) = c.step(congested(desired, 40, 90), desired);
            desired = next;
            match kind {
                StepKind::Shrink => {
                    refuted = true;
                    hold_run = 0;
                }
                StepKind::Hold if refuted => hold_run += 1,
                StepKind::Grow if refuted => break,
                _ => {}
            }
        }
        assert_eq!(hold_run, 5, "sustained healthy must reset the hold ladder");
    }

    #[test]
    fn test_single_healthy_tick_does_not_reset_ladder() {
        let mut c = PoolController::new(2, 16, CFG);
        let mut desired = 4;
        // Two full refuted cycles: ladder is at 20 for the next refute.
        let mut refutes = 0;
        while refutes < 2 {
            let (next, kind) = c.step(congested(desired, 40, 90), desired);
            if kind == StepKind::Shrink {
                refutes += 1;
            }
            desired = next;
        }
        // One noisy healthy tick mid-hold...
        let (next, _) = c.step(calm(desired, 40, 10, 1), desired);
        desired = next;
        // ...must not reset: the third refuted cycle still holds for 20.
        let mut hold_run = 0;
        let mut refuted = false;
        loop {
            let (next, kind) = c.step(congested(desired, 40, 90), desired);
            desired = next;
            match kind {
                StepKind::Shrink => {
                    refuted = true;
                    hold_run = 0;
                }
                StepKind::Hold if refuted => hold_run += 1,
                StepKind::Grow if refuted => break,
                _ => {}
            }
        }
        assert_eq!(hold_run, 20, "one healthy tick must not reset the ladder");
    }
}
