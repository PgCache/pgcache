use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::cache::types::CacheStateView;

/// Controller tick.
const TICK: Duration = Duration::from_secs(1);
/// Pool utilization target: size for `N = λ·S / RHO_TARGET` so the pool runs
/// with headroom instead of at the queueing knee (M/M/N wait explodes as
/// utilization → 1; the PGC-437 storms saturated a 2-worker pool at ~0.95).
const RHO_TARGET: f64 = 0.7;
/// Queue-wait mean above which the pool scales up. Below it, waits are already
/// harmless (the whole point is avoiding multi-second population outages), so
/// an idle or keeping-up pool never grows.
const WAIT_TARGET: Duration = Duration::from_millis(250);
/// Service-time min-filter window, in ticks (à la BBR min_rtt): the windowed
/// minimum is the uncongested service-time baseline used for sizing.
const S_MIN_WINDOW: usize = 30;
/// Task time above `S_CONGESTED_FACTOR × s_min` means the origin itself is
/// queueing — adding workers would amplify the origin's overload, so the
/// controller holds instead of scaling up.
const S_CONGESTED_FACTOR: f64 = 2.0;
/// Consecutive ticks of computed target below the current desired count
/// before shrinking by one worker — scale up fast, down slowly.
const DOWN_TICKS: u32 = 30;

/// One tick's worth of pool activity, as deltas of the shared monotonic
/// counters.
#[derive(Debug, Clone, Copy)]
pub(super) struct TickSample {
    pub enqueued: u64,
    pub task_us: u64,
    pub task_count: u64,
    pub wait_us: u64,
    pub wait_count: u64,
    pub tick_seconds: f64,
}

/// Little's-law pool sizing with BBR-style congestion guard. Pure state
/// machine so the sizing policy is unit-testable without a runtime.
pub(super) struct PoolController {
    min_workers: usize,
    max_workers: usize,
    /// Recent per-tick mean task times (seconds); its minimum is `s_min`.
    s_window: VecDeque<f64>,
    surplus_ticks: u32,
}

impl PoolController {
    pub(super) fn new(min_workers: usize, max_workers: usize) -> Self {
        Self {
            min_workers,
            max_workers,
            s_window: VecDeque::with_capacity(S_MIN_WINDOW),
            surplus_ticks: 0,
        }
    }

    /// Uncongested service-time baseline (seconds), if any sample exists yet.
    pub(super) fn s_min(&self) -> Option<f64> {
        self.s_window
            .iter()
            .copied()
            .min_by(f64::total_cmp)
            .filter(|s| *s > 0.0)
    }

    /// Advance one tick and return the new desired worker count.
    // Per-tick counts are small (bounded by demand per second), far below both
    // f64's 52-bit mantissa and any truncation-relevant magnitude; the f64
    // target is clamped to `max_workers` before the cast.
    #[allow(
        clippy::cast_precision_loss,
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss
    )]
    pub(super) fn step(&mut self, sample: TickSample, desired: usize) -> usize {
        let s_now = if sample.task_count > 0 {
            let s = sample.task_us as f64 / sample.task_count as f64 / 1e6;
            if self.s_window.len() == S_MIN_WINDOW {
                self.s_window.pop_front();
            }
            self.s_window.push_back(s);
            Some(s)
        } else {
            None
        };
        let Some(s_min) = self.s_min() else {
            // No service-time signal yet (cold pool): hold.
            return desired.clamp(self.min_workers, self.max_workers);
        };

        let lambda = sample.enqueued as f64 / sample.tick_seconds;
        let raw_target = (lambda * s_min / RHO_TARGET)
            .ceil()
            .min(self.max_workers as f64);
        let target = (raw_target as usize).clamp(self.min_workers, self.max_workers);

        let wait_now = sample
            .wait_us
            .checked_div(sample.wait_count)
            .map_or(Duration::ZERO, Duration::from_micros);
        let origin_congested = s_now.is_some_and(|s| s > s_min * S_CONGESTED_FACTOR);

        if target > desired {
            self.surplus_ticks = 0;
            // Grow only on evidence of harm (queue wait breached) and never
            // into an already-congested origin.
            if wait_now > WAIT_TARGET && !origin_congested {
                return target;
            }
            return desired.clamp(self.min_workers, self.max_workers);
        }

        if target < desired {
            self.surplus_ticks += 1;
            if self.surplus_ticks >= DOWN_TICKS {
                self.surplus_ticks = 0;
                return (desired - 1).clamp(self.min_workers, self.max_workers);
            }
        } else {
            self.surplus_ticks = 0;
        }
        desired.clamp(self.min_workers, self.max_workers)
    }
}

/// Elastic population pool controller (PGC-437). Sizes the worker pool by
/// Little's law from runtime-measured demand (enqueue rate) and uncongested
/// service time, bounded by `population_workers_min/max`; the writer's
/// reconcile tick applies the target.
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
            enqueued: now.0 - prev.0,
            task_us: now.1 - prev.1,
            task_count: now.2 - prev.2,
            wait_us: now.3 - prev.3,
            wait_count: now.4 - prev.4,
            tick_seconds: TICK.as_secs_f64(),
        };
        prev = now;

        let desired = pool.desired_workers();
        let next = controller.step(sample, desired);
        if next != desired {
            pool.desired_workers_set(next);
            if next > desired {
                handles.population_scale_up.increment(1);
            } else {
                handles.population_scale_down.increment(1);
            }
            tracing::debug!(
                "population pool target {desired} -> {next} (enqueued/tick={} s_min={:?})",
                sample.enqueued,
                controller.s_min().map(Duration::from_secs_f64),
            );
        }
        if let Some(s_min) = controller.s_min() {
            handles.population_task_floor.set(s_min);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(enqueued: u64, task_ms: u64, task_count: u64, wait_ms: u64) -> TickSample {
        TickSample {
            enqueued,
            task_us: task_ms * 1000 * task_count,
            task_count,
            wait_us: wait_ms * 1000 * task_count.max(1),
            wait_count: task_count.max(1),
            tick_seconds: 1.0,
        }
    }

    #[test]
    fn test_burst_with_breached_wait_scales_up_to_littles_law_target() {
        let mut c = PoolController::new(2, 16);
        // 150/s at 25ms uncongested => N = ceil(150 * 0.025 / 0.7) = 6.
        let mut desired = 2;
        desired = c.step(sample(150, 25, 100, 500), desired);
        assert_eq!(desired, 6);
    }

    #[test]
    fn test_no_growth_while_waits_are_healthy() {
        let mut c = PoolController::new(2, 16);
        let desired = c.step(sample(150, 25, 100, 1), 2);
        assert_eq!(desired, 2);
    }

    #[test]
    fn test_congested_origin_blocks_growth() {
        let mut c = PoolController::new(2, 16);
        let mut desired = 2;
        desired = c.step(sample(10, 25, 10, 1), desired); // seed s_min = 25ms
        // Task time tripled (origin queueing) while waits are breached: hold.
        desired = c.step(sample(150, 75, 100, 500), desired);
        assert_eq!(desired, 2);
    }

    #[test]
    fn test_target_clamped_to_max() {
        let mut c = PoolController::new(2, 4);
        let desired = c.step(sample(1000, 25, 100, 500), 2);
        assert_eq!(desired, 4);
    }

    #[test]
    fn test_scale_down_needs_sustained_surplus_and_steps_by_one() {
        let mut c = PoolController::new(2, 16);
        let mut desired = c.step(sample(150, 25, 100, 500), 2);
        assert_eq!(desired, 6);
        // Demand collapses: no shrink until DOWN_TICKS consecutive surplus
        // ticks, then one worker at a time.
        for _ in 0..DOWN_TICKS - 1 {
            desired = c.step(sample(1, 25, 1, 1), desired);
            assert_eq!(desired, 6);
        }
        desired = c.step(sample(1, 25, 1, 1), desired);
        assert_eq!(desired, 5);
    }

    #[test]
    fn test_cold_pool_holds_within_bounds() {
        let mut c = PoolController::new(2, 16);
        assert_eq!(c.step(sample(50, 0, 0, 0), 2), 2);
    }
}
