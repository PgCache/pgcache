use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::cache::types::CacheStateView;

use super::pool_controller::{PoolController, PoolControllerConfig, StepKind, TickSample};

/// Controller tick.
const TICK: Duration = Duration::from_secs(1);

/// Population-pool thresholds (ADR-052). Populations are origin-I/O tasks in
/// the tens of milliseconds, so multi-hundred-ms queue waits are the harm
/// signal (the whole point is avoiding multi-second population outages).
const CONFIG: PoolControllerConfig = PoolControllerConfig {
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
    let mut controller = PoolController::new(min_workers, max_workers, CONFIG);
    let mut prev = pool.counters();
    let mut last_tick = std::time::Instant::now();
    let mut interval = tokio::time::interval(TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let handles = &crate::metrics::handles().reg;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {}
        }
        let now = pool.counters();
        // Measure the real elapsed interval: with MissedTickBehavior::Skip a
        // stalled tick covers several seconds of counter deltas, and dividing
        // by the nominal period would inflate the throughput sample into a
        // spurious verify confirm (PGC-457).
        let elapsed = last_tick.elapsed().as_secs_f64().max(0.1);
        last_tick = std::time::Instant::now();
        let sample = TickSample {
            task_us: now.1 - prev.1,
            task_count: now.2 - prev.2,
            wait_us: now.3 - prev.3,
            wait_count: now.4 - prev.4,
            // Exclude in-flight connects: a probe's verify must wait for
            // the worker to materialize, not judge one that never ran
            // (PGC-456).
            live: pool.live_workers().saturating_sub(pool.pending_connects()),
            backlog: pool.queue_depth(),
            tick_seconds: elapsed,
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
