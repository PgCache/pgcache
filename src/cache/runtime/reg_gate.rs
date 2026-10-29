use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::cache::pressure::CpuPressure;
use crate::cache::types::CacheStateView;

const TICK: Duration = Duration::from_millis(250);
/// Pace before any drain sample exists. Low enough to blunt the cold-start
/// storm; the startup ramp grows it to the knee within a few ticks.
const R_BOOTSTRAP: f64 = 100.0;
/// Floor the paced rate never drops below — registration always makes some
/// progress even under sustained backlog.
const R_MIN: f64 = 20.0;
/// Backlog at or below this counts as "drained" (writer kept up). The internal
/// queue oscillates to ~0 between bursts when healthy.
const DRAIN_FLOOR: usize = 8;
/// Drain-rate max-filter memory: BW_WINDOW × TICK (= 10s at 250ms), à la BBR.
const BW_WINDOW: usize = 40;
/// Additive-increase step (reg/s per tick) while the writer is keeping up —
/// probes for more capacity. AIMD: gentle up, multiplicative down.
const R_STEP: f64 = 50.0;
/// Multiplicative back-off applied each tick the backlog is *growing* — we are
/// admitting faster than the writer drains. BBR's inflight cap, observed
/// directly via the backlog instead of inferred from RTT.
const R_BACKOFF: f64 = 0.75;
/// Population in-flight at or below this counts as "drained". The second
/// congestion signal (PGC-277): `Loading` queries (origin SELECTs in flight)
/// is published at the writer's ~1s gauge cadence, so a healthy pace leaves a
/// modest standing in-flight; over-admission makes it climb past this floor.
const LOADING_FLOOR: usize = 128;
/// Box CPU pressure (PSI `some` share) at which the gate treats the box as a
/// congested stage (PGC-554): the population pool's shed level, since paced
/// admission and population are the first shed on a saturated box.
const PRESSURE_LEVEL: f64 = 0.5;
/// Growth in the box's waiting-task count over one window that counts as the
/// stage "standing and growing": one member's mark, as in the pool
/// controller. PSI alone cannot show growth once it saturates.
const WAITING_GROWTH: f64 = 0.5;

/// One controller window's congestion signals.
#[derive(Debug, Clone, Copy)]
struct GateSignals {
    /// Writer command-queue backlog window-min.
    qmin: usize,
    /// Population in flight (`Loading`).
    loading: usize,
    /// The bucket denied an admission this window: demand is bumping the rate.
    shedding: bool,
    pressure: CpuPressure,
}

/// The AIMD pacer: the admit rate and the previous window's backlogs, for the
/// stand-and-grow trend. Pure, so the policy is unit-testable.
#[derive(Debug)]
struct GateAimd {
    rate: f64,
    prev_qmin: usize,
    prev_loading: usize,
    prev_waiting: f64,
}

impl GateAimd {
    fn new(rate: f64) -> Self {
        Self {
            rate,
            prev_qmin: 0,
            prev_loading: 0,
            prev_waiting: 0.0,
        }
    }

    /// Backlog trend per stage (BBR's queue signal, observed directly): back
    /// off if *any* stage's backlog is standing and growing — the writer's
    /// command queue, population in flight, or the box itself (PGC-554: both
    /// PSI windows at the level and the run queue's waiting-task count grown
    /// by a member's worth since the last window); probe up only when the
    /// pipeline's own stages are drained and demand is bumping the rate;
    /// otherwise hold (anti-windup — don't keep cutting while a transient
    /// backlog clears at the current rate). A busy box does not by itself
    /// block the probe: a box saturated by useful work reads the same on PSI
    /// as one being thrashed, and only growth tells the admit rate apart.
    fn step(&mut self, s: GateSignals) -> f64 {
        let writer_congested = s.qmin > DRAIN_FLOOR && s.qmin >= self.prev_qmin;
        let pop_congested = s.loading > LOADING_FLOOR && s.loading >= self.prev_loading;
        let box_congested = s.pressure.avg60 >= PRESSURE_LEVEL
            && s.pressure.avg10 >= PRESSURE_LEVEL
            && s.pressure.waiting - self.prev_waiting >= WAITING_GROWTH;
        if writer_congested || pop_congested || box_congested {
            self.rate *= R_BACKOFF;
        } else if s.shedding && s.qmin <= DRAIN_FLOOR && s.loading <= LOADING_FLOOR {
            // Drained *and* demand-limited: room to admit more, and something
            // wants it. If not shedding, the rate already exceeds demand — hold.
            self.rate += R_STEP;
        }
        self.prev_qmin = s.qmin;
        self.prev_loading = s.loading;
        self.prev_waiting = s.pressure.waiting;
        self.rate = self.rate.max(R_MIN);
        self.rate
    }
}

/// BBR-lite adaptive registration gate controller (PGC-277).
///
/// Registration is a pipeline with **two serialized stages on the single writer
/// thread**, and the bottleneck moves between them with the workload:
///  - **command processing** — register/CDC-apply work draining the writer's
///    command queue; binds under write-heavy CDC (the PGC-286 O(N) scan).
///  - **population** — `spawn_local` origin SELECTs; binds when the origin is
///    remote (RTT-bound), where the command queue stays near-empty and is blind
///    to the real backlog.
///
/// So the controller watches a backlog signal for **each** stage — the command
/// queue window-min (`qmin`) and the in-flight `Loading` count (`loading`) —
/// plus the box's own queue, CPU pressure (PGC-554), and AIMD-paces the admit
/// rate to whichever is most congested: multiplicative back-off if *any*
/// stage's backlog stands and grows, additive probe only when *all* are
/// drained **and** the gate is actively shedding (demand is bumping the
/// rate). The shed gate matters because a partly-warm cache offers
/// little registration load, so its backlogs are small for lack of *demand*, not
/// lack of *congestion* — without it the rate drifts up into that void and stops
/// shedding. (An earlier version watched only the command queue and ramped
/// admission unbounded on a remote origin, because population backlog is
/// invisible there; a still-earlier one had no population signal at all.) The drain rate's windowed max is kept for observability
/// (`BtlBw`); control is driven by the backlogs, not by pacing at that estimate
/// (the single-thread drain is too bursty to pace at directly). There is no rate
/// ceiling; the only floor is `R_MIN` so registration never fully stalls.
///
/// The dispatch token bucket reads `reg_rate` and forwards-without-registering
/// when it is empty, so the storm is paced to what the pipeline can actually drain.
pub(super) async fn reg_gate_controller(
    state_view: Arc<CacheStateView>,
    cancel: CancellationToken,
) {
    let gate = &state_view.reg_gate;
    let mut bw_window: VecDeque<f64> = VecDeque::with_capacity(BW_WINDOW);
    let mut last_completed = gate.completed_count();
    let mut last_denied = gate.denied_count();
    // Paced admit rate (state): AIMD toward the pipeline's sustainable rate.
    let mut aimd = GateAimd::new(R_BOOTSTRAP);

    // Pace from the first cold-start query, before the controller has any signal.
    gate.rate_set(R_BOOTSTRAP);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(TICK) => {}
        }

        let completed = gate.completed_count();
        let drained = completed.saturating_sub(last_completed);
        last_completed = completed;
        #[allow(clippy::cast_precision_loss)]
        let drain_rate = drained as f64 / TICK.as_secs_f64();

        // Whether demand bumped against the rate this window (the bucket shed).
        // Only then is probing the rate up justified — otherwise a partly-warm
        // cache with little registration load would drift the rate up into a void.
        let denied = gate.denied_count();
        let shedding = denied.saturating_sub(last_denied) > 0;
        last_denied = denied;

        let (qmin, qmax) = gate.window_take();
        // Population-stage backlog (origin SELECTs in flight). Second congestion
        // signal — the registration pipeline has two serialized stages on the one
        // writer thread, and the bottleneck moves between them with the workload:
        // the command queue (`qmin`) binds under write-heavy CDC; population binds
        // when the origin is remote (RTT-bound SELECTs). Pace to whichever is most
        // congested.
        let loading = gate.loading_get();

        // No registration load this window: hold the rate rather than probe into a
        // void or deflate the capacity estimate with a spurious 0 sample.
        if qmax == 0 && loading <= LOADING_FLOOR {
            continue;
        }

        bw_window.push_back(drain_rate);
        if bw_window.len() > BW_WINDOW {
            bw_window.pop_front();
        }
        // BtlBw analog: windowed max of the observed drain rate. Observability
        // only — the single-thread drain rate is too bursty to *pace* at directly
        // (it latches transient peaks above the sustainable rate), so control is
        // driven by the backlog trends below, not by this estimate.
        let capacity_est = bw_window.iter().copied().fold(0.0_f64, f64::max);

        let rate = aimd.step(GateSignals {
            qmin,
            loading,
            shedding,
            pressure: state_view.cpu_pressure.get(),
        });
        gate.rate_set(rate);

        #[allow(clippy::cast_precision_loss)]
        {
            let m = &crate::metrics::handles().cache;
            m.reg_gate_rate.set(rate);
            m.reg_gate_btlbw.set(capacity_est);
            m.reg_gate_queue_min.set(qmin as f64);
            m.reg_gate_drain_rate.set(drain_rate);
            m.reg_gate_loading.set(loading as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn signals(
        qmin: usize,
        loading: usize,
        shedding: bool,
        avg10: f64,
        avg60: f64,
        waiting: f64,
    ) -> GateSignals {
        GateSignals {
            qmin,
            loading,
            shedding,
            pressure: CpuPressure {
                avg10,
                avg60,
                waiting,
            },
        }
    }

    fn drained(shedding: bool, avg10: f64, avg60: f64, waiting: f64) -> GateSignals {
        signals(0, 0, shedding, avg10, avg60, waiting)
    }

    #[test]
    fn test_drained_and_shedding_probes_up() {
        let mut aimd = GateAimd::new(100.0);
        assert_eq!(aimd.step(drained(true, 0.0, 0.0, 0.0)), 100.0 + R_STEP);
        assert_eq!(
            aimd.step(drained(false, 0.0, 0.0, 0.0)),
            100.0 + R_STEP,
            "no demand: hold"
        );
    }

    #[test]
    fn test_standing_writer_backlog_backs_off_to_the_floor() {
        let mut aimd = GateAimd::new(100.0);
        let mut rate = aimd.step(signals(DRAIN_FLOOR + 1, 0, true, 0.0, 0.0, 0.0));
        assert_eq!(rate, 100.0 * R_BACKOFF);
        for _ in 0..40 {
            rate = aimd.step(signals(DRAIN_FLOOR + 1, 0, true, 0.0, 0.0, 0.0));
        }
        assert_eq!(rate, R_MIN);
    }

    /// Both PSI windows at the level and the run queue growing by a member's
    /// worth: the box is a congested stage like the other two.
    #[test]
    fn test_box_pressure_with_a_growing_run_queue_backs_off() {
        let mut aimd = GateAimd::new(100.0);
        assert_eq!(aimd.step(drained(true, 0.9, 0.9, 2.0)), 100.0 * R_BACKOFF);
        assert_eq!(
            aimd.step(drained(true, 0.95, 0.95, 2.6)),
            100.0 * R_BACKOFF * R_BACKOFF,
            "still growing: back off again"
        );
    }

    /// A saturated box whose run queue is not growing is busy, not thrashed:
    /// the drained, shedding gate probes up through it.
    #[test]
    fn test_box_pressure_without_growth_does_not_block_the_probe() {
        let mut aimd = GateAimd::new(100.0);
        let after = aimd.step(drained(true, 0.95, 0.95, 9.0));
        assert_eq!(
            after,
            100.0 * R_BACKOFF,
            "first window: growth from nothing"
        );
        assert_eq!(aimd.step(drained(true, 0.95, 0.95, 9.1)), after + R_STEP);
        assert_eq!(
            aimd.step(drained(true, 0.95, 0.95, 8.7)),
            after + R_STEP * 2.0,
            "falling: probe"
        );
    }

    /// PSI windows below the level: run-queue growth alone is not the box's
    /// congestion (it may be another workload's).
    #[test]
    fn test_run_queue_growth_below_the_level_is_ignored() {
        let mut aimd = GateAimd::new(100.0);
        assert_eq!(aimd.step(drained(true, 0.3, 0.3, 2.0)), 100.0 + R_STEP);
        assert_eq!(
            aimd.step(drained(true, 0.3, 0.3, 4.0)),
            100.0 + R_STEP * 2.0
        );
    }
}
