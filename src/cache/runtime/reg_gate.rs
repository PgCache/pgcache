use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio_util::sync::CancellationToken;

use crate::cache::types::CacheStateView;

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
/// queue window-min (`qmin`) and the in-flight `Loading` count (`loading`) — and
/// AIMD-paces the admit rate to whichever is most congested: multiplicative
/// back-off if *either* stage's backlog stands and grows, additive probe only
/// when *both* are drained **and** the gate is actively shedding (demand is
/// bumping the rate). The shed gate matters because a partly-warm cache offers
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

    let gate = &state_view.reg_gate;
    let mut bw_window: VecDeque<f64> = VecDeque::with_capacity(BW_WINDOW);
    let mut last_completed = gate.completed_count();
    let mut last_denied = gate.denied_count();
    // Paced admit rate (state): AIMD toward the writer's sustainable rate.
    let mut rate = R_BOOTSTRAP;
    // Previous-window backlog floors, for the grow/drain trend (per signal).
    let mut prev_qmin = 0usize;
    let mut prev_loading = 0usize;

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

        // Backlog trend per stage (BBR's queue signal, observed directly):
        // back off if *either* stage's backlog is standing and growing (admitting
        // faster than that stage drains); probe up only when *both* are drained;
        // otherwise hold (anti-windup — don't keep cutting while a transient
        // backlog clears at the current rate).
        let writer_congested = qmin > DRAIN_FLOOR && qmin >= prev_qmin;
        let pop_congested = loading > LOADING_FLOOR && loading >= prev_loading;
        if writer_congested || pop_congested {
            rate *= R_BACKOFF;
        } else if shedding && qmin <= DRAIN_FLOOR && loading <= LOADING_FLOOR {
            // Drained *and* demand-limited: room to admit more, and something
            // wants it. If not shedding, the rate already exceeds demand — hold.
            rate += R_STEP;
        }
        prev_qmin = qmin;
        prev_loading = loading;
        rate = rate.max(R_MIN);
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
