use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio_postgres::{Config, NoTls};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, warn};

use crate::cache::types::CacheStateView;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::memory::ThrottleDecision;
use crate::result::error_chain_format;
use crate::settings::{DynamicConfigHandle, Settings};

/// Fraction of the count cap at/above which the cache is "under pressure" and the
/// monitor enables connection recycling (PGC-251 Slice 1d).
const RECYCLE_PRESSURE_FRACTION: f64 = 0.8;

/// Samples whole-system used memory against the registration budget and toggles
/// `state_view.registration_throttled` (with hysteresis) so dispatch degrades to
/// origin-forwarding before the box exhausts RAM. "Used" is system-wide
/// (`MemTotal - MemAvailable`, or the binding cgroup's working set — usage minus
/// reclaimable page cache, PGC-354), so it counts pgcache *and* the cache
/// Postgres it manages — not just pgcache's own RSS. The
/// budget is 80% of detected RAM (cgroup-aware), optionally lowered by
/// `memory_limit`, minus the memo's reserved budget. No-op where memory can't be
/// detected (non-Linux): the flag stays clear and registration is unbounded.
#[allow(clippy::cast_precision_loss)] // byte gauges never exceed 2^52
pub(super) async fn memory_monitor(
    state_view: Arc<CacheStateView>,
    dynamic: DynamicConfigHandle,
    mut estimator: CountCapEstimator,
    cancel: CancellationToken,
) {
    const TICK: Duration = Duration::from_millis(500);

    let Some(total_ram) = crate::memory::total_budget_bytes() else {
        debug!("memory monitor: RAM not detectable; registration throttling disabled");
        return;
    };

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(TICK) => {}
        }

        let Some(used) = crate::memory::system_used_bytes() else {
            continue;
        };

        let decision = crate::memory::throttle_evaluate(
            used,
            total_ram,
            dynamic.load().memory_limit.map(|l| l as u64),
            state_view.registration_throttled.load(Ordering::Relaxed),
        );
        throttle_apply(&state_view, &decision, used);

        // Size the count cap from the measured *private* per-query cost (PGC-251).
        // `private` excludes shared_buffers (shared memory), so the marginal is
        // the growing pool — pgcache state + cache-PG plan cache — only; the
        // shared_buffers config is reserved separately against the ceiling.
        let private = crate::memory::system_private_bytes().unwrap_or(used);
        let count = state_view.registered_count.load(Ordering::Relaxed);
        let published_cap = estimator.tick(&state_view, private, decision.ceiling);

        // Connection recycling (PGC-251 Slice 1d): flag memory pressure so the
        // serve loop rolls the pool one connection at a time.
        let pressure = published_cap != usize::MAX
            && (count as f64) >= RECYCLE_PRESSURE_FRACTION * (published_cap as f64);
        state_view.recycle_wanted.store(pressure, Ordering::Relaxed);
        state_view
            .query_count_cap
            .store(published_cap, Ordering::Relaxed);

        gauges_publish(used, &decision, estimator.peak_marginal, published_cap);
    }
}

/// Flip the registration throttle when the decision changed, logging the
/// transition.
fn throttle_apply(state_view: &CacheStateView, decision: &ThrottleDecision, used: u64) {
    let throttled = state_view.registration_throttled.load(Ordering::Relaxed);
    if decision.throttled == throttled {
        return;
    }
    state_view
        .registration_throttled
        .store(decision.throttled, Ordering::Relaxed);
    if decision.throttled {
        warn!(
            used_mb = used / 1_048_576,
            budget_mb = decision.high_mark / 1_048_576,
            "memory pressure: throttling new-query registration (forwarding to origin)"
        );
    } else {
        info!("memory pressure relieved: resuming query registration");
    }
}

#[allow(clippy::cast_precision_loss)] // byte gauges never exceed 2^52
fn gauges_publish(
    used: u64,
    decision: &ThrottleDecision,
    peak_marginal: f64,
    published_cap: usize,
) {
    let cache = &crate::metrics::handles().cache;
    cache.memory_used_bytes.set(used as f64);
    cache.memory_budget_bytes.set(decision.high_mark as f64);
    cache
        .rss_bytes
        .set(crate::memory::process_rss_bytes().unwrap_or(0) as f64);
    cache
        .registration_throttled
        .set(f64::from(u8::from(decision.throttled)));
    cache.marginal_bytes_per_query.set(peak_marginal);
    // usize::MAX (no cap) would saturate the gauge; report 0 = "uncapped".
    cache.query_count_cap.set(if published_cap == usize::MAX {
        0.0
    } else {
        published_cap as f64
    });
}

/// The query count cap learned from the measured per-query private cost
/// (PGC-251), carried across monitor ticks.
pub(super) struct CountCapEstimator {
    /// The cache PG's `shared_buffers`, reserved against the ceiling.
    shared_buffers: u64,
    /// Serve-pool size: the full-pool-recycled threshold of the
    /// re-measurement probe.
    pool_size: usize,
    /// Decaying per-query cost and its peak. The cap anchors on the *live*
    /// private reading (refinement 2), so no fixed baseline is tracked.
    marginal_ewma: f64,
    peak_marginal: f64,
    /// High-water count + the private footprint at it, for the incremental
    /// Δprivate/Δcount marginal. Sampling only on a *new* high-water count
    /// means genuine growth is measured and the plateau (eviction churning
    /// the count just below the cap) produces no noisy samples.
    hw_private: Option<u64>,
    hw_count: usize,
    /// Recycle count at the last re-measurement probe, to detect a full pool
    /// refresh.
    last_recycle_count: usize,
    /// When set, a re-measurement probe is in flight: publish this bounded
    /// cap (current count + one growth chunk) until a fresh sample lands
    /// (Slice 1e).
    probe_target: Option<usize>,
    /// Previously published cap, for rate-limiting cap increases (Slice 1e).
    last_published_cap: Option<usize>,
}

impl CountCapEstimator {
    pub(super) fn new(shared_buffers: u64, pool_size: usize) -> Self {
        Self {
            shared_buffers,
            pool_size,
            marginal_ewma: 0.0,
            peak_marginal: 0.0,
            hw_private: None,
            hw_count: 0,
            last_recycle_count: 0,
            probe_target: None,
            last_published_cap: None,
        }
    }

    /// Fold one tick's readings in; the cap to publish.
    fn tick(&mut self, state_view: &CacheStateView, private: u64, ceiling: u64) -> usize {
        let count = state_view.registered_count.load(Ordering::Relaxed);
        let recycled = state_view.recycle_count.load(Ordering::Relaxed);
        let cap_decision = crate::memory::count_cap_evaluate(
            private,
            self.hw_private.unwrap_or(private),
            count,
            self.hw_count,
            ceiling,
            self.shared_buffers,
            self.marginal_ewma,
            self.peak_marginal,
        );
        self.marginal_ewma = cap_decision.marginal_ewma;
        self.peak_marginal = cap_decision.peak_marginal;
        // Advance the high-water reference when a growth chunk was sampled (or to
        // seed it on the first tick).
        if cap_decision.sampled || self.hw_private.is_none() {
            self.hw_private = Some(private);
            self.hw_count = count;
        }
        // A fresh sample re-learned the cost, so the probe (if any) is complete.
        if cap_decision.sampled {
            self.probe_target = None;
        }
        if recycled >= self.last_recycle_count + self.pool_size {
            self.probe_start(private, count, recycled);
        }

        // While probing (estimate reset, fresh sample pending) publish a bounded
        // cap permitting exactly the probe chunk (exempt from rate-limiting — it
        // must grow by MIN_GROWTH to re-sample); otherwise the re-learned cap,
        // with its *increase* rate-limited so a single bad sample can't spike it.
        let published_cap = match self.probe_target {
            Some(target) => target,
            None => crate::memory::cap_rate_limit(cap_decision.cap, self.last_published_cap),
        };
        self.last_published_cap = Some(published_cap);
        published_cap
    }

    /// Re-measurement probe (PGC-251 Slice 1e): when a full pool has been
    /// recycled, forget the stale per-query estimate, re-anchor the high-water
    /// to now, and allow one MIN_GROWTH chunk of growth. Without this the cap
    /// can't adapt to a *lighter* workload — the count stays pinned at the cap,
    /// so no growth sample is ever taken and the marginal stays frozen at the
    /// old (heavier) cost. The next chunk re-samples the current workload.
    fn probe_start(&mut self, private: u64, count: usize, recycled: usize) {
        self.last_recycle_count = recycled;
        self.hw_count = count;
        self.hw_private = Some(private);
        self.marginal_ewma = 0.0;
        self.peak_marginal = 0.0;
        self.probe_target = Some(count + crate::memory::COUNT_CAP_MIN_GROWTH);
    }
}

/// Query the cache PG's `shared_buffers` (a cluster-wide setting) so the count
/// cap can reserve it against the memory ceiling (PGC-251). Returns 0 if it
/// can't be read — the cap then reserves nothing for it (less conservative but
/// still functional).
pub(super) async fn shared_buffers_bytes_query(settings: &Settings) -> u64 {
    async fn inner(settings: &Settings) -> CacheResult<u64> {
        let (client, conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("shared_buffers query connection error: {e}");
            }
        });
        let row = client
            .query_one(
                "SELECT pg_size_bytes(current_setting('shared_buffers'))",
                &[],
            )
            .await
            .map_into_report::<CacheError>()?;
        let bytes: i64 = row.get(0);
        Ok(u64::try_from(bytes).unwrap_or(0))
    }
    match inner(settings).await {
        Ok(b) => b,
        Err(e) => {
            debug!(
                "shared_buffers query failed ({}); count cap reserves 0 for it",
                error_chain_format(e.current_context())
            );
            0
        }
    }
}
