use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tracing::info;

use crate::cache::types::RegGate;

/// Token bucket pacing new-query registration admission (PGC-277). The refill
/// rate is the BBR-lite controller's `reg_gate.reg_rate` (registrations/sec),
/// read fresh on every `try_take`; `try_take` consumes one token per admitted
/// registration and a denied caller forwards to origin without registering.
/// Burst is ~0.5s of the current rate so brief bursts pass but the sustained
/// rate is held to what the writer can drain. `PGCACHE_REG_RATE` pins a fixed
/// rate instead of the controller (testing/override).
pub(super) struct RegRateBucket {
    inner: Mutex<(f64, Instant)>,
    gate: Arc<RegGate>,
    fixed_override: Option<f64>,
}

impl RegRateBucket {
    pub(super) fn new(gate: Arc<RegGate>) -> Self {
        let fixed_override = std::env::var("PGCACHE_REG_RATE")
            .ok()
            .and_then(|s| s.parse::<f64>().ok())
            .filter(|r| *r > 0.0);
        if let Some(rate) = fixed_override {
            info!("registration rate FIXED override: {rate}/s (PGC-277 test)");
        }
        Self {
            // Start full: seed the refill clock in the past so the first
            // finite-rate `try_take` accrues a full burst. An empty start
            // spuriously denied cold-start registrations (forwarded uncached);
            // the rate, not an empty bucket, is what paces a storm.
            inner: Mutex::new((
                0.0,
                Instant::now()
                    .checked_sub(Duration::from_secs(1))
                    .unwrap_or_else(Instant::now),
            )),
            gate,
            fixed_override,
        }
    }

    pub(super) fn try_take(&self) -> bool {
        let rate = self.fixed_override.unwrap_or_else(|| self.gate.rate());
        // Ungated until the controller has set a finite rate (admit all).
        if !rate.is_finite() {
            return true;
        }
        if rate <= 0.0 {
            return false;
        }
        let burst = (rate * 0.5).max(8.0);
        let mut g = self.inner.lock().expect("lock registration bucket");
        let now = Instant::now();
        let elapsed = now.duration_since(g.1).as_secs_f64();
        g.1 = now;
        g.0 = (g.0 + elapsed * rate).min(burst);
        if g.0 >= 1.0 {
            g.0 -= 1.0;
            true
        } else {
            // Demand exceeded the rate — record the shed so the controller knows
            // it is rate-limited (and may probe up); otherwise it holds.
            self.gate.denied_inc();
            false
        }
    }
}
