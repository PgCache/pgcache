//! Shared state of the elastic cache serve pool (ADR-053): monotonic
//! demand/service counters read by the probe-and-verify controller, and the
//! desired/live connection counts reconciled by the serve loop's replenish
//! task.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Counters and sizing state shared between the serve loop (writer side) and
/// the pool controller (reader side). All counters are monotonic; the
/// controller works on per-tick deltas.
#[derive(Debug, Default)]
pub struct ServePool {
    /// Serve execution time, microseconds (connection checkout → reply sent).
    task_us: AtomicU64,
    /// Serves completed.
    task_count: AtomicU64,
    /// Time from dispatch to connection acquisition, microseconds — the
    /// queue-pressure signal.
    wait_us: AtomicU64,
    /// Waits observed (= serves dequeued).
    wait_count: AtomicU64,
    /// Controller's target connection count.
    desired: AtomicUsize,
    /// Connections currently in the pool or checked out.
    live: AtomicUsize,
    /// Serve-queue depth hint, maintained by the serve loop; the controller's
    /// saturated-vs-idle discriminator for zero-observation ticks (PGC-452).
    queue_depth: AtomicUsize,
}

impl ServePool {
    pub fn task_observe(&self, micros: u64) {
        self.task_us.fetch_add(micros, Ordering::Relaxed);
        self.task_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn wait_observe(&self, micros: u64) {
        self.wait_us.fetch_add(micros, Ordering::Relaxed);
        self.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    /// `(task_us, task_count, wait_us, wait_count)` snapshot.
    pub fn counters(&self) -> (u64, u64, u64, u64) {
        (
            self.task_us.load(Ordering::Relaxed),
            self.task_count.load(Ordering::Relaxed),
            self.wait_us.load(Ordering::Relaxed),
            self.wait_count.load(Ordering::Relaxed),
        )
    }

    pub fn desired(&self) -> usize {
        self.desired.load(Ordering::Relaxed)
    }

    pub fn desired_set(&self, n: usize) {
        self.desired.store(n, Ordering::Relaxed);
    }

    pub fn live(&self) -> usize {
        self.live.load(Ordering::Relaxed)
    }

    pub fn live_add(&self, n: usize) {
        self.live.fetch_add(n, Ordering::Relaxed);
    }

    pub fn queue_depth_set(&self, n: usize) {
        self.queue_depth.store(n, Ordering::Relaxed);
    }

    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
    }

    pub fn live_sub(&self, n: usize) {
        // Saturating: a stray extra loss signal must never wrap the gauge.
        let _ = self
            .live
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(n))
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counters_accumulate() {
        let pool = ServePool::default();
        pool.task_observe(100);
        pool.task_observe(50);
        pool.wait_observe(7);
        assert_eq!(pool.counters(), (150, 2, 7, 1));
    }

    #[test]
    fn test_live_saturates_at_zero() {
        let pool = ServePool::default();
        pool.live_add(2);
        pool.live_sub(3);
        assert_eq!(pool.live(), 0);
    }
}
