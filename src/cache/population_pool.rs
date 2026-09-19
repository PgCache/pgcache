use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

/// Shared state for the elastic population worker pool (PGC-437).
///
/// Division of labor mirrors [`RegGate`](super::reg_gate::RegGate): workers
/// publish monotonic demand/service counters here, the controller (runtime
/// side) reads them and writes `desired_workers`, and the writer reconciles
/// the live worker set toward the target on its loop tick. Population tasks
/// are origin-I/O-bound, so the right worker count follows Little's law
/// (arrival rate × service time), not the CPU-derived `num_workers`.
pub struct PopulationPool {
    /// Monotonic count of work items enqueued to the shared population queue.
    /// The controller's demand estimate (λ) is `Δenqueued / Δt`.
    enqueued: AtomicU64,
    /// Monotonic sum of population task time, in microseconds, and its count.
    /// The controller's service-time estimate (S) is `Δsum / Δcount`; its
    /// windowed minimum is the uncongested baseline (à la BBR min_rtt), and
    /// inflation above that baseline is the origin-congestion back-off signal.
    task_us: AtomicU64,
    task_count: AtomicU64,
    /// Monotonic sum of queue wait, in microseconds, and its count. The
    /// controller scales up only when the recent mean wait breaches its
    /// target, so an idle pool never grows.
    wait_us: AtomicU64,
    wait_count: AtomicU64,
    /// Worker-count target, written by the controller, applied by the writer.
    desired_workers: AtomicUsize,
    /// Live worker count. Incremented by the writer before spawning a worker
    /// (decremented again if its connections fail), decremented by a worker on
    /// any exit path.
    live_workers: AtomicUsize,
    /// Retired worker ids available for reuse, so the `worker` metric label
    /// stays bounded by the pool ceiling across scale cycles.
    free_ids: Mutex<Vec<usize>>,
    /// Next fresh worker id when `free_ids` is empty.
    next_id: AtomicUsize,
    /// Set by a spawn task whose connections failed; consumed by the writer's
    /// reconcile, which backs off further spawn attempts for a cooldown.
    spawn_failed: AtomicBool,
}

impl PopulationPool {
    pub fn new(initial_workers: usize) -> Self {
        Self {
            enqueued: AtomicU64::new(0),
            task_us: AtomicU64::new(0),
            task_count: AtomicU64::new(0),
            wait_us: AtomicU64::new(0),
            wait_count: AtomicU64::new(0),
            desired_workers: AtomicUsize::new(initial_workers),
            live_workers: AtomicUsize::new(0),
            free_ids: Mutex::new(Vec::new()),
            next_id: AtomicUsize::new(0),
            spawn_failed: AtomicBool::new(false),
        }
    }

    pub fn enqueued_mark(&self) {
        self.enqueued.fetch_add(1, Ordering::Relaxed);
    }

    pub fn task_observe(&self, micros: u64) {
        self.task_us.fetch_add(micros, Ordering::Relaxed);
        self.task_count.fetch_add(1, Ordering::Relaxed);
    }

    pub fn wait_observe(&self, micros: u64) {
        self.wait_us.fetch_add(micros, Ordering::Relaxed);
        self.wait_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Controller-side snapshot of the monotonic counters:
    /// `(enqueued, task_us, task_count, wait_us, wait_count)`.
    pub fn counters(&self) -> (u64, u64, u64, u64, u64) {
        (
            self.enqueued.load(Ordering::Relaxed),
            self.task_us.load(Ordering::Relaxed),
            self.task_count.load(Ordering::Relaxed),
            self.wait_us.load(Ordering::Relaxed),
            self.wait_count.load(Ordering::Relaxed),
        )
    }

    pub fn desired_workers(&self) -> usize {
        self.desired_workers.load(Ordering::Relaxed)
    }

    pub fn desired_workers_set(&self, n: usize) {
        self.desired_workers.store(n, Ordering::Relaxed);
    }

    pub fn live_workers(&self) -> usize {
        self.live_workers.load(Ordering::Relaxed)
    }

    /// Reserve one worker slot ahead of a spawn. The caller must call
    /// [`worker_exit`](Self::worker_exit) if the spawn fails, exactly as a
    /// running worker does when it stops.
    pub fn worker_reserve(&self) -> usize {
        self.live_workers.fetch_add(1, Ordering::Relaxed);
        let reused = {
            let mut free = self
                .free_ids
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            free.pop()
        };
        reused.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    pub fn spawn_failure_mark(&self) {
        self.spawn_failed.store(true, Ordering::Relaxed);
    }

    /// True once per recorded spawn failure (consume-on-read).
    pub fn spawn_failure_take(&self) -> bool {
        self.spawn_failed.swap(false, Ordering::Relaxed)
    }

    /// A worker (or failed spawn) leaves the pool; its id becomes reusable.
    pub fn worker_exit(&self, id: usize) {
        self.live_workers.fetch_sub(1, Ordering::Relaxed);
        let mut free = self
            .free_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        free.push(id);
    }

    /// A surplus worker retires itself: atomically claim one unit of the gap
    /// between live and desired. Returns true when the calling worker should
    /// exit (the caller then also calls [`worker_exit`](Self::worker_exit)).
    pub fn retire_claim(&self) -> bool {
        let mut live = self.live_workers.load(Ordering::Relaxed);
        loop {
            if live <= self.desired_workers.load(Ordering::Relaxed) {
                return false;
            }
            match self.live_workers.compare_exchange_weak(
                live,
                live - 1,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(actual) => live = actual,
            }
        }
    }

    /// Return a retiring worker's id after a successful
    /// [`retire_claim`](Self::retire_claim) (the live count is already
    /// decremented, so plain `worker_exit` would double-count).
    pub fn retired_id_return(&self, id: usize) {
        let mut free = self
            .free_ids
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        free.push(id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_worker_ids_reused_after_exit() {
        let pool = PopulationPool::new(2);
        let a = pool.worker_reserve();
        let b = pool.worker_reserve();
        assert_eq!((a, b), (0, 1));
        pool.worker_exit(a);
        assert_eq!(pool.live_workers(), 1);
        assert_eq!(pool.worker_reserve(), 0);
    }

    #[test]
    fn test_retire_claim_only_above_desired() {
        let pool = PopulationPool::new(2);
        let a = pool.worker_reserve();
        let _b = pool.worker_reserve();
        assert!(!pool.retire_claim());

        let _c = pool.worker_reserve();
        assert!(pool.retire_claim());
        pool.retired_id_return(a);
        assert_eq!(pool.live_workers(), 2);
        assert!(!pool.retire_claim());
    }

    #[test]
    fn test_counters_accumulate() {
        let pool = PopulationPool::new(2);
        pool.enqueued_mark();
        pool.enqueued_mark();
        pool.task_observe(1_000);
        pool.wait_observe(250);
        assert_eq!(pool.counters(), (2, 1_000, 1, 250, 1));
    }
}
