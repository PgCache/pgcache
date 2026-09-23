use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use tokio::sync::Notify;

/// How long a parked worker holds its connection pair before exiting for
/// real: long enough to absorb the controller's probe cycles at the
/// hold-ladder cap, short enough that an origin connection the workload
/// stopped needing is released (ADR-050's no-idle-connections intent —
/// bounded to exactly one pair, briefly).
pub const POPULATION_PARK_EXPIRY: Duration = Duration::from_secs(300);

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
    /// At most one retired worker parks with its connection pair instead of
    /// exiting, so a probe cycle at a capacity ceiling reuses it rather than
    /// tearing down and re-dialing origin (ADR-052/053 damping).
    parked: AtomicUsize,
    /// Unpark grant from the reconcile, consumed by the parked worker; also
    /// the reconcile's pending-live count so a grant in flight isn't
    /// double-filled by a spawn.
    unpark_credits: AtomicUsize,
    unpark_notify: Notify,
    /// Work-queue depth hint, maintained by the dispatcher; the controller's
    /// saturated-vs-idle discriminator for zero-observation ticks (PGC-452).
    queue_depth: AtomicUsize,
    /// Reserved worker slots whose connections are still being opened.
    /// `live_workers` includes them (the reconcile must not double-spawn), but
    /// the controller's live sample excludes them so a probe's verify waits
    /// for the worker to actually materialize instead of judging one that
    /// never ran (PGC-456).
    pending_connects: AtomicUsize,
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
            parked: AtomicUsize::new(0),
            unpark_credits: AtomicUsize::new(0),
            unpark_notify: Notify::new(),
            queue_depth: AtomicUsize::new(0),
            pending_connects: AtomicUsize::new(0),
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

    pub fn queue_depth_set(&self, n: usize) {
        self.queue_depth.store(n, Ordering::Relaxed);
    }

    pub fn queue_depth(&self) -> usize {
        self.queue_depth.load(Ordering::Relaxed)
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
        self.pending_connects.fetch_add(1, Ordering::Relaxed);
        let reused = {
            let mut free = self
                .free_ids
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            free.pop()
        };
        reused.unwrap_or_else(|| self.next_id.fetch_add(1, Ordering::Relaxed))
    }

    /// A reserved worker's connect resolved (successfully into a running
    /// worker, or into a failed spawn) — either way it is no longer pending.
    pub fn pending_connect_done(&self) {
        let _ = self
            .pending_connects
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |v| {
                Some(v.saturating_sub(1))
            });
    }

    pub fn pending_connects(&self) -> usize {
        self.pending_connects.load(Ordering::Relaxed)
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

    /// A retiring worker claims the single park slot (keeping its id and
    /// connections). Returns false when the slot is taken — the worker then
    /// exits for real.
    pub fn park_claim(&self) -> bool {
        self.parked
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// A parked worker leaves the slot without resuming (expiry). Any grant
    /// that raced in is swept by the caller via
    /// [`unpark_credit_take`](Self::unpark_credit_take) *after* this, so no
    /// stranded credit under-counts the reconcile's pending-live forever.
    pub fn park_release(&self) {
        self.parked.store(0, Ordering::Relaxed);
    }

    /// Reconcile-side: grant an unpark to a parked worker. Returns true only
    /// when a *fresh* grant was issued this call — a grant already in flight
    /// returns false so the reconcile falls through to spawning for the rest
    /// of the deficit instead of spinning (the in-flight grant is counted via
    /// [`unpark_pending`](Self::unpark_pending)).
    pub fn unpark_request(&self) -> bool {
        if self.parked.load(Ordering::Relaxed) == 0 {
            return false;
        }
        let granted = self
            .unpark_credits
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok();
        if granted {
            self.unpark_notify.notify_one();
        }
        granted
    }

    /// Unpark grants outstanding — the reconcile counts these as pending
    /// live workers.
    pub fn unpark_pending(&self) -> usize {
        self.unpark_credits.load(Ordering::Relaxed)
    }

    /// Parked-worker side: consume the grant, if any.
    pub fn unpark_credit_take(&self) -> bool {
        self.unpark_credits
            .compare_exchange(1, 0, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
    }

    /// A parked worker resumes: leaves the park slot and rejoins the live set.
    pub fn unpark_complete(&self) {
        self.parked.store(0, Ordering::Relaxed);
        self.live_workers.fetch_add(1, Ordering::Relaxed);
    }

    /// Wait for an unpark grant (parked-worker side).
    pub async fn unpark_notified(&self) {
        self.unpark_notify.notified().await;
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
    fn test_park_slot_is_single() {
        let pool = PopulationPool::new(2);
        assert!(pool.park_claim());
        assert!(!pool.park_claim());
        pool.park_release();
        assert!(pool.park_claim());
    }

    #[test]
    fn test_unpark_grant_flow_restores_live() {
        let pool = PopulationPool::new(2);
        let _a = pool.worker_reserve();
        let _b = pool.worker_reserve();
        let _c = pool.worker_reserve();
        // Surplus worker retires into the park slot: live drops to desired.
        assert!(pool.retire_claim());
        assert!(pool.park_claim());
        assert_eq!(pool.live_workers(), 2);
        // Grant covers one deficit unit until consumed...
        assert!(pool.unpark_request());
        assert_eq!(pool.unpark_pending(), 1);
        // ...and a second request is not a fresh grant (the reconcile falls
        // through to spawning) while pending still counts the first.
        assert!(!pool.unpark_request());
        assert_eq!(pool.unpark_pending(), 1);
        // The parked worker consumes it and rejoins.
        assert!(pool.unpark_credit_take());
        pool.unpark_complete();
        assert_eq!(pool.live_workers(), 3);
        assert_eq!(pool.unpark_pending(), 0);
        assert!(!pool.unpark_request(), "no parked worker left to grant to");
    }

    #[test]
    fn test_expiry_sweeps_stranded_credit() {
        let pool = PopulationPool::new(2);
        assert!(pool.park_claim());
        assert!(pool.unpark_request());
        // Worker expires just as the grant lands: release, then sweep.
        pool.park_release();
        assert!(pool.unpark_credit_take());
        assert_eq!(pool.unpark_pending(), 0);
        assert!(!pool.unpark_request());
    }

    /// PGC-456: a reserved worker is pending until its connect resolves —
    /// the controller's live sample excludes it, the reconcile's includes it.
    #[test]
    fn test_pending_connects_track_reserve_to_resolution() {
        let pool = PopulationPool::new(2);
        let a = pool.worker_reserve();
        assert_eq!((pool.live_workers(), pool.pending_connects()), (1, 1));
        // Connect succeeded: the worker's run entry consumes the pending.
        pool.pending_connect_done();
        assert_eq!((pool.live_workers(), pool.pending_connects()), (1, 0));
        // Connect failure consumes pending and releases the slot.
        let _b = pool.worker_reserve();
        pool.pending_connect_done();
        pool.worker_exit(a);
        assert_eq!((pool.live_workers(), pool.pending_connects()), (1, 0));
        // Saturating: a stray extra done never wraps.
        pool.pending_connect_done();
        assert_eq!(pool.pending_connects(), 0);
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
