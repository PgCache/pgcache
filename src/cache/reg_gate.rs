use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Shared state for the BBR-lite adaptive registration gate (PGC-277).
///
/// Model (à la TCP BBR): the writer's *drain rate* (registrations reaching Ready
/// per second) is the bottleneck "bandwidth"; the writer *backlog depth* is the
/// "queue". The controller max-filters the drain rate to estimate writer capacity
/// and paces the admit rate (`reg_rate`) at it, using the min-filtered backlog as
/// the standing-queue signal. There is deliberately no upper bound on `reg_rate`:
/// when the writer keeps up it rises freely (effectively "no limit").
///
/// - Writer publishes: `completed` (monotonic Ready count) and the per-iteration
///   backlog window via `queue_observe`.
/// - Controller writes: `reg_rate`.
/// - Dispatch token bucket reads: `reg_rate`.
pub struct RegGate {
    /// Admit rate (registrations/sec) the token bucket refills at — f64 in an
    /// AtomicU64. `INFINITY` means "no gate yet" (admit all); the controller
    /// replaces it with a finite paced rate once it has signal.
    reg_rate_bits: AtomicU64,
    /// Monotonic count of registrations that reached Ready. The controller's
    /// drain-rate (capacity) estimate is `Δcompleted / Δt`.
    completed: AtomicU64,
    /// Writer backlog window min since the last controller reset. `~0` ⇒ the
    /// writer drained (healthy); `> floor` ⇒ a standing queue (saturated).
    queue_min: AtomicUsize,
    /// Writer backlog window max since reset. `0` ⇒ no registration load this
    /// window (the controller holds `reg_rate` rather than probing into a void).
    queue_max: AtomicUsize,
    /// Population in-flight: queries in `Loading` (admitted, populating, not yet
    /// Ready). Published by the writer's gauge tick from the authoritative state
    /// scan (so it never drifts). The second congestion signal — the writer's
    /// command queue (`queue_min`) catches writer-stage congestion; this catches
    /// population-stage congestion (`spawn_local` origin SELECTs), which on a
    /// remote origin is the binding constraint the command queue is blind to.
    loading: AtomicUsize,
    /// Monotonic count of registrations the token bucket *denied* (shed to
    /// origin). The controller probes the rate up only when this advanced — i.e.
    /// demand is bumping against the rate — so a partly-warm cache (low miss/
    /// registration load) can't drift the rate up into a low-demand void.
    denied: AtomicU64,
}

impl RegGate {
    pub fn new() -> Self {
        Self {
            reg_rate_bits: AtomicU64::new(f64::INFINITY.to_bits()),
            completed: AtomicU64::new(0),
            queue_min: AtomicUsize::new(usize::MAX),
            queue_max: AtomicUsize::new(0),
            loading: AtomicUsize::new(0),
            denied: AtomicU64::new(0),
        }
    }

    /// Current admit rate (registrations/sec). `INFINITY` ⇒ ungated.
    pub fn rate(&self) -> f64 {
        f64::from_bits(self.reg_rate_bits.load(Ordering::Relaxed))
    }

    /// Controller: set the paced admit rate.
    pub fn rate_set(&self, rate: f64) {
        self.reg_rate_bits.store(rate.to_bits(), Ordering::Relaxed);
    }

    /// Writer: a registration reached Ready (one unit of drained work).
    pub fn completed_inc(&self) {
        self.completed.fetch_add(1, Ordering::Relaxed);
    }

    /// Controller: monotonic completed count, for the drain-rate delta.
    pub fn completed_count(&self) -> u64 {
        self.completed.load(Ordering::Relaxed)
    }

    /// Writer: fold the current backlog depth into this window's min/max.
    pub fn queue_observe(&self, depth: usize) {
        self.queue_min.fetch_min(depth, Ordering::Relaxed);
        self.queue_max.fetch_max(depth, Ordering::Relaxed);
    }

    /// Controller: read and reset the backlog window. Returns `(min, max)`;
    /// `min` is `0` when the window saw an empty backlog (or saw no samples).
    pub fn window_take(&self) -> (usize, usize) {
        let max = self.queue_max.swap(0, Ordering::Relaxed);
        let min = self.queue_min.swap(usize::MAX, Ordering::Relaxed);
        (if min == usize::MAX { 0 } else { min }, max)
    }

    /// Writer gauge tick: publish the authoritative `Loading` count (population
    /// in-flight) from the state scan.
    pub fn loading_set(&self, count: usize) {
        self.loading.store(count, Ordering::Relaxed);
    }

    /// Controller: current population in-flight (queries still populating).
    pub fn loading_get(&self) -> usize {
        self.loading.load(Ordering::Relaxed)
    }

    /// Dispatch: the token bucket denied a registration (shed to origin).
    pub fn denied_inc(&self) {
        self.denied.fetch_add(1, Ordering::Relaxed);
    }

    /// Controller: monotonic denied count, for the per-window shed delta.
    pub fn denied_count(&self) -> u64 {
        self.denied.load(Ordering::Relaxed)
    }
}

impl Default for RegGate {
    fn default() -> Self {
        Self::new()
    }
}
