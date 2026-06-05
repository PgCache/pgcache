//! Per-query timing instrumentation for tracking query latency across processing stages.
//!
//! This module provides timing infrastructure to measure how long queries spend in each
//! stage of processing. It is only active when the `query-timing` feature is enabled.

use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, Instant},
};

/// Nanoseconds between two instants, saturating at `u64::MAX`.
///
/// `Duration::as_nanos()` returns `u128`; truncating to `u64` covers ~584 years of
/// elapsed time, which is unreachable for `Instant` measurements in practice.
fn instant_diff_ns(later: Instant, earlier: Instant) -> u64 {
    duration_to_ns_u64(later.duration_since(earlier))
}

/// `Duration` expressed as `u64` nanoseconds, saturating at `u64::MAX`.
pub(crate) fn duration_to_ns_u64(d: Duration) -> u64 {
    u64::try_from(d.as_nanos()).unwrap_or(u64::MAX)
}

/// `Duration` expressed as `u64` microseconds, saturating at `u64::MAX`.
pub(crate) fn duration_to_us_u64(d: Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// Global sequence counter for generating unique query instance IDs.
static QUERY_SEQUENCE: AtomicU64 = AtomicU64::new(1);

/// Unique identifier for a query instance.
///
/// Combines the query fingerprint (identifies the query pattern) with a sequence number
/// (identifies the specific execution instance). This allows comparing timing of the
/// same query across different executions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QueryId {
    /// Query pattern fingerprint (hash of the SQL text or AST)
    pub fingerprint: u64,
    /// Unique sequence number for this execution instance
    pub sequence: u64,
}

impl QueryId {
    /// Create a new QueryId with the given fingerprint and a unique sequence number.
    pub fn new(fingerprint: u64) -> Self {
        Self {
            fingerprint,
            sequence: QUERY_SEQUENCE.fetch_add(1, Ordering::Relaxed),
        }
    }
}

/// Query processing outcome for timing classification.
#[derive(Debug, Clone, Copy)]
pub enum QueryOutcome {
    /// Query was served from cache
    CacheHit,
    /// Query was not in cache, forwarded to origin
    CacheMiss,
    /// Query was forwarded to origin (not cacheable or other reason)
    Forward,
    /// Query execution failed
    Error,
}

/// Computed durations for each processing stage (in nanoseconds for precision).
#[derive(Debug, Default)]
pub struct QueryDurations {
    /// Time spent parsing the query
    pub parse_ns: Option<u64>,
    /// Time from parsed to dispatched to cache
    pub dispatch_ns: Option<u64>,
    /// Time spent in cache lookup
    pub lookup_ns: Option<u64>,
    /// Time waiting in the worker channel queue (lookup_complete → worker_received)
    pub queue_wait_ns: Option<u64>,
    /// Time waiting for a cache database connection from the pool (worker_received → conn_acquired)
    pub conn_wait_ns: Option<u64>,
    /// Time from connection acquired to worker task start (conn_acquired → worker_start)
    pub spawn_wait_ns: Option<u64>,
    /// Time spent executing query in cache worker
    pub worker_execution_ns: Option<u64>,
    /// Time spent writing response to client
    pub response_write_ns: Option<u64>,
    /// Forward path: time from cache dispatch to bytes-on-wire-to-origin
    /// (dispatched_at → forwarded_at). Captures cache-thread decision time
    /// plus the channel hop back to the proxy.
    pub forward_decision_ns: Option<u64>,
    /// Coalesce path: cache thread book-keeping into the waiting list
    /// (lookup_complete_at → waiter_enqueued_at).
    pub coalesce_intake_ns: Option<u64>,
    /// Coalesce path: the actual wait — from joining the waiting list until
    /// the drain task picked up the group (waiter_enqueued_at →
    /// drain_started_at). Dominated by the population pipeline + writer's
    /// Ready notify + dispatch drain pickup.
    pub coalesce_wait_ns: Option<u64>,
    /// Total time from reception to completion
    pub total_ns: Option<u64>,
}

/// Per-stage timing data for a single query instance.
///
/// All timestamp fields are Option to allow incremental capture as the query
/// progresses through the system. The `received_at` field is always set.
#[derive(Debug, Clone)]
pub struct QueryTiming {
    /// Unique identifier for this query instance
    pub query_id: QueryId,

    /// When the query message was received from the client
    pub received_at: Instant,

    /// When query parsing/fingerprinting completed
    pub parsed_at: Option<Instant>,

    /// When the query was dispatched to the cache channel
    pub dispatched_at: Option<Instant>,

    /// When cache state lookup completed (DashMap.get + state check). Stamped
    /// uniformly across all paths immediately after the lookup, before the
    /// match dispatches on the resulting state. `lookup_seconds` measures
    /// proxy dispatch → this point; consistently μs-scale across paths.
    pub lookup_complete_at: Option<Instant>,

    /// Coalesce path only: when the waiter was pushed onto the waiting map
    /// (in the Loading match arm of query_dispatch).
    pub waiter_enqueued_at: Option<Instant>,

    /// Coalesce path only: when `waiting_drain_ready` (or
    /// `waiting_drain_failed`) began processing this waiter's group.
    pub drain_started_at: Option<Instant>,

    /// When the worker thread received the message from the channel (cache hit path)
    pub worker_received_at: Option<Instant>,

    /// When a cache database connection was acquired from the pool (cache hit path)
    pub conn_acquired_at: Option<Instant>,

    /// When the cache worker started processing (cache hit path)
    pub worker_start_at: Option<Instant>,

    /// When the cache database query completed (cache hit path)
    pub query_done_at: Option<Instant>,

    /// When the response was fully written to the client (cache hit path)
    pub response_written_at: Option<Instant>,

    /// When the query was forwarded to origin (cache miss/forward path)
    pub forwarded_at: Option<Instant>,

    /// When the origin response completed (forward path)
    pub origin_response_at: Option<Instant>,

    /// Final outcome of the query
    pub outcome: Option<QueryOutcome>,
}

impl QueryTiming {
    /// Create a new QueryTiming with the given ID and current time as received_at.
    pub fn new(query_id: QueryId, received_at: Instant) -> Self {
        Self {
            query_id,
            received_at,
            parsed_at: None,
            dispatched_at: None,
            lookup_complete_at: None,
            waiter_enqueued_at: None,
            drain_started_at: None,
            worker_received_at: None,
            conn_acquired_at: None,
            worker_start_at: None,
            query_done_at: None,
            response_written_at: None,
            forwarded_at: None,
            origin_response_at: None,
            outcome: None,
        }
    }

    /// Calculate per-stage durations from the captured timestamps.
    pub fn durations(&self) -> QueryDurations {
        QueryDurations {
            parse_ns: self.parsed_at.map(|t| instant_diff_ns(t, self.received_at)),
            dispatch_ns: self
                .dispatched_at
                .and_then(|d| self.parsed_at.map(|p| instant_diff_ns(d, p))),
            lookup_ns: self
                .lookup_complete_at
                .and_then(|l| self.dispatched_at.map(|d| instant_diff_ns(l, d))),
            queue_wait_ns: self
                .worker_received_at
                .and_then(|r| self.lookup_complete_at.map(|l| instant_diff_ns(r, l))),
            conn_wait_ns: self
                .conn_acquired_at
                .and_then(|c| self.worker_received_at.map(|r| instant_diff_ns(c, r))),
            spawn_wait_ns: self
                .worker_start_at
                .and_then(|w| self.conn_acquired_at.map(|c| instant_diff_ns(w, c))),
            worker_execution_ns: self
                .query_done_at
                .and_then(|e| self.worker_start_at.map(|s| instant_diff_ns(e, s))),
            response_write_ns: self.response_written_at.and_then(|r| {
                self.query_done_at
                    .or(self.origin_response_at)
                    .map(|e| instant_diff_ns(r, e))
            }),
            forward_decision_ns: self
                .forwarded_at
                .and_then(|f| self.dispatched_at.map(|d| instant_diff_ns(f, d))),
            coalesce_intake_ns: self
                .waiter_enqueued_at
                .and_then(|w| self.lookup_complete_at.map(|l| instant_diff_ns(w, l))),
            coalesce_wait_ns: self
                .drain_started_at
                .and_then(|d| self.waiter_enqueued_at.map(|w| instant_diff_ns(d, w))),
            total_ns: self
                .response_written_at
                .or(self.origin_response_at)
                .map(|end| instant_diff_ns(end, self.received_at)),
        }
    }
}

/// Record timing data to Prometheus histograms and optionally trace log.
///
/// Called when a query completes (either via cache hit or forward path).
//
// Nanosecond -> seconds conversions feed Prometheus histograms; precision loss past
// ~104 days of elapsed time is irrelevant for a per-query latency metric.
#[allow(clippy::cast_precision_loss)]
pub fn timing_record(timing: &QueryTiming) {
    let durations = timing.durations();
    let m = crate::metrics::handles();

    // Record to Prometheus histograms (converting ns to seconds)
    macro_rules! record_ns {
        ($field:expr, $handle:expr) => {
            if let Some(ns) = $field {
                $handle.record(ns as f64 / 1_000_000_000.0);
            }
        };
    }
    record_ns!(durations.parse_ns, m.stage.parse);
    record_ns!(durations.dispatch_ns, m.stage.dispatch);
    record_ns!(durations.lookup_ns, m.stage.lookup);
    record_ns!(durations.queue_wait_ns, m.stage.queue_wait);
    record_ns!(durations.conn_wait_ns, m.stage.conn_wait);
    record_ns!(durations.spawn_wait_ns, m.stage.spawn_wait);
    record_ns!(durations.worker_execution_ns, m.stage.worker_exec);
    record_ns!(durations.response_write_ns, m.stage.response_write);
    record_ns!(durations.forward_decision_ns, m.stage.forward_decision);
    record_ns!(durations.coalesce_intake_ns, m.stage.coalesce_intake);
    record_ns!(durations.coalesce_wait_ns, m.stage.coalesce_wait);
    record_ns!(durations.total_ns, m.stage.total);

    // Trace-level logging for detailed per-query debugging
    if tracing::enabled!(tracing::Level::TRACE) {
        tracing::trace!(
            fingerprint = timing.query_id.fingerprint,
            sequence = timing.query_id.sequence,
            outcome = ?timing.outcome,
            total_us = durations.total_ns.map(|n| n / 1000),
            parse_us = durations.parse_ns.map(|n| n / 1000),
            dispatch_us = durations.dispatch_ns.map(|n| n / 1000),
            lookup_us = durations.lookup_ns.map(|n| n / 1000),
            queue_wait_us = durations.queue_wait_ns.map(|n| n / 1000),
            conn_wait_us = durations.conn_wait_ns.map(|n| n / 1000),
            spawn_wait_us = durations.spawn_wait_ns.map(|n| n / 1000),
            worker_us = durations.worker_execution_ns.map(|n| n / 1000),
            write_us = durations.response_write_ns.map(|n| n / 1000),
            "query timing breakdown"
        );
    }
}
