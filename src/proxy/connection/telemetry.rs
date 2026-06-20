use std::time::Instant;

use crate::{
    query::Fingerprint,
    timing::{QueryId, QueryTiming, timing_record},
};

/// Timing instrumentation for the current query in flight.
/// Tracks timestamps for metrics recording across the origin and cache paths.
pub(in crate::proxy::connection) struct QueryTelemetry {
    /// When the client message arrived — measures end-to-end latency for both
    /// cache hits (CACHE_QUERY_LATENCY) and origin queries (ORIGIN_QUERY_LATENCY)
    pub(in crate::proxy::connection) client_received_at: Option<Instant>,

    /// When the query was forwarded to origin — measures origin-only execution
    /// time (ORIGIN_EXECUTION), excluding parse and cacheability-check overhead
    pub(in crate::proxy::connection) origin_sent_at: Option<Instant>,

    /// Per-stage timing breakdown that travels with the query through the cache
    /// pipeline (dispatch → worker) and back, only set for cache-path queries
    pub(in crate::proxy::connection) cache_timing: Option<QueryTiming>,
}

impl QueryTelemetry {
    pub(in crate::proxy::connection) fn new() -> Self {
        Self {
            client_received_at: None,
            origin_sent_at: None,
            cache_timing: None,
        }
    }

    /// Record that a client message was received.
    pub(in crate::proxy::connection) fn query_receive(&mut self) {
        self.client_received_at = Some(Instant::now());
    }

    /// Record that the query was forwarded to origin. Pass the QueryTiming
    /// returned by the cache thread to also stamp `forwarded_at` and retain
    /// it for per-stage histogram emission on completion.
    pub(in crate::proxy::connection) fn origin_forward(&mut self, timing: Option<QueryTiming>) {
        let now = Instant::now();
        self.origin_sent_at = Some(now);
        if let Some(mut t) = timing {
            t.forwarded_at = Some(now);
            self.cache_timing = Some(t);
        }
    }

    /// Create cache timing for a cacheable query.
    pub(in crate::proxy::connection) fn cache_timing_start(&mut self, fingerprint: Fingerprint) {
        let query_id = QueryId::new(fingerprint.get());
        let received_at = self.client_received_at.unwrap_or_else(Instant::now);
        let mut timing = QueryTiming::new(query_id, received_at);
        timing.parsed_at = Some(Instant::now());
        self.cache_timing = Some(timing);
    }

    /// Record origin query completion. Records ORIGIN_EXECUTION_SECONDS and
    /// ORIGIN_QUERY_LATENCY_SECONDS, and — when forward-path timing was
    /// threaded back from the cache thread — records the per-stage breakdown
    /// via `timing_record`.
    pub(in crate::proxy::connection) fn origin_complete(&mut self) {
        let now = Instant::now();
        let m = crate::metrics::handles();
        if let Some(start) = self.origin_sent_at.take() {
            m.query
                .origin_execution
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(start) = self.client_received_at.take() {
            m.query.origin_latency.record(start.elapsed().as_secs_f64());
        }
        if let Some(mut timing) = self.cache_timing.take() {
            // Forward path: `response_written_at` is intentionally left None.
            // The actual client write happens later in the event loop; stamping
            // here would record a near-zero diff that would pollute the
            // `response_write_seconds` histogram with cache-hit values.
            // `total_ns` falls back to `origin_response_at` (see timing.rs).
            timing.origin_response_at = Some(now);
            timing_record(&timing);
        }
    }

    /// Record cache query completion. Records CACHE_QUERY_LATENCY_SECONDS
    /// and per-stage timing breakdown.
    pub(in crate::proxy::connection) fn cache_complete(
        &mut self,
        reply_timing: Option<QueryTiming>,
    ) {
        if let Some(start) = self.client_received_at.take() {
            crate::metrics::handles()
                .query
                .cache_latency
                .record(start.elapsed().as_secs_f64());
        }
        if let Some(timing) = reply_timing {
            timing_record(&timing);
        }
    }

    /// Take the cache timing for dispatch to the cache pipeline.
    /// Sets dispatched_at before returning.
    pub(in crate::proxy::connection) fn cache_timing_dispatch(&mut self) -> QueryTiming {
        let mut t = self
            .cache_timing
            .take()
            .unwrap_or_else(|| QueryTiming::new(QueryId::new(0), Instant::now()));
        t.dispatched_at = Some(Instant::now());
        t
    }
}
