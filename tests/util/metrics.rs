#![allow(clippy::indexing_slicing)]

use std::io::Error;
use std::time::{Duration, Instant};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::context::TestContext;

/// Point-in-time snapshot of metrics for test assertions.
/// Populated by parsing metrics from the Prometheus HTTP endpoint.
#[derive(Debug, Clone, Default)]
pub(crate) struct MetricsSnapshot {
    pub queries_total: u64,
    pub queries_cacheable: u64,
    pub queries_uncacheable: u64,
    pub queries_unsupported: u64,
    pub queries_invalid: u64,
    pub queries_cache_hit: u64,
    pub queries_cache_miss: u64,
    pub queries_cache_error: u64,
    pub queries_allowlist_skipped: u64,
    pub cache_subsumptions: u64,
    pub cache_invalidations: u64,
    pub cache_readmissions: u64,
    pub cache_coalesce_served: u64,
    /// Coalesced waiters forwarded to origin on the population deadline (PGC-335).
    pub cache_coalesce_deadline_forward: u64,
    /// Gauge: waiters currently parked in the coalesce queue (point-in-time,
    /// not a delta).
    pub cache_coalesce_waiting: u64,
    pub cache_restarts_total: u64,
    pub cache_pool_replenished: u64,
    pub cache_serve_desync_total: u64,
    pub cache_mv_hits: u64,
    pub cache_mv_fallthrough: u64,
    pub cache_mv_builds_suppressed: u64,
    pub cache_mv_rebuilds: u64,
    pub cache_mv_skipped_rebuilds: u64,
    pub cache_mv_dirty_truncates: u64,
    pub cache_memo_hits: u64,
    pub cache_memo_captures: u64,
    pub cache_serve_settings_applied: u64,
    pub cache_reference_only_forwards: u64,
    pub cache_memo_evictions: u64,
    pub cache_cdc_local_eval_hits: u64,
    pub cache_cdc_pg_eval_hits: u64,
    pub protocol_describe_cache_hits: u64,
    pub protocol_describe_cache_misses: u64,
    pub protocol_lazy_parse_forwarded: u64,
    pub protocol_close_local: u64,
    pub cache_hit_rate: f64,
    pub cacheability_rate: f64,
    /// Writes folded into per-connection read-after-write logs (PGC-124).
    pub raw_writes_recorded: u64,
    /// Commit-LSN probes injected to bound pending writes (PGC-124).
    pub raw_probes: u64,
    /// Pending-INSERT aggregates the gate proved a read disjoint from (PGC-124).
    pub raw_serve_disjoint_insert: u64,
    pub raw_serve_disjoint_delete: u64,
    pub raw_serve_disjoint_update: u64,
    /// Unchanged-toast repair outcomes (PGC-264 / PGC-464).
    pub cache_cdc_toast_repairs: u64,
    pub cache_cdc_toast_fallbacks: u64,
    pub cache_cdc_toast_stale_aborts: u64,
    /// In-transaction serving (PGC-387).
    pub txn_served: u64,
    pub txn_forward_failed: u64,
    pub txn_forward_isolation_unknown: u64,
    pub txn_forward_isolation_strict: u64,
    pub txn_forward_pending_write: u64,
    pub conn_settings_probes: u64,
}

/// Fetch metrics via HTTP from the Prometheus endpoint.
pub(crate) async fn metrics_http_get(port: u16) -> Result<MetricsSnapshot, Error> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{}", port))
        .await
        .map_err(Error::other)?;

    // Send HTTP GET request
    let request = "GET /metrics HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::other)?;

    // Read response
    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(Error::other)?;

    // Parse Prometheus text format
    metrics_prometheus_parse(&response)
}

/// A sample's Prometheus name and the snapshot field it fills.
struct Sample {
    name: &'static str,
    field: fn(&mut MetricsSnapshot) -> &mut u64,
}

macro_rules! samples {
    ($($name:literal => $field:ident,)*) => {
        &[$(Sample { name: $name, field: |snapshot| &mut snapshot.$field },)*]
    };
}

/// Every sample the snapshot records, by its exported name.
const SAMPLES: &[Sample] = samples! {
    "pgcache_queries_total" => queries_total,
    "pgcache_queries_cacheable" => queries_cacheable,
    "pgcache_queries_uncacheable" => queries_uncacheable,
    "pgcache_queries_unsupported" => queries_unsupported,
    "pgcache_raw_writes_recorded" => raw_writes_recorded,
    "pgcache_raw_probes" => raw_probes,
    "pgcache_raw_serve_disjoint_insert" => raw_serve_disjoint_insert,
    "pgcache_raw_serve_disjoint_delete" => raw_serve_disjoint_delete,
    "pgcache_raw_serve_disjoint_update" => raw_serve_disjoint_update,
    "pgcache_cache_cdc_toast_repairs" => cache_cdc_toast_repairs,
    "pgcache_cache_cdc_toast_fallbacks" => cache_cdc_toast_fallbacks,
    "pgcache_cache_cdc_toast_stale_aborts" => cache_cdc_toast_stale_aborts,
    "pgcache_txn_served" => txn_served,
    "pgcache_txn_forwards{reason=\"failed\"}" => txn_forward_failed,
    "pgcache_txn_forwards{reason=\"isolation_unknown\"}" => txn_forward_isolation_unknown,
    "pgcache_txn_forwards{reason=\"isolation_strict\"}" => txn_forward_isolation_strict,
    "pgcache_txn_forwards{reason=\"pending_write\"}" => txn_forward_pending_write,
    "pgcache_conn_settings_probes" => conn_settings_probes,
    "pgcache_queries_invalid" => queries_invalid,
    "pgcache_queries_cache_hit" => queries_cache_hit,
    "pgcache_queries_cache_miss" => queries_cache_miss,
    "pgcache_queries_cache_error" => queries_cache_error,
    "pgcache_queries_allowlist_skipped" => queries_allowlist_skipped,
    "pgcache_cache_subsumptions" => cache_subsumptions,
    "pgcache_cache_invalidations" => cache_invalidations,
    "pgcache_cache_readmissions" => cache_readmissions,
    "pgcache_cache_coalesce_served" => cache_coalesce_served,
    "pgcache_cache_coalesce_deadline_forward_total" => cache_coalesce_deadline_forward,
    "pgcache_cache_coalesce_waiting" => cache_coalesce_waiting,
    "pgcache_cache_restarts_total" => cache_restarts_total,
    "pgcache_cache_pool_replenished" => cache_pool_replenished,
    "pgcache_cache_serve_desync_total" => cache_serve_desync_total,
    "pgcache_cache_mv_hits" => cache_mv_hits,
    "pgcache_cache_mv_fallthrough" => cache_mv_fallthrough,
    "pgcache_cache_mv_builds_suppressed" => cache_mv_builds_suppressed,
    "pgcache_cache_mv_rebuilds" => cache_mv_rebuilds,
    "pgcache_cache_mv_skipped_rebuilds" => cache_mv_skipped_rebuilds,
    "pgcache_cache_mv_dirty_truncates" => cache_mv_dirty_truncates,
    "pgcache_cache_memo_hits" => cache_memo_hits,
    "pgcache_cache_memo_captures" => cache_memo_captures,
    "pgcache_cache_serve_settings_applied" => cache_serve_settings_applied,
    "pgcache_cache_reference_only_forward_total" => cache_reference_only_forwards,
    "pgcache_cache_memo_evictions" => cache_memo_evictions,
    "pgcache_cache_cdc_local_eval_hits" => cache_cdc_local_eval_hits,
    "pgcache_cache_cdc_pg_eval_hits" => cache_cdc_pg_eval_hits,
    "pgcache_protocol_describe_cache_hits" => protocol_describe_cache_hits,
    "pgcache_protocol_describe_cache_misses" => protocol_describe_cache_misses,
    "pgcache_protocol_lazy_parse_forwarded" => protocol_lazy_parse_forwarded,
    "pgcache_protocol_close_local" => protocol_close_local,
};

/// Parse Prometheus text format into MetricsSnapshot.
fn metrics_prometheus_parse(response: &str) -> Result<MetricsSnapshot, Error> {
    let mut snapshot = MetricsSnapshot::default();
    for line in response.lines() {
        let Some((name, value)) = sample_parse(line) else {
            continue;
        };
        if let Some(sample) = SAMPLES.iter().find(|sample| sample.name == name) {
            *(sample.field)(&mut snapshot) = value;
        }
    }
    snapshot.rates_derive();
    Ok(snapshot)
}

/// `name value` from one exposition line; `None` for comments, blank lines
/// and anything else. A gauge prints as `0`, `1` or `1.0`: the integer part
/// is the value.
fn sample_parse(line: &str) -> Option<(&str, u64)> {
    if line.starts_with('#') {
        return None;
    }
    let mut parts = line.split_whitespace();
    let name = parts.next()?;
    let whole = parts.next()?.split('.').next().unwrap_or_default();
    Some((name, whole.parse().unwrap_or(0)))
}

impl MetricsSnapshot {
    /// The hit and cacheability rates from the counts.
    fn rates_derive(&mut self) {
        // Test telemetry rates; query counts in tests never approach 2^53.
        #[allow(clippy::cast_precision_loss)]
        if self.queries_cacheable > 0 {
            self.cache_hit_rate =
                (self.queries_cache_hit as f64 / self.queries_cacheable as f64) * 100.0;
        }
        let queries_select = self
            .queries_total
            .saturating_sub(self.queries_unsupported)
            .saturating_sub(self.queries_invalid);
        #[allow(clippy::cast_precision_loss)]
        if queries_select > 0 {
            self.cacheability_rate =
                (self.queries_cacheable as f64 / queries_select as f64) * 100.0;
        }
    }
}

/// Calculate metrics delta between two snapshots.
/// Useful for asserting metrics within a consolidated test where metrics accumulate.
pub(crate) fn metrics_delta(before: &MetricsSnapshot, after: &MetricsSnapshot) -> MetricsSnapshot {
    let mut before = before.clone();
    let mut after = after.clone();
    let mut delta = MetricsSnapshot::default();
    for sample in SAMPLES {
        *(sample.field)(&mut delta) = *(sample.field)(&mut after) - *(sample.field)(&mut before);
    }
    // Gauge, not a counter: carry the current value rather than a delta.
    delta.cache_coalesce_waiting = after.cache_coalesce_waiting;
    // Rates are cumulative averages, not meaningful for deltas: left at zero.
    delta
}

/// How long [`metrics_after_outcomes`] waits for outcomes to be recorded.
const OUTCOME_WAIT: Duration = Duration::from_secs(2);
const OUTCOME_POLL: Duration = Duration::from_millis(10);

/// Cache hits, misses and errors recorded between two snapshots.
fn outcomes_recorded(before: &MetricsSnapshot, after: &MetricsSnapshot) -> u64 {
    (after.queries_cache_hit + after.queries_cache_miss + after.queries_cache_error)
        - (before.queries_cache_hit + before.queries_cache_miss + before.queries_cache_error)
}

/// Re-read metrics until `outcomes` cache outcomes (hit, miss or error) have
/// been recorded since `before`, or the wait runs out — the caller's
/// assertion then reports the shortfall. A hit is counted by the proxy only
/// after the serve worker has written the response to the client, so a read
/// straight after the response can precede it (PGC-469); misses and errors
/// are counted before the query is forwarded.
pub(crate) async fn metrics_after_outcomes(
    metrics_port: u16,
    before: &MetricsSnapshot,
    outcomes: u64,
) -> Result<MetricsSnapshot, Error> {
    let deadline = Instant::now() + OUTCOME_WAIT;
    loop {
        let after = metrics_http_get(metrics_port).await?;
        if outcomes_recorded(before, &after) >= outcomes || Instant::now() >= deadline {
            return Ok(after);
        }
        tokio::time::sleep(OUTCOME_POLL).await;
    }
}

/// Assert the last cacheable query was a cache miss. Returns updated snapshot.
pub(crate) async fn assert_cache_miss(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_miss, 1, "expected cache miss");
    assert_eq!(delta.queries_cache_hit, 0, "unexpected cache hit");
    Ok(after)
}

/// Assert the last cacheable query was a cache hit. Returns updated snapshot.
pub(crate) async fn assert_cache_hit(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1, "expected cache hit");
    assert_eq!(delta.queries_cache_miss, 0, "unexpected cache miss");
    Ok(after)
}

/// Assert that the last query was subsumed (cache hit via subsumption).
/// Returns updated metrics snapshot for chaining.
pub(crate) async fn assert_subsume_hit(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "expected cache hit from subsumption"
    );
    assert_eq!(delta.queries_cache_miss, 0, "unexpected cache miss");
    assert_eq!(delta.cache_subsumptions, 1, "expected subsumption");
    Ok(after)
}

/// Assert that the last query was NOT subsumed.
/// Returns updated metrics snapshot for chaining.
pub(crate) async fn assert_not_subsumed(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.cache_subsumptions, 0, "unexpected subsumption");
    Ok(after)
}
