#![allow(clippy::indexing_slicing)]

use std::io::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

use super::context::TestContext;

/// Point-in-time snapshot of metrics for test assertions.
/// Populated by parsing metrics from the Prometheus HTTP endpoint.
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
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
    pub cache_mv_hits: u64,
    pub cache_mv_fallthrough: u64,
    pub cache_mv_rebuilds: u64,
    pub cache_mv_skipped_rebuilds: u64,
    pub cache_mv_dirty_truncates: u64,
    pub cache_cdc_local_eval_hits: u64,
    pub cache_cdc_pg_eval_hits: u64,
    pub cache_hit_rate: f64,
    pub cacheability_rate: f64,
}

/// Fetch metrics via HTTP from the Prometheus endpoint.
pub async fn metrics_http_get(port: u16) -> Result<MetricsSnapshot, Error> {
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

/// Parse Prometheus text format into MetricsSnapshot.
fn metrics_prometheus_parse(response: &str) -> Result<MetricsSnapshot, Error> {
    let mut queries_total = 0u64;
    let mut queries_cacheable = 0u64;
    let mut queries_uncacheable = 0u64;
    let mut queries_unsupported = 0u64;
    let mut queries_invalid = 0u64;
    let mut queries_cache_hit = 0u64;
    let mut queries_cache_miss = 0u64;
    let mut queries_cache_error = 0u64;
    let mut queries_allowlist_skipped = 0u64;
    let mut cache_subsumptions = 0u64;
    let mut cache_invalidations = 0u64;
    let mut cache_readmissions = 0u64;
    let mut cache_coalesce_served = 0u64;
    let mut cache_mv_hits = 0u64;
    let mut cache_mv_fallthrough = 0u64;
    let mut cache_mv_rebuilds = 0u64;
    let mut cache_mv_skipped_rebuilds = 0u64;
    let mut cache_mv_dirty_truncates = 0u64;
    let mut cache_cdc_local_eval_hits = 0u64;
    let mut cache_cdc_pg_eval_hits = 0u64;

    for line in response.lines() {
        // Skip comments and empty lines
        if line.starts_with('#') || line.is_empty() {
            continue;
        }

        // Parse "metric_name value" format
        let parts: Vec<&str> = line.split_whitespace().collect();
        if parts.len() >= 2 {
            let name = parts[0];
            let value: u64 = parts[1].parse().unwrap_or(0);

            match name {
                "pgcache_queries_total" => queries_total = value,
                "pgcache_queries_cacheable" => queries_cacheable = value,
                "pgcache_queries_uncacheable" => queries_uncacheable = value,
                "pgcache_queries_unsupported" => queries_unsupported = value,
                "pgcache_queries_invalid" => queries_invalid = value,
                "pgcache_queries_cache_hit" => queries_cache_hit = value,
                "pgcache_queries_cache_miss" => queries_cache_miss = value,
                "pgcache_queries_cache_error" => queries_cache_error = value,
                "pgcache_queries_allowlist_skipped" => queries_allowlist_skipped = value,
                "pgcache_cache_subsumptions" => cache_subsumptions = value,
                "pgcache_cache_invalidations" => cache_invalidations = value,
                "pgcache_cache_readmissions" => cache_readmissions = value,
                "pgcache_cache_coalesce_served" => cache_coalesce_served = value,
                "pgcache_cache_mv_hits" => cache_mv_hits = value,
                "pgcache_cache_mv_fallthrough" => cache_mv_fallthrough = value,
                "pgcache_cache_mv_rebuilds" => cache_mv_rebuilds = value,
                "pgcache_cache_mv_skipped_rebuilds" => cache_mv_skipped_rebuilds = value,
                "pgcache_cache_mv_dirty_truncates" => cache_mv_dirty_truncates = value,
                "pgcache_cache_cdc_local_eval_hits" => cache_cdc_local_eval_hits = value,
                "pgcache_cache_cdc_pg_eval_hits" => cache_cdc_pg_eval_hits = value,
                _ => {}
            }
        }
    }

    // Test telemetry rates; query counts in tests never approach 2^53.
    #[allow(clippy::cast_precision_loss)]
    let cache_hit_rate = if queries_cacheable > 0 {
        (queries_cache_hit as f64 / queries_cacheable as f64) * 100.0
    } else {
        0.0
    };

    let queries_select = queries_total
        .saturating_sub(queries_unsupported)
        .saturating_sub(queries_invalid);
    #[allow(clippy::cast_precision_loss)]
    let cacheability_rate = if queries_select > 0 {
        (queries_cacheable as f64 / queries_select as f64) * 100.0
    } else {
        0.0
    };

    Ok(MetricsSnapshot {
        queries_total,
        queries_cacheable,
        queries_uncacheable,
        queries_unsupported,
        queries_invalid,
        queries_cache_hit,
        queries_cache_miss,
        queries_cache_error,
        queries_allowlist_skipped,
        cache_subsumptions,
        cache_invalidations,
        cache_readmissions,
        cache_coalesce_served,
        cache_mv_hits,
        cache_mv_fallthrough,
        cache_mv_rebuilds,
        cache_mv_skipped_rebuilds,
        cache_mv_dirty_truncates,
        cache_cdc_local_eval_hits,
        cache_cdc_pg_eval_hits,
        cache_hit_rate,
        cacheability_rate,
    })
}

/// Calculate metrics delta between two snapshots.
/// Useful for asserting metrics within a consolidated test where metrics accumulate.
pub fn metrics_delta(before: &MetricsSnapshot, after: &MetricsSnapshot) -> MetricsSnapshot {
    MetricsSnapshot {
        queries_total: after.queries_total - before.queries_total,
        queries_cacheable: after.queries_cacheable - before.queries_cacheable,
        queries_uncacheable: after.queries_uncacheable - before.queries_uncacheable,
        queries_unsupported: after.queries_unsupported - before.queries_unsupported,
        queries_invalid: after.queries_invalid - before.queries_invalid,
        queries_cache_hit: after.queries_cache_hit - before.queries_cache_hit,
        queries_cache_miss: after.queries_cache_miss - before.queries_cache_miss,
        queries_cache_error: after.queries_cache_error - before.queries_cache_error,
        queries_allowlist_skipped: after.queries_allowlist_skipped
            - before.queries_allowlist_skipped,
        cache_subsumptions: after.cache_subsumptions - before.cache_subsumptions,
        cache_invalidations: after.cache_invalidations - before.cache_invalidations,
        cache_readmissions: after.cache_readmissions - before.cache_readmissions,
        cache_coalesce_served: after.cache_coalesce_served - before.cache_coalesce_served,
        cache_mv_hits: after.cache_mv_hits - before.cache_mv_hits,
        cache_mv_fallthrough: after.cache_mv_fallthrough - before.cache_mv_fallthrough,
        cache_mv_rebuilds: after.cache_mv_rebuilds - before.cache_mv_rebuilds,
        cache_mv_skipped_rebuilds: after.cache_mv_skipped_rebuilds
            - before.cache_mv_skipped_rebuilds,
        cache_mv_dirty_truncates: after.cache_mv_dirty_truncates - before.cache_mv_dirty_truncates,
        cache_cdc_local_eval_hits: after.cache_cdc_local_eval_hits
            - before.cache_cdc_local_eval_hits,
        cache_cdc_pg_eval_hits: after.cache_cdc_pg_eval_hits - before.cache_cdc_pg_eval_hits,
        // Rates are cumulative averages, not meaningful for deltas
        cache_hit_rate: 0.0,
        cacheability_rate: 0.0,
    }
}

/// Assert the last cacheable query was a cache miss. Returns updated snapshot.
pub async fn assert_cache_miss(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_miss, 1, "expected cache miss");
    assert_eq!(delta.queries_cache_hit, 0, "unexpected cache hit");
    Ok(after)
}

/// Assert the last cacheable query was a cache hit. Returns updated snapshot.
pub async fn assert_cache_hit(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.queries_cache_hit, 1, "expected cache hit");
    assert_eq!(delta.queries_cache_miss, 0, "unexpected cache miss");
    Ok(after)
}

/// Assert that the last query was subsumed (cache hit via subsumption).
/// Returns updated metrics snapshot for chaining.
pub async fn assert_subsume_hit(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
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
pub async fn assert_not_subsumed(
    ctx: &mut TestContext,
    before: MetricsSnapshot,
) -> Result<MetricsSnapshot, Error> {
    let after = ctx.metrics().await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(delta.cache_subsumptions, 0, "unexpected subsumption");
    Ok(after)
}
