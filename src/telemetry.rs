use std::sync::OnceLock;
use std::time::{Duration, Instant};

use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tokio_util::sync::CancellationToken;
use tracing::{info, trace};
use uuid::Uuid;

use crate::metrics::names;

/// Default telemetry endpoint. Override with PGCACHE_TELEMETRY_ENDPOINT env var.
const TELEMETRY_ENDPOINT: &str = "https://telemetry.pgcache.com/v1/ping";

/// Heartbeat interval (24 hours).
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(24 * 60 * 60);

/// Maximum jitter added to heartbeat interval to avoid thundering herd (60 minutes).
const HEARTBEAT_JITTER_SECS: u64 = 60 * 60;

/// HTTP request timeout.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// Origin PostgreSQL server version, captured from the first connection's ParameterStatus.
static PG_VERSION: OnceLock<String> = OnceLock::new();

/// Set the origin PostgreSQL server version. Called once from the first connection.
pub fn pg_version_set(version: String) {
    // First connection wins; subsequent calls are no-ops.
    let _ = PG_VERSION.set(version);
}

fn pg_version_get() -> &'static str {
    PG_VERSION.get().map(|s| s.as_str()).unwrap_or("unknown")
}

/// Detect whether we're running inside a Docker container.
fn deployment_detect() -> &'static str {
    // Explicit env var set by pgcache-entrypoint.sh
    if std::env::var("PGCACHE_DOCKER").is_ok() {
        return "docker";
    }
    // Fallback: Docker creates this file in every container
    if std::path::Path::new("/.dockerenv").exists() {
        return "docker";
    }
    "bare-metal"
}

#[derive(Serialize)]
struct TelemetryPayload {
    id: String,
    version: &'static str,
    pg_version: &'static str,
    os: &'static str,
    arch: &'static str,
    deployment: &'static str,
    event: &'static str,
    metrics: TelemetryMetrics,
}

#[derive(Serialize)]
struct TelemetryMetrics {
    cached_queries: u64,
    cache_hit_rate_pct: u8,
    queries_served_24h: u64,
    uptime_hours: u64,
}

/// Extract a counter or gauge value from Prometheus rendered text.
/// Looks for lines like `metric_name VALUE` (ignoring lines starting with #).
//
// Prometheus emits values as text-formatted f64; truncating back to u64 is the
// intended round-trip for counter values originally stored as u64.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
fn prometheus_metric_read(rendered: &str, metric_name: &str) -> u64 {
    // Prometheus text format: metric names have dots replaced with underscores
    let prom_name = metric_name.replace('.', "_");
    for line in rendered.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Lines look like: pgcache_queries_total 1234
        // or with labels: pgcache_queries_total{label="value"} 1234
        if let Some(rest) = line.strip_prefix(&prom_name) {
            // Must be followed by a space or { (labels)
            let value_str = if rest.starts_with(' ') {
                rest.trim()
            } else if rest.starts_with('{') {
                // Skip past the closing }
                rest.split_once('}').map(|(_, v)| v.trim()).unwrap_or("0")
            } else {
                continue;
            };
            return value_str.parse::<f64>().unwrap_or(0.0) as u64;
        }
    }
    0
}

/// Collect telemetry metrics from Prometheus counters.
fn metrics_collect(
    handle: &PrometheusHandle,
    started_at: Instant,
    prev_total: &mut u64,
) -> TelemetryMetrics {
    let rendered = handle.render();

    let cached_queries = prometheus_metric_read(&rendered, names::CACHE_QUERIES_REGISTERED);
    let total_hits = prometheus_metric_read(&rendered, names::QUERIES_CACHE_HIT);
    let total_misses = prometheus_metric_read(&rendered, names::QUERIES_CACHE_MISS);
    let total_served = total_hits.saturating_add(total_misses);

    let cache_hit_rate_pct = if total_served > 0 {
        // Result is bounded to 0..=100, fits in u8; query counts past 2^53 don't matter for a percentage.
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let pct = ((total_hits as f64 / total_served as f64) * 100.0).round() as u8;
        pct
    } else {
        0
    };

    let queries_served_24h = total_served.saturating_sub(*prev_total);
    *prev_total = total_served;

    let uptime_hours = started_at.elapsed().as_secs() / 3600;

    TelemetryMetrics {
        cached_queries,
        cache_hit_rate_pct,
        queries_served_24h,
        uptime_hours,
    }
}

fn endpoint_resolve() -> String {
    std::env::var("PGCACHE_TELEMETRY_ENDPOINT").unwrap_or_else(|_| TELEMETRY_ENDPOINT.to_owned())
}

/// Send a telemetry ping. Failures are silently ignored.
async fn ping_send(client: &reqwest::Client, endpoint: &str, payload: &TelemetryPayload) {
    let rv = client.post(endpoint).json(payload).send().await;
    trace!("Anonymous telemetry {rv:?}");
}

/// Spawn the telemetry background thread.
///
/// Runs on a dedicated thread with its own single-threaded tokio runtime.
/// Sends a startup ping, then heartbeats every 24h with jitter.
/// All network failures are silently ignored — telemetry never affects proxy operation.
pub fn telemetry_spawn(
    telemetry_enabled: bool,
    cancel: CancellationToken,
    metrics_handle: PrometheusHandle,
) -> Result<(), std::io::Error> {
    if !telemetry_enabled || cfg!(debug_assertions) {
        return Ok(());
    }

    info!(
        "Anonymous telemetry enabled. Set PGCACHE_TELEMETRY=off to disable. \
         See https://pgcache.com/docs/telemetry"
    );

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    std::thread::Builder::new()
        .name("telemetry".to_owned())
        .spawn(move || rt.block_on(telemetry_run(cancel, metrics_handle)))?;

    Ok(())
}

async fn telemetry_run(cancel: CancellationToken, metrics_handle: PrometheusHandle) {
    let instance_id = Uuid::new_v4().to_string();
    let deployment = deployment_detect();
    let endpoint = endpoint_resolve();
    let started_at = Instant::now();

    let client = reqwest::Client::builder()
        .timeout(REQUEST_TIMEOUT)
        .tls_certs_only(
            webpki_root_certs::TLS_SERVER_ROOT_CERTS
                .iter()
                .filter_map(|der| reqwest::Certificate::from_der(der.as_ref()).ok()),
        )
        .build()
        .unwrap_or_default();

    // Startup ping with zeroed metrics
    let mut prev_total: u64 = 0;
    let startup_payload = TelemetryPayload {
        id: instance_id.clone(),
        version: env!("CARGO_PKG_VERSION"),
        pg_version: pg_version_get(),
        os: std::env::consts::OS,
        arch: std::env::consts::ARCH,
        deployment,
        event: "startup",
        metrics: TelemetryMetrics {
            cached_queries: 0,
            cache_hit_rate_pct: 0,
            queries_served_24h: 0,
            uptime_hours: 0,
        },
    };
    ping_send(&client, &endpoint, &startup_payload).await;

    // Heartbeat loop
    loop {
        let jitter = jitter_duration();
        let sleep_duration = HEARTBEAT_INTERVAL + jitter;

        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = tokio::time::sleep(sleep_duration) => {}
        }

        let payload = TelemetryPayload {
            id: instance_id.clone(),
            version: env!("CARGO_PKG_VERSION"),
            pg_version: pg_version_get(),
            os: std::env::consts::OS,
            arch: std::env::consts::ARCH,
            deployment,
            event: "heartbeat",
            metrics: metrics_collect(&metrics_handle, started_at, &mut prev_total),
        };
        ping_send(&client, &endpoint, &payload).await;
    }
}

/// Generate a random jitter duration between 0 and HEARTBEAT_JITTER_SECS.
fn jitter_duration() -> Duration {
    // Simple deterministic-ish jitter using current time nanoseconds
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos();
    let jitter_secs = (nanos as u64) % HEARTBEAT_JITTER_SECS;
    Duration::from_secs(jitter_secs)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deployment_detect_bare_metal() {
        // Without PGCACHE_DOCKER env var and without /.dockerenv, should return bare-metal
        // Note: this test may behave differently in CI Docker environments
        if std::env::var("PGCACHE_DOCKER").is_err() && !std::path::Path::new("/.dockerenv").exists()
        {
            assert_eq!(deployment_detect(), "bare-metal");
        }
    }

    #[test]
    fn test_pg_version_default() {
        // Before any connection sets the version, should return "unknown"
        // Note: OnceLock is process-global, so this test is order-dependent
        // Only assert if no other test has set it
        if PG_VERSION.get().is_none() {
            assert_eq!(pg_version_get(), "unknown");
        }
    }

    #[test]
    fn test_jitter_within_bounds() {
        let jitter = jitter_duration();
        assert!(jitter.as_secs() < HEARTBEAT_JITTER_SECS);
    }

    #[test]
    fn test_payload_serialization() {
        let payload = TelemetryPayload {
            id: "test-id".to_owned(),
            version: "0.4.7",
            pg_version: "16.2",
            os: "linux",
            arch: "x86_64",
            deployment: "docker",
            event: "startup",
            metrics: TelemetryMetrics {
                cached_queries: 5,
                cache_hit_rate_pct: 72,
                queries_served_24h: 1000,
                uptime_hours: 24,
            },
        };
        let json = serde_json::to_string(&payload).expect("serialize payload");
        assert!(json.contains("\"id\":\"test-id\""));
        assert!(json.contains("\"event\":\"startup\""));
        assert!(json.contains("\"cache_hit_rate_pct\":72"));
    }

    #[test]
    fn test_prometheus_metric_read_counter() {
        let rendered = "\
# HELP pgcache_queries_cache_hit Total cache hits
# TYPE pgcache_queries_cache_hit counter
pgcache_queries_cache_hit 42
# HELP pgcache_queries_cache_miss Total cache misses
# TYPE pgcache_queries_cache_miss counter
pgcache_queries_cache_miss 8
";
        assert_eq!(
            prometheus_metric_read(rendered, "pgcache.queries.cache_hit"),
            42
        );
        assert_eq!(
            prometheus_metric_read(rendered, "pgcache.queries.cache_miss"),
            8
        );
    }

    #[test]
    fn test_prometheus_metric_read_missing() {
        let rendered = "# nothing here\n";
        assert_eq!(prometheus_metric_read(rendered, "pgcache.queries.total"), 0);
    }

    #[test]
    fn test_prometheus_metric_read_float() {
        let rendered = "pgcache_cache_queries_registered 15.0\n";
        assert_eq!(
            prometheus_metric_read(rendered, "pgcache.cache.queries_registered"),
            15
        );
    }
}
