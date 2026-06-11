use serde::{Deserialize, Serialize};
use tokio::sync::oneshot;

/// Request for current cache status, sent from the admin HTTP handler.
pub struct StatusRequest {
    pub reply_tx: oneshot::Sender<StatusResponse>,
}

/// Full status response assembled by the cache writer.
#[derive(Debug, Serialize, Deserialize)]
pub struct StatusResponse {
    pub cache: CacheStatusData,
    pub cdc: CdcStatusData,
    pub queries: Vec<QueryStatusData>,
    /// Whether this binary was built with the `fault-injection` feature.
    /// Harnesses that request fault hooks check this to fail loudly instead
    /// of silently exercising nothing.
    #[serde(default)]
    pub fault_injection: bool,
}

/// Cache subsystem state.
#[derive(Debug, Serialize, Deserialize)]
pub struct CacheStatusData {
    pub size_bytes: usize,
    pub size_limit_bytes: Option<usize>,
    pub generation: u64,
    pub tables_tracked: usize,
    pub policy: String,
    pub queries_registered: usize,
    pub uptime_ms: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
}

/// CDC / logical replication state.
///
/// Only fields whose source of truth is the writer thread live here.
/// CDC-processor-local LSNs (`received_lsn`, `flushed_lsn`) and lag values
/// are exposed as Prometheus gauges on `/metrics` instead — that's their
/// natural home, and avoids cross-thread plumbing for read-only display.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CdcStatusData {
    /// Highest LSN whose effects (cache mutations and invalidations) have
    /// been fully applied by the writer. Advances on transaction commit
    /// markers and keep-alive markers delivered through the CDC command
    /// channel — guaranteed transaction-aligned.
    pub last_applied_lsn: u64,
}

/// Per-query status for a cached query.
#[derive(Debug, Serialize, Deserialize)]
pub struct QueryStatusData {
    pub fingerprint: u64,
    pub sql_preview: String,
    pub tables: Vec<String>,
    pub state: String,
    pub cached_bytes: usize,
    pub max_limit: Option<u64>,
    pub pinned: bool,
    // Per-query operational metrics
    pub hit_count: u64,
    pub miss_count: u64,
    /// Milliseconds since last cache hit (None = no hits yet)
    pub idle_duration_ms: Option<u64>,
    /// Milliseconds since query was first seen by the cache
    pub registered_duration_ms: Option<u64>,
    /// Milliseconds since query last became Ready (None = not currently cached)
    pub cached_duration_ms: Option<u64>,
    pub invalidation_count: u64,
    pub readmission_count: u64,
    pub eviction_count: u64,
    pub subsumption_count: u64,
    pub population_count: u64,
    /// Milliseconds (None = not set)
    pub last_population_duration_ms: Option<u64>,
    pub total_bytes_served: u64,
    pub population_row_count: u64,
    pub cache_hit_latency: Option<LatencyStats>,
}

/// Summary statistics from the cache-hit latency histogram.
#[derive(Debug, Serialize, Deserialize)]
pub struct LatencyStats {
    pub count: u64,
    pub mean_us: f64,
    pub p50_us: u64,
    pub p95_us: u64,
    pub p99_us: u64,
    pub min_us: u64,
    pub max_us: u64,
}
