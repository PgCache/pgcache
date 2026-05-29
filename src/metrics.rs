use std::fmt;
use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response};
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::{PrometheusBuilder, PrometheusHandle};
use rootcause::Report;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::cache::StatusRequest;
use crate::proxy::{SharedProxyStatus, StatusSender};
use crate::settings::{
    DynamicConfig, DynamicConfigHandle, DynamicConfigPatch, config_file_dynamic_extract,
    config_file_dynamic_update,
};

/// Metrics subsystem errors.
#[derive(Debug)]
pub enum MetricsError {
    /// Prometheus recorder build failed.
    Build(String),
    /// A global metrics recorder was already installed.
    RecorderInstall,
}

impl fmt::Display for MetricsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MetricsError::Build(msg) => write!(f, "{msg}"),
            MetricsError::RecorderInstall => {
                write!(f, "global metrics recorder already installed")
            }
        }
    }
}

impl std::error::Error for MetricsError {}

pub type MetricsResult<T> = Result<T, Report<MetricsError>>;

/// Metric names as constants for consistency
pub mod names {
    // Counter metrics
    pub const QUERIES_TOTAL: &str = "pgcache.queries.total";
    pub const QUERIES_CACHEABLE: &str = "pgcache.queries.cacheable";
    pub const QUERIES_UNCACHEABLE: &str = "pgcache.queries.uncacheable";
    pub const QUERIES_UNSUPPORTED: &str = "pgcache.queries.unsupported";
    pub const QUERIES_INVALID: &str = "pgcache.queries.invalid";
    pub const QUERIES_CACHE_HIT: &str = "pgcache.queries.cache_hit";
    pub const QUERIES_CACHE_MISS: &str = "pgcache.queries.cache_miss";
    pub const QUERIES_CACHE_ERROR: &str = "pgcache.queries.cache_error";
    pub const QUERIES_ALLOWLIST_SKIPPED: &str = "pgcache.queries.allowlist_skipped";

    // Histogram metrics (latency in seconds per Prometheus convention)
    /// End-to-end latency for cache hits: client message received → response written to client.
    pub const CACHE_QUERY_LATENCY_SECONDS: &str = "pgcache.query.cache_latency_seconds";
    /// End-to-end latency for origin queries: client message received → ReadyForQuery forwarded to client.
    pub const ORIGIN_QUERY_LATENCY_SECONDS: &str = "pgcache.query.origin_latency_seconds";
    /// Pure origin execution time: forward decision made → ReadyForQuery received (excludes proxy overhead).
    pub const ORIGIN_EXECUTION_SECONDS: &str = "pgcache.origin.execution_seconds";
    pub const CACHE_LOOKUP_LATENCY_SECONDS: &str = "pgcache.cache.lookup_latency_seconds";
    pub const QUERY_REGISTRATION_LATENCY_SECONDS: &str =
        "pgcache.query.registration_latency_seconds";

    // Connection metrics
    pub const CONNECTIONS_TOTAL: &str = "pgcache.connections.total";
    pub const CONNECTIONS_ACTIVE: &str = "pgcache.connections.active";
    pub const CONNECTIONS_ERRORS: &str = "pgcache.connections.errors";

    // CDC/Replication metrics
    pub const CDC_EVENTS_PROCESSED: &str = "pgcache.cdc.events_processed";
    pub const CDC_INSERTS: &str = "pgcache.cdc.inserts";
    pub const CDC_UPDATES: &str = "pgcache.cdc.updates";
    pub const CDC_DELETES: &str = "pgcache.cdc.deletes";
    pub const CDC_LAG_BYTES: &str = "pgcache.cdc.lag_bytes";
    pub const CDC_LAG_SECONDS: &str = "pgcache.cdc.lag_seconds";
    pub const CDC_FLUSH_STALENESS_SECONDS: &str = "pgcache.cdc.flush_staleness_seconds";
    /// Last LSN received from origin via XLogData. Set in the CDC processor
    /// thread on every replication message.
    pub const CDC_RECEIVED_LSN: &str = "pgcache.cdc.received_lsn";
    /// Last LSN acknowledged to origin via standby status update. Set in the
    /// CDC processor thread after each successful keep-alive send.
    pub const CDC_FLUSHED_LSN: &str = "pgcache.cdc.flushed_lsn";
    /// Highest LSN whose effects (cache mutations and invalidations) have been
    /// fully applied by the writer. Advances on transaction-commit and keep-alive
    /// marks delivered through the CDC command channel.
    pub const CDC_APPLIED_LSN: &str = "pgcache.cdc.applied_lsn";

    // Cache performance metrics
    pub const CACHE_INVALIDATIONS: &str = "pgcache.cache.invalidations";
    pub const CACHE_FRESHNESS_HITS: &str = "pgcache.cache.freshness_hits";
    pub const CACHE_CDC_LOCAL_EVAL_HITS: &str = "pgcache.cache.cdc_local_eval_hits";
    pub const CACHE_CDC_PG_EVAL_HITS: &str = "pgcache.cache.cdc_pg_eval_hits";
    pub const CACHE_EVICTIONS: &str = "pgcache.cache.evictions";
    pub const CACHE_READMISSIONS: &str = "pgcache.cache.readmissions";
    pub const CACHE_SUBSUMPTIONS: &str = "pgcache.cache.subsumptions";
    pub const CACHE_SUBSUMPTION_LATENCY_SECONDS: &str = "pgcache.cache.subsumption_latency_seconds";

    // Materialized result metrics
    /// Requests served directly from the MV table (fast path).
    pub const CACHE_MV_HITS: &str = "pgcache.cache.mv_hits";
    /// Cache hits where mv_state != Fresh — fell through to source-row eval.
    pub const CACHE_MV_FALLTHROUGH: &str = "pgcache.cache.mv_fallthrough";
    /// Rebuilds committed by the writer (Dirty → Fresh transitions).
    pub const CACHE_MV_REBUILDS: &str = "pgcache.cache.mv_rebuilds";
    /// Rebuild messages the writer dropped because source-row state was not Ready
    /// at the time of processing (transitioned back to Dirty).
    pub const CACHE_MV_SKIPPED_REBUILDS: &str = "pgcache.cache.mv_skipped_rebuilds";
    /// Dirty MV tables truncated by the eviction pre-sweep.
    pub const CACHE_MV_DIRTY_TRUNCATES: &str = "pgcache.cache.mv_dirty_truncates";
    /// Histogram of MV build durations (seconds). Labeled by `kind={first_pop,rebuild}`
    /// so operators can correlate with replication slot lag to decide whether
    /// off-thread builds (Option B) are needed.
    pub const CACHE_MV_BUILD_DURATION_SECONDS: &str = "pgcache.cache.mv_build_duration_seconds";

    // Cache state metrics (admission/eviction policy)
    pub const CACHE_QUERIES_PENDING: &str = "pgcache.cache.queries_pending";
    pub const CACHE_QUERIES_INVALIDATED: &str = "pgcache.cache.queries_invalidated";

    // Queue depth gauges
    pub const CACHE_WRITER_QUERY_QUEUE: &str = "pgcache.cache.writer_query_queue";
    pub const CACHE_WRITER_CDC_QUEUE: &str = "pgcache.cache.writer_cdc_queue";
    pub const CACHE_WRITER_INTERNAL_QUEUE: &str = "pgcache.cache.writer_internal_queue";
    pub const CACHE_WORKER_QUEUE: &str = "pgcache.cache.worker_queue";
    pub const CACHE_POPULATION_WORKER_QUEUE: &str = "pgcache.cache.population_worker_queue";
    pub const CACHE_PROXY_MESSAGE_QUEUE: &str = "pgcache.cache.proxy_message_queue";
    pub const PROXY_WORKER_QUEUE: &str = "pgcache.proxy.worker_queue";
    pub const CACHE_HANDLE_INSERTS: &str = "pgcache.cache.handle_inserts";
    pub const CACHE_HANDLE_UPDATES: &str = "pgcache.cache.handle_updates";
    pub const CACHE_HANDLE_DELETES: &str = "pgcache.cache.handle_deletes";

    // CDC handler latency (histograms)
    pub const CACHE_HANDLE_INSERT_SECONDS: &str = "pgcache.cache.handle_insert_seconds";
    pub const CACHE_HANDLE_UPDATE_SECONDS: &str = "pgcache.cache.handle_update_seconds";
    pub const CACHE_HANDLE_DELETE_SECONDS: &str = "pgcache.cache.handle_delete_seconds";

    // Cache state metrics
    pub const CACHE_QUERIES_REGISTERED: &str = "pgcache.cache.queries_registered";
    pub const CACHE_QUERIES_LOADING: &str = "pgcache.cache.queries_loading";
    pub const CACHE_SIZE_BYTES: &str = "pgcache.cache.size_bytes";
    pub const CACHE_SIZE_LIMIT_BYTES: &str = "pgcache.cache.size_limit_bytes";
    pub const CACHE_GENERATION: &str = "pgcache.cache.generation";
    pub const CACHE_TABLES_TRACKED: &str = "pgcache.cache.tables_tracked";

    // Request coalescing metrics
    pub const CACHE_COALESCE_WAITING: &str = "pgcache.cache.coalesce_waiting";
    pub const CACHE_COALESCE_SERVED: &str = "pgcache.cache.coalesce_served";

    // Extended protocol metrics
    pub const PROTOCOL_SIMPLE_QUERIES: &str = "pgcache.protocol.simple_queries";
    pub const PROTOCOL_EXTENDED_QUERIES: &str = "pgcache.protocol.extended_queries";
    pub const PROTOCOL_PREPARED_STATEMENTS: &str = "pgcache.protocol.prepared_statements";

    // Per-connection describe-response cache, keyed by (sql, parameter_oids).
    // Hits serve Parse-only batches locally instead of forwarding to origin.
    pub const PROTOCOL_DESCRIBE_CACHE_HITS: &str = "pgcache.protocol.describe_cache.hits";
    pub const PROTOCOL_DESCRIBE_CACHE_MISSES: &str = "pgcache.protocol.describe_cache.misses";
    pub const PROTOCOL_DESCRIBE_CACHE_EVICTIONS: &str = "pgcache.protocol.describe_cache.evictions";
    pub const PROTOCOL_DESCRIBE_CACHE_INVALIDATIONS: &str =
        "pgcache.protocol.describe_cache.invalidations";
    /// Bind+Execute forwards where pgcache prepended a lazy `Parse` because
    /// origin didn't yet know the statement.
    pub const PROTOCOL_LAZY_PARSE_FORWARDED: &str = "pgcache.protocol.lazy_parse_forwarded";

    // Writer thread instrumentation (PGC-117)
    /// Per-command handler latency. Labeled with `cmd` for each QueryCommand /
    /// CdcCommand variant.
    pub const CACHE_WRITER_COMMAND_HANDLE_SECONDS: &str =
        "pgcache.cache.writer.command_handle_seconds";
    /// Phase timings inside `query_register`. Suspected O(N) growth lives in
    /// `subsumption_check` and (transitively) in `resolve.update_queries_register`.
    pub const CACHE_WRITER_REGISTER_RESOLVE_SECONDS: &str =
        "pgcache.cache.writer.register.resolve_seconds";
    pub const CACHE_WRITER_REGISTER_SUBSUMPTION_CHECK_SECONDS: &str =
        "pgcache.cache.writer.register.subsumption_check_seconds";
    pub const CACHE_WRITER_REGISTER_SUBSUME_SECONDS: &str =
        "pgcache.cache.writer.register.subsume_seconds";
    pub const CACHE_WRITER_REGISTER_INSERT_SECONDS: &str =
        "pgcache.cache.writer.register.insert_seconds";
    pub const CACHE_WRITER_REGISTER_PUBLICATION_UPDATE_SECONDS: &str =
        "pgcache.cache.writer.register.publication_update_seconds";
    pub const CACHE_WRITER_REGISTER_POPULATE_DISPATCH_SECONDS: &str =
        "pgcache.cache.writer.register.populate_dispatch_seconds";
    /// Sub-phases inside `query_resolve`.
    pub const CACHE_WRITER_RESOLVE_UPDATE_QUERIES_REGISTER_SECONDS: &str =
        "pgcache.cache.writer.resolve.update_queries_register_seconds";
    pub const CACHE_WRITER_RESOLVE_DEPARSE_SECONDS: &str =
        "pgcache.cache.writer.resolve.deparse_seconds";
    /// Population pipeline timings (per-task, per-stream, channel-wait).
    pub const CACHE_POPULATION_TASK_SECONDS: &str = "pgcache.cache.population.task_seconds";
    pub const CACHE_POPULATION_STREAM_SECONDS: &str = "pgcache.cache.population.stream_seconds";
    pub const CACHE_POPULATION_WAIT_SECONDS: &str = "pgcache.cache.population.wait_seconds";
    /// Per-worker time waiting on rx.recv() between tasks. With \`task_seconds\`
    /// and wall clock, gives a clear utilization signal at a given pool size.
    pub const CACHE_POPULATION_WORKER_IDLE_SECONDS: &str =
        "pgcache.cache.population.worker_idle_seconds";
    /// Scaling signals for correlating per-Register cost against state size.
    pub const CACHE_WRITER_UPDATE_QUERIES_TOTAL: &str = "pgcache.cache.writer.update_queries_total";
    pub const CACHE_WRITER_UPDATE_QUERIES_MAX_PER_RELATION: &str =
        "pgcache.cache.writer.update_queries_max_per_relation";

    // Per-stage timing histograms
    pub const QUERY_STAGE_PARSE_SECONDS: &str = "pgcache.query.stage.parse_seconds";
    pub const QUERY_STAGE_DISPATCH_SECONDS: &str = "pgcache.query.stage.dispatch_seconds";
    pub const QUERY_STAGE_LOOKUP_SECONDS: &str = "pgcache.query.stage.lookup_seconds";
    pub const QUERY_STAGE_QUEUE_WAIT_SECONDS: &str = "pgcache.query.stage.queue_wait_seconds";
    pub const QUERY_STAGE_CONN_WAIT_SECONDS: &str = "pgcache.query.stage.conn_wait_seconds";
    pub const QUERY_STAGE_SPAWN_WAIT_SECONDS: &str = "pgcache.query.stage.spawn_wait_seconds";
    pub const QUERY_STAGE_WORKER_EXEC_SECONDS: &str = "pgcache.query.stage.worker_exec_seconds";
    pub const QUERY_STAGE_RESPONSE_WRITE_SECONDS: &str =
        "pgcache.query.stage.response_write_seconds";
    /// Forward-path only: dispatched_at → forwarded_at. Cache-thread decision
    /// time plus the channel hop back to the proxy. Surfaces hidden cost on
    /// the cache-miss path that `lookup_seconds` doesn't capture.
    pub const QUERY_STAGE_FORWARD_DECISION_SECONDS: &str =
        "pgcache.query.stage.forward_decision_seconds";
    /// Coalesce path: lookup_complete_at → waiter_enqueued_at. Cache-thread
    /// book-keeping into the waiting map. Expected to be sub-microsecond;
    /// non-zero values would point at coordinator contention.
    pub const QUERY_STAGE_COALESCE_INTAKE_SECONDS: &str =
        "pgcache.query.stage.coalesce_intake_seconds";
    /// Coalesce path: waiter_enqueued_at → drain_started_at. The actual wait
    /// — population pipeline + writer Ready notify + coordinator drain
    /// pickup. The "we have a 2-second p95 even though population is 2 ms"
    /// signal lives here.
    pub const QUERY_STAGE_COALESCE_WAIT_SECONDS: &str = "pgcache.query.stage.coalesce_wait_seconds";
    pub const QUERY_STAGE_TOTAL_SECONDS: &str = "pgcache.query.stage.total_seconds";
}

/// Install the global Prometheus metrics recorder.
///
/// This only builds the recorder and sets it as global. Call `admin_server_spawn`
/// separately to start the HTTP server once the status channel is available.
pub fn metrics_recorder_install() -> MetricsResult<PrometheusHandle> {
    let recorder = PrometheusBuilder::new()
        .set_quantiles(&[0.5, 0.95, 0.99])
        .map_err(|e| MetricsError::Build(e.to_string()))?
        .build_recorder();

    let handle = recorder.handle();

    metrics::set_global_recorder(recorder)
        .map_err(|_| Report::new(MetricsError::RecorderInstall))?;

    Ok(handle)
}

/// Spawn the admin HTTP server thread.
///
/// Serves `/metrics`, `/healthz`, `/readyz`, `/status`, and `/config` endpoints.
/// The `/status` endpoint sends a `StatusRequest` to the cache writer and
/// returns the JSON response.
pub fn admin_server_spawn(
    addr: SocketAddr,
    metrics: PrometheusHandle,
    cancel: CancellationToken,
    shared_proxy_status: SharedProxyStatus,
    status_tx: StatusSender,
    dynamic: DynamicConfigHandle,
) -> Result<(), std::io::Error> {
    std::thread::Builder::new()
        .name("http".to_owned())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("admin server tokio runtime");
            rt.block_on(admin_server_run(
                addr,
                metrics,
                cancel,
                shared_proxy_status,
                status_tx,
                dynamic,
            ));
        })?;
    Ok(())
}

/// Admin HTTP server that serves metrics, health, config, and status endpoints.
async fn admin_server_run(
    addr: SocketAddr,
    handle: PrometheusHandle,
    cancel: CancellationToken,
    shared_proxy_status: SharedProxyStatus,
    status_tx: StatusSender,
    dynamic: DynamicConfigHandle,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("admin server bind failed on {addr}: {e}");
            return;
        }
    };

    loop {
        let stream = tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        tracing::warn!("admin server accept error: {e}");
                        continue;
                    }
                }
            }
        };

        let handle = handle.clone();
        let shared_proxy_status = shared_proxy_status.clone();
        let status_tx = status_tx.clone();
        let dynamic = dynamic.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: http::Request<hyper::body::Incoming>| {
                let h = handle.clone();
                let proxy_status = shared_proxy_status.clone();
                let status_tx = status_tx.clone();
                let dynamic = dynamic.clone();
                async move {
                    match (request.uri().path(), request.method()) {
                        ("/metrics", _) => {
                            let body = h.render();
                            Response::builder()
                                .header("Content-Type", "text/plain; charset=utf-8")
                                .header("Access-Control-Allow-Origin", "*")
                                .body(Full::new(Bytes::from(body)))
                        }
                        ("/healthz", _) => Response::builder()
                            .header("Content-Type", "text/plain")
                            .body(Full::new(Bytes::from("OK"))),
                        ("/readyz", _) => {
                            if proxy_status.is_ready() {
                                Response::builder()
                                    .header("Content-Type", "text/plain")
                                    .body(Full::new(Bytes::from("OK")))
                            } else {
                                Response::builder()
                                    .status(503)
                                    .header("Content-Type", "text/plain")
                                    .body(Full::new(Bytes::from("not ready")))
                            }
                        }
                        ("/status", _) => status_handle(status_tx).await,
                        ("/config", &Method::GET) => config_get_handle(&dynamic).await,
                        ("/config", &Method::PUT) => config_put_handle(request, &dynamic).await,
                        ("/config/reload", &Method::POST) => config_reload_handle(&dynamic).await,
                        _ => Response::builder()
                            .status(404)
                            .body(Full::new(Bytes::from("Not Found"))),
                    }
                }
            });

            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!("admin connection error: {e}");
            }
        });
    }

    tracing::debug!("admin server shutting down");
}

/// Handle a `/status` request by querying the cache writer via the status channel.
async fn status_handle(status_tx: StatusSender) -> Result<Response<Full<Bytes>>, http::Error> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let req = StatusRequest { reply_tx };

    if status_tx.send(req).await.is_err() {
        return Response::builder()
            .status(503)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(r#"{"error":"cache unavailable"}"#)));
    }

    match tokio::time::timeout(Duration::from_secs(2), reply_rx).await {
        Ok(Ok(response)) => {
            let body = serde_json::to_string(&response)
                .unwrap_or_else(|e| format!(r#"{{"error":"serialization failed: {e}"}}"#));
            Response::builder()
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(body)))
        }
        Ok(Err(_)) => Response::builder()
            .status(503)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"cache channel closed"}"#,
            ))),
        Err(_) => Response::builder()
            .status(503)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"status request timed out"}"#,
            ))),
    }
}

/// Maximum request body size for config updates (64 KiB).
const CONFIG_BODY_LIMIT: usize = 64 * 1024;

fn json_error(status: u16, message: &str) -> Result<Response<Full<Bytes>>, http::Error> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(format!(
            r#"{{"error":"{message}"}}"#
        ))))
}

#[derive(Serialize)]
struct ConfigGetResponse<'a> {
    dynamic: &'a DynamicConfig,
    restart_required: bool,
    effective_log_level: Option<String>,
}

fn config_response(dynamic: &DynamicConfigHandle) -> Result<Response<Full<Bytes>>, http::Error> {
    let cfg = dynamic.load();
    let response = ConfigGetResponse {
        dynamic: &cfg,
        restart_required: dynamic.restart_required(),
        effective_log_level: dynamic.effective_log_level(),
    };
    let body = serde_json::to_string(&response)
        .unwrap_or_else(|e| format!(r#"{{"error":"serialization failed: {e}"}}"#));
    Response::builder()
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
}

async fn config_get_handle(
    dynamic: &DynamicConfigHandle,
) -> Result<Response<Full<Bytes>>, http::Error> {
    config_response(dynamic)
}

async fn config_put_handle(
    request: http::Request<hyper::body::Incoming>,
    dynamic: &DynamicConfigHandle,
) -> Result<Response<Full<Bytes>>, http::Error> {
    let body = match http_body_util::Limited::new(request, CONFIG_BODY_LIMIT)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return json_error(400, &format!("failed to read body: {e}")),
    };

    let patch: DynamicConfigPatch = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    if let Some(path) = dynamic.config_path()
        && let Err(e) = config_file_dynamic_update(path, &patch)
    {
        return json_error(500, &format!("failed to update config file: {e}"));
    }

    let current = dynamic.load();
    let new_config = patch.apply(&current);
    dynamic.update(new_config);

    config_response(dynamic)
}

async fn config_reload_handle(
    dynamic: &DynamicConfigHandle,
) -> Result<Response<Full<Bytes>>, http::Error> {
    let Some(path) = dynamic.config_path() else {
        return json_error(400, "no config file path available");
    };

    match config_file_dynamic_extract(path) {
        Ok(new_config) => {
            dynamic.update(new_config);
            config_response(dynamic)
        }
        Err(e) => json_error(500, &format!("failed to reload config: {e}")),
    }
}
