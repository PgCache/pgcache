use std::fmt;
use std::net::SocketAddr;
use std::sync::OnceLock;
use std::time::Duration;

use metrics::{Counter, Gauge, Histogram};

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
    /// Successful cache-subsystem restarts performed by the supervisor.
    pub const CACHE_RESTARTS_TOTAL: &str = "pgcache.cache.restarts_total";
    /// Cache-DB serve-pool connections reconnected after a poisoned discard.
    pub const CACHE_POOL_REPLENISHED: &str = "pgcache.cache.pool_replenished";

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
    /// `Close(statement)` handled locally (statement never prepared on origin):
    /// CloseComplete synthesized, Close+Sync not forwarded to origin (PGC-234).
    pub const PROTOCOL_CLOSE_LOCAL: &str = "pgcache.protocol.close_local";

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
    /// non-zero values would point at dispatch contention.
    pub const QUERY_STAGE_COALESCE_INTAKE_SECONDS: &str =
        "pgcache.query.stage.coalesce_intake_seconds";
    /// Coalesce path: waiter_enqueued_at → drain_started_at. The actual wait
    /// — population pipeline + writer Ready notify + dispatch drain
    /// pickup. The "we have a 2-second p95 even though population is 2 ms"
    /// signal lives here.
    pub const QUERY_STAGE_COALESCE_WAIT_SECONDS: &str = "pgcache.query.stage.coalesce_wait_seconds";
    pub const QUERY_STAGE_TOTAL_SECONDS: &str = "pgcache.query.stage.total_seconds";
}

/// Cached metric handles, grouped by usage area.
///
/// The `metrics::histogram!`/`counter!`/`gauge!` macros re-hash the metric key on
/// every call to look the handle up in the recorder registry. Profiling under
/// load showed that key hashing + registration accounted for ~12% of on-CPU time
/// — `timing_record` alone fires 12 histograms per query. Resolving each handle
/// once and reusing it skips the per-call SipHash + registry lookup. Handles are
/// cheap clonable references to the underlying metric, so caching them is sound.
///
/// Access via [`handles`]; `names::` constants appear only in the builders below.
pub struct Handles {
    /// Connection lifecycle + wire-protocol counters (proxy connection/server).
    pub conn: ConnHandles,
    /// Per-query routing outcomes + end-to-end latency (proxy).
    pub query: QueryHandles,
    /// Per-stage query timing histograms (timing.rs).
    pub stage: StageHandles,
    /// Cache-worker serving: lookup, MV serve, request coalescing.
    pub cache: CacheHandles,
    /// CDC ingest + apply: events, row ops, lag, LSN watermarks.
    pub cdc: CdcHandles,
    /// Materialized-view rebuild instrumentation (writer).
    pub mv: MvHandles,
    /// Query registration + population pipeline (writer).
    pub reg: RegHandles,
    /// Cache state, sizing, admission/eviction, and queue-depth gauges.
    pub state: StateHandles,
}

pub struct ConnHandles {
    pub total: Counter,
    pub active: Gauge,
    pub errors: Counter,
    pub simple_queries: Counter,
    pub extended_queries: Counter,
    pub prepared_statements: Gauge,
    pub describe_hits: Counter,
    pub describe_misses: Counter,
    pub describe_evictions: Counter,
    pub describe_invalidations: Counter,
    pub lazy_parse_forwarded: Counter,
    pub close_local: Counter,
}

pub struct QueryHandles {
    pub total: Counter,
    pub cacheable: Counter,
    pub uncacheable: Counter,
    pub unsupported: Counter,
    pub invalid: Counter,
    pub cache_hit: Counter,
    pub cache_miss: Counter,
    pub cache_error: Counter,
    pub allowlist_skipped: Counter,
    pub cache_latency: Histogram,
    pub origin_latency: Histogram,
    pub origin_execution: Histogram,
}

pub struct StageHandles {
    pub parse: Histogram,
    pub dispatch: Histogram,
    pub lookup: Histogram,
    pub queue_wait: Histogram,
    pub conn_wait: Histogram,
    pub spawn_wait: Histogram,
    pub worker_exec: Histogram,
    pub response_write: Histogram,
    pub forward_decision: Histogram,
    pub coalesce_intake: Histogram,
    pub coalesce_wait: Histogram,
    pub total: Histogram,
}

pub struct CacheHandles {
    pub lookup_latency: Histogram,
    pub mv_hits: Counter,
    pub mv_fallthrough: Counter,
    pub coalesce_waiting: Gauge,
    pub coalesce_served: Counter,
    /// Incremented each time the supervisor rebuilds the cache subsystem.
    pub restarts_total: Counter,
    /// Incremented each time a discarded serve-pool connection is replaced.
    pub pool_replenished: Counter,
}

pub struct CdcHandles {
    pub events_processed: Counter,
    pub inserts: Counter,
    pub updates: Counter,
    pub deletes: Counter,
    pub lag_seconds: Gauge,
    pub lag_bytes: Gauge,
    pub flush_staleness: Gauge,
    pub received_lsn: Gauge,
    pub flushed_lsn: Gauge,
    pub applied_lsn: Gauge,
    pub invalidations: Counter,
    pub local_eval_hits: Counter,
    pub pg_eval_hits: Counter,
    pub handle_inserts: Counter,
    pub handle_updates: Counter,
    pub handle_deletes: Counter,
    pub handle_insert_seconds: Histogram,
    pub handle_update_seconds: Histogram,
    pub handle_delete_seconds: Histogram,
    // Per-command handle latency, one cached handle per CdcCommand `cmd` label.
    pub cmd_begin: Histogram,
    pub cmd_table_register: Histogram,
    pub cmd_insert: Histogram,
    pub cmd_update: Histogram,
    pub cmd_delete: Histogram,
    pub cmd_truncate: Histogram,
    pub cmd_commit_mark: Histogram,
    pub cmd_keepalive_mark: Histogram,
}

pub struct MvHandles {
    pub rebuilds: Counter,
    pub skipped_rebuilds: Counter,
    pub dirty_truncates: Counter,
    // Build-duration histogram, one cached handle per `kind` label.
    pub build_first_pop: Histogram,
    pub build_rebuild: Histogram,
}

pub struct RegHandles {
    // Per-command handle latency, one cached handle per QueryCommand `cmd` label.
    pub cmd_register: Histogram,
    pub cmd_ready: Histogram,
    pub cmd_failed: Histogram,
    pub cmd_limit_bump: Histogram,
    pub cmd_readmit: Histogram,
    pub cmd_mv_build: Histogram,
    pub register_resolve: Histogram,
    pub register_subsumption_check: Histogram,
    pub register_subsume: Histogram,
    pub register_insert: Histogram,
    pub register_publication_update: Histogram,
    pub register_populate_dispatch: Histogram,
    pub resolve_update_queries_register: Histogram,
    pub resolve_deparse: Histogram,
    pub subsumptions: Counter,
    pub subsumption_latency: Histogram,
    pub registration_latency: Histogram,
    pub population_task: Histogram,
    pub population_stream: Histogram,
    pub population_wait: Histogram,
}

pub struct StateHandles {
    pub queries_registered: Gauge,
    pub queries_loading: Gauge,
    pub queries_pending: Gauge,
    pub queries_invalidated: Gauge,
    pub size_bytes: Gauge,
    pub size_limit_bytes: Gauge,
    pub generation: Gauge,
    pub tables_tracked: Gauge,
    pub update_queries_total: Gauge,
    pub update_queries_max_per_relation: Gauge,
    pub evictions: Counter,
    pub evictions_pinned_bump: Counter,
    pub evictions_bump: Counter,
    pub readmissions: Counter,
    pub queue_writer_query: Gauge,
    pub queue_writer_cdc: Gauge,
    pub queue_writer_internal: Gauge,
    pub queue_worker: Gauge,
}

impl Handles {
    fn build() -> Self {
        use names::*;
        Self {
            conn: ConnHandles {
                total: metrics::counter!(CONNECTIONS_TOTAL),
                active: metrics::gauge!(CONNECTIONS_ACTIVE),
                errors: metrics::counter!(CONNECTIONS_ERRORS),
                simple_queries: metrics::counter!(PROTOCOL_SIMPLE_QUERIES),
                extended_queries: metrics::counter!(PROTOCOL_EXTENDED_QUERIES),
                prepared_statements: metrics::gauge!(PROTOCOL_PREPARED_STATEMENTS),
                describe_hits: metrics::counter!(PROTOCOL_DESCRIBE_CACHE_HITS),
                describe_misses: metrics::counter!(PROTOCOL_DESCRIBE_CACHE_MISSES),
                describe_evictions: metrics::counter!(PROTOCOL_DESCRIBE_CACHE_EVICTIONS),
                describe_invalidations: metrics::counter!(PROTOCOL_DESCRIBE_CACHE_INVALIDATIONS),
                lazy_parse_forwarded: metrics::counter!(PROTOCOL_LAZY_PARSE_FORWARDED),
                close_local: metrics::counter!(PROTOCOL_CLOSE_LOCAL),
            },
            query: QueryHandles {
                total: metrics::counter!(QUERIES_TOTAL),
                cacheable: metrics::counter!(QUERIES_CACHEABLE),
                uncacheable: metrics::counter!(QUERIES_UNCACHEABLE),
                unsupported: metrics::counter!(QUERIES_UNSUPPORTED),
                invalid: metrics::counter!(QUERIES_INVALID),
                cache_hit: metrics::counter!(QUERIES_CACHE_HIT),
                cache_miss: metrics::counter!(QUERIES_CACHE_MISS),
                cache_error: metrics::counter!(QUERIES_CACHE_ERROR),
                allowlist_skipped: metrics::counter!(QUERIES_ALLOWLIST_SKIPPED),
                cache_latency: metrics::histogram!(CACHE_QUERY_LATENCY_SECONDS),
                origin_latency: metrics::histogram!(ORIGIN_QUERY_LATENCY_SECONDS),
                origin_execution: metrics::histogram!(ORIGIN_EXECUTION_SECONDS),
            },
            stage: StageHandles {
                parse: metrics::histogram!(QUERY_STAGE_PARSE_SECONDS),
                dispatch: metrics::histogram!(QUERY_STAGE_DISPATCH_SECONDS),
                lookup: metrics::histogram!(QUERY_STAGE_LOOKUP_SECONDS),
                queue_wait: metrics::histogram!(QUERY_STAGE_QUEUE_WAIT_SECONDS),
                conn_wait: metrics::histogram!(QUERY_STAGE_CONN_WAIT_SECONDS),
                spawn_wait: metrics::histogram!(QUERY_STAGE_SPAWN_WAIT_SECONDS),
                worker_exec: metrics::histogram!(QUERY_STAGE_WORKER_EXEC_SECONDS),
                response_write: metrics::histogram!(QUERY_STAGE_RESPONSE_WRITE_SECONDS),
                forward_decision: metrics::histogram!(QUERY_STAGE_FORWARD_DECISION_SECONDS),
                coalesce_intake: metrics::histogram!(QUERY_STAGE_COALESCE_INTAKE_SECONDS),
                coalesce_wait: metrics::histogram!(QUERY_STAGE_COALESCE_WAIT_SECONDS),
                total: metrics::histogram!(QUERY_STAGE_TOTAL_SECONDS),
            },
            cache: CacheHandles {
                lookup_latency: metrics::histogram!(CACHE_LOOKUP_LATENCY_SECONDS),
                mv_hits: metrics::counter!(CACHE_MV_HITS),
                mv_fallthrough: metrics::counter!(CACHE_MV_FALLTHROUGH),
                coalesce_waiting: metrics::gauge!(CACHE_COALESCE_WAITING),
                coalesce_served: metrics::counter!(CACHE_COALESCE_SERVED),
                restarts_total: metrics::counter!(CACHE_RESTARTS_TOTAL),
                pool_replenished: metrics::counter!(CACHE_POOL_REPLENISHED),
            },
            cdc: CdcHandles {
                events_processed: metrics::counter!(CDC_EVENTS_PROCESSED),
                inserts: metrics::counter!(CDC_INSERTS),
                updates: metrics::counter!(CDC_UPDATES),
                deletes: metrics::counter!(CDC_DELETES),
                lag_seconds: metrics::gauge!(CDC_LAG_SECONDS),
                lag_bytes: metrics::gauge!(CDC_LAG_BYTES),
                flush_staleness: metrics::gauge!(CDC_FLUSH_STALENESS_SECONDS),
                received_lsn: metrics::gauge!(CDC_RECEIVED_LSN),
                flushed_lsn: metrics::gauge!(CDC_FLUSHED_LSN),
                applied_lsn: metrics::gauge!(CDC_APPLIED_LSN),
                invalidations: metrics::counter!(CACHE_INVALIDATIONS),
                local_eval_hits: metrics::counter!(CACHE_CDC_LOCAL_EVAL_HITS),
                pg_eval_hits: metrics::counter!(CACHE_CDC_PG_EVAL_HITS),
                handle_inserts: metrics::counter!(CACHE_HANDLE_INSERTS),
                handle_updates: metrics::counter!(CACHE_HANDLE_UPDATES),
                handle_deletes: metrics::counter!(CACHE_HANDLE_DELETES),
                handle_insert_seconds: metrics::histogram!(CACHE_HANDLE_INSERT_SECONDS),
                handle_update_seconds: metrics::histogram!(CACHE_HANDLE_UPDATE_SECONDS),
                handle_delete_seconds: metrics::histogram!(CACHE_HANDLE_DELETE_SECONDS),
                cmd_begin: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_begin"),
                cmd_table_register: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_table_register"),
                cmd_insert: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_insert"),
                cmd_update: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_update"),
                cmd_delete: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_delete"),
                cmd_truncate: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_truncate"),
                cmd_commit_mark: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_commit_mark"),
                cmd_keepalive_mark: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "cdc_keepalive_mark"),
            },
            mv: MvHandles {
                rebuilds: metrics::counter!(CACHE_MV_REBUILDS),
                skipped_rebuilds: metrics::counter!(CACHE_MV_SKIPPED_REBUILDS),
                dirty_truncates: metrics::counter!(CACHE_MV_DIRTY_TRUNCATES),
                build_first_pop: metrics::histogram!(CACHE_MV_BUILD_DURATION_SECONDS, "kind" => "first_pop"),
                build_rebuild: metrics::histogram!(CACHE_MV_BUILD_DURATION_SECONDS, "kind" => "rebuild"),
            },
            reg: RegHandles {
                cmd_register: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "register"),
                cmd_ready: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "ready"),
                cmd_failed: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "failed"),
                cmd_limit_bump: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "limit_bump"),
                cmd_readmit: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "readmit"),
                cmd_mv_build: metrics::histogram!(CACHE_WRITER_COMMAND_HANDLE_SECONDS, "cmd" => "mv_build"),
                register_resolve: metrics::histogram!(CACHE_WRITER_REGISTER_RESOLVE_SECONDS),
                register_subsumption_check: metrics::histogram!(
                    CACHE_WRITER_REGISTER_SUBSUMPTION_CHECK_SECONDS
                ),
                register_subsume: metrics::histogram!(CACHE_WRITER_REGISTER_SUBSUME_SECONDS),
                register_insert: metrics::histogram!(CACHE_WRITER_REGISTER_INSERT_SECONDS),
                register_publication_update: metrics::histogram!(
                    CACHE_WRITER_REGISTER_PUBLICATION_UPDATE_SECONDS
                ),
                register_populate_dispatch: metrics::histogram!(
                    CACHE_WRITER_REGISTER_POPULATE_DISPATCH_SECONDS
                ),
                resolve_update_queries_register: metrics::histogram!(
                    CACHE_WRITER_RESOLVE_UPDATE_QUERIES_REGISTER_SECONDS
                ),
                resolve_deparse: metrics::histogram!(CACHE_WRITER_RESOLVE_DEPARSE_SECONDS),
                subsumptions: metrics::counter!(CACHE_SUBSUMPTIONS),
                subsumption_latency: metrics::histogram!(CACHE_SUBSUMPTION_LATENCY_SECONDS),
                registration_latency: metrics::histogram!(QUERY_REGISTRATION_LATENCY_SECONDS),
                population_task: metrics::histogram!(CACHE_POPULATION_TASK_SECONDS),
                population_stream: metrics::histogram!(CACHE_POPULATION_STREAM_SECONDS),
                population_wait: metrics::histogram!(CACHE_POPULATION_WAIT_SECONDS),
            },
            state: StateHandles {
                queries_registered: metrics::gauge!(CACHE_QUERIES_REGISTERED),
                queries_loading: metrics::gauge!(CACHE_QUERIES_LOADING),
                queries_pending: metrics::gauge!(CACHE_QUERIES_PENDING),
                queries_invalidated: metrics::gauge!(CACHE_QUERIES_INVALIDATED),
                size_bytes: metrics::gauge!(CACHE_SIZE_BYTES),
                size_limit_bytes: metrics::gauge!(CACHE_SIZE_LIMIT_BYTES),
                generation: metrics::gauge!(CACHE_GENERATION),
                tables_tracked: metrics::gauge!(CACHE_TABLES_TRACKED),
                update_queries_total: metrics::gauge!(CACHE_WRITER_UPDATE_QUERIES_TOTAL),
                update_queries_max_per_relation: metrics::gauge!(
                    CACHE_WRITER_UPDATE_QUERIES_MAX_PER_RELATION
                ),
                evictions: metrics::counter!(CACHE_EVICTIONS),
                evictions_pinned_bump: metrics::counter!(CACHE_EVICTIONS, "result" => "pinned_bump"),
                evictions_bump: metrics::counter!(CACHE_EVICTIONS, "result" => "bump"),
                readmissions: metrics::counter!(CACHE_READMISSIONS),
                queue_writer_query: metrics::gauge!(CACHE_WRITER_QUERY_QUEUE),
                queue_writer_cdc: metrics::gauge!(CACHE_WRITER_CDC_QUEUE),
                queue_writer_internal: metrics::gauge!(CACHE_WRITER_INTERNAL_QUEUE),
                queue_worker: metrics::gauge!(CACHE_WORKER_QUEUE),
            },
        }
    }
}

static HANDLES: OnceLock<Handles> = OnceLock::new();

/// Cached metric handles grouped by usage area. First call binds handles to the
/// currently installed recorder; `metrics_recorder_install` primes this after
/// install so the handles bind to the real Prometheus recorder rather than a
/// no-op.
pub fn handles() -> &'static Handles {
    HANDLES.get_or_init(Handles::build)
}

/// Per-worker population handles (idle-time histogram, queue-depth gauge),
/// labeled `worker={id}`. The label value is dynamic, so these can't live in the
/// global [`Handles`]; resolve once per worker and reuse across the worker loop.
pub fn population_worker_handles(id: usize) -> (Histogram, Gauge) {
    let worker = id.to_string();
    (
        metrics::histogram!(names::CACHE_POPULATION_WORKER_IDLE_SECONDS, "worker" => worker.clone()),
        metrics::gauge!(names::CACHE_POPULATION_WORKER_QUEUE, "worker" => worker),
    )
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

    // Bind the cached handles to the recorder we just installed.
    let _ = handles();

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
