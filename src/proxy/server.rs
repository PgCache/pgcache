use std::net::SocketAddr;
use std::{sync::Arc, thread};

use arc_swap::ArcSwap;
use ecow::EcoString;
use metrics_exporter_prometheus::PrometheusHandle;
use rootcause::Report;
use tokio::net::{TcpListener, TcpStream};
use tokio::runtime::{Builder, Handle};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use super::SharedProxyStatus;
use super::{
    CacheabilityStore, ConnectionError, ConnectionResult, StatusSenderUpdater, connection_task,
};
use crate::admin::admin_server_spawn;
use crate::profiling::{runtime_build, runtime_shared_build};
use crate::result::{MapIntoReport, ReportExt, error_chain_format};
use crate::{
    cache::query::CacheableQuery,
    cache::{
        CacheDispatchHandle, CacheDispatchUpdater, PinnedQuery, cache_generation_start,
        cache_supervise,
    },
    catalog::{FunctionCatalog, FunctionVolatility, SharedFunctionCatalog},
    pg::{cdc::replication_cleanup, connect},
    query::ast::query_tree_convert_raw,
    settings::{Settings, SslMode},
    telemetry, tls,
};

fn tls_config_load(settings: &Settings) -> ConnectionResult<Option<Arc<tls::TlsAcceptor>>> {
    match (&settings.tls_cert, &settings.tls_key) {
        (Some(cert_path), Some(key_path)) => {
            debug!(
                "Loading TLS certificates from {:?} and {:?}",
                cert_path, key_path
            );
            let config = tls::server_tls_config_build(cert_path, key_path).map_err(|e| {
                Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                    "Failed to load TLS certificates: {e}"
                ))))
            })?;
            Ok(Some(Arc::new(tls::TlsAcceptor::from(config))))
        }
        (None, None) => {
            debug!("TLS not configured, accepting plaintext connections only");
            Ok(None)
        }
        _ => Err(ConnectionError::IoError(std::io::Error::other(
            "Both tls_cert and tls_key must be specified together",
        ))
        .into()),
    }
}

/// Load the function catalog from the origin database over a temporary
/// connection, as an immutable shared value.
async fn function_catalog_load(settings: &Settings) -> ConnectionResult<Arc<FunctionCatalog>> {
    let client = connect(&settings.origin, "function-catalog-load")
        .await
        .map_err(|e| {
            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                "function catalog load connection: {e}"
            ))))
        })?;
    let catalog = FunctionCatalog::load(&client).await.map_err(|e| {
        Report::from(ConnectionError::IoError(std::io::Error::other(format!(
            "function catalog load: {e}"
        ))))
    })?;
    let immutable = || {
        catalog
            .rows()
            .iter()
            .filter(|row| row.volatility == FunctionVolatility::Immutable)
    };
    info!(
        "loaded {} functions ({} immutable)",
        catalog.len(),
        immutable().count()
    );

    if tracing::enabled!(tracing::Level::TRACE) {
        let mut names: Vec<String> = immutable()
            .map(|row| format!("{}.{}", row.schema, row.name))
            .collect();
        names.sort_unstable();
        names.dedup();
        trace!("immutable functions: {}", names.join(", "));
    }

    Ok(Arc::new(catalog))
}

/// Parse and validate pinned queries at startup, returning only those that are cacheable.
fn pinned_queries_validate(
    settings: &Settings,
    function_catalog: &FunctionCatalog,
) -> Vec<PinnedQuery> {
    let Some(queries) = &settings.pinned_queries else {
        return Vec::new();
    };

    queries
        .iter()
        .filter_map(|sql| {
            let tree = match pg_query::parse_raw_scoped(sql, |tree| unsafe {
                query_tree_convert_raw(tree, sql.len())
            }) {
                Ok(Ok(q)) => q,
                Ok(Err(e)) => {
                    tracing::warn!("pinned query not convertible, skipping: {sql} ({e})");
                    return None;
                }
                Err(e) => {
                    tracing::warn!("pinned query not parseable, skipping: {sql} ({e})");
                    return None;
                }
            };
            let cacheable_query = match CacheableQuery::try_new(tree, function_catalog) {
                Ok(cq) => cq,
                Err(e) => {
                    tracing::warn!("pinned query not cacheable, skipping: {sql} ({e})");
                    return None;
                }
            };
            info!("pinned query validated: {sql}");
            Some(PinnedQuery {
                cacheable_query: Arc::new(cacheable_query),
            })
        })
        .collect()
}

#[tracing::instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub fn proxy_run(
    settings: &Settings,
    cancel: CancellationToken,
    shared_proxy_status: SharedProxyStatus,
    metrics_handle: PrometheusHandle,
) -> ConnectionResult<()> {
    // Load TLS config if certificates are provided
    let tls_acceptor = tls_config_load(settings)?;

    // Pre-scope setup: load function volatilities and validate pinned queries.
    // These must outlive thread::scope so spawned threads can borrow them.
    let pre_rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<ConnectionError>()?;
    // Replication provisioning now happens per cache generation in `cache_setup`,
    // so a restart re-establishes a slot the origin lost.
    let function_catalog: SharedFunctionCatalog = Arc::new(ArcSwap::new(
        pre_rt.block_on(function_catalog_load(settings))?,
    ));
    let pinned = pinned_queries_validate(settings, &function_catalog.load());
    drop(pre_rt);

    // One shared multi-thread runtime hosts connection tasks + worker serves
    // (PGC Option A probe). Writer and CDC remain dedicated threads.
    let worker_threads = settings.num_workers.max(2);
    let (profiling, rt) =
        runtime_shared_build(worker_threads).map_into_report::<ConnectionError>()?;

    let result = thread::scope(|scope| {
        let rt_handle = rt.handle().clone();

        // The cache publishes a CacheDispatch and a status sender via watch
        // channels; connection tasks and the admin server pick them up, and the
        // supervisor hot-swaps both across cache restarts. They start empty:
        // connections degrade to origin until the first generation publishes.
        let (dispatch_updater, dispatch_handle) = CacheDispatchUpdater::new();
        let (status_updater, status_sender) = StatusSenderUpdater::new_pending();

        let telemetry_metrics = metrics_handle.clone();
        let upkeep_metrics = metrics_handle.clone();
        if let Some(ref m) = settings.metrics {
            admin_server_spawn(
                m.socket,
                metrics_handle,
                cancel.child_token(),
                shared_proxy_status.clone(),
                status_sender,
                settings.dynamic.clone(),
            )
            .map_into_report::<ConnectionError>()
            .attach_loc("spawning admin server")?;
        }

        // Cancelled when this closure returns on any path, so a startup failure
        // doesn't leave the scope waiting on the background thread forever.
        let background_cancel = cancel.child_token();
        let _background_guard = background_cancel.clone().drop_guard();
        let background =
            background_spawn(scope, background_cancel).attach_loc("spawning background thread")?;

        telemetry::telemetry_spawn(
            settings.telemetry,
            cancel.child_token(),
            telemetry_metrics,
            &background,
        );

        // Build the first cache generation fail-fast, before accepting traffic,
        // so "Listening" implies the cache is up (the supervisor then handles
        // restarts for later generations). A failure here fails startup.
        let first_generation = cache_generation_start(
            scope,
            settings,
            rt_handle.clone(),
            &pinned,
            &function_catalog,
            &cancel,
            &dispatch_updater,
            &status_updater,
        )
        .map_err(|e| {
            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                "cache setup failed: {}",
                e.current_context()
            ))))
        })
        .attach_loc("setting up cache")?;

        // One interned cacheability store for the whole process: analysis is a
        // pure function of the SQL text, so connections share one payload per
        // distinct text instead of each retaining its own AST.
        let cacheability_store = Arc::new(CacheabilityStore::new());

        metrics_upkeep_spawn(
            &background,
            Arc::clone(&cacheability_store),
            upkeep_metrics,
            cancel.child_token(),
        );

        let context = ConnectionContext {
            ssl_mode: settings.origin.ssl_mode,
            server_name: EcoString::from(settings.origin.host.as_str()),
            origin_database: EcoString::from(settings.origin.database.as_str()),
            dispatch_handle,
            tls_acceptor,
            function_catalog: Arc::clone(&function_catalog),
            cacheability_store,
        };

        // Accept connections on a dedicated scoped thread so the proxy thread is
        // free to run the cache restart supervisor. Connections dispatch against
        // the published CacheDispatch (degrading to origin while it is down), so the
        // accept loop is independent of cache-subsystem restarts. On exit it
        // cancels the proxy token so the supervisor unwinds too.
        let accept_cancel = cancel.clone();
        let accept_rt = rt_handle.clone();
        let accept_handle = thread::Builder::new()
            .name("accept".to_owned())
            .spawn_scoped(scope, move || {
                debug!("accept loop");
                let result = accept_rt.block_on(accept_run(
                    settings,
                    context,
                    shared_proxy_status,
                    accept_cancel.clone(),
                ));
                // Whether the accept loop stopped on shutdown or a startup error,
                // cancel the proxy token so the supervisor unwinds and the proxy
                // exits rather than restarting the cache forever against a dead
                // accept loop.
                accept_cancel.cancel();
                result
            })
            .map_into_report::<ConnectionError>()
            .attach_loc("spawning accept thread")?;

        // Supervise the cache subsystem on the proxy thread: build a generation,
        // wait for it to die, reap it, rebuild after backoff — until shutdown.
        cache_supervise(
            scope,
            settings,
            rt_handle,
            &pinned,
            &function_catalog,
            cancel,
            &dispatch_updater,
            &status_updater,
            first_generation,
        );

        // Shutdown: drain the accept loop and surface its result.
        accept_handle.join().unwrap_or_else(|_panic| {
            Err(ConnectionError::IoError(std::io::Error::other("accept thread panicked")).into())
        })
    });

    // On every way out of the scope, a startup that failed after provisioning
    // included. The scope has joined the CDC thread, so the stream's
    // connection is closed before the slot it held is dropped.
    let cleaned = rt
        .block_on(replication_cleanup(settings))
        .map_err(|r| r.context_transform(ConnectionError::CdcError))
        .attach_loc("cleaning up replication");

    drop(rt);
    profiling.shutdown();
    match (result, cleaned) {
        (Err(e), Err(cleanup)) => {
            warn!(
                "could not clean up replication on origin: {}",
                error_chain_format(cleanup.current_context()),
            );
            Err(e)
        }
        (result, cleaned) => result.and(cleaned),
    }
}

/// Per-connection values cloned into every accepted connection's task.
struct ConnectionContext {
    ssl_mode: SslMode,
    server_name: EcoString,
    origin_database: EcoString,
    dispatch_handle: CacheDispatchHandle,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    function_catalog: SharedFunctionCatalog,
    cacheability_store: Arc<CacheabilityStore>,
}

impl ConnectionContext {
    fn connection_spawn(&self, socket: TcpStream, addrs: &[SocketAddr]) {
        let _ = socket.set_nodelay(true);
        crate::metrics::handles().conn.total.increment(1);
        debug!("socket accepted");

        tokio::spawn(connection_task(
            socket,
            addrs.to_vec(),
            self.ssl_mode,
            self.server_name.clone(),
            self.dispatch_handle.clone(),
            self.tls_acceptor.clone(),
            Arc::clone(&self.function_catalog),
            self.origin_database.clone(),
            Arc::clone(&self.cacheability_store),
        ));
    }
}

/// Resolve the origin, bind the listener, and accept connections until
/// `cancel`.
async fn accept_run(
    settings: &Settings,
    context: ConnectionContext,
    status: SharedProxyStatus,
    cancel: CancellationToken,
) -> ConnectionResult<()> {
    #[cfg(all(feature = "taskdump", tokio_unstable))]
    taskdump_spawn();

    let addrs: Vec<SocketAddr> =
        tokio::net::lookup_host((settings.origin.host.as_str(), settings.origin.port))
            .await
            .map_into_report::<ConnectionError>()
            .attach_loc("resolving origin host")?
            .collect();

    let listener = TcpListener::bind(settings.listen.socket)
        .await
        .map_err(|e| {
            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                "bind error [{}] {e}",
                settings.listen.socket
            ))))
        })?;
    // Listener is bound — only now is the proxy ready to accept connections,
    // so flip `/readyz` to ready (it reports not-ready through the preceding
    // cache setup).
    status.listening_set();
    info!("Listening to {}", &settings.listen.socket);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                info!("proxy shutdown signal received");
                break;
            }
            result = listener.accept() => {
                let (socket, _) = result.map_err(|e| {
                    Report::from(ConnectionError::IoError(std::io::Error::other(
                        format!("accept error: {e}"),
                    )))
                })?;
                context.connection_spawn(socket, &addrs);
            }
        }
    }

    Ok(())
}

/// Start the `background` thread: a current-thread runtime for low-priority
/// periodic work (metrics upkeep, telemetry). It exits when `cancel` fires,
/// dropping whatever tasks are still on it.
fn background_spawn<'scope, 'env>(
    scope: &'scope thread::Scope<'scope, 'env>,
    cancel: CancellationToken,
) -> ConnectionResult<Handle> {
    let mut builder = Builder::new_current_thread();
    builder.enable_all();
    let rt = runtime_build(builder, "background").map_into_report::<ConnectionError>()?;
    let handle = rt.handle().clone();
    thread::Builder::new()
        .name("background".to_owned())
        .spawn_scoped(scope, move || rt.block_on(cancel.cancelled()))
        .map_into_report::<ConnectionError>()?;
    Ok(handle)
}

/// Periodic metrics maintenance, on a 1s tick, on the background thread.
///
/// Publishing the store's size here rather than on every membership change
/// keeps an all-shards operation off the churn path: `DashMap::len`
/// read-locks every shard (the trap `state_gauges_update` hit on the
/// writer). 1s is well below typical Prometheus scrape intervals.
///
/// The upkeep call is what bounds histogram memory. `Histogram::record`
/// appends raw (value, instant) pairs to an unbounded list and a
/// `/metrics` render is the only other drain, so without this a process
/// that is never scraped retains every sample it has ever recorded —
/// ~180 bytes per query, which is unbounded growth under load. Draining
/// every second also keeps each drain small and improves the time
/// attribution of samples into the exporter's rolling windows. The drain is
/// synchronous and takes 6-27ms at 20-80k qps, so it must stay off the
/// shared runtime, where it would stall a connection worker every tick.
fn metrics_upkeep_spawn(
    background: &Handle,
    store: Arc<CacheabilityStore>,
    metrics: PrometheusHandle,
    cancel: CancellationToken,
) {
    background.spawn(async move {
        let mut tick = tokio::time::interval(std::time::Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = cancel.cancelled() => break,
                _ = tick.tick() => {
                    store.gauge_publish();
                    metrics.run_upkeep();
                }
            }
        }
    });
}

/// Task-dump on SIGUSR2 (deadlock debugging). Build with
/// `RUSTFLAGS="--cfg tokio_unstable" cargo build --features taskdump`
/// (Linux x86_64/aarch64). On signal, logs every tokio task's suspended-await
/// backtrace — works even when the runtime is wedged, since SIGUSR2 wakes this
/// task via the io driver.
#[cfg(all(feature = "taskdump", tokio_unstable))]
fn taskdump_spawn() {
    let dump_handle = tokio::runtime::Handle::current();
    tokio::spawn(async move {
        let mut sig =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::user_defined2()) {
                Ok(s) => s,
                Err(e) => {
                    tracing::error!("taskdump: failed to install SIGUSR2 handler: {e}");
                    return;
                }
            };
        info!("taskdump: armed — send SIGUSR2 to dump tokio task backtraces");
        while sig.recv().await.is_some() {
            info!("taskdump: capturing task dump...");
            let dump = dump_handle.dump().await;
            let count = dump.tasks().iter().count();
            for (i, task) in dump.tasks().iter().enumerate() {
                info!("taskdump task[{i}] id={}:\n{}", task.id(), task.trace());
            }
            info!("taskdump: complete ({count} tasks)");
        }
    });
}

#[cfg(test)]
mod tests {
    use super::pinned_queries_validate;
    use crate::catalog::{FunctionCatalog, FunctionVolatility};
    use crate::settings::{
        CdcSettings, DynamicConfigHandle, ListenSettings, PgSettings, Settings, SslMode,
    };

    fn test_settings(pinned_queries: Option<Vec<String>>) -> Settings {
        let pg = PgSettings {
            host: "localhost".to_owned(),
            port: 5432,
            user: "test".to_owned(),
            password: None,
            database: "test".to_owned(),
            ssl_mode: SslMode::Disable,
        };
        Settings {
            origin: pg.clone(),
            replication: pg.clone(),
            cache: pg,
            cdc: CdcSettings {
                publication_name: "pub".to_owned(),
                slot_name: "slot".to_owned(),
            },
            listen: ListenSettings {
                socket: "127.0.0.1:5432".parse().expect("valid socket"),
            },
            num_workers: 1,
            population_workers_min: 2,
            population_workers_max: 16,
            role_poll_interval: std::time::Duration::ZERO,
            tls_cert: None,
            tls_key: None,
            metrics: None,
            dynamic: DynamicConfigHandle::test_default(),
            pinned_queries,
            telemetry: false,
        }
    }

    #[test]
    fn test_pinned_queries_validate_none_returns_empty() {
        let settings = test_settings(None);
        let result = pinned_queries_validate(&settings, &FunctionCatalog::default());
        assert!(result.is_empty());
    }

    #[test]
    fn test_pinned_queries_validate_valid_query_accepted() {
        let settings = test_settings(Some(vec![
            "SELECT id, name FROM users WHERE active = true".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &FunctionCatalog::default());
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_pinned_queries_validate_multiple_queries() {
        let settings = test_settings(Some(vec![
            "SELECT * FROM users".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &FunctionCatalog::default());
        assert_eq!(result.len(), 2);
        assert_ne!(
            result[0].cacheable_query.tree(),
            result[1].cacheable_query.tree()
        );
    }

    #[test]
    fn test_pinned_queries_validate_unparseable_skipped() {
        let settings = test_settings(Some(vec!["NOT VALID SQL !!!".to_owned()]));
        let result = pinned_queries_validate(&settings, &FunctionCatalog::default());
        assert!(result.is_empty());
    }

    #[test]
    fn test_pinned_queries_validate_non_cacheable_skipped() {
        // INSERT is not cacheable
        let settings = test_settings(Some(vec!["INSERT INTO users (id) VALUES (1)".to_owned()]));
        let result = pinned_queries_validate(&settings, &FunctionCatalog::default());
        assert!(result.is_empty());
    }

    #[test]
    fn test_pinned_queries_validate_mixed_valid_and_invalid() {
        let settings = test_settings(Some(vec![
            "SELECT * FROM users".to_owned(),
            "NOT VALID SQL".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &FunctionCatalog::default());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn test_pinned_queries_validate_non_cacheable_function_in_where_rejected() {
        let fv = FunctionCatalog::test_builtins([("random", FunctionVolatility::Volatile)]);

        // Volatile function in WHERE clause makes query non-cacheable
        let settings = test_settings(Some(vec![
            "SELECT id FROM users WHERE random() > 0.5".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &fv);
        assert!(result.is_empty());
    }
}
