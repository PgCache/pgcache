use std::{collections::HashMap, sync::Arc, thread};

use ecow::EcoString;

use metrics_exporter_prometheus::PrometheusHandle;
use rootcause::Report;

use crate::result::{MapIntoReport, ReportExt};
use tokio::{net::TcpListener, runtime::Builder};
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace};

use crate::metrics::admin_server_spawn;

use super::SharedProxyStatus;
use crate::{
    cache::query::CacheableQuery,
    cache::{CacheDispatchUpdater, PinnedQuery, cache_generation_start, cache_supervise},
    catalog::{FunctionVolatility, function_volatility_map_load},
    pg::{cdc::replication_cleanup, connect},
    query::ast::{query_expr_convert_raw, query_expr_fingerprint},
    settings::Settings,
    telemetry, tls,
};

use super::{ConnectionError, ConnectionResult, StatusSenderUpdater, connection_task};

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

/// Load function volatilities from origin database.
///
/// Opens a temporary connection to origin, queries pg_proc for all scalar
/// function volatilities, and returns an immutable shared map.
async fn function_volatility_load(
    settings: &Settings,
) -> ConnectionResult<Arc<HashMap<EcoString, FunctionVolatility>>> {
    let client = connect(&settings.origin, "volatility-load")
        .await
        .map_err(|e| {
            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                "volatility load connection: {e}"
            ))))
        })?;
    let map = function_volatility_map_load(&client).await.map_err(|e| {
        Report::from(ConnectionError::IoError(std::io::Error::other(format!(
            "volatility map load: {e}"
        ))))
    })?;
    let immutable_count = map
        .iter()
        .filter(|(_, v)| matches!(v, FunctionVolatility::Immutable))
        .count();
    info!(
        "loaded {} function volatilities ({immutable_count} immutable)",
        map.len()
    );

    if tracing::enabled!(tracing::Level::TRACE) {
        let mut names: Vec<&str> = map
            .iter()
            .filter(|(_, v)| matches!(v, FunctionVolatility::Immutable))
            .map(|(k, _)| k.as_str())
            .collect();
        names.sort_unstable();
        trace!("immutable functions: {}", names.join(", "));
    }

    Ok(Arc::new(map))
}

/// Parse and validate pinned queries at startup, returning only those that are cacheable.
fn pinned_queries_validate(
    settings: &Settings,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Vec<PinnedQuery> {
    let Some(queries) = &settings.pinned_queries else {
        return Vec::new();
    };

    queries
        .iter()
        .filter_map(|sql| {
            let query_expr = match pg_query::parse_raw_scoped(sql, |tree| unsafe {
                query_expr_convert_raw(tree)
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
            // Fingerprint before try_new takes ownership of query_expr.
            let fingerprint = query_expr_fingerprint(&query_expr);
            let cacheable_query = match CacheableQuery::try_new(query_expr, func_volatility) {
                Ok(cq) => cq,
                Err(e) => {
                    tracing::warn!("pinned query not cacheable, skipping: {sql} ({e})");
                    return None;
                }
            };
            info!("pinned query validated: {sql} (fingerprint: {fingerprint})");
            Some(PinnedQuery {
                fingerprint,
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
    let func_volatility = pre_rt.block_on(function_volatility_load(settings))?;
    let pinned = pinned_queries_validate(settings, &func_volatility);
    drop(pre_rt);

    // One shared multi-thread runtime hosts connection tasks + worker serves
    // (PGC Option A probe). Writer and CDC remain dedicated threads.
    let worker_threads = settings.num_workers.max(2);
    let rt = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()
        .map_into_report::<ConnectionError>()?;

    thread::scope(|scope| {
        let rt_handle = rt.handle().clone();

        // The cache publishes a CacheDispatch and a status sender via watch
        // channels; connection tasks and the admin server pick them up, and the
        // supervisor hot-swaps both across cache restarts. They start empty:
        // connections degrade to origin until the first generation publishes.
        let (dispatch_updater, dispatch_handle) = CacheDispatchUpdater::new();
        let (status_updater, status_sender) = StatusSenderUpdater::new_pending();

        let telemetry_metrics = metrics_handle.clone();
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

        telemetry::telemetry_spawn(settings.telemetry, cancel.child_token(), telemetry_metrics)
            .map_into_report::<ConnectionError>()
            .attach_loc("spawning telemetry thread")?;

        // Build the first cache generation fail-fast, before accepting traffic,
        // so "Listening" implies the cache is up (the supervisor then handles
        // restarts for later generations). A failure here fails startup.
        let first_generation = cache_generation_start(
            scope,
            settings,
            rt_handle.clone(),
            &pinned,
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

        // Origin connection params, resolved once and cloned into each task.
        let ssl_mode = settings.origin.ssl_mode;
        let server_name = EcoString::from(settings.origin.host.as_str());
        let origin_database = EcoString::from(settings.origin.database.as_str());

        // Accept connections on a dedicated scoped thread so the proxy thread is
        // free to run the cache restart supervisor. Connections dispatch against
        // the published CacheDispatch (degrading to origin while it is down), so the
        // accept loop is independent of cache-subsystem restarts. On exit it
        // cancels the proxy token so the supervisor unwinds too.
        let accept_cancel = cancel.clone();
        let accept_rt = rt_handle.clone();
        let accept_status = shared_proxy_status;
        let accept_handle = thread::Builder::new()
            .name("accept".to_owned())
            .spawn_scoped(scope, move || {
                debug!("accept loop");
                let result = accept_rt.block_on(async {
                    // Task-dump on SIGUSR2 (deadlock debugging). Build with
                    // `RUSTFLAGS="--cfg tokio_unstable" cargo build --features taskdump`
                    // (Linux x86_64/aarch64). On signal, logs every tokio task's
                    // suspended-await backtrace — works even when the runtime is
                    // wedged, since SIGUSR2 wakes this task via the io driver.
                    #[cfg(all(feature = "taskdump", tokio_unstable))]
                    {
                        let dump_handle = tokio::runtime::Handle::current();
                        tokio::spawn(async move {
                            let mut sig = match tokio::signal::unix::signal(
                                tokio::signal::unix::SignalKind::user_defined2(),
                            ) {
                                Ok(s) => s,
                                Err(e) => {
                                    tracing::error!(
                                        "taskdump: failed to install SIGUSR2 handler: {e}"
                                    );
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

                    let addrs: Vec<std::net::SocketAddr> = tokio::net::lookup_host((
                        settings.origin.host.as_str(),
                        settings.origin.port,
                    ))
                    .await
                    .map_into_report::<ConnectionError>()
                    .attach_loc("resolving origin host")?
                    .collect();

                    let listener =
                        TcpListener::bind(&settings.listen.socket)
                            .await
                            .map_err(|e| {
                                Report::from(ConnectionError::IoError(std::io::Error::other(
                                    format!("bind error [{}] {e}", &settings.listen.socket),
                                )))
                            })?;
                    // Listener is bound — only now is the proxy ready to accept
                    // connections, so flip `/readyz` to ready (it reports
                    // not-ready through the preceding cache setup).
                    accept_status.listening_set();
                    info!("Listening to {}", &settings.listen.socket);

                    loop {
                        tokio::select! {
                            _ = accept_cancel.cancelled() => {
                                info!("proxy shutdown signal received");
                                break;
                            }
                            result = listener.accept() => {
                                let (socket, _) = result.map_err(|e| {
                                    Report::from(ConnectionError::IoError(std::io::Error::other(
                                        format!("accept error: {e}"),
                                    )))
                                })?;
                                let _ = socket.set_nodelay(true);
                                crate::metrics::handles().conn.total.increment(1);
                                debug!("socket accepted");

                                tokio::spawn(connection_task(
                                    socket,
                                    addrs.clone(),
                                    ssl_mode,
                                    server_name.clone(),
                                    dispatch_handle.clone(),
                                    tls_acceptor.clone(),
                                    Arc::clone(&func_volatility),
                                    origin_database.clone(),
                                ));
                            }
                        }
                    }

                    replication_cleanup(settings)
                        .await
                        .map_err(|r| r.context_transform(ConnectionError::CdcError))
                        .attach_loc("cleaning up replication")?;
                    Ok(())
                });
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
            cancel,
            &dispatch_updater,
            &status_updater,
            first_generation,
        );

        // Shutdown: drain the accept loop and surface its result.
        accept_handle.join().unwrap_or_else(|_panic| {
            Err(ConnectionError::IoError(std::io::Error::other("accept thread panicked")).into())
        })
    })
}

#[cfg(test)]
mod tests {

    use std::collections::HashMap;

    use crate::catalog::FunctionVolatility;
    use crate::settings::{
        CdcSettings, DynamicConfigHandle, ListenSettings, PgSettings, Settings, SslMode,
    };

    use super::pinned_queries_validate;

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
            tls_cert: None,
            tls_key: None,
            metrics: None,
            dynamic: DynamicConfigHandle::test_default(),
            pinned_queries,
            telemetry: false,
        }
    }

    #[test]
    fn pinned_queries_validate_none_returns_empty() {
        let settings = test_settings(None);
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn pinned_queries_validate_valid_query_accepted() {
        let settings = test_settings(Some(vec![
            "SELECT id, name FROM users WHERE active = true".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert_eq!(result.len(), 1);
        assert_ne!(result[0].fingerprint, 0);
    }

    #[test]
    fn pinned_queries_validate_multiple_queries() {
        let settings = test_settings(Some(vec![
            "SELECT * FROM users".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert_eq!(result.len(), 2);
        // Different queries should have different fingerprints
        assert_ne!(result[0].fingerprint, result[1].fingerprint);
    }

    #[test]
    fn pinned_queries_validate_unparseable_skipped() {
        let settings = test_settings(Some(vec!["NOT VALID SQL !!!".to_owned()]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn pinned_queries_validate_non_cacheable_skipped() {
        // INSERT is not cacheable
        let settings = test_settings(Some(vec!["INSERT INTO users (id) VALUES (1)".to_owned()]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert!(result.is_empty());
    }

    #[test]
    fn pinned_queries_validate_mixed_valid_and_invalid() {
        let settings = test_settings(Some(vec![
            "SELECT * FROM users".to_owned(),
            "NOT VALID SQL".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &HashMap::new());
        assert_eq!(result.len(), 2);
    }

    #[test]
    fn pinned_queries_validate_non_cacheable_function_in_where_rejected() {
        let mut fv = HashMap::new();
        fv.insert("random".into(), FunctionVolatility::Volatile);

        // Volatile function in WHERE clause makes query non-cacheable
        let settings = test_settings(Some(vec![
            "SELECT id FROM users WHERE random() > 0.5".to_owned(),
        ]));
        let result = pinned_queries_validate(&settings, &fv);
        assert!(result.is_empty());
    }
}
