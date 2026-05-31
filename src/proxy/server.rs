use std::{collections::HashMap, mem, sync::Arc, thread, time::Duration};

use ecow::EcoString;

use metrics_exporter_prometheus::PrometheusHandle;
use rootcause::Report;

use crate::result::{MapIntoReport, ReportExt};
use tokio::{
    net::{TcpListener, TcpStream},
    runtime::Builder,
    sync::mpsc::{Receiver, UnboundedSender, channel, unbounded_channel},
    time::{Sleep, sleep},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, trace};

use crate::cache::StatusRequest;
use crate::metrics::{admin_server_spawn, names};

/// Initial backoff delay for cache restart attempts
const INITIAL_BACKOFF_MS: u64 = 100;
/// Maximum backoff delay (steady state) for cache restart attempts
const STEADY_STATE_BACKOFF_MS: u64 = 60_000;
/// Minimum buffer size for the proxy→cache command channel.
const MIN_CHANNEL_SIZE: usize = 100;

struct ProxyCacheState<'scope> {
    handle: Option<thread::ScopedJoinHandle<'scope, CacheResult<()>>>,
    updater: CacheSenderUpdater,
    status_updater: StatusSenderUpdater,
    /// Current sender for detecting when the cache thread exits via `.closed()`.
    current_tx: CacheSenderInner,
    shared_status: SharedProxyStatus,
    alive: bool,
    attempts: u32,
    backoff: Option<std::pin::Pin<Box<Sleep>>>,
    backoff_ms: u64,
}

impl<'scope> ProxyCacheState<'scope> {
    fn new(
        handle: thread::ScopedJoinHandle<'scope, CacheResult<()>>,
        updater: CacheSenderUpdater,
        status_updater: StatusSenderUpdater,
        current_tx: CacheSenderInner,
        shared_status: SharedProxyStatus,
    ) -> Self {
        Self {
            handle: Some(handle),
            updater,
            status_updater,
            current_tx,
            shared_status,
            alive: true,
            attempts: 0,
            backoff: None,
            backoff_ms: INITIAL_BACKOFF_MS,
        }
    }

    fn handle_exit(&mut self) {
        self.alive = false;
        self.attempts += 1;
        self.shared_status.status_set(ProxyStatus::Degraded);

        // Mark cache as unavailable immediately so connections fall back to origin
        self.updater.sender_clear();
        self.status_updater.sender_clear();

        let attempts = self.attempts;
        match self.handle.take().map(|h| h.join()) {
            None => error!("cache thread exited (attempt {attempts}): no join handle"),
            Some(Err(_panic)) => {
                error!("cache thread exited (attempt {attempts}): thread panicked")
            }
            Some(Ok(Ok(()))) => {
                error!("cache thread exited (attempt {attempts}): unexpected clean exit")
            }
            Some(Ok(Err(e))) => error!("cache thread exited (attempt {attempts}): {e}"),
        }

        self.backoff_schedule();
    }

    async fn handle_backoff_expired<'env: 'scope, 'settings: 'scope>(
        &mut self,
        scope: &'scope thread::Scope<'scope, 'env>,
        settings: &'settings Settings,
        pinned: &'settings [PinnedQuery],
        cancel: CancellationToken,
    ) {
        self.backoff = None;
        self.attempts += 1;

        debug!(
            "backoff expired, attempting restart (attempt {})",
            self.attempts
        );

        // Create a fresh status channel for the restarted cache
        let (new_status_tx, status_rx) = channel::<StatusRequest>(2);

        if let Some((new_handle, new_tx)) =
            cache_restart_attempt(scope, settings, pinned, cancel, status_rx).await
        {
            self.handle = Some(new_handle);
            // Update all subscribers with the new cache and status senders
            self.updater.sender_update(new_tx.clone());
            self.status_updater.sender_update(new_status_tx);
            self.current_tx = new_tx;
            self.restart_reset();
            debug!("cache thread restarted successfully");
        } else {
            self.backoff_increase();
            debug!(
                "restart failed, backing off {}ms before next attempt",
                self.backoff_ms
            );
            self.backoff_schedule();
        }
    }

    fn restart_reset(&mut self) {
        self.alive = true;
        self.attempts = 0;
        self.backoff_ms = INITIAL_BACKOFF_MS;
        self.shared_status.status_set(ProxyStatus::Normal);
    }

    fn backoff_schedule(&mut self) {
        self.backoff = Some(Box::pin(sleep(Duration::from_millis(self.backoff_ms))));
    }

    fn backoff_increase(&mut self) {
        self.backoff_ms = (self.backoff_ms * 2).min(STEADY_STATE_BACKOFF_MS);
    }

    async fn backoff_wait(backoff: &mut Option<std::pin::Pin<Box<Sleep>>>) {
        match backoff {
            Some(sleep) => sleep.as_mut().await,
            None => std::future::pending().await,
        }
    }
}

use super::SharedProxyStatus;
use crate::{
    cache::query::CacheableQuery,
    cache::{CacheResult, PinnedQuery, cache_run},
    catalog::{FunctionVolatility, function_volatility_map_load},
    pg::{
        cdc::{replication_cleanup, replication_provision},
        connect,
    },
    query::ast::{query_expr_convert_raw, query_expr_fingerprint},
    settings::Settings,
    telemetry, tls,
};

use super::cache_sender::CacheSenderInner;
use super::{
    CacheSender, CacheSenderUpdater, ConnectionError, ConnectionResult, ProxyStatus,
    StatusSenderUpdater, connection_run,
};

type Worker<'scope> = (
    thread::ScopedJoinHandle<'scope, ConnectionResult<()>>,
    UnboundedSender<TcpStream>,
);

/// Shared resources passed to worker threads.
#[derive(Clone)]
struct WorkerResources {
    cache_sender: CacheSender,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
}

fn worker_create<'scope, 'env: 'scope, 'settings: 'scope>(
    worker_id: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    resources: WorkerResources,
) -> ConnectionResult<Worker<'scope>> {
    let (tx, rx) = unbounded_channel::<TcpStream>();
    let join = thread::Builder::new()
        .name(format!("cnxt {worker_id}"))
        .spawn_scoped(scope, move || {
            connection_run(
                worker_id,
                settings,
                rx,
                resources.cache_sender,
                resources.tls_acceptor,
                resources.func_volatility,
            )
        })
        .map_into_report::<ConnectionError>()?;

    Ok((join, tx))
}

fn workers_create_all<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    resources: WorkerResources,
) -> ConnectionResult<Vec<Worker<'scope>>> {
    (0..settings.num_workers)
        .map(|i| worker_create(i, scope, settings, resources.clone()))
        .collect()
}

fn worker_ensure_alive<'scope, 'env: 'scope, 'settings: 'scope>(
    workers: &mut [Worker<'scope>],
    worker_index: usize,
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    resources: WorkerResources,
) -> ConnectionResult<()> {
    let Some(worker) = workers.get_mut(worker_index) else {
        return Err(
            ConnectionError::IoError(std::io::Error::other("worker index out of bounds")).into(),
        );
    };

    if worker.0.is_finished() {
        let new_worker = worker_create(worker_index, scope, settings, resources)?;
        let old_worker = mem::replace(worker, new_worker);
        let _ = old_worker.0.join();
    }

    Ok(())
}

type Cache<'scope> = (
    thread::ScopedJoinHandle<'scope, CacheResult<()>>,
    CacheSenderInner,
);

fn cache_create<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    pinned: &'settings [PinnedQuery],
    cancel: CancellationToken,
    status_rx: Receiver<StatusRequest>,
) -> Result<Cache<'scope>, std::io::Error> {
    let channel_size = (settings.num_workers * 50).max(MIN_CHANNEL_SIZE);
    let (cache_tx, cache_rx) = channel(channel_size);

    let cache_handle = thread::Builder::new()
        .name("cache".to_owned())
        .spawn_scoped(scope, || {
            cache_run(settings, cache_rx, pinned, cancel, status_rx)
        })?;

    Ok((cache_handle, cache_tx))
}

/// Attempts to restart the cache thread.
/// On success, returns Some with the new cache handle and sender.
/// On failure, returns None.
async fn cache_restart_attempt<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    pinned: &'settings [PinnedQuery],
    cancel: CancellationToken,
    status_rx: Receiver<StatusRequest>,
) -> Option<Cache<'scope>> {
    if let Err(e) = replication_provision(settings).await {
        error!("replication provision failed: {:?}", e);
        return None;
    }

    match cache_create(scope, settings, pinned, cancel, status_rx) {
        Ok(cache) => Some(cache),
        Err(e) => {
            error!("cache creation failed: {:?}", e);
            None
        }
    }
}

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
            let cacheable_query = match CacheableQuery::try_new(&query_expr, func_volatility) {
                Ok(cq) => cq,
                Err(e) => {
                    tracing::warn!("pinned query not cacheable, skipping: {sql} ({e})");
                    return None;
                }
            };
            let fingerprint = query_expr_fingerprint(&query_expr);
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
    let _ = pre_rt.block_on(async { replication_provision(settings).await });
    let func_volatility = pre_rt.block_on(function_volatility_load(settings))?;
    let pinned = pinned_queries_validate(settings, &func_volatility);
    drop(pre_rt);

    thread::scope(|scope| {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_into_report::<ConnectionError>()?;

        let cache_cancel = cancel.child_token();

        // Create status channel for admin HTTP → cache writer communication
        let (status_tx, status_rx) = channel::<StatusRequest>(2);

        let (cache_handle, cache_tx) =
            cache_create(scope, settings, &pinned, cache_cancel.clone(), status_rx)
                .map_into_report::<ConnectionError>()
                .attach_loc("creating cache thread")?;
        let (updater, cache_sender) = CacheSenderUpdater::new(cache_tx.clone());
        let (status_updater, status_sender) = StatusSenderUpdater::new(status_tx);
        let mut cache_state = ProxyCacheState::new(
            cache_handle,
            updater,
            status_updater,
            cache_tx,
            shared_proxy_status.clone(),
        );

        // Clone metrics handle for telemetry before admin server takes ownership
        let telemetry_metrics = metrics_handle.clone();

        // Spawn admin HTTP server now that the status channel is available
        if let Some(ref m) = settings.metrics {
            admin_server_spawn(
                m.socket,
                metrics_handle,
                cancel.child_token(),
                shared_proxy_status,
                status_sender,
                settings.dynamic.clone(),
            )
            .map_into_report::<ConnectionError>()
            .attach_loc("spawning admin server")?;
        }

        // Spawn telemetry background thread
        telemetry::telemetry_spawn(settings.telemetry, cancel.child_token(), telemetry_metrics)
            .map_into_report::<ConnectionError>()
            .attach_loc("spawning telemetry thread")?;

        let resources = WorkerResources {
            cache_sender,
            tls_acceptor,
            func_volatility,
        };

        let mut workers = workers_create_all(scope, settings, resources.clone())?;

        debug!("accept loop");
        rt.block_on(async {
            let listener = TcpListener::bind(&settings.listen.socket)
                .await
                .map_err(|e| {
                    Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                        "bind error [{}] {e}",
                        &settings.listen.socket
                    ))))
                })?;
            info!("Listening to {}", &settings.listen.socket);

            let mut cur_worker = 0;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        info!("proxy shutdown signal received");
                        break;
                    }
                    // Branch 1: Accept new connections
                    result = listener.accept() => {
                        let (socket, _) = result.map_err(|e| {
                            Report::from(ConnectionError::IoError(std::io::Error::other(format!(
                                "accept error: {e}"
                            ))))
                        })?;
                        let _ = socket.set_nodelay(true);
                        metrics::counter!(names::CONNECTIONS_TOTAL).increment(1);
                        debug!("socket accepted");

                        if let Some(worker) = workers.get(cur_worker) {
                            let _ = worker.1.send(socket);
                        }

                        worker_ensure_alive(
                            &mut workers,
                            cur_worker,
                            scope,
                            settings,
                            WorkerResources {
                                cache_sender: cache_state.updater.sender_subscribe(),
                                ..resources.clone()
                            },
                        )?;

                        cur_worker = (cur_worker + 1) % settings.num_workers;
                    }

                    // Branch 2: Cache thread exited - initiate restart
                    _ = cache_state.current_tx.closed(), if cache_state.alive => {
                        cache_state.handle_exit();
                    }

                    // Branch 3: Backoff timer expired - retry restart
                    _ = ProxyCacheState::backoff_wait(&mut cache_state.backoff) => {
                        cache_state
                            .handle_backoff_expired(scope, settings, &pinned, cache_cancel.child_token())
                            .await;
                    }
                }
            }

            replication_cleanup(settings)
                .await
                .map_err(|r| r.context_transform(ConnectionError::CdcError))
                .attach_loc("cleaning up replication")?;
            Ok(())
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
