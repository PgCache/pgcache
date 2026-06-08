use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use tokio::{
    runtime::{Builder, Handle},
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
};
use tokio_postgres::{Config, NoTls};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    cache::{
        CacheDispatchPublisher, CacheDispatchUpdater, CacheError, CacheResult, MapIntoReport,
        PinnedQuery, ReportExt, StatusRequest,
        cdc::CdcProcessor,
        messages::{CacheOutcome, CacheReply, CdcCommand, WriterNotify, slices_concat},
        query_cache::{CacheDispatch, ServeRequest},
        serve::{CoalescedOutcome, SQLSTATE_UNDEFINED_TABLE, handle_cached_query},
        types::{ActiveRelations, CacheStateView},
        writer::writer_run,
    },
    pg::{
        cache_connection::CacheConnection,
        cdc::{replication_provision, slot_confirmed_lsn},
    },
    proxy::StatusSenderUpdater,
    result::error_chain_format,
    settings::Settings,
    timing::duration_to_us_u64,
};

/// Minimum number of connections in the cache serve pool.
const MIN_POOL_SIZE: usize = 4;

/// Initial backoff before retrying a serve-pool reconnection.
const POOL_REPLENISH_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Maximum backoff between serve-pool reconnection attempts.
const POOL_REPLENISH_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// Handles a serve request by executing the query and sending the reply.
/// Sends replies for both the primary client and any coalesced clients.
async fn handle_serve_request(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    replenish_tx: UnboundedSender<()>,
    mut msg: ServeRequest,
    state_view: Arc<CacheStateView>,
) {
    debug!("cache serve task spawn");

    msg.timing.worker_start_at = Some(Instant::now());

    let reply =
        match handle_cached_query(conn, return_tx, replenish_tx, &mut msg, &state_view).await {
            Ok((bytes_served, coalesced_outcomes)) => {
                let latency_us = msg
                    .timing
                    .worker_start_at
                    .map(|s| duration_to_us_u64(s.elapsed()))
                    .unwrap_or(0);
                // Record directly in the shared view (no extra hop).
                serve_metrics_record(
                    &state_view,
                    msg.fingerprint,
                    latency_us,
                    bytes_served as u64,
                );

                // Send replies to coalesced clients, returning each leased socket.
                for outcome in coalesced_outcomes {
                    match outcome {
                        CoalescedOutcome::Complete(client) => {
                            let _ = client.reply_tx.send(CacheReply {
                                socket: client.client_socket,
                                outcome: CacheOutcome::Complete(Some(client.timing)),
                            });
                        }
                        CoalescedOutcome::Failed(client) => {
                            let _ = client.reply_tx.send(CacheReply {
                                socket: client.client_socket,
                                outcome: CacheOutcome::Error(client.data),
                            });
                        }
                    }
                }

                CacheReply {
                    socket: msg.client_socket,
                    outcome: CacheOutcome::Complete(Some(msg.timing)),
                }
            }
            Err(e) => {
                // 42P01 is the expected eviction-window race; other SQLSTATEs are bugs.
                let ctx = e.current_context();
                let undefined_table = matches!(
                    ctx,
                    CacheError::CacheServerError { sqlstate: Some(s) }
                        if *s == SQLSTATE_UNDEFINED_TABLE
                );
                if undefined_table {
                    debug!("cache hit fell through to origin (table dropped during eviction)");
                } else {
                    error!("handle_cached_query failed: {}", error_chain_format(ctx));
                }
                // Coalesced clients already received Error replies inside the serve path
                let error_buf = msg
                    .forward_bytes
                    .take()
                    .map_or_else(|| msg.data.split_off(0), |slices| slices_concat(&slices));
                CacheReply {
                    socket: msg.client_socket,
                    outcome: CacheOutcome::Error(error_buf),
                }
            }
        };

    if msg.reply_tx.send(reply).is_err() {
        error!("failed to send reply: no receiver");
    }

    debug!("cache serve task done");
}

/// Creates cache database connections and returns them as a channel pair.
/// Connections are immediately available in the receiver.
async fn connection_pool_create(
    settings: &Settings,
    size: usize,
) -> CacheResult<(Sender<CacheConnection>, Receiver<CacheConnection>)> {
    let (tx, rx) = channel(size);

    for i in 0..size {
        debug!(
            "Creating connection {}/{} to cache db at {}:{}",
            i + 1,
            size,
            settings.cache.host,
            settings.cache.port
        );

        let conn = CacheConnection::connect(&settings.cache)
            .await
            .attach_loc("creating cache connection")?;

        tx.send(conn).await.map_err(|_| CacheError::NoConnection)?;
    }

    debug!("Created {} connections", size);
    Ok((tx, rx))
}

/// Reset the cache database by dropping and recreating it
fn cache_database_reset(settings: &Settings) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    rt.block_on(async {
        // Connect to postgres maintenance database
        let (admin_client, admin_conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname("postgres")
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = admin_conn.await {
                error!("admin connection error: {}", error_chain_format(&e));
            }
        });

        let db_name = &settings.cache.database;
        debug!("resetting cache database: {db_name}");

        // Terminate existing connections to the database
        admin_client
            .execute(
                &format!(
                    "SELECT pg_terminate_backend(pid) FROM pg_stat_activity \
                     WHERE datname = '{db_name}' AND pid <> pg_backend_pid()"
                ),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("terminating existing connections")?;

        admin_client
            .execute(&format!("DROP DATABASE IF EXISTS {db_name}"), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("dropping cache database")?;

        admin_client
            .execute(&format!("CREATE DATABASE {db_name}"), &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating cache database")?;

        // Disable IndexOnlyScan on the cache db (PGC-100). pgcache_pgrx's
        // tracker handles IOS correctly via a TID-based heap fetch, but that
        // path defeats IOS's heap-skipping benefit; on the cache db's mostly-
        // all-visible local heap, IOS isn't worth the slot-shape complexity.
        admin_client
            .execute(
                &format!("ALTER DATABASE {db_name} SET enable_indexonlyscan = off"),
                &[],
            )
            .await
            .map_into_report::<CacheError>()
            .attach_loc("disabling enable_indexonlyscan on cache database")?;

        // Connect to fresh cache database and create extension
        let (cache_client, cache_conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(db_name)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;

        tokio::spawn(async move {
            if let Err(e) = cache_conn.await {
                error!("cache connection error: {}", error_chain_format(&e));
            }
        });

        cache_client
            .execute("CREATE EXTENSION pg_stat_statements", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pg_stat_statements extension")?;
        cache_client
            .execute("CREATE EXTENSION pgcache_pgrx", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_pgrx extension")?;

        // Dedicated schema for materialized query results. Tables here are named
        // pgcache_mv.q_<fingerprint> and are managed by the MV subsystem (population,
        // rebuild, eviction). Not pgrx-tracked — consistency is managed via MvState.
        cache_client
            .execute("CREATE SCHEMA IF NOT EXISTS pgcache_mv", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_mv schema")?;

        // Dedicated schema for population staging tables (PGC-250). A population
        // streams its origin snapshot into pgcache_stage.stage_<fp>_<gen>_<oid>,
        // then the writer merges it into the shared cache table (filtering rows
        // CDC removed during the population) when no CDC frame is open. Regular
        // tables (not temp) so the writer's connection can read what a worker
        // connection loaded. Swept by the DROP DATABASE on reset, like pgcache_mv.
        cache_client
            .execute("CREATE SCHEMA IF NOT EXISTS pgcache_stage", &[])
            .await
            .map_into_report::<CacheError>()
            .attach_loc("creating pgcache_stage schema")?;

        Ok(())
    })
}

/// Build one generation of the cache subsystem.
///
/// Writer and CDC are dedicated `current_thread` threads (mutation
/// serialization point + replication consumer, reached only via `Send`
/// channels). The serve loop and coalesce-drain run as tasks on the shared
/// multi-thread runtime (`handle`) alongside the connection tasks, so the
/// connection ↔ serve handoffs are intra-runtime run-queue pushes instead of
/// cross-runtime eventfd wakeups.
///
/// `cache_cancel` is owned by [`cache_supervise`]; any fatal failure in this
/// generation cancels it, which tears down the writer/cdc threads and the
/// runtime tasks. The returned scoped join handles let the supervisor reap the
/// dead generation before building the next one. On partial-spawn failure the
/// already-spawned threads are cancelled and joined before returning `Err`.
#[instrument(skip_all)]
#[allow(clippy::too_many_arguments)]
pub fn cache_setup<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    handle: Handle,
    pinned: &[PinnedQuery],
    cache_cancel: CancellationToken,
    status_rx: Receiver<StatusRequest>,
    dispatch_publisher: CacheDispatchPublisher,
) -> CacheResult<Vec<thread::ScopedJoinHandle<'scope, CacheResult<()>>>> {
    // Provision replication per generation: idempotent for the slot (created only
    // if missing) and recreating the publication empty. On a restart this rebuilds
    // a slot the origin lost, so the CDC thread can resume rather than zombie; if
    // the origin is unreachable, this fails and the supervisor backs off and
    // retries instead of bringing up a generation with dead CDC.
    handle
        .block_on(replication_provision(settings))
        .map_err(|r| r.context_transform(|_| CacheError::CdcFailure))
        .attach_loc("provisioning replication on origin")?;

    cache_database_reset(settings).attach_loc("resetting cache database")?;

    let state_view = Arc::new(CacheStateView::new(settings.dynamic.clone()));
    let active_relations: ActiveRelations =
        Arc::new(ArcSwap::from_pointee(std::collections::HashSet::new()));

    let (query_tx, query_rx) = unbounded_channel();
    let (cdc_cmd_tx, cdc_cmd_rx) = unbounded_channel();
    let (notify_tx, notify_rx) = unbounded_channel::<WriterNotify>();
    let (serve_tx, serve_rx) = unbounded_channel();

    // CDC-liveness flag: set by the CDC thread, read inline by every dispatch.
    let cdc_connected = Arc::new(AtomicBool::new(true));

    // Writer thread (owns Cache, serializes all mutations). Gets a child of the
    // subsystem cancel so a subsystem teardown propagates to it.
    let settings_writer = settings.clone();
    let state_view_writer = Arc::clone(&state_view);
    let active_relations_writer = Arc::clone(&active_relations);
    let cancel_writer = cache_cancel.child_token();
    let writer_handle = thread::Builder::new()
        .name("cache writer".to_owned())
        .spawn_scoped(scope, move || {
            let result = writer_run(
                &settings_writer,
                query_rx,
                cdc_cmd_rx,
                state_view_writer,
                active_relations_writer,
                notify_tx,
                cancel_writer,
                status_rx,
            );
            if let Err(ref e) = result {
                error!(
                    "writer thread exiting with error: {}",
                    error_chain_format(e.current_context()),
                );
            }
            result
        })
        .map_into_report::<CacheError>()
        .attach_loc("spawning writer thread")?;

    // CDC thread (sends CdcCommand to writer, sets the cdc_connected flag).
    // Holds the subsystem cancel (not a child) so a fatal CDC error tears down
    // the whole cache subsystem.
    let active_relations_cdc = Arc::clone(&active_relations);
    let cancel_cdc = cache_cancel.clone();
    let cdc_connected_cdc = Arc::clone(&cdc_connected);
    let cdc_handle = match thread::Builder::new()
        .name("cdc worker".to_owned())
        .spawn_scoped(scope, move || {
            let result = cdc_run(
                settings,
                cdc_cmd_tx,
                active_relations_cdc,
                cancel_cdc,
                cdc_connected_cdc,
            );
            if let Err(ref e) = result {
                error!(
                    "cdc thread exiting with error: {}",
                    error_chain_format(e.current_context()),
                );
            }
            result
        }) {
        Ok(h) => h,
        Err(e) => {
            // Writer is already up; tear it down and reap before bailing.
            cache_cancel.cancel();
            let _ = writer_handle.join();
            return Err(e)
                .map_into_report::<CacheError>()
                .attach_loc("spawning CDC thread");
        }
    };

    // Remaining fallible setup; on error, tear down both threads and reap.
    let dispatch = match handle
        .block_on(CacheDispatch::new(
            settings,
            query_tx,
            serve_tx,
            Arc::clone(&state_view),
            cdc_connected,
        ))
        .attach_loc("creating query cache")
        .and_then(|dispatch| {
            dispatch
                .pinned_queries_register(pinned)
                .attach_loc("registering pinned queries")?;
            Ok(dispatch)
        }) {
        Ok(dispatch) => dispatch,
        Err(e) => {
            cache_cancel.cancel();
            let _ = writer_handle.join();
            let _ = cdc_handle.join();
            return Err(e);
        }
    };

    // Publish the cache for connection tasks to dispatch against inline.
    dispatch_publisher.publish(dispatch.clone());

    // Worker dispatcher serves cache queries (spread across runtime threads);
    // the coalesce-drain task dispatches coalesced waiters when the writer
    // reports a query Ready/Failed. There is no central dispatcher.
    handle.spawn(serve_loop(
        settings.clone(),
        serve_rx,
        cache_cancel.clone(),
        Arc::clone(&state_view),
    ));
    handle.spawn(coalesce_drain(
        dispatch,
        notify_rx,
        cache_cancel,
        dispatch_publisher,
    ));

    Ok(vec![writer_handle, cdc_handle])
}

/// Initial backoff before rebuilding the cache subsystem after a failure.
const RESTART_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Maximum backoff between cache-subsystem restart attempts.
const RESTART_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// One generation of the cache subsystem: the cancel token that fires on its
/// death and the scoped thread handles to reap once it does.
pub struct CacheGeneration<'scope> {
    cancel: CancellationToken,
    handles: Vec<thread::ScopedJoinHandle<'scope, CacheResult<()>>>,
}

/// Build and publish a cache generation: a fresh cancel (child of root), a fresh
/// status channel, then [`cache_setup`]. On success the status sender is swapped
/// in so the admin server reaches the new writer.
#[allow(clippy::too_many_arguments)]
pub fn cache_generation_start<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    handle: Handle,
    pinned: &[PinnedQuery],
    root_cancel: &CancellationToken,
    dispatch_updater: &CacheDispatchUpdater,
    status_updater: &StatusSenderUpdater,
) -> CacheResult<CacheGeneration<'scope>> {
    let cancel = root_cancel.child_token();
    let (status_tx, status_rx) = channel::<StatusRequest>(2);
    let handles = cache_setup(
        scope,
        settings,
        handle,
        pinned,
        cancel.clone(),
        status_rx,
        dispatch_updater.publisher(),
    )?;
    status_updater.sender_update(status_tx);
    Ok(CacheGeneration { cancel, handles })
}

/// Extract a human-readable message from a thread panic payload, which is
/// `&str` for `panic!("literal")` and `String` for formatted panics.
fn panic_message(panic: &(dyn Any + Send)) -> &str {
    panic
        .downcast_ref::<&str>()
        .copied()
        .or_else(|| panic.downcast_ref::<String>().map(String::as_str))
        .unwrap_or("unknown panic payload")
}

/// Supervise a running cache subsystem across restarts. Given the first
/// generation (built fail-fast during startup, before the proxy accepts), parks
/// until it dies, reaps it, then rebuilds with exponential backoff — repeating
/// until root shutdown.
///
/// Runs on the proxy thread (it owns the inner `thread::scope` and both watch
/// updaters). While a generation is down the `CacheDispatch`/status watches are
/// cleared, so connections degrade to origin until the next publish.
#[allow(clippy::too_many_arguments)]
pub fn cache_supervise<'scope, 'env: 'scope, 'settings: 'scope>(
    scope: &'scope thread::Scope<'scope, 'env>,
    settings: &'settings Settings,
    handle: Handle,
    pinned: &[PinnedQuery],
    root_cancel: CancellationToken,
    dispatch_updater: &CacheDispatchUpdater,
    status_updater: &StatusSenderUpdater,
    first: CacheGeneration<'scope>,
) {
    let mut generation = first;
    let mut backoff = RESTART_INITIAL_BACKOFF;

    loop {
        // Park until this generation dies (or root shutdown, which also cancels
        // this child token).
        handle.block_on(generation.cancel.cancelled());

        // Down: degrade connections to origin, then reap the dead generation's
        // threads before the next DB reset.
        dispatch_updater.clear();
        status_updater.sender_clear();
        for h in generation.handles.drain(..) {
            match h.join() {
                Ok(Err(e)) => error!(
                    "cache thread exited: {}",
                    error_chain_format(e.current_context()),
                ),
                // A panicked thread yields `Err` from `join()`; without this arm
                // the panic was silently swallowed and the cache death left no
                // trace in the logs.
                Err(panic) => error!("cache thread panicked: {}", panic_message(panic.as_ref())),
                Ok(Ok(())) => {}
            }
        }
        if root_cancel.is_cancelled() {
            break;
        }
        warn!("cache subsystem exited; restarting");

        // Rebuild with backoff, retrying until a generation comes up or shutdown.
        generation = loop {
            let interrupted = handle.block_on(async {
                tokio::select! {
                    _ = root_cancel.cancelled() => true,
                    _ = tokio::time::sleep(backoff) => false,
                }
            });
            if interrupted {
                dispatch_updater.clear();
                status_updater.sender_clear();
                debug!("cache supervisor exiting");
                return;
            }
            backoff = (backoff * 2).min(RESTART_MAX_BACKOFF);

            match cache_generation_start(
                scope,
                settings,
                handle.clone(),
                pinned,
                &root_cancel,
                dispatch_updater,
                status_updater,
            ) {
                Ok(next_gen) => {
                    backoff = RESTART_INITIAL_BACKOFF;
                    crate::metrics::handles().cache.restarts_total.increment(1);
                    info!("cache subsystem restarted");
                    break next_gen;
                }
                Err(e) => {
                    error!(
                        "cache restart failed: {}",
                        error_chain_format(e.current_context()),
                    );
                    dispatch_updater.clear();
                    status_updater.sender_clear();
                }
            }
        };
    }

    // Supervisor exiting: ensure connections degrade.
    dispatch_updater.clear();
    status_updater.sender_clear();
    debug!("cache supervisor exiting");
}

/// Cold task that drains coalesced waiters when the writer reports a query
/// `Ready`/`Failed`. Off the dispatch hot path (fires once per registration
/// completion, not per query). Retracts the published cache on exit.
async fn coalesce_drain(
    dispatch: CacheDispatch,
    mut notify_rx: UnboundedReceiver<WriterNotify>,
    cancel: CancellationToken,
    dispatch_publisher: CacheDispatchPublisher,
) {
    debug!("coalesce drain loop");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                debug!("coalesce drain shutdown signal received");
                break;
            }
            notify = notify_rx.recv() => {
                match notify {
                    Some(WriterNotify::Ready { fingerprint, generation, resolved, deparsed_sql, max_limit }) => {
                        dispatch.waiting_drain_ready(fingerprint, generation, resolved, deparsed_sql, max_limit);
                    }
                    Some(WriterNotify::Failed { fingerprint }) => {
                        dispatch.waiting_drain_failed(fingerprint);
                    }
                    None => {
                        error!("writer notify channel closed");
                        cancel.cancel();
                        break;
                    }
                }
            }
        }
    }

    debug!("coalesce drain loop exiting");
    dispatch_publisher.clear();
}

/// Record serve-reported metrics (cache-hit latency, bytes served).
fn serve_metrics_record(
    state_view: &CacheStateView,
    fingerprint: u64,
    latency_us: u64,
    bytes_served: u64,
) {
    if let Some(mut m) = state_view.metrics.get_mut(&fingerprint) {
        m.total_bytes_served += bytes_served;
        m.cache_hit_latency.saturating_record(latency_us);
    }
}

/// Worker dispatcher task: acquires a pooled cache-DB connection (the pool
/// bounds serve concurrency) and spawns the serve onto the shared runtime, so
/// serves spread across all runtime threads instead of one serve task.
async fn serve_loop(
    settings: Settings,
    mut serve_rx: UnboundedReceiver<ServeRequest>,
    cancel: CancellationToken,
    state_view: Arc<CacheStateView>,
) {
    debug!("cache serve loop");
    #[cfg(feature = "fault-injection")]
    crate::cache::serve::fault::init();
    let pool_size = (settings.num_workers * 2).max(MIN_POOL_SIZE);
    let (conn_tx, mut conn_rx) = match connection_pool_create(&settings, pool_size).await {
        Ok(pool) => pool,
        Err(e) => {
            error!(
                "creating connection pool: {}",
                error_chain_format(e.current_context())
            );
            cancel.cancel();
            return;
        }
    };

    // Replenish channel: a poisoned-connection discard signals here, and
    // `pool_replenish` reconnects a replacement so the pool stays at capacity
    // (PGC-238). Unbounded — signals are unit-sized and bounded by pool_size.
    let (replenish_tx, replenish_rx) = unbounded_channel::<()>();
    tokio::spawn(pool_replenish(
        settings.clone(),
        conn_tx.clone(),
        replenish_rx,
        cancel.clone(),
    ));

    loop {
        // Block for at least one request
        let mut msg = tokio::select! {
            _ = cancel.cancelled() => {
                debug!("cache serve shutdown signal received");
                break;
            }
            msg = serve_rx.recv() => {
                let Some(msg) = msg else { break };
                msg
            }
        };
        msg.timing.worker_received_at = Some(Instant::now());

        // Wait for an available connection
        let conn = if let Ok(conn) = conn_rx.try_recv() {
            conn
        } else {
            let Some(conn) = conn_rx.recv().await else {
                error!("cache connection pool closed");
                cancel.cancel();
                return;
            };
            conn
        };
        msg.timing.conn_acquired_at = Some(Instant::now());

        // Spawn the serve (request + connection) onto the shared runtime.
        let return_tx = conn_tx.clone();
        let replenish_tx = replenish_tx.clone();
        let state_view = Arc::clone(&state_view);
        tokio::spawn(async move {
            handle_serve_request(conn, return_tx, replenish_tx, msg, state_view).await;
        });

        // Channel depth gauge; queue length never approaches 2^53.
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles()
            .state
            .queue_worker
            .set(serve_rx.len() as f64);
    }

    debug!("cache serve loop exiting");
}

/// Replenishes the cache-DB serve pool: each signal from a poisoned-connection
/// discard triggers one reconnection, keeping the pool at `pool_size` so it can
/// never permanently shrink (PGC-238). The bounded pool channel always has room
/// for the replacement because the discard vacated a slot. Lives for the
/// generation — cancelled on subsystem teardown, or exits when the last
/// `replenish_tx` (serve loop + in-flight serves) drops.
async fn pool_replenish(
    settings: Settings,
    conn_tx: Sender<CacheConnection>,
    mut replenish_rx: UnboundedReceiver<()>,
    cancel: CancellationToken,
) {
    debug!("pool replenish task");
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            signal = replenish_rx.recv() => {
                if signal.is_none() {
                    break;
                }
            }
        }

        // Reconnect with capped backoff; abandon on subsystem teardown.
        let mut backoff = POOL_REPLENISH_INITIAL_BACKOFF;
        let conn = loop {
            match CacheConnection::connect(&settings.cache).await {
                Ok(conn) => break Some(conn),
                Err(e) => {
                    error!(
                        "serve-pool reconnect failed, retrying: {}",
                        error_chain_format(e.current_context())
                    );
                    tokio::select! {
                        _ = cancel.cancelled() => break None,
                        _ = tokio::time::sleep(backoff) => {}
                    }
                    backoff = (backoff * 2).min(POOL_REPLENISH_MAX_BACKOFF);
                }
            }
        };
        let Some(conn) = conn else { break };

        // The discard freed a slot, so this send cannot block on a full pool;
        // an error means the pool channel closed (teardown).
        if conn_tx.send(conn).await.is_err() {
            break;
        }
        crate::metrics::handles()
            .cache
            .pool_replenished
            .increment(1);
    }

    debug!("pool replenish task exiting");
}

/// Initial backoff for CDC reconnection attempts.
const CDC_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Maximum backoff for CDC reconnection attempts.
const CDC_MAX_BACKOFF: Duration = Duration::from_secs(30);

/// CDC runtime - processes change data capture events.
///
/// On stream error, attempts to reconnect by verifying the replication slot's
/// confirmed_flush_lsn matches our last acknowledged position. If the LSN matches,
/// the stream is resumed without cache invalidation. If the slot is gone or the
/// LSN diverges, signals Fatal so the proxy can perform a full restart.
fn cdc_run(
    settings: &Settings,
    cdc_tx: UnboundedSender<CdcCommand>,
    active_relations: ActiveRelations,
    cancel: CancellationToken,
    cdc_connected: Arc<AtomicBool>,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("cdc loop");
    rt.block_on(async {
        let mut cdc =
            CdcProcessor::new(settings, cdc_tx.clone(), Arc::clone(&active_relations))
                .await
                .attach_loc("initializing CDC processor")?;
        debug!("CDC processor initialized, entering stream loop");

        loop {
            let stream_result = cdc.run(cancel.clone()).await;

            // Cancel-initiated shutdown — exit cleanly
            if cancel.is_cancelled() {
                debug!("CDC shutdown complete");
                return Ok(());
            }

            // Stream ended or errored while not cancelled — treat as disconnect
            let saved_lsn = cdc.last_flushed_lsn();
            match &stream_result {
                Ok(()) => warn!(
                    "CDC stream ended unexpectedly (last_flushed_lsn: {saved_lsn})"
                ),
                Err(e) => warn!(
                    "CDC stream error (last_flushed_lsn: {saved_lsn}): {}",
                    error_chain_format(e),
                ),
            }

            // Forward all queries to origin while disconnected.
            cdc_connected.store(false, Ordering::Relaxed);

            // Reconnect loop with exponential backoff
            let mut backoff = CDC_INITIAL_BACKOFF;
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!("CDC cancelled during reconnect");
                        return Ok(());
                    }
                    _ = tokio::time::sleep(backoff) => {}
                }

                // Verify the slot hasn't been advanced past our last acknowledged position.
                // confirmed <= saved is safe: the slot is retaining WAL from an equal or
                // earlier position, so we'll receive everything from saved_lsn forward.
                // confirmed > saved means the slot was externally advanced — events may
                // have been skipped.
                match slot_confirmed_lsn(settings).await {
                    Ok(Some(confirmed_lsn)) => {
                        if confirmed_lsn > saved_lsn {
                            error!(
                                "slot advanced past our position: saved={saved_lsn}, confirmed={confirmed_lsn}"
                            );
                            cancel.cancel();
                            return Err(CacheError::CdcFailure.into());
                        }
                        debug!("slot LSN verified: confirmed={confirmed_lsn}, saved={saved_lsn}");
                    }
                    Ok(None) => {
                        error!("replication slot no longer exists");
                        cancel.cancel();
                        return Err(CacheError::CdcFailure.into());
                    }
                    Err(e) => {
                        error!(
                            "slot LSN check failed: {}",
                            error_chain_format(e.current_context()),
                        );
                        backoff = (backoff * 2).min(CDC_MAX_BACKOFF);
                        continue;
                    }
                }

                // LSN matches — attempt to re-establish the replication connection
                match CdcProcessor::new(
                    settings,
                    cdc_tx.clone(),
                    Arc::clone(&active_relations),
                )
                .await
                {
                    Ok(new_cdc) => {
                        cdc = new_cdc;
                        debug!("CDC reconnected");
                        cdc_connected.store(true, Ordering::Relaxed);
                        break; // Back to outer loop to run the stream
                    }
                    Err(e) => {
                        error!(
                            "CDC reconnect failed: {}",
                            error_chain_format(e.current_context()),
                        );
                        backoff = (backoff * 2).min(CDC_MAX_BACKOFF);
                        continue;
                    }
                }
            }
        }
    })
}
