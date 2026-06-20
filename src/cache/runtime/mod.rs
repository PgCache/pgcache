use std::any::Any;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::thread;
use std::time::Duration;

use arc_swap::ArcSwap;

use tokio::{
    runtime::Handle,
    sync::mpsc::{Receiver, UnboundedReceiver, channel, unbounded_channel},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, instrument, warn};

use crate::{
    cache::{
        CacheDispatchPublisher, CacheDispatchUpdater, CacheError, CacheResult, MapIntoReport,
        PinnedQuery, ReportExt, StatusRequest,
        messages::WriterNotify,
        query_cache::CacheDispatch,
        types::{ActiveRelations, CacheStateView},
        writer::writer_run,
    },
    pg::cdc::replication_provision,
    proxy::StatusSenderUpdater,
    result::error_chain_format,
    settings::Settings,
};

mod cdc_driver;
mod memory_monitor;
mod reg_gate;
mod reset;
mod serve_pool;

use cdc_driver::cdc_run;
use memory_monitor::{memory_monitor, shared_buffers_bytes_query};
use reg_gate::reg_gate_controller;
use reset::cache_database_reset;
use serve_pool::serve_loop;

/// Minimum number of connections in the cache serve pool.
const MIN_POOL_SIZE: usize = 4;

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

    // Lets the writer prompt the CDC thread for an immediate keepalive when a
    // populated query is gated on the apply watermark (PGC-250 Slice B).
    let watermark_nudge = Arc::new(tokio::sync::Notify::new());

    // Writer thread (owns Cache, serializes all mutations). Gets a child of the
    // subsystem cancel so a subsystem teardown propagates to it.
    let settings_writer = settings.clone();
    let state_view_writer = Arc::clone(&state_view);
    let active_relations_writer = Arc::clone(&active_relations);
    let cancel_writer = cache_cancel.child_token();
    let watermark_nudge_writer = Arc::clone(&watermark_nudge);
    let shared_runtime_writer = handle.clone();
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
                watermark_nudge_writer,
                shared_runtime_writer,
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
    let watermark_nudge_cdc = Arc::clone(&watermark_nudge);
    let cdc_handle = match thread::Builder::new()
        .name("cdc worker".to_owned())
        .spawn_scoped(scope, move || {
            let result = cdc_run(
                settings,
                cdc_cmd_tx,
                active_relations_cdc,
                cancel_cdc,
                cdc_connected_cdc,
                watermark_nudge_cdc,
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
    let shared_buffers = handle.block_on(shared_buffers_bytes_query(settings));
    let serve_pool_size = (settings.num_workers * 2).max(MIN_POOL_SIZE);
    handle.spawn(memory_monitor(
        Arc::clone(&state_view),
        settings.dynamic.clone(),
        shared_buffers,
        serve_pool_size,
        cache_cancel.clone(),
    ));
    handle.spawn(reg_gate_controller(
        Arc::clone(&state_view),
        cache_cancel.clone(),
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
/// `Ready`/`Failed`. Off the dispatch hot path. Fires on every population
/// completion and on every invalidation/eviction (the latter can be per
/// CDC-event under churn), so a `Failed` for a fingerprint with no parked
/// waiters is a cheap no-op drain. Retracts the published cache on exit.
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
