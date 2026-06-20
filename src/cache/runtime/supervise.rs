use std::any::Any;
use std::thread;
use std::time::Duration;

use tokio::runtime::Handle;
use tokio::sync::mpsc::channel;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};

use crate::cache::{CacheDispatchUpdater, CacheResult, PinnedQuery, StatusRequest};
use crate::proxy::StatusSenderUpdater;
use crate::result::error_chain_format;
use crate::settings::Settings;

use super::CacheGeneration;
use super::setup::cache_setup;

/// Initial backoff before rebuilding the cache subsystem after a failure.
const RESTART_INITIAL_BACKOFF: Duration = Duration::from_millis(500);
/// Maximum backoff between cache-subsystem restart attempts.
const RESTART_MAX_BACKOFF: Duration = Duration::from_secs(30);

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
