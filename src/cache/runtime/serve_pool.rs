use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::cache::explain::handle_explain_request;
use crate::cache::messages::{CacheOutcome, CacheReply, slices_concat};
use crate::cache::query_cache::{ServeJob, ServeRequest};
use crate::cache::serve::{CoalescedOutcome, SQLSTATE_UNDEFINED_TABLE, handle_cached_query};
use crate::cache::types::CacheStateView;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::pg::cache_connection::CacheConnection;
use crate::query::Fingerprint;
use crate::result::error_chain_format;
use crate::settings::Settings;
use crate::timing::duration_to_us_u64;

/// Minimum number of connections in the cache serve pool.
pub(super) const MIN_POOL_SIZE: usize = 4;

/// Interval between serve-pool connection recycles while under memory pressure.
/// One connection per tick → the whole pool refreshes over `pool_size × this`
/// (PGC-251 Slice 1d).
const RECYCLE_INTERVAL: Duration = Duration::from_secs(25);

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

/// Record serve-reported metrics (cache-hit latency, bytes served).
fn serve_metrics_record(
    state_view: &CacheStateView,
    fingerprint: Fingerprint,
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
pub(super) async fn serve_loop(
    settings: Settings,
    mut serve_rx: UnboundedReceiver<ServeJob>,
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

    // Recycle one idle connection per tick while the monitor flags memory pressure
    // (PGC-251 Slice 1d). The first interval tick fires immediately; harmless
    // (recycle_wanted is clear at startup).
    let mut recycle_interval = tokio::time::interval(RECYCLE_INTERVAL);
    recycle_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        // Block for at least one request
        let mut msg = tokio::select! {
            _ = cancel.cancelled() => {
                debug!("cache serve shutdown signal received");
                break;
            }
            _ = recycle_interval.tick() => {
                // Drop one idle pooled connection and replenish a fresh backend,
                // returning the dropped backend's plan-cache RSS to the OS.
                if state_view.recycle_wanted.load(Ordering::Relaxed)
                    && let Ok(conn) = conn_rx.try_recv()
                {
                    drop(conn);
                    let _ = replenish_tx.send(());
                    state_view.recycle_count.fetch_add(1, Ordering::Relaxed);
                    crate::metrics::handles().cache.pool_recycled.increment(1);
                }
                continue;
            }
            msg = serve_rx.recv() => {
                let Some(msg) = msg else { break };
                msg
            }
        };
        msg.timing_mut().worker_received_at = Some(Instant::now());

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
        msg.timing_mut().conn_acquired_at = Some(Instant::now());

        // Spawn the serve (request + connection) onto the shared runtime.
        let return_tx = conn_tx.clone();
        let replenish_tx = replenish_tx.clone();
        let state_view = Arc::clone(&state_view);
        // Serve-pool liveness gauges (PGC-278): a drained pool with serves still
        // in flight is the freeze signature. `pool_available` is read after this
        // serve's connection was taken, so it reflects what's left.
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles()
            .cache
            .pool_available
            .set(conn_rx.len() as f64);
        crate::metrics::handles()
            .cache
            .serves_in_flight
            .increment(1.0);
        tokio::spawn(async move {
            match msg {
                ServeJob::Query(request) => {
                    handle_serve_request(conn, return_tx, replenish_tx, request, state_view).await;
                }
                ServeJob::Explain(job) => {
                    handle_explain_request(conn, return_tx, replenish_tx, job, &state_view).await;
                }
            }
            crate::metrics::handles()
                .cache
                .serves_in_flight
                .decrement(1.0);
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
