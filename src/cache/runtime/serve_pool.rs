use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tokio::sync::mpsc::{
    Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::cache::explain::handle_explain_request;
use crate::cache::messages::{CacheOutcome, CacheReply, slices_concat};
use crate::cache::query_cache::{ServeJob, ServeRequest};
use crate::cache::serve::{CoalescedOutcome, SQLSTATE_UNDEFINED_TABLE, handle_cached_query};
use crate::cache::serve_pool_state::ServePool;
use crate::cache::types::CacheStateView;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::pg::cache_connection::CacheConnection;
use crate::query::Fingerprint;
use crate::result::error_chain_format;
use crate::settings::Settings;
use crate::timing::duration_to_us_u64;

use super::pool_controller::{PoolController, PoolControllerConfig, StepKind, TickSample};

/// Elastic serve-pool bounds (ADR-053): the floor keeps light traffic served
/// without ramp-up latency; the ceiling is the cache-PG protection bound and
/// what the memory monitor budgets backend RSS against. The operating size
/// between them is found by the probe-and-verify controller, not configured.
pub(super) fn serve_pool_bounds(num_workers: usize) -> (usize, usize) {
    (num_workers * 2, num_workers * 8)
}

/// Serve-pool controller thresholds (ADR-053). Serves are millisecond-scale
/// cache-PG queries, so tens of milliseconds of queue wait is already the
/// harm signal (populations use 250ms; see ADR-052). Initial value — revisit
/// against bench evidence.
const CONTROLLER_CONFIG: PoolControllerConfig = PoolControllerConfig {
    wait_target: Duration::from_millis(25),
    verify_beta: 0.5,
    verify_min_completions: 5,
    probe_hold_ticks: 5,
    probe_hold_max_ticks: 60,
    backstop_ticks: 5,
    spawn_wait_ticks: 5,
    rho_shrink: 0.6,
    down_ticks: 30,
};

/// Controller tick.
const CONTROLLER_TICK: Duration = Duration::from_secs(1);

/// Interval between serve-pool connection recycles while under memory pressure.
/// One connection per tick → the whole pool refreshes over `pool_size × this`
/// (PGC-251 Slice 1d).
const RECYCLE_INTERVAL: Duration = Duration::from_secs(25);

/// Initial backoff before retrying a serve-pool reconnection.
const POOL_REPLENISH_INITIAL_BACKOFF: Duration = Duration::from_millis(200);
/// Maximum backoff between serve-pool reconnection attempts.
const POOL_REPLENISH_MAX_BACKOFF: Duration = Duration::from_secs(10);

/// How long a parked connection may sit before it is dropped for real: long
/// enough to absorb the controller's probe cycles at the hold-ladder cap,
/// short enough that a backend the workload stopped needing is released.
const PARK_EXPIRY: Duration = Duration::from_secs(300);

/// One surplus connection kept aside instead of dropped (ADR-053): the
/// controller's probe cycle at a capacity ceiling then reuses it instead of
/// tearing down and re-establishing a backend every cycle. Guarded by a sync
/// mutex; never held across an await.
type ParkedSlot = Arc<std::sync::Mutex<Option<(CacheConnection, Instant)>>>;

fn parked_lock(
    parked: &ParkedSlot,
) -> std::sync::MutexGuard<'_, Option<(CacheConnection, Instant)>> {
    parked
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

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

/// Creates the pool channel with capacity for the elastic maximum and fills
/// it to the starting size. Connections are immediately available in the
/// receiver; the reconciler grows and shrinks the live set between the bounds.
async fn connection_pool_create(
    settings: &Settings,
    initial: usize,
    capacity: usize,
) -> CacheResult<(Sender<CacheConnection>, Receiver<CacheConnection>)> {
    let (tx, rx) = channel(capacity);

    for i in 0..initial {
        debug!(
            "Creating connection {}/{} to cache db at {}:{}",
            i + 1,
            initial,
            settings.cache.host,
            settings.cache.port
        );

        let conn = CacheConnection::connect(&settings.cache)
            .await
            .attach_loc("creating cache connection")?;

        tx.send(conn).await.map_err(|_| CacheError::NoConnection)?;
    }

    debug!("Created {} connections", initial);
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
    let (pool_min, pool_max) = serve_pool_bounds(settings.num_workers);
    let serve_pool = Arc::clone(&state_view.serve_pool);
    let (conn_tx, mut conn_rx) = match connection_pool_create(&settings, pool_min, pool_max).await {
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
    serve_pool.desired_set(pool_min);
    serve_pool.live_add(pool_min);

    // Reconcile channel: a lost connection (poison discard, mid-flight loss,
    // recycle) signals here, and `pool_reconcile` keeps the live set at the
    // controller's desired size (ADR-053; replenish semantics from PGC-238).
    // Unbounded — signals are unit-sized and bounded by pool size.
    let (replenish_tx, replenish_rx) = unbounded_channel::<()>();
    let parked: ParkedSlot = Arc::new(std::sync::Mutex::new(None));
    tokio::spawn(pool_reconcile(
        settings.clone(),
        conn_tx.clone(),
        replenish_rx,
        cancel.clone(),
        Arc::clone(&serve_pool),
        Arc::clone(&parked),
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
                parked_maintain(&parked, state_view.recycle_wanted.load(Ordering::Relaxed));
                pool_shrink_apply(&serve_pool, &mut conn_rx, &parked);
                continue;
            }
            msg = serve_rx.recv() => {
                let Some(msg) = msg else { break };
                msg
            }
        };
        msg.timing_mut().worker_received_at = Some(Instant::now());

        // Retire surplus idle connections the controller no longer wants
        // before taking one for this serve.
        pool_shrink_apply(&serve_pool, &mut conn_rx, &parked);

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
        let acquired_at = Instant::now();
        msg.timing_mut().conn_acquired_at = Some(acquired_at);
        // Queue-pressure signal for the pool controller: dispatch → connection
        // acquired covers both the serve-queue and the pool wait.
        if let Some(dispatched) = msg.timing_mut().dispatched_at {
            serve_pool.wait_observe(duration_to_us_u64(
                acquired_at.saturating_duration_since(dispatched),
            ));
        }

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
        let serve_pool_task = Arc::clone(&serve_pool);
        tokio::spawn(async move {
            // Decrement on drop, not fall-through, so a panicking or
            // cancelled serve still balances the gauge (PGC-278).
            let _in_flight = ServeInFlight;
            match msg {
                ServeJob::Query(request) => {
                    handle_serve_request(conn, return_tx, replenish_tx, request, state_view).await;
                }
                ServeJob::Explain(job) => {
                    handle_explain_request(conn, return_tx, replenish_tx, job, &state_view).await;
                }
            }
            // Service-time signal for the pool controller (both job kinds).
            serve_pool_task.task_observe(duration_to_us_u64(acquired_at.elapsed()));
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

/// Drop idle connections beyond the controller's desired size. Only idle
/// connections are retired (`try_recv`), so an in-flight serve is never
/// interrupted; a checked-out surplus connection is caught on a later pass.
fn pool_shrink_apply(
    serve_pool: &ServePool,
    conn_rx: &mut Receiver<CacheConnection>,
    parked: &ParkedSlot,
) {
    while serve_pool.live() > serve_pool.desired() {
        let Ok(conn) = conn_rx.try_recv() else {
            return;
        };
        serve_pool.live_sub(1);
        crate::metrics::handles()
            .cache
            .serve_pool_scale_down
            .increment(1);
        // The first surplus connection parks so the next probe cycle reuses
        // it (ADR-053); further surplus is a genuinely oversized pool and
        // releases its backends.
        let mut slot = parked_lock(parked);
        if slot.is_none() {
            *slot = Some((conn, Instant::now()));
        }
    }
}

/// Drop the parked connection when it has outlived its usefulness: expired,
/// or the memory monitor wants backends released (a held-back backend is
/// exactly what recycling exists to reclaim).
fn parked_maintain(parked: &ParkedSlot, recycle_wanted: bool) {
    let mut slot = parked_lock(parked);
    if let Some((_, since)) = slot.as_ref()
        && (recycle_wanted || since.elapsed() > PARK_EXPIRY)
    {
        *slot = None;
    }
}

/// Maintains the serve pool at the controller's desired size (ADR-053).
/// Each signal reports one permanently lost connection (poisoned discard,
/// mid-flight loss, or recycle drop — PGC-238/278/251); a periodic tick picks
/// up controller growth. Reconnects with capped backoff; the pool channel has
/// capacity for the elastic maximum, so sends cannot block. Lives for the
/// generation — cancelled on subsystem teardown, or exits when the last
/// `replenish_tx` (serve loop + in-flight serves) drops.
async fn pool_reconcile(
    settings: Settings,
    conn_tx: Sender<CacheConnection>,
    mut replenish_rx: UnboundedReceiver<()>,
    cancel: CancellationToken,
    serve_pool: Arc<ServePool>,
    parked: ParkedSlot,
) {
    debug!("pool reconcile task");
    let mut tick = tokio::time::interval(Duration::from_millis(500));
    tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    'outer: loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            signal = replenish_rx.recv() => {
                if signal.is_none() {
                    break;
                }
                serve_pool.live_sub(1);
            }
            _ = tick.tick() => {}
        }

        while serve_pool.live() < serve_pool.desired() {
            // A parked connection is reused before dialing: the probe cycle
            // at a capacity ceiling then costs no reconnect (ADR-053). A
            // stale parked backend that died meanwhile poisons on first use
            // and replenishes through the normal path.
            let unparked = parked_lock(&parked).take();
            if let Some((conn, _)) = unparked {
                if conn_tx.send(conn).await.is_err() {
                    break 'outer;
                }
                serve_pool.live_add(1);
                continue;
            }
            // Reconnect with capped backoff; abandon on subsystem teardown.
            let mut backoff = POOL_REPLENISH_INITIAL_BACKOFF;
            let conn = loop {
                match CacheConnection::connect(&settings.cache).await {
                    Ok(conn) => break conn,
                    Err(e) => {
                        error!(
                            "serve-pool reconnect failed, retrying: {}",
                            error_chain_format(e.current_context())
                        );
                        tokio::select! {
                            _ = cancel.cancelled() => break 'outer,
                            _ = tokio::time::sleep(backoff) => {}
                        }
                        backoff = (backoff * 2).min(POOL_REPLENISH_MAX_BACKOFF);
                    }
                }
            };

            // An error means the pool channel closed (teardown).
            if conn_tx.send(conn).await.is_err() {
                break 'outer;
            }
            serve_pool.live_add(1);
            crate::metrics::handles()
                .cache
                .pool_replenished
                .increment(1);
        }
    }

    debug!("pool reconcile task exiting");
}

/// Elastic serve pool controller (ADR-053): probes the connection count
/// upward while serve queue wait is breached — verifying each step against
/// measured serve throughput — and shrinks on sustained low utilization,
/// bounded by [`serve_pool_bounds`]. The serve loop's reconcile task applies
/// the target.
pub(super) async fn serve_pool_controller(
    state_view: Arc<CacheStateView>,
    num_workers: usize,
    cancel: CancellationToken,
) {
    let (pool_min, pool_max) = serve_pool_bounds(num_workers);
    let pool = &state_view.serve_pool;
    let mut controller = PoolController::new(pool_min, pool_max, CONTROLLER_CONFIG);
    let mut prev = pool.counters();
    let mut interval = tokio::time::interval(CONTROLLER_TICK);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let handles = &crate::metrics::handles().cache;
    loop {
        tokio::select! {
            _ = cancel.cancelled() => break,
            _ = interval.tick() => {}
        }
        let now = pool.counters();
        let sample = TickSample {
            task_us: now.0 - prev.0,
            task_count: now.1 - prev.1,
            wait_us: now.2 - prev.2,
            wait_count: now.3 - prev.3,
            live: pool.live(),
            tick_seconds: CONTROLLER_TICK.as_secs_f64(),
        };
        prev = now;

        let desired = pool.desired();
        let (next, kind) = controller.step(sample, desired);
        if next != desired {
            pool.desired_set(next);
            match kind {
                StepKind::Grow => handles.serve_pool_scale_up.increment(1),
                StepKind::GrowBackstop => {
                    handles.serve_pool_scale_up.increment(1);
                    handles.serve_pool_backstop_grows.increment(1);
                }
                // Applied lazily by `pool_shrink_apply`; counted there.
                StepKind::Shrink | StepKind::Hold => {}
            }
            tracing::debug!(
                "serve pool target {desired} -> {next} ({kind:?}, serves/tick={})",
                sample.task_count,
            );
        }
        // Live count as a gauge; pool sizes never approach 2^53.
        #[allow(clippy::cast_precision_loss)]
        handles.serve_pool_size.set(pool.live() as f64);
    }
}

/// Balances the `serves_in_flight` gauge in `Drop` so panic and
/// cancellation unwinds decrement it too.
struct ServeInFlight;

impl Drop for ServeInFlight {
    fn drop(&mut self) {
        crate::metrics::handles()
            .cache
            .serves_in_flight
            .decrement(1.0);
    }
}

/// Guard that ensures a connection is returned to the pool.
///
/// Returns the connection via async `release()` on success.
/// On error (drop without release), the connection is discarded if poisoned
/// to avoid returning a connection with stale response data in its buffer; a
/// replenish signal is sent so a fresh connection replaces it and the pool
/// cannot permanently shrink (PGC-238).
pub(crate) struct ConnectionGuard {
    pub(crate) conn: Option<CacheConnection>,
    return_tx: Sender<CacheConnection>,
    replenish_tx: UnboundedSender<()>,
    pub(crate) poisoned: bool,
    /// `release()` returned the connection to the pool. Distinguishes the
    /// happy-path drop (`conn: None` after a clean return) from a serve
    /// task that died with the connection checked out (PGC-278).
    returned: bool,
}

impl ConnectionGuard {
    pub(crate) fn new(
        conn: CacheConnection,
        return_tx: Sender<CacheConnection>,
        replenish_tx: UnboundedSender<()>,
    ) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
            replenish_tx,
            poisoned: false,
            returned: false,
        }
    }

    /// Return the connection to the pool.
    pub(crate) async fn release(mut self) -> CacheResult<()> {
        if let Some(conn) = self.conn.take() {
            self.return_tx
                .send(conn)
                .await
                .map_err(|_| CacheError::NoConnection)?;
            self.returned = true;
        }
        Ok(())
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if self.poisoned {
            // Discard the connection — may have unread response data — and
            // signal the replenish task to reconnect a replacement so the pool
            // size stays constant (PGC-238).
            self.conn.take();
            let _ = self.replenish_tx.send(());
            return;
        }
        match self.conn.take() {
            Some(conn) => {
                // try_send won't block; channel always has capacity since
                // pool size equals channel size
                let _ = self.return_tx.try_send(conn);
            }
            // The serve task panicked or was cancelled while the connection
            // was checked out of the guard (between `conn.take()` and
            // reattach) — without a replenish the pool shrinks permanently
            // (PGC-278).
            None if !self.returned => {
                warn!("serve task lost its cache connection mid-flight; replenishing the pool");
                let _ = self.replenish_tx.send(());
            }
            None => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serve_pool_bounds_scale_with_workers() {
        assert_eq!(serve_pool_bounds(1), (2, 8));
        assert_eq!(serve_pool_bounds(2), (4, 16));
        assert_eq!(serve_pool_bounds(8), (16, 64));
    }
}
