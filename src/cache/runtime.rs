use crate::query::Fingerprint;
use std::any::Any;
#[cfg(feature = "fault-injection")]
use std::collections::VecDeque;
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
    settings::{DynamicConfigHandle, Settings},
    timing::duration_to_us_u64,
};

/// Minimum number of connections in the cache serve pool.
const MIN_POOL_SIZE: usize = 4;
/// Interval between serve-pool connection recycles while under memory pressure.
/// One connection per tick → the whole pool refreshes over `pool_size × this`
/// (PGC-251 Slice 1d).
const RECYCLE_INTERVAL: Duration = Duration::from_secs(25);
/// Fraction of the count cap at/above which the cache is "under pressure" and the
/// monitor enables connection recycling (PGC-251 Slice 1d).
const RECYCLE_PRESSURE_FRACTION: f64 = 0.8;

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
    handle.spawn(coalesce_drain(
        dispatch,
        notify_rx,
        cache_cancel,
        dispatch_publisher,
    ));

    Ok(vec![writer_handle, cdc_handle])
}

/// Samples whole-system used memory against the registration budget and toggles
/// `state_view.registration_throttled` (with hysteresis) so dispatch degrades to
/// origin-forwarding before the box exhausts RAM. "Used" is system-wide
/// (`MemTotal - MemAvailable`, or the cgroup's `memory.current`), so it counts
/// pgcache *and* the cache Postgres it manages — not just pgcache's own RSS. The
/// budget is 80% of detected RAM (cgroup-aware), optionally lowered by
/// `memory_limit`, minus the memo's reserved budget. No-op where memory can't be
/// detected (non-Linux): the flag stays clear and registration is unbounded.
#[allow(clippy::cast_precision_loss)] // byte gauges never exceed 2^52
async fn memory_monitor(
    state_view: Arc<CacheStateView>,
    dynamic: DynamicConfigHandle,
    shared_buffers: u64,
    pool_size: usize,
    cancel: CancellationToken,
) {
    const TICK: Duration = Duration::from_millis(500);
    let cache = &crate::metrics::handles().cache;

    let Some(total_ram) = crate::memory::total_budget_bytes() else {
        debug!("memory monitor: RAM not detectable; registration throttling disabled");
        return;
    };

    // Decaying peak of the measured per-query private cost carried across ticks,
    // feeding the count cap (PGC-251). The cap anchors on the *live* private
    // reading (refinement 2), so no fixed baseline is tracked.
    let mut marginal_ewma = 0.0_f64;
    let mut peak_marginal = 0.0_f64;
    // High-water count + the private footprint at it, for the incremental
    // Δprivate/Δcount marginal. Sampling only on a *new* high-water count means
    // genuine growth is measured and the plateau (eviction churning the count
    // just below the cap) produces no noisy samples.
    let mut hw_private: Option<u64> = None;
    let mut hw_count: usize = 0;
    // Recycle count at the last re-measurement probe, to detect a full pool refresh.
    let mut last_recycle_count: usize = 0;
    // When set, a re-measurement probe is in flight: publish this bounded cap
    // (current count + one growth chunk) until a fresh sample lands (Slice 1e).
    let mut probe_target: Option<usize> = None;
    // Previously published cap, for rate-limiting cap increases (Slice 1e).
    let mut last_published_cap: Option<usize> = None;

    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(TICK) => {}
        }

        let Some(used) = crate::memory::system_used_bytes() else {
            continue;
        };

        let throttled = state_view.registration_throttled.load(Ordering::Relaxed);
        let decision = crate::memory::throttle_evaluate(
            used,
            total_ram,
            dynamic.load().memory_limit.map(|l| l as u64),
            state_view.memo.budget() as u64,
            state_view.memo.total_bytes() as u64,
            throttled,
        );
        let next = decision.throttled;
        let high_mark = decision.high_mark;
        if next != throttled {
            state_view
                .registration_throttled
                .store(next, Ordering::Relaxed);
            if next {
                warn!(
                    used_mb = used / 1_048_576,
                    budget_mb = high_mark / 1_048_576,
                    "memory pressure: throttling new-query registration (forwarding to origin)"
                );
            } else {
                info!("memory pressure relieved: resuming query registration");
            }
        }

        // Size the count cap from the measured *private* per-query cost (PGC-251).
        // `private` excludes shared_buffers (shared memory), so the marginal is
        // the growing pool — pgcache state + cache-PG plan cache — only; the
        // shared_buffers config is reserved separately against the ceiling.
        let private = crate::memory::system_private_bytes().unwrap_or(used);
        let count = state_view.registered_count.load(Ordering::Relaxed);
        let hw_p = hw_private.unwrap_or(private);
        let cap_decision = crate::memory::count_cap_evaluate(
            private,
            hw_p,
            count,
            hw_count,
            decision.ceiling,
            shared_buffers,
            marginal_ewma,
            peak_marginal,
        );
        marginal_ewma = cap_decision.marginal_ewma;
        peak_marginal = cap_decision.peak_marginal;
        // Advance the high-water reference when a growth chunk was sampled (or to
        // seed it on the first tick).
        if cap_decision.sampled || hw_private.is_none() {
            hw_private = Some(private);
            hw_count = count;
        }
        // A fresh sample re-learned the cost, so the probe (if any) is complete.
        if cap_decision.sampled {
            probe_target = None;
        }

        // Re-measurement probe (PGC-251 Slice 1e): when a full pool has been
        // recycled, forget the stale per-query estimate, re-anchor the high-water
        // to now, and allow one MIN_GROWTH chunk of growth. Without this the cap
        // can't adapt to a *lighter* workload — the count stays pinned at the cap,
        // so no growth sample is ever taken and the marginal stays frozen at the
        // old (heavier) cost. The next chunk re-samples the current workload.
        let recycled = state_view.recycle_count.load(Ordering::Relaxed);
        if recycled >= last_recycle_count + pool_size {
            last_recycle_count = recycled;
            hw_count = count;
            hw_private = Some(private);
            marginal_ewma = 0.0;
            peak_marginal = 0.0;
            probe_target = Some(count + crate::memory::COUNT_CAP_MIN_GROWTH);
        }

        // While probing (estimate reset, fresh sample pending) publish a bounded
        // cap permitting exactly the probe chunk (exempt from rate-limiting — it
        // must grow by MIN_GROWTH to re-sample); otherwise the re-learned cap,
        // with its *increase* rate-limited so a single bad sample can't spike it.
        let published_cap = match probe_target {
            Some(target) => target,
            None => crate::memory::cap_rate_limit(cap_decision.cap, last_published_cap),
        };
        last_published_cap = Some(published_cap);

        // Connection recycling (PGC-251 Slice 1d): flag memory pressure so the
        // serve loop rolls the pool one connection at a time.
        let pressure = published_cap != usize::MAX
            && (count as f64) >= RECYCLE_PRESSURE_FRACTION * (published_cap as f64);
        state_view.recycle_wanted.store(pressure, Ordering::Relaxed);
        state_view
            .query_count_cap
            .store(published_cap, Ordering::Relaxed);

        cache.memory_used_bytes.set(used as f64);
        cache.memory_budget_bytes.set(high_mark as f64);
        cache
            .rss_bytes
            .set(crate::memory::process_rss_bytes().unwrap_or(0) as f64);
        cache.registration_throttled.set(f64::from(u8::from(next)));
        cache.marginal_bytes_per_query.set(peak_marginal);
        // usize::MAX (no cap) would saturate the gauge; report 0 = "uncapped".
        cache.query_count_cap.set(if published_cap == usize::MAX {
            0.0
        } else {
            published_cap as f64
        });
    }
}

/// Query the cache PG's `shared_buffers` (a cluster-wide setting) so the count
/// cap can reserve it against the memory ceiling (PGC-251). Returns 0 if it
/// can't be read — the cap then reserves nothing for it (less conservative but
/// still functional).
async fn shared_buffers_bytes_query(settings: &Settings) -> u64 {
    async fn inner(settings: &Settings) -> CacheResult<u64> {
        let (client, conn) = Config::new()
            .host(&settings.cache.host)
            .port(settings.cache.port)
            .user(&settings.cache.user)
            .dbname(&settings.cache.database)
            .connect(NoTls)
            .await
            .map_into_report::<CacheError>()?;
        tokio::spawn(async move {
            if let Err(e) = conn.await {
                debug!("shared_buffers query connection error: {e}");
            }
        });
        let row = client
            .query_one(
                "SELECT pg_size_bytes(current_setting('shared_buffers'))",
                &[],
            )
            .await
            .map_into_report::<CacheError>()?;
        let bytes: i64 = row.get(0);
        Ok(u64::try_from(bytes).unwrap_or(0))
    }
    match inner(settings).await {
        Ok(b) => b,
        Err(e) => {
            debug!(
                "shared_buffers query failed ({}); count cap reserves 0 for it",
                error_chain_format(e.current_context())
            );
            0
        }
    }
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

/// Test-only constant CDC apply lag (fault-injection feature): when
/// `PGCACHE_FAULT_CDC_APPLY_LAG_MS` is set, every `CdcCommand` is held for
/// that long between the decoder and the writer — the writer applies a fixed
/// interval in the past at full throughput, simulating sustained writer lag
/// (slot acks still run ahead of apply, exactly as with real lag). Identity
/// pass-through when unset. Must be called inside a tokio runtime.
#[cfg(feature = "fault-injection")]
fn fault_cdc_apply_lag_relay(cdc_tx: UnboundedSender<CdcCommand>) -> UnboundedSender<CdcCommand> {
    let Some(lag_ms) = std::env::var("PGCACHE_FAULT_CDC_APPLY_LAG_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    else {
        return cdc_tx;
    };
    let lag = Duration::from_millis(lag_ms);
    warn!(lag_ms, "fault: CDC apply-lag relay active");
    let (relay_tx, mut relay_rx) = unbounded_channel::<CdcCommand>();
    tokio::spawn(async move {
        let mut held: VecDeque<(tokio::time::Instant, CdcCommand)> = VecDeque::new();
        loop {
            let due = held.front().map(|(t, _)| *t);
            let head_due = async {
                match due {
                    Some(t) => tokio::time::sleep_until(t).await,
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                cmd = relay_rx.recv() => match cmd {
                    Some(cmd) => held.push_back((tokio::time::Instant::now() + lag, cmd)),
                    // Decoder gone (teardown/restart): flush what's held —
                    // the lag is moot once the stream has ended.
                    None => break,
                },
                () = head_due => {
                    if let Some((_, cmd)) = held.pop_front()
                        && cdc_tx.send(cmd).is_err()
                    {
                        return;
                    }
                }
            }
        }
        for (_, cmd) in held {
            if cdc_tx.send(cmd).is_err() {
                return;
            }
        }
    });
    relay_tx
}
#[cfg(not(feature = "fault-injection"))]
fn fault_cdc_apply_lag_relay(cdc_tx: UnboundedSender<CdcCommand>) -> UnboundedSender<CdcCommand> {
    cdc_tx
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
    watermark_nudge: Arc<tokio::sync::Notify>,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("cdc loop");
    rt.block_on(async {
        // Shadowed for the whole stream/reconnect loop so a fault-injected
        // apply lag covers reconnected processors too.
        let cdc_tx = fault_cdc_apply_lag_relay(cdc_tx);
        let mut cdc = CdcProcessor::new(
            settings,
            cdc_tx.clone(),
            Arc::clone(&active_relations),
            Arc::clone(&watermark_nudge),
        )
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
                    Arc::clone(&watermark_nudge),
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
