use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use tokio::{
    runtime::Builder,
    sync::mpsc::{
        Receiver, Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel,
    },
    task::{LocalSet, spawn_local},
};
use tokio_postgres::{Config, NoTls};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, instrument, warn};

use crate::{
    cache::{
        CacheError, CacheResult, MapIntoReport, PinnedQuery, ReportExt, StatusRequest,
        cdc::CdcProcessor,
        messages::{CacheOutcome, CacheReply, CdcCommand, CdcSignal, ProxyMessage, WriterNotify},
        query_cache::{QueryCache, QueryRequest, WorkerRequest, reply_forward},
        types::{ActiveRelations, CacheStateView, WorkerMetrics},
        worker::{CoalescedOutcome, SQLSTATE_UNDEFINED_TABLE, handle_cached_query},
        writer::writer_run,
    },
    metrics::names,
    pg::{cache_connection::CacheConnection, cdc::slot_confirmed_lsn},
    result::error_chain_format,
    settings::Settings,
    timing::duration_to_us_u64,
};

/// Minimum number of connections in the cache worker pool.
const MIN_POOL_SIZE: usize = 4;

/// Handles a proxy message by converting it to a query request and dispatching it
async fn handle_proxy_message(qcache: &mut QueryCache, proxy_msg: ProxyMessage) {
    match proxy_msg.message.into_query_data() {
        Ok(query_data) => {
            let request = QueryRequest {
                query_type: query_data.query_type,
                data: query_data.data,
                cacheable_query: query_data.cacheable_query,
                result_formats: query_data.result_formats,
                client_socket: proxy_msg.client_socket,
                reply_tx: proxy_msg.reply_tx,
                search_path: proxy_msg.search_path,
                timing: proxy_msg.timing,
                pipeline: proxy_msg.pipeline,
            };
            if let Err(e) = qcache.query_dispatch(request).await {
                error!(
                    "query dispatch failed: {}",
                    error_chain_format(e.current_context()),
                );
            }
        }
        Err((e, data)) => {
            debug!("forwarding to origin due to parameter conversion error: {e}");
            let _ = reply_forward(
                proxy_msg.reply_tx,
                proxy_msg.client_socket,
                proxy_msg.pipeline,
                data,
                proxy_msg.timing,
            );
        }
    }
}

/// Handles a worker request by executing the query and sending the reply.
/// Sends replies for both the primary client and any coalesced clients.
async fn handle_worker_request(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    mut msg: WorkerRequest,
    worker_metrics_tx: UnboundedSender<WorkerMetrics>,
) {
    debug!("cache worker task spawn");

    msg.timing.worker_start_at = Some(Instant::now());

    let reply = match handle_cached_query(conn, return_tx, &mut msg).await {
        Ok((bytes_served, coalesced_outcomes)) => {
            let latency_us = msg
                .timing
                .worker_start_at
                .map(|s| duration_to_us_u64(s.elapsed()))
                .unwrap_or(0);
            let _ = worker_metrics_tx.send(WorkerMetrics {
                fingerprint: msg.fingerprint,
                latency_us,
                bytes_served: bytes_served as u64,
            });

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
            // Coalesced clients already received Error replies inside the worker
            let error_buf = msg
                .forward_bytes
                .take()
                .unwrap_or_else(|| msg.data.split_off(0));
            CacheReply {
                socket: msg.client_socket,
                outcome: CacheOutcome::Error(error_buf),
            }
        }
    };

    if msg.reply_tx.send(reply).is_err() {
        error!("failed to send reply: no receiver");
    }

    debug!("cache worker task done");
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

        Ok(())
    })
}

/// Main cache runtime - handles proxy queries and CDC events
#[instrument(skip_all)]
pub fn cache_run(
    settings: &Settings,
    cache_rx: Receiver<ProxyMessage>,
    pinned: &[PinnedQuery],
    cancel: CancellationToken,
    status_rx: Receiver<StatusRequest>,
) -> CacheResult<()> {
    // Reset cache database before starting anything
    cache_database_reset(settings).attach_loc("resetting cache database")?;

    thread::scope(|scope| {
        let rt = Builder::new_current_thread()
            .enable_all()
            .build()
            .map_into_report::<CacheError>()?;

        // Create shared state view for coordinator to read cache state
        let state_view = Arc::new(CacheStateView::new());

        // Shared set of active relation OIDs (writer writes, CDC reads)
        let active_relations: ActiveRelations =
            Arc::new(ArcSwap::from_pointee(std::collections::HashSet::new()));

        // PGC-140: a writer exit must cancel cdc/worker too, else `thread::scope`
        // never unwinds and the supervisor never restarts the cache. Shadow with a
        // child token (not `cancel` — it's shared across restarts), drop-guarded.
        let cancel = cancel.child_token();
        let _cancel_guard = cancel.clone().drop_guard();

        // Spawn writer thread (owns Cache, serializes all mutations)
        // Two channels: one for query registration, one for CDC commands
        let (query_tx, query_rx) = unbounded_channel();
        let (cdc_cmd_tx, cdc_cmd_rx) = unbounded_channel();
        let (worker_metrics_tx, worker_metrics_rx) = unbounded_channel::<WorkerMetrics>();
        let (notify_tx, mut notify_rx) = unbounded_channel::<WriterNotify>();
        let state_view_writer = Arc::clone(&state_view);
        let active_relations_writer = Arc::clone(&active_relations);
        let settings_writer = settings.clone();
        let cancel_writer = cancel.child_token();
        let _writer_handle = thread::Builder::new()
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

        // Spawn worker thread (executes cached queries - read-only)
        let (worker_tx, worker_rx) = unbounded_channel();
        let cancel_worker = cancel.child_token();
        let _worker_handle = thread::Builder::new()
            .name("cache worker".to_owned())
            .spawn_scoped(scope, || {
                let result = worker_run(settings, worker_rx, worker_metrics_tx, cancel_worker);
                if let Err(ref e) = result {
                    error!(
                        "worker thread exiting with error: {}",
                        error_chain_format(e.current_context()),
                    );
                }
                result
            })
            .map_into_report::<CacheError>()
            .attach_loc("spawning worker thread")?;

        // Spawn CDC thread -- sends CdcCommand directly to writer, signals coordinator
        let (cdc_signal_tx, mut cdc_signal_rx) = unbounded_channel::<CdcSignal>();
        let active_relations_cdc = Arc::clone(&active_relations);
        let cancel_cdc = cancel.child_token();
        let _cdc_handle = thread::Builder::new()
            .name("cdc worker".to_owned())
            .spawn_scoped(scope, move || {
                let result = cdc_run(
                    settings,
                    cdc_cmd_tx,
                    active_relations_cdc,
                    cancel_cdc,
                    cdc_signal_tx,
                );
                if let Err(ref e) = result {
                    error!(
                        "cdc thread exiting with error: {}",
                        error_chain_format(e.current_context()),
                    );
                }
                result
            })
            .map_into_report::<CacheError>()
            .attach_loc("spawning CDC thread")?;

        debug!("cache loop");
        rt.block_on(async {
            let qcache = QueryCache::new(
                settings,
                query_tx.clone(),
                worker_tx,
                Arc::clone(&state_view),
            )
            .await
            .attach_loc("creating query cache")?;

            qcache
                .pinned_queries_register(pinned)
                .attach_loc("registering pinned queries")?;

            LocalSet::new()
                .run_until(async move {
                    let mut cache_rx = cache_rx;
                    let mut worker_metrics_rx = worker_metrics_rx;
                    let mut cdc_connected = true;
                    loop {
                        // Block until at least one message arrives
                        tokio::select! {
                            _ = cancel.cancelled() => {
                                debug!("cache coordinator shutdown signal received");
                                break;
                            }
                            _ = query_tx.closed() => {
                                error!("writer thread exited unexpectedly");
                                return Err(CacheError::WriterFailure.into());
                            }
                            signal = cdc_signal_rx.recv() => {
                                match signal {
                                    Some(CdcSignal::Disconnected { last_flushed_lsn }) => {
                                        warn!("CDC disconnected (last_flushed_lsn: {last_flushed_lsn}), forwarding queries to origin");
                                        cdc_connected = false;
                                    }
                                    Some(CdcSignal::Reconnected) => {
                                        debug!("CDC reconnected, resuming cache dispatch");
                                        cdc_connected = true;
                                    }
                                    Some(CdcSignal::Fatal) | None => {
                                        error!("CDC fatal error or thread exited");
                                        return Err(CacheError::CdcFailure.into());
                                    }
                                }
                            }
                            msg = cache_rx.recv() => {
                                match msg {
                                    Some(proxy_msg) => {
                                        if cdc_connected {
                                            let mut qcache = qcache.clone();
                                            spawn_local(async move {
                                                handle_proxy_message(&mut qcache, proxy_msg).await;
                                            });
                                        } else {
                                            // CDC is down; forward to origin to avoid serving stale data
                                            let data = proxy_msg.message.into_data();
                                            let _ = reply_forward(
                                                proxy_msg.reply_tx,
                                                proxy_msg.client_socket,
                                                proxy_msg.pipeline,
                                                data,
                                                proxy_msg.timing,
                                            );
                                        }
                                    }
                                    None => {
                                        debug!("proxy channel closed");
                                        break;
                                    }
                                }
                            }
                            // Record worker metrics (cache-hit latency, bytes served)
                            msg = worker_metrics_rx.recv() => {
                                if let Some(wm) = msg {
                                    worker_metrics_record(&state_view, wm);
                                }
                            }
                            // Drain coalesced waiters when writer signals Ready/Failed
                            notify = notify_rx.recv() => {
                                match notify {
                                    Some(WriterNotify::Ready { fingerprint, generation, resolved, deparsed_sql, max_limit }) => {
                                        qcache.waiting_drain_ready(fingerprint, generation, resolved, deparsed_sql, max_limit);
                                    }
                                    Some(WriterNotify::Failed { fingerprint }) => {
                                        qcache.waiting_drain_failed(fingerprint);
                                    }
                                    None => {
                                        error!("writer notify channel closed");
                                        return Err(CacheError::WriterFailure.into());
                                    }
                                }
                            }
                        }

                        // Channel depth gauge; queue length never approaches 2^53.
                        #[allow(clippy::cast_precision_loss)]
                        metrics::gauge!(names::CACHE_PROXY_MESSAGE_QUEUE)
                            .set(cache_rx.len() as f64);
                    }

                    debug!("cache loop exiting");
                    Ok(())
                })
                .await
        })
    })
}

/// Record worker-reported metrics (cache-hit latency, bytes served).
fn worker_metrics_record(state_view: &CacheStateView, wm: WorkerMetrics) {
    if let Some(mut m) = state_view.metrics.get_mut(&wm.fingerprint) {
        m.total_bytes_served += wm.bytes_served;
        m.cache_hit_latency.saturating_record(wm.latency_us);
    }
}

/// Worker runtime - executes cached queries against the database
fn worker_run(
    settings: &Settings,
    mut worker_rx: UnboundedReceiver<WorkerRequest>,
    worker_metrics_tx: UnboundedSender<WorkerMetrics>,
    cancel: CancellationToken,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("worker loop");
    rt.block_on(async {
        let pool_size = (settings.num_workers * 2).max(MIN_POOL_SIZE);
        let (conn_tx, mut conn_rx) = connection_pool_create(settings, pool_size)
            .await
            .attach_loc("creating connection pool")?;

        LocalSet::new()
            .run_until(async move {
                loop {
                    // Block for at least one request
                    let mut msg = tokio::select! {
                        _ = cancel.cancelled() => {
                            debug!("cache worker shutdown signal received");
                            break;
                        }
                        msg = worker_rx.recv() => {
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
                            return Err(CacheError::NoConnection.into());
                        };
                        conn
                    };
                    msg.timing.conn_acquired_at = Some(Instant::now());

                    // Spawn task with both the request and connection
                    let return_tx = conn_tx.clone();
                    let metrics_tx = worker_metrics_tx.clone();
                    spawn_local(async move {
                        handle_worker_request(conn, return_tx, msg, metrics_tx).await;
                    });

                    // Channel depth gauge; queue length never approaches 2^53.
                    #[allow(clippy::cast_precision_loss)]
                    metrics::gauge!(names::CACHE_WORKER_QUEUE).set(worker_rx.len() as f64);
                }

                Ok(())
            })
            .await
    })
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
    signal_tx: UnboundedSender<CdcSignal>,
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

            // Signal coordinator to forward all queries to origin
            if signal_tx
                .send(CdcSignal::Disconnected {
                    last_flushed_lsn: saved_lsn,
                })
                .is_err()
            {
                return Err(CacheError::CdcFailure.into());
            }

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
                            let _ = signal_tx.send(CdcSignal::Fatal);
                            return Err(CacheError::CdcFailure.into());
                        }
                        debug!("slot LSN verified: confirmed={confirmed_lsn}, saved={saved_lsn}");
                    }
                    Ok(None) => {
                        error!("replication slot no longer exists");
                        let _ = signal_tx.send(CdcSignal::Fatal);
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
                        if signal_tx.send(CdcSignal::Reconnected).is_err() {
                            return Err(CacheError::CdcFailure.into());
                        }
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
