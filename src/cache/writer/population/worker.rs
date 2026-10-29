//! The population dispatcher and the persistent worker loop: idle-slot
//! rendezvous, retire / park (PGC-437), connection self-healing, deadlock
//! retry, and handing the result back to the writer.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use metrics::Histogram;
use tokio::sync::mpsc::error::SendError;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot;
use tokio::time::sleep;
use tokio_postgres::Client;
use tracing::{debug, error, trace};

use super::stream::population_task;
use super::{
    PopulationConnections, PopulationOutcome, PopulationSpawnContext, PopulationWork, WorkItem,
};
use crate::cache::messages::{PopulationMerge, QueryCommand};
use crate::cache::population_pool::{POPULATION_PARK_EXPIRY, PopulationPool};
use crate::cache::reference_settings;
use crate::cache::writer::deadlock::{SQLSTATE_DEADLOCK, cache_error_sqlstate};
use crate::cache::writer::merge_queue::StagingDiscard;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::pg;
use crate::result::error_chain_format;
use crate::settings::PgSettings;
use crate::timing::duration_to_us_u64;

/// Retry budget for a population that hits a deadlock (40P01). Populations load
/// private, unindexed staging tables (PGC-250), so the shared-index case this was
/// written for (PGC-147) can't recur; a retry stays safe because staging is
/// emptied and the snapshot retaken first.
const POPULATION_DEADLOCK_MAX_RETRIES: u32 = 5;
const POPULATION_DEADLOCK_BACKOFF_BASE: Duration = Duration::from_millis(20);

/// How long an idle worker waits for work before looping to re-evaluate
/// retirement. The only retire/park check is at the loop top, which a worker
/// suspended in the idle rendezvous never reaches — without a bounded wait a
/// quiet pool never drains to its floor (PGC-455). Matches the controller's
/// shrink cadence, so the pool releases about one worker per shrink step.
const IDLE_RETIRE_RECHECK: Duration = Duration::from_secs(30);

/// Tell the writer a population failed, so its `Failed` handler releases the
/// staging tables and deleted-key tracking and forwards waiters to origin.
fn population_failed_send(
    query_tx: &UnboundedSender<QueryCommand>,
    work: &PopulationWork,
) -> Result<(), SendError<QueryCommand>> {
    query_tx.send(QueryCommand::Failed {
        fingerprint: work.fingerprint,
        generation: work.generation,
    })
}

/// Take the next work item: a staging discard when one is queued, else a
/// population. Discards go first because each is one cheap `DELETE` that
/// returns a staging table (and its disk) to the pool, and they are never on
/// a client's path to a result; a population behind a burst of them waits a
/// few seconds at most. `None` once both queues are closed.
async fn work_item_next(
    work_rx: &mut UnboundedReceiver<PopulationWork>,
    discard_rx: &mut UnboundedReceiver<StagingDiscard>,
) -> Option<WorkItem> {
    tokio::select! {
        biased;
        discard = discard_rx.recv() => match discard {
            Some(discard) => Some(WorkItem::Discard(discard)),
            // The discard side is gone (shutdown); keep serving populations
            // until their queue closes too.
            None => work_rx.recv().await.map(WorkItem::Populate),
        },
        work = work_rx.recv() => work.map(WorkItem::Populate),
    }
}

/// Dispatcher between the shared population work queues and the idle workers.
/// Owns the work receivers; each idle worker registers a one-shot slot and the
/// dispatcher pairs the next work item with the next idle slot, so no item is
/// ever bound to a busy worker (no head-of-line blocking) and worker count can
/// change without re-routing. Discards are not population work: they do not
/// count toward the queue gauge the pool controller scales on.
pub(crate) async fn population_dispatcher(
    mut work_rx: UnboundedReceiver<PopulationWork>,
    mut discard_rx: UnboundedReceiver<StagingDiscard>,
    mut idle_rx: UnboundedReceiver<oneshot::Sender<WorkItem>>,
    query_tx: UnboundedSender<QueryCommand>,
    pool: Arc<PopulationPool>,
) {
    let queue_handle = crate::metrics::population_queue_handle();
    while let Some(mut item) = work_item_next(&mut work_rx, &mut discard_rx).await {
        // Queue gauge + controller backlog hint (PGC-452); length never
        // approaches 2^53.
        pool.queue_depth_set(work_rx.len());
        #[allow(clippy::cast_precision_loss)]
        queue_handle.set(work_rx.len() as f64);
        loop {
            let Some(slot) = idle_rx.recv().await else {
                // The idle channel closes only when the registration (which
                // holds a sender in its spawn context) is dropped — shutdown.
                // With zero live workers this recv simply blocks until the
                // reconcile tick respawns one. Best-effort fail in-hand
                // population work so the writer's `Failed` handler releases
                // its staging tables and deleted-key tracking; an in-hand
                // discard has nothing to release (its tables stay checked
                // out until the cache reset that follows a shutdown).
                if let WorkItem::Populate(work) = &item {
                    debug!(
                        "population dispatcher shutting down with query {} in hand",
                        work.fingerprint
                    );
                    let _ = population_failed_send(&query_tx, work);
                }
                return;
            };
            // A slot whose worker died hands the work back; try the next one.
            match slot.send(item) {
                Ok(()) => break,
                Err(returned) => item = returned,
            }
        }
    }
    debug!("population dispatcher shutting down");
}

/// Connect a worker's cache-side session, with staging NOTICE noise
/// suppressed: setup runs `DROP TABLE IF EXISTS` defensively before every
/// CREATE and the table almost never exists (names embed the generation).
async fn population_cache_connect(settings: &PgSettings, id: usize) -> CacheResult<Client> {
    let cache_conn = pg::connect(settings, &format!("population worker {id}"))
        .await
        .map_into_report::<CacheError>()?;
    cache_conn
        .batch_execute("SET client_min_messages = warning")
        .await
        .map_into_report::<CacheError>()?;
    reference_settings::pin(&cache_conn, "population cache").await?;
    Ok(cache_conn)
}

/// Open one population worker's connection pair (origin + cache).
pub(crate) async fn population_worker_connect(
    ctx: &PopulationSpawnContext,
    id: usize,
) -> CacheResult<PopulationConnections> {
    let cache = population_cache_connect(&ctx.cache_settings, id).await?;
    // Each worker reads from origin on its own connection so the origin
    // executes population SELECTs concurrently rather than serializing
    // them on one shared backend.
    let origin = pg::connect(&ctx.origin_settings, &format!("population origin {id}"))
        .await
        .map_into_report::<CacheError>()?;
    reference_settings::pin(&origin, "population origin").await?;
    Ok(PopulationConnections { origin, cache })
}

/// Run one persistent population worker to completion: register an idle slot
/// with the dispatcher, execute the work item it is handed, repeat. A worker
/// that finds the pool above its target retires itself between work items
/// (PGC-437).
pub(crate) async fn population_worker_run(
    ctx: PopulationSpawnContext,
    id: usize,
    connections: PopulationConnections,
) {
    PopulationWorker::new(ctx, id, connections).run().await;
}

/// What the worker does after one pass at the loop top.
enum WorkerNext {
    Work(WorkItem),
    /// The idle wait timed out; re-check retirement.
    Recheck,
    /// Retired by a pool shrink; the id is already returned.
    Retired,
    /// The dispatcher is gone.
    Shutdown,
}

struct PopulationWorker {
    id: usize,
    idle_tx: UnboundedSender<oneshot::Sender<WorkItem>>,
    origin_settings: PgSettings,
    cache_settings: PgSettings,
    query_tx: UnboundedSender<QueryCommand>,
    throttled: Arc<AtomicBool>,
    pool: Arc<PopulationPool>,
    connections: PopulationConnections,
    idle_handle: Histogram,
    idle_start: Instant,
}

impl PopulationWorker {
    fn new(ctx: PopulationSpawnContext, id: usize, connections: PopulationConnections) -> Self {
        let PopulationSpawnContext {
            idle_tx,
            cache_settings,
            origin_settings,
            query_tx,
            throttled,
            pool,
        } = ctx;
        Self {
            id,
            idle_tx,
            origin_settings,
            cache_settings,
            query_tx,
            throttled,
            pool,
            connections,
            idle_handle: crate::metrics::population_worker_idle_handle(id),
            idle_start: Instant::now(),
        }
    }

    async fn run(mut self) {
        self.pool.pending_connect_done();
        debug!("population worker {} started", self.id);
        loop {
            match self.work_next().await {
                WorkerNext::Work(WorkItem::Populate(work)) => self.work_handle(work).await,
                WorkerNext::Work(WorkItem::Discard(discard)) => self.discard_handle(discard).await,
                WorkerNext::Recheck => {}
                WorkerNext::Retired => return,
                WorkerNext::Shutdown => break,
            }
        }
        self.pool.worker_exit(self.id);
        debug!("population worker {} shutting down", self.id);
    }

    /// Retire or park if the pool is above target, then wait (bounded) for
    /// the dispatcher to hand over the next work item.
    async fn work_next(&self) -> WorkerNext {
        if self.retire_check().await {
            return WorkerNext::Retired;
        }
        let (slot_tx, slot_rx) = oneshot::channel();
        if self.idle_tx.send(slot_tx).is_err() {
            return WorkerNext::Shutdown;
        }
        // Bounded wait (PGC-455): on timeout the abandoned slot reads as dead
        // to the dispatcher — which already skips dead slots — and the loop
        // top re-checks retirement before registering a fresh one.
        match tokio::time::timeout(IDLE_RETIRE_RECHECK, slot_rx).await {
            Ok(Ok(item)) => WorkerNext::Work(item),
            // The dispatcher dropped the unused slot: shutdown.
            Ok(Err(_)) => WorkerNext::Shutdown,
            Err(_) => WorkerNext::Recheck,
        }
    }

    /// True when this worker claimed a retirement and was not unparked.
    async fn retire_check(&self) -> bool {
        if !self.pool.retire_claim() {
            return false;
        }
        if population_worker_park(&self.pool).await {
            debug!("population worker {} unparked (probe reuse)", self.id);
            return false;
        }
        self.pool.retired_id_return(self.id);
        debug!("population worker {} retired (pool shrink)", self.id);
        true
    }

    async fn work_handle(&mut self, work: PopulationWork) {
        // Under memory pressure, skip populating the in-flight backlog: building
        // these cache tables/rows is what overshoots the budget after dispatch
        // stops admitting new queries. Fail them so `query_failed_cleanup`
        // removes the query and forwards any coalesced waiters to origin.
        if self.throttled.load(Ordering::Relaxed) {
            crate::metrics::handles()
                .cache
                .registration_throttled_total
                .increment(1);
            let _ = population_failed_send(&self.query_tx, &work);
            return;
        }
        // Time spent waiting on rx — recorded as a histogram so the `_sum`
        // gives cumulative idle time per worker (utilization signal) and the
        // quantiles surface variance. Pairs with task_seconds and wall clock
        // to compute per-worker utilization.
        self.idle_handle
            .record(self.idle_start.elapsed().as_secs_f64());

        let wait = work.enqueued_at.elapsed();
        crate::metrics::handles()
            .reg
            .population_wait
            .record(wait.as_secs_f64());
        self.pool.wait_observe(duration_to_us_u64(wait));

        if !self.connections_heal().await {
            let _ = population_failed_send(&self.query_tx, &work);
            return;
        }

        let task_start = Instant::now();
        let result = self.task_run(&work).await;
        let fetch_stage = task_start.elapsed();
        crate::metrics::handles()
            .reg
            .population_task
            .record(fetch_stage.as_secs_f64());
        self.pool.task_observe(duration_to_us_u64(fetch_stage));

        self.idle_start = Instant::now();
        self.outcome_send(&work, result, fetch_stage);
    }

    /// Empty the staging tables of a population that will never merge
    /// (PGC-553) and tell the writer, which returns them to the pool. Runs
    /// under memory throttling too — it releases resources — and needs only
    /// the cache connection. Not population work: it records no wait or task
    /// time, which drive the pool controller.
    async fn discard_handle(&mut self, discard: StagingDiscard) {
        self.idle_handle
            .record(self.idle_start.elapsed().as_secs_f64());
        let emptied = self.cache_heal().await
            && staging_tables_clear(
                &self.connections.cache,
                discard.staged.iter().map(|(_, table)| table.as_str()),
            )
            .await;
        if !emptied {
            error!(
                "population worker {}: staging discard failed for {}; the writer empties it",
                self.id, discard.fingerprint
            );
        }
        self.idle_start = Instant::now();
        let ack = QueryCommand::Discarded {
            fingerprint: discard.fingerprint,
            generation: discard.generation,
            emptied,
        };
        if self.query_tx.send(ack).is_err() {
            error!("population worker {}: failed to send Discarded", self.id);
        }
    }

    /// Reopen a closed origin or cache connection before reading. A dropped
    /// connection only surfaces on the next work item (a mid-population drop
    /// fails that population, which forwards to origin); without this the
    /// worker would fail every population until the cache subsystem
    /// restarts. False when a reconnect failed.
    async fn connections_heal(&mut self) -> bool {
        self.origin_heal().await && self.cache_heal().await
    }

    async fn origin_heal(&mut self) -> bool {
        if !self.connections.origin.is_closed() {
            return true;
        }
        let id = self.id;
        match pg::connect(&self.origin_settings, &format!("population origin {id}")).await {
            Ok(c) => match reference_settings::pin(&c, "population origin").await {
                Ok(()) => {
                    self.connections.origin = c;
                    true
                }
                Err(e) => {
                    error!(
                        "population worker {id}: origin session pin failed: {}",
                        error_chain_format(e.current_context())
                    );
                    false
                }
            },
            Err(e) => {
                error!("population worker {id}: origin reconnect failed: {e}");
                false
            }
        }
    }

    /// The cache side dies the same ways (backend kill, reset) and every
    /// staging write needs it — without this check a dead cache connection
    /// made the worker a permanent population-failure sink (PGC-451).
    async fn cache_heal(&mut self) -> bool {
        if !self.connections.cache.is_closed() {
            return true;
        }
        match population_cache_connect(&self.cache_settings, self.id).await {
            Ok(c) => {
                self.connections.cache = c;
                true
            }
            Err(e) => {
                error!(
                    "population worker {}: cache reconnect failed: {}",
                    self.id,
                    error_chain_format(e.current_context())
                );
                false
            }
        }
    }

    /// Run the population task, retrying a deadlock with exponential backoff.
    async fn task_run(&self, work: &PopulationWork) -> CacheResult<PopulationOutcome> {
        let mut attempt: u32 = 0;
        loop {
            let result = population_task(work, &self.connections).await;
            let deadlock = matches!(&result, Err(e)
                if cache_error_sqlstate(e.current_context()) == Some(SQLSTATE_DEADLOCK));
            if !deadlock || attempt >= POPULATION_DEADLOCK_MAX_RETRIES {
                return result;
            }
            let backoff = POPULATION_DEADLOCK_BACKOFF_BASE * 2u32.pow(attempt);
            attempt += 1;
            trace!(
                "population worker {}: query {} deadlocked, retry {attempt}/{POPULATION_DEADLOCK_MAX_RETRIES} after {backoff:?}",
                self.id, work.fingerprint,
            );
            staging_tables_clear(
                &self.connections.cache,
                work.staging.iter().map(|(_, table, _)| table.as_str()),
            )
            .await;
            sleep(backoff).await;
        }
    }

    /// Hand the staged snapshot to the writer, or report the failure.
    fn outcome_send(
        &self,
        work: &PopulationWork,
        result: CacheResult<PopulationOutcome>,
        fetch_stage: Duration,
    ) {
        let id = self.id;
        match result {
            Ok(outcome) => {
                // The writer merges it into the shared cache table when no CDC
                // frame is open and then marks the query Ready once the
                // watermark reaches the snapshot LSN (PGC-250).
                let merge = PopulationMerge {
                    fingerprint: work.fingerprint,
                    generation: work.generation,
                    staged: outcome.staged,
                    cached_bytes: outcome.cached_bytes,
                    row_count: outcome.row_count,
                    snapshot: outcome.snapshot,
                    snapshot_lsn: outcome.snapshot_lsn,
                    enqueued_at: Instant::now(),
                    fetch_stage_ms: fetch_stage.as_secs_f64() * 1000.0,
                };
                if self.query_tx.send(QueryCommand::Merge(merge)).is_err() {
                    error!("population worker {id}: failed to send QueryMerge");
                }
            }
            Err(e) => {
                // Staging tables are returned to the pool by the writer's
                // `Failed` handler (`staging_checkin`) — the worker no longer
                // drops them (PGC-293).

                // Log the bare SQLSTATE, not the error chain: the chain walker
                // leaks the offending SQL via the PG DETAIL field (PGC-133).
                let sqlstate = cache_error_sqlstate(e.current_context());
                error!(
                    "population worker {id}: population failed for query {} sqlstate={}: {e}",
                    work.fingerprint,
                    sqlstate.unwrap_or("-"),
                );
                if population_failed_send(&self.query_tx, work).is_err() {
                    error!("population worker {id}: failed to send QueryFailed");
                }
            }
        }
    }
}

/// Empty staging tables on the worker's connection: a partial load before a
/// deadlock retry re-streams (the pool slot isn't recreated between attempts,
/// PGC-293), or the staging of a discarded population. False if any `DELETE`
/// failed.
async fn staging_tables_clear(db_cache: &Client, tables: impl Iterator<Item = &str>) -> bool {
    let mut cleared = true;
    for table in tables {
        if let Err(e) = db_cache
            .batch_execute(&format!("DELETE FROM pgcache_stage.{table}"))
            .await
        {
            error!("staging clear: emptying {table}: {e}");
            cleared = false;
        }
    }
    cleared
}

/// Park a retired worker instead of exiting: hold the connection pair until
/// the reconcile grants an unpark (resume: true) or the park expires
/// (exit: false). The credit CAS arbitrates the expiry/grant race, and a
/// grant that lands after expiry is swept post-release so the reconcile's
/// pending count can't stay stuck (ADR-052/053 damping).
async fn population_worker_park(pool: &PopulationPool) -> bool {
    if !pool.park_claim() {
        return false;
    }
    let expiry = tokio::time::Instant::now() + POPULATION_PARK_EXPIRY;
    loop {
        if unpark_try(pool) {
            return true;
        }
        if tokio::time::timeout_at(expiry, pool.unpark_notified())
            .await
            .is_err()
        {
            break;
        }
    }
    // Expired: final grant check, then leave the slot and sweep any grant
    // that raced in after that check.
    if unpark_try(pool) {
        return true;
    }
    pool.park_release();
    let _ = pool.unpark_credit_take();
    false
}

fn unpark_try(pool: &PopulationPool) -> bool {
    let granted = pool.unpark_credit_take();
    if granted {
        pool.unpark_complete();
    }
    granted
}
