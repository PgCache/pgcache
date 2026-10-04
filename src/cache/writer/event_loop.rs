//! The writer's single-threaded event loop: owns `WriterCore` plus the two
//! responsibility managers (`WriterCdc`, `WriterRegistration`) and serializes
//! their access to the core through one select loop.

use std::cmp::Reverse;
use std::sync::Arc;
use std::time::Duration;

use tokio::runtime::Builder;
use tokio::sync::mpsc::{Receiver, UnboundedReceiver};
use tokio::task::LocalSet;
use tokio::time::Interval;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use super::cdc::WriterCdc;
#[cfg(feature = "fault-injection")]
use super::core::fault;
use super::core::{WriterCore, WriterShared};
use super::frame::FrameState;
use super::merge_queue::InputQueues;
use super::registration::WriterRegistration;
use crate::cache::messages::{CdcCommand, QueryCommand};
use crate::cache::status::StatusRequest;
use crate::cache::{CacheError, CacheResult, MapIntoReport};
use crate::result::error_chain_format;
use crate::settings::Settings;

/// Max full evictions per periodic-tick `eviction_run` call (PGC-251). Bounds the
/// single-threaded writer stall when reclaiming a large count-cap overshoot; the
/// remainder is reclaimed on subsequent ticks.
const EVICTION_TICK_BUDGET: usize = 512;

/// The inputs the writer loop reads from.
pub(crate) struct WriterChannels {
    /// Query commands from dispatch.
    pub(crate) query_rx: UnboundedReceiver<QueryCommand>,
    /// CDC commands from the CDC thread.
    pub(crate) cdc_rx: UnboundedReceiver<CdcCommand>,
    /// Status requests from the admin HTTP server.
    pub(crate) status_rx: Receiver<StatusRequest>,
    pub(crate) cancel: CancellationToken,
}

/// Main writer runtime: a current-thread runtime driving [`WriterLoop`].
pub(crate) fn writer_run(
    settings: &Settings,
    channels: WriterChannels,
    shared: WriterShared,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("writer loop");
    rt.block_on(async {
        // Boxed: the writer future's state machine has outgrown clippy's
        // large-futures threshold; one heap allocation at startup keeps it
        // off the spawning task's stack.
        LocalSet::new()
            .run_until(Box::pin(async move {
                // Built inside the LocalSet so WriterRegistration can spawn_local
                // its population workers.
                let writer = WriterLoop::new(settings, channels, shared).await?;
                writer.run().await
            }))
            .await
    })
}

/// Which channel a query command arrived on.
#[derive(Clone, Copy)]
enum QuerySource {
    /// Dispatch, via `query_rx`.
    Dispatch,
    /// Spawned population tasks, via `internal_rx`.
    Internal,
}

impl QuerySource {
    fn name(self) -> &'static str {
        match self {
            Self::Dispatch => "query",
            Self::Internal => "internal",
        }
    }
}

/// Whether the loop keeps going after an event.
enum LoopStep {
    Continue,
    Stop,
}

struct WriterLoop {
    core: WriterCore,
    registration: WriterRegistration,
    writer_cdc: WriterCdc,
    query_rx: UnboundedReceiver<QueryCommand>,
    cdc_rx: UnboundedReceiver<CdcCommand>,
    /// Commands from spawned population tasks.
    internal_rx: UnboundedReceiver<QueryCommand>,
    status_rx: Receiver<StatusRequest>,
    cancel: CancellationToken,
    /// Gauges (queries_loading/pending/invalidated, disk_used_bytes,
    /// generation, tables_tracked, update_queries_total/max) used to be emitted
    /// from every query/CDC command. state_gauges_update iterates the entire
    /// state_view DashMap, which dominated writer per-command time at scale.
    /// Emit on a 1s tick instead — well below typical Prometheus scrape
    /// intervals.
    gauges_interval: Interval,
}

impl WriterLoop {
    async fn new(
        settings: &Settings,
        channels: WriterChannels,
        shared: WriterShared,
    ) -> CacheResult<Self> {
        // Loopback channel: population workers (and the writer itself) send
        // query commands back through it.
        let (query_tx, internal_rx) = tokio::sync::mpsc::unbounded_channel();
        let core = WriterCore::new(settings, shared, query_tx.clone()).await?;
        let registration = WriterRegistration::new(
            settings,
            &core.db_origin,
            query_tx,
            Arc::clone(&core.state_view.registration_throttled),
            Arc::clone(&core.state_view.population_pool),
        )
        .await?;
        let writer_cdc = WriterCdc::new(settings, Arc::clone(&core.state_view.settled_lsn)).await?;

        let mut gauges_interval = tokio::time::interval(Duration::from_secs(1));
        gauges_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        #[cfg(feature = "fault-injection")]
        fault::init();

        let WriterChannels {
            query_rx,
            cdc_rx,
            status_rx,
            cancel,
        } = channels;
        Ok(Self {
            core,
            registration,
            writer_cdc,
            query_rx,
            cdc_rx,
            internal_rx,
            status_rx,
            cancel,
            gauges_interval,
        })
    }

    async fn run(mut self) -> CacheResult<()> {
        loop {
            let step = tokio::select! {
                () = self.cancel.cancelled() => {
                    debug!("writer shutdown signal received");
                    LoopStep::Stop
                }
                _ = self.gauges_interval.tick() => {
                    self.tick_handle().await;
                    LoopStep::Continue
                }
                msg = self.query_rx.recv() => self.query_command_received(msg, QuerySource::Dispatch).await,
                msg = self.cdc_rx.recv() => self.cdc_command_received(msg).await?,
                msg = self.internal_rx.recv() => self.query_command_received(msg, QuerySource::Internal).await,
                msg = self.status_rx.recv() => {
                    if let Some(req) = msg {
                        self.core
                            .status_respond(req, self.writer_cdc.last_received_lsn)
                            .await;
                    }
                    LoopStep::Continue
                }
                // Advance the in-progress population merge by one chunk
                // (PGC-418); see `merge_chunk_due` for when.
                () = std::future::ready(()), if self.merge_chunk_due() => {
                    self.merge_chunk_step().await;
                    LoopStep::Continue
                }
            };
            if matches!(step, LoopStep::Stop) {
                return Ok(());
            }
            self.merges_drain_if_quiescent().await;
            self.queue_gauges_publish();
        }
    }

    /// The 1s tick: gauges, pool reconcile, memo GC, eviction, disk pressure.
    async fn tick_handle(&mut self) {
        self.merge_gate_diagnose();
        let core = &mut self.core;
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles()
            .reg
            .merge_pending_depth
            .set(core.merges.pending.len() as f64);
        core.disk_stats_refresh();
        core.stale_entries_cleanup();
        core.state_gauges_update();
        core.writer_scale_gauges_update();
        self.registration.population_pool_reconcile();
        core.state_view.memo.gc();
        core.state_view.memo.metrics_publish();
        // Eviction runs only here now (not per Ready, PGC-276). Enforce the
        // memory count cap independently of registration: under
        // throttle-freeze no Ready events arrive (PGC-251). Bounded per tick;
        // log-and-continue so a periodic best-effort eviction never kills the
        // writer.
        if let Err(e) = core.eviction_run(Some(EVICTION_TICK_BUDGET)).await {
            error!(
                "periodic eviction failed: {}",
                error_chain_format(e.current_context())
            );
        }
        // Disk-pressure throttle + escalating reclaim (PGC-276).
        if let Err(e) = core.disk_pressure_handle().await {
            error!(
                "disk pressure handling failed: {}",
                error_chain_format(e.current_context())
            );
        }
    }

    /// Diagnose merge-gate stalls (PGC-290): if merges are parked, report why
    /// the drain gate (frame_state == Idle && watermark >= min snapshot_lsn)
    /// is not firing.
    fn merge_gate_diagnose(&self) {
        let core = &self.core;
        if core.merges.pending.is_empty() {
            return;
        }
        let min_snap = core
            .merges
            .pending
            .peek()
            .map(|Reverse(m)| m.0.snapshot_lsn);
        debug!(
            "merge-gate: frame_state={:?} frame_open={} pending={} min_snapshot_lsn={:?} watermark={:?}",
            core.frame_state,
            core.frame_open,
            core.merges.pending.len(),
            min_snap,
            self.writer_cdc.last_received_lsn,
        );
    }

    async fn query_command_received(
        &mut self,
        msg: Option<QueryCommand>,
        source: QuerySource,
    ) -> LoopStep {
        let Some(cmd) = msg else {
            debug!("writer {} channel closed, shutting down", source.name());
            return LoopStep::Stop;
        };
        if let Err(e) = self
            .registration
            .query_command_handle(&mut self.core, cmd)
            .await
        {
            error!(
                "writer {} command failed: {}",
                source.name(),
                error_chain_format(e.current_context()),
            );
        }
        if matches!(source, QuerySource::Dispatch) {
            self.core.merges.command_handled();
        }
        LoopStep::Continue
    }

    /// A run of consecutive keepalive marks is handled as one at its highest
    /// LSN, plus the command that ended the run (PGC-420): a keepalive burst
    /// then costs this one iteration instead of one each.
    async fn cdc_command_received(&mut self, msg: Option<CdcCommand>) -> CacheResult<LoopStep> {
        let Some(cmd) = msg else {
            debug!("writer cdc channel closed, shutting down");
            return Ok(LoopStep::Stop);
        };
        let (cmd, trailing) = cdc_keepalive_run_coalesce(cmd, &mut self.cdc_rx);
        for cmd in std::iter::once(cmd).chain(trailing) {
            self.cdc_command_apply(cmd).await?;
        }
        Ok(LoopStep::Continue)
    }

    async fn cdc_command_apply(&mut self, cmd: CdcCommand) -> CacheResult<()> {
        #[cfg(feature = "fault-injection")]
        if let CdcCommand::Insert { row_data, .. } = &cmd
            && fault::writer_die_check(row_data)
        {
            error!("fault injection: writer exiting on sentinel CDC insert to exercise restart");
            return Err(CacheError::CdcFailure.into());
        }
        // Queue depth after this command drives the batch flush decision
        // (PGC-242): an empty queue flushes immediately; a backlog accumulates
        // frames.
        let queued = self.cdc_rx.len();
        self.writer_cdc
            .cdc_command_handle(&mut self.core, cmd, queued)
            .await
            .inspect_err(|e| {
                // Propagate: tears down the cache subsystem so the supervisor
                // restart rebuilds it from a clean reset.
                error!(
                    "writer cdc command failed, resetting cache: {}",
                    error_chain_format(e.current_context()),
                );
            })
    }

    /// A merge chunk runs when every input queue is drained — never ahead of
    /// queued real work, and the select is unbiased so a ready chunk would
    /// otherwise win random picks against queued commands — or, under a
    /// backlog, once each queue has yielded its unit since the last chunk (a
    /// batch flush; the commands queued at that chunk) so the merge cannot
    /// starve. Only while no frame is open — a chunk on db_cache mid-frame
    /// would join the frame's transaction.
    fn merge_chunk_due(&self) -> bool {
        self.core.frame_state == FrameState::Idle
            && self.core.merges.chunk_due(InputQueues {
                cdc_empty: self.cdc_rx.is_empty(),
                query_empty: self.query_rx.is_empty(),
                internal_empty: self.internal_rx.is_empty(),
            })
    }

    async fn merge_chunk_step(&mut self) {
        if let Err(e) = self
            .registration
            .merge_in_progress_step(&mut self.core)
            .await
        {
            error!(
                "population merge step failed: {}",
                error_chain_format(e.current_context()),
            );
        }
        self.core.merges.chunk_boundary_mark(self.query_rx.len());
    }

    /// Drain population merges while the writer is quiescent (no CDC frame
    /// open), so neither the merge nor eviction (both on db_cache) races the
    /// CDC writer's frame txn on the shared cache table (PGC-250). Each merge
    /// is additionally gated on the apply watermark reaching its snapshot LSN
    /// (PGC-272); the watermark advances on the CDC path, so re-check on every
    /// quiescent iteration — also while a drain is active, so a gated head
    /// behind it keeps being nudged (only starting the next drain waits).
    async fn merges_drain_if_quiescent(&mut self) {
        if !self.core.merges_drain_wanted() {
            return;
        }
        let slot_was_free = self.core.merges.active.is_none();
        if let Err(e) = self
            .registration
            .pending_merges_drain(&mut self.core, self.writer_cdc.last_received_lsn)
            .await
        {
            error!(
                "population merge drain failed: {}",
                error_chain_format(e.current_context()),
            );
        }
        if slot_was_free && self.core.merges.active.is_some() {
            self.core.merges.chunk_boundary_mark(self.query_rx.len());
        }
    }

    /// Fold the writer backlog into the adaptive-gate window every iteration
    /// (PGC-277): catches the drain-to-empty moments the controller's coarse
    /// tick would miss. The internal channel (population completions) is the
    /// backlog that saturates first.
    fn queue_gauges_publish(&self) {
        let internal_depth = self.internal_rx.len();
        self.core.state_view.reg_gate.queue_observe(internal_depth);

        // Channel depths are reported as f64 gauges; queue sizes never approach 2^53.
        #[allow(clippy::cast_precision_loss)]
        {
            let state = &crate::metrics::handles().state;
            state.queue_writer_query.set(self.query_rx.len() as f64);
            state.queue_writer_cdc.set(self.cdc_rx.len() as f64);
            state.queue_writer_internal.set(internal_depth as f64);
        }
    }
}

/// Collapse the run of consecutive `KeepAliveMark`s starting at `first` into
/// one mark at the run's highest LSN (PGC-420). Marks are monotonic and the
/// handler's watermark advance is a max, so one flush + one advance yields the
/// same state as handling each in turn — and no frame command can sit inside
/// the run, because it stops at the first non-keepalive. That command is
/// already off the queue, so it is returned for handling in the same
/// iteration. A non-keepalive `first` is returned untouched.
fn cdc_keepalive_run_coalesce(
    first: CdcCommand,
    cdc_rx: &mut UnboundedReceiver<CdcCommand>,
) -> (CdcCommand, Option<CdcCommand>) {
    let CdcCommand::KeepAliveMark { mut lsn } = first else {
        return (first, None);
    };
    let mut absorbed = 0u64;
    let trailing = loop {
        match cdc_rx.try_recv() {
            Ok(CdcCommand::KeepAliveMark { lsn: next }) => {
                lsn = lsn.max(next);
                absorbed += 1;
            }
            Ok(other) => break Some(other),
            Err(_) => break None,
        }
    };
    if absorbed > 0 {
        crate::metrics::handles()
            .cdc
            .keepalive_marks_coalesced
            .increment(absorbed);
    }
    (CdcCommand::KeepAliveMark { lsn }, trailing)
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::unbounded_channel;

    use super::{CdcCommand, cdc_keepalive_run_coalesce};
    use crate::pg::Lsn;

    fn mark(lsn: u64) -> CdcCommand {
        CdcCommand::KeepAliveMark {
            lsn: Lsn::from_raw(lsn),
        }
    }

    #[test]
    fn test_keepalive_run_collapses_to_max_and_returns_the_command_that_ended_it() {
        let (tx, mut rx) = unbounded_channel();
        tx.send(mark(7)).expect("queue mark");
        tx.send(mark(5)).expect("queue mark");
        tx.send(CdcCommand::Begin { xid: 42 }).expect("queue begin");
        tx.send(mark(9)).expect("queue mark after begin");

        let (first, trailing) = cdc_keepalive_run_coalesce(mark(3), &mut rx);
        assert!(matches!(first, CdcCommand::KeepAliveMark { lsn } if lsn == Lsn::from_raw(7)));
        assert!(matches!(trailing, Some(CdcCommand::Begin { xid: 42 })));
        // The mark after the run is left for the next iteration, in order.
        assert!(
            matches!(rx.try_recv(), Ok(CdcCommand::KeepAliveMark { lsn }) if lsn == Lsn::from_raw(9))
        );
    }

    #[test]
    fn test_keepalive_run_drains_to_empty_without_trailing_command() {
        let (tx, mut rx) = unbounded_channel();
        tx.send(mark(2)).expect("queue mark");
        let (first, trailing) = cdc_keepalive_run_coalesce(mark(1), &mut rx);
        assert!(matches!(first, CdcCommand::KeepAliveMark { lsn } if lsn == Lsn::from_raw(2)));
        assert!(trailing.is_none());
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn test_non_keepalive_first_command_is_passed_through_untouched() {
        let (tx, mut rx) = unbounded_channel();
        tx.send(mark(2)).expect("queue mark");
        let (first, trailing) = cdc_keepalive_run_coalesce(CdcCommand::Begin { xid: 1 }, &mut rx);
        assert!(matches!(first, CdcCommand::Begin { xid: 1 }));
        assert!(trailing.is_none());
        assert!(
            rx.try_recv().is_ok(),
            "queued mark is left for the next iteration"
        );
    }
}
