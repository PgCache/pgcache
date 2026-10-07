#[cfg(feature = "fault-injection")]
use std::collections::VecDeque;
use std::future::Future;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use rootcause::Report;
use tokio::runtime::Builder;
use tokio::sync::mpsc::UnboundedSender;
#[cfg(feature = "fault-injection")]
use tokio::sync::mpsc::{UnboundedReceiver, unbounded_channel};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::cache::cdc::{CdcError, CdcProcessor, CdcProcessorHandles};
use crate::cache::messages::CdcCommand;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::pg::Lsn;
use crate::pg::cdc::{PgCdcResult, slot_confirmed_lsn};
use crate::result::error_chain_format;
use crate::settings::Settings;

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
    warn!(lag_ms, "fault: CDC apply-lag relay active");
    let (relay_tx, relay_rx) = unbounded_channel::<CdcCommand>();
    tokio::spawn(fault_lag_relay_run(
        relay_rx,
        cdc_tx,
        Duration::from_millis(lag_ms),
    ));
    relay_tx
}
#[cfg(not(feature = "fault-injection"))]
fn fault_cdc_apply_lag_relay(cdc_tx: UnboundedSender<CdcCommand>) -> UnboundedSender<CdcCommand> {
    cdc_tx
}

/// Hold each relayed command for `lag`, then forward it in order. When the
/// decoder side closes (teardown/restart), flush what's held — the lag is moot
/// once the stream has ended.
#[cfg(feature = "fault-injection")]
async fn fault_lag_relay_run(
    mut relay_rx: UnboundedReceiver<CdcCommand>,
    cdc_tx: UnboundedSender<CdcCommand>,
    lag: Duration,
) {
    let mut held: VecDeque<(tokio::time::Instant, CdcCommand)> = VecDeque::new();
    loop {
        tokio::select! {
            cmd = relay_rx.recv() => match cmd {
                Some(cmd) => held.push_back((tokio::time::Instant::now() + lag, cmd)),
                None => break,
            },
            () = fault_lag_head_due(&held) => {
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
}

/// Resolves when the oldest held command is due; never while nothing is held.
#[cfg(feature = "fault-injection")]
async fn fault_lag_head_due(held: &VecDeque<(tokio::time::Instant, CdcCommand)>) {
    match held.front() {
        Some((due, _)) => tokio::time::sleep_until(*due).await,
        None => std::future::pending().await,
    }
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
pub(super) fn cdc_run(
    settings: &Settings,
    mut handles: CdcProcessorHandles,
    cancel: CancellationToken,
    cdc_connected: Arc<AtomicBool>,
) -> CacheResult<()> {
    let rt = Builder::new_current_thread()
        .enable_all()
        .build()
        .map_into_report::<CacheError>()?;

    debug!("cdc loop");
    rt.block_on(async {
        // Wrapped once for the whole stream/reconnect loop so a
        // fault-injected apply lag covers reconnected processors too.
        handles.cdc_tx = fault_cdc_apply_lag_relay(handles.cdc_tx);
        let mut cdc = CdcProcessor::new(settings, handles.clone())
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
                Ok(()) => warn!("CDC stream ended unexpectedly (last_flushed_lsn: {saved_lsn})"),
                Err(e @ CdcError::PgError(_)) => warn!(
                    "CDC stream error (last_flushed_lsn: {saved_lsn}): {}",
                    error_chain_format(e),
                ),
                // Resuming would skip or re-fail the undecodable change: restart
                // the cache instead.
                Err(e @ CdcError::BinaryTupleData) => {
                    return Err(Report::from(CacheError::CdcFailure))
                        .attach_loc(format!("CDC stream undecodable: {e}"));
                }
            }

            // Forward all queries to origin while disconnected.
            cdc_connected.store(false, Ordering::Relaxed);
            let Some(reconnected) = cdc_reconnect(settings, &handles, &cancel, saved_lsn).await?
            else {
                return Ok(());
            };
            cdc = reconnected;
            debug!("CDC reconnected");
            cdc_connected.store(true, Ordering::Relaxed);
        }
    })
}

/// `fut`'s output, or `None` (logging `cancelled_msg`) if `cancel` fires first.
async fn cancellable<F: Future>(
    cancel: &CancellationToken,
    fut: F,
    cancelled_msg: &str,
) -> Option<F::Output> {
    tokio::select! {
        () = cancel.cancelled() => {
            debug!("{cancelled_msg}");
            None
        }
        output = fut => Some(output),
    }
}

/// What the slot's confirmed position says about resuming from `saved_lsn`.
enum SlotPosition {
    /// The slot retains WAL from our position or earlier: resume.
    Safe,
    /// The check itself failed: back off and retry.
    Retry,
    /// The slot is gone, or was advanced past our position — events may have
    /// been skipped.
    Fatal,
}

/// confirmed <= saved is safe: the slot is retaining WAL from an equal or
/// earlier position, so we'll receive everything from saved_lsn forward.
/// confirmed > saved means the slot was externally advanced — events may have
/// been skipped.
fn slot_position_check(slot: PgCdcResult<Option<Lsn>>, saved_lsn: Lsn) -> SlotPosition {
    match slot {
        Ok(Some(confirmed_lsn)) if confirmed_lsn > saved_lsn => {
            error!("slot advanced past our position: saved={saved_lsn}, confirmed={confirmed_lsn}");
            SlotPosition::Fatal
        }
        Ok(Some(confirmed_lsn)) => {
            debug!("slot LSN verified: confirmed={confirmed_lsn}, saved={saved_lsn}");
            SlotPosition::Safe
        }
        Ok(None) => {
            error!("replication slot no longer exists");
            SlotPosition::Fatal
        }
        Err(e) => {
            error!(
                "slot LSN check failed: {}",
                error_chain_format(e.current_context()),
            );
            SlotPosition::Retry
        }
    }
}

/// Reconnect with exponential backoff once the slot is verified to still hold
/// our position. `None` when cancelled; a moved or missing slot cancels the
/// subsystem and fails.
async fn cdc_reconnect(
    settings: &Settings,
    handles: &CdcProcessorHandles,
    cancel: &CancellationToken,
    saved_lsn: Lsn,
) -> CacheResult<Option<CdcProcessor>> {
    let mut backoff = CDC_INITIAL_BACKOFF;
    loop {
        let reconnect_cancelled = "CDC cancelled during reconnect";
        if cancellable(cancel, tokio::time::sleep(backoff), reconnect_cancelled)
            .await
            .is_none()
        {
            return Ok(None);
        }
        let Some(slot) = cancellable(
            cancel,
            slot_confirmed_lsn(settings),
            "CDC cancelled during slot LSN check",
        )
        .await
        else {
            return Ok(None);
        };
        match slot_position_check(slot, saved_lsn) {
            SlotPosition::Safe => {}
            SlotPosition::Retry => {
                backoff = (backoff * 2).min(CDC_MAX_BACKOFF);
                continue;
            }
            SlotPosition::Fatal => {
                cancel.cancel();
                return Err(CacheError::CdcFailure.into());
            }
        }
        // LSN matches — attempt to re-establish the replication connection
        let processor = CdcProcessor::new(settings, handles.clone());
        let Some(reconnect) = cancellable(cancel, processor, reconnect_cancelled).await else {
            return Ok(None);
        };
        match reconnect {
            Ok(cdc) => return Ok(Some(cdc)),
            Err(e) => {
                error!(
                    "CDC reconnect failed: {}",
                    error_chain_format(e.current_context()),
                );
                backoff = (backoff * 2).min(CDC_MAX_BACKOFF);
            }
        }
    }
}
