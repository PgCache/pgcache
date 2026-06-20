use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use tokio::runtime::Builder;
use tokio::sync::mpsc::UnboundedSender;
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, warn};

use crate::cache::cdc::CdcProcessor;
use crate::cache::messages::CdcCommand;
use crate::cache::types::ActiveRelations;
use crate::cache::{CacheError, CacheResult, MapIntoReport, ReportExt};
use crate::pg::cdc::slot_confirmed_lsn;
use crate::result::error_chain_format;
use crate::settings::Settings;

#[cfg(feature = "fault-injection")]
use std::collections::VecDeque;
#[cfg(feature = "fault-injection")]
use tokio::sync::mpsc::unbounded_channel;

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
pub(super) fn cdc_run(
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
                let slot_check = tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!("CDC cancelled during slot LSN check");
                        return Ok(());
                    }
                    r = slot_confirmed_lsn(settings) => r,
                };
                match slot_check {
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
                let reconnect = tokio::select! {
                    _ = cancel.cancelled() => {
                        debug!("CDC cancelled during reconnect");
                        return Ok(());
                    }
                    r = CdcProcessor::new(
                        settings,
                        cdc_tx.clone(),
                        Arc::clone(&active_relations),
                        Arc::clone(&watermark_nudge),
                    ) => r,
                };
                match reconnect {
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
