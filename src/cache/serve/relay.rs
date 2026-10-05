//! Relaying one serve's cache-DB response to the primary client and coalesced
//! waiters, and settling the pooled connection afterwards.

use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio_util::bytes::{Buf, Bytes, BytesMut};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, trace, warn};

use super::coalesce::{
    BroadcastState, CoalescedOutcome, broadcast_error_reply, broadcast_join, push_and_broadcast,
};
#[cfg(feature = "fault-injection")]
use super::fault;
use super::response_state::ServeResponseState;
use super::sqlstate::sqlstate_extract;
use crate::cache::memo::MemoCapture;
use crate::cache::messages::PipelineDescribe;
use crate::cache::runtime::serve_pool::ConnectionGuard;
use crate::cache::types::CacheStateView;
use crate::cache::write_queue::WriteQueue;
use crate::cache::{CacheError, CacheResult};
use crate::pg::cache_connection::{CacheConnection, ParkedConnection, PrepareOutcome};
use crate::pg::protocol::PgMessage;
use crate::pg::protocol::backend::{PgBackendMessageCodec, PgBackendMessageType};
use crate::pg::protocol::encode::SERVE_ERROR_MSG;
use crate::proxy::ClientSocket;
use crate::query::Fingerprint;
use crate::timing::QueryTiming;

/// Handle an `ErrorResponse` from the cache DB on the hit path. Poisons the
/// connection (the trailing ReadyForQuery would otherwise leak to the next
/// user) and returns a typed error so `handle_serve_request` forwards to
/// origin via `CacheReply::Error`.
///
/// Safe to call only when `bytes_served == 0` — the cache emits ErrorResponse
/// before RowDescription/DataRow, so the serve pool has not streamed any cache
/// payload toward the client yet. A mid-stream error would need a different
/// recovery path.
async fn cache_error_response_handle(
    guard: &mut ConnectionGuard,
    frame_data: &[u8],
    bytes_served: usize,
    broadcast: &mut Option<BroadcastState>,
) -> rootcause::Report<CacheError> {
    guard.poisoned = true;
    let sqlstate = sqlstate_extract(frame_data);
    let sqlstate_str = sqlstate
        .as_ref()
        .and_then(|s| std::str::from_utf8(s).ok())
        .unwrap_or("?");
    debug!(
        "cache ErrorResponse sqlstate={sqlstate_str} bytes_served={bytes_served} — \
         forwarding to origin"
    );
    if let Some(bc) = broadcast.take() {
        broadcast_error_reply(bc).await;
    }
    CacheError::CacheServerError { sqlstate }.into()
}

/// The primary client's side of one serve: its socket and buffered reply, plus
/// the PGC-291 flags that decide whether a failure can still forward to origin.
///
/// `committed` defers the first client flush until the cache-DB confirms the
/// query plan (relay reaches the data phase), so a stall/error *before* that
/// leaves nothing on the wire and forwards cleanly. `bytes_sent` records whether
/// any byte actually reached the client; if so, a later failure terminates the
/// entry with a synthetic ErrorResponse instead of forwarding (origin would
/// replay the already-sent prefix — e.g. a duplicate BindComplete — desyncing
/// the client).
pub(super) struct ServeClient<'a> {
    pub(super) socket: &'a mut ClientSocket,
    pub(super) write_queue: WriteQueue,
    /// The ReadyForQuery the client expects after this entry, if any.
    pub(super) trailing_rfq: Option<&'static [u8]>,
    /// The client is inside a transaction block.
    pub(super) in_block: bool,
    pub(super) committed: bool,
    pub(super) bytes_sent: bool,
    /// Set when a client write fails mid-serve. The cache-DB connection is still
    /// healthy, so rather than poison it we stop relaying and drain the remaining
    /// cache-DB response, returning the connection to the pool protocol-clean.
    pub(super) gone: bool,
}

impl ServeClient<'_> {
    /// PGC-291: resolve a serve failure under the A+C invariant. If any byte
    /// already reached the client, the serve can no longer fall back to origin.
    /// Terminate this entry on the client with a synthetic `ErrorResponse` (plus
    /// `ReadyForQuery` when the client sent a Sync) and return `Ok`. Otherwise
    /// nothing is on the wire, so return `forward_error` and the caller forwards
    /// to origin transparently.
    ///
    /// Termination reuses the existing queue with static bytes — no allocation.
    /// Best-effort: if the client is already gone the write just fails and the
    /// connection is torn down.
    pub(super) async fn failure_resolve(
        &mut self,
        bytes_served: usize,
        forward_error: rootcause::Report<CacheError>,
    ) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
        if !self.bytes_sent {
            return Err(forward_error);
        }
        self.write_queue.clear();
        self.write_queue.push(Bytes::from_static(SERVE_ERROR_MSG));
        // Inside a block no ReadyForQuery is right: origin's block is healthy, so
        // `T` would let the client commit what it just saw fail, and `E` would
        // misreport origin. Send the error alone and have the connection close
        // (PGC-387); origin then rolls the block back.
        if self.in_block {
            let _ = self.socket.write_all_buf(&mut self.write_queue).await;
            return Err(CacheError::ServeAbandonedInBlock.into());
        }
        if let Some(rfq) = self.trailing_rfq {
            self.write_queue.push(Bytes::from_static(rfq));
        }
        let _ = self.socket.write_all_buf(&mut self.write_queue).await;
        Ok((bytes_served, Vec::new()))
    }

    /// Flush whatever is still buffered. The primary socket is dead when `gone`;
    /// its buffered reply is moot (coalesced waiters were served by broadcast).
    pub(super) async fn flush_final(&mut self) -> CacheResult<()> {
        if self.gone || self.write_queue.is_empty() {
            return Ok(());
        }
        trace!(
            "net: cache→client final flush (serve, {} bytes remaining)",
            self.write_queue.remaining()
        );
        self.socket
            .write_all_buf(&mut self.write_queue)
            .await
            .map_err(|e| {
                error!("no client: {e}");
                CacheError::Write.into()
            })
    }
}

/// Request shape logged when a serve stalls or desyncs.
pub(super) struct ServeDiagnostics {
    pub(super) fingerprint: Fingerprint,
    pub(super) has_parse: bool,
    pub(super) has_bind: bool,
    pub(super) mv: bool,
}

/// Mutable relay state plus read-only config for one serve's response stream,
/// and the guard over the pooled connection it reads from. The client side
/// lives in [`ServeClient`], so the `select!` loop's read and write arms borrow
/// the two independently.
pub(super) struct Relay {
    pub(super) state: ServeResponseState,
    pub(super) guard: ConnectionGuard,
    /// Broadcast handle for coalesced clients (`None` when not coalesced).
    pub(super) broadcast: Option<BroadcastState>,
    /// In-flight memo capture (`None` when this serve isn't being memoized).
    pub(super) memo_capture: Option<MemoCapture>,
    /// Pending Describe('S') ParameterDescription, relayed before RowDescription.
    pub(super) parameter_description: Option<Bytes>,
    pub(super) bytes_served: usize,
    // Read-only for the relay's duration:
    pub(super) prepare: PrepareOutcome,
    pub(super) include_describe: bool,
    pub(super) pipeline_describe: PipelineDescribe,
    pub(super) diagnostics: ServeDiagnostics,
}

impl Relay {
    /// Advance the response state machine for one cache-DB frame: update
    /// `state`, relay the frame to the primary client (and broadcast to
    /// coalesced clients), and feed the memo capture. Returns `Err` on a
    /// cache-DB `ErrorResponse` or a desync — the connection is poisoned and
    /// coalesced waiters get an error reply. The caller breaks the loop once
    /// `state` reaches `Done`.
    pub(super) async fn frame_apply(
        &mut self,
        frame: PgMessage<PgBackendMessageType>,
        write_queue: &mut WriteQueue,
        timing: &mut QueryTiming,
    ) -> CacheResult<()> {
        // Fault (tests): corrupt the frame type so it lands in the desync arm.
        #[cfg(feature = "fault-injection")]
        let frame = {
            let mut frame = frame;
            if fault::desync_serve() {
                frame.message_type = PgBackendMessageType::CopyData;
            }
            frame
        };
        let message_type = frame.message_type;
        if let Some(next) = self
            .state
            .advance(message_type, &self.prepare, self.include_describe)
        {
            self.state = next;
            return Ok(());
        }
        let Some(frame) = self.frame_relay(frame, write_queue, timing) else {
            return Ok(());
        };
        if message_type == PgBackendMessageType::ErrorResponse {
            return Err(cache_error_response_handle(
                &mut self.guard,
                &frame.data,
                self.bytes_served,
                &mut self.broadcast,
            )
            .await);
        }
        // Async session messages can arrive at any point in the exchange and
        // carry no sequencing meaning — pass over them.
        if matches!(
            message_type,
            PgBackendMessageType::ParameterStatus
                | PgBackendMessageType::NoticeResponse
                | PgBackendMessageType::NotificationResponse
        ) {
            return Ok(());
        }
        // Any other frame is a protocol desync: poison immediately rather than
        // letting the stall deadline catch it 10s later (PGC-278). NoData is
        // intentionally NOT special-cased: a row-returning portal Describe
        // always yields RowDescription (zero-field for a column-less SELECT),
        // so NoData here would itself be a desync.
        warn!(
            "unexpected cache backend frame {message_type:?} in serve state {:?}; poisoning connection",
            self.state
        );
        crate::metrics::handles()
            .cache
            .serve_desync_total
            .increment(1);
        self.poison().await;
        Err(CacheError::InvalidMessage.into())
    }

    /// Relay a response frame the client receives — RowDescription, DataRow,
    /// CommandComplete — feeding the memo capture. Hands the frame back when it
    /// is not one this state relays.
    fn frame_relay(
        &mut self,
        frame: PgMessage<PgBackendMessageType>,
        write_queue: &mut WriteQueue,
        timing: &mut QueryTiming,
    ) -> Option<PgMessage<PgBackendMessageType>> {
        match (self.state, frame.message_type) {
            (ServeResponseState::DescribeRow, PgBackendMessageType::RowDescription) => {
                self.row_description_relay(frame.data, write_queue);
            }
            (ServeResponseState::DataRows, PgBackendMessageType::DataRows) => {
                trace!(
                    "net: cache→client DataRow (serve, {} bytes)",
                    frame.data.len()
                );
                self.bytes_served += frame.data.len();
                if let Some(cap) = &mut self.memo_capture {
                    cap.data_push(&frame.data);
                }
                push_and_broadcast(write_queue, &self.broadcast, frame.data);
            }
            (ServeResponseState::DataRows, PgBackendMessageType::CommandComplete) => {
                trace!(
                    "net: cache→client CommandComplete (serve, {} bytes)",
                    frame.data.len()
                );
                if let Some(cap) = &mut self.memo_capture {
                    cap.command_complete_push(&frame.data);
                }
                push_and_broadcast(write_queue, &self.broadcast, frame.data);
                timing.query_done_at = Some(Instant::now());
            }
            (_, _) => return Some(frame),
        }
        None
    }

    /// Relay the RowDescription, preceded by the pending ParameterDescription
    /// when the pipeline described the statement.
    fn row_description_relay(&mut self, data: BytesMut, write_queue: &mut WriteQueue) {
        if self.pipeline_describe == PipelineDescribe::Statement
            && let Some(param_desc) = self.parameter_description.take()
        {
            trace!(
                "net: cache→client ParameterDescription (serve, {} bytes)",
                param_desc.len()
            );
            push_and_broadcast(write_queue, &self.broadcast, param_desc);
        }
        trace!(
            "net: cache→client RowDescription (serve, {} bytes)",
            data.len()
        );
        if let Some(cap) = &mut self.memo_capture {
            cap.row_description_push(&data);
        }
        push_and_broadcast(write_queue, &self.broadcast, data);
        self.state = ServeResponseState::DataRows;
    }

    /// Poison the cache-DB connection and fail any coalesced waiters. The caller
    /// then resolves the primary client's fate (forward vs. terminate) separately
    /// — see [`ServeClient::failure_resolve`].
    pub(super) async fn poison(&mut self) {
        self.guard.poisoned = true;
        if let Some(bc) = self.broadcast.take() {
            broadcast_error_reply(bc).await;
        }
    }

    /// Return the cache-DB connection to the pool, or discard it if the serve
    /// left bytes unread.
    ///
    /// PGC-278: the response is fully consumed at `Done` — the single trailing
    /// Sync means `ReadyForQuery` is the last frame, and no next response arrives
    /// until this connection serves again. Any bytes still buffered are therefore
    /// a response desync introduced by THIS serve (the state machine consumed the
    /// wrong number of frames for its pipeline shape). Discard the connection
    /// (poison ⇒ replenish) rather than hand the leftover frame to the next
    /// borrower, and record it at its source. The client's reply is unaffected —
    /// it already received a complete response.
    pub(super) fn connection_return_or_discard(
        &mut self,
        framed: FramedRead<TcpStream, PgBackendMessageCodec>,
        parked: ParkedConnection,
    ) {
        let residual = framed.read_buffer().len();
        if residual == 0 {
            self.guard.conn = Some(CacheConnection::from_framed(framed, parked));
            return;
        }

        let lead = framed.read_buffer().first().copied().map(char::from);
        let diagnostics = &self.diagnostics;
        warn!(
            fingerprint = %diagnostics.fingerprint,
            residual_bytes = residual,
            ?lead,
            has_parse = diagnostics.has_parse,
            has_bind = diagnostics.has_bind,
            include_describe = self.include_describe,
            mv = diagnostics.mv,
            sent_setgen_parse = self.prepare.sent_setgen_parse,
            sent_parse = self.prepare.sent_parse,
            sent_close = self.prepare.sent_close,
            "serve left unconsumed cache-DB bytes after ReadyForQuery (response desync); discarding connection (PGC-278)"
        );
        crate::metrics::handles()
            .cache
            .serve_dirty_return_total
            .increment(1);
        self.guard.poisoned = true;
        // Drop the dirty connection; the poisoned guard's `Drop` replenishes.
        drop(framed);
        drop(parked);
    }

    /// Finalize a completed serve: return the connection to the pool, append
    /// the trailing ReadyForQuery when the client expects one, join the
    /// coalesced broadcast, flush any buffered bytes to the primary, and store
    /// the memo capture. The connection must already be reattached to the guard.
    pub(super) async fn finish(
        self,
        client: &mut ServeClient<'_>,
        timing: &mut QueryTiming,
        state_view: &CacheStateView,
    ) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
        let Relay {
            guard,
            broadcast,
            memo_capture,
            bytes_served,
            ..
        } = self;
        let broadcast = connection_release(guard, broadcast).await?;
        let outcomes = broadcast_finish(broadcast, client).await;
        client.flush_final().await?;
        timing.response_written_at = Some(Instant::now());

        // Store the captured snapshot iff the serve completed cleanly and no CDC
        // change touched a read relation across the capture (re-checked in
        // `finish`). Error paths returned earlier, so reaching here means a
        // clean response.
        if let Some(cap) = memo_capture {
            cap.finish(&state_view.memo);
        }

        debug!("cache hit");
        Ok((bytes_served, outcomes))
    }
}

/// Return the connection to the pool, handing the broadcast back; on failure
/// coalesced waiters get an error reply.
async fn connection_release(
    guard: ConnectionGuard,
    broadcast: Option<BroadcastState>,
) -> CacheResult<Option<BroadcastState>> {
    if let Err(e) = guard.release().await {
        if let Some(bc) = broadcast {
            broadcast_error_reply(bc).await;
        }
        return Err(e);
    }
    Ok(broadcast)
}

/// Append the trailing ReadyForQuery when the client expects one and join the
/// coalesced broadcast. Simple-query clients always terminate with
/// ReadyForQuery; extended clients do when their trailing Execute carried the
/// Sync.
async fn broadcast_finish(
    broadcast: Option<BroadcastState>,
    client: &mut ServeClient<'_>,
) -> Vec<CoalescedOutcome> {
    if let Some(rfq) = client.trailing_rfq {
        trace!("net: cache→client ReadyForQuery");
        push_and_broadcast(&mut client.write_queue, &broadcast, Bytes::from_static(rfq));
    }
    match broadcast {
        Some(bc) => broadcast_join(bc).await,
        None => vec![],
    }
}
