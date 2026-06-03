use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc::Sender;
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_util::bytes::{Buf, Bytes};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, instrument, trace};

use crate::cache::messages::{CacheOutcome, CacheReply, PipelineDescribe};
use crate::pg::cache_connection::{CacheConnection, PrepareOutcome};
use crate::pg::protocol::backend::PgBackendMessageType;
use crate::pg::protocol::encode::{
    BIND_COMPLETE_MSG, PARSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG,
};
use crate::query::ast::{Deparse, LiteralValue};

use super::{
    CacheError, CacheResult,
    mv::{MvServe, mv_serve_sql_into},
    query_cache::{CoalescedClient, QueryType, WorkerRequest},
    types::CacheStateView,
    write_queue::WriteQueue,
};

/// Outcome of a coalesced client's write task.
pub enum CoalescedOutcome {
    /// All bytes were delivered successfully.
    Complete(CoalescedClient),
    /// Write failed or broadcast lagged — byte stream is corrupted.
    Failed(CoalescedClient),
}

/// Broadcast state for coalesced request handling.
struct BroadcastState {
    tx: broadcast::Sender<Bytes>,
    tasks: Vec<JoinHandle<Result<CoalescedClient, CoalescedClient>>>,
}

/// SQLSTATE `42P01` — `undefined_table`. The expected outcome when the cache
/// table is dropped between dispatch and SELECT (eviction-window race).
pub(crate) const SQLSTATE_UNDEFINED_TABLE: [u8; 5] = *b"42P01";

/// Extract the 5-char SQLSTATE from a backend `ErrorResponse` frame.
///
/// Frame layout: `'E' (1 byte) | len (4 bytes BE) | field* | 0`, where each
/// field is `code (1 byte) | value (null-terminated string)`. Field code `'C'`
/// carries SQLSTATE — always exactly 5 ASCII bytes per the protocol.
/// Returns `None` when the frame is malformed or the field is missing.
fn sqlstate_extract(frame_data: &[u8]) -> Option<[u8; 5]> {
    let payload = frame_data.get(5..)?;
    let mut i = 0;
    while i < payload.len() {
        let code = *payload.get(i)?;
        if code == 0 {
            return None;
        }
        let value_start = i + 1;
        let rest = payload.get(value_start..)?;
        let value_len = rest.iter().position(|&b| b == 0)?;
        if code == b'C' && value_len == 5 {
            let value = rest.get(..5)?;
            let mut out = [0u8; 5];
            out.copy_from_slice(value);
            return Some(out);
        }
        i = value_start + value_len + 1;
    }
    None
}

/// Handle an `ErrorResponse` from the cache DB on the hit path. Poisons the
/// connection (the trailing ReadyForQuery would otherwise leak to the next
/// user) and returns a typed error so `handle_worker_request` forwards to
/// origin via `CacheReply::Error`.
///
/// Safe to call only when `bytes_served == 0` — the cache emits ErrorResponse
/// before RowDescription/DataRow, so the worker hasn't streamed any cache
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

/// Push bytes to the primary WriteQueue and broadcast to coalesced clients.
fn push_and_broadcast(
    write_queue: &mut WriteQueue,
    broadcast: &Option<BroadcastState>,
    data: impl Into<Bytes>,
) {
    if let Some(bc) = broadcast {
        let bytes: Bytes = data.into();
        let _ = bc.tx.send(bytes.clone());
        write_queue.push(bytes);
    } else {
        write_queue.push(data);
    }
}

/// Create broadcast channel and spawn per-client write tasks.
/// Returns None if there are no coalesced clients.
fn broadcast_setup(msg: &mut WorkerRequest) -> Option<BroadcastState> {
    if msg.coalesced.is_empty() {
        return None;
    }

    let (tx, _) = broadcast::channel::<Bytes>(64);

    let tasks = msg
        .coalesced
        .drain(..)
        .map(|mut client| {
            let mut rx = tx.subscribe();
            tokio::spawn(async move {
                loop {
                    match rx.recv().await {
                        Ok(chunk) => {
                            if client.client_socket.write_all(&chunk).await.is_err() {
                                return Err(client);
                            }
                        }
                        Err(RecvError::Closed) => return Ok(client),
                        Err(RecvError::Lagged(_)) => return Err(client),
                    }
                }
            })
        })
        .collect();

    Some(BroadcastState { tx, tasks })
}

/// Drop the broadcast sender, join all tasks, and collect outcomes.
async fn broadcast_join(bc: BroadcastState) -> Vec<CoalescedOutcome> {
    drop(bc.tx);

    let mut outcomes = Vec::with_capacity(bc.tasks.len());
    for task in bc.tasks {
        match task.await {
            Ok(Ok(client)) => outcomes.push(CoalescedOutcome::Complete(client)),
            Ok(Err(client)) => outcomes.push(CoalescedOutcome::Failed(client)),
            Err(_) => {} // JoinError — task panicked
        }
    }
    outcomes
}

/// Drop the broadcast sender, join all tasks, and send Error replies.
/// Used when the primary path fails after broadcast was created.
async fn broadcast_error_reply(bc: BroadcastState) {
    drop(bc.tx);

    for task in bc.tasks {
        let client = match task.await {
            Ok(Ok(c)) | Ok(Err(c)) => c,
            Err(_) => continue,
        };
        let _ = client.reply_tx.send(CacheReply {
            socket: client.client_socket,
            outcome: CacheOutcome::Error(client.data),
        });
    }
}

/// Guard that ensures a connection is returned to the pool.
///
/// Returns the connection via async `release()` on success.
/// On error (drop without release), the connection is discarded if poisoned
/// to avoid returning a connection with stale response data in its buffer.
struct ConnectionGuard {
    conn: Option<CacheConnection>,
    return_tx: Sender<CacheConnection>,
    poisoned: bool,
}

impl ConnectionGuard {
    fn new(conn: CacheConnection, return_tx: Sender<CacheConnection>) -> Self {
        Self {
            conn: Some(conn),
            return_tx,
            poisoned: false,
        }
    }

    /// Return the connection to the pool.
    async fn release(mut self) -> CacheResult<()> {
        if let Some(conn) = self.conn.take() {
            self.return_tx
                .send(conn)
                .await
                .map_err(|_| CacheError::NoConnection)?;
        }
        Ok(())
    }
}

impl Drop for ConnectionGuard {
    fn drop(&mut self) {
        if self.poisoned {
            // Discard connection — may have unread response data
            self.conn.take();
            return;
        }
        if let Some(conn) = self.conn.take() {
            // try_send won't block; channel always has capacity since
            // pool size equals channel size
            let _ = self.return_tx.try_send(conn);
        }
    }
}

/// Render a `LIMIT`/`OFFSET` clause field into text for its `$1`/`$2` bind. An
/// integer limit — virtually every real one — formats into the caller's stack
/// `itoa::Buffer` with no allocation. Any other literal (e.g. a float) deparses
/// into `other` (a heap `String`); it then fails the `int8` coercion on the
/// cache DB, erroring that hit so it forwards to origin rather than silently
/// dropping the limit and over-returning rows. `None` binds NULL (no limit /
/// offset 0). The two scratch buffers are caller-owned so the returned `&str`
/// outlives the bind.
fn limit_bind_text<'a>(
    value: Option<&LiteralValue>,
    itoa_buf: &'a mut itoa::Buffer,
    other: &'a mut String,
) -> Option<&'a str> {
    match value {
        None => None,
        Some(LiteralValue::Integer(n)) => Some(itoa_buf.format(*n)),
        Some(v) => {
            v.deparse(other);
            Some(other.as_str())
        }
    }
}

/// Response state machine for the unified serve path (text and binary clients;
/// source-row uses a named prepared statement, MV an unnamed one). Result
/// format (text/binary) is chosen per client; the message *sequence* is the
/// same.
///
/// Source-row pipeline (PGC-235): set_config(generation) + [Close] +
/// Parse/Bind/[Describe('P')]/Execute under one Sync, producing
/// [SetGen ParseComplete →] SetGen BindComplete → SetGen DataRow → SetGen
/// CommandComplete → [CloseComplete →] [SELECT ParseComplete →] BindComplete →
/// [RowDescription →] DataRow* → CommandComplete (SELECT) → ReadyForQuery. The
/// set_config response (a one-row SELECT) is consumed, not relayed; only the
/// SELECT's BindComplete-onward reaches the client. MV path has no set_config
/// prefix and starts at `ParseComplete`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ServeResponseState {
    /// Waiting for the set_config statement's ParseComplete (first serve only)
    SetGenParse,
    /// Waiting for the set_config statement's BindComplete
    SetGenBind,
    /// Consuming the set_config one-row result (DataRow then CommandComplete)
    SetGenData,
    /// Waiting for CloseComplete (only when a statement was evicted)
    CloseComplete,
    /// Waiting for ParseComplete
    ParseComplete,
    /// Waiting for BindComplete
    BindComplete,
    /// Waiting for RowDescription (only when include_describe is true)
    DescribeRow,
    /// Streaming DataRow messages
    DataRows,
    /// Done — final ReadyForQuery received
    Done,
}

/// Serve a cache hit: execute the cached query on a pooled cache-DB connection
/// (a named prepared statement for source-row, unnamed extended for MV) and
/// relay the response to the client in its requested format. Returns the DataRow
/// bytes served and any coalesced client outcomes.
#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub async fn handle_cached_query(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
    state_view: &CacheStateView,
) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
    debug!("message query generation {}", msg.generation);
    let mut guard = ConnectionGuard::new(conn, return_tx);
    let mut conn = guard.conn.take().ok_or(CacheError::NoConnection)?;

    // Serve in the client's result format (text/binary). Simple-query clients
    // always expect a RowDescription, so request Describe from the cache DB even
    // when no Describe message was pipelined.
    let binary_results = msg.result_formats.first().is_some_and(|&f| f != 0);
    let query_type = msg.query_type;
    let include_describe =
        query_type == QueryType::Simple || msg.pipeline_describe != PipelineDescribe::None;

    // What was sent on the cache-DB extended query this hit, so the response
    // state machine knows which completions to expect. The MV path always sends
    // an unnamed Parse (no Close); the source-row path sends a Parse only on the
    // first use of a fingerprint on this connection, plus a Close when that
    // prepare evicted the connection's oldest statement.
    let prepare: PrepareOutcome;
    if let MvServe::Mv(cols) = &msg.mv {
        // MV fast path: extended query only, no SET prefix. Render into the
        // connection's recycled SQL buffer rather than a fresh String.
        mv_serve_sql_into(
            &mut conn.sql_buf,
            msg.fingerprint,
            &msg.resolved,
            msg.limit.as_ref(),
            cols,
        );
        conn.extended_query_unnamed_send(include_describe, binary_results)
            .await
            .inspect_err(|_| {
                guard.poisoned = true;
            })?;
        prepare = PrepareOutcome {
            sent_setgen_parse: false,
            sent_parse: true,
            sent_close: false,
        };
    } else {
        // One step of round-robin reconciliation: close a prepared statement
        // whose query has been evicted from the cache (kept across invalidation,
        // which leaves the entry in place). Self-tunes to the live working set —
        // no cap. The Close is pipelined ahead of this serve's Parse/Bind.
        let close_victim = conn
            .prepared
            .reconcile_one(|fp| state_view.cached_queries.contains_key(&fp));

        // Named prepared statement with parameterized LIMIT/OFFSET — body is
        // stable per fingerprint, so PG parses/plans once per connection and
        // reuses across hits and across limit values.
        conn.sql_buf.clear();
        conn.sql_buf.push_str(&msg.deparsed_sql);
        conn.sql_buf.push_str(" LIMIT $1 OFFSET $2");
        let mut limit_itoa = itoa::Buffer::new();
        let mut offset_itoa = itoa::Buffer::new();
        let mut limit_other = String::new();
        let mut offset_other = String::new();
        let limit_text = limit_bind_text(
            msg.limit.as_ref().and_then(|l| l.count.as_ref()),
            &mut limit_itoa,
            &mut limit_other,
        );
        let offset_text = limit_bind_text(
            msg.limit.as_ref().and_then(|l| l.offset.as_ref()),
            &mut offset_itoa,
            &mut offset_other,
        );
        prepare = conn
            .pipelined_named_query_send(
                msg.fingerprint,
                msg.generation,
                limit_text,
                offset_text,
                include_describe,
                binary_results,
                close_victim,
            )
            .await
            .inspect_err(|_| {
                guard.poisoned = true;
            })?;
    }

    // Create broadcast for coalesced clients (after query is sent, before streaming)
    let mut broadcast = broadcast_setup(msg);

    // Stream results to client
    let CacheConnection {
        stream,
        read_buf,
        codec,
        sql_buf,
        write_buf,
        prepared,
        setgen_parsed,
    } = conn;
    // `with_capacity(.., 0)` instead of `new` so FramedRead doesn't allocate its
    // default 8 KiB read buffer — we immediately swap in the connection's
    // recycled `read_buf`, which would otherwise drop that fresh allocation every
    // hit.
    let mut framed = FramedRead::with_capacity(stream, codec, 0);
    *framed.read_buffer_mut() = read_buf;

    let emit_rfq = msg.emit_rfq;
    let has_parse = msg.has_parse;
    let has_bind = msg.has_bind;
    let pipeline_describe = msg.pipeline_describe;
    let mut parameter_description = msg.parameter_description.take();
    let client_socket = &mut msg.client_socket;

    let mut write_queue = WriteQueue::new();

    if has_parse {
        push_and_broadcast(
            &mut write_queue,
            &broadcast,
            Bytes::from_static(PARSE_COMPLETE_MSG),
        );
    }
    if has_bind {
        push_and_broadcast(
            &mut write_queue,
            &broadcast,
            Bytes::from_static(BIND_COMPLETE_MSG),
        );
    }

    // MV path: no set_config prefix, so start at the SELECT's ParseComplete.
    // Source-row path: consume the set_config response first — its Parse only on
    // the first serve of this connection, otherwise straight to its BindComplete.
    let mut state = if matches!(msg.mv, MvServe::Mv(_)) {
        ServeResponseState::ParseComplete
    } else if prepare.sent_setgen_parse {
        ServeResponseState::SetGenParse
    } else {
        ServeResponseState::SetGenBind
    };
    let mut bytes_served: usize = 0;
    // Set when a client write fails mid-serve. The cache-DB connection is still
    // healthy, so rather than poison it we stop relaying and drain the remaining
    // cache-DB response, returning the connection to the pool protocol-clean.
    let mut client_gone = false;

    loop {
        tokio::select! {
            frame = framed.next() => {
                let frame = match frame {
                    Some(Ok(frame)) => frame,
                    Some(Err(_)) | None => {
                        guard.poisoned = true;
                        if let Some(bc) = broadcast.take() {
                            broadcast_error_reply(bc).await;
                        }
                        return Err(CacheError::InvalidMessage.into());
                    }
                };

                match (state, frame.message_type) {
                    (ServeResponseState::SetGenParse, PgBackendMessageType::ParseComplete) => {
                        state = ServeResponseState::SetGenBind;
                    }
                    (ServeResponseState::SetGenBind, PgBackendMessageType::BindComplete) => {
                        state = ServeResponseState::SetGenData;
                    }
                    // set_config returns one row; consume it without relaying.
                    (ServeResponseState::SetGenData, PgBackendMessageType::DataRows) => {}
                    (ServeResponseState::SetGenData, PgBackendMessageType::CommandComplete) => {
                        // set_config done. A Close (if a statement was evicted)
                        // precedes the SELECT; on statement reuse neither Close nor
                        // Parse is sent, so skip straight to Bind.
                        state = if prepare.sent_close {
                            ServeResponseState::CloseComplete
                        } else if prepare.sent_parse {
                            ServeResponseState::ParseComplete
                        } else {
                            ServeResponseState::BindComplete
                        };
                    }
                    (ServeResponseState::CloseComplete, PgBackendMessageType::CloseComplete) => {
                        // A reconciliation Close can ride a reuse serve (no Parse),
                        // so the Parse only follows when one was actually sent.
                        state = if prepare.sent_parse {
                            ServeResponseState::ParseComplete
                        } else {
                            ServeResponseState::BindComplete
                        };
                    }
                    (ServeResponseState::ParseComplete, PgBackendMessageType::ParseComplete) => {
                        state = ServeResponseState::BindComplete;
                    }
                    (ServeResponseState::BindComplete, PgBackendMessageType::BindComplete) => {
                        state = if include_describe {
                            ServeResponseState::DescribeRow
                        } else {
                            ServeResponseState::DataRows
                        };
                    }
                    (ServeResponseState::DescribeRow, PgBackendMessageType::RowDescription) => {
                        if pipeline_describe == PipelineDescribe::Statement
                            && let Some(param_desc) = parameter_description.take()
                        {
                            trace!("net: cache→client ParameterDescription (serve, {} bytes)", param_desc.len());
                            push_and_broadcast(&mut write_queue, &broadcast, param_desc);
                        }
                        trace!("net: cache→client RowDescription (serve, {} bytes)", frame.data.len());
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        state = ServeResponseState::DataRows;
                    }
                    (ServeResponseState::DataRows, PgBackendMessageType::DataRows) => {
                        trace!("net: cache→client DataRow (serve, {} bytes)", frame.data.len());
                        bytes_served += frame.data.len();
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                    }
                    (ServeResponseState::DataRows, PgBackendMessageType::CommandComplete) => {
                        trace!("net: cache→client CommandComplete (serve, {} bytes)", frame.data.len());
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        msg.timing.query_done_at = Some(Instant::now());
                    }
                    // Single trailing Sync → one terminal ReadyForQuery. It can't
                    // arrive mid-set_config (those advance on Parse/Bind/Data/CC).
                    (_, PgBackendMessageType::ReadyForQuery)
                        if !matches!(
                            state,
                            ServeResponseState::SetGenParse
                                | ServeResponseState::SetGenBind
                                | ServeResponseState::SetGenData
                        ) =>
                    {
                        state = ServeResponseState::Done;
                    }
                    (_, PgBackendMessageType::ErrorResponse) => {
                        return Err(cache_error_response_handle(
                            &mut guard,
                            &frame.data,
                            bytes_served,
                            &mut broadcast,
                        )
                        .await);
                    }
                    _ => {}
                }
            }
            result = client_socket.write_buf(&mut write_queue),
                if !write_queue.is_empty() && !client_gone =>
            {
                match result {
                    Ok(cnt) => {
                        trace!("net: cache→client flush (serve, partial write, {} bytes)", cnt);
                    }
                    Err(_) => {
                        // Client went away mid-serve. The cache-DB connection is
                        // healthy — do NOT poison it. Stop relaying, fail any
                        // coalesced waiters, and keep reading the cache-DB
                        // response to completion so the connection returns to the
                        // pool protocol-clean (avoids serve-pool exhaustion).
                        debug!("client write failed mid-serve; draining cache-DB response to preserve pooled connection");
                        client_gone = true;
                        if let Some(bc) = broadcast.take() {
                            broadcast_error_reply(bc).await;
                        }
                    }
                }
            }
        }

        // While draining for a departed client, discard the relay buffer so it
        // can't grow with the rest of the response.
        if client_gone {
            write_queue.clear();
        }

        if state == ServeResponseState::Done {
            break;
        }
    }

    // Cache DB response fully consumed — return connection to pool immediately
    let parts = framed.into_parts();
    guard.conn = Some(CacheConnection {
        stream: parts.io,
        read_buf: parts.read_buf,
        codec: parts.codec,
        sql_buf,
        write_buf,
        prepared,
        setgen_parsed,
    });
    if let Err(e) = guard.release().await {
        if let Some(bc) = broadcast.take() {
            broadcast_error_reply(bc).await;
        }
        return Err(e);
    }

    // Client departed mid-serve: the connection has been drained and returned to
    // the pool. Surface the write error without further client I/O.
    if client_gone {
        return Err(CacheError::Write.into());
    }

    // Simple-query clients always terminate with ReadyForQuery; extended clients
    // do when their trailing Execute carried the Sync.
    if query_type == QueryType::Simple || emit_rfq {
        trace!("net: cache→client ReadyForQuery");
        push_and_broadcast(
            &mut write_queue,
            &broadcast,
            Bytes::from_static(READY_FOR_QUERY_IDLE_MSG),
        );
    }

    let outcomes = match broadcast.take() {
        Some(bc) => broadcast_join(bc).await,
        None => vec![],
    };

    if !write_queue.is_empty() {
        trace!(
            "net: cache→client final flush (serve, {} bytes remaining)",
            write_queue.remaining()
        );
        if let Err(e) = client_socket.write_all_buf(&mut write_queue).await {
            error!("no client: {e}");
            return Err(CacheError::Write.into());
        }
    }

    msg.timing.response_written_at = Some(Instant::now());

    debug!("cache hit");
    Ok((bytes_served, outcomes))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Build a minimal PG ErrorResponse frame with the given (code, value)
    /// fields. Layout: `'E' | len(u32 BE) | (code: u8, value: cstring)* | 0`.
    fn error_response_frame(fields: &[(u8, &[u8])]) -> Vec<u8> {
        let mut payload = Vec::new();
        for (code, value) in fields {
            payload.push(*code);
            payload.extend_from_slice(value);
            payload.push(0);
        }
        payload.push(0); // terminator

        let mut frame = Vec::with_capacity(5 + payload.len());
        frame.push(b'E');
        let len = u32::try_from(4 + payload.len()).expect("test frame fits in u32");
        frame.extend_from_slice(&len.to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    #[test]
    fn limit_bind_text_binds_value_and_never_drops_non_integers() {
        let mut itoa_buf = itoa::Buffer::new();
        let mut other = String::new();

        // No clause field → NULL bind (no limit, offset 0).
        assert_eq!(limit_bind_text(None, &mut itoa_buf, &mut other), None);

        // Integer binds its decimal value (negatives included) with no alloc.
        assert_eq!(
            limit_bind_text(
                Some(&LiteralValue::Integer(5)),
                &mut itoa_buf,
                &mut other
            ),
            Some("5")
        );
        assert!(other.is_empty(), "integer path must not touch the heap buffer");
        assert_eq!(
            limit_bind_text(
                Some(&LiteralValue::Integer(-2)),
                &mut itoa_buf,
                &mut other
            ),
            Some("-2")
        );

        // PGC-229 review #1: a non-integer limit must still BIND (not None, which
        // would bind NULL and silently drop the limit, returning every row). It
        // binds text that fails int8 coercion on the cache DB, erroring that hit
        // so it forwards to origin.
        let float = LiteralValue::Float(
            ordered_float::NotNan::new(3.7).expect("non-NaN test value"),
        );
        let bound = limit_bind_text(Some(&float), &mut itoa_buf, &mut other);
        assert!(bound.is_some(), "non-integer limit must bind, not drop");
    }

    #[test]
    fn sqlstate_extract_undefined_table() {
        let frame = error_response_frame(&[
            (b'S', b"ERROR"),
            (b'C', b"42P01"),
            (b'M', b"relation \"public.evict_a\" does not exist"),
        ]);
        assert_eq!(sqlstate_extract(&frame), Some(*b"42P01"));
    }

    #[test]
    fn sqlstate_extract_first_field() {
        // SQLSTATE-first ordering should still parse.
        let frame = error_response_frame(&[(b'C', b"23505"), (b'S', b"ERROR")]);
        assert_eq!(sqlstate_extract(&frame), Some(*b"23505"));
    }

    #[test]
    fn sqlstate_extract_missing_returns_none() {
        let frame = error_response_frame(&[(b'S', b"ERROR"), (b'M', b"boom")]);
        assert_eq!(sqlstate_extract(&frame), None);
    }

    #[test]
    fn sqlstate_extract_wrong_length_returns_none() {
        // SQLSTATE must be exactly 5 chars; anything else is malformed.
        let frame = error_response_frame(&[(b'C', b"42P0")]);
        assert_eq!(sqlstate_extract(&frame), None);
    }

    #[test]
    fn sqlstate_extract_short_frame_returns_none() {
        // Frame shorter than the 5-byte header (tag + length) — graceful None.
        assert_eq!(sqlstate_extract(b"E\x00"), None);
    }
}
