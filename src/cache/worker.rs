use std::fmt::Write as _;
use std::time::Instant;

use tokio::io::AsyncWriteExt;
use tokio::sync::broadcast::{self, error::RecvError};
use tokio::sync::mpsc::Sender;
use tokio::task::{JoinHandle, spawn_local};
use tokio_stream::StreamExt;
use tokio_util::bytes::{Buf, Bytes};
use tokio_util::codec::FramedRead;
use tracing::{debug, error, instrument, trace};

use crate::cache::messages::{CacheReply, PipelineDescribe};
use crate::pg::cache_connection::CacheConnection;
use crate::pg::protocol::backend::PgBackendMessageType;
use crate::pg::protocol::encode::{
    BIND_COMPLETE_MSG, PARSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG,
};
use crate::query::ast::Deparse;

use super::{
    CacheError, CacheResult,
    mv::{MvServe, mv_serve_sql},
    query_cache::{CoalescedClient, QueryType, WorkerRequest},
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
            spawn_local(async move {
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
        let _ = client.reply_tx.send(CacheReply::Error(client.data));
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

/// Returns the number of DataRow bytes served and coalesced client outcomes on success.
#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub async fn handle_cached_query(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
    debug!("message query generation {}", msg.generation);

    let (bytes_served, outcomes) = if msg.result_formats.first().is_none_or(|&f| f == 0) {
        handle_cached_query_text(conn, return_tx, msg).await?
    } else {
        handle_cached_query_binary(conn, return_tx, msg).await?
    };

    debug!("cache hit");
    Ok((bytes_served, outcomes))
}

/// Response state machine for the text (simple query) path.
///
/// A combined `SET gen; SELECT ...` produces:
/// CommandComplete (SET) → RowDescription → DataRow* → CommandComplete (SELECT) → ReadyForQuery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TextResponseState {
    /// Waiting for SET CommandComplete
    SetComplete,
    /// Waiting for RowDescription (or DataRows if no rows)
    RowDescription,
    /// Streaming DataRow messages
    DataRows,
    /// Done — ReadyForQuery received
    Done,
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_cached_query_text(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
    let mut guard = ConnectionGuard::new(conn, return_tx);
    let mut conn = guard.conn.take().ok_or(CacheError::NoConnection)?;

    // Generate SQL query from resolved AST (with schema-qualified table names)
    #[cfg(feature = "hotpath")]
    let _m = hotpath::functions::MeasurementGuardSync::new("hcqt:deparse", false, false);

    // Assemble the wire SQL into the connection's recycled buffer to avoid
    // per-request allocations. Format differs between MV and source-row paths.
    conn.sql_buf.clear();
    if let MvServe::Mv(cols) = &msg.mv {
        // MV fast path: SELECT <aliased cols> FROM pgcache_mv.q_<fp>
        // [ORDER BY] [LIMIT]. No generation SET — MV tables are not
        // pgcache_pgrx-tracked.
        conn.sql_buf.push_str(&mv_serve_sql(
            msg.fingerprint,
            &msg.resolved,
            msg.limit.as_ref(),
            cols,
        ));
        conn.sql_buf.push(';');
    } else {
        write!(
            conn.sql_buf,
            "SET mem.query_generation = {}; ",
            msg.generation
        )
        .expect("write to String");
        conn.sql_buf.push_str(&msg.deparsed_sql);
        if let Some(limit) = &msg.limit {
            limit.deparse(&mut conn.sql_buf);
        }
        conn.sql_buf.push(';');
    }

    #[cfg(feature = "hotpath")]
    drop(_m);

    // Send query to cache database
    #[cfg(feature = "hotpath")]
    let _m = hotpath::functions::MeasurementGuardSync::new("hcqt:send_query", false, false);

    conn.simple_query_send().await.inspect_err(|_| {
        guard.poisoned = true;
    })?;

    #[cfg(feature = "hotpath")]
    drop(_m);

    // Create broadcast for coalesced clients (after query is sent, before streaming)
    let mut broadcast = broadcast_setup(msg);

    // Stream results to client
    #[cfg(feature = "hotpath")]
    let _m = hotpath::functions::MeasurementGuardSync::new("hcqt:stream", false, false);

    let CacheConnection {
        stream,
        read_buf,
        codec,
        sql_buf,
    } = conn;
    let mut framed = FramedRead::new(stream, codec);
    *framed.read_buffer_mut() = read_buf;

    let query_type = msg.query_type;
    let has_sync = msg.has_sync;
    let has_parse = msg.has_parse;
    let has_bind = msg.has_bind;
    let pipeline_describe = msg.pipeline_describe;
    let mut parameter_description = msg.parameter_description.take();
    let client_socket = &mut msg.client_socket;

    let mut write_queue = WriteQueue::new();

    // Prepend ParseComplete / BindComplete for messages the proxy buffered
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

    // MV path: no SET statement was sent, so skip the SetComplete waiting state
    // and start directly at RowDescription.
    let mut state = if matches!(msg.mv, MvServe::Mv(_)) {
        TextResponseState::RowDescription
    } else {
        TextResponseState::SetComplete
    };
    let mut bytes_served: usize = 0;

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

                #[cfg(feature = "hotpath")]
                let _match = hotpath::functions::MeasurementGuardSync::new("hcqt:match", false, false);

                match (state, frame.message_type) {
                    (TextResponseState::SetComplete, PgBackendMessageType::CommandComplete) => {
                        state = TextResponseState::RowDescription;
                    }
                    (TextResponseState::RowDescription, PgBackendMessageType::RowDescription) => {
                        if query_type == QueryType::Simple {
                            trace!("net: cache→client RowDescription ({} bytes, simple)", frame.data.len());
                            push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        } else if pipeline_describe != PipelineDescribe::None {
                            if pipeline_describe == PipelineDescribe::Statement
                                && let Some(param_desc) = parameter_description.take()
                            {
                                trace!("net: cache→client ParameterDescription ({} bytes, pipeline)", param_desc.len());
                                push_and_broadcast(&mut write_queue, &broadcast, param_desc);
                            }
                            trace!("net: cache→client RowDescription ({} bytes, pipeline)", frame.data.len());
                            push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        } else {
                            trace!("net: cache skip RowDescription ({} bytes, extended — no Describe in pipeline)", frame.data.len());
                        }
                        state = TextResponseState::DataRows;
                    }
                    (TextResponseState::DataRows, PgBackendMessageType::DataRows) => {
                        trace!("net: cache→client DataRow ({} bytes)", frame.data.len());
                        bytes_served += frame.data.len();
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                    }
                    (TextResponseState::DataRows, PgBackendMessageType::CommandComplete) => {
                        trace!("net: cache→client CommandComplete ({} bytes)", frame.data.len());
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        msg.timing.query_done_at = Some(Instant::now());
                    }
                    (_, PgBackendMessageType::ReadyForQuery) => {
                        state = TextResponseState::Done;
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

                #[cfg(feature = "hotpath")]
                drop(_match);

            }
            result = client_socket.write_buf(&mut write_queue), if !write_queue.is_empty() =>
            {
                match result {
                    Ok(cnt) => {
                        trace!("net: cache→client flush (text, partial write, {} bytes)", cnt);
                    }
                    Err(_) => {
                        guard.poisoned = true;
                        error!("no client");
                        if let Some(bc) = broadcast.take() {
                            broadcast_error_reply(bc).await;
                        }
                        return Err(CacheError::Write.into());
                    }
                }
            }
        }

        if state == TextResponseState::Done {
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
    });
    if let Err(e) = guard.release().await {
        if let Some(bc) = broadcast.take() {
            broadcast_error_reply(bc).await;
        }
        return Err(e);
    }

    // Append ReadyForQuery for simple queries (always) and extended queries with Sync
    if query_type == QueryType::Simple || has_sync {
        trace!("net: cache→client ReadyForQuery");
        push_and_broadcast(
            &mut write_queue,
            &broadcast,
            Bytes::from_static(READY_FOR_QUERY_IDLE_MSG),
        );
    }

    // Tear down broadcast and collect coalesced outcomes
    let outcomes = match broadcast.take() {
        Some(bc) => broadcast_join(bc).await,
        None => vec![],
    };

    // Final flush to primary client
    if !write_queue.is_empty() {
        trace!(
            "net: cache→client final flush (text, {} bytes remaining)",
            write_queue.remaining()
        );
        if let Err(e) = client_socket.write_all_buf(&mut write_queue).await {
            error!("no client: {e}");
            return Err(CacheError::Write.into());
        }
    }

    msg.timing.response_written_at = Some(Instant::now());

    #[cfg(feature = "hotpath")]
    drop(_m);

    Ok((bytes_served, outcomes))
}

/// Response state machine for the binary (pipelined extended query) path.
///
/// Pipelined: SET (simple query) + Parse/Bind/[Describe('P')]/Execute/Sync produces:
/// CommandComplete (SET) → ReadyForQuery → ParseComplete → BindComplete →
/// [RowDescription →] DataRow* → CommandComplete (SELECT) → ReadyForQuery
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BinaryResponseState {
    /// Waiting for SET CommandComplete
    SetComplete,
    /// Waiting for ReadyForQuery after SET
    SetReady,
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

#[cfg_attr(feature = "hotpath", hotpath::measure)]
async fn handle_cached_query_binary(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    msg: &mut WorkerRequest,
) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
    let mut guard = ConnectionGuard::new(conn, return_tx);
    let mut conn = guard.conn.take().ok_or(CacheError::NoConnection)?;

    let include_describe = msg.pipeline_describe != PipelineDescribe::None;

    if let MvServe::Mv(cols) = &msg.mv {
        // MV fast path: extended query only, no SET prefix.
        let sql = mv_serve_sql(msg.fingerprint, &msg.resolved, msg.limit.as_ref(), cols);
        conn.extended_binary_query_send(&sql, include_describe)
            .await
            .inspect_err(|_| {
                guard.poisoned = true;
            })?;
    } else {
        conn.sql_buf.clear();
        conn.sql_buf.push_str(&msg.deparsed_sql);
        if let Some(limit) = &msg.limit {
            limit.deparse(&mut conn.sql_buf);
        }
        let set_sql = format!("SET mem.query_generation = {}", msg.generation);
        conn.pipelined_binary_query_send(&set_sql, include_describe)
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
    } = conn;
    let mut framed = FramedRead::new(stream, codec);
    *framed.read_buffer_mut() = read_buf;

    let has_sync = msg.has_sync;
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

    // MV path: no SET statement was sent, so skip SetComplete/SetReady and
    // start directly at ParseComplete (first message from the extended query).
    let mut state = if matches!(msg.mv, MvServe::Mv(_)) {
        BinaryResponseState::ParseComplete
    } else {
        BinaryResponseState::SetComplete
    };
    let mut bytes_served: usize = 0;

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
                    (BinaryResponseState::SetComplete, PgBackendMessageType::CommandComplete) => {
                        state = BinaryResponseState::SetReady;
                    }
                    (BinaryResponseState::SetReady, PgBackendMessageType::ReadyForQuery) => {
                        state = BinaryResponseState::ParseComplete;
                    }
                    (BinaryResponseState::ParseComplete, PgBackendMessageType::ParseComplete) => {
                        state = BinaryResponseState::BindComplete;
                    }
                    (BinaryResponseState::BindComplete, PgBackendMessageType::BindComplete) => {
                        state = if include_describe {
                            BinaryResponseState::DescribeRow
                        } else {
                            BinaryResponseState::DataRows
                        };
                    }
                    (BinaryResponseState::DescribeRow, PgBackendMessageType::RowDescription) => {
                        if pipeline_describe == PipelineDescribe::Statement
                            && let Some(param_desc) = parameter_description.take()
                        {
                            trace!("net: cache→client ParameterDescription (binary, {} bytes)", param_desc.len());
                            push_and_broadcast(&mut write_queue, &broadcast, param_desc);
                        }
                        trace!("net: cache→client RowDescription (binary, {} bytes)", frame.data.len());
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        state = BinaryResponseState::DataRows;
                    }
                    (BinaryResponseState::DataRows, PgBackendMessageType::DataRows) => {
                        trace!("net: cache→client DataRow (binary, {} bytes)", frame.data.len());
                        bytes_served += frame.data.len();
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                    }
                    (BinaryResponseState::DataRows, PgBackendMessageType::CommandComplete) => {
                        trace!("net: cache→client CommandComplete (binary, {} bytes)", frame.data.len());
                        push_and_broadcast(&mut write_queue, &broadcast, frame.data);
                        msg.timing.query_done_at = Some(Instant::now());
                    }
                    (_, PgBackendMessageType::ReadyForQuery)
                        if state != BinaryResponseState::SetComplete
                            && state != BinaryResponseState::SetReady =>
                    {
                        state = BinaryResponseState::Done;
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
            result = client_socket.write_buf(&mut write_queue), if !write_queue.is_empty() =>
            {
                match result {
                    Ok(cnt) => {
                        trace!("net: cache→client flush (binary, partial write, {} bytes)", cnt);
                    }
                    Err(_) => {
                        guard.poisoned = true;
                        error!("no client");
                        if let Some(bc) = broadcast.take() {
                            broadcast_error_reply(bc).await;
                        }
                        return Err(CacheError::Write.into());
                    }
                }
            }
        }

        if state == BinaryResponseState::Done {
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
    });
    if let Err(e) = guard.release().await {
        if let Some(bc) = broadcast.take() {
            broadcast_error_reply(bc).await;
        }
        return Err(e);
    }

    if has_sync {
        trace!("net: cache→client ReadyForQuery (binary pipeline)");
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
            "net: cache→client final flush (binary, {} bytes remaining)",
            write_queue.remaining()
        );
        if let Err(e) = client_socket.write_all_buf(&mut write_queue).await {
            error!("no client: {e}");
            return Err(CacheError::Write.into());
        }
    }

    msg.timing.response_written_at = Some(Instant::now());

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
