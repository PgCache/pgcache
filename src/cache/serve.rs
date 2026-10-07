use std::fmt::Write as _;
use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tokio::sync::mpsc::{Sender, UnboundedSender};
use tokio_stream::StreamExt;
use tokio_util::bytes::Bytes;
use tracing::{debug, instrument, trace, warn};

use super::{
    CacheError, CacheResult,
    memo::{MEMO_CAPTURE_MIN_HITS, MemoCapture, MemoKey, MemoShape},
    mv::{MvServe, MvState, mv_serve_sql_into},
    query_cache::{QueryType, ServeRequest},
    types::CacheStateView,
    write_queue::WriteQueue,
};
use crate::cache::messages::PipelineDescribe;
use crate::oid::Oid;
use crate::pg::cache_connection::{CacheConnection, PrepareOutcome};
use crate::pg::protocol::backend::TransactionStatus;
use crate::pg::protocol::encode::{BIND_COMPLETE_MSG, PARSE_COMPLETE_MSG};
use crate::query::ast::{AstNode, Deparse, LiteralValue};
use crate::query::query_shape_derive;
use crate::query::resolved::ResolvedTableNode;

mod coalesce;
mod relay;
mod response_state;
mod sqlstate;

use super::runtime::serve_pool::ConnectionGuard;
pub(super) use coalesce::CoalescedOutcome;
use coalesce::{broadcast_setup, push_and_broadcast};
use relay::{Relay, ServeClient, ServeDiagnostics};
use response_state::ServeResponseState;
pub(crate) use sqlstate::SQLSTATE_UNDEFINED_TABLE;

/// Max gap between cache-DB frames while draining a response for a departed
/// primary client before the connection is treated as stalled and discarded.
const DRAIN_STALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Total wall-clock budget for a single cache-hit serve (PGC-278). A serve that
/// exceeds this — a stalled cache-DB read or a response desync that parks on
/// `framed.next()` forever — is poisoned (discard + replenish) and forwarded to
/// origin, so a stuck serve can never permanently strand a pool connection.
const SERVE_STALL_TIMEOUT: Duration = Duration::from_secs(10);

/// Decide whether this serve should be captured into the in-process result memo
/// (PGC-236) and, if so, begin a capture — stamping the read relations' seqlock
/// versions *before* the serve query is issued. Returns `None` for MV serves,
/// disabled memoization, cold fingerprints, non-keyable shapes, or relations
/// mid-write.
///
/// Serves with no RowDescription on the wire (a reused prepared statement —
/// `Bind`/`Execute` only, no `Describe`) are captured too, with `rd_len == 0`.
/// That core (`DataRow* + CommandComplete`) serves any request that doesn't want
/// a RowDescription — i.e. the same reused-prepared-statement path the demo runs
/// exclusively. Requests that *do* want a RowDescription fall through unless a
/// later RowDescription-bearing serve upgrades the entry (see `memo_serve_plan`).
fn memo_capture_begin(
    state_view: &CacheStateView,
    msg: &ServeRequest,
    binary: bool,
) -> Option<MemoCapture> {
    // Only source-row serves are memoized (MV-eligible queries stay in q_<fp>).
    if !matches!(msg.mv, MvServe::SourceRow) {
        return None;
    }
    // Mutually exclusive with MVs: memoize only the terminal MV-ineligible
    // partition (Skip, or a Measure query whose size gate failed → Ineligible).
    // A Measure query serves source rows while its MV is still Pending; memoizing
    // that would shadow the MV once it's built. Both states are sticky, so a memo
    // captured here never later acquires an MV.
    let mv_ineligible = state_view
        .cached_queries
        .get(&msg.fingerprint)
        .is_some_and(|v| matches!(v.mv.state(), MvState::Skipped | MvState::Ineligible));
    if !mv_ineligible {
        return None;
    }
    let hits = state_view
        .metrics
        .get(&msg.fingerprint)
        .map(|m| m.hit_count)
        .unwrap_or(0);
    if hits < MEMO_CAPTURE_MIN_HITS {
        return None;
    }
    let shape = MemoShape::from_limit(&msg.limit)?;
    // Every relation the query *reads*, via the AST walk — deliberately broader
    // than the precomputed `CachedQuery.relation_oids` (which excludes subquery
    // tables): a change to any read relation, subqueries included, must evict the
    // snapshot, so this set must not be narrowed to that field.
    let mut oids: Vec<Oid> = msg
        .resolved
        .nodes::<ResolvedTableNode>()
        .map(|t| t.relation_oid)
        .collect();
    oids.sort_unstable();
    oids.dedup();
    if oids.is_empty() {
        return None;
    }
    let key = MemoKey {
        fingerprint: msg.fingerprint,
        binary,
        shape,
    };
    MemoCapture::begin(&state_view.memo, key, &oids)
}

/// Deterministic fault injection for serve-pool replenishment (PGC-238): poison
/// the first N cache serves so a test can drive repeated poisoned discards and
/// assert the pool refills. Compiled out unless built with `--features
/// fault-injection`.
#[cfg(feature = "fault-injection")]
pub(crate) mod fault {
    use std::sync::Once;
    use std::sync::atomic::{AtomicU64, Ordering};

    static POISON_SERVES: AtomicU64 = AtomicU64::new(0);
    static LOSE_SERVES: AtomicU64 = AtomicU64::new(0);
    static DESYNC_SERVES: AtomicU64 = AtomicU64::new(0);
    static MIDSTREAM_SERVES: AtomicU64 = AtomicU64::new(0);
    static INIT: Once = Once::new();

    fn env_budget(var: &str, budget: &AtomicU64) {
        if let Some(n) = std::env::var(var).ok().and_then(|s| s.parse::<u64>().ok()) {
            budget.store(n, Ordering::Relaxed);
        }
    }

    fn budget_consume(budget: &AtomicU64) -> bool {
        budget
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |n| {
                (n > 0).then(|| n - 1)
            })
            .is_ok()
    }

    /// Arm from the environment (read once for the process).
    pub(crate) fn init() {
        INIT.call_once(|| {
            env_budget("PGCACHE_FAULT_POISON_SERVES", &POISON_SERVES);
            env_budget("PGCACHE_FAULT_LOSE_SERVES", &LOSE_SERVES);
            env_budget("PGCACHE_FAULT_MIDSTREAM_SERVES", &MIDSTREAM_SERVES);
            env_budget("PGCACHE_FAULT_DESYNC_SERVES", &DESYNC_SERVES);
        });
    }

    /// Returns true (consuming one) while serves remain to be poisoned.
    pub(crate) fn poison_serve() -> bool {
        budget_consume(&POISON_SERVES)
    }

    /// Abandon the serve after the connection left the guard, without
    /// poisoning — exercises the lost-slot replenish branch (PGC-278).
    /// Fail the first N serves after bytes have reached the client, exercising
    /// the PGC-291 error-the-client path (and its in-block variant, PGC-387).
    pub(crate) fn midstream_serve() -> bool {
        init();
        budget_consume(&MIDSTREAM_SERVES)
    }

    pub(crate) fn lose_serve() -> bool {
        budget_consume(&LOSE_SERVES)
    }

    /// Corrupt the next backend frame's message type so the relay hits the
    /// desync arm (PGC-278).
    pub(crate) fn desync_serve() -> bool {
        budget_consume(&DESYNC_SERVES)
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

/// Issue the cached query on the pooled cache-DB connection and return what was
/// sent, so the response state machine knows which completions to expect. The MV
/// fast path sends an unnamed extended query (no SET prefix); the source-row path
/// sends a shape-keyed named prepared statement with parameterized literals +
/// LIMIT/OFFSET (a Parse only on first use of the shape on this connection, plus a
/// Close when preparing it evicted the FIFO cap's oldest shape). The caller poisons
/// the connection on error.
async fn serve_query_send(
    conn: &mut CacheConnection,
    msg: &ServeRequest,
    include_describe: bool,
    binary_results: bool,
) -> CacheResult<PrepareOutcome> {
    if let MvServe::Mv(plan) = &msg.mv {
        // Render into the connection's recycled SQL buffer rather than a fresh
        // String.
        mv_serve_sql_into(&mut conn.sql_buf, plan, msg.limit.as_ref());
        conn.extended_query_unnamed_send(include_describe, binary_results)
            .await?;
        return Ok(PrepareOutcome {
            sent_setgen_parse: false,
            sent_parse: true,
            sent_close: false,
        });
    }

    // Source-row serve via a shape-keyed prepared statement (PGC-294): the shape
    // SQL carries `$1..$k` placeholders for its literals, so a single plan is
    // shared by every query of this shape regardless of literal values (collapsing
    // the per-literal plan explosion that drove relcache-invalidation storms).
    // Most hits carry a precomputed shape; subsumed serves have none and derive it
    // from the resolved query here (cold path).
    let derived_shape;
    let shape = match &msg.serve_shape {
        Some(shape) => shape,
        None => {
            derived_shape = query_shape_derive(msg.resolved.as_ref());
            &derived_shape
        }
    };

    // Shape body, then the trailing `LIMIT $(k+1) OFFSET $(k+2)` placeholders.
    conn.sql_buf.clear();
    conn.sql_buf.push_str(&shape.sql);
    let literal_count = shape.literals.len();
    let _ = write!(
        conn.sql_buf,
        " LIMIT ${} OFFSET ${}",
        literal_count + 1,
        literal_count + 2
    );

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
    conn.pipelined_named_query_send(
        shape.key,
        msg.generation,
        &shape.literals,
        limit_text,
        offset_text,
        include_describe,
        binary_results,
    )
    .await
}

/// Serve a cache hit: execute the cached query on a pooled cache-DB connection
/// (a named prepared statement for source-row, unnamed extended for MV) and
/// relay the response to the client in its requested format. Returns the DataRow
/// bytes served and any coalesced client outcomes.
// Span at trace level: at info/debug the fmt layer allocates per-span
// extensions, which would put one heap allocation on every cache hit.
#[instrument(skip_all, level = "trace")]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn handle_cached_query(
    conn: CacheConnection,
    return_tx: Sender<CacheConnection>,
    replenish_tx: UnboundedSender<()>,
    msg: &mut ServeRequest,
    state_view: &CacheStateView,
) -> CacheResult<(usize, Vec<CoalescedOutcome>)> {
    debug!("message query generation {}", msg.generation);
    let mut guard = ConnectionGuard::new(conn, return_tx, replenish_tx);

    // Fault injection: poison this serve (discard the connection, fall through to
    // origin) to exercise serve-pool replenishment under induced poison churn.
    #[cfg(feature = "fault-injection")]
    if fault::poison_serve() {
        guard.poisoned = true;
        return Err(CacheError::InvalidMessage.into());
    }

    let mut conn = guard.conn.take().ok_or(CacheError::NoConnection)?;

    // Serve in the client's result format (text/binary). Simple-query clients
    // always expect a RowDescription, so request Describe from the cache DB even
    // when no Describe message was pipelined.
    let binary_results = msg.result_formats.is_binary();
    let include_describe =
        msg.query_type == QueryType::Simple || msg.pipeline_describe != PipelineDescribe::None;

    // Begin a result-memo capture for hot source-row serves. Stamps read-relation
    // versions now, before the serve query is issued (capture ordering invariant).
    let memo_capture = memo_capture_begin(state_view, msg, binary_results);

    // Issue the query on the cache-DB connection; `prepare` records what was sent
    // so the response state machine knows which completions to expect.
    let prepare = serve_query_send(&mut conn, msg, include_describe, binary_results)
        .await
        .inspect_err(|_| {
            guard.poisoned = true;
        })?;

    // Create broadcast for coalesced clients (after query is sent, before streaming)
    let broadcast = broadcast_setup(msg);

    // Stream results to client: move the read half into a FramedRead and park
    // the rest of the connection to restore once the response is drained.
    let (mut framed, parked) = conn.into_framed();

    // Fault (tests): drop the serve with the connection checked out and NOT
    // poisoned — the guard must replenish the lost slot (PGC-278).
    #[cfg(feature = "fault-injection")]
    if fault::lose_serve() {
        return Err(CacheError::InvalidMessage.into());
    }

    let mut relay = Relay {
        state: ServeResponseState::initial(&msg.mv, &prepare),
        guard,
        broadcast,
        memo_capture,
        parameter_description: msg.parameter_description.take(),
        bytes_served: 0,
        prepare,
        include_describe,
        pipeline_describe: msg.pipeline_describe,
        diagnostics: ServeDiagnostics {
            fingerprint: msg.fingerprint,
            has_parse: msg.has_parse,
            has_bind: msg.has_bind,
            mv: matches!(msg.mv, MvServe::Mv(_)),
        },
    };
    let mut client = ServeClient {
        // Captured before the `client_socket` borrow so the post-commit error
        // path (PGC-291) can decide whether to append a ReadyForQuery.
        trailing_rfq: msg.trailing_rfq(),
        in_block: msg.transaction_status == TransactionStatus::InTransaction,
        socket: &mut msg.client_socket,
        write_queue: WriteQueue::new(),
        committed: false,
        bytes_sent: false,
        gone: false,
    };

    if relay.diagnostics.has_parse {
        push_and_broadcast(
            &mut client.write_queue,
            &relay.broadcast,
            Bytes::from_static(PARSE_COMPLETE_MSG),
        );
    }
    if relay.diagnostics.has_bind {
        push_and_broadcast(
            &mut client.write_queue,
            &relay.broadcast,
            Bytes::from_static(BIND_COMPLETE_MSG),
        );
    }

    // PGC-278: bound the whole serve. The primary read arm (`framed.next()`) has
    // no per-read timeout, so a stalled cache-DB connection or a response desync
    // would park here forever holding the pooled connection. On the deadline,
    // poison (discard + replenish) and forward to origin.
    let serve_deadline = tokio::time::Instant::now() + SERVE_STALL_TIMEOUT;

    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(serve_deadline) => {
                let diagnostics = &relay.diagnostics;
                warn!(
                    fingerprint = %diagnostics.fingerprint,
                    state = ?relay.state,
                    bytes_served = relay.bytes_served,
                    client_gone = client.gone,
                    has_parse = diagnostics.has_parse,
                    has_bind = diagnostics.has_bind,
                    include_describe,
                    mv = diagnostics.mv,
                    sent_setgen_parse = relay.prepare.sent_setgen_parse,
                    sent_parse = relay.prepare.sent_parse,
                    sent_close = relay.prepare.sent_close,
                    "cache serve exceeded stall deadline; poisoning connection; forwarding to origin if no bytes sent, else erroring the client (PGC-278/PGC-291)"
                );
                crate::metrics::handles().cache.serve_stall_total.increment(1);
                relay.poison().await;
                return client.failure_resolve(relay.bytes_served, CacheError::Write.into()).await;
            }
            frame = framed.next() => {
                // Fault (tests): once any response bytes exist, push them to the
                // client and fail — the PGC-291 "already on the wire" shape.
                #[cfg(feature = "fault-injection")]
                if (client.bytes_sent || !client.write_queue.is_empty()) && fault::midstream_serve() {
                    let _ = client.socket.write_all_buf(&mut client.write_queue).await;
                    client.bytes_sent = true;
                    relay.poison().await;
                    return client
                        .failure_resolve(relay.bytes_served, CacheError::InvalidMessage.into())
                        .await;
                }
                let Some(Ok(frame)) = frame else {
                    relay.poison().await;
                    return client
                        .failure_resolve(relay.bytes_served, CacheError::InvalidMessage.into())
                        .await;
                };
                // frame_apply errors only on a cache-DB ErrorResponse or desync,
                // where it has already poisoned the guard and replied to coalesced
                // waiters. Same PGC-291 invariant: forward only if nothing was sent.
                if let Err(e) = relay.frame_apply(frame, &mut client.write_queue, &mut msg.timing).await {
                    return client.failure_resolve(relay.bytes_served, e).await;
                }
            }
            result = client.socket.write_buf(&mut client.write_queue),
                if client.committed && !client.write_queue.is_empty() && !client.gone =>
            {
                match result {
                    Ok(cnt) => {
                        if cnt > 0 {
                            client.bytes_sent = true;
                        }
                        trace!("net: cache→client flush (serve, partial write, {} bytes)", cnt);
                    }
                    Err(_) => {
                        // Primary client went away mid-serve. The cache-DB
                        // connection is healthy — do NOT poison it. Stop relaying
                        // to the primary but keep reading (and broadcasting) the
                        // rest of the response: coalesced waiters are independent
                        // and must still be served. Draining to completion also
                        // returns the connection to the pool protocol-clean
                        // (avoids serve-pool exhaustion).
                        debug!("primary client write failed mid-serve; draining cache-DB response, coalesced waiters still served");
                        client.gone = true;
                    }
                }
            }
            // Bound the drain: if the cache-DB goes silent while we're draining
            // for a departed primary, the connection is mid-response and unsafe
            // to reuse. Discard it (poison) rather than hold a pool slot forever
            // (PGC-238 replenish heals the pool).
            _ = tokio::time::sleep(DRAIN_STALL_TIMEOUT), if client.gone => {
                debug!("cache-DB stalled while draining for departed client; discarding connection");
                relay.poison().await;
                return Err(CacheError::Write.into());
            }
        }

        // PGC-291: once the relay reaches the data phase the cache-DB has
        // confirmed the query plan (BindComplete) and rows/CommandComplete are
        // imminent — commit, releasing the buffered response prefix to flush.
        // Before this point write_queue stays unflushed, so a stall/error
        // forwards transparently with nothing on the client wire.
        if matches!(
            relay.state,
            ServeResponseState::DataRows | ServeResponseState::Done
        ) {
            client.committed = true;
        }

        // While draining for a departed client, discard the relay buffer so it
        // can't grow with the rest of the response.
        if client.gone {
            client.write_queue.clear();
        }

        if relay.state == ServeResponseState::Done {
            break;
        }
    }

    relay.connection_return_or_discard(framed, parked);
    relay.finish(&mut client, &mut msg.timing, state_view).await
}

#[cfg(test)]
mod tests {
    use super::sqlstate::sqlstate_extract;
    use super::*;
    use crate::pg::protocol::encode::SERVE_ERROR_MSG;

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

    /// The hand-encoded static `SERVE_ERROR_MSG` (PGC-291) must be a well-formed
    /// ErrorResponse — guard against an encoding typo in the byte literal.
    #[test]
    fn test_serve_error_msg_layout() {
        let expected = error_response_frame(&[
            (b'S', b"ERROR"),
            (b'C', b"58000"),
            (b'M', b"pgcache: cache serve failed"),
        ]);
        assert_eq!(SERVE_ERROR_MSG, expected.as_slice());
    }

    #[test]
    fn test_limit_bind_text_binds_value_and_never_drops_non_integers() {
        let mut itoa_buf = itoa::Buffer::new();
        let mut other = String::new();

        // No clause field → NULL bind (no limit, offset 0).
        assert_eq!(limit_bind_text(None, &mut itoa_buf, &mut other), None);

        // Integer binds its decimal value (negatives included) with no alloc.
        assert_eq!(
            limit_bind_text(Some(&LiteralValue::Integer(5)), &mut itoa_buf, &mut other),
            Some("5")
        );
        assert!(
            other.is_empty(),
            "integer path must not touch the heap buffer"
        );
        assert_eq!(
            limit_bind_text(Some(&LiteralValue::Integer(-2)), &mut itoa_buf, &mut other),
            Some("-2")
        );

        // PGC-229 review #1: a non-integer limit must still BIND (not None, which
        // would bind NULL and silently drop the limit, returning every row). It
        // binds text that fails int8 coercion on the cache DB, erroring that hit
        // so it forwards to origin.
        let float =
            LiteralValue::Float(ordered_float::NotNan::new(3.7).expect("non-NaN test value"));
        let bound = limit_bind_text(Some(&float), &mut itoa_buf, &mut other);
        assert!(bound.is_some(), "non-integer limit must bind, not drop");
    }

    #[test]
    fn test_sqlstate_extract_undefined_table() {
        let frame = error_response_frame(&[
            (b'S', b"ERROR"),
            (b'C', b"42P01"),
            (b'M', b"relation \"public.evict_a\" does not exist"),
        ]);
        assert_eq!(sqlstate_extract(&frame), Some(*b"42P01"));
    }

    #[test]
    fn test_sqlstate_extract_first_field() {
        // SQLSTATE-first ordering should still parse.
        let frame = error_response_frame(&[(b'C', b"23505"), (b'S', b"ERROR")]);
        assert_eq!(sqlstate_extract(&frame), Some(*b"23505"));
    }

    #[test]
    fn test_sqlstate_extract_missing_returns_none() {
        let frame = error_response_frame(&[(b'S', b"ERROR"), (b'M', b"boom")]);
        assert_eq!(sqlstate_extract(&frame), None);
    }

    #[test]
    fn test_sqlstate_extract_wrong_length_returns_none() {
        // SQLSTATE must be exactly 5 chars; anything else is malformed.
        let frame = error_response_frame(&[(b'C', b"42P0")]);
        assert_eq!(sqlstate_extract(&frame), None);
    }

    #[test]
    fn test_sqlstate_extract_short_frame_returns_none() {
        // Frame shorter than the 5-byte header (tag + length) — graceful None.
        assert_eq!(sqlstate_extract(b"E\x00"), None);
    }
}
