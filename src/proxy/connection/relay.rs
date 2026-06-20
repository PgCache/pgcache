use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use lru::LruCache;

use ecow::EcoString;

use crate::catalog::FunctionVolatility;

use tokio::{io::AsyncWriteExt, net::TcpStream, select};
use tokio_stream::StreamExt;
use tokio_util::{
    bytes::{Buf, Bytes, BytesMut},
    codec::FramedRead,
};
use tracing::{debug, error, instrument, trace, warn};

use crate::{
    cache::{
        CacheDispatchHandle, CacheMessage, CacheOutcome, CacheReply, ProxyMessage, ReplySlot,
        ReplyState, messages::slices_concat,
    },
    pg::protocol::{
        ProtocolError,
        backend::{
            AUTHENTICATION_SASL, PgBackendMessage, PgBackendMessageCodec, PgBackendMessageType,
            authentication_type, parameter_status_parse,
        },
        extended::PreparedStatement,
        frontend::{
            PgFrontendMessage, PgFrontendMessageCodec, PgFrontendMessageType,
            simple_query_message_build, startup_message_parameter,
        },
    },
    proxy::egress::EgressQueue,
    query::ast::query_expr_fingerprint,
    settings::SslMode,
    telemetry::pg_version_set,
    timing::QueryTiming,
    tls::{self},
};

use super::super::client_stream::{ClientSocket, ClientStream, OwnedClientReadHalf};
use super::super::query::{Action, CacheabilityCache, ForwardReason, handle_query};
use super::super::{ConnectionError, ConnectionResult, ProxyMode, ProxyStatus};
use crate::result::ReportExt;

use super::*;

/// Process-global "log at most once per window" gate, shared across all
/// connections. A flapping cache subsystem can drop one client per query; this
/// keeps the operator-facing warning to a single line per window per call site
/// rather than one per affected client. `last` is per-call-site state holding
/// the last emit time in whole seconds since first use (`u64::MAX` = never).
fn log_gate(last: &AtomicU64, window_secs: u64) -> bool {
    static START: LazyLock<Instant> = LazyLock::new(Instant::now);
    let now = START.elapsed().as_secs();
    let prev = last.load(Ordering::Relaxed);
    if prev != u64::MAX && now.wrapping_sub(prev) < window_secs {
        return false;
    }
    last.compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

/// Window for the cache-down operator warnings, in seconds.
const CACHE_DOWN_LOG_WINDOW_SECS: u64 = 5;

/// Gate state: cache died with a query in flight (client connection dropped).
static CACHE_DEAD_INFLIGHT_LOG: AtomicU64 = AtomicU64::new(u64::MAX);

/// Gate state: cache unavailable at dispatch (connections degraded to origin).
static CACHE_DEGRADED_LOG: AtomicU64 = AtomicU64::new(u64::MAX);

/// Guard that decrements active connections gauge when dropped.
struct ActiveConnectionGuard;

impl Drop for ActiveConnectionGuard {
    fn drop(&mut self) {
        crate::metrics::handles().conn.active.decrement(1.0);
    }
}

/// Prepend a `Parse` to `origin_write_buf` when origin doesn't yet know the
/// statement. No-op if origin already knows it, if no `parse_bytes` were
/// captured, if the statement is unnamed (origin's unnamed slot doesn't
/// persist across Sync), or if another `OriginIntercept` is already active.
///
/// Free function (not a method) to allow disjoint field borrows from the
/// event-loop call sites that work with `state.*` after partial moves.
pub(in crate::proxy::connection) fn forward_lazy_parse_install(
    stmt_name: &str,
    prepared_statements: &HashMap<EcoString, PreparedStatement>,
    origin_write_buf: &mut VecDeque<BytesMut>,
    origin_intercept: &mut OriginIntercept,
) {
    if stmt_name.is_empty() {
        return;
    }
    if !matches!(origin_intercept, OriginIntercept::None) {
        return;
    }
    let Some(stmt) = prepared_statements.get(stmt_name) else {
        return;
    };
    if stmt.origin_prepared {
        return;
    }
    let Some(parse_bytes) = stmt.parse_bytes.clone() else {
        return;
    };
    // Cold lazy-Parse forward: materialize the refcounted slice into the
    // origin write queue (BytesMut).
    origin_write_buf.push_back(slices_concat(std::slice::from_ref(&parse_bytes)));
    // The prepended Parse's ParseComplete is swallowed by the LazyParseInline
    // intercept (which marks origin_prepared) — it is NOT a client-awaited
    // response, so it must not be queued in pending_parse_statements.
    *origin_intercept = OriginIntercept::LazyParseInline {
        statement_name: stmt_name.into(),
    };
    crate::metrics::handles()
        .conn
        .lazy_parse_forwarded
        .increment(1);
}

#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    client_stream: ClientStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: &str,
    dispatch_handle: CacheDispatchHandle,
    func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
    origin_database: EcoString,
) -> ConnectionResult<()> {
    // Track active connections - guard ensures decrement on any exit path
    crate::metrics::handles().conn.active.increment(1.0);
    let _connection_guard = ActiveConnectionGuard;

    // Connect to origin database (with TLS if required)
    let mut origin_stream = origin_connect(&addrs, ssl_mode, server_name)
        .await
        .attach_loc("connecting to origin")?;

    // Split origin stream (borrowed halves with .writable() support)
    let (origin_read, origin_write) = origin_stream.split();
    let origin_framed_read = FramedRead::new(origin_read, PgBackendMessageCodec::default());

    // Split the client stream into OWNED halves with one reactor registration.
    // The connection keeps the read half (framed) and owns the write half
    // (`socket`), which it leases to the cache worker per query — no per-query
    // `dup`. `socket` is a plain owned value; while it is lent to the worker
    // there is simply no `socket` binding to write through.
    let (client_read, mut socket) = client_stream.into_split();
    let client_framed_read = FramedRead::new(client_read, PgFrontendMessageCodec::default());

    let mut state = ConnectionState::new(func_volatility, origin_database);

    // Reused for every query's reply: allocated once here instead of a per-query
    // oneshot, keeping the serve hot path allocation-free.
    let reply_slot = Arc::new(ReplySlot::<CacheReply>::default());

    tokio::pin!(origin_framed_read);
    tokio::pin!(client_framed_read);
    tokio::pin!(origin_write);

    loop {
        match state.proxy_mode {
            ProxyMode::Read => {
                if let Err(err) = state
                    .connection_select(
                        &mut origin_framed_read,
                        &mut client_framed_read,
                        &mut origin_write,
                        &mut socket,
                    )
                    .await
                {
                    debug!("read error [{}]", err);
                    break;
                }
            }
            ProxyMode::OriginDrain => {
                // The cacheable query is queued in the egress queue. `cache_dispatch`
                // returns it (and marks its slot serving) only once the slot has
                // reached the head; until then we drain origin and flush earlier
                // responses without reading the client (no read-ahead), so the
                // cache response can't jump ahead of an in-flight origin response.
                let Some(msg) = state.egress.cache_dispatch() else {
                    if let Err(err) = state
                        .connection_select_drain(
                            &mut origin_framed_read,
                            &mut origin_write,
                            &mut socket,
                        )
                        .await
                    {
                        debug!("read error [{}]", err);
                        break;
                    }
                    continue;
                };

                // The cache query is in hand and its slot is serving. Resolve
                // search_path; if unknown, forward to origin instead of caching.
                let Some(resolved_search_path) = state.search_path_state.resolve() else {
                    debug!("search_path unknown, forwarding to origin");
                    crate::metrics::handles().query.uncacheable.increment(1);
                    state.cache_slot_forward_to_origin(msg);
                    continue;
                };

                crate::metrics::handles().query.cacheable.increment(1);

                let reply_tx = reply_slot.sender();
                let timing = state.telemetry.cache_timing_dispatch();

                // Lease the owned write half to the worker for this query.
                let proxy_msg = ProxyMessage {
                    message: msg,
                    client_socket: socket,
                    reply_tx,
                    search_path: resolved_search_path,
                    timing,
                    pipeline: state.extended.pipeline_take(),
                };

                // Inline dispatch: the connection dispatches against the shared
                // CacheDispatch directly (inline, no extra hop). A hit goes straight to
                // the worker, a miss/coalesce/registration is handled inline; the
                // reply (and the leased write half) comes back via `reply_rx`.
                match dispatch_handle.current() {
                    Some(mut dispatch) => {
                        // The write half is now leased into the dispatch. Await the
                        // reply, which returns it; origin messages buffer in egress
                        // meanwhile and flush once we are back in `Read`.
                        dispatch.dispatch_proxy(proxy_msg).await;
                        match state
                            .cache_serve_wait(
                                &mut origin_framed_read,
                                &mut origin_write,
                                &reply_slot,
                            )
                            .await
                        {
                            Ok(returned) => {
                                // `handle_cache_outcome` set the next mode:
                                // `OriginDrain` to serve the next batched slot,
                                // or `Read` when the batch is done / forwarded.
                                socket = returned;
                            }
                            Err(err) => {
                                debug!("cache serve error [{}]", err);
                                break;
                            }
                        }
                    }
                    None => {
                        // Cache is unavailable: recover the leased write half and
                        // fall back to proxying directly to origin.
                        debug!("cache unavailable");
                        if log_gate(&CACHE_DEGRADED_LOG, CACHE_DOWN_LOG_WINDOW_SECS) {
                            warn!(
                                "cache subsystem unavailable; forwarding connections directly to origin (root cause logged by the cache supervisor)"
                            );
                        }
                        state.proxy_status = ProxyStatus::Degraded;
                        // The query never reaches a worker; disarm so dropping the
                        // sender leaves no stale permit in the reusable reply slot
                        // for the next query's wait to trip over.
                        proxy_msg.reply_tx.disarm();
                        socket = proxy_msg.client_socket;
                        // The cache slot is now serving (message already taken);
                        // forward this entry (lazy-Parsing if origin doesn't know
                        // its statement) plus the rest of the batch + one Sync.
                        let bytes = proxy_msg.pipeline.map_or_else(
                            || proxy_msg.message.into_data(),
                            |p| slices_concat(&p.buffered_bytes),
                        );
                        state.forward_current_and_rest(bytes);
                    }
                }
            }
        }
    }

    // Clean up prepared statements gauge before connection state is dropped
    let remaining_stmts = state.prepared_statements.len();
    if remaining_stmts > 0 {
        // Gauge value; per-connection prepared statement count never approaches 2^53.
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles()
            .conn
            .prepared_statements
            .decrement(remaining_stmts as f64);
    }

    match state.proxy_status {
        ProxyStatus::Degraded => Err(ConnectionError::CacheDead.into()),
        ProxyStatus::Normal => Ok(()),
    }
}

/// Handle one accepted client connection end-to-end (TLS negotiation + the
/// proxy loop). Spawned per accept onto the shared multi-thread runtime.
#[instrument(skip_all)]
#[cfg_attr(feature = "hotpath", hotpath::measure)]
#[allow(clippy::too_many_arguments)]
pub async fn connection_task(
    socket: TcpStream,
    addrs: Vec<SocketAddr>,
    ssl_mode: SslMode,
    server_name: EcoString,
    dispatch_handle: CacheDispatchHandle,
    tls_acceptor: Option<Arc<tls::TlsAcceptor>>,
    func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
    origin_database: EcoString,
) {
    debug!("task spawn");

    // Negotiate client TLS if configured
    let client_stream = match tls::client_tls_negotiate(socket, tls_acceptor.as_deref()).await {
        Ok(tls::ClientTlsResult::Tls {
            tcp_stream,
            tls_state,
        }) => ClientStream::tls(tcp_stream, tls_state),
        Ok(tls::ClientTlsResult::Plain(stream)) => ClientStream::plain(stream),
        Ok(tls::ClientTlsResult::Closed) => {
            // Peer closed before sending anything (e.g. L4 health check). Drop it
            // without dialing origin.
            debug!("client closed before startup");
            return;
        }
        Err(e) => {
            crate::metrics::handles().conn.errors.increment(1);
            error!("TLS negotiation failed: {}", e);
            return;
        }
    };

    let res = handle_connection(
        client_stream,
        addrs,
        ssl_mode,
        &server_name,
        dispatch_handle,
        func_volatility,
        origin_database,
    )
    .await;

    if let Err(e) = res {
        error!("{}", e);
        crate::metrics::handles().conn.errors.increment(1);
    }

    debug!("task done");
}

impl ConnectionState {
    pub(in crate::proxy::connection) fn new(
        func_volatility: Arc<HashMap<EcoString, FunctionVolatility>>,
        origin_database: EcoString,
    ) -> Self {
        Self {
            origin_write_buf: VecDeque::new(),
            egress: EgressQueue::new(),
            flush_describe_pending: false,
            cacheability_cache: CacheabilityCache::default(),
            in_transaction: false,
            proxy_mode: ProxyMode::Read,
            proxy_status: ProxyStatus::Normal,
            prepared_statements: HashMap::new(),
            portals: HashMap::new(),
            session_user: None,
            origin_intercept: OriginIntercept::None,
            search_path_state: SearchPathState::Unknown,
            search_path_just_piggyback_resolved: false,
            search_path_auto_reported: false,
            telemetry: QueryTelemetry::new(),
            func_volatility,
            extended: ExtendedPending::new(),
            origin_database,
            cache_disabled: false,
            describe_cache: LruCache::new(DESCRIBE_CACHE_CAPACITY),
        }
    }
    /// Push a Sync-terminated batch to origin, recording the forward in
    /// telemetry and reserving an ordered client-response slot so locally
    /// produced responses (synth, cache) can't jump ahead of this one.
    pub(in crate::proxy::connection) fn origin_dispatch(
        &mut self,
        bytes: BytesMut,
        timing: Option<QueryTiming>,
    ) {
        self.telemetry.origin_forward(timing);
        self.egress.origin_open();
        self.origin_write_buf.push_back(bytes);
    }

    /// Handle a message from the client (frontend).
    /// Determines whether to forward to origin, check cache, or take other action.
    #[expect(clippy::wildcard_enum_match_arm)]
    pub(in crate::proxy::connection) async fn handle_client_message(
        &mut self,
        mut msg: PgFrontendMessage,
    ) {
        trace!("net: client→proxy {:?}", msg.message_type);

        // A prior Flush forwarded a Describe sub-request whose response carries
        // no ReadyForQuery. The client only sends this next message after
        // reading that response, so the describe egress slot is now complete:
        // seal it so a following cache/forward response stays correctly ordered
        // behind it instead of being blocked by the unsealed slot.
        if self.flush_describe_pending {
            self.flush_describe_pending = false;
            self.egress.origin_seal();
        }
        match msg.message_type {
            PgFrontendMessageType::Query => {
                let m = crate::metrics::handles();
                m.query.total.increment(1);
                m.conn.simple_queries.increment(1);
                self.telemetry.query_receive();

                self.search_path_inspect_query(&mut msg);

                if !self.in_transaction && !self.cache_disabled {
                    self.proxy_mode = match handle_query(
                        &msg.data,
                        &mut self.cacheability_cache,
                        &self.func_volatility,
                    )
                    .await
                    {
                        Ok(Action::Forward(reason)) => {
                            match reason {
                                ForwardReason::UnsupportedStatement => {
                                    m.query.unsupported.increment(1);
                                }
                                ForwardReason::UncacheableSelect => {
                                    m.query.uncacheable.increment(1);
                                }
                                ForwardReason::Invalid => {
                                    m.query.invalid.increment(1);
                                }
                            }
                            self.origin_dispatch(msg.data, None);
                            ProxyMode::Read
                        }
                        Ok(Action::CacheCheck(ast)) => {
                            let fingerprint = query_expr_fingerprint(&ast.query);
                            self.telemetry.cache_timing_start(fingerprint);
                            self.extended.dispatch_is_extended = false;
                            self.egress.cache_push(CacheMessage::Query(msg.data, ast));
                            ProxyMode::OriginDrain
                        }
                        Err(e) => {
                            m.query.uncacheable.increment(1);
                            m.query.invalid.increment(1);
                            error!("handle_query {}", e);
                            self.origin_dispatch(msg.data, None);
                            ProxyMode::Read
                        }
                    };
                } else {
                    m.query.uncacheable.increment(1);
                    self.origin_dispatch(msg.data, None);
                }
            }
            PgFrontendMessageType::Parse => {
                self.handle_parse_message(msg);
            }
            PgFrontendMessageType::Bind => {
                self.handle_bind_message(msg);
            }
            PgFrontendMessageType::Execute => {
                self.handle_execute_message(msg);
            }
            PgFrontendMessageType::Describe => {
                self.handle_describe_message(msg);
            }
            PgFrontendMessageType::Close => {
                self.handle_close_message(msg);
            }
            PgFrontendMessageType::Sync => {
                self.handle_sync_message(msg);
            }
            PgFrontendMessageType::Flush => {
                self.handle_flush_message(msg);
            }
            PgFrontendMessageType::Startup => {
                self.session_user = startup_message_parameter(&msg.data, "user").map(String::from);

                // The cache is connected to a specific database and cannot serve queries
                // for other databases.
                if let Some(client_db) = startup_message_parameter(&msg.data, "database")
                    && client_db != self.origin_database.as_str()
                {
                    warn!(
                        "client database '{}' does not match cache database '{}', caching disabled for this connection",
                        client_db, self.origin_database
                    );
                    self.cache_disabled = true;
                }

                self.origin_write_buf.push_back(msg.data);
            }
            PgFrontendMessageType::SslRequest => {
                // SSLRequest should be handled during connection setup before framing begins.
                // If we receive it here, something unexpected happened - log a warning.
                // Respond with 'N' to allow the connection to continue.
                debug!("unexpected SslRequest after TLS negotiation phase, responding 'N'");
                self.egress.synth_push(Bytes::from_static(b"N"));
            }
            PgFrontendMessageType::PasswordMessageFamily => {
                // Forward password/SASL messages to origin.
                // Note: Channel binding cannot be modified in transit because SCRAM includes
                // the gs2-header in the cryptographic proof. Clients connecting via TLS must
                // use channel_binding=disable in their connection string.
                self.origin_write_buf.push_back(msg.data);
            }
            _ => {
                // All other message types - forward to origin
                self.origin_write_buf.push_back(msg.data);
            }
        }
    }

    /// Handle a message from the origin database (backend).
    /// Updates transaction state, captures parameter OIDs, and forwards to client.
    #[expect(clippy::wildcard_enum_match_arm)]
    pub(in crate::proxy::connection) fn handle_origin_message(
        &mut self,
        mut msg: PgBackendMessage,
    ) {
        trace!("net: origin→proxy {:?}", msg.message_type);

        if self.origin_intercept_handle(&msg) {
            // Swallowed by an intercept (injected SHOW, lazy ParseComplete, …):
            // these have no client egress slot, so an RFQ here seals nothing.
            return;
        }

        match msg.message_type {
            PgBackendMessageType::ParameterStatus => {
                if let Some((name, value)) = parameter_status_parse(&msg.data) {
                    match name {
                        "search_path" => {
                            // PG18+ reports search_path as a GUC_REPORT parameter,
                            // emitting this message on every change (including
                            // startup, SET, DISCARD ALL, and SET LOCAL reverts at
                            // transaction end). First arrival tells us we can
                            // skip the defensive SHOW machinery.
                            debug!("received search_path from ParameterStatus: {}", value);
                            self.search_path_state =
                                SearchPathState::resolved(value, self.session_user.as_deref());
                            self.search_path_auto_reported = true;
                        }
                        "server_version" => {
                            pg_version_set(value.to_owned());
                        }
                        _ => {}
                    }
                }
            }
            PgBackendMessageType::ParameterDescription => {
                self.extended
                    .parameter_description_received(&msg.data, &mut self.prepared_statements);
            }
            PgBackendMessageType::RowDescription => {
                if let Some(name) = self
                    .extended
                    .row_description_received(&msg.data, &mut self.prepared_statements)
                {
                    self.describe_cache_populate(&name);
                }
            }
            PgBackendMessageType::NoData => {
                if let Some(name) = self
                    .extended
                    .no_data_received(&mut self.prepared_statements)
                {
                    self.describe_cache_populate(&name);
                }
            }
            PgBackendMessageType::ParseComplete => {
                self.extended.parse_complete(&mut self.prepared_statements);
            }
            PgBackendMessageType::Authentication => {
                if authentication_type(&msg.data).is_some_and(|v| v == AUTHENTICATION_SASL) {
                    // Strip SCRAM-SHA-256-PLUS from SASL authentication options.
                    // Channel binding cannot be supported because the proxy terminates TLS.
                    let needle = b"SCRAM-SHA-256-PLUS\0";
                    if let Some(pos) = msg
                        .data
                        .windows(needle.len())
                        .position(|window| window == needle)
                    {
                        // Post-strip length field (bytes 1-4, big-endian i32, excludes tag byte).
                        // Computed before mutating so an out-of-range length (unreachable: auth
                        // messages are tiny) degrades to forwarding the message unmodified rather
                        // than panicking — the strip is best-effort anyway.
                        if let Some(new_len) = msg
                            .data
                            .len()
                            .checked_sub(needle.len() + 1)
                            .and_then(|n| i32::try_from(n).ok())
                        {
                            // Remove needle in place using split/unsplit
                            let mut tail = msg.data.split_off(pos);
                            let after_needle = tail.split_off(needle.len());
                            msg.data.unsplit(after_needle);

                            #[expect(
                                clippy::indexing_slicing,
                                reason = "PostgreSQL message format guarantees 5+ bytes"
                            )]
                            msg.data[1..5].copy_from_slice(&new_len.to_be_bytes());
                        }
                    }
                }
            }
            PgBackendMessageType::ReadyForQuery => {
                // ReadyForQuery message contains transaction status at byte 5
                // 'I' = idle (not in transaction)
                // 'T' = in transaction block
                // 'E' = in failed transaction block
                let was_in_transaction = self.in_transaction;
                self.in_transaction = msg.data.get(5).is_some_and(|&b| b == b'T' || b == b'E');

                self.telemetry.origin_complete();

                // Clean up unnamed portals when transaction ends
                if !self.in_transaction {
                    self.portals.retain(|name, _| !name.is_empty());
                }

                // Transaction ended: any SET (including SET LOCAL) within the
                // txn reverts, so the cached search_path may no longer match.
                // Skipped on PG18+: ParameterStatus already arrived earlier in
                // this same batch with the post-txn value. Also skipped if a
                // piggyback intercept in this batch has already resolved
                // search_path — marking unknown would clobber that fresh
                // value.
                if was_in_transaction
                    && !self.in_transaction
                    && !self.search_path_just_piggyback_resolved
                    && !self.search_path_auto_reported
                {
                    debug!("txn ended, marking search_path unknown");
                    self.search_path_mark_unknown();
                }
                self.search_path_just_piggyback_resolved = false;

                // If search_path is unknown (initial discovery on pre-PG18, or
                // after a detected mutation / txn-end), inject a SHOW query to
                // re-sync. Skipped if another intercept is active — another
                // RFQ will follow.
                if let SearchPathState::Unknown = self.search_path_state
                    && matches!(self.origin_intercept, OriginIntercept::None)
                {
                    debug!("search_path unknown, sending SHOW search_path query");
                    self.origin_intercept = OriginIntercept::SearchPath;
                    let query_msg = simple_query_message_build("SHOW search_path;");
                    // Injected query: its response is fully swallowed by the
                    // SearchPath intercept, so it gets no client egress slot.
                    self.origin_write_buf.push_back(query_msg);
                }
            }
            _ => {}
        }

        trace!(
            "net: origin→client {:?} ({} bytes)",
            msg.message_type,
            msg.data.len()
        );
        let was_rfq = matches!(msg.message_type, PgBackendMessageType::ReadyForQuery);
        self.egress.origin_append(msg.data.freeze());
        if was_rfq {
            // ReadyForQuery ends this request's response: seal its slot so the
            // next slot (and any locally-produced response behind it) can flush.
            self.egress.origin_seal();
        }
    }

    /// Handle the outcome of a cache reply (the leased socket has already been
    /// recovered by `cache_serve_wait`). If the cache indicates error or needs
    /// forwarding, send the query to origin instead.
    pub(in crate::proxy::connection) fn handle_cache_outcome(&mut self, outcome: CacheOutcome) {
        trace!(
            "net: cache→proxy reply={}",
            match &outcome {
                CacheOutcome::Complete(_) => "Complete",
                CacheOutcome::Forward(_, _) => "Forward",
                CacheOutcome::Error(_) => "Error",
            }
        );
        match outcome {
            CacheOutcome::Complete(timing) => {
                crate::metrics::handles().query.cache_hit.increment(1);
                self.telemetry.cache_complete(timing);

                // Cache hit: the worker wrote the full response directly to the
                // client socket. Pop the serving cache slot so the next slot can
                // flush. Origin saw nothing; lazy-Parse-on-forward catches up the
                // next time we actually have to forward.
                self.egress.cache_done();
                self.extended.pending_parse_statements.clear();
                self.extended.pending_describe_statements.clear();
                self.extended.pending_lazy_parse.take();

                // Advance to the next batched execute, or finish.
                self.cache_batch_advance();
            }
            CacheOutcome::Error(buf) => {
                crate::metrics::handles().query.cache_error.increment(1);
                debug!("forwarding to origin");
                self.cache_reply_forward(buf, None);
            }
            CacheOutcome::Forward(buf, timing) => {
                crate::metrics::handles().query.cache_miss.increment(1);
                debug!("forwarding to origin");
                self.cache_reply_forward(buf, Some(timing));
            }
        }
    }

    /// Convert the serving `Cache` slot to an `Origin` slot in place, lazy-Parse
    /// the current entry if origin doesn't yet know its statement, forward its
    /// bytes followed by the rest of the batch and a single trailing Sync, and
    /// return to `Read`. Shared by every cache→origin fallback (miss/error,
    /// search_path-unknown, cache-unavailable).
    pub(in crate::proxy::connection) fn forward_current_and_rest(
        &mut self,
        current_bytes: BytesMut,
    ) {
        self.egress.cache_to_origin();
        if let Some(stmt_name) = self.extended.pending_lazy_parse.take() {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }
        self.origin_write_buf.push_back(current_bytes);
        self.batch_remaining_forward();
        self.proxy_mode = ProxyMode::Read;
    }

    /// Forward a cache miss/error to origin: the worker returns the missed
    /// entry's bytes, which we forward along with the rest of the batch.
    pub(in crate::proxy::connection) fn cache_reply_forward(
        &mut self,
        buf: BytesMut,
        timing: Option<QueryTiming>,
    ) {
        self.telemetry.origin_forward(timing);
        self.forward_current_and_rest(buf);
    }

    /// Fall back to forwarding a cacheable query to origin after it was taken
    /// from its egress slot (search_path unknown or client-socket creation
    /// failed): forward the pipeline/raw bytes plus the rest of the batch.
    pub(in crate::proxy::connection) fn cache_slot_forward_to_origin(&mut self, msg: CacheMessage) {
        let bytes = self
            .extended
            .pipeline_take()
            .map_or_else(|| msg.into_data(), |p| slices_concat(&p.buffered_bytes));
        self.forward_current_and_rest(bytes);
    }

    /// Whether the connection's global state currently permits cache serving.
    pub(in crate::proxy::connection) fn cache_globally_enabled(&self) -> bool {
        !self.in_transaction && !self.cache_disabled && self.proxy_status == ProxyStatus::Normal
    }

    /// Dispatch the result of an origin-read poll: handle the message, or map a
    /// decode error / EOF to a connection error. Shared by the select loops.
    pub(in crate::proxy::connection) fn origin_read_dispatch(
        &mut self,
        res: Option<Result<PgBackendMessage, ProtocolError>>,
    ) -> ConnectionResult<()> {
        match res {
            Some(Ok(msg)) => {
                self.handle_origin_message(msg);
                Ok(())
            }
            Some(Err(err)) => {
                debug!("origin read error [{}]", err);
                Err(ConnectionError::ProtocolError(err).into())
            }
            None => {
                debug!("origin stream closed");
                Err(ConnectionError::IoError(io::Error::new(
                    io::ErrorKind::ConnectionReset,
                    "origin disconnected",
                ))
                .into())
            }
        }
    }

    /// Write one buffered chunk to origin, popping it once fully drained. Guard
    /// the call site with `!self.origin_write_buf.is_empty()`.
    #[expect(
        clippy::indexing_slicing,
        reason = "VecDeque access guarded by !is_empty() at the call site"
    )]
    pub(in crate::proxy::connection) async fn origin_write_flush(
        &mut self,
        origin_write: &mut Pin<&mut OriginWriteHalf<'_>>,
    ) -> ConnectionResult<()> {
        origin_write
            .write_buf(&mut self.origin_write_buf[0])
            .await
            .map_err(ConnectionError::IoError)?;
        if !self.origin_write_buf[0].has_remaining() {
            self.origin_write_buf.pop_front();
        }
        Ok(())
    }

    /// Write the next ready egress chunk to the client. Guard the call site with
    /// `self.egress.has_writable()`.
    pub(in crate::proxy::connection) async fn client_egress_flush(
        &mut self,
        client_write: &mut ClientSocket,
    ) -> ConnectionResult<()> {
        let n = client_write
            .write(self.egress.write_chunk())
            .await
            .map_err(ConnectionError::IoError)?;
        self.egress.advance(n);
        Ok(())
    }

    pub(in crate::proxy::connection) async fn connection_select<'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        client_read: &mut Pin<&mut FramedRead<OwnedClientReadHalf, PgFrontendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut ClientSocket,
    ) -> ConnectionResult<()> {
        select! {
            res = client_read.next() => {
                match res {
                    Some(Ok(msg)) => {
                        self.handle_client_message(msg).await;
                    }
                    Some(Err(err)) => {
                        debug!("client read error [{}]", err);
                        return Err(ConnectionError::ProtocolError(err).into());
                    }
                    None => {
                        debug!("client stream closed");
                        return Err(ConnectionError::IoError(io::Error::new(
                            io::ErrorKind::ConnectionReset,
                            "client disconnected",
                        )).into());
                    }
                }
            }
            res = origin_read.next() => {
                self.origin_read_dispatch(res)?;
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                self.origin_write_flush(origin_write).await?;
            }
            _ = client_write.writable(), if self.egress.has_writable() => {
                self.client_egress_flush(client_write).await?;
            }
        };

        Ok(())
    }

    /// Await the cache worker's reply for the in-flight query, returning the
    /// leased client write half it hands back. The client write half is with the
    /// worker, so this does **not** write the client — origin messages that
    /// arrive meanwhile buffer in the egress queue and flush once the socket is
    /// restored and we are back in `Read`.
    pub(in crate::proxy::connection) async fn cache_serve_wait<'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        reply_slot: &ReplySlot<CacheReply>,
    ) -> ConnectionResult<ClientSocket> {
        // One pinned `Notified` polled across the whole loop: recreating it per
        // iteration would race with `notify_one` and drop the wakeup.
        let notified = reply_slot.notified();
        tokio::pin!(notified);
        loop {
            select! {
                res = origin_read.next() => {
                    self.origin_read_dispatch(res)?;
                }
                _ = &mut notified => {
                    match reply_slot.take() {
                        ReplyState::Sent(reply) => {
                            self.handle_cache_outcome(reply.outcome);
                            return Ok(reply.socket);
                        }
                        ReplyState::Dropped => {
                            // Sender dropped without sending: cache died in flight.
                            if log_gate(&CACHE_DEAD_INFLIGHT_LOG, CACHE_DOWN_LOG_WINDOW_SECS) {
                                warn!(
                                    "cache subsystem died with a query in flight; dropping client connection (root cause logged by the cache supervisor)"
                                );
                            } else {
                                debug!("cache channel closed");
                            }
                            self.proxy_status = ProxyStatus::Degraded;
                            return Err(ConnectionError::CacheDead.into());
                        }
                        ReplyState::Empty => {
                            // Stale permit from a sender dropped while no query
                            // was waiting (dispatch-unavailable fallback): this
                            // query's outcome is still pending. The completed
                            // future must be replaced before polling again.
                            debug!("stale reply-slot wakeup; re-arming");
                            notified.set(reply_slot.notified());
                        }
                    }
                }
                _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                    self.origin_write_flush(origin_write).await?;
                }
            }
        }
    }

    /// Select loop for `OriginDrain`: a cacheable query is queued behind earlier
    /// responses. Drains origin and flushes the egress queue **without reading
    /// the client**, so no later request is processed before the queued cache
    /// query — preserving response order until the cache slot reaches the head.
    pub(in crate::proxy::connection) async fn connection_select_drain<'b>(
        &mut self,
        origin_read: &mut Pin<&mut FramedRead<OriginReadHalf<'b>, PgBackendMessageCodec>>,
        origin_write: &mut Pin<&mut OriginWriteHalf<'b>>,
        client_write: &mut ClientSocket,
    ) -> ConnectionResult<()> {
        select! {
            res = origin_read.next() => {
                self.origin_read_dispatch(res)?;
            }
            _ = origin_write.writable(), if !self.origin_write_buf.is_empty() => {
                self.origin_write_flush(origin_write).await?;
            }
            _ = client_write.writable(), if self.egress.has_writable() => {
                self.client_egress_flush(client_write).await?;
            }
        };

        Ok(())
    }
}
