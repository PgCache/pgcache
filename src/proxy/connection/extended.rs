use std::{
    collections::{HashMap, VecDeque},
    sync::Arc,
};

use ecow::EcoString;
use smallvec::SmallVec;

use tokio_util::bytes::{BufMut, Bytes, BytesMut};
use tracing::{debug, trace};

use crate::{
    cache::{
        CacheMessage, QueryParameters,
        messages::{MessageSlices, PipelineContext, PipelineDescribe, slices_concat},
        query::CacheableQuery,
    },
    pg::protocol::{
        encode::{CLOSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG},
        extended::{
            ParsedBindMessage, ParsedParseMessage, Portal, PreparedStatement, ResultFormats,
            StatementType, parse_bind_message, parse_close_message, parse_describe_message,
            parse_execute_message, parse_parameter_description, parse_parse_message,
        },
        frontend::PgFrontendMessage,
    },
    query::{Fingerprint, ast::query_expr_fingerprint},
};

use super::super::ProxyMode;
use super::super::query::{Action, ForwardReason, analyze};

use super::*;

/// Synth response for a Parse-only batch (no Describe): ParseComplete + RFQ('I').
const PARSE_COMPLETE_RFQ_IDLE: &[u8] = &[b'1', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'];

/// Wire bytes of a bare `Sync` message (`'S'` + length 4). Synthesized to close
/// origin's implicit transaction when a multi-execute cache batch falls back to
/// forwarding (the client's own `Sync` isn't replayed per entry).
const SYNC_MESSAGE: [u8; 5] = [b'S', 0, 0, 0, 4];

/// A cacheable-query snapshot captured at Execute time. Taken eagerly (not at
/// Sync) because a multi-execute batch typically reuses the unnamed portal /
/// statement, so `self.portals` / `prepared_statements` reflect only the *last*
/// Bind/Parse by Sync time. Snapshotting per entry keeps each execute's own
/// parameters and query.
pub(in crate::proxy::connection) struct CacheCandidate {
    pub(in crate::proxy::connection) cacheable_query: Arc<CacheableQuery>,
    pub(in crate::proxy::connection) parameters: QueryParameters,
    pub(in crate::proxy::connection) result_formats: ResultFormats,
    /// ParameterDescription bytes, present only for a Describe('S') entry.
    pub(in crate::proxy::connection) parameter_description: Option<Bytes>,
    /// Target statement name (for the lazy-Parse-on-forward decision).
    pub(in crate::proxy::connection) statement_name: EcoString,
    /// Whether origin already knows the statement (no lazy Parse needed).
    pub(in crate::proxy::connection) origin_prepared: bool,
}

impl CacheCandidate {
    /// Whether forwarding a Bind-without-Parse execute against this candidate
    /// requires prepending a lazy Parse (origin doesn't know the named
    /// statement). Combine with the entry's `has_parse` at the call site.
    pub(in crate::proxy::connection) fn lazy_parse_needed(&self) -> bool {
        !self.origin_prepared && !self.statement_name.is_empty()
    }
}

/// Parse/Bind/Describe messages accumulated toward the next Execute. Sealed
/// into an [`ExecuteEntry`] when an Execute arrives.
#[derive(Default)]
pub(in crate::proxy::connection) struct Segment {
    /// Raw bytes of the segment's messages, one refcounted slice per message in
    /// arrival order. Accumulating `Bytes` (zero-copy frozen from the codec
    /// split) avoids deep-copying every Parse/Bind/Describe into a contiguous
    /// buffer that is only ever needed on the (cold) forward path. Inline-stored
    /// (`MessageSlices`) so the common segment never heap-allocates.
    pub(in crate::proxy::connection) bytes: MessageSlices,
    /// Whether a Parse was buffered in this segment.
    pub(in crate::proxy::connection) has_parse: bool,
    /// Whether a Bind was buffered in this segment.
    pub(in crate::proxy::connection) has_bind: bool,
    /// Whether/what Describe was buffered in this segment.
    pub(in crate::proxy::connection) describe: PipelineDescribe,
    /// Statement name of each Parse in this segment, in order. One per Parse —
    /// a dirty segment has several. Drives `pending_parse_statements` on forward.
    pub(in crate::proxy::connection) parse_statement_names: SmallVec<[EcoString; 1]>,
    /// Statement name of each Describe('S') in this segment, in order.
    pub(in crate::proxy::connection) describe_statement_names: SmallVec<[EcoString; 1]>,
    /// True once the segment holds more than one of any Parse/Bind/Describe —
    /// i.e. more than one executable's worth of prep. Such a segment can't be
    /// served from cache (the worker synthesizes exactly one ParseComplete /
    /// BindComplete / Describe response), so it forces the forward path.
    pub(in crate::proxy::connection) dirty: bool,
}

/// One Execute plus the Parse/Bind/Describe messages that preceded it since the
/// previous Execute (or batch start). Sealed at Execute; carries its own bytes
/// so it can be dispatched independently (cached) or concatenated for forward.
pub(in crate::proxy::connection) struct ExecuteEntry {
    /// Raw bytes of this execute's Parse/Bind/Describe/Execute run, one
    /// refcounted slice per message in order.
    pub(in crate::proxy::connection) bytes: MessageSlices,
    /// Portal name from Execute (None if the Execute failed to parse).
    pub(in crate::proxy::connection) portal_name: Option<EcoString>,
    pub(in crate::proxy::connection) has_parse: bool,
    pub(in crate::proxy::connection) has_bind: bool,
    pub(in crate::proxy::connection) describe: PipelineDescribe,
    /// Statement name of each Parse / Describe('S') in this entry, in order.
    /// A clean (cacheable) entry has at most one of each.
    pub(in crate::proxy::connection) parse_statement_names: SmallVec<[EcoString; 1]>,
    pub(in crate::proxy::connection) describe_statement_names: SmallVec<[EcoString; 1]>,
    /// Carried from the segment: more than one P/B/D, so not cacheable.
    pub(in crate::proxy::connection) dirty: bool,
    /// Cacheable-query snapshot captured at Execute time, if this execute is a
    /// cacheable SELECT with a resolvable portal. `None` ⇒ not cacheable.
    pub(in crate::proxy::connection) candidate: Option<CacheCandidate>,
}

impl ExecuteEntry {
    /// Whether forwarding this entry to origin requires prepending a lazy Parse
    /// (Bind-without-Parse against a named statement origin doesn't yet know).
    pub(in crate::proxy::connection) fn needs_lazy_parse(&self) -> bool {
        !self.has_parse
            && self
                .candidate
                .as_ref()
                .is_some_and(CacheCandidate::lazy_parse_needed)
    }
}

/// Buffered extended protocol messages, accumulated until Sync/Flush.
/// All decision-making (cache vs. forward) is deferred to Sync/Flush time.
#[derive(Default)]
pub(in crate::proxy::connection) struct ExtendedBuffer {
    /// Sealed executes, in arrival order. One per Execute message.
    pub(in crate::proxy::connection) entries: SmallVec<[ExecuteEntry; 1]>,
    /// Messages accumulated since the last Execute (or batch start).
    pub(in crate::proxy::connection) pending: Segment,
}

impl ExtendedBuffer {
    /// Seal the pending segment together with this Execute's bytes into an entry.
    pub(in crate::proxy::connection) fn pending_seal(
        &mut self,
        execute_bytes: Bytes,
        portal_name: Option<EcoString>,
        candidate: Option<CacheCandidate>,
    ) {
        let mut seg = std::mem::take(&mut self.pending);
        seg.bytes.push(execute_bytes);
        self.entries.push(ExecuteEntry {
            bytes: seg.bytes,
            portal_name,
            has_parse: seg.has_parse,
            has_bind: seg.has_bind,
            describe: seg.describe,
            parse_statement_names: seg.parse_statement_names,
            describe_statement_names: seg.describe_statement_names,
            dirty: seg.dirty,
            candidate,
        });
    }

    /// Every Parse statement name across the window in wire order (entries then
    /// the trailing pending segment) — one per Parse. Drives the ordered
    /// `pending_parse_statements` queue so each origin ParseComplete marks the
    /// right statement `origin_prepared`.
    pub(in crate::proxy::connection) fn parse_statements_all(&self) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .flat_map(|e| e.parse_statement_names.iter())
            .chain(self.pending.parse_statement_names.iter())
            .map(EcoString::as_str)
    }

    /// Every Describe('S') statement name across the window in wire order.
    pub(in crate::proxy::connection) fn describe_statements_all(
        &self,
    ) -> impl Iterator<Item = &str> {
        self.entries
            .iter()
            .flat_map(|e| e.describe_statement_names.iter())
            .chain(self.pending.describe_statement_names.iter())
            .map(EcoString::as_str)
    }

    /// Concatenate all buffered bytes (entries in order, then the trailing
    /// pending segment) into the wire stream as originally received. Only the
    /// (cold) forward path needs the contiguous form.
    pub(in crate::proxy::connection) fn bytes_concat(&self) -> BytesMut {
        let mut out = BytesMut::new();
        for entry in &self.entries {
            for slice in &entry.bytes {
                out.extend_from_slice(slice);
            }
        }
        for slice in &self.pending.bytes {
            out.extend_from_slice(slice);
        }
        out
    }

    /// Whether any Parse was buffered across the whole window.
    pub(in crate::proxy::connection) fn any_has_parse(&self) -> bool {
        self.pending.has_parse || self.entries.iter().any(|e| e.has_parse)
    }
}

/// State for the extended query protocol pipeline.
/// Accumulates messages until Sync/Flush, then tracks pending origin responses
/// and pipeline context for cache dispatch.
pub(in crate::proxy::connection) struct ExtendedPending {
    /// Statement names whose ParseCompletes we're awaiting from origin, in wire
    /// order — one per forwarded Parse. Each origin ParseComplete pops the front
    /// and marks that statement `origin_prepared`.
    pub(in crate::proxy::connection) pending_parse_statements: VecDeque<EcoString>,

    /// Statement names being described, in wire order — one per forwarded
    /// Describe('S'). ParameterDescription peeks the front; RowDescription/NoData
    /// pops it.
    pub(in crate::proxy::connection) pending_describe_statements: VecDeque<EcoString>,

    /// Statement name to lazily Parse on the next origin forward. Set at Sync
    /// time for Bind-without-Parse batches against statements origin doesn't
    /// know; consumed by the forward paths in `handle_cache_reply`. Cleared on
    /// every Sync so stale state from a prior cache hit doesn't leak.
    pub(in crate::proxy::connection) pending_lazy_parse: Option<EcoString>,

    /// Buffered extended protocol messages accumulated until Sync/Flush.
    /// Decision-making deferred to Sync time.
    pub(in crate::proxy::connection) buffer: Option<ExtendedBuffer>,

    /// Pipeline context ready for cache dispatch.
    /// Built at Sync time from ExtendedBuffer, consumed by ProxyMessage.
    pub(in crate::proxy::connection) pipeline_context: Option<PipelineContext>,

    /// Remaining cache slots of a multi-execute batch, queued in order. The
    /// current in-flight slot lives in `pipeline_context` (+ the egress Cache
    /// slot); each hit advances to the next here. On a miss the remainder is
    /// forwarded to origin as one run.
    pub(in crate::proxy::connection) batch: VecDeque<DispatchContext>,

    /// Whether the in-flight cache dispatch is an extended-protocol pipeline (vs
    /// a self-terminating simple `Query`). Gates the synthesized trailing `Sync`
    /// on the forward-fallback path: extended entries carry no `Sync`, a simple
    /// `Query` already triggers its own `ReadyForQuery`.
    pub(in crate::proxy::connection) dispatch_is_extended: bool,

    /// Count of `Close(statement)` messages handled locally (statement never
    /// `origin_prepared`, so the origin never knew it) whose `CloseComplete` is
    /// still owed to the client. Synthesized — and the counter reset — at the
    /// next Sync (or before any origin forward, to preserve response order).
    /// PGC-234: avoids forwarding useless Close+Sync round-trips to origin for
    /// cache-served statements.
    pub(in crate::proxy::connection) deferred_close_completes: u32,

    /// Whether anything was forwarded to origin in the current Sync group (a
    /// forwarded Close or a Flush). Gates the bare-Sync local-`ReadyForQuery`
    /// optimization: only synthesize the RFQ when the group is purely local.
    pub(in crate::proxy::connection) group_origin_forwarded: bool,
}

/// Everything needed to dispatch one execute as a cache slot, computed at Sync
/// from an [`ExecuteEntry`]'s snapshot. Held in `ExtendedPending::batch` until
/// its turn; on dispatch the pipeline/statement state is applied to the
/// connection and the message is leased to a worker.
pub(in crate::proxy::connection) struct DispatchContext {
    pub(in crate::proxy::connection) msg: CacheMessage,
    pub(in crate::proxy::connection) pipeline: PipelineContext,
    pub(in crate::proxy::connection) fingerprint: Fingerprint,
    pub(in crate::proxy::connection) lazy_parse: Option<EcoString>,
    pub(in crate::proxy::connection) parse_statement: Option<EcoString>,
    pub(in crate::proxy::connection) describe_statement: Option<EcoString>,
}

impl DispatchContext {
    /// Assemble a dispatch context from an entry and its cache candidate.
    /// `is_last` carries the single trailing `ReadyForQuery` for the batch.
    pub(in crate::proxy::connection) fn build(
        entry: ExecuteEntry,
        candidate: CacheCandidate,
        is_last: bool,
    ) -> Self {
        let fingerprint = query_expr_fingerprint(&candidate.cacheable_query.query);
        let lazy_parse = (!entry.has_parse && candidate.lazy_parse_needed())
            .then(|| candidate.statement_name.clone());
        let pipeline = PipelineContext {
            buffered_bytes: entry.bytes,
            describe: entry.describe,
            parameter_description: candidate.parameter_description,
            has_parse: entry.has_parse,
            has_bind: entry.has_bind,
            emit_rfq: is_last,
        };
        let msg = CacheMessage::QueryParameterized(
            BytesMut::new(),
            candidate.cacheable_query,
            candidate.parameters,
            candidate.result_formats,
        );
        Self {
            msg,
            pipeline,
            fingerprint,
            lazy_parse,
            // A cacheable (non-dirty) entry has at most one Parse / Describe('S').
            parse_statement: if entry.has_parse {
                entry.parse_statement_names.into_iter().next()
            } else {
                None
            },
            describe_statement: entry.describe_statement_names.into_iter().next(),
        }
    }
}

impl ExtendedPending {
    pub(in crate::proxy::connection) fn new() -> Self {
        Self {
            pending_parse_statements: VecDeque::new(),
            pending_describe_statements: VecDeque::new(),
            pending_lazy_parse: None,
            buffer: None,
            pipeline_context: None,
            batch: VecDeque::new(),
            dispatch_is_extended: false,
            deferred_close_completes: 0,
            group_origin_forwarded: false,
        }
    }

    /// Get or create the ExtendedBuffer for accumulating messages.
    pub(in crate::proxy::connection) fn buffer_get_or_create(&mut self) -> &mut ExtendedBuffer {
        self.buffer.get_or_insert_with(ExtendedBuffer::default)
    }

    /// Take the buffer contents. Returns None if no buffer was active.
    pub(in crate::proxy::connection) fn buffer_take(&mut self) -> Option<ExtendedBuffer> {
        self.buffer.take()
    }

    /// Capture every forwarded Parse/Describe('S') statement name, in wire
    /// order, into the pending origin-response queues (shared by the flush and
    /// forward paths). Replaces any prior contents — this forwards a whole
    /// buffer, so the queues describe exactly its responses.
    pub(in crate::proxy::connection) fn pending_statements_capture(
        &mut self,
        buffer: &ExtendedBuffer,
    ) {
        self.pending_parse_statements =
            buffer.parse_statements_all().map(EcoString::from).collect();
        self.pending_describe_statements = buffer
            .describe_statements_all()
            .map(EcoString::from)
            .collect();
    }

    /// Flush any buffered extended protocol messages.
    /// Extracts pending statement names from buffer metadata.
    /// Returns the buffer's bytes for the caller to push to origin.
    pub(in crate::proxy::connection) fn buffer_flush(&mut self) -> Option<BytesMut> {
        let buffer = self.buffer.take()?;
        self.pending_statements_capture(&buffer);
        Some(buffer.bytes_concat())
    }

    /// Forward buffer to origin with trailing bytes (Sync or Flush).
    /// Extracts pending statement names from buffer metadata.
    /// Returns bytes to push to origin.
    pub(in crate::proxy::connection) fn buffer_forward(
        &mut self,
        buffer: ExtendedBuffer,
        trailing_bytes: &[u8],
    ) -> BytesMut {
        self.pending_statements_capture(&buffer);
        let mut bytes = buffer.bytes_concat();
        bytes.extend_from_slice(trailing_bytes);
        bytes
    }

    /// Handle ParseComplete from origin: mark the next awaited statement as
    /// origin_prepared (one ParseComplete per forwarded Parse, in order).
    pub(in crate::proxy::connection) fn parse_complete(
        &mut self,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) {
        if let Some(stmt_name) = self.pending_parse_statements.pop_front()
            && let Some(stmt) = prepared_statements.get_mut(stmt_name.as_str())
        {
            stmt.origin_prepared = true;
            trace!("origin_prepared set for statement '{}'", stmt_name);
        }
    }

    /// Update the front pending statement's parameter OIDs. Peeks (does not pop)
    /// the queue; the following `RowDescription` or `NoData` pops it.
    pub(in crate::proxy::connection) fn parameter_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) {
        if let Some(stmt_name) = self.pending_describe_statements.front()
            && let Ok(parsed) = parse_parameter_description(msg_data)
            && let Some(stmt) = prepared_statements.get_mut(stmt_name.as_str())
        {
            debug!(
                "updated statement '{}' with parameter OIDs {:?}",
                stmt_name, parsed.parameter_oids
            );
            stmt.parameter_oids = parsed.parameter_oids;
            stmt.parameter_description = Some(Bytes::copy_from_slice(msg_data));
        }
    }

    /// Store the raw RowDescription on the front pending statement and pop it.
    /// Returns the statement name so the caller can populate the per-connection
    /// describe cache.
    pub(in crate::proxy::connection) fn row_description_received(
        &mut self,
        msg_data: &BytesMut,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) -> Option<EcoString> {
        let stmt_name = self.pending_describe_statements.pop_front()?;
        let stmt = prepared_statements.get_mut(stmt_name.as_str())?;
        stmt.row_description = Some(Bytes::copy_from_slice(msg_data));
        stmt.describe_no_data = false;
        Some(stmt_name)
    }

    /// Record NoData (statement has no result columns, e.g. INSERT without
    /// RETURNING) on the front pending statement and pop it. Returns the
    /// statement name so the caller can populate the per-connection describe cache.
    pub(in crate::proxy::connection) fn no_data_received(
        &mut self,
        prepared_statements: &mut HashMap<EcoString, PreparedStatement>,
    ) -> Option<EcoString> {
        let stmt_name = self.pending_describe_statements.pop_front()?;
        let stmt = prepared_statements.get_mut(stmt_name.as_str())?;
        stmt.row_description = None;
        stmt.describe_no_data = true;
        Some(stmt_name)
    }

    /// Take pipeline context (for origin fallback or cache dispatch).
    pub(in crate::proxy::connection) fn pipeline_take(&mut self) -> Option<PipelineContext> {
        self.pipeline_context.take()
    }
}

impl ConnectionState {
    /// Flush any buffered extended protocol messages to origin.
    pub(in crate::proxy::connection) fn extended_buffer_flush_to_origin(&mut self) {
        if let Some(bytes) = self.extended.buffer_flush() {
            self.origin_write_buf.push_back(bytes);
        }
    }

    /// Forward an extended buffer to origin, appending the trailing message bytes (Sync or Flush).
    /// Records metrics for any Execute in the buffer.
    pub(in crate::proxy::connection) fn extended_buffer_forward_to_origin(
        &mut self,
        buffer: ExtendedBuffer,
        trailing_bytes: &[u8],
    ) {
        let mut lazy_parse_stmt: Option<EcoString> = None;
        if let Some(first) = buffer.entries.first() {
            crate::metrics::handles().query.uncacheable.increment(1);

            if let Some(portal_name) = &first.portal_name
                && let Some(portal) = self.portals.get(portal_name.as_str())
                && let Some(stmt) = self.prepared_statements.get(&portal.statement_name)
            {
                match &stmt.sql_type {
                    StatementType::NonSelect => {
                        crate::metrics::handles().query.unsupported.increment(1);
                    }
                    StatementType::ParseError => {
                        crate::metrics::handles().query.invalid.increment(1);
                    }
                    StatementType::Cacheable(_) | StatementType::UncacheableSelect => {}
                }
                if !buffer.any_has_parse() && !stmt.origin_prepared {
                    lazy_parse_stmt = Some(portal.statement_name.clone());
                }
            }
        }

        if let Some(stmt_name) = lazy_parse_stmt {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }

        let bytes = self.extended.buffer_forward(buffer, trailing_bytes);
        self.origin_dispatch(bytes, None);
    }

    /// Handle Parse message — analyze cacheability, store statement, buffer bytes.
    pub(in crate::proxy::connection) fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        // Freeze the codec's zero-copy slice up front so the parsed SQL can be
        // a refcounted view into the frame instead of a fresh String.
        let data = msg.data.freeze();
        if let Ok(parsed) = parse_parse_message(&data) {
            // Cacheability analysis is memoized in `cacheability_cache` (shared
            // with the simple-query path); a hit skips the parse/convert/classify
            // entirely. search_path mutation detection — which the inline parse
            // used to fold in — isn't captured by that cache, so it's replayed
            // for the non-SELECT statements that can mutate it (no piggyback for
            // extended; a standalone SHOW is issued via the lazy path on RFQ).
            let sql_type = match analyze(
                &parsed.sql,
                &mut self.cacheability_cache,
                &self.func_volatility,
            ) {
                Ok(Action::CacheCheck(ast)) => StatementType::Cacheable(ast),
                Ok(Action::Forward(ForwardReason::UncacheableSelect)) => {
                    StatementType::UncacheableSelect
                }
                Ok(Action::Forward(
                    ForwardReason::UnsupportedStatement | ForwardReason::Invalid,
                )) => {
                    self.search_path_parse_inspect(&parsed.sql);
                    StatementType::NonSelect
                }
                Err(_) => StatementType::ParseError,
            };

            let statement_name = parsed.statement_name.clone();
            self.statement_store(parsed, sql_type, data.clone());

            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.has_parse {
                seg.dirty = true;
            }
            seg.has_parse = true;
            seg.parse_statement_names.push(statement_name);
            seg.bytes.push(data);
            trace!("net: Parse buffered");
            return;
        }
        // Parse failed: forward raw. No views of `data` exist on this path, so
        // try_into_mut reclaims the buffer without copying.
        self.origin_write_buf.push_back(
            data.try_into_mut()
                .unwrap_or_else(|b| BytesMut::from(&b[..])),
        );
    }

    /// Handle Bind message — store portal, buffer bytes.
    pub(in crate::proxy::connection) fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_bind_message(&msg.data) {
            self.portal_store(parsed);

            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.has_bind {
                seg.dirty = true;
            }
            seg.has_bind = true;
            seg.bytes.push(msg.data.freeze());
            trace!("net: Bind buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Execute message — record metrics, parse portal name, buffer bytes.
    /// Decision-making deferred to Sync.
    pub(in crate::proxy::connection) fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
        let m = crate::metrics::handles();
        m.query.total.increment(1);
        m.conn.extended_queries.increment(1);
        self.telemetry.query_receive();

        let portal_name = parse_execute_message(&msg.data).ok().map(|p| p.portal_name);

        // Snapshot the cache candidate now, while the portal/statement still
        // reflect this execute's Bind/Parse (a later execute may rebind the
        // same — usually unnamed — portal). The current segment's Describe
        // governs the ParameterDescription requirement.
        let describe = self
            .extended
            .buffer
            .as_ref()
            .map_or(PipelineDescribe::None, |b| b.pending.describe);
        let candidate = self.execute_cache_candidate(portal_name.as_deref(), describe);

        self.extended.buffer_get_or_create().pending_seal(
            msg.data.freeze(),
            portal_name,
            candidate,
        );
        trace!("net: Execute buffered");
    }

    /// Snapshot a cacheable-query candidate for the portal an Execute targets.
    /// Returns None when the portal/statement doesn't resolve to a cacheable
    /// SELECT with uniform result formats (and, for Describe('S'), a cached
    /// ParameterDescription). Global cache gating is checked separately at Sync.
    pub(in crate::proxy::connection) fn execute_cache_candidate(
        &self,
        portal_name: Option<&str>,
        describe: PipelineDescribe,
    ) -> Option<CacheCandidate> {
        let portal = self.portals.get(portal_name?)?;

        // Only handle implicit or uniform result formats
        if let ResultFormats::PerColumn(_) = portal.result_formats {
            trace!("result format is not implicit or uniform");
            return None;
        }

        let stmt = self.prepared_statements.get(&portal.statement_name)?;

        let cacheable_query = match &stmt.sql_type {
            StatementType::Cacheable(query) => Arc::clone(query),
            StatementType::NonSelect
            | StatementType::UncacheableSelect
            | StatementType::ParseError => return None,
        };

        // Describe('S'): require a cached parameter_description
        if describe == PipelineDescribe::Statement && stmt.parameter_description.is_none() {
            return None;
        }

        Some(CacheCandidate {
            cacheable_query,
            parameters: QueryParameters {
                values: portal.parameter_values.clone(),
                formats: portal.parameter_formats.clone(),
                oids: stmt.parameter_oids.clone(),
            },
            result_formats: portal.result_formats.clone(),
            parameter_description: if describe == PipelineDescribe::Statement {
                stmt.parameter_description.clone()
            } else {
                None
            },
            statement_name: portal.statement_name.clone(),
            origin_prepared: stmt.origin_prepared,
        })
    }

    /// Handle Describe message — buffer bytes and track describe metadata.
    pub(in crate::proxy::connection) fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_describe_message(&msg.data) {
            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.describe != PipelineDescribe::None {
                seg.dirty = true;
            }

            match parsed.describe_type {
                b'S' => {
                    seg.describe = PipelineDescribe::Statement;
                    seg.describe_statement_names.push(parsed.name);
                }
                b'P' => {
                    seg.describe = PipelineDescribe::Portal;
                }
                _ => {}
            }

            seg.bytes.push(msg.data.freeze());
            trace!("net: Describe buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Emit any deferred `CloseComplete`s (PGC-234: locally-handled Closes of
    /// statements the origin never prepared) as one ordered synth slot, so they
    /// keep their place ahead of whatever origin/cache response follows. Returns
    /// the count flushed.
    pub(in crate::proxy::connection) fn deferred_close_completes_flush(&mut self) -> u32 {
        let n = self.extended.deferred_close_completes;
        if n == 1 {
            self.extended.deferred_close_completes = 0;
            self.egress
                .synth_push(Bytes::from_static(CLOSE_COMPLETE_MSG));
        } else if n > 1 {
            self.extended.deferred_close_completes = 0;
            let mut out = BytesMut::with_capacity(CLOSE_COMPLETE_MSG.len() * n as usize);
            for _ in 0..n {
                out.put_slice(CLOSE_COMPLETE_MSG);
            }
            self.egress.synth_push(out.freeze());
        }
        n
    }

    /// Handle Close message. A `Close(statement)` for a statement that was served
    /// from cache and never prepared on the origin (`origin_prepared == false`)
    /// is handled locally — the origin never knew it, so forwarding the Close (and
    /// its paired Sync) is a useless round-trip (PGC-234). We defer the
    /// `CloseComplete` (synthesized at the next Sync) and leave origin untouched.
    /// Everything else forwards as before: origin-prepared statements, portals, a
    /// Close mid-batch (`buffer` present), or once anything has already been
    /// forwarded this group (so deferred completions can't reorder ahead of it).
    pub(in crate::proxy::connection) fn handle_close_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_close_message(&msg.data) {
            if parsed.close_type == b'S'
                && self.extended.buffer.is_none()
                && !self.extended.group_origin_forwarded
                && self
                    .prepared_statements
                    .get(parsed.name.as_str())
                    .is_some_and(|s| !s.origin_prepared)
            {
                self.statement_close(&parsed.name);
                self.extended.deferred_close_completes += 1;
                crate::metrics::handles().conn.close_local.increment(1);
                return;
            }
            self.deferred_close_completes_flush();
            self.extended_buffer_flush_to_origin();
            match parsed.close_type {
                b'S' => self.statement_close(&parsed.name),
                b'P' => self.portal_close(&parsed.name),
                _ => {}
            }
        } else {
            self.deferred_close_completes_flush();
            self.extended_buffer_flush_to_origin();
        }
        self.extended.group_origin_forwarded = true;
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Sync message — all cache vs. forward decision-making happens here.
    ///
    /// If every Execute in the batch is an independently cacheable read, each is
    /// dispatched as its own cache slot (in order). Otherwise the batch is
    /// synthesized (Parse-only) or forwarded whole to origin.
    pub(in crate::proxy::connection) fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
        // Emit any deferred CloseCompletes (locally-handled Closes) as an ordered
        // synth slot before this Sync's responses (PGC-234).
        let local_closes = self.deferred_close_completes_flush();
        let group_origin_forwarded = self.extended.group_origin_forwarded;
        self.extended.group_origin_forwarded = false;

        let Some(buffer) = self.extended.buffer_take() else {
            // Bare Sync. If this group only handled Closes locally (nothing was
            // forwarded to origin), the origin has no pending work to ack —
            // synthesize the ReadyForQuery instead of a useless round-trip.
            if local_closes > 0 && !group_origin_forwarded {
                trace!("net: bare Sync → synth ReadyForQuery (local closes only)");
                self.egress
                    .synth_push(Bytes::from_static(READY_FOR_QUERY_IDLE_MSG));
            } else {
                trace!("net: proxy→origin Sync (no buffer)");
                self.egress.origin_open();
                self.origin_write_buf.push_back(msg.data);
            }
            return;
        };

        if self.cache_batch_eligible(&buffer) {
            let entries = buffer.entries;
            trace!("net: Sync → cache batch ({} executes)", entries.len());
            self.cache_batch_dispatch(entries);
        } else if self.try_synthesize_parse_describe_response(&buffer) {
            trace!("net: Sync → synthesized ParseComplete+Describe response");
        } else {
            self.extended_buffer_forward_to_origin(buffer, &msg.data);
            trace!("net: Sync → origin (forwarded buffer)");
        }
    }

    /// Whether every Execute in the batch is an independently cacheable read, so
    /// the whole batch can be served as a sequence of cache slots. Requires a
    /// clean `[P?][B?][D?] E` shape per entry, no trailing prep, global cache
    /// gating, and at most one entry needing a lazy Parse on forward (the
    /// single-intercept forward path can absorb only one).
    pub(in crate::proxy::connection) fn cache_batch_eligible(
        &self,
        buffer: &ExtendedBuffer,
    ) -> bool {
        !buffer.entries.is_empty()
            && buffer.pending.bytes.is_empty()
            && self.cache_globally_enabled()
            && buffer
                .entries
                .iter()
                .all(|e| !e.dirty && e.candidate.is_some())
            && buffer
                .entries
                .iter()
                .filter(|e| e.needs_lazy_parse())
                .count()
                <= 1
    }

    /// Build a dispatch context per entry, queue them, and begin the first slot.
    /// Caller guarantees [`Self::cache_batch_eligible`] (every entry has a
    /// candidate and the list is non-empty).
    pub(in crate::proxy::connection) fn cache_batch_dispatch(
        &mut self,
        entries: SmallVec<[ExecuteEntry; 1]>,
    ) {
        // Common case: a single Parse/Bind/Describe/Execute. Begin it directly
        // without allocating a batch queue (the trailing-most slots empty).
        if entries.len() == 1 {
            let mut entry = entries.into_iter().next().expect("one entry");
            if let Some(candidate) = entry.candidate.take() {
                self.extended.batch.clear();
                self.cache_slot_begin(DispatchContext::build(entry, candidate, true));
                self.proxy_mode = ProxyMode::OriginDrain;
            }
            return;
        }
        let last = entries.len() - 1;
        let mut contexts = VecDeque::with_capacity(entries.len());
        for (i, mut entry) in entries.into_iter().enumerate() {
            // Eligibility guarantees a candidate; skip defensively rather than
            // panic if that invariant is ever violated.
            let Some(candidate) = entry.candidate.take() else {
                continue;
            };
            contexts.push_back(DispatchContext::build(entry, candidate, i == last));
        }
        if let Some(first) = contexts.pop_front() {
            self.extended.batch = contexts;
            self.cache_slot_begin(first);
            self.proxy_mode = ProxyMode::OriginDrain;
        }
    }

    /// Apply a dispatch context as the current in-flight cache slot: stamp
    /// timing, install pipeline + forward-fallback state, and push the egress
    /// Cache slot.
    pub(in crate::proxy::connection) fn cache_slot_begin(&mut self, ctx: DispatchContext) {
        self.telemetry.cache_timing_start(ctx.fingerprint);
        self.extended.dispatch_is_extended = true;
        self.extended.pipeline_context = Some(ctx.pipeline);
        // Reset the awaited-response queues to just this slot's statement(s);
        // on a miss `batch_remaining_forward` appends the rest in order.
        self.extended.pending_parse_statements.clear();
        self.extended
            .pending_parse_statements
            .extend(ctx.parse_statement);
        self.extended.pending_describe_statements.clear();
        self.extended
            .pending_describe_statements
            .extend(ctx.describe_statement);
        self.extended.pending_lazy_parse = ctx.lazy_parse;
        self.egress.cache_push(ctx.msg);
    }

    /// Advance the batch after a cache hit: begin the next queued slot (staying
    /// in `OriginDrain`) or, when the batch is exhausted, return to `Read`.
    pub(in crate::proxy::connection) fn cache_batch_advance(&mut self) {
        if let Some(next) = self.extended.batch.pop_front() {
            self.cache_slot_begin(next);
            self.proxy_mode = ProxyMode::OriginDrain;
        } else {
            self.proxy_mode = ProxyMode::Read;
        }
    }

    /// Forward the remaining batch entries (each without a Sync) followed by one
    /// synthesized `Sync`, so origin runs them in a single implicit transaction
    /// and emits exactly one ReadyForQuery. Installs a lazy Parse for any entry
    /// that needs one (eligibility bounds this to at most one across the batch).
    pub(in crate::proxy::connection) fn batch_remaining_forward(&mut self) {
        while let Some(next) = self.extended.batch.pop_front() {
            if let Some(stmt_name) = next.lazy_parse {
                forward_lazy_parse_install(
                    &stmt_name,
                    &self.prepared_statements,
                    &mut self.origin_write_buf,
                    &mut self.origin_intercept,
                );
            }
            // Track this entry's awaited ParseComplete / Describe responses so
            // they mark the right statement origin_prepared, in order.
            self.extended
                .pending_parse_statements
                .extend(next.parse_statement);
            self.extended
                .pending_describe_statements
                .extend(next.describe_statement);
            self.origin_write_buf
                .push_back(slices_concat(&next.pipeline.buffered_bytes));
        }
        // A simple `Query` is self-terminating (origin emits its own RFQ);
        // only extended-pipeline entries need a synthesized Sync to close the
        // implicit transaction and produce the single trailing RFQ.
        if self.extended.dispatch_is_extended {
            self.origin_write_buf
                .push_back(BytesMut::from(SYNC_MESSAGE.as_slice()));
        }
    }

    /// Return the named statement targeted by a Parse-only / Parse+Describe('S')
    /// Sync batch that's eligible for synthesize. `None` if the batch shape,
    /// statement state, or session state disqualifies it.
    ///
    /// In-transaction is excluded because a statement Parsed mid-txn would
    /// resolve against the txn's snapshot. Portal Describe is excluded
    /// because no portal exists without a Bind. Unnamed statements are
    /// excluded because origin's unnamed slot is one-shot per Sync.
    pub(in crate::proxy::connection) fn synth_eligible<'a>(
        &self,
        buffer: &'a ExtendedBuffer,
    ) -> Option<&'a str> {
        // Synthesize only applies to a Parse-only batch: no Execute (no entries),
        // a Parse but no Bind in the pending segment.
        if !buffer.entries.is_empty() {
            return None;
        }
        let seg = &buffer.pending;
        // A dirty segment holds more than one Parse/Describe — synth produces
        // exactly one response, so it must forward instead.
        if !seg.has_parse || seg.has_bind || seg.dirty {
            return None;
        }
        if seg.describe == PipelineDescribe::Portal {
            return None;
        }
        if self.in_transaction {
            return None;
        }
        let stmt_name = seg.parse_statement_names.first().map(EcoString::as_str)?;
        if stmt_name.is_empty() {
            return None;
        }
        let stmt = self.prepared_statements.get(stmt_name)?;
        if !matches!(stmt.sql_type, StatementType::Cacheable(_)) {
            return None;
        }
        Some(stmt_name)
    }

    /// Attempt to serve a `Parse+Describe('S')+Sync` (or `Parse+Sync`) batch
    /// from the per-connection describe-response cache. Returns `true` on
    /// hit, in which case the synthesized response was pushed (or deferred)
    /// and the caller must not forward to origin. Returns `false` on miss
    /// or ineligible batch — caller falls through to the normal forward.
    pub(in crate::proxy::connection) fn try_synthesize_parse_describe_response(
        &mut self,
        buffer: &ExtendedBuffer,
    ) -> bool {
        let Some(stmt_name) = self.synth_eligible(buffer) else {
            return false;
        };
        // synth_eligible already verified the statement exists.
        let Some(stmt) = self.prepared_statements.get(stmt_name) else {
            return false;
        };
        let key = DescribeKey {
            sql: stmt.sql.clone(),
            parameter_oids: stmt.client_parameter_oids.clone(),
        };
        let Some(entry) = self.describe_cache.get(&key) else {
            crate::metrics::handles().conn.describe_misses.increment(1);
            return false;
        };
        // Cheap (refcount) clones now that the describe metadata is `Bytes`.
        let parameter_description = entry.parameter_description.clone();
        let row_description = entry.row_description.clone();
        let parameter_oids = entry.parameter_oids.clone();
        let describe_response = entry.describe_response.clone();
        // `stmt_name` borrows `buffer` (aliases `self.extended`); detach it as an
        // EcoString (inline for the short statement names clients use) so the
        // `&mut self` populate below doesn't conflict with that borrow.
        let stmt_name = EcoString::from(stmt_name);

        // Populate the freshly-Parsed statement with the cached Describe
        // metadata so a subsequent Bind+Execute can build a parameterized
        // cache message without an origin round-trip.
        if let Some(stmt_mut) = self.prepared_statements.get_mut(stmt_name.as_str()) {
            if let Some(oids) = parameter_oids {
                stmt_mut.parameter_oids = oids;
            }
            stmt_mut.parameter_description = Some(parameter_description);
            stmt_mut.describe_no_data = row_description.is_none();
            stmt_mut.row_description = row_description;
        }

        crate::metrics::handles().conn.describe_hits.increment(1);

        let out = if buffer.pending.describe == PipelineDescribe::Statement {
            describe_response
        } else {
            Bytes::from_static(PARSE_COMPLETE_RFQ_IDLE)
        };

        // Enqueue as an ordered slot: the egress queue keeps it behind any
        // earlier in-flight origin response so the synth bytes can't jump ahead.
        self.egress.synth_push(out);

        true
    }

    /// Handle Flush message — forward buffer to origin, no cache attempt.
    /// Handles JDBC pattern: Parse/Bind/Describe/Flush then Execute/Sync.
    pub(in crate::proxy::connection) fn handle_flush_message(&mut self, msg: PgFrontendMessage) {
        // Anything reaching origin must come after any deferred CloseCompletes,
        // and marks the group as having origin work (PGC-234).
        self.deferred_close_completes_flush();
        self.extended.group_origin_forwarded = true;
        let Some(buffer) = self.extended.buffer_take() else {
            self.origin_write_buf.push_back(msg.data);
            return;
        };

        self.extended_buffer_forward_to_origin(buffer, &msg.data);
        // The forwarded batch ends in Flush, not Sync — origin will send the
        // describe response with no ReadyForQuery. Mark the opened egress slot
        // so the next client message seals it (see `handle_client_message`).
        self.flush_describe_pending = true;
        trace!("net: Flush → origin (forwarded buffer)");
    }

    /// Store a prepared statement in connection state.
    ///
    /// For unnamed statements (empty name), always overwrite — the protocol allows reuse of
    /// the unnamed slot with a new Parse. For named statements, `or_insert` preserves existing
    /// metadata (parameter_description, origin_prepared) accumulated during the cold path.
    pub(in crate::proxy::connection) fn statement_store(
        &mut self,
        parsed: ParsedParseMessage,
        sql_type: StatementType,
        parse_bytes: Bytes,
    ) {
        let client_parameter_oids = parsed.parameter_oids.clone();
        let stmt = PreparedStatement {
            name: parsed.statement_name.clone(),
            sql: parsed.sql,
            parameter_oids: parsed.parameter_oids,
            client_parameter_oids,
            sql_type,
            parameter_description: None,
            row_description: None,
            describe_no_data: false,
            origin_prepared: false,
            parse_bytes: Some(parse_bytes),
        };
        debug!("parsed statement insert {}", parsed.statement_name);

        if parsed.statement_name.is_empty() {
            // Unnamed statement: always overwrite per protocol spec
            self.prepared_statements.insert(parsed.statement_name, stmt);
        } else {
            // Named statement: preserve existing metadata from first cold path
            if !self
                .prepared_statements
                .contains_key(&parsed.statement_name)
            {
                crate::metrics::handles()
                    .conn
                    .prepared_statements
                    .increment(1.0);
            }
            self.prepared_statements
                .entry(parsed.statement_name)
                .or_insert(stmt);
        }
    }

    /// Store a portal in connection state.
    pub(in crate::proxy::connection) fn portal_store(&mut self, parsed: ParsedBindMessage) {
        let portal = Portal {
            name: parsed.portal_name.clone(),
            statement_name: parsed.statement_name,
            parameter_values: parsed.parameter_values,
            parameter_formats: parsed.parameter_formats,
            result_formats: parsed.result_formats,
        };

        debug!("parsed portal insert {:?}", portal);
        self.portals.insert(parsed.portal_name, portal);
    }

    /// Remove a prepared statement from connection state.
    pub(in crate::proxy::connection) fn statement_close(&mut self, name: &str) {
        if self.prepared_statements.remove(name).is_some() {
            crate::metrics::handles()
                .conn
                .prepared_statements
                .decrement(1.0);
        }
    }

    /// Remove a portal from connection state.
    pub(in crate::proxy::connection) fn portal_close(&mut self, name: &str) {
        self.portals.remove(name);
    }

    /// Clear all prepared statements from connection state.
    #[expect(unused)]
    pub(in crate::proxy::connection) fn statements_clear(&mut self) {
        self.prepared_statements.clear();
    }

    /// Clear all portals from connection state.
    #[expect(unused)]
    pub(in crate::proxy::connection) fn portals_clear(&mut self) {
        self.portals.clear();
    }
}
