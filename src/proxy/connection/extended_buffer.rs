//! The extended-protocol buffer a connection accumulates until Sync/Flush,
//! the per-execute snapshots it seals, and the origin-response bookkeeping
//! for what it forwards.

use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;
use tokio_util::bytes::{Bytes, BytesMut};
use tracing::{debug, trace};

use crate::cache::messages::{MessageSlices, PipelineContext, PipelineDescribe};
use crate::cache::query::CacheableQuery;
use crate::cache::{CacheMessage, QueryParameters};
use crate::pg::protocol::extended::parse_parameter_description;
use crate::pg::protocol::session::{PreparedStatement, ResultFormats};
use crate::query::Fingerprint;
use crate::query::ast::query_expr_fingerprint;
use crate::query::write::StatementEffects;

/// Each Execute's effects snapshot, in batch order, for a forwarded buffer.
/// Captured at Execute time (`execute_effects`), not resolved here: by
/// Sync/Flush a later execute in the batch has usually rebound the same —
/// typically unnamed — portal, so the live portal map reflects only the
/// *last* Bind's values (PGC-445).
pub(super) fn buffer_effects(buffer: &ExtendedBuffer) -> SmallVec<[StatementEffects; 1]> {
    buffer
        .entries
        .iter()
        .map(|entry| entry.effects.clone())
        .collect()
}

/// A cacheable-query snapshot captured at Execute time. Taken eagerly (not at
/// Sync) because a multi-execute batch typically reuses the unnamed portal /
/// statement, so `self.portals` / `prepared_statements` reflect only the *last*
/// Bind/Parse by Sync time. Snapshotting per entry keeps each execute's own
/// parameters and query.
pub(super) struct CacheCandidate {
    pub(super) cacheable_query: Arc<CacheableQuery>,
    pub(super) parameters: QueryParameters,
    pub(super) result_formats: ResultFormats,
    /// ParameterDescription bytes, present only for a Describe('S') entry.
    pub(super) parameter_description: Option<Bytes>,
    /// Target statement name (for the lazy-Parse-on-forward decision).
    pub(super) statement_name: EcoString,
    /// Whether origin already knows the statement (no lazy Parse needed).
    pub(super) origin_prepared: bool,
}

impl CacheCandidate {
    /// Whether forwarding a Bind-without-Parse execute against this candidate
    /// requires prepending a lazy Parse (origin doesn't know the named
    /// statement). Combine with the entry's `has_parse` at the call site.
    pub(super) fn lazy_parse_needed(&self) -> bool {
        !self.origin_prepared && !self.statement_name.is_empty()
    }
}

/// Parse/Bind/Describe messages accumulated toward the next Execute. Sealed
/// into an [`ExecuteEntry`] when an Execute arrives.
#[derive(Default)]
pub(super) struct Segment {
    /// Raw bytes of the segment's messages, one refcounted slice per message in
    /// arrival order. Accumulating `Bytes` (zero-copy frozen from the codec
    /// split) avoids deep-copying every Parse/Bind/Describe into a contiguous
    /// buffer that is only ever needed on the (cold) forward path. Inline-stored
    /// (`MessageSlices`) so the common segment never heap-allocates.
    pub(super) bytes: MessageSlices,
    /// Whether a Parse was buffered in this segment.
    pub(super) has_parse: bool,
    /// Whether a Bind was buffered in this segment.
    pub(super) has_bind: bool,
    /// Whether/what Describe was buffered in this segment.
    pub(super) describe: PipelineDescribe,
    /// Statement name of each Parse in this segment, in order. One per Parse —
    /// a dirty segment has several. Drives `pending_parse_statements` on forward.
    pub(super) parse_statement_names: SmallVec<[EcoString; 1]>,
    /// Statement name of each Describe('S') in this segment, in order.
    pub(super) describe_statement_names: SmallVec<[EcoString; 1]>,
    /// True once the segment holds more than one of any Parse/Bind/Describe —
    /// i.e. more than one executable's worth of prep. Such a segment can't be
    /// served from cache (the worker synthesizes exactly one ParseComplete /
    /// BindComplete / Describe response), so it forces the forward path.
    pub(super) dirty: bool,
}

impl Segment {
    /// The statement a Parse-only (or Parse + Describe('S')) segment targets,
    /// when its shape allows a synthesized response: exactly one Parse, no
    /// Bind (no portal exists), no portal Describe, and a named statement —
    /// origin's unnamed slot is one-shot per Sync.
    pub(super) fn synth_parse_target(&self) -> Option<&str> {
        let one_parse = self.has_parse && !self.dirty;
        let no_portal = !self.has_bind && self.describe != PipelineDescribe::Portal;
        if !(one_parse && no_portal) {
            return None;
        }
        self.parse_statement_names
            .first()
            .map(EcoString::as_str)
            .filter(|name| !name.is_empty())
    }
}

/// What an Execute snapshots at arrival (see [`ExecuteEntry`]).
pub(super) struct ExecuteSnapshot {
    /// Portal name from Execute (None if the Execute failed to parse).
    pub(super) portal_name: Option<EcoString>,
    pub(super) candidate: Option<CacheCandidate>,
    pub(super) effects: StatementEffects,
}

/// One Execute plus the Parse/Bind/Describe messages that preceded it since the
/// previous Execute (or batch start). Sealed at Execute; carries its own bytes
/// so it can be dispatched independently (cached) or concatenated for forward.
pub(super) struct ExecuteEntry {
    /// The sealed segment; its `bytes` end with the Execute message. A clean
    /// (cacheable) entry has at most one Parse / Describe('S').
    pub(super) prep: Segment,
    /// Portal name from Execute (None if the Execute failed to parse).
    pub(super) portal_name: Option<EcoString>,
    /// Cacheable-query snapshot captured at Execute time, if this execute is a
    /// cacheable SELECT with a resolvable portal. `None` ⇒ not cacheable.
    pub(super) candidate: Option<CacheCandidate>,
    /// What forwarding this execute does to the connection's tracked state,
    /// captured at Execute time with this execute's own bind values — the same
    /// rebind hazard `candidate` documents (PGC-445). An unresolvable
    /// portal/statement snapshots the conservative unknown effects.
    pub(super) effects: StatementEffects,
}

impl ExecuteEntry {
    /// Whether forwarding this entry to origin requires prepending a lazy Parse
    /// (Bind-without-Parse against a named statement origin doesn't yet know).
    pub(super) fn needs_lazy_parse(&self) -> bool {
        !self.prep.has_parse
            && self
                .candidate
                .as_ref()
                .is_some_and(CacheCandidate::lazy_parse_needed)
    }
}

/// Buffered extended protocol messages, accumulated until Sync/Flush.
/// All decision-making (cache vs. forward) is deferred to Sync/Flush time.
#[derive(Default)]
pub(super) struct ExtendedBuffer {
    /// Sealed executes, in arrival order. One per Execute message.
    pub(super) entries: SmallVec<[ExecuteEntry; 1]>,
    /// Messages accumulated since the last Execute (or batch start).
    pub(super) pending: Segment,
}

impl ExtendedBuffer {
    /// Seal the pending segment together with this Execute's bytes into an
    /// entry, starting a fresh segment for the next Execute.
    pub(super) fn pending_seal(&mut self, execute_bytes: Bytes, snapshot: ExecuteSnapshot) {
        let mut prep = std::mem::take(&mut self.pending);
        prep.bytes.push(execute_bytes);
        self.entries.push(ExecuteEntry {
            prep,
            portal_name: snapshot.portal_name,
            candidate: snapshot.candidate,
            effects: snapshot.effects,
        });
    }

    /// Every segment in wire order: the sealed entries', then the trailing
    /// pending one.
    fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.entries
            .iter()
            .map(|e| &e.prep)
            .chain(std::iter::once(&self.pending))
    }

    /// Every Parse statement name across the window in wire order (entries then
    /// the trailing pending segment) — one per Parse. Drives the ordered
    /// `pending_parse_statements` queue so each origin ParseComplete marks the
    /// right statement `origin_prepared`.
    pub(super) fn parse_statements_all(&self) -> impl Iterator<Item = &str> {
        self.segments()
            .flat_map(|s| s.parse_statement_names.iter())
            .map(EcoString::as_str)
    }

    /// Every Describe('S') statement name across the window in wire order.
    pub(super) fn describe_statements_all(&self) -> impl Iterator<Item = &str> {
        self.segments()
            .flat_map(|s| s.describe_statement_names.iter())
            .map(EcoString::as_str)
    }

    /// Concatenate all buffered bytes (entries in order, then the trailing
    /// pending segment) into the wire stream as originally received. Only the
    /// (cold) forward path needs the contiguous form.
    pub(super) fn bytes_concat(&self) -> BytesMut {
        let mut out = BytesMut::new();
        for slice in self.segments().flat_map(|s| s.bytes.iter()) {
            out.extend_from_slice(slice);
        }
        out
    }

    /// Whether any Parse was buffered across the whole window.
    pub(super) fn any_has_parse(&self) -> bool {
        self.segments().any(|s| s.has_parse)
    }
}

/// State for the extended query protocol pipeline.
/// Accumulates messages until Sync/Flush, then tracks pending origin responses
/// and pipeline context for cache dispatch.
pub(super) struct ExtendedPending {
    /// Statement names whose ParseCompletes we're awaiting from origin, in wire
    /// order — one per forwarded Parse. Each origin ParseComplete pops the front
    /// and marks that statement `origin_prepared`.
    pub(super) pending_parse_statements: VecDeque<EcoString>,

    /// Statement names being described, in wire order — one per forwarded
    /// Describe('S'). ParameterDescription peeks the front; RowDescription/NoData
    /// pops it.
    pub(super) pending_describe_statements: VecDeque<EcoString>,

    /// Statement name to lazily Parse on the next origin forward. Set at Sync
    /// time for Bind-without-Parse batches against statements origin doesn't
    /// know; consumed by the forward paths in `handle_cache_reply`. Cleared on
    /// every Sync so stale state from a prior cache hit doesn't leak.
    pub(super) pending_lazy_parse: Option<EcoString>,

    /// Buffered extended protocol messages accumulated until Sync/Flush.
    /// Decision-making deferred to Sync time.
    pub(super) buffer: Option<ExtendedBuffer>,

    /// Pipeline context ready for cache dispatch.
    /// Built at Sync time from ExtendedBuffer, consumed by ProxyMessage.
    pub(super) pipeline_context: Option<PipelineContext>,

    /// Remaining cache slots of a multi-execute batch, queued in order. The
    /// current in-flight slot lives in `pipeline_context` (+ the egress Cache
    /// slot); each hit advances to the next here. On a miss the remainder is
    /// forwarded to origin as one run.
    pub(super) batch: VecDeque<DispatchContext>,

    /// Whether the in-flight cache dispatch is an extended-protocol pipeline (vs
    /// a self-terminating simple `Query`). Gates the synthesized trailing `Sync`
    /// on the forward-fallback path: extended entries carry no `Sync`, a simple
    /// `Query` already triggers its own `ReadyForQuery`.
    pub(super) dispatch_is_extended: bool,

    /// Count of `Close(statement)` messages handled locally (statement never
    /// `origin_prepared`, so the origin never knew it) whose `CloseComplete` is
    /// still owed to the client. Synthesized — and the counter reset — at the
    /// next Sync (or before any origin forward, to preserve response order).
    /// PGC-234: avoids forwarding useless Close+Sync round-trips to origin for
    /// cache-served statements.
    pub(super) deferred_close_completes: u32,

    /// Whether anything was forwarded to origin in the current Sync group (a
    /// forwarded Close or a Flush). Gates the bare-Sync local-`ReadyForQuery`
    /// optimization: only synthesize the RFQ when the group is purely local.
    pub(super) group_origin_forwarded: bool,
}

/// Everything needed to dispatch one execute as a cache slot, computed at Sync
/// from an [`ExecuteEntry`]'s snapshot. Held in `ExtendedPending::batch` until
/// its turn; on dispatch the pipeline/statement state is applied to the
/// connection and the message is leased to a worker.
pub(super) struct DispatchContext {
    pub(super) msg: CacheMessage,
    pub(super) pipeline: PipelineContext,
    pub(super) fingerprint: Fingerprint,
    pub(super) lazy_parse: Option<EcoString>,
    pub(super) parse_statement: Option<EcoString>,
    pub(super) describe_statement: Option<EcoString>,
}

impl DispatchContext {
    /// Assemble a dispatch context from an entry and its cache candidate.
    /// `is_last` carries the single trailing `ReadyForQuery` for the batch.
    pub(super) fn build(entry: ExecuteEntry, candidate: CacheCandidate, is_last: bool) -> Self {
        let prep = entry.prep;
        let fingerprint = query_expr_fingerprint(candidate.cacheable_query.query());
        let lazy_parse = (!prep.has_parse && candidate.lazy_parse_needed())
            .then(|| candidate.statement_name.clone());
        let pipeline = PipelineContext {
            buffered_bytes: prep.bytes,
            describe: prep.describe,
            parameter_description: candidate.parameter_description,
            has_parse: prep.has_parse,
            has_bind: prep.has_bind,
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
            parse_statement: if prep.has_parse {
                prep.parse_statement_names.into_iter().next()
            } else {
                None
            },
            describe_statement: prep.describe_statement_names.into_iter().next(),
        }
    }
}

impl ExtendedPending {
    pub(super) fn new() -> Self {
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
    pub(super) fn buffer_get_or_create(&mut self) -> &mut ExtendedBuffer {
        self.buffer.get_or_insert_with(ExtendedBuffer::default)
    }

    /// Take the buffer contents. Returns None if no buffer was active.
    pub(super) fn buffer_take(&mut self) -> Option<ExtendedBuffer> {
        self.buffer.take()
    }

    /// Borrow the active buffer, if any — for inspecting entries before a flush.
    pub(super) fn buffer_peek(&self) -> Option<&ExtendedBuffer> {
        self.buffer.as_ref()
    }

    /// Capture every forwarded Parse/Describe('S') statement name, in wire
    /// order, into the pending origin-response queues (shared by the flush and
    /// forward paths). Replaces any prior contents — this forwards a whole
    /// buffer, so the queues describe exactly its responses.
    pub(super) fn pending_statements_capture(&mut self, buffer: &ExtendedBuffer) {
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
    pub(super) fn buffer_flush(&mut self) -> Option<BytesMut> {
        let buffer = self.buffer.take()?;
        self.pending_statements_capture(&buffer);
        Some(buffer.bytes_concat())
    }

    /// Forward buffer to origin with trailing bytes (Sync or Flush).
    /// Extracts pending statement names from buffer metadata.
    /// Returns bytes to push to origin.
    pub(super) fn buffer_forward(
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
    pub(super) fn parse_complete(
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
    pub(super) fn parameter_description_received(
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
    pub(super) fn row_description_received(
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
    pub(super) fn no_data_received(
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
    pub(super) fn pipeline_take(&mut self) -> Option<PipelineContext> {
        self.pipeline_context.take()
    }
}
