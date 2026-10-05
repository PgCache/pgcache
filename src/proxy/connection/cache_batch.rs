//! Serving an extended-protocol batch from the cache: one cache slot per
//! Execute, dispatched in order, with the remainder forwarded to origin as one
//! run on a miss.

use std::collections::VecDeque;

use smallvec::SmallVec;
use tokio_util::bytes::BytesMut;

use super::ConnectionState;
use super::extended_buffer::{DispatchContext, ExecuteEntry, ExtendedBuffer};
use super::forward_lazy_parse_install;
use crate::cache::messages::slices_concat;
use crate::proxy::{ProxyMode, ProxyStatus};

/// Wire bytes of a bare `Sync` message (`'S'` + length 4). Synthesized to close
/// origin's implicit transaction when a multi-execute cache batch falls back to
/// forwarding (the client's own `Sync` isn't replayed per entry).
const SYNC_MESSAGE: [u8; 5] = [b'S', 0, 0, 0, 4];

impl ConnectionState {
    /// Whether every Execute in the batch is an independently cacheable read, so
    /// the whole batch can be served as a sequence of cache slots. Requires a
    /// clean `[P?][B?][D?] E` shape per entry, no trailing prep, global cache
    /// gating, and at most one entry needing a lazy Parse on forward (the
    /// single-intercept forward path can absorb only one).
    pub(super) fn cache_batch_eligible(&self, buffer: &ExtendedBuffer) -> bool {
        !buffer.entries.is_empty()
            && buffer.pending.bytes.is_empty()
            && self.cache_dispatch_possible()
            && self.proxy_status == ProxyStatus::Normal
            && buffer
                .entries
                .iter()
                .all(|e| !e.prep.dirty && e.candidate.is_some())
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
    pub(super) fn cache_batch_dispatch(&mut self, entries: SmallVec<[ExecuteEntry; 1]>) {
        // Common case: a single Parse/Bind/Describe/Execute. Begin it directly
        // without allocating a batch queue (the trailing-most slots empty).
        if entries.len() == 1 {
            if let Some(mut entry) = entries.into_iter().next()
                && let Some(candidate) = entry.candidate.take()
            {
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
    pub(super) fn cache_slot_begin(&mut self, ctx: DispatchContext) {
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
    pub(super) fn cache_batch_advance(&mut self) {
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
    pub(super) fn batch_remaining_forward(&mut self) {
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
}
