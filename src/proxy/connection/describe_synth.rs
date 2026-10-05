//! Answering a Parse-only (or Parse + Describe('S')) batch from the
//! per-connection describe cache, without an origin round-trip.

use ecow::EcoString;
use tokio_util::bytes::Bytes;

use super::extended_buffer::ExtendedBuffer;
use super::{ConnectionState, DescribeKey};
use crate::cache::messages::PipelineDescribe;
use crate::pg::protocol::session::StatementType;

/// Synth response for a Parse-only batch (no Describe): ParseComplete + RFQ('I').
const PARSE_COMPLETE_RFQ_IDLE: &[u8] = &[b'1', 0, 0, 0, 4, b'Z', 0, 0, 0, 5, b'I'];

impl ConnectionState {
    /// Return the named statement targeted by a Parse-only / Parse+Describe('S')
    /// Sync batch that's eligible for synthesize. `None` if the batch shape,
    /// statement state, or session state disqualifies it.
    ///
    /// In-transaction is excluded because a statement Parsed mid-txn would
    /// resolve against the txn's snapshot. The batch shape is
    /// [`Segment::synth_parse_target`](super::extended_buffer::Segment::synth_parse_target),
    /// with no Execute at all.
    pub(super) fn synth_eligible<'a>(&self, buffer: &'a ExtendedBuffer) -> Option<&'a str> {
        if !buffer.entries.is_empty() || self.in_transaction() {
            return None;
        }
        let stmt_name = buffer.pending.synth_parse_target()?;
        let stmt = self.prepared_statements.get(stmt_name)?;
        matches!(stmt.sql_type, StatementType::Cacheable(_)).then_some(stmt_name)
    }

    /// Attempt to serve a `Parse+Describe('S')+Sync` (or `Parse+Sync`) batch
    /// from the per-connection describe-response cache. Returns `true` on
    /// hit, in which case the synthesized response was pushed (or deferred)
    /// and the caller must not forward to origin. Returns `false` on miss
    /// or ineligible batch — caller falls through to the normal forward.
    pub(super) fn try_synthesize_parse_describe_response(
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
}
