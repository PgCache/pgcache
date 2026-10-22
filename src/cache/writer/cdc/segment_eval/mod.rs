//! Batched evaluation for one segment of frame row events (PGC-241): PgEval
//! membership (`membership`) and the row-change / old-image lookup
//! (`lookup`, fed by the `old_image` overlay pass), collected into one
//! [`SegmentMembership`] matrix the decide pass consults per event.

use std::collections::{HashMap, HashSet};

use super::WriterCdc;
use crate::cache::CacheResult;
use crate::cache::update_query::{RowChanges, UpdateQueryKey, UpdateQueryKeySet};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::FrameRowEvent;
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;

mod lookup;
mod membership;
mod old_image;

/// Statement-cache key for prepared membership eval. Deliberately a named
/// type: when update queries become shape-parameterized (PGC-257) this swaps
/// to `(relation, shape)` here, and the cache machinery carries over.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PreparedEvalKey {
    pub(super) relation_oid: Oid,
    update_query: UpdateQueryKey,
}

/// One segment's row events, with the frame index of its first: event
/// `offset` of `events` is frame event `base_idx + offset`.
#[derive(Clone, Copy)]
pub(super) struct SegmentEvents<'a> {
    pub(super) events: &'a [FrameRowEvent],
    pub(super) base_idx: usize,
}

impl<'a> SegmentEvents<'a> {
    /// Each event with its frame index.
    fn indexed(self) -> impl Iterator<Item = (usize, &'a FrameRowEvent)> {
        self.events
            .iter()
            .enumerate()
            .map(move |(offset, event)| (self.base_idx + offset, event))
    }
}

/// Batched PgEval membership + row-change results for one segment of frame
/// row events (PGC-241). Built by `segment_membership_eval` /
/// `segment_row_changes_eval`, consumed per event by the decide pass.
#[derive(Default)]
pub(super) struct SegmentMembership {
    relations: HashMap<Oid, RelationBatch>,
}

/// One relation's batched-eval results within a segment. Every event belongs
/// to exactly one relation, so the coverage invariants (which fingerprints a
/// covered event may consult) live inside one entry instead of across
/// parallel collections.
#[derive(Default)]
struct RelationBatch {
    /// Batchable Fresh-MV update queries, evaluated for every `covered` event
    /// (Fresh queries are always fully evaluated so every match dirty-marks —
    /// same as the per-row path).
    fresh_keys: UpdateQueryKeySet,
    /// Batchable non-Fresh update queries, evaluated only for `rest_covered`
    /// events (rows with no LocalEval match and no fresh hit) — mirroring the
    /// per-row path's `if !matched` short-circuit, which does zero PgEval
    /// round-trips for locally-matched rows.
    rest_keys: UpdateQueryKeySet,
    /// Event indexes whose row was in the fresh membership batch (rows of
    /// unexpected arity stay out and fall back to per-row eval).
    covered: HashSet<usize>,
    /// Event indexes whose row was in the rest membership batch.
    rest_covered: HashSet<usize>,
    /// `(event index, update query)` membership hits.
    hits: HashSet<(usize, UpdateQueryKey)>,
    /// Update events whose row-change SELECT was batched. Covered-but-absent
    /// from `row_changes` ⇒ the row isn't in the cache table (the per-row
    /// `None` case).
    row_change_covered: HashSet<usize>,
    /// Per covered update event: column → changed (`IS DISTINCT FROM`).
    row_changes: HashMap<usize, RowChanges>,
    /// Per update/delete event: the recovered old image (PGC-255), resolved
    /// from the rung-1 overlay or the batched rung-2 lookup; absent =
    /// wildcard fallback.
    old_images: HashMap<usize, OldImage>,
    /// Events whose rung-2 lookup ran. Covered without an image ⇒ no cache
    /// table row had the key before the frame (PGC-528).
    old_image_lookup_covered: HashSet<usize>,
}

/// A recovered pre-event row: full width, `known` the positions it carries
/// real values for. Elsewhere `None` is unknown, which probes as the
/// `Unknown` wildcard.
pub(super) struct OldImage {
    pub(super) row: Vec<Option<ByteString>>,
    pub(super) known: Vec<usize>,
}

impl SegmentMembership {
    /// The matrix view for one event, or `None` if neither the membership nor
    /// the row-change batch covered it.
    pub(super) fn view(&self, relation_oid: Oid, event_idx: usize) -> Option<BatchEvalView<'_>> {
        let batch = self.relations.get(&relation_oid)?;
        let fresh_keys = batch
            .covered
            .contains(&event_idx)
            .then_some(&batch.fresh_keys);
        let rest_keys = batch
            .rest_covered
            .contains(&event_idx)
            .then_some(&batch.rest_keys);
        let row_change = batch
            .row_change_covered
            .contains(&event_idx)
            .then(|| batch.row_changes.get(&event_idx));
        let old_image = batch.old_images.get(&event_idx);
        let old_image_absent =
            old_image.is_none() && batch.old_image_lookup_covered.contains(&event_idx);
        let present = [
            fresh_keys.is_some(),
            rest_keys.is_some(),
            row_change.is_some(),
            old_image.is_some(),
            old_image_absent,
        ];
        if !present.contains(&true) {
            return None;
        }
        Some(BatchEvalView {
            fresh_keys,
            rest_keys,
            hits: &batch.hits,
            event_idx,
            row_change,
            old_image: old_image.map(|image| image.row.as_slice()),
            old_image_known: old_image.map_or(&[], |image| image.known.as_slice()),
            old_image_absent,
        })
    }

    fn relation_batch(&mut self, relation_oid: Oid) -> &mut RelationBatch {
        self.relations.entry(relation_oid).or_default()
    }
}

/// One event's window into a [`SegmentMembership`] matrix.
pub(super) struct BatchEvalView<'a> {
    fresh_keys: Option<&'a UpdateQueryKeySet>,
    rest_keys: Option<&'a UpdateQueryKeySet>,
    hits: &'a HashSet<(usize, UpdateQueryKey)>,
    event_idx: usize,
    /// Outer `None` = row-change not batched for this event (fall back to the
    /// per-row SELECT); `Some(inner)` mirrors `query_row_changes`' return.
    pub(super) row_change: Option<Option<&'a RowChanges>>,
    /// The event's recovered old image, when the recovery ladder resolved one
    /// — `None` = use the wildcard probe (PGC-255).
    pub(super) old_image: Option<&'a [Option<ByteString>]>,
    /// The positions `old_image` carries real values for.
    pub(super) old_image_known: &'a [usize],
    /// The rung-2 lookup ran and no cache-table row had the event's key.
    pub(super) old_image_absent: bool,
}

impl BatchEvalView<'_> {
    /// Whether `update_query` was batch-evaluated (consult `hit` instead of a
    /// per-row round-trip). A rest query outside both covered sets falls back
    /// to the per-row path, whose `if !matched` guard skips it exactly as the
    /// pre-batch flow did.
    pub(super) fn covers(&self, update_query: UpdateQueryKey) -> bool {
        self.fresh_keys
            .is_some_and(|keys| keys.contains(&update_query))
            || self
                .rest_keys
                .is_some_and(|keys| keys.contains(&update_query))
    }

    /// Whether this event's row matched `update_query`'s predicate.
    pub(super) fn hit(&self, update_query: UpdateQueryKey) -> bool {
        self.hits.contains(&(self.event_idx, update_query))
    }
}

/// Shared array params for a prepared eval statement over one row chunk:
/// `$1` = row ordinals, `$2..` = one `text[]` per column in
/// `table_metadata.columns` order. The unnest transform's parameter numbering
/// and the prepared row-change SQL builder both follow the same column order —
/// this is the single place the binding contract is produced.
fn chunk_arrays_build<'a, R>(
    table_metadata: &TableMetadata,
    row_chunk: &[R],
    row_of: impl Fn(&R) -> &'a [Option<ByteString>],
) -> (Vec<i32>, Vec<Vec<Option<&'a str>>>) {
    // Chunk length is bounded by PG_EVAL_ROW_CHUNK (64): never wraps.
    #[allow(clippy::cast_possible_wrap, clippy::cast_possible_truncation)]
    let ordinals: Vec<i32> = (0..row_chunk.len() as i32).collect();
    let column_arrays = table_metadata
        .columns
        .iter()
        .map(|column_meta| {
            row_chunk
                .iter()
                .map(|r| {
                    row_of(r)
                        .get(column_meta.index())
                        .and_then(|v| v.as_deref())
                })
                .collect()
        })
        .collect();
    (ordinals, column_arrays)
}

impl WriterCdc {
    /// Run both batch passes for a segment: PgEval membership, then row-change
    /// detection, into one [`SegmentMembership`] matrix (PGC-241).
    pub(super) async fn segment_eval(
        &mut self,
        core: &mut WriterCore,
        segment: SegmentEvents<'_>,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = self.segment_membership_eval(core, segment).await?;
        self.segment_row_changes_eval(core, segment, &mut membership)
            .await?;
        Ok(membership)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::Fingerprint;

    /// A batch covers the update queries it evaluated, not their fingerprint:
    /// another update query of the fingerprint falls back to per-row eval
    /// (PGC-506).
    #[test]
    fn test_batch_view_covers_update_query_not_fingerprint() {
        let fingerprint = Fingerprint::from_raw(7);
        let batched = UpdateQueryKey {
            fingerprint,
            ordinal: 0,
        };
        let per_row = UpdateQueryKey {
            fingerprint,
            ordinal: 1,
        };
        let relation_oid = Oid::from_raw(1001);
        let mut membership = SegmentMembership::default();
        let batch = membership.relation_batch(relation_oid);
        batch.fresh_keys.insert(batched);
        batch.covered.insert(0);
        batch.hits.insert((0, batched));

        let view = membership
            .view(relation_oid, 0)
            .expect("event 0 is covered");
        assert!(view.covers(batched));
        assert!(view.hit(batched));
        assert!(!view.covers(per_row));
        assert!(!view.hit(per_row));
    }
}
