//! Batched evaluation for one segment of frame row events (PGC-241): PgEval
//! membership (`membership`) and the row-change / old-image lookup
//! (`lookup`, fed by the `old_image` overlay pass), collected into one
//! [`SegmentMembership`] matrix the decide pass consults per event.

use std::collections::{HashMap, HashSet};

use super::WriterCdc;
use crate::cache::CacheResult;
use crate::cache::update_query::RowChanges;
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::FrameRowEvent;
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::{Fingerprint, FingerprintSet};

mod lookup;
mod membership;
mod old_image;

/// Statement-cache key for prepared membership eval. Deliberately a named
/// type: when update queries become shape-parameterized (PGC-257) this swaps
/// to `(relation, shape)` here, and the cache machinery carries over.
#[derive(Clone, Copy, PartialEq, Eq, Hash)]
pub(super) struct PreparedEvalKey {
    pub(super) relation_oid: Oid,
    fingerprint: Fingerprint,
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
    /// Batchable Fresh-MV fingerprints, evaluated for every `covered` event
    /// (Fresh queries are always fully evaluated so every match dirty-marks —
    /// same as the per-row path).
    fresh_fps: FingerprintSet,
    /// Batchable non-Fresh fingerprints, evaluated only for `rest_covered`
    /// events (rows with no LocalEval match and no fresh hit) — mirroring the
    /// per-row path's `if !matched` short-circuit, which does zero PgEval
    /// round-trips for locally-matched rows.
    rest_fps: FingerprintSet,
    /// Event indexes whose row was in the fresh membership batch (rows of
    /// unexpected arity stay out and fall back to per-row eval).
    covered: HashSet<usize>,
    /// Event indexes whose row was in the rest membership batch.
    rest_covered: HashSet<usize>,
    /// `(event index, fingerprint)` membership hits.
    hits: HashSet<(usize, Fingerprint)>,
    /// Update events whose row-change SELECT was batched. Covered-but-absent
    /// from `row_changes` ⇒ the row isn't in the cache table (the per-row
    /// `None` case).
    row_change_covered: HashSet<usize>,
    /// Per covered update event: column → changed (`IS DISTINCT FROM`).
    row_changes: HashMap<usize, RowChanges>,
    /// Per update/delete event: the recovered old image as a full-width row
    /// (eval-index columns carry values, the rest `None` → `Unknown` in the
    /// probe). Resolved from the rung-1 overlay or the batched rung-2 lookup;
    /// absent = wildcard fallback (PGC-255).
    old_images: HashMap<usize, Vec<Option<ByteString>>>,
}

impl SegmentMembership {
    /// The matrix view for one event, or `None` if neither the membership nor
    /// the row-change batch covered it.
    pub(super) fn view(&self, relation_oid: Oid, event_idx: usize) -> Option<BatchEvalView<'_>> {
        let batch = self.relations.get(&relation_oid)?;
        let fresh_fps = batch
            .covered
            .contains(&event_idx)
            .then_some(&batch.fresh_fps);
        let rest_fps = batch
            .rest_covered
            .contains(&event_idx)
            .then_some(&batch.rest_fps);
        let row_change = batch
            .row_change_covered
            .contains(&event_idx)
            .then(|| batch.row_changes.get(&event_idx));
        let old_image = batch.old_images.get(&event_idx).map(Vec::as_slice);
        let present = [
            fresh_fps.is_some(),
            rest_fps.is_some(),
            row_change.is_some(),
            old_image.is_some(),
        ];
        if !present.contains(&true) {
            return None;
        }
        Some(BatchEvalView {
            fresh_fps,
            rest_fps,
            hits: &batch.hits,
            event_idx,
            row_change,
            old_image,
        })
    }

    fn relation_batch(&mut self, relation_oid: Oid) -> &mut RelationBatch {
        self.relations.entry(relation_oid).or_default()
    }
}

/// One event's window into a [`SegmentMembership`] matrix.
pub(super) struct BatchEvalView<'a> {
    fresh_fps: Option<&'a FingerprintSet>,
    rest_fps: Option<&'a FingerprintSet>,
    hits: &'a HashSet<(usize, Fingerprint)>,
    event_idx: usize,
    /// Outer `None` = row-change not batched for this event (fall back to the
    /// per-row SELECT); `Some(inner)` mirrors `query_row_changes`' return.
    pub(super) row_change: Option<Option<&'a RowChanges>>,
    /// The event's recovered old image, when the recovery ladder resolved one
    /// — `None` = use the wildcard probe (PGC-255).
    pub(super) old_image: Option<&'a [Option<ByteString>]>,
}

impl BatchEvalView<'_> {
    /// Whether `fingerprint` was batch-evaluated (consult `hit` instead of a
    /// per-row round-trip). A rest query outside both covered sets falls back
    /// to the per-row path, whose `if !matched` guard skips it exactly as the
    /// pre-batch flow did.
    pub(super) fn covers(&self, fingerprint: Fingerprint) -> bool {
        self.fresh_fps.is_some_and(|fps| fps.contains(&fingerprint))
            || self.rest_fps.is_some_and(|fps| fps.contains(&fingerprint))
    }

    /// Whether this event's row matched `fingerprint`'s predicate.
    pub(super) fn hit(&self, fingerprint: Fingerprint) -> bool {
        self.hits.contains(&(self.event_idx, fingerprint))
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
        events: &[FrameRowEvent],
        base_idx: usize,
    ) -> CacheResult<SegmentMembership> {
        let mut membership = self.segment_membership_eval(core, events, base_idx).await?;
        self.segment_row_changes_eval(core, events, base_idx, &mut membership)
            .await?;
        Ok(membership)
    }
}
