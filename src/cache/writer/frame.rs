use crate::catalog::Oid;
use crate::pg::Lsn;
use std::collections::HashMap;

use ecow::EcoString;

use crate::pg::protocol::ByteString;

use super::super::messages::{CdcValue, cdc_values_convert};

use super::core::*;
use super::staging::pk_body_render;

/// Preallocated capacity for the per-frame SQL write buffer (PGC-228). Fixed up
/// front so the buffer never reallocates in steady state; also the byte
/// threshold at which a frame's writes are chunk-flushed to bound memory.
pub(super) const FRAME_BUF_CAPACITY: usize = 256 * 1024;

/// Cap on recycled toast-overlay `Values` Vecs retained across batches, so an
/// unusually large batch doesn't pin its peak overlay footprint forever.
const TOAST_OVERLAY_POOL_MAX: usize = 4096;

/// Maximum buffered row events per frame before a mid-frame partial replay —
/// bounds frame memory the way `FRAME_BUF_CAPACITY` bounds `frame_buf`.
/// Replaying a prefix early is exactly the per-arrival behavior, so ordering
/// and results are unchanged.
pub(super) const FRAME_ROWS_CAPACITY: usize = 4096;

/// Cap on recycled row Vecs (`cdc_values_convert` output). A replay drains at
/// most `FRAME_ROWS_CAPACITY` events, each holding up to two row Vecs.
const ROW_VEC_POOL_MAX: usize = 2 * FRAME_ROWS_CAPACITY;

/// One buffered row event of the in-progress CDC frame (PGC-241). Events are
/// collected at arrival and replayed at the `CommitMark` flush boundary, in
/// arrival order — order is what makes the deferral pure: same-key sequences
/// (an INSERT then DELETE of one PK) and TRUNCATE-vs-row interleavings emit
/// exactly as per-arrival handling did.
pub(super) enum FrameRowEvent {
    Insert {
        relation_oid: Oid,
        row_data: Vec<Option<ByteString>>,
    },
    Update {
        relation_oid: Oid,
        key_data: Vec<Option<ByteString>>,
        new_row_data: Vec<Option<ByteString>>,
    },
    /// An UPDATE whose image carries unchanged-toast markers, awaiting repair
    /// (PGC-264). Resolved by the replay pre-pass (`toast_repair_events`) into
    /// a plain `Update` (values from the batch overlay or the batched cache
    /// lookup) or an `UpdateToastFallback` — no other consumer ever sees one.
    /// `Toasted` values are already mapped to `None` in `new_row_data`;
    /// `toasted` holds their column indexes.
    UpdateToasted {
        relation_oid: Oid,
        key_data: Vec<Option<ByteString>>,
        new_row_data: Vec<Option<ByteString>>,
        toasted: Vec<usize>,
    },
    /// An UPDATE whose unchanged-toast columns could not be repaired (row
    /// absent from the cache table, or its in-batch state untrustworthy —
    /// PGC-264). Excluded from segment eval; the decide pass conservatively
    /// invalidates affected queries instead of upserting the incomplete image.
    /// `Toasted` values are already mapped to `None` in `new_row_data`;
    /// `toasted_columns` names the elided columns.
    UpdateToastFallback {
        relation_oid: Oid,
        key_data: Vec<Option<ByteString>>,
        new_row_data: Vec<Option<ByteString>>,
        toasted_columns: Vec<EcoString>,
    },
    Delete {
        relation_oid: Oid,
        row_data: Vec<Option<ByteString>>,
    },
    Truncate {
        relation_oids: Vec<Oid>,
    },
    /// A source-transaction commit boundary (PGC-242). Carries the frame's
    /// commit LSN so per-frame bookkeeping produced *during replay* — deleted
    /// keys (PGC-250) and truncate abort watermarks — is stamped with the
    /// right frame's LSN when the log spans multiple frames. Does not split
    /// eval segments (cross-frame batching is the point).
    Boundary {
        commit_lsn: Lsn,
    },
}

/// One entry of `batch_toast_overlay` (PGC-264): what this batch last did to
/// a PK's toastable columns.
pub(super) enum OverlayEntry {
    /// Toastable-column `(position, value)` pairs from the last in-batch
    /// write of the row.
    Values(Vec<(usize, Option<ByteString>)>),
    /// The PK was deleted this batch with no subsequent write. The pre-batch
    /// image is stale, and origin cannot update a deleted row, so a toasted
    /// update hitting this is defensive-fallback territory.
    Deleted,
}

/// CDC source-txn frame state on `WriterCdc`'s write connection (PGC-108).
/// `Idle →Begin Active →write TxnOpen →Commit Idle`; `* →40P01 Recovering`.
/// Writes are buffered (PGC-228): a `BEGIN` and the cache-table statements
/// accumulate in `frame_buf` and reach the server only when the buffer is
/// chunk-flushed mid-frame or flushed as one `BEGIN; …; COMMIT` at `CommitMark`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum FrameState {
    /// Between source transactions.
    Idle,
    /// In a frame, no cache-table write yet — no `BEGIN`/locks, but
    /// invalidations/oids may be accumulating for the `CommitMark` flush.
    Active,
    /// A cache-table write has been buffered: a `BEGIN` is owed and a `COMMIT`
    /// pending at `CommitMark` (was `frame_open`). Holds no server-side locks
    /// until the buffer flushes (chunk-flush or `CommitMark`); from the first
    /// flush on, the open txn holds row locks until `COMMIT`.
    TxnOpen,
    /// Hit `40P01`, rolled back; drains the rest of the txn with no DB apply,
    /// recovers affected relations at `CommitMark` (PGC-147). Holds no locks.
    Recovering,
}

/// Drop one relation's entries from an overlay map, harvesting `Values` Vecs
/// into the shared pool.
fn overlay_relation_drain(
    map: &mut HashMap<(Oid, EcoString), OverlayEntry>,
    pool: &mut Vec<Vec<(usize, Option<ByteString>)>>,
    relation_oid: Oid,
) {
    map.retain(|(r, _), entry| {
        if *r != relation_oid {
            return true;
        }
        if let OverlayEntry::Values(values) = entry
            && pool.len() < TOAST_OVERLAY_POOL_MAX
        {
            let mut harvested = std::mem::take(values);
            harvested.clear();
            pool.push(harvested);
        }
        false
    });
}

/// Drain an overlay map, harvesting `Values` Vec allocations into the shared
/// pool for reuse by the next batch's recording instead of dropping them.
fn overlay_drain_into_pool(
    map: &mut HashMap<(Oid, EcoString), OverlayEntry>,
    pool: &mut Vec<Vec<(usize, Option<ByteString>)>>,
) {
    for (_, entry) in map.drain() {
        if pool.len() >= TOAST_OVERLAY_POOL_MAX {
            return;
        }
        if let OverlayEntry::Values(mut values) = entry {
            values.clear();
            pool.push(values);
        }
    }
}

impl WriterCore {
    /// Clear both batch overlays (toast repair and old-image, same lifecycle)
    /// at batch end, harvesting `Values` Vecs into the shared pool.
    pub(super) fn toast_overlay_reset(&mut self) {
        overlay_drain_into_pool(&mut self.batch_toast_overlay, &mut self.toast_overlay_pool);
        overlay_drain_into_pool(
            &mut self.batch_old_image_overlay,
            &mut self.toast_overlay_pool,
        );
        self.batch_old_image_guard_oids.clear();
        self.batch_old_image_epochs.clear();
    }

    /// Invalidate a relation as a repair/old-image source mid-batch (PGC-264,
    /// PGC-255): drop its entries from both overlays (harvesting `Values` Vecs
    /// back into the pool) and guard it so a later event with no fresh
    /// in-batch write falls back instead of resolving from a now-invalid
    /// image. Used when the relation is truncated or DDL-recreated this batch
    /// — both empty (or reshape) its cache table, voiding the pre-batch
    /// committed image and any overlay entries recorded under the old layout.
    /// The overlays are consulted before the guards during resolution, so
    /// dropping the stale entries (not just guarding) is what prevents a
    /// positional misuse.
    pub(super) fn toast_overlay_relation_invalidate(&mut self, relation_oid: Oid) {
        overlay_relation_drain(
            &mut self.batch_toast_overlay,
            &mut self.toast_overlay_pool,
            relation_oid,
        );
        overlay_relation_drain(
            &mut self.batch_old_image_overlay,
            &mut self.toast_overlay_pool,
            relation_oid,
        );
        self.batch_toast_guard_oids.insert(relation_oid);
        self.batch_old_image_guard_oids.insert(relation_oid);
    }

    /// Reconcile a relation's old-image overlay with the current registration
    /// epoch (code review, PGC-255): entries are batch-lifetime but recording
    /// is gated on the current query set, so a mid-batch registration or
    /// eviction changes what gets recorded — entries stamped under an older
    /// epoch are untrustworthy. On mismatch, drop the relation's entries and
    /// guard it for the batch remainder (rung 2 included: unrecorded
    /// tombstones from the gap make the committed image unreliable too).
    pub(super) fn old_image_overlay_epoch_reconcile(&mut self, relation_oid: Oid, epoch: u64) {
        match self.batch_old_image_epochs.get(&relation_oid) {
            Some(&stamped) if stamped == epoch => {}
            Some(_) => {
                overlay_relation_drain(
                    &mut self.batch_old_image_overlay,
                    &mut self.toast_overlay_pool,
                    relation_oid,
                );
                self.batch_old_image_guard_oids.insert(relation_oid);
                self.batch_old_image_epochs.insert(relation_oid, epoch);
            }
            None => {
                self.batch_old_image_epochs.insert(relation_oid, epoch);
            }
        }
    }

    /// Recycle a `Values` Vec displaced from either overlay (a same-PK
    /// rewrite or tombstone within one batch).
    pub(super) fn toast_overlay_recycle(&mut self, entry: Option<OverlayEntry>) {
        if let Some(OverlayEntry::Values(mut values)) = entry
            && self.toast_overlay_pool.len() < TOAST_OVERLAY_POOL_MAX
        {
            values.clear();
            self.toast_overlay_pool.push(values);
        }
    }

    /// Record an in-batch complete write's eval-index column values under the
    /// row's PK (PGC-255 rung 1): later same-PK events resolve their old image
    /// from these instead of the (now stale) pre-batch committed row.
    /// `index_positions` is the relation's eval-index column position set —
    /// callers skip relations where it is empty (no value-dependent entries,
    /// nothing to resolve).
    pub(super) fn old_image_overlay_record_write(
        &mut self,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
        index_positions: &[usize],
    ) {
        let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
            return;
        };
        let Some(key) = pk_body_render(table_metadata, row_data) else {
            return;
        };
        self.old_image_overlay_record_write_keyed(relation_oid, key, row_data, index_positions);
    }

    /// Tombstone a PK in the old-image overlay (PGC-255): the row was deleted
    /// (or its old key vacated) this batch, so neither the pre-batch committed
    /// image nor any earlier overlay value is a valid old-image source.
    pub(super) fn old_image_overlay_record_delete(
        &mut self,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) {
        let Some(table_metadata) = self.cache.tables.get1(&relation_oid) else {
            return;
        };
        let Some(key) = pk_body_render(table_metadata, row_data) else {
            return;
        };
        self.old_image_overlay_record_delete_keyed(relation_oid, key);
    }

    /// Rung-1 old-image lookup by pre-rendered PK key: `Some(Values)` =
    /// resolved from an in-batch write, `Some(Deleted)` = tombstoned (fall
    /// back to the wildcard probe), `None` = no in-batch state — consult the
    /// batched pre-batch lookup unless the relation is guarded
    /// (`batch_old_image_guard_oids`). Callers render the key once per event
    /// (`pk_body_render`) and reuse it for the recording calls.
    pub(super) fn old_image_overlay_get(
        &self,
        relation_oid: Oid,
        key: &EcoString,
    ) -> Option<&OverlayEntry> {
        self.batch_old_image_overlay
            .get(&(relation_oid, key.clone()))
    }

    /// Keyed variant of `old_image_overlay_record_write` — the caller already
    /// rendered the row's PK.
    pub(super) fn old_image_overlay_record_write_keyed(
        &mut self,
        relation_oid: Oid,
        key: EcoString,
        row_data: &[Option<ByteString>],
        index_positions: &[usize],
    ) {
        let mut values = self.toast_overlay_pool.pop().unwrap_or_default();
        values.extend(
            index_positions
                .iter()
                .map(|&p| (p, row_data.get(p).cloned().flatten())),
        );
        let displaced = self
            .batch_old_image_overlay
            .insert((relation_oid, key), OverlayEntry::Values(values));
        self.toast_overlay_recycle(displaced);
    }

    /// Keyed variant of `old_image_overlay_record_delete`.
    pub(super) fn old_image_overlay_record_delete_keyed(
        &mut self,
        relation_oid: Oid,
        key: EcoString,
    ) {
        let displaced = self
            .batch_old_image_overlay
            .insert((relation_oid, key), OverlayEntry::Deleted);
        self.toast_overlay_recycle(displaced);
    }

    /// `cdc_values_convert` into a recycled row Vec from `row_vec_pool`.
    /// Empty inputs (an update with no key tuple) skip the pool — an empty
    /// `Vec::new` never allocates.
    pub(super) fn row_convert(
        &mut self,
        values: Vec<CdcValue>,
    ) -> (Vec<Option<ByteString>>, Vec<usize>) {
        if values.is_empty() {
            return (Vec::new(), Vec::new());
        }
        let mut row_data = self.row_vec_pool.pop().unwrap_or_default();
        let toasted = cdc_values_convert(values, &mut row_data);
        (row_data, toasted)
    }

    /// Return a replay-drained event's row Vecs to `row_vec_pool`.
    pub(super) fn row_vecs_recycle(&mut self, event: FrameRowEvent) {
        let (first, second) = match event {
            FrameRowEvent::Insert { row_data, .. } | FrameRowEvent::Delete { row_data, .. } => {
                (Some(row_data), None)
            }
            FrameRowEvent::Update {
                key_data,
                new_row_data,
                ..
            }
            | FrameRowEvent::UpdateToasted {
                key_data,
                new_row_data,
                ..
            }
            | FrameRowEvent::UpdateToastFallback {
                key_data,
                new_row_data,
                ..
            } => (Some(key_data), Some(new_row_data)),
            FrameRowEvent::Truncate { .. } | FrameRowEvent::Boundary { .. } => (None, None),
        };
        for mut row in [first, second].into_iter().flatten() {
            // Empty key Vecs carry no allocation worth pooling.
            if row.capacity() == 0 || self.row_vec_pool.len() >= ROW_VEC_POOL_MAX {
                continue;
            }
            row.clear();
            self.row_vec_pool.push(row);
        }
    }

    /// Buffer the frame's `BEGIN` on the first cache-table write (`Active →
    /// TxnOpen`); idempotent for later writes. The actual `BEGIN` reaches the
    /// server only when `frame_buf` is flushed. A write while `Idle` (no
    /// preceding `Begin`) is a bug.
    ///
    /// `relations` are the cache tables whose SQL the caller is about to append
    /// — recorded into `frame_buf_relations` here so a write cannot reach the
    /// buffer without marking its relation (the signal a mid-frame DDL uses to
    /// choose discard vs. frame recovery). Every buffer write goes through this
    /// chokepoint.
    pub(super) fn frame_begin_ensure(&mut self, relations: impl IntoIterator<Item = Oid>) {
        debug_assert!(
            !matches!(self.frame_state, FrameState::Idle),
            "cache-table write before Begin (frame not entered)"
        );
        if self.frame_state == FrameState::Active {
            self.frame_buf.push_str("BEGIN; ");
            self.frame_state = FrameState::TxnOpen;
        }
        self.frame_buf_relations.extend(relations);
    }

    /// Frame holds row locks (a `BEGIN` is open) — maintenance paths defer
    /// cache-table DDL/purges while true.
    pub(super) fn frame_holds_locks(&self) -> bool {
        matches!(self.frame_state, FrameState::TxnOpen)
    }
}
