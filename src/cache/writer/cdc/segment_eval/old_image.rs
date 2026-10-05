//! Old-image recovery for a segment (PGC-255): the per-relation fetch spec,
//! and the arrival-order rung-1 overlay pass that resolves old images from
//! earlier in-batch writes and queues the rest for the batched rung-2 lookup.

use std::collections::HashMap;

use ecow::EcoString;

use super::SegmentMembership;
use super::lookup::LookupRow;
use crate::cache::writer::cdc::row_changes::table_has_reserved_columns;
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::{FrameRowEvent, OverlayEntry, ToastState};
use crate::cache::writer::staging::pk_body_render;
use crate::catalog::ColumnMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;

/// Per-relation lookup shape for one segment, computed once per relation:
/// whether update events consume changed-column booleans, and the eval-index
/// columns (positions for overlay recording and old-image expansion;
/// `fetch_columns` for the rung-2 SQL projection). REPLICA IDENTITY FULL
/// relations skip the ladder entirely — the 'O' tuple is authoritative
/// (rung 0 in the dispatch handlers).
pub(super) struct RelationFetchSpec {
    pub(super) needs_changes: bool,
    replica_identity_full: bool,
    /// `UpdateQueries::epoch` at spec build — stamped into the overlay via
    /// `old_image_overlay_epoch_reconcile` before any overlay activity, and
    /// the prepared row-change statement's cache tag.
    pub(super) epoch: u64,
    /// `__pgc_`-named real columns exist: their aliases would collide with
    /// the projection-alias namespace (including the batch ordinal), so the
    /// relation skips the batched lookup and the old-image ladder entirely —
    /// booleans come from the per-row fallback, old images stay wildcard.
    pub(super) reserved_columns: bool,
    /// Eval-index columns whose `::text` rendering is wire-canonical
    /// (`old_image_text_stable`) — the only ones rung 2 may fetch. Excluded
    /// columns stay `None` in the expanded image → `Unknown` wildcard.
    pub(super) fetch_columns: Vec<EcoString>,
    index_positions: Vec<usize>,
    full_width: usize,
}

impl RelationFetchSpec {
    fn build(core: &WriterCore, relation_oid: Oid) -> Self {
        let update_queries = core.cache.update_queries.get(&relation_oid);
        let index_columns: Vec<EcoString> = update_queries
            .map(|uq| uq.eval_index.columns().cloned().collect())
            .unwrap_or_default();
        let mut spec = Self {
            needs_changes: update_queries.is_some_and(|uq| uq.needs_change_eval()),
            replica_identity_full: false,
            epoch: update_queries.map_or(0, |uq| uq.epoch()),
            reserved_columns: false,
            fetch_columns: Vec::new(),
            index_positions: Vec::new(),
            full_width: 0,
        };
        if let Some(t) = core.cache.tables.get1(&relation_oid) {
            spec.replica_identity_full = t.replica_identity_full;
            spec.reserved_columns = table_has_reserved_columns(t);
            spec.fetch_columns = index_columns
                .iter()
                .filter(|c| {
                    t.columns
                        .get(c.as_str())
                        .is_some_and(|m| old_image_text_stable(&m.cache_type_name))
                })
                .cloned()
                .collect();
            spec.index_positions = index_columns
                .iter()
                .filter_map(|c| t.columns.get(c.as_str()).map(|m| m.index()))
                .collect();
            spec.full_width = t.columns.len();
        }
        spec
    }

    /// Whether the relation runs the old-image ladder at all: it has eval-index
    /// columns to recover, isn't REPLICA IDENTITY FULL (whose 'O' tuple is
    /// authoritative), and has no reserved-prefix columns.
    fn tracks_old_images(&self) -> bool {
        !self.index_positions.is_empty() && !self.replica_identity_full && !self.reserved_columns
    }

    /// Whether an overlay miss may be queued for the rung-2 lookup: something
    /// is fetchable and the relation isn't guarded off this batch.
    fn lookup_allowed(&self, core: &WriterCore, relation_oid: Oid) -> bool {
        !self.fetch_columns.is_empty() && !core.batch_old_image_guard_oids.contains(&relation_oid)
    }
}

/// Whether a column type's `::text` cast renders byte-identically to its
/// pgoutput wire text under any GUC settings — the precondition for probing a
/// rung-2-fetched value against the eval index, whose equality keys carry
/// wire/query-literal spellings. `bool` qualifies via parse-time
/// normalization ('true'/'false' → 't'/'f'); date/time types are DateStyle/
/// TimeZone-sensitive and floats are extra_float_digits-sensitive, so they
/// are excluded (their probe positions degrade to the wildcard).
fn old_image_text_stable(cache_type_name: &str) -> bool {
    matches!(
        cache_type_name,
        "int2"
            | "int4"
            | "int8"
            | "oid"
            | "numeric"
            | "text"
            | "varchar"
            | "bpchar"
            | "uuid"
            | "bool"
    )
}

/// `bool::text` renders 'true'/'false' but the eval-index keys carry wire
/// text ('t'/'f') — normalize so the fetched value probes identically.
pub(super) fn old_image_bool_normalize<'a>(meta: &ColumnMetadata, value: &'a str) -> &'a str {
    if meta.cache_type_name != "bool" {
        return value;
    }
    match value {
        "true" => "t",
        "false" => "f",
        _ => value,
    }
}

/// Expand sparse eval-index `(position, value)` pairs to a full-width row.
/// Unfetched positions stay `None`, which probes as the `Unknown` wildcard —
/// vacuous, since the probe only consults index columns, all of which are
/// fetched.
pub(super) fn old_image_expand(
    full_width: usize,
    values: &[(usize, Option<ByteString>)],
) -> Vec<Option<ByteString>> {
    let mut row = vec![None; full_width];
    for (position, value) in values {
        if let Some(slot) = row.get_mut(*position) {
            *slot = value.clone();
        }
    }
    row
}

/// The relation's fetch spec, computed on first use per segment. An untracked
/// relation yields an empty spec (no booleans, no old images).
fn relation_fetch_spec<'s>(
    core: &WriterCore,
    specs: &'s mut HashMap<Oid, RelationFetchSpec>,
    relation_oid: Oid,
) -> &'s RelationFetchSpec {
    specs
        .entry(relation_oid)
        .or_insert_with(|| RelationFetchSpec::build(core, relation_oid))
}

/// The spec of a relation that runs the old-image ladder, with the overlay's
/// epoch reconciled before any overlay activity; `None` otherwise.
fn tracked_spec<'s>(
    core: &mut WriterCore,
    specs: &'s mut HashMap<Oid, RelationFetchSpec>,
    relation_oid: Oid,
) -> Option<&'s RelationFetchSpec> {
    let spec = relation_fetch_spec(core, specs, relation_oid);
    if !spec.tracks_old_images() {
        return None;
    }
    core.old_image_overlay_epoch_reconcile(relation_oid, spec.epoch);
    Some(spec)
}

fn pk_key(core: &WriterCore, relation_oid: Oid, row: &[Option<ByteString>]) -> Option<EcoString> {
    core.cache
        .tables
        .get1(&relation_oid)
        .and_then(|t| pk_body_render(t, row))
}

/// What the rung-1 overlay knows about a row's pre-event image.
enum OverlayResolution {
    /// An earlier in-batch write left the old image.
    Hit(Vec<Option<ByteString>>),
    /// Tombstoned: no valid old-image source → wildcard.
    Tombstone,
    /// Not in the overlay — a candidate for the rung-2 lookup.
    Miss,
}

impl OverlayResolution {
    /// Record a hit into the matrix; true for a miss.
    fn record(
        self,
        membership: &mut SegmentMembership,
        relation_oid: Oid,
        event_idx: usize,
    ) -> bool {
        match self {
            Self::Hit(row) => {
                membership
                    .relation_batch(relation_oid)
                    .old_images
                    .insert(event_idx, row);
                false
            }
            Self::Tombstone => false,
            Self::Miss => true,
        }
    }
}

fn overlay_resolve(
    core: &WriterCore,
    spec: &RelationFetchSpec,
    relation_oid: Oid,
    key: &EcoString,
) -> OverlayResolution {
    match core.old_image_overlay_get(relation_oid, key) {
        Some(OverlayEntry::Values(values)) => {
            OverlayResolution::Hit(old_image_expand(spec.full_width, values))
        }
        Some(OverlayEntry::Deleted) => OverlayResolution::Tombstone,
        None => OverlayResolution::Miss,
    }
}

/// Arrival-order pass over a segment: maintain the rung-1 old-image overlay,
/// resolve overlay hits into the matrix, and queue lookup rows (row-change
/// booleans and/or rung-2 old images) per relation.
pub(super) struct OldImagePrepass<'a, 'c> {
    core: &'c mut WriterCore,
    membership: &'c mut SegmentMembership,
    specs: HashMap<Oid, RelationFetchSpec>,
    lookups: HashMap<Oid, Vec<LookupRow<'a>>>,
}

impl<'a, 'c> OldImagePrepass<'a, 'c> {
    pub(super) fn new(core: &'c mut WriterCore, membership: &'c mut SegmentMembership) -> Self {
        Self {
            core,
            membership,
            specs: HashMap::new(),
            lookups: HashMap::new(),
        }
    }

    /// The per-relation specs and queued lookup rows.
    pub(super) fn into_parts(
        self,
    ) -> (
        HashMap<Oid, RelationFetchSpec>,
        HashMap<Oid, Vec<LookupRow<'a>>>,
    ) {
        (self.specs, self.lookups)
    }

    pub(super) fn event_record(&mut self, event_idx: usize, event: &'a FrameRowEvent) {
        match event {
            FrameRowEvent::Insert {
                relation_oid,
                row_data,
            } => self.insert(*relation_oid, row_data),
            FrameRowEvent::Update {
                relation_oid,
                key_data,
                new_row_data,
                toast: ToastState::Complete,
            } => self.update(event_idx, *relation_oid, key_data, new_row_data),
            FrameRowEvent::Delete {
                relation_oid,
                row_data,
            } => self.delete(event_idx, *relation_oid, row_data),
            FrameRowEvent::Update {
                relation_oid,
                key_data,
                new_row_data,
                toast: ToastState::Unrepaired(_),
            } => self.toast_fallback(*relation_oid, key_data, new_row_data),
            FrameRowEvent::Update {
                toast: ToastState::Pending(_),
                ..
            }
            | FrameRowEvent::Truncate { .. }
            | FrameRowEvent::Boundary { .. } => {}
        }
    }

    fn insert(&mut self, relation_oid: Oid, row_data: &[Option<ByteString>]) {
        if let Some(spec) = tracked_spec(self.core, &mut self.specs, relation_oid) {
            self.core
                .old_image_overlay_record_write(relation_oid, row_data, &spec.index_positions);
        }
    }

    fn update(
        &mut self,
        event_idx: usize,
        relation_oid: Oid,
        key_data: &'a [Option<ByteString>],
        new_row_data: &'a [Option<ByteString>],
    ) {
        // PGC-227: booleans only for relations where some query's UPDATE
        // invalidation depends on changed columns. Reserved-prefix relations
        // never enter the batch (their column aliases would collide with the
        // projection namespace) — the per-row fallback carries their booleans.
        let spec = relation_fetch_spec(self.core, &mut self.specs, relation_oid);
        let wants_changes = spec.needs_changes && !spec.reserved_columns;
        let wants_old_image =
            self.update_old_image(event_idx, relation_oid, key_data, new_row_data);
        if wants_changes || wants_old_image {
            self.lookup_queue(
                relation_oid,
                LookupRow {
                    event_idx,
                    row: new_row_data,
                    wants_changes,
                    wants_old_image,
                },
            );
        }
    }

    /// Resolve and re-record the overlay for an UPDATE; true when the old
    /// image should come from the rung-2 lookup. Under REPLICA IDENTITY FULL
    /// every event carries its own authoritative 'O' tuple, which the dispatch
    /// handlers probe directly (rung 0); in the DEFAULT branch a present key
    /// tuple IS the PK-change signal.
    fn update_old_image(
        &mut self,
        event_idx: usize,
        relation_oid: Oid,
        key_data: &[Option<ByteString>],
        new_row_data: &[Option<ByteString>],
    ) -> bool {
        let Some(spec) = tracked_spec(self.core, &mut self.specs, relation_oid) else {
            return false;
        };
        let pk_changed = !key_data.is_empty();
        // The old image lives under the pre-event PK; render each key once per
        // event and reuse it.
        let source_row = if pk_changed { key_data } else { new_row_data };
        let Some(source_key) = pk_key(self.core, relation_oid, source_row) else {
            return false;
        };
        let miss = overlay_resolve(self.core, spec, relation_oid, &source_key).record(
            self.membership,
            relation_oid,
            event_idx,
        );
        // PK-change events never take rung 2: the lookup joins by the tuple's
        // (new) PK, but the old image lives under the old PK — wildcard instead.
        let wants_old_image = miss && !pk_changed && spec.lookup_allowed(self.core, relation_oid);
        if pk_changed {
            self.core
                .old_image_overlay_record_delete_keyed(relation_oid, source_key);
            self.core.old_image_overlay_record_write(
                relation_oid,
                new_row_data,
                &spec.index_positions,
            );
        } else {
            self.core.old_image_overlay_record_write_keyed(
                relation_oid,
                source_key,
                new_row_data,
                &spec.index_positions,
            );
        }
        wants_old_image
    }

    /// Under REPLICA IDENTITY FULL the delete tuple is the complete old row —
    /// `handle_delete` probes it directly.
    fn delete(&mut self, event_idx: usize, relation_oid: Oid, row_data: &'a [Option<ByteString>]) {
        let Some(spec) = tracked_spec(self.core, &mut self.specs, relation_oid) else {
            return;
        };
        let Some(key) = pk_key(self.core, relation_oid, row_data) else {
            return;
        };
        let miss = overlay_resolve(self.core, spec, relation_oid, &key).record(
            self.membership,
            relation_oid,
            event_idx,
        );
        let wants_old_image = miss && spec.lookup_allowed(self.core, relation_oid);
        self.core
            .old_image_overlay_record_delete_keyed(relation_oid, key);
        if wants_old_image {
            self.lookup_queue(
                relation_oid,
                LookupRow {
                    event_idx,
                    row: row_data,
                    wants_changes: false,
                    wants_old_image: true,
                },
            );
        }
    }

    /// The post-image is incomplete: neither PK is a trustworthy old-image
    /// source for later same-PK events.
    fn toast_fallback(
        &mut self,
        relation_oid: Oid,
        key_data: &[Option<ByteString>],
        new_row_data: &[Option<ByteString>],
    ) {
        if tracked_spec(self.core, &mut self.specs, relation_oid).is_none() {
            return;
        }
        if !key_data.is_empty() {
            self.core
                .old_image_overlay_record_delete(relation_oid, key_data);
        }
        self.core
            .old_image_overlay_record_delete(relation_oid, new_row_data);
    }

    fn lookup_queue(&mut self, relation_oid: Oid, row: LookupRow<'a>) {
        self.lookups.entry(relation_oid).or_default().push(row);
    }
}
