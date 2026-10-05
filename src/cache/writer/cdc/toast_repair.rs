use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use ecow::EcoString;
use postgres_protocol::escape;
use tokio_postgres::SimpleQueryMessage;
use tracing::{debug, error};

use super::{SQL_BUFFER_CAPACITY, WriterCdc, update_pk_changed};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::frame::{FrameRowEvent, OverlayEntry, ToastState};
use crate::cache::writer::staging::pk_body_render;
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;

/// Toastable-column `(position, value)` pairs of one row image.
type ToastableValues = Vec<(usize, Option<ByteString>)>;

/// Batched lookup result: raw source-PK values → the row's toastable values.
type LookupRows = HashMap<Vec<ByteString>, ToastableValues>;

/// One queued toast repair awaiting the batched pre-batch-image lookup
/// (PGC-264).
struct PendingRepairSlot {
    event_idx: usize,
    /// Rendered source PK, for overlay bookkeeping.
    overlay_key: EcoString,
    /// Raw source-PK column values, for matching lookup result rows.
    raw_pk: Vec<ByteString>,
}

/// Pass-1 outcome for one toasted update (PGC-264).
enum ToastResolution {
    /// Overlay hit: the toasted positions' values to substitute.
    Repaired(ToastableValues),
    /// No in-batch state: queue for the batched lookup.
    Queue {
        overlay_key: EcoString,
        raw_pk: Vec<ByteString>,
    },
    Fallback,
}

/// A toast-pending update's fields, borrowed in place from the event log:
/// resolution patches `new_row_data` and moves `toast` to `Complete` or
/// `Unrepaired` without taking the event out.
struct PendingUpdate<'a> {
    relation_oid: Oid,
    key_data: &'a [Option<ByteString>],
    new_row_data: &'a mut [Option<ByteString>],
    toast: &'a mut ToastState,
}

impl<'a> PendingUpdate<'a> {
    /// The event's fields when it is an update still awaiting repair.
    fn of(event: &'a mut FrameRowEvent) -> Option<Self> {
        match event {
            FrameRowEvent::Update {
                relation_oid,
                key_data,
                new_row_data,
                toast,
            } if matches!(toast, ToastState::Pending(_)) => Some(Self {
                relation_oid: *relation_oid,
                key_data,
                new_row_data,
                toast,
            }),
            FrameRowEvent::Update { .. }
            | FrameRowEvent::Insert { .. }
            | FrameRowEvent::Delete { .. }
            | FrameRowEvent::Truncate { .. }
            | FrameRowEvent::Boundary { .. } => None,
        }
    }

    fn pk_changed(&self, table_metadata: &TableMetadata) -> bool {
        update_pk_changed(table_metadata, self.key_data, self.new_row_data)
    }

    /// The row the unchanged-toast marker refers to: the cached copy lives
    /// under the row's PRE-image key, so after a PK change it is the old PK.
    fn source_row(&self, pk_changed: bool) -> &[Option<ByteString>] {
        if pk_changed {
            self.key_data
        } else {
            self.new_row_data
        }
    }
}

/// The elided column positions of a pending toast state.
fn pending_positions(toast: &ToastState) -> &[usize] {
    match toast {
        ToastState::Pending(positions) => positions,
        ToastState::Complete | ToastState::Unrepaired(_) => &[],
    }
}

/// Names of the elided columns at `positions`.
pub(super) fn toasted_column_names(
    table_metadata: Option<&TableMetadata>,
    positions: &[usize],
) -> Vec<EcoString> {
    table_metadata
        .map(|table_metadata| {
            table_metadata
                .columns
                .iter()
                .filter(|c| positions.contains(&c.index()))
                .map(|c| c.name.clone())
                .collect()
        })
        .unwrap_or_default()
}

/// Collect a row image's toastable-column `(position, value)` pairs into
/// `values` — the payload of a [`OverlayEntry::Values`].
fn toastable_values_extend(
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    values: &mut ToastableValues,
) {
    values.extend(
        table_metadata
            .columns
            .iter()
            .filter(|c| c.is_toastable())
            .map(|c| (c.index(), row_data.get(c.index()).cloned().flatten())),
    );
}

/// The overlay's values for every toasted position, or `None` if any is
/// missing.
fn overlay_values_pick(
    values: &[(usize, Option<ByteString>)],
    toasted: &[usize],
) -> Option<ToastableValues> {
    let mut repaired = Vec::with_capacity(toasted.len());
    for &t in toasted {
        let (_, v) = values.iter().find(|(pos, _)| *pos == t)?;
        repaired.push((t, v.clone()));
    }
    Some(repaired)
}

/// Raw PK values of `row` for matching lookup result rows; a NULL PK value
/// can never match a lookup row, so it yields `None`.
fn raw_pk_values(
    table_metadata: &TableMetadata,
    row: &[Option<ByteString>],
) -> Option<Vec<ByteString>> {
    table_metadata
        .primary_key_columns
        .iter()
        .map(|pk_column| {
            table_metadata
                .columns
                .get(pk_column.as_str())
                .and_then(|c| row.get(c.index()).cloned().flatten())
        })
        .collect()
}

/// Substitute every toasted position `values` has into `row`; true when all
/// were found. A partially substituted row still falls back.
fn toasted_values_apply(
    values: &[(usize, Option<ByteString>)],
    toasted: &[usize],
    row: &mut [Option<ByteString>],
) -> bool {
    let mut complete = true;
    for &t in toasted {
        match values.iter().find(|(pos, _)| *pos == t) {
            Some((_, v)) => {
                if let Some(cell) = row.get_mut(t) {
                    *cell = v.clone();
                }
            }
            None => complete = false,
        }
    }
    complete
}

/// Pass-1 resolution for a toasted update whose relation is known.
fn toast_resolution(
    core: &WriterCore,
    table_metadata: &TableMetadata,
    update: &PendingUpdate<'_>,
    pk_changed: bool,
) -> ToastResolution {
    let source_row = update.source_row(pk_changed);
    let Some(key) = pk_body_render(table_metadata, source_row) else {
        return ToastResolution::Fallback;
    };
    match core
        .batch_toast_overlay
        .get(&(update.relation_oid, key.clone()))
    {
        Some(OverlayEntry::Values(values)) => {
            overlay_values_pick(values, pending_positions(update.toast))
                .map_or(ToastResolution::Fallback, ToastResolution::Repaired)
        }
        Some(OverlayEntry::Deleted) => ToastResolution::Fallback,
        None if core.batch_toast_guard_oids.contains(&update.relation_oid) => {
            ToastResolution::Fallback
        }
        None => match raw_pk_values(table_metadata, source_row) {
            Some(raw_pk) => ToastResolution::Queue {
                overlay_key: key,
                raw_pk,
            },
            None => ToastResolution::Fallback,
        },
    }
}

/// Per-PK toastable state of one relation's queued repairs as pass 2
/// advances through them in arrival order — a queued event is an in-batch
/// write the overlay never saw, so the next same-PK event must repair from
/// its post-image, not the pre-batch image.
struct ToastChain {
    entries: HashMap<EcoString, OverlayEntry>,
    /// The batched pre-batch-image lookup; `None` if it failed.
    lookup: Option<LookupRows>,
}

impl ToastChain {
    fn new(lookup: Option<LookupRows>) -> Self {
        Self {
            entries: HashMap::new(),
            lookup,
        }
    }

    /// The values to repair `slot` from: the chain's latest post-image, else
    /// the lookup row.
    fn source(&self, slot: &PendingRepairSlot) -> Option<&ToastableValues> {
        match self.entries.get(&slot.overlay_key) {
            Some(OverlayEntry::Values(values)) => Some(values),
            Some(OverlayEntry::Deleted) => None,
            None => self.lookup.as_ref().and_then(|rows| rows.get(&slot.raw_pk)),
        }
    }

    /// Advance to a repaired event's post-image under the row's resulting PK;
    /// a vacated old PK is dead as a repair source.
    fn advance(
        &mut self,
        table_metadata: &TableMetadata,
        overlay_key: EcoString,
        row: &[Option<ByteString>],
        pk_changed: bool,
    ) {
        let mut post = Vec::new();
        toastable_values_extend(table_metadata, row, &mut post);
        let result_key = if pk_changed {
            self.entries.insert(overlay_key, OverlayEntry::Deleted);
            pk_body_render(table_metadata, row)
        } else {
            Some(overlay_key)
        };
        if let Some(key) = result_key {
            self.entries.insert(key, OverlayEntry::Values(post));
        }
    }

    /// The fallback handler deletes the row: later queued events of either PK
    /// must not repair from the (stale) pre-batch image.
    fn tombstone(
        &mut self,
        table_metadata: Option<&TableMetadata>,
        overlay_key: EcoString,
        row: &[Option<ByteString>],
        pk_changed: bool,
    ) {
        self.entries.insert(overlay_key, OverlayEntry::Deleted);
        if !pk_changed {
            return;
        }
        if let Some(key) = table_metadata.and_then(|t| pk_body_render(t, row)) {
            self.entries.insert(key, OverlayEntry::Deleted);
        }
    }

    /// Flush the chain's final post-images. `or_insert`: a pass-1 entry always
    /// stems from a complete write later in arrival order than every queued
    /// event, so it must win; tombstones were already recorded eagerly (pass-1
    /// Queue branch for vacated old PKs, `toast_fallback_mark` for
    /// fallen-back rows).
    fn flush(self, core: &mut WriterCore, relation_oid: Oid) {
        for (key, entry) in self.entries {
            if matches!(entry, OverlayEntry::Values(_)) {
                core.batch_toast_overlay
                    .entry((relation_oid, key))
                    .or_insert(entry);
            }
        }
    }
}

fn column_list_push<'a>(sql: &mut String, columns: impl Iterator<Item = &'a EcoString>) {
    for (i, column) in columns.enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(column);
    }
}

/// The deduplicated `IN` list of queued source PKs, as tuples for a
/// multi-column PK.
fn pk_literals_push(sql: &mut String, pendings: &[PendingRepairSlot], multi_pk: bool) {
    let mut seen: HashSet<&[ByteString]> = HashSet::new();
    let unique = pendings.iter().filter(|p| seen.insert(p.raw_pk.as_slice()));
    for (i, p) in unique.enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        pk_tuple_push(sql, &p.raw_pk, multi_pk);
    }
}

fn pk_tuple_push(sql: &mut String, raw_pk: &[ByteString], multi_pk: bool) {
    if multi_pk {
        sql.push('(');
    }
    for (i, value) in raw_pk.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&escape::escape_literal(value));
    }
    if multi_pk {
        sql.push(')');
    }
}

/// `SELECT <pk cols>, <toastable cols> FROM rel WHERE <pk> IN (…)`.
fn toast_lookup_sql(
    table_metadata: &TableMetadata,
    pk_columns: &[&EcoString],
    toastable: &[(usize, &EcoString)],
    pendings: &[PendingRepairSlot],
) -> Option<String> {
    let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
    sql.push_str("SELECT ");
    column_list_push(
        &mut sql,
        pk_columns
            .iter()
            .copied()
            .chain(toastable.iter().map(|(_, name)| *name)),
    );
    let _ = write!(
        sql,
        " FROM {}.{} WHERE ",
        table_metadata.schema, table_metadata.name
    );
    let multi_pk = pk_columns.len() > 1;
    if multi_pk {
        sql.push('(');
        column_list_push(&mut sql, pk_columns.iter().copied());
        sql.push(')');
    } else {
        sql.push_str(pk_columns.first()?);
    }
    sql.push_str(" IN (");
    pk_literals_push(&mut sql, pendings, multi_pk);
    sql.push(')');
    Some(sql)
}

/// Decode lookup result rows into raw PK → toastable values.
fn toast_lookup_rows(
    msgs: Vec<SimpleQueryMessage>,
    pk_count: usize,
    toastable: &[(usize, &EcoString)],
) -> LookupRows {
    let mut rows = HashMap::new();
    for msg in msgs {
        let SimpleQueryMessage::Row(row) = msg else {
            continue;
        };
        let key: Option<Vec<ByteString>> = (0..pk_count)
            .map(|i| row.get(i).map(ByteString::from))
            .collect();
        let Some(key) = key else { continue };
        let values: ToastableValues = toastable
            .iter()
            .enumerate()
            .map(|(j, (pos, _))| (*pos, row.get(pk_count + j).map(ByteString::from)))
            .collect();
        rows.insert(key, values);
    }
    rows
}

impl WriterCdc {
    /// Defensive (PGC-264): an unchanged-toast marker in a tuple that cannot
    /// carry one per the pgoutput protocol (insert images, delete/key tuples).
    /// The event is dropped by the caller; invalidating every query over the
    /// relation keeps that safe.
    pub(super) fn toast_unexpected_invalidate(
        core: &mut WriterCore,
        relation_oid: Oid,
        tuple_kind: &str,
    ) {
        error!(
            relation_oid = %relation_oid,
            tuple_kind, "unexpected unchanged-toast marker; invalidating relation queries"
        );
        if let Some(update_queries) = core.cache.update_queries.get(&relation_oid) {
            core.frame_invalidations
                .extend(update_queries.queries.values().map(|q| q.fingerprint));
        }
        crate::metrics::handles().cdc.toast_fallbacks.increment(1);
    }

    /// Record a complete in-batch write of a row into the toast overlay
    /// (PGC-264): later toasted updates of the same PK repair from these
    /// values instead of the (now stale) pre-batch committed image. Gated on
    /// the relation having a toastable column — only those can see a toasted
    /// update, so only they ever consult the overlay.
    fn toast_overlay_record_write(
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) {
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return;
        };
        if !table_metadata.has_toastable_column() {
            return;
        }
        let Some(key) = pk_body_render(table_metadata, row_data) else {
            return;
        };
        // Reuse a pooled Vec (field access keeps the `core.cache` borrow of
        // `table_metadata` disjoint from the pool and overlay borrows).
        let mut values = core.toast_overlay_pool.pop().unwrap_or_default();
        toastable_values_extend(table_metadata, row_data, &mut values);
        let displaced = core
            .batch_toast_overlay
            .insert((relation_oid, key), OverlayEntry::Values(values));
        core.toast_overlay_recycle(displaced);
    }

    /// Tombstone a PK in the toast overlay (PGC-264): the row was deleted (or
    /// its old key vacated) this batch, so its pre-batch image must not be
    /// used as a repair source.
    fn toast_overlay_record_delete(
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) {
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return;
        };
        if !table_metadata.has_toastable_column() {
            return;
        }
        if let Some(key) = pk_body_render(table_metadata, row_data) {
            let displaced = core
                .batch_toast_overlay
                .insert((relation_oid, key), OverlayEntry::Deleted);
            core.toast_overlay_recycle(displaced);
        }
    }

    /// Pass-1 overlay bookkeeping for a non-toasted event: complete writes
    /// record their toastable values per PK, deletes (and vacated old PKs)
    /// tombstone, truncates guard the relation and drop its prior entries.
    fn toast_overlay_event_track(core: &mut WriterCore, event: &FrameRowEvent) {
        match event {
            FrameRowEvent::Insert {
                relation_oid,
                row_data,
            } => Self::toast_overlay_record_write(core, *relation_oid, row_data),
            FrameRowEvent::Update {
                relation_oid,
                key_data,
                new_row_data,
                toast: ToastState::Complete,
            } => {
                Self::toast_overlay_record_write(core, *relation_oid, new_row_data);
                // Under REPLICA IDENTITY FULL `key_data` is present on every
                // update; tombstone only a genuinely vacated PK.
                let pk_changed = core
                    .cache
                    .tables
                    .get1(relation_oid)
                    .is_some_and(|t| update_pk_changed(t, key_data, new_row_data));
                if pk_changed {
                    Self::toast_overlay_record_delete(core, *relation_oid, key_data);
                }
            }
            FrameRowEvent::Delete {
                relation_oid,
                row_data,
            } => Self::toast_overlay_record_delete(core, *relation_oid, row_data),
            FrameRowEvent::Truncate { relation_oids } => {
                for &oid in relation_oids.iter() {
                    core.toast_overlay_relation_invalidate(oid);
                }
            }
            FrameRowEvent::Update {
                toast: ToastState::Pending(_) | ToastState::Unrepaired(_),
                ..
            }
            | FrameRowEvent::Boundary { .. } => {}
        }
    }

    /// Resolve every toast-pending update in one replay's events (PGC-264),
    /// in two passes over the arrival order:
    ///
    /// 1. Maintain the batch toast overlay (`toast_overlay_event_track`;
    ///    writes after a truncate re-arm repair). A toasted update whose
    ///    source PK (the old PK when the PK changed) has an overlay value
    ///    repairs from it in memory; a tombstone or guarded relation falls
    ///    back; anything else queues for the lookup pass.
    /// 2. One batched lookup per relation against the pre-batch committed
    ///    image. The only in-batch writes pass 1 couldn't see for a queued
    ///    event's source PK are earlier queued toasted updates themselves
    ///    (a complete write or delete in between would have armed pass-1
    ///    repair or fallback), so repairs chain in arrival order: each
    ///    repaired event's post-image is the repair source for the next
    ///    same-PK event, seeded from the lookup. Absent rows (and lookup
    ///    failures — the slot is already acked at decode time, PGC-147, so
    ///    there is no redelivery to lean on) fall back. The chain's final
    ///    post-images then land in the overlay without displacing pass-1
    ///    entries, which always stem from arrival-later complete writes.
    ///
    /// No `ToastState::Pending` remains in `events` afterwards.
    pub(super) async fn toast_repair_events(core: &mut WriterCore, events: &mut [FrameRowEvent]) {
        let mut pending: HashMap<Oid, Vec<PendingRepairSlot>> = HashMap::new();
        for (idx, event) in events.iter_mut().enumerate() {
            match PendingUpdate::of(event) {
                Some(update) => Self::toast_resolve_from_overlay(core, &mut pending, idx, update),
                None => Self::toast_overlay_event_track(core, event),
            }
        }
        for (relation_oid, pendings) in pending {
            Self::toast_repair_relation(core, events, relation_oid, pendings).await;
        }
    }

    /// Pass-1 resolution of one pending update: repair from the overlay,
    /// fall back, or queue for the batched lookup (leaving it pending). Also
    /// performs the event's own overlay bookkeeping.
    fn toast_resolve_from_overlay(
        core: &mut WriterCore,
        pending: &mut HashMap<Oid, Vec<PendingRepairSlot>>,
        event_idx: usize,
        update: PendingUpdate<'_>,
    ) {
        let relation_oid = update.relation_oid;
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            // Unknown relation: handlers no-op on it either way.
            *update.toast = ToastState::Complete;
            return;
        };
        let pk_changed = update.pk_changed(table_metadata);
        let resolution = toast_resolution(core, table_metadata, &update, pk_changed);
        // A vacated old PK is gone whatever happens to the new one; on a
        // queued update the new PK's overlay entry is written by pass 2.
        if pk_changed {
            Self::toast_overlay_record_delete(core, relation_oid, update.key_data);
        }
        match resolution {
            ToastResolution::Repaired(values) => {
                for (t, v) in values {
                    if let Some(cell) = update.new_row_data.get_mut(t) {
                        *cell = v;
                    }
                }
                crate::metrics::handles().cdc.toast_repairs.increment(1);
                Self::toast_overlay_record_write(core, relation_oid, update.new_row_data);
                *update.toast = ToastState::Complete;
            }
            ToastResolution::Queue {
                overlay_key,
                raw_pk,
            } => pending
                .entry(relation_oid)
                .or_default()
                .push(PendingRepairSlot {
                    event_idx,
                    overlay_key,
                    raw_pk,
                }),
            ToastResolution::Fallback => Self::toast_fallback_mark(core, update),
        }
    }

    /// Pass 2 for one relation: one batched lookup, then resolve its queued
    /// events in arrival order along the repair chain.
    async fn toast_repair_relation(
        core: &mut WriterCore,
        events: &mut [FrameRowEvent],
        relation_oid: Oid,
        pendings: Vec<PendingRepairSlot>,
    ) {
        let lookup = Self::toast_lookup_batch(core, relation_oid, &pendings).await;
        let mut chain = ToastChain::new(lookup);
        for slot in pendings {
            let Some(update) = events.get_mut(slot.event_idx).and_then(PendingUpdate::of) else {
                continue;
            };
            Self::toast_chain_resolve(core, &mut chain, slot, update);
        }
        chain.flush(core, relation_oid);
    }

    /// Repair one queued update from the chain (or the lookup), or fall back.
    fn toast_chain_resolve(
        core: &mut WriterCore,
        chain: &mut ToastChain,
        slot: PendingRepairSlot,
        update: PendingUpdate<'_>,
    ) {
        let repaired = chain.source(&slot).is_some_and(|values| {
            toasted_values_apply(values, pending_positions(update.toast), update.new_row_data)
        });
        let table_metadata = core.cache.tables.get1(&update.relation_oid);
        let pk_changed = table_metadata.is_some_and(|t| update.pk_changed(t));
        if repaired {
            crate::metrics::handles().cdc.toast_repairs.increment(1);
            if let Some(table_metadata) = table_metadata {
                chain.advance(
                    table_metadata,
                    slot.overlay_key,
                    update.new_row_data,
                    pk_changed,
                );
            }
            *update.toast = ToastState::Complete;
            return;
        }
        chain.tombstone(
            table_metadata,
            slot.overlay_key,
            update.new_row_data,
            pk_changed,
        );
        Self::toast_fallback_mark(core, update);
    }

    /// Mark an unrepairable update `Unrepaired`, tombstoning its
    /// (to-be-deleted) row in the overlay.
    fn toast_fallback_mark(core: &mut WriterCore, update: PendingUpdate<'_>) {
        let toasted_columns = toasted_column_names(
            core.cache.tables.get1(&update.relation_oid),
            pending_positions(update.toast),
        );
        // The fallback handler deletes the row; later in-batch repairs must
        // not trust either image.
        Self::toast_overlay_record_delete(core, update.relation_oid, update.new_row_data);
        crate::metrics::handles().cdc.toast_fallbacks.increment(1);
        debug!(relation_oid = %update.relation_oid, "toast repair fell back");
        *update.toast = ToastState::Unrepaired(toasted_columns);
    }

    /// One batched pre-batch-image lookup for a relation's queued repairs,
    /// deduplicated by PK. Returns raw-PK → toastable `(position, value)`
    /// pairs, or `None` if the lookup failed (callers fall back).
    async fn toast_lookup_batch(
        core: &WriterCore,
        relation_oid: Oid,
        pendings: &[PendingRepairSlot],
    ) -> Option<LookupRows> {
        let table_metadata = core.cache.tables.get1(&relation_oid)?;
        let pk_columns: Vec<&EcoString> = table_metadata
            .primary_key_columns
            .iter()
            .map(|pk_column| {
                table_metadata
                    .columns
                    .get(pk_column.as_str())
                    .map(|c| &c.name)
            })
            .collect::<Option<Vec<_>>>()?;
        let toastable: Vec<(usize, &EcoString)> = table_metadata
            .columns
            .iter()
            .filter(|c| c.is_toastable())
            .map(|c| (c.index(), &c.name))
            .collect();
        let sql = toast_lookup_sql(table_metadata, &pk_columns, &toastable, pendings)?;

        match core.db_cache.simple_query(&sql).await {
            Ok(msgs) => Some(toast_lookup_rows(msgs, pk_columns.len(), &toastable)),
            Err(e) => {
                error!(
                    relation_oid = %relation_oid,
                    "batched toast repair lookup failed, falling back to invalidation: {e}"
                );
                None
            }
        }
    }
}
