use crate::catalog::Oid;
use crate::pg::Lsn;
use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use ecow::EcoString;
use postgres_protocol::escape;
use tokio_postgres::SimpleQueryMessage;
use tracing::{debug, error};

use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;

use super::super::core::{FrameRowEvent, ToastOverlayEntry, WriterCore};
use super::super::staging::pk_body_render;

use super::*;

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
    Repaired(Vec<(usize, Option<ByteString>)>),
    /// No in-batch state: queue for the batched lookup, keyed by these raw
    /// source-PK values.
    Queue(Vec<ByteString>),
    Fallback,
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
        Self::toastable_values_extend(table_metadata, row_data, &mut values);
        let displaced = core
            .batch_toast_overlay
            .insert((relation_oid, key), ToastOverlayEntry::Values(values));
        core.toast_overlay_recycle(displaced);
    }

    /// Collect a row image's toastable-column `(position, value)` pairs into
    /// `values` — the payload of a [`ToastOverlayEntry::Values`].
    fn toastable_values_extend(
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
        values: &mut Vec<(usize, Option<ByteString>)>,
    ) {
        values.extend(
            table_metadata
                .columns
                .iter()
                .filter(|c| c.is_toastable())
                .map(|c| (c.index(), row_data.get(c.index()).cloned().flatten())),
        );
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
                .insert((relation_oid, key), ToastOverlayEntry::Deleted);
            core.toast_overlay_recycle(displaced);
        }
    }

    /// Resolve every `UpdateToasted` in one replay's events (PGC-264), in two
    /// passes over the arrival order:
    ///
    /// 1. Maintain the batch toast overlay: complete writes record their
    ///    toastable values per PK, deletes (and vacated old PKs) tombstone,
    ///    truncates guard the relation and drop its prior entries (writes
    ///    after the truncate re-arm repair). A toasted update whose source PK
    ///    (the old PK when the PK changed) has an overlay value repairs from
    ///    it in memory; a tombstone or guarded relation falls back; anything
    ///    else queues for the lookup pass.
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
    /// No `UpdateToasted` remains in `events` afterwards.
    pub(super) async fn toast_repair_events(core: &mut WriterCore, events: &mut [FrameRowEvent]) {
        let metrics = &crate::metrics::handles().cdc;
        let mut pending: HashMap<Oid, Vec<PendingRepairSlot>> = HashMap::new();

        for (idx, event) in events.iter_mut().enumerate() {
            match event {
                FrameRowEvent::Insert {
                    relation_oid,
                    row_data,
                } => {
                    Self::toast_overlay_record_write(core, *relation_oid, row_data);
                }
                FrameRowEvent::Update {
                    relation_oid,
                    key_data,
                    new_row_data,
                } => {
                    Self::toast_overlay_record_write(core, *relation_oid, new_row_data);
                    if !key_data.is_empty() {
                        Self::toast_overlay_record_delete(core, *relation_oid, key_data);
                    }
                }
                FrameRowEvent::Delete {
                    relation_oid,
                    row_data,
                } => {
                    Self::toast_overlay_record_delete(core, *relation_oid, row_data);
                }
                FrameRowEvent::Truncate { relation_oids } => {
                    for &oid in relation_oids.iter() {
                        core.toast_overlay_relation_invalidate(oid);
                    }
                }
                FrameRowEvent::Boundary { .. } | FrameRowEvent::UpdateToastFallback { .. } => {}
                FrameRowEvent::UpdateToasted { .. } => {
                    let FrameRowEvent::UpdateToasted {
                        relation_oid,
                        key_data,
                        mut new_row_data,
                        toasted,
                    } = std::mem::replace(
                        event,
                        FrameRowEvent::Boundary {
                            commit_lsn: Lsn::from_raw(0),
                        },
                    )
                    else {
                        continue;
                    };
                    *event = Self::toast_resolve_from_overlay(
                        core,
                        &mut pending,
                        idx,
                        relation_oid,
                        key_data,
                        &mut new_row_data,
                        toasted,
                    );
                    // `new_row_data` was moved back inside the resolved event.
                }
            }
        }

        // Pass 2: batched lookups, one statement per relation. `chain` holds
        // the per-PK toastable state as it advances through this relation's
        // queued events in arrival order — a queued event is an in-batch
        // write the overlay never saw, so the next same-PK event must repair
        // from its post-image, not the pre-batch image.
        for (relation_oid, pendings) in pending {
            let lookup = Self::toast_lookup_batch(core, relation_oid, &pendings).await;
            let mut chain: HashMap<EcoString, ToastOverlayEntry> = HashMap::new();
            for p in pendings {
                let Some(slot) = events.get_mut(p.event_idx) else {
                    continue;
                };
                let FrameRowEvent::UpdateToasted {
                    relation_oid,
                    key_data,
                    mut new_row_data,
                    toasted,
                } = std::mem::replace(
                    slot,
                    FrameRowEvent::Boundary {
                        commit_lsn: Lsn::from_raw(0),
                    },
                )
                else {
                    continue;
                };
                let source_values = match chain.get(&p.overlay_key) {
                    Some(ToastOverlayEntry::Values(values)) => Some(values),
                    Some(ToastOverlayEntry::Deleted) => None,
                    None => lookup.as_ref().and_then(|rows| rows.get(&p.raw_pk)),
                };
                let mut repaired = source_values.is_some();
                if let Some(values) = source_values {
                    for &t in &toasted {
                        match values.iter().find(|(pos, _)| *pos == t) {
                            Some((_, v)) => {
                                if let Some(cell) = new_row_data.get_mut(t) {
                                    *cell = v.clone();
                                }
                            }
                            None => repaired = false,
                        }
                    }
                }

                let pk_changed = !key_data.is_empty();
                *slot = if repaired {
                    metrics.toast_repairs.increment(1);
                    // Advance the chain to this event's post-image under the
                    // row's resulting PK; a vacated old PK is dead as a
                    // repair source.
                    if let Some(table_metadata) = core.cache.tables.get1(&relation_oid) {
                        let mut post = Vec::new();
                        Self::toastable_values_extend(table_metadata, &new_row_data, &mut post);
                        let result_key = if pk_changed {
                            pk_body_render(table_metadata, &new_row_data)
                        } else {
                            Some(p.overlay_key.clone())
                        };
                        if pk_changed {
                            chain.insert(p.overlay_key.clone(), ToastOverlayEntry::Deleted);
                        }
                        if let Some(key) = result_key {
                            chain.insert(key, ToastOverlayEntry::Values(post));
                        }
                    }
                    FrameRowEvent::Update {
                        relation_oid,
                        key_data,
                        new_row_data,
                    }
                } else {
                    // The fallback handler deletes the row: later queued
                    // events of either PK must not repair from the (stale)
                    // pre-batch image.
                    chain.insert(p.overlay_key.clone(), ToastOverlayEntry::Deleted);
                    if pk_changed
                        && let Some(table_metadata) = core.cache.tables.get1(&relation_oid)
                        && let Some(key) = pk_body_render(table_metadata, &new_row_data)
                    {
                        chain.insert(key, ToastOverlayEntry::Deleted);
                    }
                    Self::toast_fallback_build(core, relation_oid, key_data, new_row_data, &toasted)
                };
            }
            // Flush the chain's final post-images. `or_insert`: a pass-1
            // entry always stems from a complete write later in arrival
            // order than every queued event, so it must win; tombstones were
            // already recorded eagerly (pass-1 Queue branch for vacated old
            // PKs, `toast_fallback_build` for fallen-back rows).
            for (key, entry) in chain {
                if matches!(entry, ToastOverlayEntry::Values(_)) {
                    core.batch_toast_overlay
                        .entry((relation_oid, key))
                        .or_insert(entry);
                }
            }
        }
    }

    /// Pass-1 resolution of one `UpdateToasted`: repair from the overlay,
    /// fall back, or queue for the batched lookup (returning the event
    /// unchanged). Also performs the event's own overlay bookkeeping.
    #[allow(clippy::too_many_arguments)]
    fn toast_resolve_from_overlay(
        core: &mut WriterCore,
        pending: &mut HashMap<Oid, Vec<PendingRepairSlot>>,
        event_idx: usize,
        relation_oid: Oid,
        key_data: Vec<Option<ByteString>>,
        new_row_data: &mut Vec<Option<ByteString>>,
        toasted: Vec<usize>,
    ) -> FrameRowEvent {
        let metrics = &crate::metrics::handles().cdc;
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            // Unknown relation: handlers no-op on it either way.
            return FrameRowEvent::Update {
                relation_oid,
                key_data,
                new_row_data: std::mem::take(new_row_data),
            };
        };

        // The cached copy the unchanged-toast marker refers to lives under
        // the row's PRE-image key: when this UPDATE changed the PK
        // (`key_data` non-empty), the source row is the old PK.
        let pk_changed = !key_data.is_empty();
        let source_row: &[Option<ByteString>] = if pk_changed { &key_data } else { new_row_data };
        let source_pk = pk_body_render(table_metadata, source_row);

        let resolution = match &source_pk {
            None => ToastResolution::Fallback,
            Some(key) => match core.batch_toast_overlay.get(&(relation_oid, key.clone())) {
                Some(ToastOverlayEntry::Values(values)) => {
                    let mut complete = true;
                    let mut repaired: Vec<(usize, Option<ByteString>)> =
                        Vec::with_capacity(toasted.len());
                    for &t in &toasted {
                        match values.iter().find(|(pos, _)| *pos == t) {
                            Some((_, v)) => repaired.push((t, v.clone())),
                            None => complete = false,
                        }
                    }
                    if complete {
                        ToastResolution::Repaired(repaired)
                    } else {
                        ToastResolution::Fallback
                    }
                }
                Some(ToastOverlayEntry::Deleted) => ToastResolution::Fallback,
                None if core.batch_toast_guard_oids.contains(&relation_oid) => {
                    ToastResolution::Fallback
                }
                None => {
                    // Raw PK values for matching the lookup result; a NULL PK
                    // value can never match a lookup row, so fall back.
                    let raw: Option<Vec<ByteString>> = table_metadata
                        .primary_key_columns
                        .iter()
                        .map(|pk_column| {
                            table_metadata
                                .columns
                                .get(pk_column.as_str())
                                .and_then(|c| source_row.get(c.index()).cloned().flatten())
                        })
                        .collect();
                    match raw {
                        Some(raw_pk) => ToastResolution::Queue(raw_pk),
                        None => ToastResolution::Fallback,
                    }
                }
            },
        };

        match resolution {
            ToastResolution::Repaired(values) => {
                for (t, v) in values {
                    if let Some(slot) = new_row_data.get_mut(t) {
                        *slot = v;
                    }
                }
                metrics.toast_repairs.increment(1);
                let new_row_data = std::mem::take(new_row_data);
                Self::toast_overlay_record_write(core, relation_oid, &new_row_data);
                if pk_changed {
                    Self::toast_overlay_record_delete(core, relation_oid, &key_data);
                }
                FrameRowEvent::Update {
                    relation_oid,
                    key_data,
                    new_row_data,
                }
            }
            ToastResolution::Queue(raw_pk) => {
                // The vacated old PK is gone whatever pass 2 decides; the new
                // PK's overlay entry is written by pass 2.
                if pk_changed {
                    Self::toast_overlay_record_delete(core, relation_oid, &key_data);
                }
                pending
                    .entry(relation_oid)
                    .or_default()
                    .push(PendingRepairSlot {
                        event_idx,
                        overlay_key: source_pk.expect("queued resolution rendered a source pk"),
                        raw_pk,
                    });
                FrameRowEvent::UpdateToasted {
                    relation_oid,
                    key_data,
                    new_row_data: std::mem::take(new_row_data),
                    toasted,
                }
            }
            ToastResolution::Fallback => {
                if pk_changed {
                    Self::toast_overlay_record_delete(core, relation_oid, &key_data);
                }
                Self::toast_fallback_build(
                    core,
                    relation_oid,
                    key_data,
                    std::mem::take(new_row_data),
                    &toasted,
                )
            }
        }
    }

    /// Build the conservative fallback event for an unrepairable toasted
    /// update, tombstoning its (to-be-deleted) row in the overlay.
    fn toast_fallback_build(
        core: &mut WriterCore,
        relation_oid: Oid,
        key_data: Vec<Option<ByteString>>,
        new_row_data: Vec<Option<ByteString>>,
        toasted: &[usize],
    ) -> FrameRowEvent {
        let toasted_columns: Vec<EcoString> = core
            .cache
            .tables
            .get1(&relation_oid)
            .map(|table_metadata| {
                table_metadata
                    .columns
                    .iter()
                    .filter(|c| toasted.contains(&c.index()))
                    .map(|c| c.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        // The fallback handler deletes the row; later in-batch repairs must
        // not trust either image.
        Self::toast_overlay_record_delete(core, relation_oid, &new_row_data);
        crate::metrics::handles().cdc.toast_fallbacks.increment(1);
        debug!(relation_oid = %relation_oid, "toast repair fell back");
        FrameRowEvent::UpdateToastFallback {
            relation_oid,
            key_data,
            new_row_data,
            toasted_columns,
        }
    }

    /// One batched pre-batch-image lookup for a relation's queued repairs:
    /// `SELECT <pk cols>, <toastable cols> FROM rel WHERE <pk> IN (…)`,
    /// deduplicated by PK. Returns raw-PK → toastable `(position, value)`
    /// pairs, or `None` if the lookup failed (callers fall back).
    async fn toast_lookup_batch(
        core: &WriterCore,
        relation_oid: Oid,
        pendings: &[PendingRepairSlot],
    ) -> Option<HashMap<Vec<ByteString>, Vec<(usize, Option<ByteString>)>>> {
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

        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        sql.push_str("SELECT ");
        for (i, column) in pk_columns
            .iter()
            .copied()
            .chain(toastable.iter().map(|(_, name)| *name))
            .enumerate()
        {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(column);
        }
        let _ = write!(
            sql,
            " FROM {}.{} WHERE ",
            table_metadata.schema, table_metadata.name
        );
        let multi_pk = pk_columns.len() > 1;
        if multi_pk {
            sql.push('(');
            for (i, column) in pk_columns.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(column);
            }
            sql.push(')');
        } else {
            sql.push_str(pk_columns.first()?);
        }
        sql.push_str(" IN (");
        let mut seen: HashSet<&[ByteString]> = HashSet::new();
        let mut first = true;
        for p in pendings {
            if !seen.insert(p.raw_pk.as_slice()) {
                continue;
            }
            if !first {
                sql.push_str(", ");
            }
            first = false;
            if multi_pk {
                sql.push('(');
            }
            for (i, value) in p.raw_pk.iter().enumerate() {
                if i > 0 {
                    sql.push_str(", ");
                }
                sql.push_str(&escape::escape_literal(value));
            }
            if multi_pk {
                sql.push(')');
            }
        }
        sql.push(')');

        match core.db_cache.simple_query(&sql).await {
            Ok(msgs) => {
                let mut rows = HashMap::new();
                for msg in msgs {
                    let SimpleQueryMessage::Row(row) = msg else {
                        continue;
                    };
                    let key: Option<Vec<ByteString>> = (0..pk_columns.len())
                        .map(|i| row.get(i).map(ByteString::from))
                        .collect();
                    let Some(key) = key else { continue };
                    let values: Vec<(usize, Option<ByteString>)> = toastable
                        .iter()
                        .enumerate()
                        .map(|(j, (pos, _))| {
                            (*pos, row.get(pk_columns.len() + j).map(ByteString::from))
                        })
                        .collect();
                    rows.insert(key, values);
                }
                Some(rows)
            }
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
