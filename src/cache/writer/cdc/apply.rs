//! Decide-pass handlers for replayed row events: each CDC INSERT / UPDATE /
//! DELETE / TRUNCATE is checked against the relation's cached queries
//! (invalidate, maintain in place, or skip), and its cache-table write is
//! buffered into the open frame (PGC-228).

use std::time::Instant;

use ecow::EcoString;
use tracing::{error, instrument, trace};

use super::segment_eval::BatchEvalView;
use super::{
    CdcOperation, MembershipRow, RelationRow, RelationUpdate, RowEvent, WriterCdc,
    eval_candidates_into, memo_frame_accumulate, toast_fallback_structural_invalidate,
    update_queries_check_invalidate, update_query_matches_locally,
};
use crate::cache::update_query::{RowChanges, UpdateEvalStrategy, UpdateQuery};
use crate::cache::writer::core::WriterCore;
use crate::cache::writer::staging::pk_body_render;
use crate::cache::{CacheError, CacheResult, ReportExt};
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::{Fingerprint, FingerprintSet};

/// Where an UPDATE's old-image candidates are probed from.
enum OldImageProbe<'a> {
    /// A complete old image: probe its real values.
    Exact(&'a [Option<ByteString>]),
    /// Only the PK is known: wildcard over the other columns.
    PkOnly(&'a [Option<ByteString>]),
}

/// Old-image candidates, probed once and reused by both the symmetric memo
/// eviction and the MV removed-row dirty-mark (ADR-045 — avoids a second
/// eval_index probe per UPDATE). When the recovery ladder resolved the old
/// image (PGC-255), probe it with real values — 6.5× fewer candidates than the
/// PK-only wildcard fallback, same never-under-return.
fn old_image_probe<'a>(
    update: RelationUpdate<'a>,
    old_image: Option<&'a [Option<ByteString>]>,
    replica_identity_full: bool,
    pk_changed: bool,
) -> OldImageProbe<'a> {
    match old_image {
        Some(old_image) => OldImageProbe::Exact(old_image),
        // REPLICA IDENTITY FULL: the 'O' tuple IS the authoritative complete
        // old image — probe it directly (rung 0; the overlay ladder is skipped
        // for FULL relations in the segment pre-pass).
        None if replica_identity_full && !update.key_data.is_empty() => {
            OldImageProbe::Exact(update.key_data)
        }
        None => OldImageProbe::PkOnly(update.removed_probe_row(pk_changed)),
    }
}

fn old_image_candidates_into(
    core: &WriterCore,
    relation_oid: Oid,
    probe: OldImageProbe<'_>,
    out: &mut FingerprintSet,
) {
    match probe {
        OldImageProbe::Exact(row) => eval_candidates_into(core, relation_oid, row, out),
        OldImageProbe::PkOnly(row) => {
            core.eval_candidates_removed_into(relation_oid, row, true, out)
        }
    }
}

/// The UPDATE's new-image candidates, shared by the invalidation check and
/// the in-place matcher, and its old-image candidates. Memo eviction is
/// symmetric (a row leaving a query makes its memo stale), so it needs
/// new ∪ old. Both sets are borrowed from the scratch pool and returned by the
/// caller, so their backing allocations are reused across rows
/// (PGC-341/344).
fn update_candidates_probe(
    core: &mut WriterCore,
    update: RelationUpdate<'_>,
    old_image: OldImageProbe<'_>,
) -> (FingerprintSet, FingerprintSet) {
    let mut local_candidates = core.candidate_set_take();
    eval_candidates_into(
        core,
        update.relation_oid,
        update.new_row_data,
        &mut local_candidates,
    );
    let mut old_candidates = core.candidate_set_take();
    old_image_candidates_into(core, update.relation_oid, old_image, &mut old_candidates);
    (local_candidates, old_candidates)
}

/// An UPDATE's row-change classification: borrowed from the segment's batched
/// eval (PGC-241 stage 3), or fetched by the per-row SELECT.
enum UpdateRowChanges<'a> {
    Batched(Option<&'a RowChanges>),
    Fetched(Option<RowChanges>),
}

impl UpdateRowChanges<'_> {
    fn get(&self) -> Option<&RowChanges> {
        match self {
            Self::Batched(row_changes) => *row_changes,
            Self::Fetched(row_changes) => row_changes.as_ref(),
        }
    }
}

/// PGC-227: when no cached query over this relation can have its UPDATE
/// invalidation depend on which columns changed or whether the row is cached,
/// both `query_row_changes` (a SELECT round-trip) and the invalidation check
/// are provably no-ops — skip them.
fn relation_needs_change_eval(core: &WriterCore, relation_oid: Oid) -> bool {
    core.cache
        .update_queries
        .get(&relation_oid)
        .is_some_and(|q| q.needs_change_eval())
}

/// Whether the batch deleted this row's PK after the pre-batch snapshot that
/// the row-change lookups read (PGC-242).
fn batch_deleted_contains(
    core: &WriterCore,
    relation_oid: Oid,
    row_data: &[Option<ByteString>],
) -> bool {
    !core.batch_deleted_pks.is_empty()
        && core
            .cache
            .tables
            .get1(&relation_oid)
            .and_then(|table_metadata| pk_body_render(table_metadata, row_data))
            .is_some_and(|key| core.batch_deleted_pks.contains(&(relation_oid, key)))
}

fn update_invalidations_collect(
    core: &WriterCore,
    update: RelationUpdate<'_>,
    row_changes: Option<&RowChanges>,
    candidates: &FingerprintSet,
) -> Vec<Fingerprint> {
    trace!("row_changes {:?}", row_changes);
    let update_event = RowEvent {
        row_data: update.new_row_data,
        key_data: Some(update.key_data),
        operation: CdcOperation::Upsert,
        row_changes,
    };
    let fp_list =
        update_queries_check_invalidate(core, update.relation_oid, &update_event, candidates);
    trace!("invalidation_count {}", fp_list.len());
    fp_list
}

/// What the toast fallback does with one cached query over the relation.
enum ToastFallbackDecision {
    Invalidate,
    /// Membership needs a PgEval round-trip; a match invalidates.
    PgEval,
    Unaffected,
}

/// Invalidate on structural sensitivity to the elided columns, or on a
/// membership match (the matched row can't be upserted).
fn toast_fallback_decide(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    event: &RowEvent,
    toasted_columns: &[EcoString],
) -> ToastFallbackDecision {
    if toast_fallback_structural_invalidate(update_query, table_metadata, event, toasted_columns) {
        return ToastFallbackDecision::Invalidate;
    }
    match update_query.eval_strategy {
        UpdateEvalStrategy::LocalEval
            if update_query_matches_locally(update_query, event.row_data) =>
        {
            ToastFallbackDecision::Invalidate
        }
        UpdateEvalStrategy::LocalEval => ToastFallbackDecision::Unaffected,
        UpdateEvalStrategy::PgEval => ToastFallbackDecision::PgEval,
    }
}

/// Under recording an in-flight population may have staged this row from
/// before the event; the merge cannot repair it (incomplete image) nor omit it
/// (alive at origin). Record the key so the merge aborts exactly the
/// populations whose staging holds it (PGC-464) instead of invalidating every
/// query over the relation.
fn toast_stale_key_record(
    core: &mut WriterCore,
    relation_oid: Oid,
    row_data: &[Option<ByteString>],
) {
    let Some(key) = core
        .cache
        .tables
        .get1(&relation_oid)
        .and_then(|table_metadata| pk_body_render(table_metadata, row_data))
    else {
        return;
    };
    core.frame_toast_stale_keys.push((relation_oid, key));
}

/// Buffer a PK-qualified delete of `row_data` from the relation's cache table
/// into the open frame (PGC-228), opening the frame txn if needed. The
/// relation is known-present by the time a handler reaches a delete, so a
/// missing entry is a hard `UnknownTable` error rather than a skip.
fn frame_delete_buffer(
    core: &mut WriterCore,
    relation_oid: Oid,
    row_data: &[Option<ByteString>],
    record_lost_key: bool,
) -> CacheResult<()> {
    core.frame_begin_ensure([relation_oid]);
    let table_metadata = core
        .cache
        .tables
        .get1(&relation_oid)
        .ok_or(CacheError::UnknownTable {
            oid: Some(relation_oid),
            name: None,
        })?;
    // Buffer the removed PK for any in-flight population over this relation
    // so its merge doesn't resurrect the row (PGC-250). Stamped with the
    // frame's commit LSN and recorded at CommitMark (the commit LSN isn't
    // known yet); dropped if the frame rolls back. Skip rendering the key
    // entirely when no population is recording this relation (the steady
    // state) — `record` would discard it anyway.
    let deleted_key = if record_lost_key && core.population_deleted_keys.is_recording(relation_oid)
    {
        pk_body_render(table_metadata, row_data)
    } else {
        None
    };
    // Track batch-deleted PKs so a later batched frame's row-change
    // classification sees the deletion the pre-batch snapshot can't
    // (PGC-242). Re-rendered when not already rendered for PGC-250.
    if let Some(key) = deleted_key
        .clone()
        .or_else(|| pk_body_render(table_metadata, row_data))
    {
        core.batch_deleted_pks.insert((relation_oid, key));
    }
    WriterCdc::cache_delete_into(&mut core.frame_buf, table_metadata, row_data)?;
    if let Some(key) = deleted_key {
        core.frame_deleted_keys.push((relation_oid, key));
    }
    Ok(())
}

impl WriterCdc {
    /// Buffer an unconditional upsert of `row_data` into the relation's cache
    /// table in the open frame (PGC-228), opening the frame txn if needed.
    pub(super) async fn frame_cache_upsert(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        core.frame_begin_ensure([relation_oid]);
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;
        // A re-upserted PK is present again for later batched frames' row-
        // change classification (PGC-242). Gated: rendering is free when no
        // batch deletes are outstanding.
        if !core.batch_deleted_pks.is_empty()
            && let Some(key) = pk_body_render(table_metadata, row_data)
        {
            core.batch_deleted_pks.remove(&(relation_oid, key));
        }
        Self::cache_upsert_unconditional_into(&mut core.frame_buf, table_metadata, row_data);
        self.frame_write_finish(core).await
    }

    /// Buffer a recorded delete (see [`frame_delete_buffer`]).
    async fn frame_cache_delete(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        frame_delete_buffer(core, relation_oid, row_data, true)?;
        self.frame_write_finish(core).await
    }

    /// `frame_cache_delete` without the PGC-250 lost-key record: for evicting
    /// a row that is still alive at origin while every query over the relation
    /// is being invalidated (toast fallback under active tracking, PGC-264).
    /// Recording the key would make later populations' merges omit the live
    /// row (PGC-261); the invalidations supersede the in-flight populations
    /// (generation bump), so nothing can resurrect the evicted stale version.
    async fn frame_cache_delete_unrecorded(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<()> {
        frame_delete_buffer(core, relation_oid, row_data, false)?;
        self.frame_write_finish(core).await
    }

    /// Handle INSERT operation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn handle_insert(
        &mut self,
        core: &mut WriterCore,
        row: RelationRow<'_>,
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_inserts.increment(1);
        let RelationRow {
            relation_oid,
            row_data,
        } = row;

        // CDC event for a relation we don't cache (never cached, or its
        // queries were evicted) is a benign no-op — not a frame-consistency
        // failure. Skip without erroring so it doesn't trip the reset path.
        if !core.cache.tables.contains_key1(&relation_oid) {
            return Ok(());
        }

        // Probe the eval index once; the invalidation check, the in-place
        // matcher, and the memo eviction pass below all consume this candidate
        // set (ADR-045). Borrowed from the scratch pool and returned below.
        let mut local_candidates = core.candidate_set_take();
        eval_candidates_into(core, relation_oid, row_data, &mut local_candidates);

        let insert_event = RowEvent {
            row_data,
            key_data: None,
            operation: CdcOperation::Upsert,
            row_changes: None,
        };
        let fp_list =
            update_queries_check_invalidate(core, relation_oid, &insert_event, &local_candidates);

        // Defer the actual invalidation to just before the frame COMMIT
        // (frame_invalidations_flush) so it is atomic with the maintenance
        // it accompanies rather than visible mid-frame.
        core.frame_invalidations.extend(fp_list);

        let membership_row = MembershipRow {
            row_data,
            candidates: &local_candidates,
            batch,
        };
        let matched = self
            .update_queries_execute_batch(core, relation_oid, membership_row)
            .await?;

        // Rung 3b: evict memos this insert grows into. An INSERT only adds the
        // row, so the new-row candidates are the full memo-eviction set (ADR-045).
        memo_frame_accumulate(core, relation_oid, local_candidates.iter().copied());
        core.candidate_set_return(local_candidates);

        // The inserted row is alive at origin: cancel any tracked deletion of
        // its key so population merges don't omit it (PGC-260). When the key
        // was tracked but no query matched (nothing upserted), write the row
        // anyway — its presence in the shared table is what makes the
        // cancellation safe in every merge interleaving (merges never
        // overwrite, so neither an old-snapshot nor a new-snapshot population
        // can regress it).
        let tracked = core.population_deleted_key_cancel(relation_oid, row_data);
        if tracked && !matched {
            self.frame_cache_upsert(core, relation_oid, row_data)
                .await?;
        }

        crate::metrics::handles()
            .cdc
            .handle_insert_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle UPDATE operation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn handle_update(
        &mut self,
        core: &mut WriterCore,
        update: RelationUpdate<'_>,
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_updates.increment(1);
        let relation_oid = update.relation_oid;

        // See handle_insert: an untracked relation's CDC is a benign skip.
        let Some((replica_identity_full, pk_changed)) =
            core.cache.tables.get1(&relation_oid).map(|table_metadata| {
                (
                    table_metadata.replica_identity_full,
                    update.pk_changed(table_metadata),
                )
            })
        else {
            return Ok(());
        };

        let probe = old_image_probe(
            update,
            batch.as_ref().and_then(|view| view.old_image),
            replica_identity_full,
            pk_changed,
        );
        let (local_candidates, old_candidates) = update_candidates_probe(core, update, probe);

        if relation_needs_change_eval(core, relation_oid) {
            let batched = batch.as_ref().and_then(|view| view.row_change);
            let row_changes = self
                .update_row_changes_resolve(core, update, batched)
                .await?;
            let fp_list =
                update_invalidations_collect(core, update, row_changes.get(), &local_candidates);
            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
        }

        // Rung 3b: memo eviction over the symmetric candidate set. Independent of
        // the PGC-227 invalidation skip — an in-place value change leaves the
        // query Ready but its memo stale, so memo must run regardless (ADR-045).
        // Symmetric new ∪ old candidates, chained to avoid allocating the union.
        memo_frame_accumulate(
            core,
            relation_oid,
            local_candidates
                .iter()
                .copied()
                .chain(old_candidates.iter().copied()),
        );

        let membership_row = MembershipRow {
            row_data: update.new_row_data,
            candidates: &local_candidates,
            batch,
        };
        let matched = self
            .update_queries_execute_batch(core, relation_oid, membership_row)
            .await?;
        self.update_cache_write(core, update, matched).await?;

        // Any update may move the row out of a Fresh MV's predicate, and only
        // membership *hits* dirty-mark — a row leaving query A while still
        // matching query B (`matched` above), or a PK-change with other columns
        // changed, would otherwise leave A's MV serving the departed row forever
        // (PGC-254/PGC-265; the old image isn't available to detect departure
        // precisely — PGC-255 tracks precision). Reuse the old-image candidates
        // already probed above for the memo pass; `mv_dirty_mark` self-gates
        // (Fresh and Building only).
        core.mv_dirty_mark_candidates(&old_candidates);

        core.candidate_set_return(local_candidates);
        core.candidate_set_return(old_candidates);

        // Delete the vacated old PK on a genuine PK change (under REPLICA
        // IDENTITY FULL `key_data` is present on every update, so presence
        // alone is not the signal — `update_pk_changed` compares PK columns).
        if pk_changed {
            self.frame_cache_delete(core, relation_oid, update.key_data)
                .await?;
        }

        crate::metrics::handles()
            .cdc
            .handle_update_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// The UPDATE's row-change classification: the batched result if the
    /// segment eval covered this event, else the per-row SELECT. A PK the
    /// batch deleted is classified UNCACHED so the entering-invalidation the
    /// per-frame flow produced still fires (PGC-242; lost otherwise on
    /// cross-frame delete/PK-flip + update).
    async fn update_row_changes_resolve<'b>(
        &self,
        core: &WriterCore,
        update: RelationUpdate<'_>,
        batched: Option<Option<&'b RowChanges>>,
    ) -> CacheResult<UpdateRowChanges<'b>> {
        if batch_deleted_contains(core, update.relation_oid, update.new_row_data) {
            return Ok(UpdateRowChanges::Batched(None));
        }
        match batched {
            Some(row_changes) => Ok(UpdateRowChanges::Batched(row_changes)),
            None => Ok(UpdateRowChanges::Fetched(
                self.query_row_changes(core, update.relation_oid, update.new_row_data)
                    .await?,
            )),
        }
    }

    /// Write the updated row to the shared table, or remove it.
    async fn update_cache_write(
        &mut self,
        core: &mut WriterCore,
        update: RelationUpdate<'_>,
        matched: bool,
    ) -> CacheResult<()> {
        let relation_oid = update.relation_oid;
        if matched {
            // The upserted row supersedes any tracked deletion of its key —
            // including a previously-deleted PK this row's new PK reuses
            // (PGC-260).
            core.population_deleted_key_cancel(relation_oid, update.new_row_data);
            return Ok(());
        }
        // Update-out: the row left every live predicate, but it is still
        // alive at origin. While populations are in flight, deleting it
        // and recording its key would make a later population's merge omit
        // a live row (PGC-261) — instead upsert the new version: serving
        // re-evaluates predicates so nothing serves it, an old-snapshot
        // merge can't resurrect the old version (merges never overwrite),
        // and a later population finds it present. With no population in
        // flight, keep the delete (shared-table leanness; the key record
        // would be discarded anyway).
        if core.population_deleted_keys.is_recording(relation_oid) {
            core.population_deleted_key_cancel(relation_oid, update.new_row_data);
            self.frame_cache_upsert(core, relation_oid, update.new_row_data)
                .await
        } else {
            self.frame_cache_delete(core, relation_oid, update.new_row_data)
                .await
        }
    }

    /// Conservative decide-pass path for an UPDATE whose unchanged-toast
    /// columns could not be repaired (PGC-264). The image is incomplete: it
    /// must never reach the shared cache table, and predicates over the elided
    /// columns can't be evaluated.
    ///
    /// Invalidate every query that might be affected (structural sensitivity,
    /// or membership match — the matched row can't be upserted); provably
    /// unaffected queries need nothing beyond the row's eviction.
    ///
    /// With a population recording the relation, the PGC-261 hazard applies:
    /// the row is alive at origin, so deleting it with a lost-key record would
    /// make later merges omit it, yet a population that staged the row before
    /// this event would merge a copy the cache can't repair. The row is
    /// evicted without a record and its key goes into the toast-stale set
    /// (PGC-464): the merge aborts exactly the populations whose staging holds
    /// it, and every other population — including ones registered after this
    /// event, which snapshot post-update origin — merges cleanly.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    pub(super) async fn handle_update_toast_fallback(
        &mut self,
        core: &mut WriterCore,
        update: RelationUpdate<'_>,
        toasted_columns: &[EcoString],
    ) -> CacheResult<()> {
        let relation_oid = update.relation_oid;
        // See handle_insert: an untracked relation's CDC is a benign skip.
        let Some(pk_changed) = core
            .cache
            .tables
            .get1(&relation_oid)
            .map(|table_metadata| update.pk_changed(table_metadata))
        else {
            return Ok(());
        };

        let recording = core.population_deleted_keys.is_recording(relation_oid);
        let fp_list = self
            .toast_fallback_invalidations(core, update, toasted_columns)
            .await?;
        // Only the row's live (new) PK: after a PK change the old PK's row is
        // genuinely dead at origin and its recorded delete below lets merges
        // omit it, which is exact where a stale key would only abort.
        if recording {
            toast_stale_key_record(core, relation_oid, update.new_row_data);
        }
        trace!(
            relation_oid = %relation_oid,
            recording,
            invalidations = fp_list.len(),
            "toast fallback handled"
        );
        // Deferred to frame_invalidations_flush (see handle_insert).
        core.frame_invalidations.extend(fp_list);

        // Same Fresh-MV rule as handle_update (PGC-254), narrowed via the
        // eval-index probe (PGC-292). Old non-PK values are gone, so PK-only.
        // Use the scratch pool like the other CDC paths (PGC-341/344).
        let mut removed_candidates = core.candidate_set_take();
        core.eval_candidates_removed_into(
            relation_oid,
            update.removed_probe_row(pk_changed),
            true,
            &mut removed_candidates,
        );
        core.mv_dirty_mark_candidates(&removed_candidates);
        core.candidate_set_return(removed_candidates);

        // The new-PK row is alive at origin: under recording its eviction must
        // not be recorded (see doc comment). The old PK after a PK change is
        // genuinely dead at origin, so that delete records normally.
        if recording {
            self.frame_cache_delete_unrecorded(core, relation_oid, update.new_row_data)
                .await?;
        } else {
            self.frame_cache_delete(core, relation_oid, update.new_row_data)
                .await?;
        }
        if pk_changed {
            self.frame_cache_delete(core, relation_oid, update.key_data)
                .await?;
        }
        Ok(())
    }

    /// The queries a toast-fallback UPDATE invalidates (see
    /// [`toast_fallback_decide`]), skipping ones already invalidated this
    /// frame.
    async fn toast_fallback_invalidations(
        &mut self,
        core: &WriterCore,
        update: RelationUpdate<'_>,
        toasted_columns: &[EcoString],
    ) -> CacheResult<Vec<Fingerprint>> {
        let mut fp_list: Vec<Fingerprint> = Vec::new();
        let (Some(update_queries), Some(table_metadata)) = (
            core.cache.update_queries.get(&update.relation_oid),
            core.cache.tables.get1(&update.relation_oid),
        ) else {
            return Ok(fp_list);
        };
        let toast_event = RowEvent {
            row_data: update.new_row_data,
            key_data: Some(update.key_data),
            operation: CdcOperation::Upsert,
            row_changes: None,
        };
        let mut pg_eval: Vec<&UpdateQuery> = Vec::new();
        for update_query in update_queries.queries.values() {
            if core.frame_invalidations.contains(&update_query.fingerprint) {
                continue;
            }
            match toast_fallback_decide(update_query, table_metadata, &toast_event, toasted_columns)
            {
                ToastFallbackDecision::Invalidate => fp_list.push(update_query.fingerprint),
                ToastFallbackDecision::PgEval => pg_eval.push(update_query),
                ToastFallbackDecision::Unaffected => {}
            }
        }
        if !pg_eval.is_empty() {
            let matched = self
                .pg_eval_matches(&pg_eval, table_metadata, update.new_row_data)
                .await
                .attach_loc("toast fallback membership eval")?;
            fp_list.extend(matched);
        }
        Ok(fp_list)
    }

    /// Handle DELETE operation.
    ///
    /// Deletes the row from cache tables and checks for subquery invalidations.
    /// For Exclusion subquery tables (NOT IN, NOT EXISTS), a DELETE shrinks the
    /// exclusion set, which grows the outer result set — requiring invalidation.
    // Trace level: at info/debug the fmt layer allocates per-span extensions,
    // which would put a heap allocation on every CDC event.
    #[instrument(skip_all, level = "trace")]
    pub(super) async fn handle_delete(
        &mut self,
        core: &mut WriterCore,
        row: RelationRow<'_>,
        batch: Option<BatchEvalView<'_>>,
    ) -> CacheResult<()> {
        let start = Instant::now();
        crate::metrics::handles().cdc.handle_deletes.increment(1);
        let RelationRow {
            relation_oid,
            row_data,
        } = row;

        if !core.cache.tables.contains_key1(&relation_oid) {
            error!("No table metadata found for relation_oid: {}", relation_oid);
            crate::metrics::handles()
                .cdc
                .handle_delete_seconds
                .record(start.elapsed().as_secs_f64());
            return Ok(());
        }

        // Buffer the delete for the frame flush (PGC-228).
        self.frame_cache_delete(core, relation_oid, row_data)
            .await?;

        // Rung 3b: evict memos the deleted row belonged to (ADR-045). The
        // recovered old image (PGC-255) probes with real values; the fallback
        // is the delete tuple itself — exact under REPLICA IDENTITY FULL,
        // PK-only (`Unknown` wildcard over-return) under DEFAULT.
        let mut del_candidates = core.candidate_set_take();
        let old_image = batch
            .as_ref()
            .and_then(|view| view.old_image)
            .unwrap_or(row_data);
        eval_candidates_into(core, relation_oid, old_image, &mut del_candidates);
        memo_frame_accumulate(core, relation_oid, del_candidates.iter().copied());

        // A deleted row leaves stale rows in any Fresh MV that materialized it;
        // CDC removals never went through the upsert path's dirty-mark, so the
        // MV would serve the deleted row forever. The delete tuple's candidate
        // set (the genuine old image; exact under REPLICA IDENTITY FULL, PK-only
        // via `Unknown` wildcard under DEFAULT) is identical to `del_candidates`
        // above — reuse it instead of re-probing (PGC-292/ADR-045).
        core.mv_dirty_mark_candidates(&del_candidates);
        core.candidate_set_return(del_candidates);

        // Check for subquery invalidations — removing a row can expand the
        // final result set for Exclusion/Scalar subquery tables
        if core.cache.update_queries.contains_key(&relation_oid) {
            // DELETE narrowing needs no candidate probe — its set is
            // `has_limit_from ∪ always_check` (ADR-045): a delete invalidates
            // all limit queries unconditionally, non-limit FromClause deletes
            // never invalidate, and subquery/outer-join queries are always_check.
            let delete_event = RowEvent {
                row_data,
                key_data: None,
                operation: CdcOperation::Delete,
                row_changes: None,
            };
            let fp_list = update_queries_check_invalidate(
                core,
                relation_oid,
                &delete_event,
                &FingerprintSet::default(),
            );

            // Deferred to frame_invalidations_flush (see handle_insert).
            core.frame_invalidations.extend(fp_list);
        }

        crate::metrics::handles()
            .cdc
            .handle_delete_seconds
            .record(start.elapsed().as_secs_f64());
        Ok(())
    }

    /// Handle TRUNCATE operation.
    ///
    /// The physical `TRUNCATE` of the source tables' cache tables runs in-frame
    /// on `cdc_write_conn` (atomic with the rest of the source transaction).
    /// Additionally, every cached query referencing a truncated relation is
    /// invalidated: a table-wide empty can change derived/multi-table results
    /// in ways the in-place model can't track, so those queries repopulate
    /// from origin.
    #[instrument(skip_all)]
    pub(crate) async fn handle_truncate(
        &mut self,
        core: &mut WriterCore,
        relation_oids: &[Oid],
    ) -> CacheResult<()> {
        if let Some(sql) = Self::truncate_sql_build(core, relation_oids.iter().copied()) {
            core.frame_begin_ensure(relation_oids.iter().copied());
            core.frame_buf.push_str(&sql);
            self.frame_write_finish(core).await?;
        }

        for oid in relation_oids {
            core.cache_table_invalidate(*oid)
                .await
                .attach_loc("invalidating queries on truncate")?;
            // A population reading this relation with a pre-truncate snapshot
            // would resurrect truncated rows on merge. Raise its abort watermark
            // to the truncate's commit LSN at CommitMark (PGC-250).
            core.frame_truncated_relations.push(*oid);
        }

        Ok(())
    }
}
