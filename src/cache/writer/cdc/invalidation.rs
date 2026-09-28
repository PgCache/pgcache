use std::time::Instant;

use ecow::EcoString;
use tracing::debug;

use super::row_match::{
    join_membership_unchanged, row_constraints_match, window_move_is_promotion,
};
use super::{CdcOperation, WriterCdc};
use crate::cache::CacheResult;
use crate::cache::messages::QueryCommand;
use crate::cache::types::CachedQueryState;
use crate::cache::update_query::{
    RowChanges, SubqueryKind, UpdateQueries, UpdateQuery, UpdateQuerySource,
};
use crate::cache::writer::core::WriterCore;
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::constraint_index::row_value_forms;
use crate::query::{Fingerprint, FingerprintSet};
use crate::settings::CachePolicy;

/// One CDC row event as the invalidation checks see it.
pub(super) struct RowEvent<'a> {
    pub(super) row_data: &'a [Option<ByteString>],
    /// UPDATE key tuple (empty when the PK didn't change); `None` for
    /// INSERT/DELETE.
    pub(super) key_data: Option<&'a [Option<ByteString>]>,
    pub(super) operation: CdcOperation,
    /// `Some` → row is cached (UPDATE main path); `None` → row not cached
    /// (INSERT, DELETE, or UPDATE of an uncached row).
    pub(super) row_changes: Option<&'a RowChanges>,
}

/// Candidate fingerprints whose extracted constraints a CDC row could satisfy,
/// probed over the relation's full `eval_index` into a caller-provided scratch
/// set (cleared first) so the CDC hot path reuses the allocation across rows
/// (PGC-341/344). The in-place matcher (`update_queries_execute_batch`, filtered
/// to LocalEval) and the memo-eviction pass (`memo_frame_accumulate`) share one
/// probe. Leaves `out` empty when the relation has no cached queries.
pub(super) fn eval_candidates_into(
    core: &WriterCore,
    relation_oid: Oid,
    row: &[Option<ByteString>],
    out: &mut FingerprintSet,
) {
    match (
        core.cache.update_queries.get(&relation_oid),
        core.cache.tables.get1(&relation_oid),
    ) {
        (Some(uqs), Some(table_metadata)) => uqs
            .eval_index
            .candidates_point_into(|c| row_value_forms(table_metadata, row, c), out),
        _ => out.clear(),
    }
}

/// Accumulate the memoized fingerprints this CDC row change affects into
/// `frame_memo_evictions` (rung 3b); the frame flush bumps `SlotKey::Memo(F)`
/// for the set, so eviction is predicate-matched rather than relation-coarse.
///
/// `memo_candidates` is the union of the new-row and old-image probes
/// (`candidates(new) ∪ candidates(old)`; for a DELETE just the old image, for an
/// INSERT just the new row — see the dispatch). A memo's result changes only if
/// the row matched the query now or before, which makes the query satisfy its
/// extracted constraints → it is in that union (the never-under-return guarantee
/// of ADR-037 holds in both directions). So membership alone is complete — no
/// per-memo predicate eval, and no PgEval special case (a non-candidate provably
/// can't be in the result). Over-eviction (a candidate whose result didn't
/// actually change) is harmless. Orphan memos (query no longer registered) are
/// invalidated eagerly at eviction (`cache_query_evict`), not here.
pub(super) fn memo_frame_accumulate(
    core: &mut WriterCore,
    relation_oid: Oid,
    memo_candidates: impl IntoIterator<Item = Fingerprint>,
) {
    if core.state_view.memo.is_empty() {
        return;
    }
    // Takes an iterator so callers can chain the new- and old-image candidate
    // sets without materializing their union (PGC-340). `frame_memo_evictions`
    // is a set, so a fingerprint present in both images inserts idempotently.
    core.state_view.memo.candidates_memoized_into(
        relation_oid,
        memo_candidates,
        &mut core.frame_memo_evictions,
    );
}

/// Whether the row is relevant to the query's view of this table: it matches
/// the table's WHERE constraints, or — with none — its join membership may
/// have changed. Membership is unchanged when the PK didn't change (empty
/// `key_data`) and every join column is a PK column.
fn row_relevant(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    event: &RowEvent,
) -> bool {
    if update_query
        .constraints
        .table_constraints
        .contains_key(table_metadata.name.as_str())
    {
        row_constraints_match(&update_query.constraints, table_metadata, event.row_data)
    } else {
        !join_membership_unchanged(update_query, table_metadata, event.key_data)
    }
}

/// Whether the change can expand the final result set. Changes that can only
/// contract it are safe to skip (extra cached rows are acceptable, missing
/// rows are not).
///
/// INSERT + Inclusion: grows IN set → expands result → invalidate.
/// INSERT + Exclusion: grows exclusion set → contracts result → skip.
/// DELETE + Inclusion: shrinks IN set → contracts result → skip.
/// DELETE + Exclusion: shrinks exclusion set → expands result → invalidate.
/// Scalar: any change can shift the value → always invalidate.
fn subquery_change_expands(kind: SubqueryKind, operation: CdcOperation) -> bool {
    match (kind, operation) {
        (SubqueryKind::Scalar, _) => true,
        (SubqueryKind::Inclusion, CdcOperation::Upsert) => true,
        (SubqueryKind::Inclusion, CdcOperation::Delete) => false,
        (SubqueryKind::Exclusion, CdcOperation::Upsert) => false,
        (SubqueryKind::Exclusion, CdcOperation::Delete) => true,
    }
}

/// Whether a query must be invalidated when the row is not currently cached.
fn row_uncached_invalidates(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    event: &RowEvent,
) -> bool {
    match update_query.source {
        UpdateQuerySource::FromClause => {
            from_clause_uncached_invalidates(update_query, table_metadata, event)
        }
        UpdateQuerySource::Subquery(kind) => {
            row_relevant(update_query, table_metadata, event)
                && subquery_change_expands(kind, event.operation)
        }
        // Terminal optional side of an outer join. Changes here only affect
        // NULL-padded columns — the preserved side already has the row. No
        // cross-table dependencies, so the update query execution handles it
        // (upsert into cache table).
        UpdateQuerySource::OuterJoinTerminal => false,
        // Non-terminal optional side of an outer join. Changes here can
        // cascade to affect other tables' result set membership (e.g. a new
        // match may activate a downstream join path that was previously
        // NULL-padded). Invalidate if the row is relevant to this query.
        UpdateQuerySource::OuterJoinOptional => {
            row_constraints_match(&update_query.constraints, table_metadata, event.row_data)
        }
    }
}

fn from_clause_uncached_invalidates(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    event: &RowEvent,
) -> bool {
    // DELETE: the row is already removed from the cache table. For INNER JOIN
    // (the only join type that gets FromClause source), removing a row can
    // only shrink the result set — except that a limited query's cached result
    // may now have fewer rows than the LIMIT window, so invalidate to
    // repopulate.
    if event.operation == CdcOperation::Delete {
        return update_query.has_limit;
    }
    // Single-table queries don't need invalidation for uncached rows.
    !update_query.is_single_table && row_relevant(update_query, table_metadata, event)
}

fn columns_changed<'a>(
    columns: impl IntoIterator<Item = &'a EcoString>,
    row_changes: &RowChanges,
) -> bool {
    columns
        .into_iter()
        .any(|c| row_changes.get(c.as_str()).is_some_and(|cc| cc.changed))
}

/// Whether a query must be invalidated when the row exists in cache.
fn row_cached_invalidates(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    row_changes: &RowChanges,
) -> bool {
    // Subquery and non-terminal outer join tables: always invalidate on
    // UPDATE — column changes could shift set membership or cascade to
    // affect downstream joins/predicates
    if matches!(
        update_query.source,
        UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional
    ) {
        return true;
    }
    if limit_window_move_invalidates(update_query, table_metadata, row_data, row_changes) {
        return true;
    }
    join_columns_changed(update_query, table_metadata, row_changes)
        && row_constraints_match(&update_query.constraints, table_metadata, row_data)
}

/// LIMIT windowing: an UPDATE that changes a column defining this query's
/// window boundary (ORDER BY / WHERE / HAVING) may push the cached row out of
/// the window — and the untracked row that should take its place is, by
/// definition, not in the cache. Invalidate to force repopulation. PGC-94.
fn limit_window_move_invalidates(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    row_changes: &RowChanges,
) -> bool {
    let windowed =
        update_query.has_limit && matches!(update_query.source, UpdateQuerySource::FromClause);
    if !windowed || !columns_changed(&update_query.limit_window_columns, row_changes) {
        return false;
    }
    // PGC-336: the row can only affect this query's window if it is (or was)
    // inside the query's predicate region. If a predicate column changed we
    // can't see the pre-image cheaply, so stay conservative; otherwise the
    // predicate truth is stable and a row that fails it can neither be in nor
    // enter the window.
    if columns_changed(&update_query.predicate_columns, row_changes) {
        return true;
    }
    // PGC-334: a promotion can only push already-cached rows down — the
    // in-place upsert plus serve-time re-sort keeps the window correct without
    // invalidation. Only demotions (and anything direction can't prove) can
    // open an uncached gap at the boundary.
    row_constraints_match(&update_query.constraints, table_metadata, row_data)
        && !window_move_is_promotion(update_query, table_metadata, row_data, row_changes)
}

fn join_columns_changed(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    row_changes: &RowChanges,
) -> bool {
    update_query
        .constraints
        .table_join_columns(&table_metadata.name)
        .any(|column| {
            // Missing column would mean query constraints reference a column
            // that wasn't projected — a builder invariant violation. Default
            // to changed (→ invalidate): the safe direction, since assuming
            // unchanged could skip a required invalidation and serve stale data.
            debug_assert!(
                row_changes.contains_key(column),
                "constraint column {column} missing from row_changes projection"
            );
            row_changes.get(column).is_none_or(|cc| cc.changed)
        })
}

/// Whether a query must be invalidated on the toast-fallback path without
/// (or regardless of) membership evaluation (PGC-264). The row may or may
/// not be cached and its column changes are unknowable, so this folds both
/// `row_cached_invalidates` (changes assumed) and `row_uncached_invalidates`
/// (Upsert) into their conservative union. Queries passing this still get
/// membership-evaluated by the caller — a match invalidates too, since the
/// incomplete image can't be upserted.
pub(super) fn toast_fallback_structural_invalidate(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    event: &RowEvent,
    toasted_columns: &[EcoString],
) -> bool {
    // Constraint/predicate evaluation reads an elided column → nothing
    // below (nor the caller's membership eval) can be trusted.
    if update_query
        .predicate_columns
        .iter()
        .any(|c| toasted_columns.contains(c))
    {
        return true;
    }
    match update_query.source {
        // Always invalidated on a cached UPDATE (row_cached_invalidates).
        UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional => true,
        // Window-boundary columns may have changed; the replacement row is by
        // definition uncached (PGC-94). Single-table: membership eval alone
        // decides (the eval is trustworthy past the predicate_columns gate
        // above). Multi-table: a join-column change can create join matches
        // the cache tables can't see, so membership eval saying "no match"
        // doesn't rule out growth.
        UpdateQuerySource::FromClause | UpdateQuerySource::OuterJoinTerminal => {
            update_query.has_limit
                || (!update_query.is_single_table
                    && row_relevant(update_query, table_metadata, event))
        }
    }
}

/// ADR-045: examine only the narrowed set, not every query on the relation.
/// `candidates` (the new-row probe) covers every "row now matches" branch; the
/// carve-outs cover the branches that fire regardless of whether the
/// post-image row matches — unconditional subquery / outer-join
/// (`always_check`); a DELETE on any `has_limit` query; and an UPDATE of a
/// limit predicate column that can push a row out of a window. Single-table
/// non-limit FromClause queries provably never invalidate, so excluding them
/// is the bulk of the saving. The sets are chained rather than unioned
/// (PGC-340); a fingerprint in more than one is re-checked, and the
/// `frame_invalidations` set dedupes it.
fn narrowed_candidates<'a>(
    update_queries: &'a UpdateQueries,
    event: &RowEvent,
    candidates: &'a FingerprintSet,
) -> impl Iterator<Item = Fingerprint> + 'a {
    let expand_limit = match (event.operation, event.row_changes) {
        (CdcOperation::Delete, _) => true,
        (CdcOperation::Upsert, Some(rc)) => update_queries.limit_predicate_changed(rc),
        (CdcOperation::Upsert, None) => false,
    };
    candidates
        .iter()
        .copied()
        .chain(update_queries.always_check.iter().copied())
        .chain(
            expand_limit
                .then(|| update_queries.has_limit_from.iter().copied())
                .into_iter()
                .flatten(),
        )
}

fn query_row_invalidates(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    event: &RowEvent,
) -> bool {
    let invalidate = match event.row_changes {
        Some(row_changes) => {
            row_cached_invalidates(update_query, table_metadata, event.row_data, row_changes)
        }
        None => row_uncached_invalidates(update_query, table_metadata, event),
    };
    // Drift guard (PGC-227): on the UPDATE path (`Upsert`), a query that
    // invalidates here MUST be `change_dependent`, or `handle_update` would
    // have skipped this check and served stale. `update_invalidation_possible`
    // is the single source of truth; this fails the moment a check branch
    // diverges from it. DELETE has its own invalidation branches that fire
    // independently of the flag, so scope the guard to `Upsert`.
    debug_assert!(
        !invalidate || event.operation != CdcOperation::Upsert || update_query.change_dependent,
        "invalidation fired for a non-change_dependent query on the UPDATE \
         path: update_invalidation_possible is out of sync with \
         row_*_invalidates"
    );
    invalidate
}

/// Fingerprints of the relation's cached queries this row event invalidates.
/// No cached query referencing the relation (never registered, or all evicted)
/// means nothing to invalidate — not an error.
#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) fn update_queries_check_invalidate(
    core: &WriterCore,
    relation_oid: Oid,
    event: &RowEvent,
    candidates: &FingerprintSet,
) -> Vec<Fingerprint> {
    let (Some(update_queries), Some(table_metadata)) = (
        core.cache.update_queries.get(&relation_oid),
        core.cache.tables.get1(&relation_oid),
    ) else {
        return Vec::new();
    };
    narrowed_candidates(update_queries, event, candidates)
        .filter(|fingerprint| {
            update_queries
                .queries
                .get(fingerprint)
                .is_some_and(|q| query_row_invalidates(q, table_metadata, event))
        })
        .collect()
}

/// Ready→Invalidated: stop serving the query's generation, keep the entry for
/// metadata reuse on readmission, and drain waiters parked on its now-dead
/// population.
fn query_invalidated_mark(core: &mut WriterCore, fingerprint: Fingerprint, generation: u64) {
    if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
        m.invalidation_count += 1;
        m.cached_since_ns = None;
    }
    core.cache.generations.remove(&generation);
    if let Some(mut query) = core.cache.cached_queries.get1_mut(&fingerprint) {
        query.invalidated = true;
    }
    // Fold the MV dirty transition into the same get_mut block so dispatches
    // observe both transitions atomically — a reader that sees
    // state=Invalidated never sees the MV in a stale-Fresh state. `dirty_apply`
    // is used directly (not `mv_dirty_mark`) to keep both writes under one guard.
    if let Some(mut entry) = core.state_view.cached_queries.get_mut(&fingerprint) {
        entry.state = CachedQueryState::Invalidated;
        entry.referenced = false;
        entry.mv.dirty_apply(Instant::now());
    }
    core.waiters_fail(fingerprint);
}

impl WriterCdc {
    /// CDC-triggered invalidation of a cached query.
    /// For FIFO: delegates to full eviction.
    /// For CLOCK: marks the entry as Invalidated, keeping metadata for fast readmission.
    /// Removes from generations BTreeSet and purges stale rows, but preserves
    /// cached_queries entry and update_queries for reuse on readmission.
    ///
    /// Returns `true` iff this call performed a real invalidation event — a
    /// Ready→Invalidated transition, a FIFO eviction, or a pinned query's
    /// deferred readmit. Returns `false` for no-ops — the query is already gone
    /// or already invalidated — so the aggregate `cache_invalidations` metric
    /// counts real events and is not inflated by re-flagging the standing
    /// invalidated set every frame.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn cache_query_cdc_invalidate(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
    ) -> CacheResult<bool> {
        // Pinned queries: defer readmission to the writer event loop. Still
        // drain parked waiters — the readmit's Ready can itself be superseded
        // under churn (waiting on it risks the same hang as the unpinned path).
        // A pinned invalidation is a real event (it queues a readmit), so it
        // counts — unlike the already-invalidated re-flag below.
        if core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|q| q.pinned)
        {
            debug!("pinned query invalidated, deferring readmit {fingerprint}");
            let _ = core.query_tx.send(QueryCommand::Readmit { fingerprint });
            core.waiters_fail(fingerprint);
            return Ok(true);
        }

        if core.cache.dynamic.load().cache_policy == CachePolicy::Fifo {
            return core.cache_query_evict(fingerprint).await.map(|()| true);
        }

        let Some(generation) = core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .filter(|query| !query.invalidated)
            .map(|query| query.generation)
        else {
            return Ok(false);
        };

        debug!("cdc invalidating query {fingerprint}");
        let prev_generation_threshold = core.cache.generation_purge_threshold();
        query_invalidated_mark(core, fingerprint, generation);

        // Purge stale rows if the generation threshold moved and the cache
        // volume is under disk pressure (statvfs, PGC-276).
        let new_threshold = core.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold && core.disk_pressure() {
            core.generation_purge(new_threshold).await?;
        }

        Ok(true)
    }
}
