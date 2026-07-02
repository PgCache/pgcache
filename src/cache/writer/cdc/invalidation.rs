use crate::catalog::Oid;
use crate::query::{Fingerprint, FingerprintSet};
use std::collections::HashSet;
use std::fmt::Write;

use ecow::EcoString;
use postgres_protocol::escape;
use tokio_postgres::SimpleQueryMessage;
use tracing::{debug, trace};

use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;

use crate::query::ast::BinaryOp;
use crate::query::cast::cast_target_coerce_text;
use crate::query::constraint_index::row_value_forms;
use crate::query::constraints::{QueryConstraints, TableConstraint};
use crate::query::evaluate::{literal_compare, pg_bool_parse, where_value_compare_string};

use crate::settings::CachePolicy;

use super::super::super::messages::QueryCommand;
use super::super::super::types::CachedQueryState;
use super::super::super::update_query::{
    RowChanges, SubqueryKind, UpdateEvalStrategy, UpdateQuery, UpdateQuerySource,
};
use super::super::super::{CacheError, CacheResult, MapIntoReport};
use super::super::core::WriterCore;

use super::*;

/// Check that every WHERE constraint for `table_metadata` matches `row_data`.
/// Returns true when there are no constraints for this table (full-scan
/// cached query), or when every constraint evaluates to true on the row.
///
/// CastComparison constraints coerce the row's wire-text via
/// `cast_target_coerce_text` and compare via `literal_compare`; a coercion
/// failure is treated as non-match (the row would have errored at origin).
pub(super) fn row_constraints_match(
    constraints: &QueryConstraints,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    let Some(constraints) = constraints
        .table_constraints
        .get(table_metadata.name.as_str())
    else {
        return true;
    };

    for constraint in constraints {
        let column_name = match constraint {
            TableConstraint::Comparison(col, ..)
            | TableConstraint::AnyOf(col, ..)
            | TableConstraint::CastComparison(col, ..) => col.as_str(),
        };

        if let Some(column_meta) = table_metadata.columns.get(column_name) {
            let position = column_meta.index();
            if let Some(row_value) = row_data.get(position) {
                let matches = match row_value {
                    Some(row_str) => match constraint {
                        TableConstraint::Comparison(_, op, val) => {
                            where_value_compare_string(val, row_str, *op)
                        }
                        TableConstraint::AnyOf(_, values) => values
                            .iter()
                            .any(|v| where_value_compare_string(v, row_str, BinaryOp::Equal)),
                        TableConstraint::CastComparison(_, cast, op, val) => {
                            match cast_target_coerce_text(cast, row_str) {
                                Some(coerced) => literal_compare(&coerced, *op, val),
                                // Coercion failure (e.g. `'abc'::int4`): the
                                // row would error at origin and never match
                                // — safe to treat as non-matching here.
                                None => false,
                            }
                        }
                    },
                    // NULL never matches comparison operators
                    None => false,
                };
                if !matches {
                    return false;
                }
            }
        }
    }

    true
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

/// Alias prefixes for the ordering projections the row-change SQL emits for
/// limit-window ORDER BY columns (PGC-334), folded back into the base column's
/// `ColumnChange` by `row_change_column_fold`. If a >54-byte column name makes
/// PG truncate the alias, the fold misses and the ordering fields stay
/// unknown — degrading to the conservative always-invalidate path.
pub(super) const OLD_LESS_THAN_ALIAS_PREFIX: &str = "__pgc_lt_";
pub(super) const OLD_IS_NULL_ALIAS_PREFIX: &str = "__pgc_nl_";

/// Whether any of the relation's columns uses the reserved `__pgc_` prefix.
/// Such a column's own result alias would collide with the projection-alias
/// namespace and mis-route through the fold, so the ordering/old-image
/// projections are suppressed for the relation and the fold routes every
/// result column as plain (conservative: direction punts to invalidate, old
/// images stay wildcard).
pub(super) fn table_has_reserved_columns(table_metadata: &TableMetadata) -> bool {
    table_metadata
        .columns
        .iter()
        .any(|c| c.name.starts_with("__pgc_"))
}

/// Fold one row-change result column into the per-column map: plain names
/// carry the `IS DISTINCT FROM` changed flag, prefixed aliases carry the
/// ordering projections. `None` (SQL NULL) leaves ordering unknown and, for
/// the changed flag, defensively counts as unchanged — matching the historic
/// text parse. `strip_prefixes` is false when the builder suppressed every
/// prefixed projection (`table_has_reserved_columns`): every result column is
/// then a real column, including `__pgc_`-named ones.
pub(super) fn row_change_column_fold(
    changes: &mut RowChanges,
    name: &str,
    value: Option<bool>,
    strip_prefixes: bool,
) {
    if strip_prefixes && let Some(col) = name.strip_prefix(OLD_LESS_THAN_ALIAS_PREFIX) {
        changes
            .entry(EcoString::from(col))
            .or_default()
            .old_less_than_new = value;
    } else if strip_prefixes && let Some(col) = name.strip_prefix(OLD_IS_NULL_ALIAS_PREFIX) {
        changes.entry(EcoString::from(col)).or_default().old_is_null = value == Some(true);
    } else {
        changes.entry(EcoString::from(name)).or_default().changed = value == Some(true);
    }
}

/// Whether this UPDATE changed the row's primary key. Under REPLICA IDENTITY
/// DEFAULT the presence of a key tuple IS the signal ('K' is sent only on PK
/// change); under FULL every update carries the complete old row ('O'), so
/// the PK columns are compared. A missing column defaults to changed — the
/// conservative direction for every consumer (old-PK delete, ladder skip).
pub(super) fn update_pk_changed(
    table_metadata: &TableMetadata,
    key_data: &[Option<ByteString>],
    new_row_data: &[Option<ByteString>],
) -> bool {
    if key_data.is_empty() {
        return false;
    }
    if !table_metadata.replica_identity_full {
        return true;
    }
    table_metadata.primary_key_columns.iter().any(|pk| {
        let Some(meta) = table_metadata.columns.get(pk.as_str()) else {
            return true;
        };
        key_data.get(meta.index()) != new_row_data.get(meta.index())
    })
}

/// Whether this UPDATE moves the cached row strictly toward the front of the
/// query's ORDER BY output — a promotion, which can only push already-cached
/// rows down and therefore never opens an uncached gap at the LIMIT-window
/// boundary (PGC-334; holds under OFFSET too, since only boundary crossings
/// change the cached prefix's membership). Compares the old and new sort
/// tuples lexicographically: the first ORDER BY key whose value changed
/// decides. Any ambiguity — no key spec, a changed window column that isn't a
/// key, an incomparable pair, a missing value — returns `false`, which callers
/// must treat as "invalidate".
fn window_move_is_promotion(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    row_changes: &RowChanges,
) -> bool {
    let Some(keys) = &update_query.order_by_keys else {
        return false;
    };
    // Every changed window column must be an ORDER BY key: predicate columns
    // are already known unchanged here, but e.g. a HAVING reference isn't
    // direction-analyzable.
    for column in &update_query.limit_window_columns {
        if row_changes.get(column).is_some_and(|cc| cc.changed)
            && !keys.iter().any(|k| k.column == *column)
        {
            return false;
        }
    }
    for key in keys.iter() {
        let Some(change) = row_changes.get(&key.column) else {
            return false;
        };
        if !change.changed {
            continue;
        }
        let Some(new_value) = table_metadata
            .columns
            .get(key.column.as_str())
            .and_then(|meta| row_data.get(meta.index()))
        else {
            return false;
        };
        return match (change.old_is_null, new_value.is_none()) {
            // Contradicts `changed`; punt.
            (true, true) => false,
            // Left the NULL region: forward iff NULLs sort last.
            (true, false) => !key.nulls_first,
            // Entered the NULL region: forward iff NULLs sort first.
            (false, true) => key.nulls_first,
            (false, false) => match change.old_less_than_new {
                // Value rose: forward iff larger sorts earlier.
                Some(true) => key.descending,
                Some(false) => !key.descending,
                None => false,
            },
        };
    }
    // `window_changed` held but no key registered a change — punt.
    false
}

/// Evaluate a LocalEval update query's WHERE against the CDC row.
///
/// Must only be called when `update_query.eval_strategy == LocalEval` — the
/// classifier has already ensured the query is single-table, FromClause, with
/// no GROUP BY / HAVING and a supported WHERE shape. A WHERE of `None` means
/// the query loads every row, so the match is unconditional.
pub(super) fn update_query_matches_locally(
    update_query: &UpdateQuery,
    _table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
) -> bool {
    // `compiled_where` is built from this query's WHERE at registration (PGC-339):
    // `None` means no WHERE clause → unconditional match (the LocalEval contract,
    // since this is only called for `eval_strategy == LocalEval`).
    match &update_query.compiled_where {
        None => {
            // `None` must mean "LocalEval SELECT with no WHERE", never a non-SELECT
            // shape: the old code returned `false` for a non-SELECT, and the
            // classifier only assigns LocalEval to SELECTs. Guard that invariant so
            // a future classifier change can't silently make every row match here.
            debug_assert!(
                update_query.resolved.as_select().is_some(),
                "LocalEval query must be a SELECT; classifier/compiled_where invariant broken"
            );
            true
        }
        Some(predicate) => predicate.eval(row_data),
    }
}

/// Check if a row's membership in a joined result set is unchanged.
/// Returns true when the primary key didn't change and all join columns
/// are primary key columns — meaning the row's join relationships are stable.
fn join_membership_unchanged(
    update_query: &UpdateQuery,
    table_metadata: &TableMetadata,
    key_data: Option<&[Option<ByteString>]>,
) -> bool {
    let Some(key) = key_data else {
        return false;
    };

    if !key.is_empty() {
        return false;
    }

    // Non-empty AND every join column is a PK column — decided on the iterator
    // so this per-candidate check allocates nothing (PGC-342). `peek` proves
    // non-empty; `all` then checks every column (including the peeked one).
    let mut join_columns = update_query
        .constraints
        .table_join_columns(&table_metadata.name)
        .peekable();

    join_columns.peek().is_some()
        && join_columns.all(|col| {
            table_metadata
                .primary_key_columns
                .iter()
                .any(|pk| pk == col)
        })
}

impl WriterCdc {
    /// Whether a query must be invalidated on the toast-fallback path without
    /// (or regardless of) membership evaluation (PGC-264). The row may or may
    /// not be cached and its column changes are unknowable, so this folds both
    /// `row_cached_invalidation_check` (changes assumed) and
    /// `row_uncached_invalidation_check` (Upsert) into their conservative
    /// union. Queries passing this still get membership-evaluated by the
    /// caller — a match invalidates too, since the incomplete image can't be
    /// upserted.
    pub(super) fn toast_fallback_structural_invalidate(
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        new_row_data: &[Option<ByteString>],
        key_data: &[Option<ByteString>],
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
            // Always invalidated on a cached UPDATE (row_cached_invalidation_check).
            UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional => true,
            UpdateQuerySource::FromClause | UpdateQuerySource::OuterJoinTerminal => {
                // Window-boundary columns may have changed; the replacement
                // row is by definition uncached (PGC-94).
                if update_query.has_limit {
                    return true;
                }
                // Single-table: membership eval alone decides (the eval is
                // trustworthy past the predicate_columns gate above).
                if update_query.is_single_table {
                    return false;
                }
                // Multi-table: a join-column change can create join matches
                // the cache tables can't see, so membership eval saying
                // "no match" doesn't rule out growth. Mirror
                // row_uncached_invalidation_check's relevance tests.
                if update_query
                    .constraints
                    .table_constraints
                    .contains_key(table_metadata.name.as_str())
                {
                    row_constraints_match(&update_query.constraints, table_metadata, new_row_data)
                } else {
                    !join_membership_unchanged(update_query, table_metadata, Some(key_data))
                }
            }
        }
    }

    /// CDC-triggered invalidation of a cached query.
    /// For FIFO: delegates to full eviction.
    /// For CLOCK: marks the entry as Invalidated, keeping metadata for fast readmission.
    /// Removes from generations BTreeSet and purges stale rows, but preserves
    /// cached_queries entry and update_queries for reuse on readmission.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    /// Returns `true` iff this call performed a real invalidation event — a
    /// Ready→Invalidated transition, a FIFO eviction, or a pinned query's
    /// deferred readmit. Returns `false` for no-ops — the query is already gone
    /// or already invalidated — so the aggregate `cache_invalidations` metric
    /// counts real events and is not inflated by re-flagging the standing
    /// invalidated set every frame.
    pub(super) async fn cache_query_cdc_invalidate(
        &self,
        core: &mut WriterCore,
        fingerprint: Fingerprint,
    ) -> CacheResult<bool> {
        // Pinned queries: defer readmission to the writer event loop. Still
        // drain parked waiters — the readmit's Ready can itself be superseded
        // under churn (waiting on it risks the same hang as the unpinned path).
        if core
            .cache
            .cached_queries
            .get1(&fingerprint)
            .is_some_and(|q| q.pinned)
        {
            debug!("pinned query invalidated, deferring readmit {fingerprint}");
            let _ = core.query_tx.send(QueryCommand::Readmit { fingerprint });
            core.waiters_fail(fingerprint);
            // A pinned invalidation is a real event (it queues a readmit), so it
            // counts — unlike the already-invalidated re-flag below.
            return Ok(true);
        }

        let cfg = core.cache.dynamic.load();

        if cfg.cache_policy == CachePolicy::Fifo {
            return core.cache_query_evict(fingerprint).await.map(|()| true);
        }

        let Some(query) = core.cache.cached_queries.get1(&fingerprint) else {
            return Ok(false);
        };

        // Already invalidated — nothing to do
        if query.invalidated {
            return Ok(false);
        }

        let generation = query.generation;
        debug!("cdc invalidating query {fingerprint}");
        if let Some(mut m) = core.state_view.metrics.get_mut(&fingerprint) {
            m.invalidation_count += 1;
            m.cached_since_ns = None;
        }

        let prev_generation_threshold = core.cache.generation_purge_threshold();

        // Remove from active generations (no longer serving cached results)
        core.cache.generations.remove(&generation);

        // Mark as invalidated (keep entry for metadata reuse on readmission)
        if let Some(mut query) = core.cache.cached_queries.get1_mut(&fingerprint) {
            query.invalidated = true;
        }

        // Update state view to Invalidated. Fold the MV dirty transition into
        // the same get_mut block so dispatches observe both transitions
        // atomically — a reader that sees state=Invalidated never sees the MV
        // in a stale-Fresh state. `state_set` is used directly here (not
        // `mv_state_write`) to keep both writes under one guard.
        if let Some(mut entry) = core.state_view.cached_queries.get_mut(&fingerprint) {
            entry.state = CachedQueryState::Invalidated;
            entry.referenced = false;
            if let Some(dirtied) = entry.mv.state().dirtied() {
                entry.mv.state_set(dirtied);
            }
        }

        // Drain coalesced waiters parked on this query's now-dead population.
        core.waiters_fail(fingerprint);

        // Purge stale rows if the generation threshold moved and the cache
        // volume is under disk pressure (statvfs, PGC-276).
        let new_threshold = core.cache.generation_purge_threshold();
        if new_threshold > prev_generation_threshold && core.disk_pressure() {
            core.generation_purge(new_threshold).await?;
        }

        Ok(true)
    }

    /// SELECT one row from the cache, projecting a boolean per non-PK column
    /// that's true iff the cached value differs from the incoming `row_data`
    /// value. Used by CDC UPDATE handling to decide whether a column change
    /// actually shifts query membership. For the relation's limit-window ORDER
    /// BY key columns it additionally projects old-vs-new ordering
    /// (`col < new`, `col IS NULL`) so the window check can tell promotions
    /// from demotions (PGC-334). Returns `None` when the row isn't in the
    /// cache (no PK match).
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn query_row_changes(
        &self,
        core: &WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
    ) -> CacheResult<Option<RowChanges>> {
        let table_metadata =
            core.cache
                .tables
                .get1(&relation_oid)
                .ok_or(CacheError::UnknownTable {
                    oid: Some(relation_oid),
                    name: None,
                })?;

        // Build SELECT ... FROM ... WHERE ... in a single String.
        // Comparison columns go in the SELECT list, PK conditions in the WHERE clause.
        let mut sql = String::with_capacity(SQL_BUFFER_CAPACITY);
        sql.push_str("SELECT ");

        let reserved_columns = table_has_reserved_columns(table_metadata);
        let order_columns: HashSet<&EcoString> = if reserved_columns {
            HashSet::new()
        } else {
            core.cache
                .update_queries
                .get(&relation_oid)
                .map(|uq| uq.limit_order_columns().collect())
                .unwrap_or_default()
        };

        let mut first_col = true;
        for column_meta in &table_metadata.columns {
            let position = column_meta.index();
            if let Some(row_value) = row_data.get(position) {
                let value = row_value
                    .as_deref()
                    .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                if !first_col {
                    sql.push_str(", ");
                }
                // Quoted throughout: the cache-table DDL quotes column names,
                // so reserved-word / mixed-case columns exist here.
                let name = escape::escape_identifier(&column_meta.name);
                let _ = write!(sql, "{name} IS DISTINCT FROM {value} AS {name}");
                if order_columns.contains(&column_meta.name) {
                    let less_than_alias = escape::escape_identifier(&format!(
                        "{OLD_LESS_THAN_ALIAS_PREFIX}{}",
                        column_meta.name
                    ));
                    let is_null_alias = escape::escape_identifier(&format!(
                        "{OLD_IS_NULL_ALIAS_PREFIX}{}",
                        column_meta.name
                    ));
                    let _ = write!(
                        sql,
                        ", {name} < {value} AS {less_than_alias}, {name} IS NULL AS {is_null_alias}",
                    );
                }
                first_col = false;
            }
        }

        let _ = write!(
            sql,
            " FROM {}.{} WHERE ",
            escape::escape_identifier(&table_metadata.schema),
            escape::escape_identifier(&table_metadata.name)
        );

        let mut has_pk = false;
        for pk_column in &table_metadata.primary_key_columns {
            if let Some(column_meta) = table_metadata.columns.get(pk_column.as_str()) {
                let position = column_meta.index();
                if let Some(row_value) = row_data.get(position) {
                    let value = row_value
                        .as_deref()
                        .map_or_else(|| "NULL".to_owned(), escape::escape_literal);
                    if has_pk {
                        sql.push_str(" AND ");
                    }
                    let _ = write!(sql, "{} = {value}", escape::escape_identifier(pk_column));
                    has_pk = true;
                }
            }
        }

        if !has_pk {
            return Err(CacheError::NoPrimaryKey.into());
        }

        let msgs = core
            .db_cache
            .simple_query(&sql)
            .await
            .map_into_report::<CacheError>()?;

        for msg in msgs {
            if let SimpleQueryMessage::Row(row) = msg {
                let mut changes = RowChanges::with_capacity(row.len());
                for (idx, col) in row.columns().iter().enumerate() {
                    row_change_column_fold(
                        &mut changes,
                        col.name(),
                        row.get(idx).and_then(pg_bool_parse),
                        !reserved_columns,
                    );
                }
                return Ok(Some(changes));
            }
        }
        Ok(None)
    }

    /// Determine if a query should be invalidated when the row is not currently cached.
    /// Returns true if the query should be invalidated.
    fn row_uncached_invalidation_check(
        &self,
        update_query: &UpdateQuery,
        table_metadata: &TableMetadata,
        row_data: &[Option<ByteString>],
        key_data: Option<&[Option<ByteString>]>,
        operation: CdcOperation,
    ) -> bool {
        match update_query.source {
            UpdateQuerySource::FromClause => {
                // DELETE on a limited query's table: cached result may have fewer
                // rows than the LIMIT window. Invalidate to trigger re-population.
                if update_query.has_limit && operation == CdcOperation::Delete {
                    return true;
                }

                // DELETE on FromClause source: the row is already removed from the cache
                // table. For INNER JOIN (the only join type that gets FromClause source),
                // removing a row can only shrink the result set, never expand it.
                // Serve-time re-evaluation handles correctness.
                if operation == CdcOperation::Delete {
                    return false;
                }

                // Single-table queries don't need invalidation for uncached rows
                if update_query.is_single_table {
                    return false;
                }

                let has_table_constraints = update_query
                    .constraints
                    .table_constraints
                    .contains_key(table_metadata.name.as_str());

                // If key_data is empty, PK didn't change. If all join columns are PK columns
                // and there are no WHERE constraints for this table, the row's membership
                // in the result set is unchanged - skip invalidation.
                if !has_table_constraints {
                    !join_membership_unchanged(update_query, table_metadata, key_data)
                } else {
                    // Check if row matches table constraints - invalidate only if it matches
                    row_constraints_match(&update_query.constraints, table_metadata, row_data)
                }
            }
            UpdateQuerySource::Subquery(kind) => {
                let has_table_constraints = update_query
                    .constraints
                    .table_constraints
                    .contains_key(table_metadata.name.as_str());

                // If key_data is empty, PK didn't change. If all join columns are PK columns
                // and there are no WHERE constraints for this table, the row's membership
                // in the result set is unchanged - skip invalidation.
                let row_added = if !has_table_constraints {
                    !join_membership_unchanged(update_query, table_metadata, key_data)
                } else {
                    row_constraints_match(&update_query.constraints, table_metadata, row_data)
                };

                // Check constraints — if row doesn't match constraints for this
                // table, it's not relevant to the cached query
                if !row_added {
                    return false;
                }

                // Only invalidate when the change can expand the final result set.
                // Changes that can only contract it are safe to skip (extra cached
                // rows are acceptable, missing rows are not).
                //
                // INSERT + Inclusion: grows IN set → expands result → invalidate.
                // INSERT + Exclusion: grows exclusion set → contracts result → skip.
                // DELETE + Inclusion: shrinks IN set → contracts result → skip.
                // DELETE + Exclusion: shrinks exclusion set → expands result → invalidate.
                // Scalar: any change can shift the value → always invalidate.
                match (kind, operation) {
                    (SubqueryKind::Scalar, _) => true,
                    (SubqueryKind::Inclusion, CdcOperation::Upsert) => true,
                    (SubqueryKind::Inclusion, CdcOperation::Delete) => false,
                    (SubqueryKind::Exclusion, CdcOperation::Upsert) => false,
                    (SubqueryKind::Exclusion, CdcOperation::Delete) => true,
                }
            }
            UpdateQuerySource::OuterJoinTerminal => {
                // Terminal optional side of an outer join. Changes here only
                // affect NULL-padded columns — the preserved side already has
                // the row. No cross-table dependencies, so the update query
                // execution handles it (upsert into cache table).
                false
            }
            UpdateQuerySource::OuterJoinOptional => {
                // Non-terminal optional side of an outer join. Changes here can
                // cascade to affect other tables' result set membership (e.g. a
                // new match may activate a downstream join path that was previously
                // NULL-padded). Invalidate if the row is relevant to this query.
                row_constraints_match(&update_query.constraints, table_metadata, row_data)
            }
        }
    }

    /// Determine if a query should be invalidated when the row exists in cache.
    /// Returns true if the query should be invalidated.
    fn row_cached_invalidation_check(
        &self,
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

        // LIMIT windowing: an UPDATE that changes a column defining this
        // query's window boundary (ORDER BY / WHERE / HAVING) may push the
        // cached row out of the window — and the untracked row that should
        // take its place is, by definition, not in the cache. Invalidate
        // to force repopulation. PGC-94.
        if update_query.has_limit && matches!(update_query.source, UpdateQuerySource::FromClause) {
            let window_changed = update_query
                .limit_window_columns
                .iter()
                .any(|c| row_changes.get(c.as_str()).is_some_and(|cc| cc.changed));
            if window_changed {
                // PGC-336: the row can only affect this query's window if it is
                // (or was) inside the query's predicate region. If a predicate
                // column changed we can't see the pre-image cheaply, so stay
                // conservative; otherwise the predicate truth is stable and a
                // row that fails it can neither be in nor enter the window.
                let predicate_changed = update_query
                    .predicate_columns
                    .iter()
                    .any(|c| row_changes.get(c.as_str()).is_some_and(|cc| cc.changed));
                if predicate_changed {
                    return true;
                }
                if row_constraints_match(&update_query.constraints, table_metadata, row_data) {
                    // PGC-334: a promotion can only push already-cached rows
                    // down — the in-place upsert plus serve-time re-sort keeps
                    // the window correct without invalidation. Only demotions
                    // (and anything direction can't prove) can open an
                    // uncached gap at the boundary.
                    if !window_move_is_promotion(
                        update_query,
                        table_metadata,
                        row_data,
                        row_changes,
                    ) {
                        return true;
                    }
                }
            }
        }

        for column in update_query
            .constraints
            .table_join_columns(&table_metadata.name)
        {
            // Missing column would mean query constraints reference a column
            // that wasn't projected — a builder invariant violation. Default
            // to changed (→ invalidate): the safe direction, since assuming
            // unchanged could skip a required invalidation and serve stale
            // data.
            debug_assert!(
                row_changes.contains_key(column),
                "constraint column {column} missing from row_changes projection"
            );
            let column_changed = row_changes.get(column).is_none_or(|cc| cc.changed);

            if !column_changed {
                continue;
            }

            // Check constraints - skip if row doesn't match
            if !row_constraints_match(&update_query.constraints, table_metadata, row_data) {
                continue;
            }

            return true;
        }

        false
    }

    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    #[allow(clippy::too_many_arguments)] // cohesive per-row CDC inputs; candidates are shared from dispatch, not recomputed
    pub(super) fn update_queries_check_invalidate(
        &self,
        core: &WriterCore,
        relation_oid: Oid,
        row_changes: Option<&RowChanges>,
        row_data: &[Option<ByteString>],
        key_data: Option<&[Option<ByteString>]>,
        operation: CdcOperation,
        candidates: &FingerprintSet,
    ) -> CacheResult<Vec<Fingerprint>> {
        // No cached query references this relation (never registered, or all
        // its queries were evicted) → nothing to invalidate. Not an error.
        let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
            return Ok(Vec::new());
        };
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return Ok(Vec::new());
        };

        // ADR-045: examine only the narrowed set, not every query on the
        // relation. `candidates` (the new-row probe) covers every "row now
        // matches" branch; the carve-outs cover the branches that fire
        // regardless of whether the post-image row matches — unconditional
        // subquery / outer-join (`always_check`); a DELETE on any `has_limit`
        // query; and an UPDATE of a limit predicate column that can push a row
        // out of a window. Single-table non-limit FromClause queries provably
        // never invalidate, so excluding them is the bulk of the saving.
        // Chain the carve-out sets onto the candidate probe instead of
        // materializing their union (PGC-340). `fp_list` is deduped by the
        // `frame_invalidations` set, so a fingerprint present in more than one
        // set is harmless — re-checked, never double-invalidated.
        let expand_limit = match (operation, row_changes) {
            (CdcOperation::Delete, _) => true,
            (CdcOperation::Upsert, Some(rc)) => update_queries.limit_predicate_changed(rc),
            (CdcOperation::Upsert, None) => false,
        };
        let narrowed = candidates
            .iter()
            .copied()
            .chain(update_queries.always_check.iter().copied())
            .chain(
                expand_limit
                    .then(|| update_queries.has_limit_from.iter().copied())
                    .into_iter()
                    .flatten(),
            );

        let mut fp_list = Vec::new();
        for fingerprint in narrowed {
            let Some(update_query) = update_queries.queries.get(&fingerprint) else {
                continue;
            };
            // `Some` → row is cached (UPDATE main path); `None` → row not cached
            // (INSERT, DELETE, or UPDATE of an uncached row).
            let invalidate = match row_changes {
                Some(row_changes) => self.row_cached_invalidation_check(
                    update_query,
                    table_metadata,
                    row_data,
                    row_changes,
                ),
                None => self.row_uncached_invalidation_check(
                    update_query,
                    table_metadata,
                    row_data,
                    key_data,
                    operation,
                ),
            };
            if invalidate {
                // Drift guard (PGC-227): on the UPDATE path (`Upsert`), a query
                // that invalidates here MUST be `change_dependent`, or
                // `handle_update` would have skipped this check and served
                // stale. `update_invalidation_possible` is the single source of
                // truth; this fails the moment a check branch diverges from it.
                // DELETE has its own invalidation branches that fire
                // independently of the flag, so scope the guard to `Upsert`.
                debug_assert!(
                    operation != CdcOperation::Upsert || update_query.change_dependent,
                    "invalidation fired for a non-change_dependent query on the UPDATE \
                     path: update_invalidation_possible is out of sync with \
                     row_*_invalidation_check"
                );
                fp_list.push(fingerprint);
            }
        }

        Ok(fp_list)
    }

    /// Decide whether a CDC row belongs in cache and, if so, upsert it once.
    ///
    /// Phase A (read-only): determine which cached queries the row matches.
    /// LocalEval queries are evaluated in Rust; PgEval queries are batched into
    /// combined `SELECT EXISTS (p1), …` round-trips on one autocommit pool
    /// connection, so they observe the pre-source-transaction snapshot, never
    /// the in-flight frame's uncommitted writes.
    ///
    /// Phase B (in-frame): if any query matched, a single unconditional upsert
    /// into the source table's cache table on `cdc_write_conn`. The shared
    /// cache table holds the row iff some cached query needs it, so one upsert
    /// suffices regardless of how many matched.
    ///
    /// Returns true if the row matched any cached query.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn update_queries_execute_batch(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row_data: &[Option<ByteString>],
        batch: Option<BatchEvalView<'_>>,
        local_candidates: &FingerprintSet,
    ) -> CacheResult<bool> {
        // Phase A: membership evaluation. The `core` borrow is confined to
        // this block so the in-frame write below doesn't hold it.
        let matched = {
            // No cached query references this relation → nothing to upsert.
            // Not an error (the relation simply isn't maintained in place).
            let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
                return Ok(false);
            };
            let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
                return Ok(false);
            };

            let total_queries = update_queries.queries.len();
            trace!("update_queries_execute_batch start [{total_queries}]");
            if total_queries == 0 {
                return Ok(false);
            }

            // Fresh-MV queries must be fully evaluated so every match is
            // dirty-marked (else a Fresh MV silently goes stale). Non-Fresh
            // queries only decide whether the row belongs in the shared
            // cache table — one match triggers the single upsert, so they
            // short-circuit, exactly as before MV existed.
            let mut matched = false;

            // LocalEval: Rust evaluation, complexity-ordered (simplest
            // first). Cheap, so always evaluated in full; `mv_dirty_mark`
            // self-gates on `Fresh`.
            let mut local_hit = false;
            // Candidate queries whose extracted constraints the row could
            // satisfy (computed once by the caller, shared with the memo pass).
            // The index holds the full LocalEval population (unconstrained
            // queries are always-candidates), so this never drops a true match.
            for &fingerprint in local_candidates {
                // Flagged for invalidation this frame → not maintained in
                // place (it forwards to origin and repopulates). Matches the
                // pre-deferral ordering, where the inline invalidate ran
                // before this executor so such queries were already excluded.
                if core.frame_invalidations.contains(&fingerprint) {
                    continue;
                }
                let Some(update_query) = update_queries.queries.get(&fingerprint) else {
                    continue;
                };
                // The eval index holds the full population (PGC-292); the local
                // matcher only handles LocalEval — PgEval candidates are matched
                // by the separate PgEval path below.
                if update_query.eval_strategy != UpdateEvalStrategy::LocalEval {
                    continue;
                }
                if !update_query_matches_locally(update_query, table_metadata, row_data) {
                    continue;
                }
                trace!("update_queries local-eval matched fingerprint {fingerprint}");
                core.mv_dirty_mark(fingerprint);
                matched = true;
                local_hit = true;
            }
            if local_hit {
                crate::metrics::handles().cdc.local_eval_hits.increment(1);
            }

            // PgEval (the expensive set): only Fresh-MV queries need full
            // evaluation (to dirty-mark matches); the rest short-circuit. Built
            // from the shared per-row candidate probe rather than a full sweep of
            // the relation's queries: `eval_index` holds the whole population
            // (LocalEval and PgEval alike), so `local_candidates` already contains
            // every PgEval query this row could match — narrowing here never drops
            // a true match, exactly as for the LocalEval loop above (PGC-292).
            let pg_eval: Vec<&UpdateQuery> = local_candidates
                .iter()
                .filter_map(|fp| update_queries.queries.get(fp))
                .filter(|q| {
                    q.eval_strategy == UpdateEvalStrategy::PgEval
                        && !core.frame_invalidations.contains(&q.fingerprint)
                })
                .collect();

            if !pg_eval.is_empty() {
                // Batch-covered queries consult the precomputed segment matrix
                // (PGC-241) — no round-trip. `frame_invalidations` was already
                // applied above (the matrix is built unfiltered); the Fresh-MV
                // dirty-mark self-gates, mirroring the per-row fresh/rest split.
                let (batched, fallback): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = match &batch {
                    Some(view) => pg_eval
                        .into_iter()
                        .partition(|q| view.covers(q.fingerprint)),
                    None => (Vec::new(), pg_eval),
                };
                if let Some(view) = &batch {
                    for update_query in &batched {
                        if view.hit(update_query.fingerprint) {
                            trace!(
                                "update_queries batched pg-eval matched fingerprint {}",
                                update_query.fingerprint
                            );
                            core.mv_dirty_mark(update_query.fingerprint);
                            matched = true;
                        }
                    }
                }

                // Per-row fallback for non-batchable shapes / uncovered rows.
                let (fresh_pg, rest_pg): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = fallback
                    .into_iter()
                    .partition(|q| core.mv_dirty_eval_required(q.fingerprint));

                let fresh_hits = self
                    .pg_eval_matches(&fresh_pg, table_metadata, row_data)
                    .await?;
                for &fingerprint in &fresh_hits {
                    core.mv_dirty_mark(fingerprint);
                }
                let mut pg_hit = !fresh_hits.is_empty();
                matched |= pg_hit;

                // Non-Fresh queries only decide the upsert: skip them entirely
                // once anything matched, else stop at the first match.
                if !matched && self.pg_eval_any(&rest_pg, table_metadata, row_data).await? {
                    matched = true;
                    pg_hit = true;
                }

                if pg_hit {
                    crate::metrics::handles().cdc.pg_eval_hits.increment(1);
                }
            }

            matched
        };

        // Phase B: single in-frame write, buffered for the frame flush (PGC-228).
        if matched {
            self.frame_cache_upsert(core, relation_oid, row_data)
                .await?;
        }

        Ok(matched)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore};
    use crate::query::ast::LiteralValue;
    use crate::query::cast::CastTarget;
    use tokio_postgres::types::Type;

    // Row layout: [id INT4 (PK), name TEXT, created_at TIMESTAMP].
    fn fixture_table() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "created_at".into(),
                position: 3,
                type_oid: 1114,
                data_type: Type::TIMESTAMP,
                type_name: "timestamp".into(),
                cache_type_name: "timestamp".into(),
                is_primary_key: false,
            },
        ]);
        TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(1001),
            name: "users".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn constraints_for(table: &str, tcs: Vec<TableConstraint>) -> QueryConstraints {
        let mut q = QueryConstraints::default();
        q.table_constraints.insert(EcoString::from(table), tcs);
        q
    }

    #[test]
    fn no_constraints_for_table_matches() {
        let table = fixture_table();
        let constraints = QueryConstraints::default();
        let row = vec![Some("1".into()), Some("alice".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn bare_comparison_matches_when_value_equal() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::Comparison(
                "id".into(),
                BinaryOp::Equal,
                LiteralValue::Integer(1),
            )],
        );
        let row = vec![Some("1".into()), Some("alice".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn bare_comparison_misses_when_value_differs() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::Comparison(
                "id".into(),
                BinaryOp::Equal,
                LiteralValue::Integer(1),
            )],
        );
        let row = vec![Some("2".into()), Some("alice".into()), None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    // PGC-182: CastComparison constraints must coerce the row's wire-text via
    // the cast target before comparing. These tests exercise the new arm.

    #[test]
    fn cast_comparison_int4_matches_when_coerced_value_equal() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        // name="42" coerces to Integer(42) → matches literal Integer(42).
        let row = vec![Some("1".into()), Some("42".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_int4_misses_when_coerced_value_differs() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        let row = vec![Some("1".into()), Some("99".into()), None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_int4_misses_when_row_unparseable() {
        // `'abc'::int4` raises in postgres; locally we treat it as non-match.
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        let row = vec![Some("1".into()), Some("abc".into()), None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_bool_matches_via_pg_bool_spelling() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Bool,
                BinaryOp::Equal,
                LiteralValue::Boolean(true),
            )],
        );
        let row = vec![Some("1".into()), Some("yes".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_date_matches_via_timestamp_prefix() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "created_at".into(),
                CastTarget::Date,
                BinaryOp::Equal,
                LiteralValue::String("2024-01-15".into()),
            )],
        );
        let row = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-01-15 09:00:00".into()),
        ];
        assert!(row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_null_row_value_misses() {
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::Equal,
                LiteralValue::Integer(42),
            )],
        );
        let row = vec![Some("1".into()), None, None];
        assert!(!row_constraints_match(&constraints, &table, &row));
    }

    #[test]
    fn cast_comparison_inequality_compares_numerically() {
        // Locks the PGC-186 op-flip fix on the CDC pre-filter path too:
        // `name::int4 > 100` matches when name="500".
        let table = fixture_table();
        let constraints = constraints_for(
            "users",
            vec![TableConstraint::CastComparison(
                "name".into(),
                CastTarget::Int4,
                BinaryOp::GreaterThan,
                LiteralValue::Integer(100),
            )],
        );
        let row = vec![Some("1".into()), Some("500".into()), None];
        assert!(row_constraints_match(&constraints, &table, &row));
    }
}
