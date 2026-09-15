//! Per-table pending-write aggregates: row-precise stores (column-major
//! insert rows, shape-grouped equality tuples, per-statement range maps),
//! their caps, and the disjointness checks the gate runs against them.

use std::collections::HashMap;
use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{
    ColumnRange, column_range_contains, column_ranges_from_comparisons,
};
use crate::query::write::{InsertRow, InsertStatement, UpdateStatement, WriteComparison};

/// Cap on the combined update + delete predicate maps a single table may hold in
/// one tier before it degrades to opaque. Counts *statements* (each
/// UPDATE/DELETE contributes one predicate map) — only the non-mergeable
/// shapes land here; all-equality predicates go to the far larger merged
/// tuple store ([`MERGED_PREDICATE_CAP`]).
pub(super) const UPDATE_DELETE_PREDICATE_CAP: usize = 64;

/// Cap on merged all-equality DELETE/UPDATE tuples one table's tier may hold
/// before it degrades to opaque. A tuple is one literal per predicate column,
/// so this sits far above the per-statement map cap: a row-at-a-time workload
/// (`DELETE ... WHERE id = $1` repeated) stays row-precise for this many
/// statements.
pub(super) const MERGED_PREDICATE_CAP: usize = 1024;

/// Cap on inserted rows one table's tier may accumulate before it degrades to
/// opaque. Column-major storage keeps a row to one cell per column, so this
/// sits far above the classification-time extraction cap
/// ([`crate::query::write::INSERT_MAX_ROWS`]): a row-at-a-time bulk insert
/// stays row-precise for this many rows.
pub(super) const INSERT_MERGED_ROWS_CAP: usize = 1024;

/// Cap on total cells (rows × union column width) one table's tier may hold.
/// Rows are stored dense to the union of the folded statements' column lists,
/// so wide or heterogeneous column lists hit this budget before the row cap —
/// it bounds the aggregate's memory, which the row count alone does not.
pub(super) const INSERT_MERGED_CELLS_CAP: usize = 8192;
/// Pending write state for one table within a tier.
#[derive(Debug, Default, Clone)]
pub(in crate::proxy::connection) struct TableAggregate {
    /// A non-row-enumerable write (MERGE/TRUNCATE, or a degraded
    /// INSERT/DELETE/UPDATE) is pending against this table → any read of it
    /// intersects, regardless of `inserts`/`deletes`/`updates`.
    pub opaque: bool,
    /// Row-enumerable INSERTs pending against this table (PGC-369). A read that
    /// is provably disjoint from every inserted row may still be served. `None`
    /// once `opaque` is set (an opaque write dominates).
    pub inserts: Option<InsertAggregate>,
    /// All-equality DELETE predicates pending against this table (PGC-381),
    /// shape-grouped: one literal tuple per statement, keyed by the sorted
    /// predicate column list. Semantically each tuple is the same per-column
    /// `Equal` map a legacy entry would hold — stored flat so the common
    /// row-at-a-time shape costs one tuple, not one map. Cleared once `opaque`
    /// is set.
    pub merged_deletes: MergedTuples,
    /// Per-column predicate ranges of DELETEs with a non-mergeable shape (a
    /// range comparison, or a duplicated column), one entry per DELETE
    /// (PGC-381). A read whose predicate is provably disjoint from every one
    /// may still be served — a delete only shrinks. Cleared once `opaque` is
    /// set.
    pub deletes: Vec<HashMap<EcoString, ColumnRange>>,
    /// All-equality-WHERE UPDATE predicates pending against this table
    /// (PGC-382), shape-grouped like `merged_deletes`, each entry carrying its
    /// WHERE tuple and SET values so the two-sided (WHERE + post-update image)
    /// check runs over flat tuples. Cleared once `opaque` is set.
    pub merged_updates: MergedUpdates,
    /// Total tuples across `merged_deletes` and `merged_updates` — the count
    /// [`MERGED_PREDICATE_CAP`] bounds, maintained incrementally so recording
    /// never rescans the shape maps.
    pub merged_predicates: usize,
    /// UPDATEs with a non-mergeable shape (a range WHERE comparison, a
    /// duplicated column), one entry each (PGC-382). A read is unaffected only
    /// if disjoint from both the WHERE predicate and the post-update image.
    /// Cleared once `opaque` is set.
    pub updates: Vec<UpdatePredicate>,
}

/// An UPDATE's affected rows, as two per-column range maps (PGC-382): `where_`
/// bounds the rows it touches (the shrink/value-change side); `image` bounds
/// their post-update column values (the grow side — a row moving *into* a read).
#[derive(Debug, Clone)]
pub(in crate::proxy::connection) struct UpdatePredicate {
    pub(super) where_ranges: HashMap<EcoString, ColumnRange>,
    pub(super) image_ranges: HashMap<EcoString, ColumnRange>,
}

/// Row-enumerable INSERTs pending against one table, stored column-major
/// (PGC-383): one shared column list, one literal tuple per inserted row —
/// each row is the point predicate `column = value` over its known cells.
/// Bounded by [`INSERT_MERGED_ROWS_CAP`] rows; beyond the cap the table
/// degrades to `opaque`.
#[derive(Debug, Clone, Default)]
pub(in crate::proxy::connection) struct InsertAggregate {
    /// Union of the folded statements' column lists, in first-seen order.
    columns: Vec<EcoString>,
    /// Positionally aligned with `columns`. `None` = cell value unknown
    /// (DEFAULT/expression, or a column this row's statement didn't mention).
    /// A row folded before later statements widened `columns` stays short —
    /// a missing trailing cell reads as `None`.
    rows: Vec<InsertRow>,
}

impl InsertAggregate {
    /// Fold in another INSERT's rows. Returns `false` if the total would exceed
    /// [`INSERT_MERGED_ROWS_CAP`] or [`INSERT_MERGED_CELLS_CAP`], signalling
    /// the caller to degrade the table to opaque. Checked before any mutation.
    pub(super) fn fold(&mut self, insert: &Arc<InsertStatement>) -> bool {
        if self.caps_exceeded(&insert.columns, insert.rows.len()) {
            return false;
        }
        let positions = self.column_positions_resolve(&insert.columns);
        for row in &insert.rows {
            self.row_push(&positions, row.iter().cloned());
        }
        true
    }

    /// Fold another aggregate's rows in. Returns `false` on cap overflow.
    pub(super) fn merge(&mut self, other: InsertAggregate) -> bool {
        if self.caps_exceeded(&other.columns, other.rows.len()) {
            return false;
        }
        if self.rows.is_empty() {
            *self = other;
            return true;
        }
        let positions = self.column_positions_resolve(&other.columns);
        for row in other.rows {
            self.row_push(&positions, row.into_iter());
        }
        true
    }

    /// Whether folding `incoming_rows` rows with `incoming_columns` would
    /// overflow the row or cell budget. The cell budget uses the prospective
    /// union width — a conservative bound, since rows folded before a widening
    /// stay short.
    fn caps_exceeded(&self, incoming_columns: &[EcoString], incoming_rows: usize) -> bool {
        let new_columns = incoming_columns
            .iter()
            .filter(|column| !self.columns.contains(column))
            .count();
        let rows_total = self.rows.len() + incoming_rows;
        rows_total > INSERT_MERGED_ROWS_CAP
            || rows_total * (self.columns.len() + new_columns) > INSERT_MERGED_CELLS_CAP
    }

    /// Map a statement's column list onto `self.columns`, extending it with
    /// names not seen before.
    fn column_positions_resolve(&mut self, statement_columns: &[EcoString]) -> Vec<usize> {
        statement_columns
            .iter()
            .map(|column| {
                self.columns
                    .iter()
                    .position(|existing| existing == column)
                    .unwrap_or_else(|| {
                        self.columns.push(column.clone());
                        self.columns.len() - 1
                    })
            })
            .collect()
    }

    /// Append one row, scattering its cells to their aggregate positions.
    fn row_push(
        &mut self,
        positions: &[usize],
        values: impl Iterator<Item = Option<LiteralValue>>,
    ) {
        let mut cells = InsertRow::new();
        cells.resize(self.columns.len(), None);
        for (value, &position) in values.zip(positions) {
            if let Some(cell) = cells.get_mut(position) {
                *cell = value;
            }
        }
        self.rows.push(cells);
    }

    /// Whether the read is provably disjoint from every inserted row — i.e. no
    /// inserted row can match the read, so it may be served. A row is excluded
    /// when some column the read constrains has a known cell value the read's
    /// range provably excludes ([`column_range_contains`] `== Some(false)`) —
    /// the same per-column test the map-based [`column_ranges_disjoint`] check
    /// applies, minus the per-row maps.
    pub(super) fn disjoint(&self, read_ranges: &HashMap<EcoString, ColumnRange>) -> bool {
        // An unsatisfiable read matches no row at all.
        if read_ranges
            .values()
            .any(|range| matches!(range, ColumnRange::Empty))
        {
            return true;
        }
        // Resolve the read's constrained columns to positions once.
        let constrained: Vec<(usize, &ColumnRange)> = self
            .columns
            .iter()
            .enumerate()
            .filter_map(|(position, column)| read_ranges.get(column).map(|range| (position, range)))
            .collect();
        if constrained.is_empty() {
            return self.rows.is_empty();
        }
        self.rows.iter().all(|row| {
            constrained.iter().any(|(position, range)| {
                matches!(
                    row.get(*position),
                    Some(Some(value)) if column_range_contains(range, value) == Some(false)
                )
            })
        })
    }
}

/// One all-equality predicate as its literal values, aligned with its shape's
/// sorted column list.
type EqualityTuple = SmallVec<[LiteralValue; 2]>;

/// Merged all-equality predicates, grouped by shape (the sorted column list).
type MergedTuples = HashMap<Box<[EcoString]>, Vec<EqualityTuple>>;

/// The sorted, distinct-column equality tuple of a WHERE predicate, or `None`
/// when the shape doesn't merge: a non-equality comparison, or a column
/// constrained twice (the legacy range-map path handles both, including the
/// contradictory `c = 1 AND c = 2` case it folds to `Empty`).
pub(super) fn equality_tuple_build(
    comparisons: &[WriteComparison],
) -> Option<(Box<[EcoString]>, EqualityTuple)> {
    if comparisons.iter().any(|(_, op, _)| *op != BinaryOp::Equal) {
        return None;
    }
    let mut pairs: SmallVec<[(&EcoString, &LiteralValue); 2]> = comparisons
        .iter()
        .map(|(column, _, value)| (column, value))
        .collect();
    pairs.sort_by(|a, b| a.0.cmp(b.0));
    if pairs.windows(2).any(|w| matches!(w, [a, b] if a.0 == b.0)) {
        return None;
    }
    let columns = pairs.iter().map(|(column, _)| (*column).clone()).collect();
    let values = pairs.iter().map(|(_, value)| (*value).clone()).collect();
    Some((columns, values))
}

/// Whether the read is provably disjoint from every merged tuple — the exact
/// check [`column_ranges_disjoint`] applies to a per-column `Equal` map, minus
/// the maps: a tuple is excluded when some column the read constrains carries
/// a value the read's range provably excludes.
pub(super) fn merged_tuples_disjoint(
    merged: &MergedTuples,
    read_ranges: &HashMap<EcoString, ColumnRange>,
) -> bool {
    // An unsatisfiable read matches no row at all.
    if read_ranges
        .values()
        .any(|range| matches!(range, ColumnRange::Empty))
    {
        return true;
    }
    merged.iter().all(|(columns, tuples)| {
        // Resolve the shape's columns against the read once.
        let constrained: SmallVec<[(usize, &ColumnRange); 2]> = columns
            .iter()
            .enumerate()
            .filter_map(|(position, column)| read_ranges.get(column).map(|range| (position, range)))
            .collect();
        if constrained.is_empty() {
            return tuples.is_empty();
        }
        tuples.iter().all(|tuple| {
            constrained.iter().any(|(position, range)| {
                matches!(
                    tuple.get(*position),
                    Some(value) if column_range_contains(range, value) == Some(false)
                )
            })
        })
    })
}

/// One merged UPDATE's SET values, aligned with its shape's SET column list.
/// `None` = non-literal RHS, post-update value unknown.
type SetTuple = SmallVec<[Option<LiteralValue>; 4]>;

/// A merged UPDATE's shape: the sorted WHERE column list and the SET column
/// list (statement order).
type UpdateShape = (Box<[EcoString]>, Box<[EcoString]>);

/// Merged all-equality-WHERE UPDATEs, grouped by [`UpdateShape`].
type MergedUpdates = HashMap<UpdateShape, Vec<(EqualityTuple, SetTuple)>>;

/// The shape key and tuples of a mergeable UPDATE, or `None` when it doesn't
/// merge: a non-equality/duplicated-column WHERE (see [`equality_tuple_build`]), or a
/// duplicated SET column (whose last-assignment-wins semantics only the legacy
/// map path preserves).
pub(super) fn update_tuple_build(
    update: &UpdateStatement,
) -> Option<(UpdateShape, (EqualityTuple, SetTuple))> {
    let (where_columns, where_values) = equality_tuple_build(&update.where_comparisons)?;
    let duplicate_set = update.set.iter().enumerate().any(|(i, (column, _))| {
        update
            .set
            .iter()
            .skip(i + 1)
            .any(|(other, _)| other == column)
    });
    if duplicate_set {
        return None;
    }
    // Sort SET pairs by column so `SET a = ?, b = ?` and `SET b = ?, a = ?`
    // share one shape instead of fragmenting the store.
    let mut set_pairs: SmallVec<[(&EcoString, &Option<LiteralValue>); 4]> = update
        .set
        .iter()
        .map(|(column, value)| (column, value))
        .collect();
    set_pairs.sort_by(|a, b| a.0.cmp(b.0));
    let set_columns = set_pairs
        .iter()
        .map(|(column, _)| (*column).clone())
        .collect();
    let set_values = set_pairs
        .iter()
        .map(|(_, value)| (*value).clone())
        .collect();
    Some(((where_columns, set_columns), (where_values, set_values)))
}

/// Whether the read is provably disjoint from every merged UPDATE — from both
/// the WHERE tuple (the rows it touches) and the post-update image (the WHERE
/// values with SET overrides applied; an unknown SET value constrains
/// nothing). The tuple-level equivalent of checking [`UpdatePredicate`]'s two
/// range maps.
pub(super) fn merged_updates_disjoint(
    merged: &MergedUpdates,
    read_ranges: &HashMap<EcoString, ColumnRange>,
) -> bool {
    // An unsatisfiable read matches no row at all.
    if read_ranges
        .values()
        .any(|range| matches!(range, ColumnRange::Empty))
    {
        return true;
    }
    merged.iter().all(|((where_columns, set_columns), tuples)| {
        // Resolve the shape's columns against the read once.
        let where_constrained: SmallVec<[(usize, &ColumnRange); 2]> = where_columns
            .iter()
            .enumerate()
            .filter_map(|(position, column)| read_ranges.get(column).map(|range| (position, range)))
            .collect();
        // Image side: WHERE columns keep their value unless a SET overrides
        // them; SET columns carry their new value.
        let where_in_image: SmallVec<[(usize, &ColumnRange); 2]> = where_constrained
            .iter()
            .filter(|(position, _)| {
                where_columns
                    .get(*position)
                    .is_some_and(|column| !set_columns.contains(column))
            })
            .copied()
            .collect();
        let set_constrained: SmallVec<[(usize, &ColumnRange); 4]> = set_columns
            .iter()
            .enumerate()
            .filter_map(|(position, column)| read_ranges.get(column).map(|range| (position, range)))
            .collect();
        tuples.iter().all(|(where_values, set_values)| {
            let where_excluded = where_constrained.iter().any(|(position, range)| {
                matches!(
                    where_values.get(*position),
                    Some(value) if column_range_contains(range, value) == Some(false)
                )
            });
            if !where_excluded {
                return false;
            }
            where_in_image.iter().any(|(position, range)| {
                matches!(
                    where_values.get(*position),
                    Some(value) if column_range_contains(range, value) == Some(false)
                )
            }) || set_constrained.iter().any(|(position, range)| {
                matches!(
                    set_values.get(*position),
                    Some(Some(value)) if column_range_contains(range, value) == Some(false)
                )
            })
        })
    })
}

impl TableAggregate {
    /// Fold another aggregate for the same table into this one. `opaque`
    /// dominates (and clears inserts/deletes); otherwise inserts and delete
    /// predicates combine, degrading to opaque if either overflows its cap.
    pub(super) fn merge(&mut self, other: TableAggregate) {
        self.opaque |= other.opaque;
        if self.opaque {
            self.degrade_opaque();
            return;
        }
        if let Some(other_inserts) = other.inserts {
            let mut inserts = self.inserts.take().unwrap_or_default();
            if !inserts.merge(other_inserts) {
                self.degrade_opaque();
                crate::metrics::handles()
                    .raw
                    .cap_degraded_insert
                    .increment(1);
                return;
            }
            self.inserts = Some(inserts);
        }
        for (columns, tuples) in other.merged_deletes {
            self.merged_deletes
                .entry(columns)
                .or_default()
                .extend(tuples);
        }
        for (shape, tuples) in other.merged_updates {
            self.merged_updates.entry(shape).or_default().extend(tuples);
        }
        self.merged_predicates += other.merged_predicates;
        self.deletes.extend(other.deletes);
        self.updates.extend(other.updates);
        if self.deletes.len() + self.updates.len() > UPDATE_DELETE_PREDICATE_CAP
            || self.merged_predicates > MERGED_PREDICATE_CAP
        {
            self.degrade_opaque();
            crate::metrics::handles()
                .raw
                .cap_degraded_update_delete
                .increment(1);
        }
    }

    /// Collapse to table-level opaque, discarding the finer row-predicate state.
    pub(super) fn degrade_opaque(&mut self) {
        self.opaque = true;
        self.inserts = None;
        self.merged_deletes.clear();
        self.deletes.clear();
        self.merged_updates.clear();
        self.updates.clear();
        self.merged_predicates = 0;
    }

    pub(super) fn has_row_predicates(&self) -> bool {
        self.inserts.is_some()
            || !self.merged_deletes.is_empty()
            || !self.deletes.is_empty()
            || !self.merged_updates.is_empty()
            || !self.updates.is_empty()
    }
}

/// Build an [`UpdatePredicate`] from a classified UPDATE (PGC-382): the WHERE
/// ranges bound the affected rows; the image ranges are those with each SET
/// column overridden by its new value (`Equal(literal)`), or made unconstrained
/// (removed) when the new value is unknown — a non-literal SET RHS.
pub(super) fn update_predicate_build(update: &UpdateStatement) -> UpdatePredicate {
    let where_ranges = column_ranges_from_comparisons(&update.where_comparisons);
    let mut image_ranges = where_ranges.clone();
    for (column, value) in &update.set {
        match value {
            Some(literal) => {
                image_ranges.insert(column.clone(), ColumnRange::Equal(literal.clone()));
            }
            None => {
                image_ranges.remove(column);
            }
        }
    }
    UpdatePredicate {
        where_ranges,
        image_ranges,
    }
}
