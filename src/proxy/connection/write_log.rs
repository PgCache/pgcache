//! Per-connection log of forwarded writes for read-after-write consistency
//! (PGC-124 / PGC-366).
//!
//! Strictly per-connection: a connection tracks only the writes *it* forwarded
//! to origin, so a subsequent cacheable read on the same connection can be
//! forwarded (never served stale) while those writes are in the
//! commit→CDC-apply window. No cross-connection guarantees — that is CDC's job.
//!
//! # Shape
//!
//! Writes aggregate per table, not per statement: thousands of writes to one
//! table collapse to O(1) state, so a bulk load never bloats memory or the
//! per-read gate. Each table carries its own bounded **active/waiting** pair of
//! tiers giving per-table LSN granularity — `active` gathers new (unstamped)
//! writes; the stamped `waiting` tier drains as the CDC apply watermark passes
//! its commit-LSN bound. Bounds are per table, so one table's clearance is
//! never held back by another's later bound, and a fresh write never gates an
//! older, already-applied one: the waiting tier clears on its own LSN even
//! while active holds newer writes.
//!
//! The gate consulting this log ([`WriteLog::decide`]) forwards a read that
//! could be superseded by a pending write. A non-row-enumerable write makes its
//! table `opaque` (any read of it intersects); a row-enumerable INSERT keeps its
//! rows ([`InsertAggregate`]), a DELETE keeps its WHERE predicate, and an UPDATE
//! keeps its WHERE plus post-update image ([`UpdatePredicate`]) — so a read
//! provably disjoint from every one can still be served (PGC-369/381/382).

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;

use crate::pg::Lsn;
use crate::query::ast::{AstNode, BinaryOp, LiteralValue, QueryExpr, TableNode};
use crate::query::constraints::{
    ColumnRange, column_range_contains, column_ranges_disjoint, column_ranges_from_comparisons,
};
use crate::query::write::{
    InsertRow, InsertStatement, RelationRef, UpdateStatement, WriteClass, WriteComparison,
};

/// Cap on the combined update + delete predicate maps a single table may hold in
/// one tier before it degrades to opaque. Counts *statements* (each
/// UPDATE/DELETE contributes one predicate map) — only the non-mergeable
/// shapes land here; all-equality predicates go to the far larger merged
/// tuple store ([`MERGED_PREDICATE_CAP`]).
const UPDATE_DELETE_PREDICATE_CAP: usize = 64;

/// Cap on merged all-equality DELETE/UPDATE tuples one table's tier may hold
/// before it degrades to opaque. A tuple is one literal per predicate column,
/// so this sits far above the per-statement map cap: a row-at-a-time workload
/// (`DELETE ... WHERE id = $1` repeated) stays row-precise for this many
/// statements.
const MERGED_PREDICATE_CAP: usize = 1024;

/// Cap on inserted rows one table's tier may accumulate before it degrades to
/// opaque. Column-major storage keeps a row to one cell per column, so this
/// sits far above the classification-time extraction cap
/// ([`crate::query::write::INSERT_MAX_ROWS`]): a row-at-a-time bulk insert
/// stays row-precise for this many rows.
const INSERT_MERGED_ROWS_CAP: usize = 1024;

/// Cap on total cells (rows × union column width) one table's tier may hold.
/// Rows are stored dense to the union of the folded statements' column lists,
/// so wide or heterogeneous column lists hit this budget before the row cap —
/// it bounds the aggregate's memory, which the row count alone does not.
const INSERT_MERGED_CELLS_CAP: usize = 8192;

/// Reason the read-after-write gate forwarded a cacheable read to origin
/// instead of serving it from cache (PGC-124), for metrics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum RawForwardReason {
    /// A pending write against a table the read references.
    Table,
    /// A connection-scoped pending write (unknown target table): every read on
    /// the connection intersects until it clears.
    Connection,
}

/// The read-after-write gate's verdict for one cacheable read ([`WriteLog::decide`]).
/// Side-effect-free so the caller records metrics exactly once per read; the three
/// variants make the illegal "forwarding yet proven disjoint" state unrepresentable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum RawDecision {
    /// Serve from cache — no pending write on this connection touches the read.
    Serve,
    /// Serve from cache — a pending row-enumerable write on a referenced table
    /// was proven row-level disjoint from the read (PGC-379); carries which write
    /// kinds contributed, for per-kind metrics.
    ServeDisjoint(DisjointKinds),
    /// Forward to origin — a pending write the read can't rule out.
    Forward(RawForwardReason),
}

/// Which pending write kinds a served read was proven disjoint from (PGC-384).
/// A read can be disjoint from several kinds at once.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(in crate::proxy::connection) struct DisjointKinds {
    pub insert: bool,
    pub delete: bool,
    pub update: bool,
}

impl DisjointKinds {
    fn any(self) -> bool {
        self.insert || self.delete || self.update
    }
}

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
    where_ranges: HashMap<EcoString, ColumnRange>,
    image_ranges: HashMap<EcoString, ColumnRange>,
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
    fn fold(&mut self, insert: &Arc<InsertStatement>) -> bool {
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
    fn merge(&mut self, other: InsertAggregate) -> bool {
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
    fn disjoint(&self, read_ranges: &HashMap<EcoString, ColumnRange>) -> bool {
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
fn equality_tuple_build(
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
fn merged_tuples_disjoint(
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
fn update_tuple_build(
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
fn merged_updates_disjoint(
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
    fn merge(&mut self, other: TableAggregate) {
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
    fn degrade_opaque(&mut self) {
        self.opaque = true;
        self.inserts = None;
        self.merged_deletes.clear();
        self.deletes.clear();
        self.merged_updates.clear();
        self.updates.clear();
        self.merged_predicates = 0;
    }

    fn has_row_predicates(&self) -> bool {
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
fn update_predicate_build(update: &UpdateStatement) -> UpdatePredicate {
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

/// One table's pending writes, as an active/waiting tier pair with per-table
/// LSN bounds.
#[derive(Debug, Default)]
struct TableTiers {
    /// Stamped with its commit-LSN bound; drains once the CDC apply watermark
    /// passes it.
    waiting: Option<(Lsn, TableAggregate)>,
    /// Gathering unstamped writes. The sequence is that of the newest write
    /// folded in — the probe stamps a tier only when no write arrived after the
    /// probe sampled its bound.
    active: Option<(u64, TableAggregate)>,
}

impl TableTiers {
    fn is_empty(&self) -> bool {
        self.waiting.is_none() && self.active.is_none()
    }

    /// Fold a write into the active tier, marking it with `seq`.
    fn active_mut(&mut self, seq: u64) -> &mut TableAggregate {
        let (latest_seq, agg) = self
            .active
            .get_or_insert_with(|| (seq, TableAggregate::default()));
        *latest_seq = seq;
        agg
    }

    /// Promote the active tier to waiting under the probe's bound, if no write
    /// arrived after the probe sampled it. On a waiting-tier collision (the
    /// previous stamp hasn't cleared yet) the old aggregate merges into the new
    /// one under the later bound — conservative, and scoped to this table only.
    fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        if !matches!(&self.active, Some((latest_seq, _)) if *latest_seq <= stamp_seq) {
            return;
        }
        let Some((_, mut agg)) = self.active.take() else {
            return;
        };
        if let Some((prior_lsn, prior_agg)) = self.waiting.take() {
            crate::metrics::handles().raw.tier_merges.increment(1);
            agg.merge(prior_agg);
            // A stamp's LSN is sampled after the prior one, so the new bound is
            // the later; keep it (max is belt-and-braces for the invariant).
            self.waiting = Some((lsn.max(prior_lsn), agg));
        } else {
            self.waiting = Some((lsn, agg));
        }
    }
}

/// Connection-scoped pending writes whose target table is unknown (DDL, CALL,
/// EXECUTE, multi-statement, unparseable forwards). While pending, *every* read
/// on the connection intersects.
#[derive(Debug, Default)]
struct ConnectionTiers {
    /// Stamped: clears once the watermark passes the bound.
    waiting: Option<Lsn>,
    /// Unstamped: the newest connection-scoped write's sequence.
    active: Option<u64>,
    /// `PREPARE TRANSACTION`: the commit happens later, possibly from another
    /// session, so no probe LSN can bound it. Never cleared by the watermark;
    /// dropped only at connection close.
    unstampable: bool,
}

impl ConnectionTiers {
    fn is_empty(&self) -> bool {
        self.waiting.is_none() && self.active.is_none() && !self.unstampable
    }

    fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        // A 2PC prepare must never take an LSN bound: its commit happens
        // later, possibly from another session. `record` refuses writes while
        // unstampable, but this state must hold even if that ever changes.
        if self.unstampable {
            return;
        }
        let stampable = matches!(self.active, Some(latest_seq) if latest_seq <= stamp_seq);
        if !stampable {
            return;
        }
        self.active = None;
        // Later bound wins on collision, as for tables.
        if self.waiting.is_some() {
            crate::metrics::handles().raw.tier_merges.increment(1);
        }
        self.waiting = Some(self.waiting.map_or(lsn, |prior| lsn.max(prior)));
    }
}

/// The schema variants of one table name with pending writes. Keyed by bare
/// name so the gate's fuzzy schema matching (an unqualified side matches any
/// schema) is one hash probe plus a scan of this — almost always singleton —
/// bucket, instead of a scan of every logged relation.
type SchemaBucket = HashMap<Option<EcoString>, TableTiers>;

/// Per-connection write log. See the module docs for the model.
pub(in crate::proxy::connection) struct WriteLog {
    tables: HashMap<EcoString, SchemaBucket>,
    connection: ConnectionTiers,
    next_seq: u64,
    /// Recording is off entirely when the feature is disabled or the connection
    /// can't serve from cache (`cache_disabled`).
    enabled: bool,
}

impl WriteLog {
    pub(in crate::proxy::connection) fn new(enabled: bool) -> Self {
        Self {
            tables: HashMap::new(),
            connection: ConnectionTiers::default(),
            next_seq: 0,
            enabled,
        }
    }

    /// Disable recording (e.g. the connection's database didn't match and the
    /// cache is off for its lifetime).
    pub(in crate::proxy::connection) fn disable(&mut self) {
        self.enabled = false;
        self.tables.clear();
        self.connection = ConnectionTiers::default();
    }

    pub(in crate::proxy::connection) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(in crate::proxy::connection) fn is_empty(&self) -> bool {
        self.tables.is_empty() && self.connection.is_empty()
    }

    /// Whether any pending write is row-enumerable (an INSERT, DELETE, or
    /// UPDATE) — the cases that benefit from deriving the read's per-column
    /// ranges (PGC-369/381/382). Opaque/connection writes ignore them.
    pub(in crate::proxy::connection) fn has_row_predicates(&self) -> bool {
        self.tiers()
            .any(|tiers| tiers.aggregates().any(TableAggregate::has_row_predicates))
    }

    fn tiers(&self) -> impl Iterator<Item = &TableTiers> {
        self.tables.values().flat_map(HashMap::values)
    }

    /// Record a forwarded write into its table's (or the connection's) active
    /// tier. No-op when disabled.
    pub(in crate::proxy::connection) fn record(&mut self, class: &WriteClass) {
        if !self.enabled {
            return;
        }
        crate::metrics::handles().raw.writes_recorded.increment(1);
        let seq = self.next_seq;
        self.next_seq += 1;
        // An unstampable connection-scoped write forwards every read until
        // connection close, so any finer per-table state is unreachable — skip
        // recording it (and drop what exists, freeing the memory early).
        if self.connection.unstampable {
            return;
        }
        match class {
            // Row-enumerable INSERT: keep the rows for row-level disjointness
            // (PGC-369), unless the table is already opaque or the rows overflow
            // the cap (then degrade to opaque).
            WriteClass::InsertRows(insert) => {
                let agg = self.table_active(&insert.relation, seq);
                if !agg.opaque {
                    let mut inserts = agg.inserts.take().unwrap_or_default();
                    if inserts.fold(insert) {
                        agg.inserts = Some(inserts);
                    } else {
                        agg.degrade_opaque();
                        crate::metrics::handles()
                            .raw
                            .cap_degraded_insert
                            .increment(1);
                    }
                }
            }
            // Row-enumerable DELETE: keep the predicate for row-level
            // disjointness (PGC-381), unless the table is already opaque or the
            // predicate count overflows the cap (then degrade to opaque).
            WriteClass::DeleteRows(delete) => {
                let agg = self.table_active(&delete.relation, seq);
                if !agg.opaque {
                    let overflow = match equality_tuple_build(&delete.comparisons) {
                        Some((columns, values)) => {
                            agg.merged_deletes.entry(columns).or_default().push(values);
                            agg.merged_predicates += 1;
                            agg.merged_predicates > MERGED_PREDICATE_CAP
                        }
                        None => {
                            agg.deletes
                                .push(column_ranges_from_comparisons(&delete.comparisons));
                            agg.deletes.len() + agg.updates.len() > UPDATE_DELETE_PREDICATE_CAP
                        }
                    };
                    if overflow {
                        agg.degrade_opaque();
                        crate::metrics::handles()
                            .raw
                            .cap_degraded_update_delete
                            .increment(1);
                    }
                }
            }
            // Row-enumerable UPDATE: keep the WHERE predicate and the post-update
            // image for the two-sided disjointness check (PGC-382), unless the
            // table is opaque or the predicate count overflows the cap.
            WriteClass::UpdateRows(update) => {
                let agg = self.table_active(&update.relation, seq);
                if !agg.opaque {
                    let overflow = match update_tuple_build(update) {
                        Some((shape, tuple)) => {
                            agg.merged_updates.entry(shape).or_default().push(tuple);
                            agg.merged_predicates += 1;
                            agg.merged_predicates > MERGED_PREDICATE_CAP
                        }
                        None => {
                            agg.updates.push(update_predicate_build(update));
                            agg.deletes.len() + agg.updates.len() > UPDATE_DELETE_PREDICATE_CAP
                        }
                    };
                    if overflow {
                        agg.degrade_opaque();
                        crate::metrics::handles()
                            .raw
                            .cap_degraded_update_delete
                            .increment(1);
                    }
                }
            }
            WriteClass::Table(relation) => {
                self.table_active(relation, seq).degrade_opaque();
            }
            WriteClass::Connection => {
                self.connection.active = Some(seq);
            }
            WriteClass::ConnectionUnstampable => {
                self.connection.unstampable = true;
                self.connection.active = None;
                self.connection.waiting = None;
                // Every read forwards until close; per-table state is moot.
                self.tables.clear();
            }
        }
    }

    fn table_active(&mut self, relation: &RelationRef, seq: u64) -> &mut TableAggregate {
        // Recording routes by exact identity; fuzzy matching is read-side only.
        self.tables
            .entry(relation.name.clone())
            .or_default()
            .entry(relation.schema.clone())
            .or_default()
            .active_mut(seq)
    }

    /// The sequence to hand the probe at injection: writes recorded up to and
    /// including this may be stamped with the probe's LSN. `None` when there is
    /// nothing to stamp (no active tier awaiting a bound).
    pub(in crate::proxy::connection) fn stamp_seq(&self) -> Option<u64> {
        let tables = self
            .tiers()
            .filter_map(|tiers| tiers.active.as_ref().map(|(seq, _)| *seq));
        tables.chain(self.connection.active).max()
    }

    /// Stamp each active tier with the probe's LSN bound — per table, and only
    /// where no write arrived after the probe sampled it (`latest_seq <=
    /// stamp_seq`). A table with a later write is skipped on its own (the sample
    /// may predate that write's commit) and a subsequent probe retries it;
    /// every other table still stamps.
    pub(in crate::proxy::connection) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        for tiers in self.tables.values_mut().flat_map(HashMap::values_mut) {
            tiers.stamp(stamp_seq, lsn);
        }
        self.connection.stamp(stamp_seq, lsn);
    }

    /// Drop every waiting tier the watermark has passed. Active (unstamped)
    /// tiers and an unstampable connection entry never clear here. Runs per
    /// gated read, but the scan is over the handful of tables with pending
    /// writes — no derived fast-path state to keep consistent.
    pub(in crate::proxy::connection) fn purge(&mut self, watermark: Lsn) {
        self.tables.retain(|_, bucket| {
            bucket.retain(|_, tiers| {
                if matches!(&tiers.waiting, Some((lsn, _)) if *lsn <= watermark) {
                    tiers.waiting = None;
                }
                !tiers.is_empty()
            });
            !bucket.is_empty()
        });
        if matches!(self.connection.waiting, Some(lsn) if lsn <= watermark) {
            self.connection.waiting = None;
        }
    }

    /// The gate's verdict for `query`: whether it could read data superseded by
    /// a still-pending write on this connection and so must be forwarded rather
    /// than served from cache. Conservative — any uncertainty forwards. Call
    /// [`Self::purge`] first so applied writes don't force needless forwards.
    ///
    /// `read_ranges` are the read's per-column value ranges for its single table
    /// (PGC-369), used for row-level INSERT disjointness; `None` (a multi-table
    /// read, or one whose ranges couldn't be derived) makes any pending insert
    /// forward conservatively.
    pub(in crate::proxy::connection) fn decide(
        &self,
        query: &QueryExpr,
        read_ranges: Option<&HashMap<EcoString, ColumnRange>>,
    ) -> RawDecision {
        // A connection-scoped pending write (unknown table) poisons every read.
        if !self.connection.is_empty() {
            return RawDecision::Forward(RawForwardReason::Connection);
        }
        // Otherwise a read forwards iff it references a table with a pending
        // write it can't rule out. The walk covers joins, subqueries, and CTEs.
        let mut kinds = DisjointKinds::default();
        let intersects = query
            .try_for_each_node::<TableNode, ()>(&mut |table| {
                if self.table_intersects(table, read_ranges, &mut kinds) {
                    ControlFlow::Break(())
                } else {
                    ControlFlow::Continue(())
                }
            })
            .is_break();
        if intersects {
            RawDecision::Forward(RawForwardReason::Table)
        } else if kinds.any() {
            RawDecision::ServeDisjoint(kinds)
        } else {
            RawDecision::Serve
        }
    }

    /// Fold the read's disjointness from `table`'s pending writes into `kinds`;
    /// returns `true` if the read intersects a pending write it can't rule out.
    /// An opaque write always intersects; pending INSERTs/DELETEs/UPDATEs
    /// intersect unless the read is provably disjoint from every one.
    fn table_intersects(
        &self,
        table: &TableNode,
        read_ranges: Option<&HashMap<EcoString, ColumnRange>>,
        kinds: &mut DisjointKinds,
    ) -> bool {
        let Some(bucket) = self.tables.get(&table.name) else {
            return false;
        };
        for (schema, tiers) in bucket {
            if !schema_matches(schema, &table.schema) {
                continue;
            }
            for agg in tiers.aggregates() {
                if agg.opaque {
                    return true;
                }
                if let Some(inserts) = &agg.inserts {
                    if read_ranges.is_some_and(|ranges| inserts.disjoint(ranges)) {
                        kinds.insert = true;
                    } else {
                        return true;
                    }
                }
                if !agg.merged_deletes.is_empty() {
                    if read_ranges
                        .is_some_and(|ranges| merged_tuples_disjoint(&agg.merged_deletes, ranges))
                    {
                        kinds.delete = true;
                    } else {
                        return true;
                    }
                }
                for delete in &agg.deletes {
                    if read_ranges.is_some_and(|ranges| column_ranges_disjoint(ranges, delete)) {
                        kinds.delete = true;
                    } else {
                        return true;
                    }
                }
                if !agg.merged_updates.is_empty() {
                    if read_ranges
                        .is_some_and(|ranges| merged_updates_disjoint(&agg.merged_updates, ranges))
                    {
                        kinds.update = true;
                    } else {
                        return true;
                    }
                }
                for update in &agg.updates {
                    // Serveable only if the read touches neither the updated rows
                    // (WHERE) nor their post-update image (grow).
                    let disjoint = read_ranges.is_some_and(|ranges| {
                        column_ranges_disjoint(ranges, &update.where_ranges)
                            && column_ranges_disjoint(ranges, &update.image_ranges)
                    });
                    if disjoint {
                        kinds.update = true;
                    } else {
                        return true;
                    }
                }
            }
        }
        false
    }

    /// Whether a table the read references has a pending row-enumerable write (an
    /// INSERT, DELETE, or UPDATE). Cheap gate so the read-after-write gate skips
    /// the expensive per-column range derivation for reads no such write could
    /// affect (PGC-369, PGC-381, PGC-382).
    pub(in crate::proxy::connection) fn table_has_row_predicate(&self, table: &TableNode) -> bool {
        self.tables.get(&table.name).is_some_and(|bucket| {
            bucket.iter().any(|(schema, tiers)| {
                schema_matches(schema, &table.schema)
                    && tiers.aggregates().any(TableAggregate::has_row_predicates)
            })
        })
    }
}

impl TableTiers {
    /// Both tiers' aggregates, waiting first.
    fn aggregates(&self) -> impl Iterator<Item = &TableAggregate> {
        self.waiting
            .iter()
            .map(|(_, agg)| agg)
            .chain(self.active.iter().map(|(_, agg)| agg))
    }
}

/// Whether a logged write's schema matches a read table's, given equal names
/// (the bucket key). Compared only when *both* sides are schema-qualified — an
/// unqualified name on either side conservatively matches, since the proxy
/// can't resolve `search_path` to a concrete schema.
fn schema_matches(relation: &Option<EcoString>, table: &Option<EcoString>) -> bool {
    match (relation, table) {
        (Some(a), Some(b)) => a == b,
        _ => true,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::BinaryOp;
    use crate::query::write::{
        DeleteStatement, InsertRow, InsertStatement, RelationRef, UpdateStatement,
    };
    use ordered_float::NotNan;
    use std::sync::Arc;

    fn table(name: &str) -> WriteClass {
        WriteClass::Table(RelationRef {
            schema: None,
            name: name.into(),
        })
    }

    fn insert(name: &str) -> WriteClass {
        WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: name.into(),
            },
            columns: vec!["id".into()],
            rows: vec![InsertRow::new()],
        }))
    }

    /// Whether `relation` has any pending write (opaque or insert) in any tier.
    fn table_pending(log: &WriteLog, relation: &str) -> bool {
        log.tables.get(relation).is_some_and(|bucket| {
            bucket
                .values()
                .flat_map(TableTiers::aggregates)
                .any(|a| a.opaque || a.inserts.is_some())
        })
    }

    fn connection_pending(log: &WriteLog) -> bool {
        !log.connection.is_empty()
    }

    fn query(sql: &str) -> QueryExpr {
        crate::query::ast::query_expr_parse(sql).expect("parse query")
    }

    #[test]
    fn test_intersects_referenced_table_only() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), None),
            RawDecision::Forward(RawForwardReason::Table)
        );
        assert_eq!(
            log.decide(&query("SELECT * FROM items WHERE id = 1"), None),
            RawDecision::Serve
        );
    }

    #[test]
    fn test_intersects_connection_scope_poisons_all_reads() {
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::Connection);
        assert_eq!(
            log.decide(&query("SELECT * FROM whatever"), None),
            RawDecision::Forward(RawForwardReason::Connection)
        );
    }

    #[test]
    fn test_intersects_covers_joins_and_subqueries() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        assert_eq!(
            log.decide(
                &query("SELECT * FROM users u JOIN orders o ON u.id = o.uid"),
                None
            ),
            RawDecision::Forward(RawForwardReason::Table)
        );
        assert_eq!(
            log.decide(
                &query("SELECT * FROM users WHERE id IN (SELECT uid FROM orders)"),
                None
            ),
            RawDecision::Forward(RawForwardReason::Table)
        );
        assert_eq!(
            log.decide(
                &query("SELECT * FROM users u JOIN items i ON u.id = i.uid"),
                None
            ),
            RawDecision::Serve
        );
    }

    #[test]
    fn test_intersects_schema_matching() {
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::Table(RelationRef {
            schema: Some("sales".into()),
            name: "orders".into(),
        }));
        assert_eq!(
            log.decide(&query("SELECT * FROM sales.orders"), None),
            RawDecision::Forward(RawForwardReason::Table)
        );
        // Different schema, same name → not the same table.
        assert_eq!(
            log.decide(&query("SELECT * FROM other.orders"), None),
            RawDecision::Serve
        );
        // Unqualified read conservatively matches (search_path unresolvable).
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), None),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    fn insert_int(table: &str, col: &str, values: &[i64]) -> WriteClass {
        WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: table.into(),
            },
            columns: vec![col.into()],
            rows: values
                .iter()
                .map(|v| [Some(LiteralValue::Integer(*v))].into_iter().collect())
                .collect(),
        }))
    }

    fn ranges(col: &str, range: ColumnRange) -> HashMap<EcoString, ColumnRange> {
        HashMap::from([(col.into(), range)])
    }

    #[test]
    fn test_intersects_insert_row_level_disjointness() {
        let mut log = WriteLog::new(true);
        log.record(&insert_int("orders", "id", &[2]));
        let q = query("SELECT * FROM orders WHERE id = 1");

        // Read on id = 5 is disjoint from the inserted id = 2 → serve, and the
        // row-level proof is recorded.
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&q, Some(&r5)),
            RawDecision::ServeDisjoint(DisjointKinds {
                insert: true,
                ..Default::default()
            })
        );

        // Read on id = 2 matches the inserted row → forward (no disjoint proof).
        let r2 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(2)));
        assert_eq!(
            log.decide(&q, Some(&r2)),
            RawDecision::Forward(RawForwardReason::Table)
        );

        // Without the read's ranges (unregistered / multi-table) → conservative.
        assert_eq!(
            log.decide(&q, None),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_intersects_insert_multi_row() {
        let mut log = WriteLog::new(true);
        log.record(&insert_int("orders", "id", &[2, 3, 4]));
        let q = query("SELECT * FROM orders");

        // 5 is outside every inserted value → disjoint.
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&q, Some(&r5)),
            RawDecision::ServeDisjoint(DisjointKinds {
                insert: true,
                ..Default::default()
            })
        );

        // 3 matches one inserted row → intersects.
        let r3 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(3)));
        assert_eq!(
            log.decide(&q, Some(&r3)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_intersects_insert_int_float_numeric() {
        // A pending INSERT of a float value against an integer-literal read must
        // compare by numeric value, not `LiteralValue` variant (PGC-124): `= 10`
        // is disjoint from `10.0` only when the numbers differ.
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            columns: vec!["id".into()],
            rows: vec![
                [Some(LiteralValue::Float(NotNan::new(20.0).unwrap()))]
                    .into_iter()
                    .collect(),
            ],
        })));
        let q = query("SELECT * FROM orders WHERE id = 10");

        // id = 10 vs inserted 20.0 → provably disjoint → serve.
        let r10 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(10)));
        assert_eq!(
            log.decide(&q, Some(&r10)),
            RawDecision::ServeDisjoint(DisjointKinds {
                insert: true,
                ..Default::default()
            })
        );

        // id = 20 vs inserted 20.0 → numerically equal → intersects.
        let r20 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(20)));
        assert_eq!(
            log.decide(&q, Some(&r20)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_intersects_insert_unknown_cell_forwards() {
        // A row whose predicate-column value is unknown (DEFAULT/expr) can't be
        // proven disjoint.
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            columns: vec!["id".into()],
            rows: vec![[None].into_iter().collect()],
        })));
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r5)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_insert_overflow_degrades_to_opaque() {
        let mut log = WriteLog::new(true);
        let first: Vec<i64> = (0..600).collect();
        let second: Vec<i64> = (600..1200).collect(); // 600 + 600 > cap
        log.record(&insert_int("orders", "id", &first));
        log.record(&insert_int("orders", "id", &second));
        // Degraded to opaque: even a disjoint read forwards.
        let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_insert_wide_rows_hit_cells_cap() {
        // 20-column rows exhaust the cell budget (8192 / 20 ≈ 409 rows) long
        // before the 1024-row cap — the memory bound, not the row count, must
        // govern wide inserts.
        fn wide_insert(row_count: usize) -> WriteClass {
            let columns: Vec<EcoString> =
                (0..20).map(|c| EcoString::from(format!("c{c}"))).collect();
            let rows = (0..row_count)
                .map(|r| {
                    (0..20)
                        .map(|c| {
                            let cell = i64::try_from(r * 20 + c).expect("cell id fits i64");
                            Some(LiteralValue::Integer(cell))
                        })
                        .collect()
                })
                .collect();
            WriteClass::InsertRows(Arc::new(InsertStatement {
                relation: RelationRef {
                    schema: None,
                    name: "orders".into(),
                },
                columns,
                rows,
            }))
        }
        let mut log = WriteLog::new(true);
        log.record(&wide_insert(300));
        // Still precise below the budget.
        let r = ranges("c0", ColumnRange::Equal(LiteralValue::Integer(-1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE c0 = -1"), Some(&r)),
            RawDecision::ServeDisjoint(DisjointKinds {
                insert: true,
                ..Default::default()
            })
        );
        // 300 + 200 = 500 rows × 20 columns = 10000 cells > 8192 → opaque.
        log.record(&wide_insert(200));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE c0 = -1"), Some(&r)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_insert_row_at_a_time_stays_precise() {
        // The motivating workload: an ORM inserting rows one statement at a
        // time. Hundreds of single-row INSERTs must keep row-level precision
        // (the old per-statement cap degraded to opaque at 64).
        let mut log = WriteLog::new(true);
        for i in 0..500 {
            log.record(&insert_int("orders", "id", &[i]));
        }
        let q = query("SELECT * FROM orders WHERE id = 9999");
        let disjoint = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
        assert_eq!(
            log.decide(&q, Some(&disjoint)),
            RawDecision::ServeDisjoint(DisjointKinds {
                insert: true,
                ..Default::default()
            })
        );
        let hit = ranges("id", ColumnRange::Equal(LiteralValue::Integer(250)));
        assert_eq!(
            log.decide(&q, Some(&hit)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_insert_diverging_column_lists() {
        // Statements with different column lists fold into one aggregate; a
        // column a row's statement didn't mention is unknown for that row and
        // can never prove disjointness.
        let mut log = WriteLog::new(true);
        log.record(&insert_int("orders", "a", &[1]));
        log.record(&insert_int("orders", "b", &[2]));
        let q = query("SELECT * FROM orders WHERE a = 5");
        // Read on a = 5: the a-row is excluded (1 ≠ 5) but the b-row's `a` cell
        // is unknown → forward.
        let ra = ranges("a", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&q, Some(&ra)),
            RawDecision::Forward(RawForwardReason::Table)
        );
        // Read constraining both columns: each row excluded via its own column.
        let rab = HashMap::from([
            (
                EcoString::from("a"),
                ColumnRange::Equal(LiteralValue::Integer(5)),
            ),
            (
                EcoString::from("b"),
                ColumnRange::Equal(LiteralValue::Integer(5)),
            ),
        ]);
        assert_eq!(
            log.decide(&q, Some(&rab)),
            RawDecision::ServeDisjoint(DisjointKinds {
                insert: true,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_non_insert_write_dominates_inserts() {
        // An UPDATE (opaque) after an INSERT forces table-level regardless of the
        // read's disjointness from the earlier inserted rows.
        let mut log = WriteLog::new(true);
        log.record(&insert_int("orders", "id", &[2]));
        log.record(&table("orders"));
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r5)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    fn delete_eq(table: &str, col: &str, value: i64) -> WriteClass {
        WriteClass::DeleteRows(Arc::new(DeleteStatement {
            relation: RelationRef {
                schema: None,
                name: table.into(),
            },
            comparisons: vec![(col.into(), BinaryOp::Equal, LiteralValue::Integer(value))],
        }))
    }

    #[test]
    fn test_decide_delete_predicate_disjointness() {
        let mut log = WriteLog::new(true);
        log.record(&delete_eq("orders", "id", 5));
        let q = query("SELECT * FROM orders WHERE id = 1");

        // Read on id = 1 is disjoint from the delete's id = 5 → serve.
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&q, Some(&r1)),
            RawDecision::ServeDisjoint(DisjointKinds {
                delete: true,
                ..Default::default()
            })
        );

        // Read on id = 5 overlaps the delete predicate → forward.
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&q, Some(&r5)),
            RawDecision::Forward(RawForwardReason::Table)
        );

        // Without the read's ranges (multi-table / unregistered) → conservative.
        assert_eq!(
            log.decide(&q, None),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_decide_delete_only_forwards_referenced_table() {
        let mut log = WriteLog::new(true);
        log.record(&delete_eq("orders", "id", 5));
        // A read of an unrelated table is unaffected.
        assert_eq!(
            log.decide(&query("SELECT * FROM items WHERE id = 5"), None),
            RawDecision::Serve
        );
    }

    /// A DELETE with a range comparison — the non-mergeable shape that lands in
    /// the legacy per-statement predicate list.
    fn delete_range(table: &str, col: &str, below: i64) -> WriteClass {
        WriteClass::DeleteRows(Arc::new(DeleteStatement {
            relation: RelationRef {
                schema: None,
                name: table.into(),
            },
            comparisons: vec![(col.into(), BinaryOp::LessThan, LiteralValue::Integer(below))],
        }))
    }

    #[test]
    fn test_delete_row_at_a_time_stays_precise() {
        // The motivating workload: hundreds of single-row equality DELETEs must
        // keep row-level precision (the old per-statement cap degraded at 64).
        let mut log = WriteLog::new(true);
        for i in 0..500 {
            log.record(&delete_eq("orders", "id", i));
        }
        let q = query("SELECT * FROM orders WHERE id = 9999");
        let disjoint = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
        assert_eq!(
            log.decide(&q, Some(&disjoint)),
            RawDecision::ServeDisjoint(DisjointKinds {
                delete: true,
                ..Default::default()
            })
        );
        let hit = ranges("id", ColumnRange::Equal(LiteralValue::Integer(250)));
        assert_eq!(
            log.decide(&q, Some(&hit)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_delete_merged_overflow_degrades_to_opaque() {
        let mut log = WriteLog::new(true);
        for i in 0..=MERGED_PREDICATE_CAP {
            log.record(&delete_eq(
                "orders",
                "id",
                i64::try_from(i).expect("cap fits i64"),
            ));
        }
        // Past the merged cap the table is opaque: even a disjoint read forwards.
        let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(999_999)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_delete_legacy_overflow_degrades_to_opaque() {
        let mut log = WriteLog::new(true);
        // Range deletes don't merge; one past the per-statement cap degrades.
        for i in 0..=UPDATE_DELETE_PREDICATE_CAP {
            log.record(&delete_range(
                "orders",
                "id",
                i64::try_from(i).expect("cap fits i64"),
            ));
        }
        let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(999_999)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_delete_mixed_merged_and_legacy_shapes() {
        // Equality deletes (merged) and a range delete (legacy) on one table:
        // the read must be disjoint from both stores to serve.
        let mut log = WriteLog::new(true);
        log.record(&delete_eq("orders", "id", 5));
        log.record(&delete_range("orders", "id", 3)); // id < 3
        let q = query("SELECT * FROM orders WHERE id = 10");
        // id = 10 clears both the id = 5 tuple and the id < 3 range.
        let r10 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(10)));
        assert_eq!(
            log.decide(&q, Some(&r10)),
            RawDecision::ServeDisjoint(DisjointKinds {
                delete: true,
                ..Default::default()
            })
        );
        // id = 2 clears the tuple but overlaps the range → forward.
        let r2 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(2)));
        assert_eq!(
            log.decide(&q, Some(&r2)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_delete_multi_column_equality_tuples() {
        // WHERE a = .. AND b = ..: tuples exclude per-statement, possibly via
        // different columns per tuple — a per-column value union would be
        // unsound, the tuple check is not.
        fn delete_ab(a: i64, b: i64) -> WriteClass {
            WriteClass::DeleteRows(Arc::new(DeleteStatement {
                relation: RelationRef {
                    schema: None,
                    name: "orders".into(),
                },
                comparisons: vec![
                    ("a".into(), BinaryOp::Equal, LiteralValue::Integer(a)),
                    ("b".into(), BinaryOp::Equal, LiteralValue::Integer(b)),
                ],
            }))
        }
        let mut log = WriteLog::new(true);
        log.record(&delete_ab(1, 10));
        log.record(&delete_ab(2, 20));
        let q = query("SELECT * FROM orders WHERE a = 2 AND b = 10");
        // a = 2 excludes the first tuple, b = 10 excludes the second.
        let r = HashMap::from([
            (
                EcoString::from("a"),
                ColumnRange::Equal(LiteralValue::Integer(2)),
            ),
            (
                EcoString::from("b"),
                ColumnRange::Equal(LiteralValue::Integer(10)),
            ),
        ]);
        assert_eq!(
            log.decide(&q, Some(&r)),
            RawDecision::ServeDisjoint(DisjointKinds {
                delete: true,
                ..Default::default()
            })
        );
        // a = 2, b = 20 matches the second tuple exactly → forward.
        let r_hit = HashMap::from([
            (
                EcoString::from("a"),
                ColumnRange::Equal(LiteralValue::Integer(2)),
            ),
            (
                EcoString::from("b"),
                ColumnRange::Equal(LiteralValue::Integer(20)),
            ),
        ]);
        assert_eq!(
            log.decide(&q, Some(&r_hit)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_delete_duplicate_column_routes_legacy() {
        // `id = 1 AND id = 2` is contradictory — the legacy range path folds it
        // to `Empty` (matches nothing), so any read is disjoint from it.
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::DeleteRows(Arc::new(DeleteStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            comparisons: vec![
                ("id".into(), BinaryOp::Equal, LiteralValue::Integer(1)),
                ("id".into(), BinaryOp::Equal, LiteralValue::Integer(2)),
            ],
        })));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::ServeDisjoint(DisjointKinds {
                delete: true,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_opaque_write_dominates_delete() {
        // A later whole-table (opaque) write forces table-level regardless of the
        // read's disjointness from an earlier delete predicate.
        let mut log = WriteLog::new(true);
        log.record(&delete_eq("orders", "id", 5));
        log.record(&table("orders"));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    fn update_eq(
        table: &str,
        where_col: &str,
        where_val: i64,
        set: &[(&str, Option<i64>)],
    ) -> WriteClass {
        WriteClass::UpdateRows(Arc::new(UpdateStatement {
            relation: RelationRef {
                schema: None,
                name: table.into(),
            },
            where_comparisons: vec![(
                where_col.into(),
                BinaryOp::Equal,
                LiteralValue::Integer(where_val),
            )],
            set: set
                .iter()
                .map(|(c, v)| ((*c).into(), v.map(LiteralValue::Integer)))
                .collect(),
        }))
    }

    #[test]
    fn test_decide_update_disjoint_serves() {
        // UPDATE ... WHERE id = 5; a read on id = 1 is disjoint from both the
        // matched rows and their image → serve.
        let mut log = WriteLog::new(true);
        log.record(&update_eq("orders", "id", 5, &[("v", Some(99))]));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::ServeDisjoint(DisjointKinds {
                update: true,
                ..Default::default()
            })
        );
        // A read on the matched rows (id = 5) overlaps the WHERE → forward.
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 5"), Some(&r5)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_decide_update_grow_forwards() {
        // UPDATE ... SET id = 1 WHERE id = 5 moves a row *into* `id = 1`: the
        // WHERE is disjoint from the read, but the image is not → forward.
        let mut log = WriteLog::new(true);
        log.record(&update_eq("orders", "id", 5, &[("id", Some(1))]));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_decide_update_value_change_forwards() {
        // UPDATE ... SET v = 99 WHERE id = 1 changes a value in the read set.
        let mut log = WriteLog::new(true);
        log.record(&update_eq("orders", "id", 1, &[("v", Some(99))]));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_decide_update_unknown_set_disjoint_serves() {
        // A non-literal SET (unknown image) on id = 5 rows doesn't touch id = 1.
        let mut log = WriteLog::new(true);
        log.record(&update_eq("orders", "id", 5, &[("v", None)]));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::ServeDisjoint(DisjointKinds {
                update: true,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_update_merged_survives_tier_collision() {
        // A second stamp before the first clears merges the waiting aggregate
        // into the new one; the prior tier's merged UPDATE tuples must survive
        // — losing them serves stale reads of the still-pending rows.
        let mut log = WriteLog::new(true);
        log.record(&update_eq("orders", "id", 5, &[("v", Some(7))]));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100));
        log.record(&update_eq("orders", "id", 6, &[("v", Some(8))]));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(200));
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 5"), Some(&r5)),
            RawDecision::Forward(RawForwardReason::Table),
            "the id = 5 UPDATE is still pending and must forward"
        );
        // And both clear once the watermark passes the merged bound.
        log.purge(Lsn::from_raw(200));
        assert!(log.is_empty());
    }

    #[test]
    fn test_delete_merged_survives_tier_collision() {
        let mut log = WriteLog::new(true);
        log.record(&delete_eq("orders", "id", 5));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100));
        log.record(&delete_eq("orders", "id", 6));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(200));
        let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 5"), Some(&r5)),
            RawDecision::Forward(RawForwardReason::Table),
            "the id = 5 DELETE is still pending and must forward"
        );
    }

    #[test]
    fn test_update_row_at_a_time_stays_precise() {
        // Hundreds of single-row `UPDATE ... SET v = ? WHERE id = ?` statements
        // must keep row-level precision (the old per-statement cap degraded at
        // 64).
        let mut log = WriteLog::new(true);
        for i in 0..500 {
            log.record(&update_eq("orders", "id", i, &[("v", Some(i))]));
        }
        let q = query("SELECT * FROM orders WHERE id = 9999");
        let disjoint = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
        assert_eq!(
            log.decide(&q, Some(&disjoint)),
            RawDecision::ServeDisjoint(DisjointKinds {
                update: true,
                ..Default::default()
            })
        );
        let hit = ranges("id", ColumnRange::Equal(LiteralValue::Integer(250)));
        assert_eq!(
            log.decide(&q, Some(&hit)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_update_merged_grow_forwards() {
        // The grow case must survive the merged path: with many merged updates
        // pending, one whose SET moves a row *into* the read still forwards.
        let mut log = WriteLog::new(true);
        for i in 100..200 {
            log.record(&update_eq("orders", "id", i, &[("v", Some(0))]));
        }
        log.record(&update_eq("orders", "id", 500, &[("id", Some(1))]));
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_update_set_order_shares_one_shape() {
        // `SET a = ?, b = ?` and `SET b = ?, a = ?` are the same shape; an ORM
        // iterating a hash-ordered dirty set must not fragment the store.
        let mut log = WriteLog::new(true);
        log.record(&update_eq(
            "orders",
            "id",
            1,
            &[("a", Some(1)), ("b", Some(2))],
        ));
        log.record(&update_eq(
            "orders",
            "id",
            2,
            &[("b", Some(3)), ("a", Some(4))],
        ));
        let shapes: usize = log
            .tables
            .values()
            .flat_map(HashMap::values)
            .flat_map(TableTiers::aggregates)
            .map(|agg| agg.merged_updates.len())
            .sum();
        assert_eq!(shapes, 1, "reordered SET lists must share one shape");
        // The image check still tracks each tuple's own values.
        let ra = ranges("a", ColumnRange::Equal(LiteralValue::Integer(4)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE a = 4"), Some(&ra)),
            RawDecision::Forward(RawForwardReason::Table),
            "a row updated into a = 4 must forward"
        );
    }

    #[test]
    fn test_update_delete_share_merged_cap() {
        // Merged deletes and updates draw on one combined budget.
        let mut log = WriteLog::new(true);
        for i in 0..600 {
            log.record(&delete_eq("orders", "id", i));
        }
        for i in 0..425 {
            log.record(&update_eq("orders", "id", i, &[("v", Some(0))]));
        }
        // 600 + 425 = 1025 > cap → opaque: even a disjoint read forwards.
        let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(999_999)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_update_duplicate_set_column_routes_legacy() {
        // `SET v = 1, v = 2` (last wins) can't merge; the legacy map path keeps
        // the override semantics: the final image v = 2 is what the read must
        // be disjoint from.
        let mut log = WriteLog::new(true);
        log.record(&update_eq(
            "orders",
            "id",
            5,
            &[("v", Some(1)), ("v", Some(2))],
        ));
        // Read on v = 1 (the overwritten value) is disjoint from the image
        // v = 2 and the WHERE id = 5 leaves v unconstrained... the read must
        // still check id: unconstrained on id → WHERE not excluded → forward.
        let rv1 = ranges("v", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE v = 1"), Some(&rv1)),
            RawDecision::Forward(RawForwardReason::Table)
        );
        // Disjoint via id on both sides → serve.
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
            RawDecision::ServeDisjoint(DisjointKinds {
                update: true,
                ..Default::default()
            })
        );
    }

    #[test]
    fn test_intersects_none_after_purge() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100));
        log.purge(Lsn::from_raw(100));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), None),
            RawDecision::Serve
        );
    }

    #[test]
    fn test_record_aggregates_per_table() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        log.record(&table("orders"));
        log.record(&insert("users"));
        // Thousands would collapse the same way: one active tier per table.
        assert!(table_pending(&log, "orders"));
        assert!(table_pending(&log, "users"));
        assert!(!table_pending(&log, "items"));
    }

    #[test]
    fn test_disabled_records_nothing() {
        let mut log = WriteLog::new(false);
        log.record(&table("orders"));
        assert!(log.is_empty());
    }

    #[test]
    fn test_connection_scope_write() {
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::Connection);
        assert!(connection_pending(&log));
    }

    #[test]
    fn test_stamp_then_purge_clears_table() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("active awaiting a bound");
        log.stamp(seq, Lsn::from_raw(100));
        // Not yet applied.
        log.purge(Lsn::from_raw(50));
        assert!(table_pending(&log, "orders"));
        // Watermark reaches the bound → clears.
        log.purge(Lsn::from_raw(100));
        assert!(log.is_empty());
    }

    #[test]
    fn test_stamp_partial_on_race() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        // A write races in after the probe sampled its bound.
        log.record(&table("items"));
        log.stamp(seq, Lsn::from_raw(100));
        // Per-table stamping: `orders` (recorded before the sample) is bounded
        // and clears; `items` (after) stays unstamped for the next probe.
        log.purge(Lsn::from_raw(1_000));
        assert!(!table_pending(&log, "orders"));
        assert!(table_pending(&log, "items"));
        // The next probe bounds `items`.
        let seq = log.stamp_seq().expect("items awaiting a bound");
        log.stamp(seq, Lsn::from_raw(2_000));
        log.purge(Lsn::from_raw(2_000));
        assert!(log.is_empty());
    }

    #[test]
    fn test_stamp_same_table_race_holds_earlier_writes() {
        // A racing write to the SAME table keeps that table's earlier writes
        // pending too: one active tier per table, guarded by its latest seq.
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        log.record(&table("orders"));
        log.stamp(seq, Lsn::from_raw(100));
        log.purge(Lsn::from_raw(1_000));
        assert!(table_pending(&log, "orders"));
    }

    #[test]
    fn test_active_waiting_separation() {
        // A fresh write must not gate an older, already-applied batch.
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100)); // orders waiting @ 100
        log.record(&table("items")); // items active, unstamped
        // Watermark passes the waiting tier but not the active writes.
        log.purge(Lsn::from_raw(100));
        assert!(!table_pending(&log, "orders")); // cleared independently
        assert!(table_pending(&log, "items")); // active still pending
    }

    #[test]
    fn test_tables_clear_on_own_bounds() {
        // Per-table bounds: a table stamped low clears without waiting for a
        // table stamped high (the cross-table coupling the segmented log had).
        let mut log = WriteLog::new(true);
        for (i, tbl) in ["a", "b", "c"].iter().enumerate() {
            log.record(&table(tbl));
            let seq = log.stamp_seq().expect("bound pending");
            log.stamp(seq, Lsn::from_raw((i as u64 + 1) * 100));
        }
        log.purge(Lsn::from_raw(100));
        assert!(!table_pending(&log, "a"));
        assert!(table_pending(&log, "b"));
        assert!(table_pending(&log, "c"));
        log.purge(Lsn::from_raw(300));
        assert!(log.is_empty());
    }

    #[test]
    fn test_waiting_collision_merges_under_later_bound() {
        // Two stamps on the same table before the first clears: the aggregates
        // merge and the later bound governs (conservative, scoped to the table).
        let mut log = WriteLog::new(true);
        log.record(&insert_int("orders", "id", &[1]));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100));
        log.record(&insert_int("orders", "id", &[2]));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(200));
        // Both inserts pending under the 200 bound.
        let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r1)),
            RawDecision::Forward(RawForwardReason::Table)
        );
        log.purge(Lsn::from_raw(100));
        assert!(table_pending(&log, "orders"));
        log.purge(Lsn::from_raw(200));
        assert!(log.is_empty());
    }

    #[test]
    fn test_unstampable_never_clears() {
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::ConnectionUnstampable);
        // A probe cannot bound a 2PC prepare.
        assert_eq!(log.stamp_seq(), None);
        log.purge(Lsn::from_raw(u64::MAX));
        assert!(connection_pending(&log));
    }

    #[test]
    fn test_unstampable_connection_never_takes_a_bound() {
        // Defense in depth for the deleted PendingLsn::Unstampable type guard:
        // even if a future change records a connection-scoped write while
        // unstampable (today `record` refuses), a probe must not bound it.
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::ConnectionUnstampable);
        log.connection.active = Some(log.next_seq);
        log.stamp(log.next_seq, Lsn::from_raw(100));
        log.purge(Lsn::from_raw(u64::MAX));
        assert!(connection_pending(&log), "2PC state must survive any probe");
    }

    #[test]
    fn test_unstampable_drops_and_blocks_table_state() {
        // Once the connection is unstampable (2PC prepare), every read forwards
        // until close, so per-table state is unreachable — dropped, and later
        // writes aren't recorded.
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        log.record(&WriteClass::ConnectionUnstampable);
        assert!(!table_pending(&log, "orders"));
        log.record(&table("items"));
        assert!(!table_pending(&log, "items"));
        assert_eq!(log.stamp_seq(), None);
        log.purge(Lsn::from_raw(u64::MAX));
        assert!(connection_pending(&log));
        assert_eq!(
            log.decide(&query("SELECT * FROM anything"), None),
            RawDecision::Forward(RawForwardReason::Connection)
        );
    }

    #[test]
    fn test_disable_clears_existing() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        log.disable();
        assert!(log.is_empty());
        assert!(!log.is_enabled());
        log.record(&table("orders"));
        assert!(log.is_empty());
    }
}
