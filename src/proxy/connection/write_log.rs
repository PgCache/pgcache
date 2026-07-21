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
//! per-read gate. On top of that sits a bounded **active/waiting** pair of
//! segments giving LSN granularity — `active` gathers new (unstamped) writes;
//! at most one stamped `waiting` segment drains as the CDC apply watermark
//! passes its commit-LSN bound. Separating the two means a fresh write never
//! gates an older, already-applied one: the waiting batch clears on its own
//! LSN even while active holds newer writes.
//!
//! The gate consulting this log ([`WriteLog::decide`]) forwards a read that
//! could be superseded by a pending write. A non-row-enumerable write makes its
//! table `opaque` (any read of it intersects); a row-enumerable INSERT keeps its
//! rows ([`InsertAggregate`]), a DELETE keeps its WHERE predicate, and an UPDATE
//! keeps its WHERE plus post-update image ([`UpdatePredicate`]) — so a read
//! provably disjoint from every one can still be served (PGC-369/381/382).

use std::collections::{HashMap, VecDeque};
use std::ops::ControlFlow;
use std::sync::Arc;

use ecow::EcoString;

use crate::pg::Lsn;
use crate::query::ast::{AstNode, LiteralValue, QueryExpr, TableNode};
use crate::query::constraints::{
    ColumnRange, column_ranges_disjoint, column_ranges_from_comparisons,
};
use crate::query::write::{
    INSERT_MAX_ROWS, InsertStatement, RelationRef, UpdateStatement, WriteClass,
};

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
    /// Serve from cache — a pending INSERT on a referenced table was proven
    /// row-level disjoint from the read (PGC-369); a precision win to record.
    ServeDisjointInsert,
    /// Forward to origin — a pending write the read can't rule out.
    Forward(RawForwardReason),
}

/// What a single referenced table contributes to the gate decision.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TableWrite {
    /// No pending write on this table.
    None,
    /// A pending write the read can't rule out → the read must forward.
    Intersects,
    /// Only pending INSERTs, all proven disjoint from the read → serveable.
    DisjointInsert,
}

/// Max segments kept before the two oldest are merged. Two — one `active`
/// gathering writes, one `waiting` to clear — recovers LSN granularity without
/// per-statement state; when CDC keeps up the waiting batch clears before each
/// probe, so no merge happens and precision is per-batch. Raising this to keep
/// more LSN tiers under sustained lag is a one-line change.
const MAX_SEGMENTS: usize = 2;

/// The commit-LSN bound on a segment's writes — the point past which the CDC
/// apply watermark guarantees every write in the segment is applied.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(in crate::proxy::connection) enum PendingLsn {
    /// No bound sampled yet (the probe hasn't stamped this segment). Never
    /// clears until stamped.
    Unstamped,
    /// The apply watermark clears this segment once it reaches `lsn`.
    Stamped(Lsn),
    /// `PREPARE TRANSACTION`: the commit happens later, possibly from another
    /// session, so no probe LSN can bound it. Never cleared by the watermark;
    /// dropped only at connection close.
    Unstampable,
}

impl PendingLsn {
    /// The later-clearing of two bounds, for merging segments. A bound that
    /// never clears via the watermark (`Unstampable`, or an `Unstamped` segment
    /// that — once merged — is no longer the active one a probe could stamp)
    /// dominates a `Stamped` one; two `Stamped` bounds take the max. Keeping the
    /// *more* conservative bound guarantees a merged segment never clears before
    /// either input would have.
    fn later_of(self, other: PendingLsn) -> PendingLsn {
        match (self, other) {
            (PendingLsn::Unstampable, _) | (_, PendingLsn::Unstampable) => PendingLsn::Unstampable,
            (PendingLsn::Unstamped, _) | (_, PendingLsn::Unstamped) => PendingLsn::Unstamped,
            (PendingLsn::Stamped(a), PendingLsn::Stamped(b)) => PendingLsn::Stamped(a.max(b)),
        }
    }
}

/// Pending write state for one table within a segment.
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
    /// Per-column predicate ranges of DELETEs pending against this table
    /// (PGC-381), one entry per DELETE. A read whose predicate is provably
    /// disjoint from every one may still be served — a delete only shrinks.
    /// Cleared once `opaque` is set.
    pub deletes: Vec<HashMap<EcoString, ColumnRange>>,
    /// UPDATEs pending against this table (PGC-382), one entry each. A read is
    /// unaffected only if disjoint from both the WHERE predicate and the
    /// post-update image. Cleared once `opaque` is set.
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

/// Row-enumerable INSERTs pending against one table, as one point-predicate
/// range map per inserted row (PGC-383) — each row is a conjunction of
/// `column = value` over its known cells. Bounded by [`INSERT_MAX_ROWS`] rows;
/// beyond the cap the table degrades to `opaque`.
#[derive(Debug, Clone, Default)]
pub(in crate::proxy::connection) struct InsertAggregate {
    rows: Vec<HashMap<EcoString, ColumnRange>>,
}

impl InsertAggregate {
    /// Fold in another INSERT's rows. Returns `false` if the total would exceed
    /// [`INSERT_MAX_ROWS`], signalling the caller to degrade the table to opaque.
    fn fold(&mut self, insert: &Arc<InsertStatement>) -> bool {
        if self.rows.len() + insert.rows.len() > INSERT_MAX_ROWS {
            return false;
        }
        self.rows
            .extend(insert.rows.iter().map(|row| insert_row_ranges(insert, row)));
        true
    }

    /// Fold another aggregate's rows in. Returns `false` on cap overflow.
    fn merge(&mut self, other: InsertAggregate) -> bool {
        if self.rows.len() + other.rows.len() > INSERT_MAX_ROWS {
            return false;
        }
        self.rows.extend(other.rows);
        true
    }

    /// Whether the read is provably disjoint from every inserted row — i.e. no
    /// inserted row can match the read, so it may be served. An insert row is a
    /// point predicate, so this is the same [`column_ranges_disjoint`] test used
    /// for DELETE/UPDATE predicates.
    fn disjoint(&self, read_ranges: &HashMap<EcoString, ColumnRange>) -> bool {
        self.rows
            .iter()
            .all(|row| column_ranges_disjoint(read_ranges, row))
    }
}

/// The point-predicate range map for one inserted row: `column = value` over its
/// known cells (an unknown DEFAULT/expression cell leaves that column
/// unconstrained, so it can never establish disjointness).
fn insert_row_ranges(
    insert: &InsertStatement,
    row: &[Option<LiteralValue>],
) -> HashMap<EcoString, ColumnRange> {
    insert
        .columns
        .iter()
        .zip(row)
        .filter_map(|(column, value)| {
            value
                .as_ref()
                .map(|v| (column.clone(), ColumnRange::Equal(v.clone())))
        })
        .collect()
}

/// Connection-scoped pending writes whose target table is unknown (DDL, CALL,
/// EXECUTE, multi-statement, unparseable forwards). While set, *every* read
/// intersects until the segment clears.
#[derive(Debug, Default, Clone)]
pub(in crate::proxy::connection) struct ConnAggregate {
    pub opaque: bool,
}

/// One LSN tier of aggregated writes.
#[derive(Debug, Clone)]
pub(in crate::proxy::connection) struct WriteSegment {
    lsn: PendingLsn,
    /// Sequence of the newest write folded in — the probe stamps a segment only
    /// when no write arrived after the probe sampled its bound.
    latest_seq: u64,
    tables: HashMap<RelationRef, TableAggregate>,
    connection: ConnAggregate,
}

impl WriteSegment {
    fn new(seq: u64) -> Self {
        Self {
            lsn: PendingLsn::Unstamped,
            latest_seq: seq,
            tables: HashMap::new(),
            connection: ConnAggregate::default(),
        }
    }

    /// Fold the other segment's aggregates into this one (used when merging the
    /// two oldest segments on overflow). Opaque flags OR together, and the
    /// merged bound is the later-clearing of the two — so a never-clearing
    /// `Unstampable` (2PC) segment is never lost into a clearable one.
    fn absorb(&mut self, other: WriteSegment) {
        for (relation, other_agg) in other.tables {
            self.tables.entry(relation).or_default().merge(other_agg);
        }
        self.connection.opaque |= other.connection.opaque;
        self.lsn = self.lsn.later_of(other.lsn);
    }
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
                return;
            }
            self.inserts = Some(inserts);
        }
        self.deletes.extend(other.deletes);
        self.updates.extend(other.updates);
        if self.deletes.len() + self.updates.len() > INSERT_MAX_ROWS {
            self.degrade_opaque();
        }
    }

    /// Collapse to table-level opaque, discarding the finer row-predicate state.
    fn degrade_opaque(&mut self) {
        self.opaque = true;
        self.inserts = None;
        self.deletes.clear();
        self.updates.clear();
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

/// Per-connection write log. See the module docs for the model.
pub(in crate::proxy::connection) struct WriteLog {
    /// Newest-last. The back segment is `active` (unstamped, gathering) whenever
    /// one exists; every segment ahead of it is stamped. Bounded to
    /// [`MAX_SEGMENTS`] after each stamp by merging the two oldest.
    segments: VecDeque<WriteSegment>,
    next_seq: u64,
    /// Recording is off entirely when the feature is disabled or the connection
    /// can't serve from cache (`cache_disabled`).
    enabled: bool,
}

impl WriteLog {
    pub(in crate::proxy::connection) fn new(enabled: bool) -> Self {
        Self {
            segments: VecDeque::new(),
            next_seq: 0,
            enabled,
        }
    }

    /// Disable recording (e.g. the connection's database didn't match and the
    /// cache is off for its lifetime).
    pub(in crate::proxy::connection) fn disable(&mut self) {
        self.enabled = false;
        self.segments.clear();
    }

    pub(in crate::proxy::connection) fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub(in crate::proxy::connection) fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Whether any pending write is row-enumerable (an INSERT, DELETE, or
    /// UPDATE) — the cases that benefit from deriving the read's per-column
    /// ranges (PGC-369/381/382). Opaque/connection writes ignore them.
    pub(in crate::proxy::connection) fn has_row_predicates(&self) -> bool {
        self.segments.iter().any(|s| {
            s.tables
                .values()
                .any(|a| a.inserts.is_some() || !a.deletes.is_empty() || !a.updates.is_empty())
        })
    }

    /// Number of live LSN tiers — asserted in tests; a gate/metric consumer
    /// arrives in PGC-368.
    #[allow(dead_code)]
    pub(in crate::proxy::connection) fn segment_count(&self) -> usize {
        self.segments.len()
    }

    /// The `active` (back, unstamped) segment, creating one if the log is empty
    /// or its back segment has already been stamped.
    fn active(&mut self) -> &mut WriteSegment {
        let need_new = match self.segments.back() {
            None => true,
            Some(back) => !matches!(back.lsn, PendingLsn::Unstamped),
        };
        if need_new {
            let seq = self.next_seq;
            self.segments.push_back(WriteSegment::new(seq));
        }
        self.segments.back_mut().expect("active segment present")
    }

    /// Record a forwarded write into the active segment. No-op when disabled.
    pub(in crate::proxy::connection) fn record(&mut self, class: &WriteClass) {
        if !self.enabled {
            return;
        }
        crate::metrics::handles().raw.writes_recorded.increment(1);
        let seq = self.next_seq;
        self.next_seq += 1;
        let seg = self.active();
        seg.latest_seq = seq;
        match class {
            // Row-enumerable INSERT: keep the rows for row-level disjointness
            // (PGC-369), unless the table is already opaque or the rows overflow
            // the cap (then degrade to opaque).
            WriteClass::InsertRows(insert) => {
                let agg = seg.tables.entry(insert.relation.clone()).or_default();
                if !agg.opaque {
                    let mut inserts = agg.inserts.take().unwrap_or_default();
                    if inserts.fold(insert) {
                        agg.inserts = Some(inserts);
                    } else {
                        agg.degrade_opaque();
                    }
                }
            }
            // Row-enumerable DELETE: keep the predicate for row-level
            // disjointness (PGC-381), unless the table is already opaque or the
            // predicate count overflows the cap (then degrade to opaque).
            WriteClass::DeleteRows(delete) => {
                let agg = seg.tables.entry(delete.relation.clone()).or_default();
                if !agg.opaque {
                    agg.deletes
                        .push(column_ranges_from_comparisons(&delete.comparisons));
                    if agg.deletes.len() + agg.updates.len() > INSERT_MAX_ROWS {
                        agg.degrade_opaque();
                    }
                }
            }
            // Row-enumerable UPDATE: keep the WHERE predicate and the post-update
            // image for the two-sided disjointness check (PGC-382), unless the
            // table is opaque or the predicate count overflows the cap.
            WriteClass::UpdateRows(update) => {
                let agg = seg.tables.entry(update.relation.clone()).or_default();
                if !agg.opaque {
                    agg.updates.push(update_predicate_build(update));
                    if agg.deletes.len() + agg.updates.len() > INSERT_MAX_ROWS {
                        agg.degrade_opaque();
                    }
                }
            }
            WriteClass::Table(relation) => {
                seg.tables
                    .entry(relation.clone())
                    .or_default()
                    .degrade_opaque();
            }
            WriteClass::Connection => {
                seg.connection.opaque = true;
            }
            WriteClass::ConnectionUnstampable => {
                seg.connection.opaque = true;
                seg.lsn = PendingLsn::Unstampable;
            }
        }
    }

    /// The sequence to hand the probe at injection: writes recorded up to and
    /// including this may be stamped with the probe's LSN. `None` when there is
    /// nothing to stamp (no active segment awaiting a bound).
    pub(in crate::proxy::connection) fn stamp_seq(&self) -> Option<u64> {
        self.segments
            .back()
            .filter(|s| matches!(s.lsn, PendingLsn::Unstamped))
            .map(|s| s.latest_seq)
    }

    /// Stamp the active segment with the probe's LSN bound, but only if no write
    /// arrived after the probe sampled it (`active.latest_seq <= stamp_seq`).
    /// Otherwise the sample may predate a later write's commit, so skip and let
    /// a subsequent probe retry. Rolls the stamped segment to `waiting` and
    /// enforces the segment bound.
    pub(in crate::proxy::connection) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        let Some(active) = self.segments.back_mut() else {
            return;
        };
        // An Unstampable (2PC) segment must never take an LSN bound.
        if !matches!(active.lsn, PendingLsn::Unstamped) || active.latest_seq > stamp_seq {
            return;
        }
        active.lsn = PendingLsn::Stamped(lsn);
        self.segments_bound();
    }

    /// Drop every leading segment the watermark has passed. `Unstamped` and
    /// `Unstampable` segments never clear here.
    pub(in crate::proxy::connection) fn purge(&mut self, watermark: Lsn) {
        while let Some(front) = self.segments.front() {
            match front.lsn {
                PendingLsn::Stamped(lsn) if lsn <= watermark => {
                    self.segments.pop_front();
                }
                PendingLsn::Stamped(_) | PendingLsn::Unstamped | PendingLsn::Unstampable => break,
            }
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
        if self.segments.iter().any(|s| s.connection.opaque) {
            return RawDecision::Forward(RawForwardReason::Connection);
        }
        // Otherwise a read forwards iff it references a table with a pending
        // write it can't rule out. The walk covers joins, subqueries, and CTEs.
        let mut disjoint_proved = false;
        let intersects = query
            .try_for_each_node::<TableNode, ()>(&mut |table| match self
                .table_write(table, read_ranges)
            {
                TableWrite::Intersects => ControlFlow::Break(()),
                TableWrite::DisjointInsert => {
                    disjoint_proved = true;
                    ControlFlow::Continue(())
                }
                TableWrite::None => ControlFlow::Continue(()),
            })
            .is_break();
        if intersects {
            RawDecision::Forward(RawForwardReason::Table)
        } else if disjoint_proved {
            RawDecision::ServeDisjointInsert
        } else {
            RawDecision::Serve
        }
    }

    /// What a referenced table contributes to the gate decision. An opaque write
    /// always intersects; pending INSERTs/DELETEs/UPDATEs intersect unless the
    /// read is provably disjoint from every one (in which case the table is
    /// serveable but records that a row-level proof carried it).
    fn table_write(
        &self,
        table: &TableNode,
        read_ranges: Option<&HashMap<EcoString, ColumnRange>>,
    ) -> TableWrite {
        let mut outcome = TableWrite::None;
        for segment in &self.segments {
            for (relation, agg) in &segment.tables {
                if !relation_matches(relation, table) {
                    continue;
                }
                if agg.opaque {
                    return TableWrite::Intersects;
                }
                if let Some(inserts) = &agg.inserts {
                    if read_ranges.is_some_and(|ranges| inserts.disjoint(ranges)) {
                        outcome = TableWrite::DisjointInsert;
                    } else {
                        return TableWrite::Intersects;
                    }
                }
                for delete in &agg.deletes {
                    if read_ranges.is_some_and(|ranges| column_ranges_disjoint(ranges, delete)) {
                        outcome = TableWrite::DisjointInsert;
                    } else {
                        return TableWrite::Intersects;
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
                        outcome = TableWrite::DisjointInsert;
                    } else {
                        return TableWrite::Intersects;
                    }
                }
            }
        }
        outcome
    }

    /// Whether a table the read references has a pending row-enumerable write (an
    /// INSERT or DELETE). Cheap gate so the read-after-write gate skips the
    /// expensive per-column range derivation for reads no such write could affect
    /// (PGC-369, PGC-381).
    pub(in crate::proxy::connection) fn table_has_row_predicate(&self, table: &TableNode) -> bool {
        self.segments.iter().any(|segment| {
            segment.tables.iter().any(|(relation, agg)| {
                (agg.inserts.is_some() || !agg.deletes.is_empty() || !agg.updates.is_empty())
                    && relation_matches(relation, table)
            })
        })
    }

    /// Merge the two oldest segments until at most [`MAX_SEGMENTS`] remain. The
    /// merged segment takes the higher (later) LSN and the union of aggregates —
    /// conservative (it clears no earlier than either input) and it never
    /// touches the active (back) segment.
    fn segments_bound(&mut self) {
        while self.segments.len() > MAX_SEGMENTS {
            crate::metrics::handles().raw.segment_merges.increment(1);
            let older = self.segments.pop_front().expect("two segments to merge");
            let next = self.segments.front_mut().expect("merge target present");
            // `absorb` unions the aggregates and takes the later-clearing bound,
            // so merging never lets an Unstampable (2PC) segment clear early.
            next.absorb(older);
        }
    }
}

/// Whether a logged write's relation matches a table the read references.
/// Names must be equal; schemas are compared only when *both* sides are
/// schema-qualified — an unqualified name on either side conservatively matches,
/// since the proxy can't resolve `search_path` to a concrete schema.
fn relation_matches(relation: &RelationRef, table: &TableNode) -> bool {
    relation.name == table.name
        && match (&relation.schema, &table.schema) {
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

    /// Whether `relation` has any pending write (opaque or insert) in any segment.
    fn table_pending(log: &WriteLog, relation: &str) -> bool {
        let rel = RelationRef {
            schema: None,
            name: relation.into(),
        };
        log.segments.iter().any(|s| {
            s.tables
                .get(&rel)
                .is_some_and(|a| a.opaque || a.inserts.is_some())
        })
    }

    fn connection_pending(log: &WriteLog) -> bool {
        log.segments.iter().any(|s| s.connection.opaque)
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
        assert_eq!(log.decide(&q, Some(&r5)), RawDecision::ServeDisjointInsert);

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
        assert_eq!(log.decide(&q, Some(&r5)), RawDecision::ServeDisjointInsert);

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
        assert_eq!(log.decide(&q, Some(&r10)), RawDecision::ServeDisjointInsert);

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
        let first: Vec<i64> = (0..40).collect();
        let second: Vec<i64> = (40..90).collect(); // 40 + 50 > cap
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
        assert_eq!(log.decide(&q, Some(&r1)), RawDecision::ServeDisjointInsert);

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

    #[test]
    fn test_delete_overflow_degrades_to_opaque() {
        let mut log = WriteLog::new(true);
        // One past the predicate cap (values are irrelevant — the cap is on count).
        for _ in 0..=INSERT_MAX_ROWS {
            log.record(&delete_eq("orders", "id", 5));
        }
        // Past the predicate cap the table is opaque: even a disjoint read forwards.
        let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
        assert_eq!(
            log.decide(&query("SELECT * FROM orders"), Some(&r)),
            RawDecision::Forward(RawForwardReason::Table)
        );
    }

    #[test]
    fn test_opaque_write_dominates_delete() {
        // A later UPDATE (opaque) forces table-level regardless of the read's
        // disjointness from an earlier delete predicate.
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
            RawDecision::ServeDisjointInsert
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
            RawDecision::ServeDisjointInsert
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
    fn test_record_aggregates_into_active_segment() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        log.record(&table("orders"));
        log.record(&insert("users"));
        // Thousands would collapse the same way: one segment, per-table opaque.
        assert_eq!(log.segment_count(), 1);
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
    fn test_stamp_then_purge_clears_segment() {
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
    fn test_stamp_skipped_when_write_arrived_after_injection() {
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        // A write races in after the probe sampled its bound.
        log.record(&table("items"));
        log.stamp(seq, Lsn::from_raw(100));
        // The sample may predate `items`' commit, so nothing is stamped; the
        // whole active segment stays unstamped and never clears on this LSN.
        log.purge(Lsn::from_raw(1_000));
        assert!(table_pending(&log, "orders"));
        assert!(table_pending(&log, "items"));
    }

    #[test]
    fn test_active_waiting_separation() {
        // A fresh write must not gate an older, already-applied batch.
        let mut log = WriteLog::new(true);
        log.record(&table("orders"));
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(100)); // waiting: orders @ 100
        log.record(&table("items")); // active: items, unstamped
        assert_eq!(log.segment_count(), 2);
        // Watermark passes the waiting batch but not the active writes.
        log.purge(Lsn::from_raw(100));
        assert!(!table_pending(&log, "orders")); // cleared independently
        assert!(table_pending(&log, "items")); // active still pending
    }

    #[test]
    fn test_overflow_merges_oldest_two() {
        let mut log = WriteLog::new(true);
        // Three stamped batches at rising LSNs, none cleared.
        for (i, tbl) in ["a", "b", "c"].iter().enumerate() {
            log.record(&table(tbl));
            let seq = log.stamp_seq().expect("bound pending");
            log.stamp(seq, Lsn::from_raw((i as u64 + 1) * 100));
        }
        // Bounded to MAX_SEGMENTS; the two oldest merged, all tables retained.
        assert_eq!(log.segment_count(), MAX_SEGMENTS);
        assert!(table_pending(&log, "a"));
        assert!(table_pending(&log, "b"));
        assert!(table_pending(&log, "c"));
        // The merged (oldest) segment took the later bound (200), so `a` and `b`
        // clear together only once the watermark reaches it.
        log.purge(Lsn::from_raw(100));
        assert!(table_pending(&log, "a"));
        log.purge(Lsn::from_raw(200));
        assert!(!table_pending(&log, "a"));
        assert!(!table_pending(&log, "b"));
        assert!(table_pending(&log, "c"));
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
    fn test_unstampable_survives_merge() {
        // A 2PC prepare's pending state must not clear when overflow merges its
        // (never-clearing) segment into a stamped one.
        let mut log = WriteLog::new(true);
        log.record(&WriteClass::ConnectionUnstampable); // oldest: never clears
        for (i, tbl) in ["x", "y"].iter().enumerate() {
            log.record(&table(tbl));
            let seq = log.stamp_seq().expect("bound pending");
            log.stamp(seq, Lsn::from_raw((i as u64 + 1) * 100));
        }
        // Overflow merged the Unstampable segment into a stamped one; the merged
        // bound must stay never-clearing.
        assert_eq!(log.segment_count(), MAX_SEGMENTS);
        log.purge(Lsn::from_raw(u64::MAX));
        assert!(
            connection_pending(&log),
            "2PC pending state must survive a segment merge"
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
