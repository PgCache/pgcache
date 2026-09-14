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

use crate::pg::Lsn;
use crate::query::ast::{AstNode, LiteralValue, QueryExpr, TableNode};
use crate::query::constraints::{
    ColumnRange, column_ranges_disjoint, column_ranges_from_comparisons,
};
use crate::query::write::{
    INSERT_MAX_ROWS, InsertStatement, RelationRef, UpdateStatement, WriteClass,
};

/// Cap on the combined update + delete predicate maps a single table may hold in
/// one tier before it degrades to opaque. Unlike [`INSERT_MAX_ROWS`] this
/// counts *statements* (each UPDATE/DELETE contributes one predicate), not rows,
/// so it is tracked separately even though it currently shares the same value.
const UPDATE_DELETE_PREDICATE_CAP: usize = 64;

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
        self.deletes.extend(other.deletes);
        self.updates.extend(other.updates);
        if self.deletes.len() + self.updates.len() > UPDATE_DELETE_PREDICATE_CAP {
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
        self.deletes.clear();
        self.updates.clear();
    }

    fn has_row_predicates(&self) -> bool {
        self.inserts.is_some() || !self.deletes.is_empty() || !self.updates.is_empty()
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
        let stampable = matches!(self.active, Some(latest_seq) if latest_seq <= stamp_seq);
        if !stampable {
            return;
        }
        self.active = None;
        // Later bound wins on collision, as for tables.
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
    /// Earliest stamped bound across all waiting tiers — lets [`Self::purge`]
    /// skip its scan (it runs per gated read) until the watermark can clear
    /// something. `None` = nothing stamped.
    min_stamped_lsn: Option<Lsn>,
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
            min_stamped_lsn: None,
            enabled,
        }
    }

    /// Disable recording (e.g. the connection's database didn't match and the
    /// cache is off for its lifetime).
    pub(in crate::proxy::connection) fn disable(&mut self) {
        self.enabled = false;
        self.tables.clear();
        self.connection = ConnectionTiers::default();
        self.min_stamped_lsn = None;
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
                    agg.deletes
                        .push(column_ranges_from_comparisons(&delete.comparisons));
                    if agg.deletes.len() + agg.updates.len() > UPDATE_DELETE_PREDICATE_CAP {
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
                    agg.updates.push(update_predicate_build(update));
                    if agg.deletes.len() + agg.updates.len() > UPDATE_DELETE_PREDICATE_CAP {
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
                self.min_stamped_lsn = None;
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
        self.min_stamped_lsn_recompute();
    }

    /// Drop every waiting tier the watermark has passed. Active (unstamped)
    /// tiers and an unstampable connection entry never clear here.
    pub(in crate::proxy::connection) fn purge(&mut self, watermark: Lsn) {
        // Runs per gated read: skip the scan while nothing can clear.
        if self.min_stamped_lsn.is_none_or(|min| watermark < min) {
            return;
        }
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
        self.min_stamped_lsn_recompute();
    }

    fn min_stamped_lsn_recompute(&mut self) {
        let tables = self
            .tiers()
            .filter_map(|tiers| tiers.waiting.as_ref().map(|(lsn, _)| *lsn));
        self.min_stamped_lsn = tables.chain(self.connection.waiting).min();
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
                for delete in &agg.deletes {
                    if read_ranges.is_some_and(|ranges| column_ranges_disjoint(ranges, delete)) {
                        kinds.delete = true;
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

    #[test]
    fn test_delete_overflow_degrades_to_opaque() {
        let mut log = WriteLog::new(true);
        // One past the predicate cap (values are irrelevant — the cap is on count).
        for _ in 0..=UPDATE_DELETE_PREDICATE_CAP {
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
