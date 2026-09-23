//! The [`WriteLog`]: recording forwarded writes, stamping commit-LSN bounds,
//! purging on the settled watermark, and the per-read gate verdict.

use std::collections::HashMap;
use std::ops::ControlFlow;

use ecow::EcoString;

use crate::pg::Lsn;
use crate::query::ast::{AstNode, QueryExpr, TableNode};
use crate::query::constraints::{
    ColumnRange, column_ranges_disjoint, column_ranges_from_comparisons,
};
use crate::query::write::{RelationRef, WriteClass};

use super::aggregate::{
    MERGED_PREDICATE_CAP, TableAggregate, UPDATE_DELETE_PREDICATE_CAP, equality_tuple_build,
    merged_tuples_disjoint, merged_updates_disjoint, update_predicate_build, update_tuple_build,
};
use super::tiers::{ConnectionTiers, TableTiers};
use super::{DisjointKinds, RawBlocker, RawDecision, RawForwardReason};

/// The schema variants of one table name with pending writes. Keyed by bare
/// name so the gate's fuzzy schema matching (an unqualified side matches any
/// schema) is one hash probe plus a scan of this — almost always singleton —
/// bucket, instead of a scan of every logged relation.
type SchemaBucket = HashMap<Option<EcoString>, TableTiers>;

/// Per-connection write log. See the module docs for the model.
pub(in crate::proxy::connection) struct WriteLog {
    pub(super) tables: HashMap<EcoString, SchemaBucket>,
    pub(super) connection: ConnectionTiers,
    pub(super) next_seq: u64,
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
        let seq = self.next_seq;
        self.next_seq += 1;
        // An unstampable connection-scoped write forwards every read until
        // connection close, so any finer per-table state is unreachable — skip
        // recording it (and drop what exists, freeing the memory early).
        if self.connection.unstampable {
            return;
        }
        crate::metrics::handles().raw.writes_recorded.increment(1);
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
        let clearance = &crate::metrics::handles().raw.clearance;
        self.tables.retain(|_, bucket| {
            bucket.retain(|_, tiers| {
                tiers.waiting.retain(|tier| {
                    if tier.bound > watermark {
                        return true;
                    }
                    clearance.record(tier.stamped_at.elapsed());
                    false
                });
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
            return RawDecision::Forward(RawForwardReason::Connection, self.connection.blocker());
        }
        // Otherwise a read forwards iff it references a table with a pending
        // write it can't rule out. The walk covers joins, subqueries, and CTEs.
        // The break payload is the first blocking table's clearance constraint;
        // a later table could in principle carry a still-later bound, but one
        // blocking table suffices for attribution (PGC-440).
        let mut kinds = DisjointKinds::default();
        let blocked = query.try_for_each_node::<TableNode, RawBlocker>(&mut |table| match self
            .table_intersects(table, read_ranges, &mut kinds)
        {
            Some(blocker) => ControlFlow::Break(blocker),
            None => ControlFlow::Continue(()),
        });
        if let ControlFlow::Break(blocker) = blocked {
            RawDecision::Forward(RawForwardReason::Table, blocker)
        } else if kinds.any() {
            RawDecision::ServeDisjoint(kinds)
        } else {
            RawDecision::Serve
        }
    }

    /// Fold the read's disjointness from `table`'s pending writes into `kinds`;
    /// returns the table's clearance constraint if the read intersects a pending
    /// write it can't rule out (`None` = no intersection). All of the table's
    /// tiers are checked — not just the first blocking one — so the returned
    /// blocker is the slowest-to-clear constraint (PGC-440 attribution).
    fn table_intersects(
        &self,
        table: &TableNode,
        read_ranges: Option<&HashMap<EcoString, ColumnRange>>,
        kinds: &mut DisjointKinds,
    ) -> Option<RawBlocker> {
        let bucket = self.tables.get(&table.name)?;
        let mut blocker: Option<RawBlocker> = None;
        for (schema, tiers) in bucket {
            if !schema_matches(schema, &table.schema) {
                continue;
            }
            for (bound, agg) in tiers.aggregates_bounded() {
                if Self::aggregate_intersects(agg, read_ranges, kinds) {
                    let tier = bound.map_or(RawBlocker::Unstamped, RawBlocker::Stamped);
                    blocker = Some(blocker.map_or(tier, |prior| prior.merge(tier)));
                }
            }
        }
        blocker
    }

    /// Whether the read intersects one tier aggregate's pending writes. An
    /// opaque write always intersects; pending INSERTs/DELETEs/UPDATEs
    /// intersect unless the read is provably disjoint from every one (the
    /// proven-disjoint kinds fold into `kinds`).
    fn aggregate_intersects(
        agg: &TableAggregate,
        read_ranges: Option<&HashMap<EcoString, ColumnRange>>,
        kinds: &mut DisjointKinds,
    ) -> bool {
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
            if read_ranges.is_some_and(|ranges| merged_tuples_disjoint(&agg.merged_deletes, ranges))
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
