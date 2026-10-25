//! The [`WriteLog`]: recording forwarded writes, stamping commit-LSN bounds,
//! purging on the settled watermark, and the per-read gate verdict.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use ecow::EcoString;
use smallvec::SmallVec;

use super::aggregate::{
    MERGED_PREDICATE_CAP, TableAggregate, UPDATE_DELETE_PREDICATE_CAP, equality_tuple_build,
    merged_tuples_disjoint, merged_updates_disjoint, references_disjoint, set_unreferenced,
    update_predicate_build, update_tuple_build,
};
use super::tiers::{ConnectionTiers, TableTiers};
use super::{DisjointKinds, RawBlocker, RawDecision, RawForwardReason};
use crate::cache::WriteTargetMap;
use crate::catalog::RelationName;
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::ast::rope::QueryTree;
use crate::query::constraints::{column_ranges_disjoint, table_column_ranges};
use crate::query::update::{ReadRelation, ReadRelations, ReferenceRanges};
use crate::query::write::WriteClass;

/// Pending writes resolved to one cached relation (PGC-542).
struct RelationTiers {
    name: RelationName,
    tiers: TableTiers,
}

/// The relations of one bare name; inline for a name cached in one or two
/// schemas.
type RelationOids = SmallVec<[Oid; 2]>;

/// Pending writes per cached relation, by OID, indexed by bare name for
/// matching a read's typed names. The index moves with the entries, so
/// nothing outside can leave the two apart.
#[derive(Default)]
struct Relations {
    by_oid: HashMap<Oid, RelationTiers>,
    by_name: HashMap<EcoString, RelationOids>,
}

impl Relations {
    fn is_empty(&self) -> bool {
        self.by_oid.is_empty()
    }

    fn clear(&mut self) {
        self.by_oid.clear();
        self.by_name.clear();
    }

    fn get(&self, oid: Oid) -> Option<&RelationTiers> {
        self.by_oid.get(&oid)
    }

    fn tiers(&self) -> impl Iterator<Item = &TableTiers> {
        self.by_oid.values().map(|relation| &relation.tiers)
    }

    fn tiers_mut(&mut self) -> impl Iterator<Item = &mut TableTiers> {
        self.by_oid.values_mut().map(|relation| &mut relation.tiers)
    }

    /// The relations a typed name could denote: those of the bare name, in
    /// `schema` when the name was qualified.
    fn named<'a>(
        &'a self,
        schema: Option<&'a str>,
        name: &str,
    ) -> impl Iterator<Item = &'a RelationTiers> + 'a {
        self.by_name
            .get(name)
            .into_iter()
            .flatten()
            .filter_map(|oid| self.by_oid.get(oid))
            .filter(move |relation| schema.is_none_or(|schema| relation.name.schema == schema))
    }

    /// The active tier of relation `oid`, entered under `name` if new.
    fn active_mut(&mut self, oid: Oid, name: &RelationName, seq: u64) -> &mut TableAggregate {
        let relation = self.by_oid.entry(oid).or_insert_with(|| {
            self.by_name.entry(name.name.clone()).or_default().push(oid);
            RelationTiers {
                name: name.clone(),
                tiers: TableTiers::default(),
            }
        });
        relation.tiers.active_mut(seq)
    }

    /// Drop every relation whose tiers `keep` rejects.
    fn retain(&mut self, mut keep: impl FnMut(&mut TableTiers) -> bool) {
        let by_name = &mut self.by_name;
        self.by_oid.retain(|oid, relation| {
            if keep(&mut relation.tiers) {
                return true;
            }
            if let Some(oids) = by_name.get_mut(&relation.name.name) {
                oids.retain(|candidate| candidate != oid);
                if oids.is_empty() {
                    by_name.remove(&relation.name.name);
                }
            }
            false
        });
    }
}

/// The schemas origin could have resolved an unresolved write's name in: the
/// one written, the path in force, or any (`None`).
type UnresolvedScope = Option<Arc<[EcoString]>>;

/// Pending writes to a name that resolved to no cached relation, or to an
/// unqualified name whose real target may be an uncached relation earlier on
/// the path. Matched by name against a relation cached after the write, so a
/// read of it still forwards.
struct UnresolvedTiers {
    /// The cached relations of the name at record time: origin wrote one of
    /// them (recorded by OID) or none of them, so a read of one proves
    /// nothing here.
    known: RelationOids,
    tiers: TableTiers,
}

/// Per-connection write log. See the module docs for the model.
pub(crate) struct WriteLog {
    relations: Relations,
    /// Keyed by the name as written, then by the scope it was written under.
    unresolved: HashMap<EcoString, HashMap<UnresolvedScope, UnresolvedTiers>>,
    pub(super) connection: ConnectionTiers,
    pub(super) next_seq: u64,
    /// Recording is off entirely when the feature is disabled or the connection
    /// can't serve from cache (`cache_disabled`).
    enabled: bool,
}

/// Test-only read-after-write disable (fault-injection feature): the integration
/// harness writes and reads back on one connection, where the gate would turn
/// deterministic cache hits into timing-dependent forwards. Always enabled in a
/// production build.
#[cfg(feature = "fault-injection")]
fn fault_read_your_writes_off() -> bool {
    std::env::var("PGCACHE_FAULT_READ_YOUR_WRITES_OFF")
        .is_ok_and(|v| !matches!(v.to_lowercase().as_str(), "" | "0" | "off" | "false"))
}
#[cfg(not(feature = "fault-injection"))]
fn fault_read_your_writes_off() -> bool {
    false
}

impl WriteLog {
    pub(crate) fn new() -> Self {
        Self::with_enabled(!fault_read_your_writes_off())
    }

    pub(crate) fn with_enabled(enabled: bool) -> Self {
        Self {
            relations: Relations::default(),
            unresolved: HashMap::new(),
            connection: ConnectionTiers::default(),
            next_seq: 0,
            enabled,
        }
    }

    /// Disable recording (e.g. the connection's database didn't match and the
    /// cache is off for its lifetime).
    pub(crate) fn disable(&mut self) {
        self.enabled = false;
        self.tables_clear();
        self.connection = ConnectionTiers::default();
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.relations.is_empty() && self.unresolved.is_empty() && self.connection.is_empty()
    }

    fn tables_clear(&mut self) {
        self.relations.clear();
        self.unresolved.clear();
    }

    pub(super) fn tiers(&self) -> impl Iterator<Item = &TableTiers> {
        let unresolved = self
            .unresolved
            .values()
            .flat_map(|scopes| scopes.values())
            .map(|entry| &entry.tiers);
        self.relations.tiers().chain(unresolved)
    }

    fn tiers_mut(&mut self) -> impl Iterator<Item = &mut TableTiers> {
        let unresolved = self
            .unresolved
            .values_mut()
            .flat_map(|scopes| scopes.values_mut())
            .map(|entry| &mut entry.tiers);
        self.relations.tiers_mut().chain(unresolved)
    }

    /// The unresolved entries of `name` whose scope admits `schema`.
    fn unresolved_named<'a>(
        &'a self,
        name: &str,
        schema: Option<&'a str>,
    ) -> impl Iterator<Item = &'a UnresolvedTiers> + 'a {
        self.unresolved
            .get(name)
            .into_iter()
            .flat_map(|scopes| scopes.iter())
            .filter(move |(scope, _)| scope_admits(scope, schema))
            .map(|(_, entry)| entry)
    }

    /// The tiers of every pending write on a relation the typed name could
    /// denote: the relations of that name and the unresolved entries
    /// admitting it.
    pub(super) fn pending_named<'a>(
        &'a self,
        schema: Option<&'a str>,
        name: &str,
    ) -> impl Iterator<Item = &'a TableTiers> + 'a {
        let relations = self
            .relations
            .named(schema, name)
            .map(|relation| &relation.tiers);
        let unresolved = self
            .unresolved_named(name, schema)
            .map(|entry| &entry.tiers);
        relations.chain(unresolved)
    }

    /// Record a forwarded write into its relations' (or the connection's)
    /// active tier. No-op when disabled.
    ///
    /// A table-scoped write is resolved through `targets` the way origin
    /// resolves its name (PGC-542): a qualified name in its schema, an
    /// unqualified one in every schema of `search_path` (origin takes the
    /// first holding the name among those the role has `USAGE` on, neither
    /// of which the proxy sees in full), an unknown path in any. Each cached
    /// relation it can land in records the write: with its rows for the table
    /// itself or a partition of it, opaque through a view (ADR-048). A name
    /// resolving to nothing, and an unqualified name whose real target may be
    /// an uncached relation earlier on the path, is recorded by name too, so
    /// a read of a relation cached after the write still forwards. The name
    /// entry keeps the rows only when no relation does: it matches nothing
    /// but a relation cached later, where an opaque mark costs little, while
    /// duplicating every unqualified write's rows would not.
    pub(crate) fn record(
        &mut self,
        class: &WriteClass,
        targets: Option<&WriteTargetMap>,
        search_path: Option<&Arc<[EcoString]>>,
    ) {
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
            WriteClass::Connection => {
                self.connection.active = Some(seq);
            }
            WriteClass::ConnectionUnstampable => {
                self.connection.unstampable = true;
                self.connection.active = None;
                self.connection.waiting = None;
                // Every read forwards until close; per-table state is moot.
                self.tables_clear();
            }
            WriteClass::InsertRows(_)
            | WriteClass::DeleteRows(_)
            | WriteClass::UpdateRows(_)
            | WriteClass::Table(_) => {
                let Some(relation) = class.relation() else {
                    return;
                };
                let path = search_path.map(|path| path.as_ref());
                let mut resolved = false;
                let mut known = RelationOids::new();
                let named = targets
                    .into_iter()
                    .flat_map(|targets| targets.named(&relation.name));
                for target in named {
                    if !known.contains(&target.relation) {
                        known.push(target.relation);
                    }
                    if !target.lands_in(relation.schema.as_deref(), path) {
                        continue;
                    }
                    resolved = true;
                    let agg = self
                        .relations
                        .active_mut(target.relation, &target.table, seq);
                    if target.opaque {
                        agg.degrade_opaque();
                    } else {
                        aggregate_record(agg, class);
                    }
                }
                if !resolved {
                    crate::metrics::handles().raw.writes_unresolved.increment(1);
                }
                if !resolved || relation.schema.is_none() {
                    let scope = match &relation.schema {
                        Some(schema) => Some(Arc::from([schema.clone()])),
                        None => search_path.cloned(),
                    };
                    let agg = self.unresolved_active(&relation.name, scope, &known, seq);
                    if resolved {
                        agg.degrade_opaque();
                    } else {
                        aggregate_record(agg, class);
                    }
                }
            }
        }
    }

    /// The active tier of the unresolved entry for `name` under `scope`,
    /// narrowing its `known` set to the relations known at every write it
    /// folds, so none that a later write could have reached is excluded.
    fn unresolved_active(
        &mut self,
        name: &EcoString,
        scope: UnresolvedScope,
        known: &[Oid],
        seq: u64,
    ) -> &mut TableAggregate {
        let entry = self
            .unresolved
            .entry(name.clone())
            .or_default()
            .entry(scope)
            .or_insert_with(|| UnresolvedTiers {
                known: known.iter().copied().collect(),
                tiers: TableTiers::default(),
            });
        entry.known.retain(|oid| known.contains(oid));
        entry.tiers.active_mut(seq)
    }

    /// The sequence to hand the probe at injection: writes recorded up to and
    /// including this may be stamped with the probe's LSN. `None` when there is
    /// nothing to stamp (no active tier awaiting a bound).
    pub(crate) fn stamp_seq(&self) -> Option<u64> {
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
    pub(crate) fn stamp(&mut self, stamp_seq: u64, lsn: Lsn) {
        for tiers in self.tiers_mut() {
            tiers.stamp(stamp_seq, lsn);
        }
        self.connection.stamp(stamp_seq, lsn);
    }

    /// Drop every waiting tier the watermark has passed. Active (unstamped)
    /// tiers and an unstampable connection entry never clear here. Runs per
    /// gated read, but the scan is over the handful of tables with pending
    /// writes — no derived fast-path state to keep consistent.
    pub(crate) fn purge(&mut self, watermark: Lsn) {
        let clearance = &crate::metrics::handles().raw.clearance;
        let tiers_purge = |tiers: &mut TableTiers| {
            tiers.waiting.retain(|tier| {
                if tier.bound > watermark {
                    return true;
                }
                clearance.record(tier.stamped_at.elapsed());
                false
            });
            !tiers.is_empty()
        };
        self.relations.retain(|tiers| tiers_purge(tiers));
        self.unresolved.retain(|_, scopes| {
            scopes.retain(|_, entry| tiers_purge(&mut entry.tiers));
            !scopes.is_empty()
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
    /// `read` is the registered read's form: its relations by OID, each with
    /// the columns it references and its per-reference ranges, which rule a
    /// pending write out when every reference of its relation is disjoint
    /// from it (PGC-369, PGC-512, PGC-542). `None` is an unregistered read,
    /// matched by the names it types: any pending write on a relation of that
    /// name forwards it.
    pub(crate) fn decide(&self, query: &QueryTree, read: Option<&ReadRelations>) -> RawDecision {
        // A connection-scoped pending write (unknown table) poisons every read.
        if !self.connection.is_empty() {
            return RawDecision::Forward(RawForwardReason::Connection, self.connection.blocker());
        }
        // Otherwise a read forwards iff it references a relation with a pending
        // write it can't rule out. The break payload is the first blocking
        // relation's clearance constraint; a later one could in principle
        // carry a still-later bound, but one blocking relation suffices for
        // attribution (PGC-440).
        let mut kinds = DisjointKinds::default();
        let blocked = match read {
            Some(read) => read
                .iter()
                .find_map(|(oid, relation)| self.relation_intersects(oid, relation, &mut kinds)),
            None => query
                .table_names()
                .find_map(|(schema, name)| self.name_intersects(schema, name, &mut kinds)),
        };
        if let Some(blocker) = blocked {
            RawDecision::Forward(RawForwardReason::Table, blocker)
        } else if kinds.any() {
            RawDecision::ServeDisjoint(kinds)
        } else {
            RawDecision::Serve
        }
    }

    /// Whether any pending table-scoped write is on a relation one of
    /// `names` could denote, so unrelated reads skip the registered-form
    /// lookup.
    pub(crate) fn names_pending<'a>(
        &self,
        names: impl Iterator<Item = (Option<&'a str>, &'a str)>,
    ) -> bool {
        let mut names = names;
        names.any(|(schema, name)| self.pending_named(schema, name).next().is_some())
    }

    /// Fold a registered read relation's disjointness from its pending writes
    /// into `kinds`: the writes resolved to it, and the unresolved writes of
    /// its name whose scope admits its schema and that did not know it. Returns
    /// the clearance constraint if some write can't be ruled out (`None` = no
    /// intersection), the slowest-to-clear across every tier (PGC-440).
    fn relation_intersects(
        &self,
        oid: Oid,
        relation: &ReadRelation,
        kinds: &mut DisjointKinds,
    ) -> Option<RawBlocker> {
        let ranges = relation.ranges.as_deref();
        let columns = relation.columns.as_ref();
        let resolved = self.relations.get(oid).map(|entry| &entry.tiers);
        let unresolved = self
            .unresolved_named(&relation.name, Some(&relation.schema))
            .filter(|entry| !entry.known.contains(&oid))
            .map(|entry| &entry.tiers);
        resolved
            .into_iter()
            .chain(unresolved)
            .fold(None, |blocker, tiers| {
                Self::tiers_intersects(tiers, ranges, columns, kinds, blocker)
            })
    }

    /// [`Self::relation_intersects`] for an unregistered read, which has
    /// nothing to prove with: every pending write on a relation the typed name
    /// could denote intersects.
    fn name_intersects(
        &self,
        schema: Option<&str>,
        name: &str,
        kinds: &mut DisjointKinds,
    ) -> Option<RawBlocker> {
        self.pending_named(schema, name)
            .fold(None, |blocker, tiers| {
                Self::tiers_intersects(tiers, None, None, kinds, blocker)
            })
    }

    /// Merge into `blocker` the constraint of every tier of `tiers` the read
    /// intersects. All tiers are checked, not just the first blocking one, so
    /// the result is the slowest-to-clear constraint (PGC-440 attribution).
    fn tiers_intersects(
        tiers: &TableTiers,
        read_ranges: Option<&[ReferenceRanges]>,
        read_columns: Option<&HashSet<EcoString>>,
        kinds: &mut DisjointKinds,
        mut blocker: Option<RawBlocker>,
    ) -> Option<RawBlocker> {
        for (bound, agg) in tiers.aggregates_bounded() {
            if Self::aggregate_intersects(agg, read_ranges, read_columns, kinds) {
                let tier = bound.map_or(RawBlocker::Unstamped, RawBlocker::Stamped);
                blocker = Some(blocker.map_or(tier, |prior| prior.merge(tier)));
            }
        }
        blocker
    }

    /// Whether the read intersects one tier aggregate's pending writes. An
    /// opaque write always intersects; pending INSERTs/DELETEs/UPDATEs
    /// intersect unless every reference of the table is provably disjoint from
    /// every one (the proven-disjoint kinds fold into `kinds`), or, for an
    /// UPDATE, the read never references a column it assigns.
    fn aggregate_intersects(
        agg: &TableAggregate,
        read_ranges: Option<&[ReferenceRanges]>,
        read_columns: Option<&HashSet<EcoString>>,
        kinds: &mut DisjointKinds,
    ) -> bool {
        if agg.opaque {
            return true;
        }
        if let Some(inserts) = &agg.inserts {
            if references_disjoint(read_ranges, |ranges| inserts.disjoint(ranges)) {
                kinds.insert = true;
            } else {
                return true;
            }
        }
        if !agg.merged_deletes.is_empty() {
            if references_disjoint(read_ranges, |ranges| {
                merged_tuples_disjoint(&agg.merged_deletes, ranges)
            }) {
                kinds.delete = true;
            } else {
                return true;
            }
        }
        for delete in &agg.deletes {
            if references_disjoint(read_ranges, |ranges| column_ranges_disjoint(ranges, delete)) {
                kinds.delete = true;
            } else {
                return true;
            }
        }
        if !agg.merged_updates.is_empty() {
            if merged_updates_disjoint(&agg.merged_updates, read_ranges, read_columns) {
                kinds.update = true;
            } else {
                return true;
            }
        }
        for update in &agg.updates {
            // Serveable if the read never looks at an assigned column, or
            // touches neither the updated rows (WHERE) nor their post-update
            // image (grow).
            let disjoint = set_unreferenced(&update.set_columns, read_columns)
                || references_disjoint(read_ranges, |ranges| {
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
}

/// Fold a table-scoped write into one relation's active aggregate.
fn aggregate_record(agg: &mut TableAggregate, class: &WriteClass) {
    match class {
        // Row-enumerable INSERT: keep the rows for row-level disjointness
        // (PGC-369), unless the table is already opaque or the rows overflow
        // the cap (then degrade to opaque).
        WriteClass::InsertRows(insert) => {
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
            if !agg.opaque {
                let overflow = match equality_tuple_build(&delete.comparisons) {
                    Some((columns, values)) => {
                        agg.merged_deletes.entry(columns).or_default().push(values);
                        agg.merged_predicates += 1;
                        agg.merged_predicates > MERGED_PREDICATE_CAP
                    }
                    None => {
                        agg.deletes.push(table_column_ranges(&delete.comparisons));
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
        WriteClass::Table(_) => agg.degrade_opaque(),
        WriteClass::Connection | WriteClass::ConnectionUnstampable => {}
    }
}

/// Whether an unresolved write's scope admits a read relation's schema: an
/// unknown scope or an unqualified typed name conservatively does.
fn scope_admits(scope: &UnresolvedScope, schema: Option<&str>) -> bool {
    match (scope, schema) {
        (Some(scope), Some(schema)) => scope.iter().any(|s| s == schema),
        _ => true,
    }
}
