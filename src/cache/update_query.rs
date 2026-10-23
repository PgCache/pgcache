use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use ecow::EcoString;
use iddqd::{IdHashItem, id_upcast};
use rootcause::Report;

use crate::cache::{CacheError, CacheResult};
#[cfg(feature = "proxy")]
use crate::catalog::TableMetadata;
#[cfg(feature = "proxy")]
use crate::id_hash::BuildIdHasher;
use crate::id_hash::impl_id_hashable;
use crate::oid::Oid;
use crate::query::ast::NodeId;
use crate::query::compiled_predicate::CompiledPredicate;
#[cfg(feature = "proxy")]
use crate::query::constraint_index::ColumnForms;
use crate::query::constraint_index::ConstraintIndex;
#[cfg(feature = "proxy")]
use crate::query::constraint_index::table_constraints_keyed;
use crate::query::constraints::TableConstraint;
use crate::query::predicates::RelationPredicates;
use crate::query::resolve::{QueryBranch, ResolvedQuery};
use crate::query::transform::PgEvalTemplate;
use crate::query::{Fingerprint, FingerprintMap, FingerprintSet};

/// The kind of subquery context a table was found in.
/// Determines invalidation behavior based on whether the subquery's
/// result set growing or shrinking affects the outer query.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum SubqueryKind {
    /// IN, EXISTS, FROM subquery, CTE.
    /// Set growth → outer result grows → invalidate.
    /// Set shrink → outer result shrinks → skip.
    Inclusion,
    /// NOT IN (<> ALL), NOT EXISTS.
    /// Set growth → outer result shrinks → skip.
    /// Set shrink → outer result grows → invalidate.
    Exclusion,
    /// Scalar subquery returning a single value.
    /// Any change → invalidate.
    Scalar,
}

/// Strategy for evaluating an update query's WHERE clause during CDC handling.
///
/// Determined once at registration time based on the shape of the resolved query.
/// `LocalEval` rows skip per-query PG round-trips; `PgEval` rows fall through to
/// the `INSERT ... WHERE EXISTS` dispatch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateEvalStrategy {
    /// WHERE clause is fully representable by the Rust evaluator and can be
    /// decided against a single CDC row without touching the cache DB.
    LocalEval,
    /// WHERE clause shape is not representable by the Rust evaluator (subqueries,
    /// GROUP BY/HAVING, unsupported expressions, multi-table, or non-FromClause
    /// source). CDC must use the per-query `INSERT ... WHERE EXISTS` path.
    PgEval,
}

/// One ORDER BY key of a LIMIT-windowed query, resolved to a plain local
/// column with PG's sort semantics made explicit (`Default` null ordering is
/// resolved at construction: NULLs sort as larger than every value, so
/// ASC → last, DESC → first). Consumed by the direction-aware window
/// invalidation check (PGC-334).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderByKey {
    pub column: EcoString,
    pub descending: bool,
    pub nulls_first: bool,
}

/// Per-column result of the CDC row-change lookup. `changed` mirrors the
/// original `IS DISTINCT FROM` projection; the ordering fields are populated
/// only for columns in the relation's `limit_order_columns` union and feed the
/// window-direction check (PGC-334). `old_less_than_new` is `None` when either side
/// is NULL (or the projection wasn't emitted) — consumers treat that as
/// unknown and stay conservative.
#[derive(Debug, Clone, Copy, Default)]
pub struct ColumnChange {
    pub changed: bool,
    pub old_less_than_new: Option<bool>,
    pub old_is_null: bool,
}

/// Column → change info for one CDC UPDATE row, from `query_row_changes` or
/// the batched segment eval.
pub(super) type RowChanges = HashMap<EcoString, ColumnChange>;

/// Whether an update query was derived from a direct table or a subquery table
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum UpdateQuerySource {
    /// Table appears in the FROM clause (inner join or single table)
    FromClause,
    /// Table appears inside a subquery, CTE, or derived table
    Subquery(SubqueryKind),
    /// Table is on the terminal optional side of an outer join.
    /// Its columns don't appear in WHERE or other join conditions, so
    /// CDC INSERT/DELETE can be handled in place without invalidation.
    /// The preserved side already has the row — changes here only affect
    /// which values fill the NULL-padded columns.
    OuterJoinTerminal,
    /// Table is on the non-terminal optional side of an outer join
    /// (its columns appear in WHERE or other join conditions).
    /// CDC events trigger full query invalidation rather than row-level updates.
    OuterJoinOptional,
}

/// One update query of a cached query on a relation. A query that reads the
/// relation from several branches has one per branch, numbered by `ordinal`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UpdateQueryKey {
    pub fingerprint: Fingerprint,
    pub ordinal: usize,
}

impl Hash for UpdateQueryKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // `IdHasher` keeps one write, so the ordinal is mixed into the
        // fingerprint, which is already a uniform hash.
        state.write_u64(
            self.fingerprint.get() ^ (self.ordinal as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15),
        );
    }
}

impl_id_hashable!(UpdateQueryKey);

/// `HashSet` of `UpdateQueryKey` with the passthrough hasher.
#[cfg(feature = "proxy")]
pub(crate) type UpdateQueryKeySet = HashSet<UpdateQueryKey, BuildIdHasher<UpdateQueryKey>>;

/// Query used to update cached results when data changes
#[derive(Debug, Clone)]
pub struct UpdateQuery {
    /// Fingerprint of cached query that generated this update query
    pub fingerprint: Fingerprint,
    /// Position among the fingerprint's update queries on this relation,
    /// assigned when they are registered.
    pub ordinal: usize,
    /// The query the update query is a branch of: the cached query, or its
    /// decorrelated form. Every update query derived from it shares it.
    pub bound: Arc<ResolvedQuery>,
    /// The branch the table sits in, by node id (ADR-057).
    pub branch: NodeId,
    /// Whether this table was found directly in FROM or inside a subquery
    pub source: UpdateQuerySource,
    /// The table reference it stands for: the `Table` node of the branch.
    pub table: NodeId,
    /// What that reference's rows answer to, by column name: the filters CDC
    /// matches rows against and the link columns whose change can move a row
    /// to partners that were never loaded.
    pub predicates: RelationPredicates,
    /// Whether the parent cached query's population is capped
    /// (`max_limit.is_some()`). CDC then invalidates on any change that could
    /// open a gap at the window's boundary: every DELETE on a FromClause
    /// reference, and a shrink as well as a growth of a subquery (ADR-060).
    pub has_limit: bool,
    /// Eval strategy for this query's WHERE clause during CDC row matching.
    pub eval_strategy: UpdateEvalStrategy,
    /// Columns from ORDER BY / WHERE / HAVING that define the LIMIT window.
    /// Populated only when `has_limit`; consumed by row_cached_invalidation_check
    /// to detect updates that may demote a cached row out of the window (PGC-94).
    pub limit_window_columns: HashSet<EcoString>,
    /// The full ORDER BY key spec when every key is a plain local column and
    /// the shape is a single-table FromClause SELECT without GROUP BY / HAVING
    /// / DISTINCT. Lets the window check tell promotions (row can only rise —
    /// provably safe in place) from demotions (may open an uncached gap at the
    /// window boundary — must invalidate). `None` → always invalidate on a
    /// window-column change, the pre-PGC-334 behavior.
    pub order_by_keys: Option<Box<[OrderByKey]>>,
    /// Whether a CDC UPDATE for this query can ever invalidate. When `false`,
    /// `handle_update` skips the `query_row_changes` SELECT and the invalidation
    /// check entirely (PGC-227). Set at registration from
    /// `update_invalidation_possible` (the single source of truth).
    pub change_dependent: bool,
    /// Whether this query's PgEval membership can be evaluated for many CDC
    /// rows in one multi-row VALUES statement (PGC-241). Requires per-row
    /// independence: no GROUP BY / HAVING and no aggregates in the SELECT list
    /// (those evaluate against the substituted rows *as a set*, so a multi-row
    /// VALUES would change the per-row answer). Set at registration; only
    /// meaningful for `eval_strategy == PgEval`.
    pub pg_batchable: bool,
    /// This relation's columns whose values CDC eval reads: WHERE, join
    /// predicates, GROUP BY, HAVING — not the outer SELECT list. An
    /// unchanged-toast UPDATE that elides one of these and can't be repaired
    /// makes every eval verdict for this query untrustworthy, forcing
    /// invalidation (PGC-264).
    pub predicate_columns: HashSet<EcoString>,
    /// `QueryBranch::is_single_table` of the branch, kept because the CDC
    /// invalidation checks consult it per row per query and it walks the tree.
    pub is_single_table: bool,
    /// Pre-canonicalized WHERE clause for the LocalEval membership probe
    /// (`update_query_matches_locally`), built once at registration so the per
    /// row CDC path skips re-destructuring the tree (PGC-339). `None`
    /// means either no WHERE clause (unconditional match) or a non-LocalEval
    /// query (never consulted — `eval_strategy != LocalEval`).
    pub compiled_where: Option<CompiledPredicate>,
    /// Precomputed membership-SELECT template for the PgEval path (PGC-343): the
    /// query with its CDC'd table replaced by a `VALUES` subquery, deparsed once
    /// so the per-row check renders only the row literals instead of cloning and
    /// re-deparsing the whole AST. `None` for LocalEval queries or shapes the
    /// template builder can't handle (the per-row deparse still works).
    pub pg_eval_template: Option<PgEvalTemplate>,
}

impl UpdateQuery {
    pub fn key(&self) -> UpdateQueryKey {
        UpdateQueryKey {
            fingerprint: self.fingerprint,
            ordinal: self.ordinal,
        }
    }

    /// The branch CDC evaluates a changed row against.
    pub fn branch(&self) -> CacheResult<QueryBranch<'_>> {
        QueryBranch::find(&self.bound, self.branch).ok_or_else(|| {
            debug_assert!(false, "an update query names a branch of its query");
            Report::from(CacheError::InvalidQuery)
        })
    }

    /// Whether a CDC UPDATE (`operation = Upsert`) for this query could ever
    /// invalidate it: the static upper bound of `row_cached_invalidation_check`
    /// ∪ `row_uncached_invalidation_check` over all possible row values. This is
    /// the single source of truth for `change_dependent` (PGC-227); each clause
    /// mirrors a check branch and the two must stay in lockstep — the
    /// `debug_assert` in `update_queries_check_invalidate` enforces it at
    /// runtime by failing if a check ever invalidates while this returns false.
    pub fn update_invalidation_possible(&self) -> bool {
        // Cached path: Subquery / non-terminal outer join always invalidate.
        matches!(
            self.source,
            UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional
        )
        // Cached path: a windowed FromClause query invalidates when a window
        // column changes (PGC-94).
        || (matches!(self.source, UpdateQuerySource::FromClause)
            && self.has_limit
            && !self.limit_window_columns.is_empty())
        // Cached path: a link column on this table changing can invalidate.
        || !self.predicates.link_columns.is_empty()
        // Uncached path: any multi-table FromClause query invalidates on a row
        // that newly satisfies the join — the partner side may not be cached,
        // so in-place maintenance can't materialize it. Independent of whether
        // the equivalence was recorded as `col = col`, so cross joins and
        // expression/cast equi-joins land here rather than the join-column clause.
        || (matches!(self.source, UpdateQuerySource::FromClause) && !self.is_single_table)
    }
}

/// The rows a subsumption parent holds in full on the relation.
#[derive(Debug, Clone)]
pub struct SubsumerRegion {
    /// As the parent's filters read, for the precise coverage check.
    pub filters: Vec<TableConstraint>,
    /// In their key forms, for the candidate index (PGC-478).
    pub keyed: Vec<TableConstraint>,
}

/// An update query to register, with what the relation's indexes take.
pub struct UpdateQueryRegistration {
    pub update_query: UpdateQuery,
    /// Constraints to index under, in their key forms.
    pub index_constraints: Vec<TableConstraint>,
    /// Its region when it could parent a subsumption but for a LIMIT, which
    /// can change after registration.
    pub subsumer_region: Option<SubsumerRegion>,
}

#[cfg(feature = "proxy")]
impl UpdateQueryRegistration {
    /// `update_query`'s registration, its filters keyed once for `table`'s
    /// column types (PGC-478) and shared by both indexes.
    pub(crate) fn new(
        update_query: UpdateQuery,
        subsumer_shape: bool,
        table: Option<&TableMetadata>,
    ) -> Self {
        let filters = &update_query.predicates.filters;
        let index_constraints = table.map_or_else(
            || filters.clone(),
            |table| table_constraints_keyed(filters, table),
        );
        let subsumer_region = subsumer_shape.then(|| SubsumerRegion {
            keyed: index_constraints.clone(),
            filters: filters.clone(),
        });
        Self {
            update_query,
            index_constraints,
            subsumer_region,
        }
    }
}

/// Collection of update queries for a specific relation.
///
/// `queries` holds every update query of a fingerprint on the relation: one
/// per branch reading it (PGC-506). A fingerprint's group is registered and
/// removed as a whole, through `fingerprint_insert`/`fingerprint_remove`,
/// which keep the indexes and carve-outs in lockstep with it.
#[derive(Debug)]
pub struct UpdateQueries {
    pub relation_oid: Oid,
    queries: FingerprintMap<Vec<UpdateQuery>>,
    /// Typed index over subsumption parents' regions, for sub-linear
    /// candidate lookup (PGC-119). Holds the `subsumer_regions` fingerprints
    /// without a LIMIT.
    subsumption: ConstraintIndex<Fingerprint>,
    /// The region of each fingerprint that could parent a subsumption.
    subsumer_regions: FingerprintMap<SubsumerRegion>,
    /// Per-table constraint index over the *full* update-query population of
    /// this relation — LocalEval and PgEval alike (PGC-292). Probed point-wise
    /// (`eval_candidates_into`) to narrow CDC per-row work: the upsert matcher
    /// (`update_queries_execute_batch`, which filters to LocalEval after the
    /// probe), the memo-eviction pass, and `mv_dirty_mark_removed_row`. Queries
    /// with no/partial extractable constraints land in the unconstrained class
    /// and are returned for every row, so narrowing never drops a true match
    /// (no stale reads).
    eval_index: ConstraintIndex<UpdateQueryKey>,
    /// ADR-045 invalidation carve-outs — queries that can invalidate even when
    /// the changed row is not in the `eval_index` candidate set, so the narrowed
    /// invalidation pass must always include them. `always_check`: Subquery /
    /// OuterJoinOptional sources (invalidate unconditionally). `has_limit_from`:
    /// `has_limit` FromClause queries (a DELETE invalidates all of them; an
    /// UPDATE of a limit predicate column can push a row out of their window).
    /// A fingerprint is in a set when any of its update queries qualifies.
    pub always_check: FingerprintSet,
    pub has_limit_from: FingerprintSet,
    /// Refcounted union of `predicate_columns` over `has_limit_from`. Lets the
    /// UPDATE narrowing decide in O(changed cols) whether a change could move a
    /// row out of some limit window (then it expands to `has_limit_from`).
    limit_predicate_columns: HashMap<EcoString, usize>,
    /// Refcounted union of `order_by_keys` columns over `has_limit_from`. The
    /// row-change SQL projects old-vs-new ordering for exactly these columns
    /// (PGC-334); columns of queries with `order_by_keys == None` are excluded
    /// (those queries never consult direction).
    limit_order_columns: HashMap<EcoString, usize>,
    /// Bumped on every insert/remove. Keys the batched row-change
    /// prepared-statement cache so a registration that changes
    /// `limit_order_columns` (or the column contract generally) can't keep
    /// serving a stale prepared projection list.
    epoch: u64,
    /// Count of update queries with `change_dependent == true`, so
    /// `needs_change_eval` is O(1) on the CDC hot path. `needs_change_eval`
    /// debug-asserts it against a recompute so a missed account call can't
    /// silently desync (a missed increment risks stale reads, a missed
    /// decrement disables the PGC-227 skip).
    change_dependent_count: usize,
}

impl UpdateQueries {
    pub fn new(relation_oid: Oid) -> Self {
        Self {
            relation_oid,
            queries: HashMap::default(),
            subsumption: ConstraintIndex::new(),
            subsumer_regions: HashMap::default(),
            eval_index: ConstraintIndex::new(),
            always_check: FingerprintSet::default(),
            has_limit_from: FingerprintSet::default(),
            limit_predicate_columns: HashMap::new(),
            limit_order_columns: HashMap::new(),
            epoch: 0,
            change_dependent_count: 0,
        }
    }

    /// The update queries `fingerprint` registered on this relation.
    pub fn queries_of(&self, fingerprint: Fingerprint) -> &[UpdateQuery] {
        self.queries.get(&fingerprint).map_or(&[], Vec::as_slice)
    }

    /// Every update query on this relation.
    pub fn iter(&self) -> impl Iterator<Item = &UpdateQuery> {
        self.queries.values().flatten()
    }

    /// The fingerprints with update queries on this relation.
    pub fn fingerprints(&self) -> impl Iterator<Item = Fingerprint> + '_ {
        self.queries.keys().copied()
    }

    pub fn fingerprint_count(&self) -> usize {
        self.queries.len()
    }

    pub fn update_query_count(&self) -> usize {
        self.queries.values().map(Vec::len).sum()
    }

    /// The fingerprints whose update queries a row with `col_forms_fn`'s
    /// values could satisfy, into `out` (cleared first).
    #[cfg(feature = "proxy")]
    pub(crate) fn eval_candidates_into<F>(&self, col_forms_fn: F, out: &mut FingerprintSet)
    where
        F: Fn(&str) -> ColumnForms,
    {
        self.eval_index
            .candidates_point_into(col_forms_fn, out, |key| key.fingerprint);
    }

    /// The columns the eval index probes a row by.
    pub fn eval_index_columns(&self) -> impl Iterator<Item = &EcoString> {
        self.eval_index.columns()
    }

    /// Subsumption parents whose constraints might cover `keyed`, the new
    /// query's constraints in their key forms.
    pub fn subsumption_candidates(&self, keyed: &[TableConstraint]) -> FingerprintSet {
        self.subsumption.candidates(keyed).into_iter().collect()
    }

    /// The region `fingerprint` holds in full, when it can parent a
    /// subsumption on this relation.
    pub fn subsumer_region(&self, fingerprint: Fingerprint) -> Option<&SubsumerRegion> {
        self.subsumer_regions.get(&fingerprint)
    }

    /// Carve-out membership for a query: `(always_check, has_limit_fromclause)`.
    fn carveout_membership(q: &UpdateQuery) -> (bool, bool) {
        let always = matches!(
            q.source,
            UpdateQuerySource::Subquery(_) | UpdateQuerySource::OuterJoinOptional
        );
        let limit_from = q.has_limit && matches!(q.source, UpdateQuerySource::FromClause);
        (always, limit_from)
    }

    /// Add (`added`) or remove a fingerprint's carve-out memberships, keeping
    /// `always_check` / `has_limit_from` / `limit_predicate_columns` /
    /// `limit_order_columns` in lockstep with `queries`. The sets hold the
    /// fingerprint when any of `entries` qualifies; the column unions are
    /// refcounted per entry, so several limit queries sharing a column are
    /// handled correctly.
    fn carveout_account(&mut self, fp: Fingerprint, entries: &[UpdateQuery], added: bool) {
        for q in entries {
            let (always, limit_from) = Self::carveout_membership(q);
            if always && added {
                self.always_check.insert(fp);
            }
            if limit_from {
                if added {
                    self.has_limit_from.insert(fp);
                }
                Self::column_refcounts_account(
                    &mut self.limit_predicate_columns,
                    q.predicate_columns.iter(),
                    added,
                );
                Self::column_refcounts_account(
                    &mut self.limit_order_columns,
                    q.order_by_keys.iter().flatten().map(|k| &k.column),
                    added,
                );
            }
        }
        if !added {
            self.always_check.remove(&fp);
            self.has_limit_from.remove(&fp);
        }
    }

    /// Increment (`added`) or decrement a refcounted column union, dropping
    /// zero-count entries.
    fn column_refcounts_account<'a>(
        counts: &mut HashMap<EcoString, usize>,
        columns: impl Iterator<Item = &'a EcoString>,
        added: bool,
    ) {
        for c in columns {
            if added {
                *counts.entry(c.clone()).or_insert(0) += 1;
            } else if let Some(n) = counts.get_mut(c) {
                *n = n.saturating_sub(1);
                if *n == 0 {
                    counts.remove(c);
                }
            }
        }
    }

    /// Columns the row-change SQL must project old-vs-new ordering for: the
    /// ORDER BY key columns of every `has_limit` FromClause query with a usable
    /// key spec (PGC-334).
    pub fn limit_order_columns(&self) -> impl Iterator<Item = &EcoString> {
        self.limit_order_columns.keys()
    }

    /// Registration epoch — bumped on every insert/remove. Keys caches whose
    /// contents depend on this relation's query population (e.g. the batched
    /// row-change prepared statement).
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Whether any column the UPDATE changed is a predicate column of some
    /// `has_limit` FromClause query — i.e. the change could push a row out of a
    /// limit window, so the invalidation pass must expand to `has_limit_from`.
    pub fn limit_predicate_changed(&self, row_changes: &RowChanges) -> bool {
        row_changes
            .iter()
            .any(|(c, cc)| cc.changed && self.limit_predicate_columns.contains_key(c))
    }

    /// Whether any query over this relation needs `query_row_changes` to decide
    /// a CDC UPDATE's invalidation. When false, `handle_update` skips the
    /// SELECT and the invalidation check entirely (PGC-227).
    pub fn needs_change_eval(&self) -> bool {
        debug_assert_eq!(
            self.change_dependent_count,
            self.iter().filter(|q| q.change_dependent).count(),
            "change_dependent_count desynced from queries — insert/remove must go \
             through fingerprint_insert/fingerprint_remove"
        );
        self.change_dependent_count > 0
    }

    /// Register `fingerprint`'s update queries on this relation, replacing
    /// any it had: one entry per table reference.
    pub fn fingerprint_insert(
        &mut self,
        fingerprint: Fingerprint,
        registrations: Vec<UpdateQueryRegistration>,
    ) {
        self.fingerprint_remove(fingerprint);
        self.epoch += 1;
        let mut entries: Vec<UpdateQuery> = Vec::with_capacity(registrations.len());
        let mut subsumer = None;
        for registration in registrations {
            if subsumer.is_none() {
                subsumer = registration.subsumer_region;
            }
            let mut update_query = registration.update_query;
            update_query.fingerprint = fingerprint;
            update_query.ordinal = entries.len();
            self.eval_index
                .insert(update_query.key(), &registration.index_constraints);
            self.change_dependent_account(update_query.change_dependent, true);
            entries.push(update_query);
        }
        if let Some(region) = subsumer {
            if entries.iter().all(|entry| !entry.has_limit) {
                self.subsumption.insert(fingerprint, &region.keyed);
            }
            self.subsumer_regions.insert(fingerprint, region);
        }
        self.carveout_account(fingerprint, &entries, true);
        self.queries.insert(fingerprint, entries);
    }

    /// Remove every update query of `fingerprint` on this relation, with its
    /// index entries. Returns whether it had any.
    pub fn fingerprint_remove(&mut self, fingerprint: Fingerprint) -> bool {
        self.subsumption.remove(fingerprint);
        self.subsumer_regions.remove(&fingerprint);
        let Some(entries) = self.queries.remove(&fingerprint) else {
            return false;
        };
        self.epoch += 1;
        for entry in &entries {
            self.eval_index.remove(entry.key());
            self.change_dependent_account(entry.change_dependent, false);
        }
        self.carveout_account(fingerprint, &entries, false);
        true
    }

    /// Set whether `fingerprint`'s parent query has a LIMIT, re-deriving what
    /// depends on it: the carve-outs, `change_dependent`, and its place in
    /// the subsumption index, which holds only unlimited parents.
    pub fn has_limit_set(&mut self, fingerprint: Fingerprint, has_limit: bool) {
        let Some(mut entries) = self.queries.remove(&fingerprint) else {
            return;
        };
        self.epoch += 1;
        self.carveout_account(fingerprint, &entries, false);
        for entry in &mut entries {
            self.change_dependent_account(entry.change_dependent, false);
            entry.has_limit = has_limit;
            entry.change_dependent = entry.update_invalidation_possible();
            self.change_dependent_account(entry.change_dependent, true);
        }
        self.carveout_account(fingerprint, &entries, true);
        self.queries.insert(fingerprint, entries);
        self.subsumption.remove(fingerprint);
        if !has_limit && let Some(region) = self.subsumer_regions.get(&fingerprint) {
            self.subsumption.insert(fingerprint, &region.keyed);
        }
    }

    /// Account for a query being added to / removed from the set. `added`
    /// increments on insert, decrements on remove.
    fn change_dependent_account(&mut self, change_dependent: bool, added: bool) {
        if !change_dependent {
            return;
        }
        if added {
            self.change_dependent_count += 1;
        } else {
            self.change_dependent_count = self.change_dependent_count.saturating_sub(1);
        }
    }
}

impl IdHashItem for UpdateQueries {
    type Key<'a> = Oid;

    fn key(&self) -> Self::Key<'_> {
        self.relation_oid
    }

    id_upcast!();
}

#[cfg(test)]
mod tests {
    use std::hash::BuildHasher;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::cache::admission::{AdmissionDepth, AdmissionQuery, query_admission_analyze};
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::id_hash::BuildIdHasher;
    use crate::oid::TypeOid;
    use crate::pg::protocol::ByteString;
    use crate::query::constraint_index::row_value_forms;

    const FINGERPRINT: Fingerprint = Fingerprint::from_raw(0x5eed);
    const ITEMS_OID: u32 = 1001;

    fn column(name: &str, position: i16, data_type: Type, type_name: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(data_type.oid()),
            data_type,
            type_name: type_name.into(),
            cache_type_name: type_name.into(),
            is_primary_key: position == 1,
        }
    }

    fn items() -> TableMetadata {
        TableMetadata::for_test(
            "items",
            ITEMS_OID,
            [
                column("id", 1, Type::INT4, "int4"),
                column("kind", 2, Type::TEXT, "text"),
            ],
        )
    }

    fn catalog() -> Catalog {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(items().into());
        tables
    }

    /// The registrations of `sql` on `items`, built as the writer builds them.
    fn registrations(sql: &str, has_limit: bool) -> Vec<UpdateQueryRegistration> {
        let tables = catalog();
        let bound =
            Arc::new(ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve"));
        let analysis = query_admission_analyze(
            AdmissionQuery {
                bound: &bound,
                decorrelated: None,
                fingerprint: FINGERPRINT,
                has_limit,
            },
            &HashSet::new(),
            &tables,
            AdmissionDepth::Full,
        )
        .expect("analyze");
        analysis
            .tables
            .into_iter()
            .map(|admission| {
                UpdateQueryRegistration::new(
                    admission.update_query,
                    admission.subsumer_shape,
                    Some(&items()),
                )
            })
            .collect()
    }

    fn registered(sql: &str, has_limit: bool) -> UpdateQueries {
        let mut queries = UpdateQueries::new(Oid::from_raw(ITEMS_OID));
        queries.fingerprint_insert(FINGERPRINT, registrations(sql, has_limit));
        queries
    }

    /// Whether the eval index offers the fingerprint for an `items` row of `kind`.
    fn row_candidate(queries: &UpdateQueries, kind: &str) -> bool {
        let table = items();
        let row: Vec<Option<ByteString>> = vec![Some("1".into()), Some(kind.into())];
        let mut candidates = FingerprintSet::default();
        queries.eval_candidates_into(|c| row_value_forms(&table, &row, c), &mut candidates);
        candidates.contains(&FINGERPRINT)
    }

    fn subsumer(queries: &UpdateQueries) -> bool {
        queries.subsumption_candidates(&[]).contains(&FINGERPRINT)
            || queries
                .subsumption_candidates(&[TableConstraint {
                    column: "kind".into(),
                    kind: crate::query::constraints::ColumnConstraintKind::Comparison {
                        op: crate::query::ast::BinaryOp::Equal,
                        value: crate::query::ast::LiteralValue::String("x".into()),
                    },
                }])
                .contains(&FINGERPRINT)
    }

    const UNION: &str = "SELECT id FROM items WHERE kind = 'x' \
                         UNION SELECT id FROM items WHERE kind = 'y'";

    /// Each branch reading the relation keeps its own update query, and a
    /// row matching either branch reaches the fingerprint (PGC-506).
    #[test]
    fn test_update_queries_branch_each_registered() {
        let queries = registered(UNION, false);
        assert_eq!(queries.queries_of(FINGERPRINT).len(), 2);
        assert_eq!(queries.fingerprint_count(), 1);
        assert!(row_candidate(&queries, "x"));
        assert!(row_candidate(&queries, "y"));
        assert!(!row_candidate(&queries, "z"));
    }

    /// Each reference of a self-join is its own entry with its own filters,
    /// so a row matching either reference's filter is a candidate and one
    /// matching neither is not.
    #[test]
    fn test_update_queries_self_join_entry_per_reference() {
        let queries = registered(
            "SELECT a.id FROM items a JOIN items b ON b.id = a.id \
             WHERE a.kind = 'x' AND b.kind = 'y'",
            false,
        );
        assert_eq!(queries.queries_of(FINGERPRINT).len(), 2);
        assert!(row_candidate(&queries, "x"));
        assert!(row_candidate(&queries, "y"));
        assert!(!row_candidate(&queries, "z"));
    }

    /// Registering a fingerprint again replaces its whole group.
    #[test]
    fn test_update_queries_insert_replaces_group() {
        let mut queries = registered(UNION, false);
        queries.fingerprint_insert(
            FINGERPRINT,
            registrations("SELECT id FROM items WHERE kind = 'x'", false),
        );
        assert_eq!(queries.update_query_count(), 1);
        assert!(row_candidate(&queries, "x"));
        assert!(!row_candidate(&queries, "y"));
    }

    /// Removing a fingerprint clears its group, its index entries and its
    /// carve-outs.
    #[test]
    fn test_update_queries_remove_clears_indexes() {
        let mut queries = registered(
            "SELECT id FROM items WHERE kind = 'x' \
             AND id IN (SELECT id FROM items WHERE kind = 'y')",
            false,
        );
        assert_eq!(queries.queries_of(FINGERPRINT).len(), 2);
        assert!(queries.always_check.contains(&FINGERPRINT));
        assert!(queries.fingerprint_remove(FINGERPRINT));
        assert_eq!(queries.update_query_count(), 0);
        assert!(queries.always_check.is_empty());
        assert!(!row_candidate(&queries, "x"));
        assert!(!row_candidate(&queries, "y"));
        assert!(!queries.fingerprint_remove(FINGERPRINT));
    }

    /// A LIMIT keeps a parent out of the subsumption index; dropping it
    /// re-adds the parent and its carve-outs follow.
    #[test]
    fn test_update_queries_has_limit_set_reindexes() {
        let mut queries = registered("SELECT id FROM items WHERE kind = 'x'", true);
        assert!(queries.has_limit_from.contains(&FINGERPRINT));
        assert!(!subsumer(&queries));

        queries.has_limit_set(FINGERPRINT, false);
        assert!(queries.has_limit_from.is_empty());
        assert!(subsumer(&queries));

        queries.has_limit_set(FINGERPRINT, true);
        assert!(queries.has_limit_from.contains(&FINGERPRINT));
        assert!(!subsumer(&queries));
    }

    /// A fingerprint with several update queries on the relation never
    /// parents a subsumption, with or without a LIMIT.
    #[test]
    fn test_update_queries_several_never_subsumer() {
        let mut queries = registered(UNION, false);
        assert!(!subsumer(&queries));
        queries.has_limit_set(FINGERPRINT, true);
        queries.has_limit_set(FINGERPRINT, false);
        assert!(!subsumer(&queries));
    }

    /// Update queries of one fingerprint hash apart; the first hashes as its
    /// fingerprint.
    #[test]
    fn test_update_query_key_hash() {
        let hasher = BuildIdHasher::<UpdateQueryKey>::default();
        let key = |ordinal| UpdateQueryKey {
            fingerprint: FINGERPRINT,
            ordinal,
        };
        assert_eq!(hasher.hash_one(key(0)), FINGERPRINT.get());
        assert_ne!(hasher.hash_one(key(0)), hasher.hash_one(key(1)));
    }
}
