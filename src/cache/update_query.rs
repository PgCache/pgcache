use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use iddqd::{IdHashItem, id_upcast};

use crate::catalog::Oid;
use crate::query::constraint_index::ConstraintIndex;
use crate::query::constraints::QueryConstraints;
use crate::query::predicate::CompiledPredicate;
use crate::query::resolved::ResolvedQueryExpr;
use crate::query::{Fingerprint, FingerprintMap, FingerprintSet};

/// The kind of subquery context a table was found in.
/// Determines invalidation behavior based on whether the subquery's
/// result set growing or shrinking affects the outer query.
#[derive(Debug, Clone, Copy, PartialEq)]
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

/// Query used to update cached results when data changes
#[derive(Debug, Clone)]
pub struct UpdateQuery {
    /// Fingerprint of cached query that generated this update query
    pub fingerprint: Fingerprint,
    /// Resolved AST query
    pub resolved: ResolvedQueryExpr,
    /// Whether this table was found directly in FROM or inside a subquery
    pub source: UpdateQuerySource,
    /// WHERE clause constraints for CDC invalidation filtering
    pub constraints: QueryConstraints,
    /// Whether the parent cached query has a LIMIT (max_limit.is_some()).
    /// Used by CDC to determine if DELETE should trigger invalidation.
    pub has_limit: bool,
    /// Eval strategy for this query's WHERE clause during CDC row matching.
    pub eval_strategy: UpdateEvalStrategy,
    /// Columns from ORDER BY / WHERE / HAVING that define the LIMIT window.
    /// Populated only when `has_limit`; consumed by row_cached_invalidation_check
    /// to detect updates that may demote a cached row out of the window (PGC-94).
    pub limit_window_columns: HashSet<EcoString>,
    /// Whether a CDC UPDATE for this query can ever invalidate. When `false`,
    /// `handle_update` skips the `query_row_changes` SELECT and the invalidation
    /// check entirely (PGC-227). Set at registration from
    /// `update_invalidation_possible` (the single source of truth — needs the
    /// relation's table name).
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
    /// Precomputed `resolved.is_single_table()`. Cached because the CDC
    /// invalidation checks consult it per row per query, and the underlying
    /// `ResolvedQueryExpr::is_single_table` walks the AST.
    pub is_single_table: bool,
    /// Pre-canonicalized WHERE clause for the LocalEval membership probe
    /// (`update_query_matches_locally`), built once at registration so the per
    /// row CDC path skips re-destructuring the resolved AST (PGC-339). `None`
    /// means either no WHERE clause (unconditional match) or a non-LocalEval
    /// query (never consulted — `eval_strategy != LocalEval`).
    pub compiled_where: Option<CompiledPredicate>,
}

impl UpdateQuery {
    /// Whether a CDC UPDATE (`operation = Upsert`) for this query could ever
    /// invalidate it: the static upper bound of `row_cached_invalidation_check`
    /// ∪ `row_uncached_invalidation_check` over all possible row values. This is
    /// the single source of truth for `change_dependent` (PGC-227); each clause
    /// mirrors a check branch and the two must stay in lockstep — the
    /// `debug_assert` in `update_queries_check_invalidate` enforces it at
    /// runtime by failing if a check ever invalidates while this returns false.
    pub fn update_invalidation_possible(&self, table_name: &str) -> bool {
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
        // Cached path: a join column on this table changing can invalidate.
        || self.constraints.table_join_columns(table_name).next().is_some()
        // Uncached path: any multi-table FromClause query invalidates on a row
        // that newly satisfies the join — the partner side may not be cached,
        // so in-place maintenance can't materialize it. Independent of whether
        // the equivalence was recorded as `col = col`, so cross joins and
        // expression/cast equi-joins land here rather than the join-column clause.
        || (matches!(self.source, UpdateQuerySource::FromClause) && !self.is_single_table)
    }
}

/// Collection of update queries for a specific relation.
///
/// `queries` is keyed by fingerprint for O(1) lookup. `subsumption` is a typed
/// index over the queries' WHERE-clause constraints used by
/// `subsumption_check` for sub-linear candidate lookup (see PGC-119).
#[derive(Debug)]
pub struct UpdateQueries {
    pub relation_oid: Oid,
    pub queries: FingerprintMap<UpdateQuery>,
    pub subsumption: ConstraintIndex<Fingerprint>,
    /// Per-table constraint index over the *full* update-query population of
    /// this relation — LocalEval and PgEval alike (PGC-292). Probed point-wise
    /// (`candidates_point`) to narrow CDC per-row work: the upsert matcher
    /// (`update_queries_execute_batch`, which filters to LocalEval after the
    /// probe), the memo-eviction pass, and `mv_dirty_mark_removed_row`. Queries
    /// with no/partial extractable constraints land in the unconstrained class
    /// and are returned for every row, so narrowing never drops a true match
    /// (no stale reads).
    pub eval_index: ConstraintIndex<Fingerprint>,
    /// ADR-045 invalidation carve-outs — queries that can invalidate even when
    /// the changed row is not in the `eval_index` candidate set, so the narrowed
    /// invalidation pass must always include them. `always_check`: Subquery /
    /// OuterJoinOptional sources (invalidate unconditionally). `has_limit_from`:
    /// `has_limit` FromClause queries (a DELETE invalidates all of them; an
    /// UPDATE of a limit predicate column can push a row out of their window).
    /// Both maintained on `query_insert`/`query_remove`.
    pub always_check: FingerprintSet,
    pub has_limit_from: FingerprintSet,
    /// Refcounted union of `predicate_columns` over `has_limit_from`. Lets the
    /// UPDATE narrowing decide in O(changed cols) whether a change could move a
    /// row out of some limit window (then it expands to `has_limit_from`).
    limit_predicate_columns: HashMap<EcoString, usize>,
    /// Count of queries with `change_dependent == true`. Maintained on
    /// insert/remove via `change_dependent_account` so `needs_change_eval` is
    /// O(1) on the CDC hot path. Derivable from `queries`; `needs_change_eval`
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
            eval_index: ConstraintIndex::new(),
            always_check: FingerprintSet::default(),
            has_limit_from: FingerprintSet::default(),
            limit_predicate_columns: HashMap::new(),
            change_dependent_count: 0,
        }
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

    /// Add (`added`) or remove a query's carve-out memberships, keeping
    /// `always_check` / `has_limit_from` / `limit_predicate_columns` in lockstep
    /// with `queries`. Idempotent per fingerprint for the sets; refcounted for
    /// the column union so replacement (same fingerprint, changed shape) and
    /// multiple limit queries sharing a predicate column are handled correctly.
    fn carveout_account(
        &mut self,
        fp: Fingerprint,
        always: bool,
        limit_from: bool,
        predicate_columns: &HashSet<EcoString>,
        added: bool,
    ) {
        if always {
            if added {
                self.always_check.insert(fp);
            } else {
                self.always_check.remove(&fp);
            }
        }
        if limit_from {
            if added {
                self.has_limit_from.insert(fp);
                for c in predicate_columns {
                    *self.limit_predicate_columns.entry(c.clone()).or_insert(0) += 1;
                }
            } else {
                self.has_limit_from.remove(&fp);
                for c in predicate_columns {
                    if let Some(n) = self.limit_predicate_columns.get_mut(c) {
                        *n -= 1;
                        if *n == 0 {
                            self.limit_predicate_columns.remove(c);
                        }
                    }
                }
            }
        }
    }

    /// Whether any column the UPDATE changed is a predicate column of some
    /// `has_limit` FromClause query — i.e. the change could push a row out of a
    /// limit window, so the invalidation pass must expand to `has_limit_from`.
    pub fn limit_predicate_changed(&self, row_changes: &HashMap<EcoString, bool>) -> bool {
        row_changes
            .iter()
            .any(|(c, &changed)| changed && self.limit_predicate_columns.contains_key(c))
    }

    /// Whether any query over this relation needs `query_row_changes` to decide
    /// a CDC UPDATE's invalidation. When false, `handle_update` skips the
    /// SELECT and the invalidation check entirely (PGC-227).
    pub fn needs_change_eval(&self) -> bool {
        debug_assert_eq!(
            self.change_dependent_count,
            self.queries.values().filter(|q| q.change_dependent).count(),
            "change_dependent_count desynced from queries — insert/remove must go \
             through query_insert/query_remove"
        );
        self.change_dependent_count > 0
    }

    /// Insert (or replace) an update query, keeping `change_dependent_count` in
    /// sync with `queries`. Returns the replaced entry, if any. Replacement is
    /// real: the same fingerprint can be registered more than once for a
    /// relation (e.g. a self-correlated subquery decorrelates into multiple
    /// update queries over the same table), and the map is keyed by fingerprint.
    pub fn query_insert(&mut self, query: UpdateQuery) -> Option<UpdateQuery> {
        let fp = query.fingerprint;
        let change_dependent = query.change_dependent;
        let (new_always, new_limit_from) = Self::carveout_membership(&query);
        let new_pcols = query.predicate_columns.clone();
        let prev = self.queries.insert(fp, query);
        if let Some(prev) = &prev {
            self.change_dependent_account(prev.change_dependent, false);
            let (pa, pl) = Self::carveout_membership(prev);
            self.carveout_account(fp, pa, pl, &prev.predicate_columns, false);
        }
        self.change_dependent_account(change_dependent, true);
        self.carveout_account(fp, new_always, new_limit_from, &new_pcols, true);
        prev
    }

    /// Remove an update query by fingerprint, keeping `change_dependent_count`
    /// in sync with `queries`. Returns the removed entry, if any.
    pub fn query_remove(&mut self, fingerprint: Fingerprint) -> Option<UpdateQuery> {
        let removed = self.queries.remove(&fingerprint);
        if let Some(removed) = &removed {
            self.change_dependent_account(removed.change_dependent, false);
            let (a, l) = Self::carveout_membership(removed);
            self.carveout_account(fingerprint, a, l, &removed.predicate_columns, false);
        }
        removed
    }

    /// Account for a query being added to / removed from the set. `added`
    /// increments on insert, decrements on remove. Private: callers mutate
    /// `queries` only via `query_insert`/`query_remove`, which keep the count
    /// in lockstep.
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
