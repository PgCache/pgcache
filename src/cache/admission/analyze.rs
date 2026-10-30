//! The per-table admission derivation, extracted verbatim from the writer's
//! registration path (PGC-391) so pgcache-fit runs the identical analysis.

use std::collections::{HashMap, HashSet};
use std::ptr;
use std::sync::Arc;

use ecow::EcoString;

use super::update_classify::{
    limit_order_keys_collect, limit_window_columns_collect, pg_batchable_classify,
    predicate_columns_collect, update_eval_strategy_classify,
};
use super::{AdmissionAnalysis, TableAdmission};
use crate::cache::mv_shape::select_computes;
#[cfg(feature = "proxy")]
use crate::cache::mv_shape::{ShapeGate, shape_classify};
use crate::cache::query::{limit_clause_of, limit_rows_needed};
use crate::cache::update_query::{OrderByKey, UpdateEvalStrategy, UpdateQuery, UpdateQuerySource};
use crate::cache::{CacheError, CacheResult};
use crate::catalog::{Catalog, ColumnMask};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::rope::{Body, NodeKind, QueryTree, TreeBuilder, View};
use crate::query::compiled_predicate::CompiledPredicate;
use crate::query::decorrelate::query_decorrelate;
use crate::query::join_region::JoinRegion;
use crate::query::literal_settings::LiteralSettings;
use crate::query::resolve::ResolveResult;
use crate::query::resolve::{QueryBranch, ResolvedQuery, query_tree_resolve};
use crate::query::transform::{
    LiteralCanonOutcome, PgEvalTemplate, query_literals_canonicalise, query_predicate_pushdown,
};
use crate::query::update::{TableUpdateQuery, query_table_update_queries};
use crate::result::ReportExt;

/// How much of each [`TableAdmission`] to build. The writer needs the full
/// [`UpdateQuery`] including its CDC-eval caches; offline analysis
/// (pgcache-fit) keeps only the decision fields, so the caches — a per-table
/// deparse among them — are skipped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AdmissionDepth {
    Full,
    DecisionOnly,
}

/// The query without its root LIMIT, and max_limit for population. Set
/// operations force max_limit = None since population runs per-branch.
pub fn base_query_prepare(query: &QueryTree) -> (QueryTree, Option<u64>) {
    let root = query.root();
    let max_limit = match root.body() {
        Body::SetOp(_) => None,
        Body::Select(_) | Body::Values(_) => limit_rows_needed(&limit_clause_of(root)),
    };
    (query_tree_without_limit(query), max_limit)
}

/// The population cap: the root's LIMIT plus OFFSET when the population form
/// (`decorrelated` when there is one) is a plain projection whose ORDER BY
/// the capped read can render. A shape that computes its result gets none,
/// whether or not its ORDER BY can be served: its rows map to no bounded set
/// of source rows (ADR-060).
pub fn population_max_limit(
    bound: &ResolvedQuery,
    decorrelated: Option<&ResolvedQuery>,
    aggregate_functions: &HashSet<EcoString>,
    user_max_limit: Option<u64>,
) -> Option<u64> {
    let population = decorrelated.unwrap_or(bound);
    population
        .select()
        .filter(|select| {
            !select_computes(*select, aggregate_functions)
                && population
                    .tree()
                    .root()
                    .order_by()
                    .all(|clause| population.order_by_source(*select, clause).is_some())
        })
        .and(user_max_limit)
}

/// `query` with its root LIMIT/OFFSET dropped: the root re-emitted without
/// the limit children, everything under it kept. The tree itself when it
/// has none.
fn query_tree_without_limit(query: &QueryTree) -> QueryTree {
    let root = query.root();
    let NodeKind::Query {
        cte_count,
        order_by_count,
        ..
    } = *root.node_ref().kind()
    else {
        return query.clone();
    };
    if !root.has_limit() {
        return query.clone();
    }
    let mut builder = TreeBuilder::over(query);
    let open = builder.open_from(
        NodeKind::Query {
            cte_count,
            order_by_count,
            has_limit_count: false,
            has_limit_offset: false,
        },
        root.id(),
    );
    for cte in root.ctes() {
        builder.keep(cte.id());
    }
    builder.keep(root.body().id());
    for clause in root.order_by() {
        builder.keep(clause.id());
    }
    builder.close(open);
    builder.finish().0
}

/// The identity-bearing form of a query.
pub struct CanonicalResolution {
    /// The canonical plain tree with its bindings (ADR-057).
    pub bound: Arc<ResolvedQuery>,
    pub literals: LiteralCanonOutcome,
}

/// Resolve `base_query` on `search_path` and canonicalise its date-time
/// literals under `settings`.
pub fn query_resolve_canonical(
    base_query: QueryTree,
    tables: &Catalog,
    search_path: &[&str],
    settings: &LiteralSettings,
) -> ResolveResult<CanonicalResolution> {
    let bindings = query_tree_resolve(&base_query, tables, search_path)?;
    let (tree, bindings, literals) = query_literals_canonicalise(base_query, bindings, settings);
    Ok(CanonicalResolution {
        bound: Arc::new(ResolvedQuery::new(tree, bindings)),
        literals,
    })
}

/// Predicate pushdown (ADR-012) on the bound query; the query comes back as
/// it was when nothing moves.
pub fn query_pushdown(bound: Arc<ResolvedQuery>) -> Arc<ResolvedQuery> {
    match query_predicate_pushdown(&bound) {
        Some(pushed) => Arc::new(pushed),
        None => bound,
    }
}

/// Decorrelation (ADR-014 to ADR-017) on the bound query; `None` when there
/// is nothing to decorrelate. Runs once at registration for the shape gate,
/// admission and population alike.
pub fn query_decorrelate_checked(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
) -> CacheResult<Option<ResolvedQuery>> {
    query_decorrelate(bound, aggregate_functions)
        .map_err(|e| e.context_transform(CacheError::from))
        .attach_loc("decorrelating correlated subqueries")
}

/// Classify a query's shape for MV eligibility on its decorrelated form
/// when it has one, so the shape classified is the shape population and
/// rebuild see.
#[cfg(feature = "proxy")]
pub(crate) fn shape_gate_classify(
    bound: &ResolvedQuery,
    decorrelated: Option<&ResolvedQuery>,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    shape_classify(decorrelated.unwrap_or(bound), aggregate_functions)
}

/// The query as admission reads it: as registered, and decorrelated when
/// it had correlated subqueries, with its identity and whether the
/// registered form carried a LIMIT (stripped before binding).
pub struct AdmissionQuery<'a> {
    pub bound: &'a Arc<ResolvedQuery>,
    pub decorrelated: Option<&'a Arc<ResolvedQuery>>,
    pub fingerprint: Fingerprint,
    pub has_limit: bool,
    /// `bound`'s join region, built once by the caller for the subsumption
    /// check and the admission alike.
    pub join_region: Option<&'a JoinRegion>,
}

/// Derive one [`TableAdmission`] per update query of the query, or of its
/// decorrelated form when it had correlated subqueries: the built
/// [`UpdateQuery`], its constraints, and the subsumer-eligibility gates.
/// Pure — the caller stores (writer) or simulates (fit) the results.
pub fn query_admission_analyze(
    query: AdmissionQuery<'_>,
    aggregate_functions: &HashSet<EcoString>,
    tables: &Catalog,
    depth: AdmissionDepth,
) -> CacheResult<AdmissionAnalysis> {
    let update_source = query.decorrelated.unwrap_or(query.bound);
    let update_queries = query_table_update_queries(update_source);
    let read_columns =
        (depth == AdmissionDepth::Full).then(|| read_columns_collect(query.bound, update_source));
    let context = AdmissionContext {
        query: &query,
        update_source,
        aggregate_functions,
        tables,
        depth,
        read_columns,
    };
    let admissions = update_queries
        .into_iter()
        .map(|bound_update| context.table_admission_build(bound_update))
        .collect();
    // A join with a region is one SELECT with no subquery, so nothing was
    // decorrelated and the registered form is the one population loads.
    let join_region = query.join_region.filter(|region| region.exact).cloned();
    Ok(AdmissionAnalysis {
        transformed: query.decorrelated.is_some(),
        tables: admissions,
        join_region,
    })
}

/// The columns each relation is read through, in the registered form and,
/// when it differs, the decorrelated form the update queries come from.
fn read_columns_collect(
    bound: &ResolvedQuery,
    update_source: &ResolvedQuery,
) -> HashMap<Oid, HashSet<EcoString>> {
    let mut columns = bound.privilege_needs().relations;
    if !ptr::eq(bound, update_source) {
        for (relation_oid, names) in update_source.privilege_needs().relations {
            columns.entry(relation_oid).or_default().extend(names);
        }
    }
    columns
}

/// What every table's admission is derived against.
struct AdmissionContext<'a> {
    query: &'a AdmissionQuery<'a>,
    /// The query the update queries are read from: the decorrelated form
    /// when there is one.
    update_source: &'a Arc<ResolvedQuery>,
    aggregate_functions: &'a HashSet<EcoString>,
    tables: &'a Catalog,
    depth: AdmissionDepth,
    /// The read columns per relation (`UpdateQuery::read_columns`); only a
    /// full admission derives them.
    read_columns: Option<HashMap<Oid, HashSet<EcoString>>>,
}

impl AdmissionContext<'_> {
    fn full(&self) -> bool {
        self.depth == AdmissionDepth::Full
    }

    /// The relation's read columns as a mask over its table, `None` when
    /// not derived or when a name is not a column of the table.
    fn read_columns(&self, relation_oid: Oid) -> Option<ColumnMask> {
        let names = self.read_columns.as_ref()?.get(&relation_oid)?;
        let table = self.tables.get1(&relation_oid)?;
        ColumnMask::columns_named(table, names.iter().map(EcoString::as_str))
    }

    fn table_admission_build(&self, bound_update: TableUpdateQuery<'_>) -> TableAdmission {
        let table_node = bound_update.table;
        let source = bound_update.source;
        let relation_oid = bound_update.relation_oid;
        let branch = bound_update.branch;
        let table_name = table_node.name().as_str();
        let eval_strategy = update_eval_strategy_classify(branch, source);
        let is_single_table = branch.is_single_table();
        let has_limit = self.query.has_limit;
        let mut update_query = UpdateQuery {
            fingerprint: self.query.fingerprint,
            ordinal: 0,
            bound: Arc::clone(self.update_source),
            branch: branch.select.id(),
            source,
            table: table_node.id(),
            predicates: bound_update.predicates,
            has_limit,
            eval_strategy,
            limit_window_columns: self.limit_window_columns(table_name),
            order_by_keys: self.order_by_keys(table_name, is_single_table, source),
            change_dependent: false,
            pg_batchable: self.pg_batchable(branch, eval_strategy),
            predicate_columns: predicate_columns_collect(branch, table_name),
            read_columns: self.read_columns(relation_oid),
            is_single_table,
            compiled_where: self.compiled_where(branch, table_name, eval_strategy),
            pg_eval_template: self.pg_eval_template(branch, relation_oid, eval_strategy),
        };
        // Whether a CDC UPDATE for this query can ever invalidate — i.e.
        // whether `handle_update` must run `query_row_changes` + the
        // invalidation check rather than skip them (PGC-227). Derived from
        // the single source of truth so the flag can't drift.
        if self.full() {
            update_query.change_dependent = update_query.update_invalidation_possible();
        }
        // Population loads each exact reference's region in full whatever
        // the relation's other references load, so each parents on its own.
        let subsumer_shape = update_query.predicates.exact;
        TableAdmission {
            relation_oid,
            table_name: table_node.name().clone(),
            update_query,
            subsumer_shape,
            subsumer_eligible: subsumer_shape && !has_limit,
        }
    }

    /// Walks the whole query: the branch has no ORDER BY.
    fn limit_window_columns(&self, table_name: &str) -> HashSet<EcoString> {
        if self.query.has_limit {
            limit_window_columns_collect(self.query.bound, table_name)
        } else {
            HashSet::new()
        }
    }

    /// Direction-aware window spec (PGC-334) — the shapes the cached-path
    /// window check can reason about. Walks the parent query like
    /// `limit_window_columns` (the branch has no ORDER BY).
    fn order_by_keys(
        &self,
        table_name: &str,
        is_single_table: bool,
        source: UpdateQuerySource,
    ) -> Option<Box<[OrderByKey]>> {
        let windowed =
            self.query.has_limit && is_single_table && source == UpdateQuerySource::FromClause;
        windowed
            .then(|| limit_order_keys_collect(self.query.bound, table_name))
            .flatten()
    }

    fn pg_batchable(&self, branch: QueryBranch<'_>, eval_strategy: UpdateEvalStrategy) -> bool {
        self.full()
            && eval_strategy == UpdateEvalStrategy::PgEval
            && pg_batchable_classify(branch, self.aggregate_functions)
    }

    /// Compile the LocalEval WHERE once so the per-row CDC membership probe
    /// doesn't re-destructure the tree (PGC-339). PgEval queries never
    /// consult this, so skip building it.
    fn compiled_where(
        &self,
        branch: QueryBranch<'_>,
        table_name: &str,
        eval_strategy: UpdateEvalStrategy,
    ) -> Option<CompiledPredicate> {
        if !self.full() || eval_strategy != UpdateEvalStrategy::LocalEval {
            return None;
        }
        branch
            .select
            .where_clause()
            .map(|w| CompiledPredicate::compile(self.update_source, w, table_name))
    }

    /// Precompute the PgEval membership template so the per-row check
    /// skips the per-row deparse (PGC-343). Only PgEval uses it; needs the
    /// relation's TableMetadata.
    fn pg_eval_template(
        &self,
        branch: QueryBranch<'_>,
        relation_oid: Oid,
        eval_strategy: UpdateEvalStrategy,
    ) -> Option<PgEvalTemplate> {
        if !self.full() || eval_strategy != UpdateEvalStrategy::PgEval {
            return None;
        }
        let table_metadata = self.tables.get1(&relation_oid)?;
        PgEvalTemplate::build(branch, table_metadata, branch.select.id())
    }
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::cache::admission::{
        CandidateState, SubsumerCandidate, SubsumerSource, SubsumptionReference,
        subsumption_covered, subsumption_pending_parent, subsumption_references,
    };
    use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::constraints::{TableRanges, table_constraints_subsumed};
    use crate::query::join_region::query_join_region;

    fn catalog() -> Catalog {
        let int_column = |name: &str, position: i16| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: position == 1,
        };
        let mut tables = BiHashMap::new();
        for (oid, name) in (1001..).zip(["a", "b", "c"]) {
            tables.insert_overwrite(
                TableMetadata::for_test(
                    name,
                    oid,
                    [int_column("id", 1), int_column("x", 2), int_column("y", 3)],
                )
                .into(),
            );
        }
        tables
    }

    fn admission_analysis(sql: &str) -> AdmissionAnalysis {
        let tables = catalog();
        let bound =
            Arc::new(ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve"));
        let aggregate_functions = HashSet::from([EcoString::from("count")]);
        let decorrelated = query_decorrelate_checked(&bound, &aggregate_functions)
            .expect("decorrelate")
            .map(Arc::new);
        let join_region = query_join_region(&bound);
        query_admission_analyze(
            AdmissionQuery {
                bound: &bound,
                decorrelated: decorrelated.as_ref(),
                fingerprint: Fingerprint::from_raw(1),
                has_limit: false,
                join_region,
            },
            &aggregate_functions,
            &tables,
            AdmissionDepth::DecisionOnly,
        )
        .expect("analyze")
    }

    /// Each update query's table with its subsumer shape, sorted.
    fn subsumer_shapes(sql: &str) -> Vec<(String, bool)> {
        let mut shapes: Vec<(String, bool)> = admission_analysis(sql)
            .tables
            .iter()
            .map(|admission| (admission.table_name.to_string(), admission.subsumer_shape))
            .collect();
        shapes.sort();
        shapes
    }

    /// Queries with the subsumer shape of each update query's table.
    #[rustfmt::skip]
    const SUBSUMER_SHAPE_CASES: &[(&str, &[(&str, bool)])] = &[
        ("SELECT * FROM a WHERE x = 1", &[("a", true)]),
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE a.x = 1",
         &[("a", true), ("b", false)]),
        ("SELECT * FROM a RIGHT JOIN b ON b.id = a.id WHERE b.x = 1",
         &[("a", false), ("b", true)]),
        ("SELECT * FROM a LEFT JOIN (b JOIN c ON c.id = b.id) ON b.id = a.id",
         &[("a", true), ("b", false), ("c", false)]),
        ("SELECT * FROM (a LEFT JOIN b ON b.id = a.id) JOIN c ON c.id = a.id",
         &[("a", false), ("b", false), ("c", false)]),
        ("SELECT * FROM a, b WHERE a.x = 1",
         &[("a", false), ("b", false)]),
        // An ON the extractor can't read drops no preserved row.
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND b.y = a.x + 1",
         &[("a", true), ("b", false)]),
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND b.y IS NULL WHERE a.x = 1",
         &[("a", true), ("b", false)]),
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND a.y IS NULL WHERE a.x = 1",
         &[("a", true), ("b", false)]),
        // A WHERE on the nullable side drops unmatched or matched rows.
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x = 1",
         &[("a", false), ("b", false)]),
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL",
         &[("a", false), ("b", false)]),
        // A null test is a constraint: alone it describes a region, and
        // `IS NOT NULL` on the nullable side makes the join inner.
        ("SELECT * FROM a WHERE x IS NULL", &[("a", true)]),
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NOT NULL",
         &[("a", false), ("b", false)]),
        ("SELECT * FROM a WHERE x = 1 OR y = 2", &[("a", false)]),
        // A same-reference equality filters rows no region describes.
        ("SELECT * FROM a WHERE x = y", &[("a", false)]),
        // The preserved reference of a self-join is loaded in full.
        ("SELECT * FROM a a1 LEFT JOIN a a2 ON a2.id = a1.x WHERE a1.y = 1",
         &[("a", false), ("a", true)]),
        ("SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id)",
         &[("a", false), ("b", false)]),
        // A derived table's table is judged in its own branch.
        ("SELECT * FROM a JOIN (SELECT * FROM b WHERE x = 1) d ON d.id = a.id",
         &[("a", false), ("b", true)]),
        ("SELECT id FROM a WHERE x = 1 UNION SELECT id FROM b WHERE x = 2",
         &[("a", true), ("b", true)]),
        // Each exact reference of a relation is loaded in full, whatever its
        // other references load (PGC-511).
        ("SELECT id FROM a WHERE x = 1 UNION SELECT id FROM a WHERE x = 2",
         &[("a", true), ("a", true)]),
        ("SELECT id FROM a WHERE x = 1 UNION ALL SELECT id FROM a WHERE x = 2 \
          UNION ALL SELECT id FROM a WHERE x = 3",
         &[("a", true), ("a", true), ("a", true)]),
        ("SELECT id FROM a WHERE x = 1 EXCEPT SELECT id FROM a WHERE y = 2",
         &[("a", true), ("a", true)]),
        ("SELECT id FROM a WHERE x = 1 UNION ALL SELECT id FROM a WHERE x = 2 OR y = 2",
         &[("a", false), ("a", true)]),
        ("SELECT a.id FROM a JOIN b ON b.id = a.id WHERE a.x = 2 \
          UNION ALL SELECT id FROM a WHERE x = 1",
         &[("a", false), ("a", true), ("b", false)]),
        // The IN narrows the outer reference; the subquery reads its own whole.
        ("SELECT * FROM a WHERE x = 1 AND id IN (SELECT id FROM a WHERE y = 2)",
         &[("a", false), ("a", true)]),
        ("WITH c AS (SELECT id FROM a WHERE y = 2) \
          SELECT * FROM a WHERE x = 1 AND id IN (SELECT id FROM c)",
         &[("a", false), ("a", true)]),
    ];

    /// A table is a subsumer only where population loads every row its
    /// constraints describe: no join on its path can drop one, and WHERE
    /// reads it alone.
    #[test]
    fn test_subsumer_shape_table_complete() {
        for (sql, expected) in SUBSUMER_SHAPE_CASES {
            let expected: Vec<(String, bool)> = expected
                .iter()
                .map(|(table, shape)| (table.to_string(), *shape))
                .collect();
            assert_eq!(subsumer_shapes(sql), expected, "{sql}");
        }
    }

    /// The population cap goes to a plain projection whose ORDER BY the read
    /// can render, judged on the decorrelated form (ADR-060).
    #[test]
    fn test_population_max_limit() {
        let tables = catalog();
        let aggregate_functions = HashSet::from([EcoString::from("count")]);
        for (sql, capped) in [
            ("SELECT id, x FROM a ORDER BY x DESC LIMIT 3", true),
            ("SELECT * FROM a ORDER BY 3 LIMIT 3", true),
            ("SELECT x FROM a ORDER BY 2 LIMIT 3", false),
            (
                "SELECT x, count(*) FROM a GROUP BY x ORDER BY x LIMIT 3",
                false,
            ),
            (
                "SELECT a.id FROM a JOIN b ON b.id = a.x ORDER BY b.y LIMIT 3",
                false,
            ),
            (
                "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.x) ORDER BY id LIMIT 3",
                false,
            ),
        ] {
            let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve");
            let decorrelated =
                query_decorrelate_checked(&bound, &aggregate_functions).expect("decorrelate");
            assert_eq!(
                population_max_limit(&bound, decorrelated.as_ref(), &aggregate_functions, Some(3))
                    .is_some(),
                capped,
                "{sql}"
            );
        }
    }

    /// Each update query's table with its link columns.
    type TableLinkColumns = &'static [(&'static str, &'static [&'static str])];

    /// Each update query's link columns, those of its own reference, whether
    /// the conjunct linking them is recognized or not.
    #[test]
    fn test_update_query_link_columns() {
        #[rustfmt::skip]
        let cases: &[(&str, TableLinkColumns)] = &[
            ("SELECT * FROM a WHERE x = 1", &[("a", &[])]),
            ("SELECT * FROM a JOIN b ON b.id = a.x", &[("a", &["x"]), ("b", &["id"])]),
            ("SELECT * FROM a JOIN b ON b.id = a.x + 1", &[("a", &["x"]), ("b", &["id"])]),
            ("SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x < b.y",
             &[("a", &["id", "x"]), ("b", &["id", "y"])]),
            // A terminal reference has no links; the preserved side keeps its ON's.
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND a.x = 1",
             &[("a", &["id", "x"]), ("b", &[])]),
            // One update query per reference, each with its own links.
            ("SELECT * FROM a e JOIN a m ON m.id = e.x",
             &[("a", &["id"]), ("a", &["x"])]),
            // An uncorrelated subquery's own read loads its rows, so it links nothing.
            ("SELECT * FROM a WHERE x IN (SELECT id FROM b)", &[("a", &[]), ("b", &[])]),
            ("SELECT * FROM a WHERE x NOT IN (SELECT id FROM b WHERE y > 1)",
             &[("a", &[]), ("b", &[])]),
        ];
        for (sql, expected) in cases {
            let mut actual: Vec<(String, Vec<String>)> = admission_analysis(sql)
                .tables
                .iter()
                .map(|admission| {
                    let mut columns: Vec<String> = admission
                        .update_query
                        .predicates
                        .link_columns
                        .iter()
                        .map(ToString::to_string)
                        .collect();
                    columns.sort();
                    (admission.table_name.to_string(), columns)
                })
                .collect();
            actual.sort();
            let expected: Vec<(String, Vec<String>)> = expected
                .iter()
                .map(|(table, columns)| {
                    (
                        table.to_string(),
                        columns.iter().map(ToString::to_string).collect(),
                    )
                })
                .collect();
            assert_eq!(actual, expected, "{sql}");
        }
    }

    /// Whether `cached`'s region on `a` holds every row `new`'s one
    /// reference to `a` reads.
    fn parent_covers(cached: &str, new: &str) -> bool {
        let region = admission_analysis(cached)
            .tables
            .into_iter()
            .find(|admission| admission.subsumer_shape)
            .map(|admission| admission.update_query.predicates.filters.clone());
        let tables = catalog();
        let bound = ResolvedQuery::parse_resolve(new, &tables, &["public"]).expect("resolve");
        let references = subsumption_references(&bound).expect("references");
        let [reference] = references.as_slice() else {
            panic!("one reference in {new}");
        };
        region.is_some_and(|region| table_constraints_subsumed(&reference.filters, &region))
    }

    /// A cached `x = y` loaded only the rows where the two columns agree, so
    /// it is no parent; on the new side the equality only narrows (PGC-509).
    #[test]
    fn test_same_reference_equality_parent() {
        let cases = [
            (
                "SELECT * FROM a WHERE x = y",
                "SELECT * FROM a WHERE id = 5",
                false,
            ),
            (
                "SELECT * FROM a WHERE x = y AND id > 0",
                "SELECT * FROM a WHERE id = 5",
                false,
            ),
            ("SELECT * FROM a", "SELECT * FROM a WHERE x = y", true),
            (
                "SELECT * FROM a WHERE id > 0",
                "SELECT * FROM a WHERE x = y AND id = 5",
                true,
            ),
        ];
        for (cached, new, expected) in cases {
            assert_eq!(
                parent_covers(cached, new),
                expected,
                "cached {cached:?} covers new {new:?}"
            );
        }
    }

    /// One cached parent's region on a relation, in a state.
    struct Parent {
        fingerprint: Fingerprint,
        relation_oid: Oid,
        region: TableRanges,
        has_limit: bool,
        state: CandidateState,
    }

    /// Every subsumer-shaped reference of the cached queries, as a source.
    struct Parents(Vec<Parent>);

    impl SubsumerSource for Parents {
        fn candidates(
            &self,
            reference: &SubsumptionReference,
        ) -> impl Iterator<Item = SubsumerCandidate<'_>> {
            self.0
                .iter()
                .filter(move |parent| parent.relation_oid == reference.relation_oid)
                .map(|parent| SubsumerCandidate {
                    fingerprint: parent.fingerprint,
                    region: &parent.region,
                    has_limit: parent.has_limit,
                    state: parent.state,
                })
        }

        fn join_candidates(
            &self,
            _join: &JoinRegion,
        ) -> impl Iterator<Item = SubsumerCandidate<'_>> {
            std::iter::empty()
        }
    }

    /// The exact references of each `(fingerprint, cached query, LIMIT,
    /// state)` as parents.
    fn parents(cached: &[CachedParent<'_>]) -> Parents {
        Parents(
            cached
                .iter()
                .flat_map(|&(fingerprint, sql, has_limit, state)| {
                    admission_analysis(sql)
                        .tables
                        .into_iter()
                        .filter(|admission| admission.subsumer_shape)
                        .map(move |admission| Parent {
                            fingerprint: Fingerprint::from_raw(fingerprint),
                            relation_oid: admission.relation_oid,
                            region: admission.update_query.predicates.ranges.clone(),
                            has_limit,
                            state,
                        })
                })
                .collect(),
        )
    }

    /// A cached parent as `(fingerprint, query, LIMIT, state)`.
    type CachedParent<'a> = (u64, &'a str, bool, CandidateState);

    fn new_references(new: &str) -> Vec<SubsumptionReference> {
        let tables = catalog();
        let bound = ResolvedQuery::parse_resolve(new, &tables, &["public"]).expect("resolve");
        subsumption_references(&bound).expect("references")
    }

    /// Whether the exact references of `cached` cover every reference of
    /// `new`.
    fn parents_cover(cached: &str, new: &str) -> bool {
        let parents = parents(&[(1, cached, false, CandidateState::Ready)]);
        subsumption_covered(&new_references(new), None, &parents).is_some()
    }

    /// A query no Ready parent covers waits on the one populating parent
    /// that would: not on two, not on a limited one, and not when only a
    /// split of Ready parents would cover it (PGC-496).
    #[test]
    fn test_subsumption_pending_parent() {
        use CandidateState::{Populating, Ready};
        #[rustfmt::skip]
        let cases: &[(&[CachedParent<'_>], &str, Option<u64>, bool)] = &[
            (&[(7, "SELECT * FROM a WHERE x > 0", false, Populating)],
             "SELECT * FROM a WHERE x > 5", Some(7), false),
            (&[(7, "SELECT * FROM a WHERE x > 0", false, Ready)],
             "SELECT * FROM a WHERE x > 5", None, true),
            (&[(7, "SELECT * FROM a WHERE x > 0", false, Populating),
               (8, "SELECT * FROM a WHERE x > 0", false, Ready)],
             "SELECT * FROM a WHERE x > 5", None, true),
            (&[(7, "SELECT * FROM a WHERE x > 0", true, Populating)],
             "SELECT * FROM a WHERE x > 5", None, false),
            (&[(7, "SELECT * FROM a", false, Ready),
               (8, "SELECT * FROM b", false, Populating)],
             "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1", Some(8), false),
            (&[(7, "SELECT * FROM a", false, Populating),
               (8, "SELECT * FROM b", false, Populating)],
             "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1", None, false),
            (&[(7, "SELECT * FROM a", false, Populating)],
             "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1", None, false),
            (&[(7, "SELECT * FROM a WHERE x = 1", false, Populating),
               (8, "SELECT * FROM a WHERE x = 2", false, Populating)],
             "SELECT * FROM a WHERE x IN (1, 2)", None, false),
            (&[(7, "SELECT * FROM a WHERE x = 1", false, Ready),
               (8, "SELECT * FROM a WHERE x = 2", false, Ready)],
             "SELECT * FROM a WHERE x IN (1, 2)", None, true),
        ];
        for (cached, new, pending, covered) in cases {
            let parents = parents(cached);
            let references = new_references(new);
            assert_eq!(
                subsumption_covered(&references, None, &parents).is_some(),
                *covered,
                "covered: {new}"
            );
            assert_eq!(
                subsumption_pending_parent(&references, None, &parents),
                pending.map(Fingerprint::from_raw),
                "pending: {new}"
            );
        }
    }

    /// Only one SELECT over several base tables whose WHERE the model
    /// reads whole parents a join of its shape.
    #[test]
    fn test_join_region_parent_gate() {
        let cases = [
            ("SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1", true),
            (
                "SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL",
                true,
            ),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1 OR a.y = 2",
                false,
            ),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = b.y",
                false,
            ),
            ("SELECT * FROM a WHERE x = 1", false),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id WHERE EXISTS (SELECT 1 FROM c WHERE c.id = a.id)",
                false,
            ),
        ];
        for (sql, parent) in cases {
            assert_eq!(
                admission_analysis(sql).join_region.is_some(),
                parent,
                "{sql}"
            );
        }
    }

    /// Each exact reference of a relation parents a child under it on its
    /// own; a child spanning two references is covered value by value
    /// (PGC-494); a reference a join, an OR or an IN narrows is no parent
    /// (PGC-511).
    #[test]
    fn test_repeated_reference_parents_cover() {
        const UNION_ARMS: &str =
            "SELECT id FROM a WHERE x = 1 UNION ALL SELECT id FROM a WHERE x = 2";
        const MIXED_ARMS: &str =
            "SELECT id FROM a WHERE x = 1 UNION ALL SELECT id FROM a WHERE x = 2 OR y = 2";
        const JOIN_ARM: &str = "SELECT a.id FROM a JOIN b ON b.id = a.id WHERE a.x = 2 \
                                UNION ALL SELECT id FROM a WHERE x = 1";
        const SUBQUERY: &str =
            "SELECT * FROM a WHERE x = 1 AND id IN (SELECT id FROM a WHERE y = 2)";
        let cases = [
            (UNION_ARMS, "SELECT * FROM a WHERE x = 1 AND y = 3", true),
            (UNION_ARMS, "SELECT * FROM a WHERE x = 2", true),
            (UNION_ARMS, "SELECT * FROM a WHERE x = 3", false),
            (UNION_ARMS, "SELECT * FROM a WHERE x IN (1, 2)", true),
            (UNION_ARMS, "SELECT * FROM a WHERE x > 0", false),
            (
                UNION_ARMS,
                "SELECT id FROM a WHERE x = 2 UNION SELECT id FROM a WHERE x = 1 AND y = 1",
                true,
            ),
            (
                UNION_ARMS,
                "SELECT id FROM a WHERE x = 1 UNION SELECT id FROM a WHERE x = 3",
                false,
            ),
            (MIXED_ARMS, "SELECT * FROM a WHERE x = 1 AND y = 3", true),
            (MIXED_ARMS, "SELECT * FROM a WHERE x = 2", false),
            (JOIN_ARM, "SELECT * FROM a WHERE x = 1", true),
            (JOIN_ARM, "SELECT * FROM a WHERE x = 2", false),
            (SUBQUERY, "SELECT * FROM a WHERE y = 2 AND x = 5", true),
            (SUBQUERY, "SELECT * FROM a WHERE x = 1", false),
        ];
        for (cached, new, expected) in cases {
            assert_eq!(
                parents_cover(cached, new),
                expected,
                "cached {cached:?} covers new {new:?}"
            );
        }
    }

    /// A child constraining a column to a finite set is covered when each
    /// value's piece is held by some parent on its own; set columns split
    /// together, up to a bounded number of pieces (PGC-494).
    #[test]
    fn test_in_set_child_covered_value_by_value() {
        const POINTS: &str = "SELECT id FROM a WHERE x = 1 UNION ALL SELECT id FROM a WHERE x = 2";
        const GRID: &str = "SELECT id FROM a WHERE x = 1 AND y = 3 \
                            UNION ALL SELECT id FROM a WHERE x = 1 AND y = 4 \
                            UNION ALL SELECT id FROM a WHERE x = 2 AND y = 3 \
                            UNION ALL SELECT id FROM a WHERE x = 2 AND y = 4";
        let cases = [
            (POINTS, "SELECT * FROM a WHERE x IN (1, 2)", true),
            (POINTS, "SELECT * FROM a WHERE x IN (1, 2, 3)", false),
            (POINTS, "SELECT * FROM a WHERE x IN (1, 2) AND y = 3", true),
            (
                POINTS,
                "SELECT * FROM a WHERE x IN (1, 2) AND y IN (3, 4)",
                true,
            ),
            (POINTS, "SELECT * FROM a WHERE x IN (1, 2) AND x > 1", true),
            (POINTS, "SELECT * FROM a WHERE x IN (1, 2) OR y = 1", false),
            (
                GRID,
                "SELECT * FROM a WHERE x IN (1, 2) AND y IN (3, 4)",
                true,
            ),
            (GRID, "SELECT * FROM a WHERE x IN (1, 2) AND y = 3", true),
            (
                GRID,
                "SELECT * FROM a WHERE x IN (1, 2) AND y IN (3, 5)",
                false,
            ),
            (GRID, "SELECT * FROM a WHERE x IN (1, 2)", false),
        ];
        for (cached, new, expected) in cases {
            assert_eq!(
                parents_cover(cached, new),
                expected,
                "cached {cached:?} covers new {new:?}"
            );
        }

        // The split is bounded: 64 pieces are probed, 65 fall back to the
        // single-parent rule and miss.
        let arms: Vec<String> = (1..=65)
            .map(|value| format!("SELECT id FROM a WHERE x = {value}"))
            .collect();
        let parents = arms.join(" UNION ALL ");
        let values = |count: usize| {
            let values: Vec<String> = (1..=count).map(|value| value.to_string()).collect();
            format!("SELECT * FROM a WHERE x IN ({})", values.join(", "))
        };
        assert!(parents_cover(&parents, &values(64)), "64 pieces probed");
        assert!(
            !parents_cover(&parents, &values(65)),
            "65 pieces exceed the cap"
        );
    }

    /// When both in-set columns cannot split within the cap, the column that
    /// has parents is chosen, whichever set is smaller: splitting on `x`
    /// alone leaves pieces no per-`y` parent holds, splitting on `y` alone
    /// covers each piece.
    #[test]
    fn test_in_set_split_prefers_the_covered_column() {
        let arms: Vec<String> = (1..=40)
            .map(|value| format!("SELECT id FROM a WHERE y = {value}"))
            .collect();
        let parents = arms.join(" UNION ALL ");
        let values: Vec<String> = (1..=40).map(|value| value.to_string()).collect();
        let child = format!(
            "SELECT * FROM a WHERE x IN (1, 2) AND y IN ({})",
            values.join(", ")
        );
        assert!(parents_cover(&parents, &child), "split on y alone covers");
        let grid_child = format!(
            "SELECT * FROM a WHERE x IN (1, 2) AND y IN ({})",
            values[..32].join(", ")
        );
        assert!(
            parents_cover(&parents, &grid_child),
            "x and y together fit the cap and y's pieces are covered"
        );
    }
}
