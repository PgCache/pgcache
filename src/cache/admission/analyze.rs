//! The per-table admission derivation, extracted verbatim from the writer's
//! registration path (PGC-391) so pgcache-fit runs the identical analysis.

use std::collections::{HashMap, HashSet};
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
use crate::catalog::Catalog;
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::NodeId;
use crate::query::ast::rope::{Body, NodeKind, QueryTree, TreeBuilder, View};
use crate::query::compiled_predicate::CompiledPredicate;
use crate::query::decorrelate::query_decorrelate;
use crate::query::literal_settings::LiteralSettings;
use crate::query::resolve::ResolveResult;
use crate::query::resolve::{QueryBranch, ResolvedQuery};
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
    let bound = ResolvedQuery::resolve(base_query, tables, search_path)?;
    let (bound, literals) = query_literals_canonicalise(bound, settings);
    Ok(CanonicalResolution {
        bound: Arc::new(bound),
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

/// The relations whose every reference sits in one SELECT branch. Several
/// branches load the union of their rows, which one region doesn't describe.
fn single_branch_relations(update_queries: &[TableUpdateQuery<'_>]) -> HashSet<Oid> {
    let mut branches: HashMap<Oid, HashSet<NodeId>> = HashMap::new();
    for update_query in update_queries {
        branches
            .entry(update_query.relation_oid)
            .or_default()
            .insert(update_query.branch.select.id());
    }
    branches
        .into_iter()
        .filter(|(_, branches)| branches.len() == 1)
        .map(|(relation_oid, _)| relation_oid)
        .collect()
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
    let context = AdmissionContext {
        query: &query,
        update_source,
        single_branch: single_branch_relations(&update_queries),
        aggregate_functions,
        tables,
        depth,
    };
    let admissions = update_queries
        .into_iter()
        .map(|bound_update| context.table_admission_build(bound_update))
        .collect();
    Ok(AdmissionAnalysis {
        transformed: query.decorrelated.is_some(),
        tables: admissions,
    })
}

/// What every table's admission is derived against.
struct AdmissionContext<'a> {
    query: &'a AdmissionQuery<'a>,
    /// The query the update queries are read from: the decorrelated form
    /// when there is one.
    update_source: &'a Arc<ResolvedQuery>,
    single_branch: HashSet<Oid>,
    aggregate_functions: &'a HashSet<EcoString>,
    tables: &'a Catalog,
    depth: AdmissionDepth,
}

impl AdmissionContext<'_> {
    fn full(&self) -> bool {
        self.depth == AdmissionDepth::Full
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
        let subsumer_shape =
            self.single_branch.contains(&relation_oid) && update_query.predicates.exact;
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
    use crate::cache::admission::subsumption_references;
    use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::constraints::table_constraints_subsumed;

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
        query_admission_analyze(
            AdmissionQuery {
                bound: &bound,
                decorrelated: decorrelated.as_ref(),
                fingerprint: Fingerprint::from_raw(1),
                has_limit: false,
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
        // A WHERE on the nullable side drops unmatched or matched rows.
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x = 1",
         &[("a", false), ("b", false)]),
        ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL",
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
        ("SELECT id FROM a WHERE x = 1 UNION SELECT id FROM a WHERE x = 2",
         &[("a", false), ("a", false)]),
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
            .map(|admission| admission.update_query.predicates.filters);
        let tables = catalog();
        let bound = ResolvedQuery::parse_resolve(new, &tables, &["public"]).expect("resolve");
        let references = subsumption_references(&bound).expect("references");
        let [(_, filters)] = references.as_slice() else {
            panic!("one reference in {new}");
        };
        region.is_some_and(|region| table_constraints_subsumed(filters, &region))
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
}
