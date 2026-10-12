//! Predicate pushdown (ADR-012) on the bound query: when a SELECT's sole
//! FROM source is a derived table, each outer WHERE conjunct that refers
//! only to that table and can be restated against every branch's own
//! columns moves into the branches, so population fetches and the cache
//! holds only the rows the predicate admits.
//!
//! The rewrite builds a new tree from pieces of the old one: a moved
//! conjunct keeps its nodes, its column references are replaced by copies
//! of the branch's own column nodes (or a reference a star stood for), and
//! the bindings follow through the refinalize remap (ADR-057).

use std::collections::HashMap;
use std::sync::Arc;

use ecow::EcoString;

use crate::query::ast::{
    AstNode, BinaryExpr, BinaryOp, ColumnNode, CteRefNode, FunctionCall, MultiExpr, NodeId,
    QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectColumns, SelectNode, SetOpNode,
    TableSource, TableSubqueryNode, UnaryExpr, WhereExpr, query_expr_refinalize,
};
use crate::query::resolve::{
    Bindings, BindingsBuilder, ColumnBinding, ColumnTarget, ResolvedQuery, SlotId, StarItem,
};

/// What a pushed predicate's column reference becomes in one branch: the
/// branch's own column node, copied with its id so its binding follows, or
/// a reference built for a column a star expanded to, bound after the
/// remap by its provisional id.
#[derive(Clone)]
enum BranchColumn {
    Node(ColumnNode),
    Star(ColumnBinding),
}

/// Builds the rewritten tree and remembers the references it had to make.
struct Pushdown<'a> {
    bound: &'a ResolvedQuery,
    /// Bindings for the references built for star columns, by provisional
    /// id order.
    synthesised: Vec<ColumnBinding>,
    changed: bool,
}

/// Push the outer WHERE conjuncts of every sole-derived-table SELECT into
/// the derived table's branches, where the predicate can be restated.
/// `None` when nothing moves.
pub fn query_predicate_pushdown(bound: &ResolvedQuery) -> Option<ResolvedQuery> {
    let mut pushdown = Pushdown {
        bound,
        synthesised: Vec::new(),
        changed: false,
    };
    let mut tree = pushdown.query(bound.tree());
    if !pushdown.changed {
        return None;
    }
    let remap = query_expr_refinalize(&mut tree);
    let mut builder = BindingsBuilder::from_remapped(bound.bindings(), &remap);
    for (k, binding) in pushdown.synthesised.iter().enumerate() {
        let provisional = NodeId::provisional(u32::try_from(k).unwrap_or(u32::MAX));
        if let Some(node) = remap.new_id(provisional) {
            builder.column_bind(node, ColumnTarget::Column(*binding), false);
        } else {
            debug_assert!(false, "a synthesised reference was not placed in the tree");
        }
    }
    Some(ResolvedQuery::new(Arc::new(tree), builder.finish()))
}

impl Pushdown<'_> {
    fn bindings(&self) -> &Bindings {
        self.bound.bindings()
    }

    fn query(&mut self, query: &QueryExpr) -> QueryExpr {
        let body = match &query.body {
            QueryBody::Select(select) => QueryBody::Select(Box::new(self.select(select))),
            // Set-operation branches may each be a pushdown candidate.
            QueryBody::SetOp(set_op) => QueryBody::SetOp(SetOpNode {
                left: Box::new(self.query(&set_op.left)),
                right: Box::new(self.query(&set_op.right)),
                ..set_op.clone()
            }),
            QueryBody::Values(values) => QueryBody::Values(values.clone()),
        };
        QueryExpr {
            body,
            ..query.clone()
        }
    }

    /// One SELECT: its sole FROM source must be a derived table without
    /// LIMIT, and it must have a WHERE to push from.
    fn select(&mut self, select: &SelectNode) -> SelectNode {
        let (Some(where_clause), [source]) = (&select.where_clause, select.from.as_slice()) else {
            return select.clone();
        };
        let (derived, body) = match source {
            TableSource::Subquery(subquery) => (subquery.id, subquery.query.as_ref()),
            TableSource::CteRef(cte_ref) => (cte_ref.id, cte_ref.query.as_ref()),
            TableSource::Table(_) | TableSource::Join(_) => return select.clone(),
        };
        if body.limit.is_some() {
            return select.clone();
        }
        let Some(slot) = self.bindings().source(derived) else {
            return select.clone();
        };
        let positions: HashMap<EcoString, usize> = self
            .bound
            .output_names(body)
            .into_iter()
            .enumerate()
            .filter_map(|(position, name)| name.map(|name| (name, position)))
            .collect();

        let mut body = body.clone();
        let mut remaining = Vec::new();
        for conjunct in where_clause.clone().conjuncts() {
            if !self.conjunct_targets(&conjunct, slot) {
                remaining.push(conjunct);
                continue;
            }
            match self.query_push(&body, &conjunct, &positions) {
                Some(pushed) => {
                    body = pushed;
                    self.changed = true;
                }
                None => remaining.push(conjunct),
            }
        }

        let source = match source {
            TableSource::Subquery(subquery) => TableSource::Subquery(TableSubqueryNode {
                query: Box::new(body),
                ..subquery.clone()
            }),
            TableSource::CteRef(cte_ref) => TableSource::CteRef(CteRefNode {
                query: Box::new(body),
                ..cte_ref.clone()
            }),
            TableSource::Table(_) | TableSource::Join(_) => return select.clone(),
        };
        SelectNode {
            from: [source].into_iter().collect(),
            where_clause: WhereExpr::conjuncts_join(remaining),
            ..select.clone()
        }
    }

    /// A conjunct can move when every column it references binds to the
    /// derived table, it references at least one, and it holds no subquery.
    fn conjunct_targets(&self, conjunct: &WhereExpr, slot: SlotId) -> bool {
        let mut columns = conjunct.nodes::<ColumnNode>().peekable();
        columns.peek().is_some()
            && columns.all(|column| {
                matches!(
                    self.bindings().column(column),
                    Some(ColumnTarget::Column(binding)) if binding.slot == slot
                )
            })
            && conjunct.nodes::<QueryExpr>().next().is_none()
    }

    /// `query` with `conjunct` restated against each branch's own columns,
    /// or `None` when some branch cannot take it.
    fn query_push(
        &mut self,
        query: &QueryExpr,
        conjunct: &WhereExpr,
        positions: &HashMap<EcoString, usize>,
    ) -> Option<QueryExpr> {
        let body = match &query.body {
            QueryBody::Select(select) => {
                QueryBody::Select(Box::new(self.branch_push(select, conjunct, positions)?))
            }
            QueryBody::SetOp(set_op) => QueryBody::SetOp(SetOpNode {
                left: Box::new(self.query_push(&set_op.left, conjunct, positions)?),
                right: Box::new(self.query_push(&set_op.right, conjunct, positions)?),
                ..set_op.clone()
            }),
            QueryBody::Values(_) => return None,
        };
        Some(QueryExpr {
            body,
            ..query.clone()
        })
    }

    fn branch_push(
        &mut self,
        branch: &SelectNode,
        conjunct: &WhereExpr,
        positions: &HashMap<EcoString, usize>,
    ) -> Option<SelectNode> {
        if !branch_is_safe(branch) {
            return None;
        }
        let outputs = self.branch_outputs(branch);
        // Only the columns the conjunct names need a plain column behind
        // them; other output positions may be anything.
        let mut replacements: HashMap<EcoString, BranchColumn> = HashMap::new();
        for column in conjunct.nodes::<ColumnNode>() {
            let position = *positions.get(&column.column)?;
            let replacement = outputs.get(position)?.clone()?;
            replacements.insert(column.column.clone(), replacement);
        }
        let pushed = self.where_remap(conjunct, &replacements)?;
        let where_clause = match &branch.where_clause {
            Some(existing) => WhereExpr::Binary(BinaryExpr {
                id: NodeId::PENDING,
                op: BinaryOp::And,
                lexpr: Box::new(existing.clone()),
                rexpr: Box::new(pushed),
            }),
            None => pushed,
        };
        Some(SelectNode {
            where_clause: Some(where_clause),
            ..branch.clone()
        })
    }

    /// Each output position of a branch as the column it projects, stars
    /// expanded; `None` where the output is not a plain column.
    fn branch_outputs(&self, branch: &SelectNode) -> Vec<Option<BranchColumn>> {
        let SelectColumns::Columns(columns) = &branch.columns else {
            return Vec::new();
        };
        let mut outputs = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                SelectColumn::Expr { expr, .. } => outputs.push(match expr {
                    ScalarExpr::Column(node)
                        if matches!(
                            self.bindings().column(node),
                            Some(ColumnTarget::Column(_))
                        ) =>
                    {
                        Some(BranchColumn::Node(node.clone()))
                    }
                    ScalarExpr::Column(_)
                    | ScalarExpr::Function(_)
                    | ScalarExpr::Literal(_)
                    | ScalarExpr::Case(_)
                    | ScalarExpr::Arithmetic(_)
                    | ScalarExpr::Subquery(_)
                    | ScalarExpr::Array(_)
                    | ScalarExpr::TypeCast { .. } => None,
                }),
                SelectColumn::Star(star) => {
                    for item in self.bindings().star(star).unwrap_or(&[]) {
                        outputs.push(match item {
                            StarItem::Column(binding) => Some(BranchColumn::Star(*binding)),
                            StarItem::Merged(_) => None,
                        });
                    }
                }
            }
        }
        outputs
    }

    /// `conjunct` with every column reference replaced by its branch
    /// column; `None` for an expression form the rewrite does not restate.
    fn where_remap(
        &mut self,
        expr: &WhereExpr,
        replacements: &HashMap<EcoString, BranchColumn>,
    ) -> Option<WhereExpr> {
        Some(match expr {
            WhereExpr::Scalar(scalar) => {
                WhereExpr::Scalar(self.scalar_remap(scalar, replacements)?)
            }
            WhereExpr::Unary(unary) => WhereExpr::Unary(UnaryExpr {
                expr: Box::new(self.where_remap(&unary.expr, replacements)?),
                ..unary.clone()
            }),
            WhereExpr::Binary(binary) => WhereExpr::Binary(BinaryExpr {
                lexpr: Box::new(self.where_remap(&binary.lexpr, replacements)?),
                rexpr: Box::new(self.where_remap(&binary.rexpr, replacements)?),
                ..binary.clone()
            }),
            WhereExpr::Multi(multi) => WhereExpr::Multi(MultiExpr {
                exprs: multi
                    .exprs
                    .iter()
                    .map(|e| self.where_remap(e, replacements))
                    .collect::<Option<_>>()?,
                ..multi.clone()
            }),
            // Conjuncts holding a subquery never reach here.
            WhereExpr::Subquery { .. } => return None,
        })
    }

    fn scalar_remap(
        &mut self,
        expr: &ScalarExpr,
        replacements: &HashMap<EcoString, BranchColumn>,
    ) -> Option<ScalarExpr> {
        Some(match expr {
            ScalarExpr::Column(column) => match replacements.get(&column.column)? {
                BranchColumn::Node(node) => ScalarExpr::Column(node.clone()),
                BranchColumn::Star(binding) => ScalarExpr::Column(self.reference_make(*binding)),
            },
            ScalarExpr::Literal(literal) => ScalarExpr::Literal(literal.clone()),
            ScalarExpr::Array(elements) => ScalarExpr::Array(
                elements
                    .iter()
                    .map(|e| self.scalar_remap(e, replacements))
                    .collect::<Option<_>>()?,
            ),
            ScalarExpr::Function(_)
            | ScalarExpr::Case(_)
            | ScalarExpr::Arithmetic(_)
            | ScalarExpr::Subquery(_)
            | ScalarExpr::TypeCast { .. } => return None,
        })
    }

    /// A reference to a column a star stood for, under a provisional id
    /// that the remap resolves once the tree is placed.
    fn reference_make(&mut self, binding: ColumnBinding) -> ColumnNode {
        let bindings = self.bindings();
        let (table, column) = match (
            bindings.slot(binding.slot),
            bindings.binding_column(&binding),
        ) {
            (Some(slot), Some(column)) => (EcoString::from(slot.qualifier()), column.name.clone()),
            _ => {
                debug_assert!(false, "star item without a slot or column");
                (EcoString::new(), EcoString::new())
            }
        };
        let k = u32::try_from(self.synthesised.len()).unwrap_or(u32::MAX);
        self.synthesised.push(binding);
        ColumnNode {
            id: NodeId::provisional(k),
            table: Some(table),
            column,
        }
    }
}

/// A branch can take a predicate unless grouping, HAVING or a window
/// function in its select list would change what the predicate means.
fn branch_is_safe(branch: &SelectNode) -> bool {
    if !branch.group_by.is_empty() || branch.having.is_some() {
        return false;
    }
    match &branch.columns {
        SelectColumns::Columns(columns) => !columns.iter().any(|column| {
            matches!(
                column,
                SelectColumn::Expr {
                    expr: ScalarExpr::Function(FunctionCall { over: Some(_), .. }),
                    ..
                }
            )
        }),
        SelectColumns::None => true,
    }
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_expr_parse;
    use crate::query::ast::rope::QueryTree;
    use crate::query::resolve::{
        Bindings, query_expr_resolve, query_tree_resolve, query_tree_sql_into,
    };
    use crate::query::transform::query_predicate_pushdown_tree;

    /// A catalog of text-column tables, given as (name, columns), with
    /// relation OIDs from 1001 in order.
    fn tables(specs: &[(&str, &[&str])]) -> Catalog {
        let mut tables = BiHashMap::new();
        for (oid, (name, columns)) in (1001..).zip(specs) {
            let store =
                ColumnStore::new(
                    columns
                        .iter()
                        .enumerate()
                        .map(|(i, column)| ColumnMetadata {
                            name: (*column).into(),
                            position: ColumnPosition::from_index(i)
                                .expect("column position in range"),
                            type_oid: TypeOid::from_raw(25),
                            data_type: Type::TEXT,
                            type_name: "text".into(),
                            cache_type_name: "text".into(),
                            is_primary_key: i == 0,
                        }),
                );
            tables.insert_overwrite(
                TableMetadata {
                    replica_identity_full: false,
                    relation_oid: Oid::from_raw(oid),
                    name: (*name).into(),
                    schema: "public".into(),
                    columns: store,
                    primary_key_columns: vec![columns[0].into()],
                    indexes: Vec::new(),
                }
                .into(),
            );
        }
        tables
    }

    fn two_tables() -> Catalog {
        tables(&[
            ("t1", &["a", "b"]),
            ("t2", &["a", "b"]),
            ("t3", &["c", "d"]),
        ])
    }

    /// (sql, whether a predicate moves) over the shapes pushdown decides on.
    const CORPUS: &[(&str, bool)] = &[
        (
            "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x'",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t3) sub WHERE sub.a = 'x'",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1 WHERE b = 'y' UNION ALL SELECT a, b FROM t2) sub \
          WHERE sub.a = 'x'",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a = 'x' AND sub.b = 'y'",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a IN ('x', 'y') AND sub.b IS NOT NULL",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, 'CONST' AS label, NULL AS extra FROM t1 UNION ALL \
          SELECT a, 'OTHER' AS label, NULL AS extra FROM t2) sub WHERE sub.a = 'x'",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1) sub1 WHERE sub1.a = 'x' UNION ALL \
          SELECT * FROM (SELECT a, b FROM t2) sub2 WHERE sub2.a = 'y'",
            true,
        ),
        (
            "SELECT * FROM (SELECT * FROM t1) sub WHERE sub.a = 'x'",
            true,
        ),
        (
            "SELECT * FROM (SELECT t1.*, b AS other FROM t1) sub WHERE sub.b = 'x' AND sub.other = 'y'",
            true,
        ),
        (
            "WITH sub AS (SELECT a, b FROM t1) SELECT sub.a FROM sub WHERE sub.a = 'x'",
            true,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a = 'x' AND sub.a <> ALL (ARRAY['y'])",
            true,
        ),
        (
            "SELECT * FROM (SELECT upper(a) AS a, b FROM t1) sub WHERE sub.a = 'x'",
            false,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1 GROUP BY a, b) sub WHERE sub.a = 'x'",
            false,
        ),
        (
            "SELECT * FROM (SELECT a, row_number() OVER () AS rn FROM t1) sub WHERE sub.a = 'x'",
            false,
        ),
        ("SELECT * FROM t1 WHERE a = 'x'", false),
        (
            "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a IN (SELECT a FROM t2)",
            false,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1 LIMIT 5) sub WHERE sub.a = 'x'",
            false,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2 GROUP BY a, b) sub \
          WHERE sub.a = 'x'",
            false,
        ),
        (
            "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a = 'x' AND upper(sub.b) = 'Y'",
            true,
        ),
    ];

    fn bound(sql: &str, catalog: &Catalog) -> ResolvedQuery {
        let query = Arc::new(query_expr_parse(sql).expect("parse"));
        let bindings = query_expr_resolve(&query, catalog, &["public"]).expect("resolve");
        ResolvedQuery::new(query, bindings)
    }

    /// Every corpus query moves its predicate or keeps it as expected, and
    /// a moved predicate's remapped bindings are the ones the resolver
    /// would make for the rewritten tree.
    #[test]
    fn test_bound_pushdown_bindings_match_resolver() {
        let catalog = two_tables();
        for (sql, moves) in CORPUS {
            let pushed = query_predicate_pushdown(&bound(sql, &catalog));
            assert_eq!(pushed.is_some(), *moves, "{sql}");
            let Some(pushed) = pushed else {
                continue;
            };
            let expected = query_expr_resolve(pushed.tree(), &catalog, &["public"])
                .expect("the pushed-down tree resolves");
            assert_eq!(pushed.bindings().describe(), expected.describe(), "{sql}");
        }
    }

    fn tree_sql(tree: &QueryTree, bindings: &Bindings) -> String {
        let mut buf = String::new();
        query_tree_sql_into(tree, bindings, &mut buf);
        buf
    }

    /// The rope rewrite moves what the owned rewrite moves, renders the
    /// same SQL, and its carried bindings are the ones the resolver makes
    /// for the rewritten tree.
    #[test]
    fn test_rope_pushdown_matches_the_owned_rewrite() {
        let catalog = two_tables();
        for (sql, moves) in CORPUS {
            let bound = bound(sql, &catalog);
            let owned = query_predicate_pushdown(&bound);
            let rope = query_predicate_pushdown_tree(&bound);
            assert_eq!(rope.is_some(), *moves, "{sql}");
            let (Some(owned), Some((tree, bindings))) = (owned, rope) else {
                continue;
            };
            assert_eq!(tree_sql(&tree, &bindings), owned.sql(), "{sql}");
            let resolved = query_tree_resolve(&tree, &catalog, &["public"])
                .unwrap_or_else(|e| panic!("{sql}: the pushed-down rope resolves: {e:?}"));
            let mut carried = bindings.describe();
            let mut expected = resolved.describe();
            carried.sort();
            expected.sort();
            assert_eq!(carried, expected, "{sql}");
        }
    }

    #[test]
    fn test_rope_pushdown_render_cases() {
        render_cases_check(|bound| {
            query_predicate_pushdown_tree(bound).map(|(tree, bindings)| tree_sql(&tree, &bindings))
        });
    }

    #[test]
    fn test_pushed_predicate_is_restated_per_branch() {
        let catalog = two_tables();
        let pushed = query_predicate_pushdown(&bound(
            "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t3) sub WHERE sub.a = 'x'",
            &catalog,
        ))
        .expect("moves");
        assert_eq!(
            pushed.sql(),
            "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 \
             WHERE public.t1.a = 'x' UNION ALL SELECT public.t3.c, public.t3.d FROM public.t3 \
             WHERE public.t3.c = 'x') sub"
        );
    }

    /// A branch column a star stood for has no node to copy; the rewrite
    /// makes the reference and binds it to the star's column.
    #[test]
    fn test_star_branch_column_gets_a_bound_reference() {
        let catalog = two_tables();
        let pushed = query_predicate_pushdown(&bound(
            "SELECT * FROM (SELECT * FROM t1) sub WHERE sub.b = 'x'",
            &catalog,
        ))
        .expect("moves");
        assert_eq!(
            pushed.sql(),
            "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 \
             WHERE public.t1.b = 'x') sub"
        );
    }

    type PushedCase = (&'static str, Catalog, &'static str, &'static str);
    type KeptCase = (&'static str, Catalog, &'static str);

    /// Where a predicate lands: every branch of a pushable derived table,
    /// AND-ed onto a branch's own WHERE with its columns restated per
    /// branch; and shapes whose result it would change, which keep the
    /// outer WHERE.
    fn render_cases() -> (Vec<PushedCase>, Vec<KeptCase>) {
        let ab_ab = || tables(&[("t1", &["a", "b"]), ("t2", &["a", "b"])]);
        let ab_cd = || tables(&[("t1", &["a", "b"]), ("t2", &["c", "d"])]);
        let ab = || tables(&[("t1", &["a", "b"])]);
        #[rustfmt::skip]
        let pushed = vec![
            ("both UNION branches",       ab_ab(), "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x'",
             "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 WHERE public.t1.a = 'x' UNION ALL SELECT public.t2.a, public.t2.b FROM public.t2 WHERE public.t2.a = 'x') sub"),
            ("columns remapped by position", ab_cd(), "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2) sub WHERE sub.a = 'x'",
             "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 WHERE public.t1.a = 'x' UNION ALL SELECT public.t2.c, public.t2.d FROM public.t2 WHERE public.t2.c = 'x') sub"),
            ("existing WHERE AND-ed",     ab_ab(), "SELECT * FROM (SELECT a, b FROM t1 WHERE b = 'y' UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x'",
             "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 WHERE public.t1.b = 'y' AND public.t1.a = 'x' UNION ALL SELECT public.t2.a, public.t2.b FROM public.t2 WHERE public.t2.a = 'x') sub"),
            ("every conjunct",            ab_ab(), "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x' AND sub.b = 'y'",
             "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 WHERE public.t1.a = 'x' AND public.t1.b = 'y' UNION ALL SELECT public.t2.a, public.t2.b FROM public.t2 WHERE public.t2.a = 'x' AND public.t2.b = 'y') sub"),
            ("plain derived table",       ab(),    "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a = 'x'",
             "SELECT sub.a, sub.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 WHERE public.t1.a = 'x') sub"),
            ("literal columns beside",    ab_ab(), "SELECT * FROM (SELECT a, 'CONST' AS label, NULL AS extra FROM t1 UNION ALL SELECT a, 'OTHER' AS label, NULL AS extra FROM t2) sub WHERE sub.a = 'x'",
             "SELECT sub.a, sub.label, sub.extra FROM (SELECT public.t1.a, 'CONST' AS label, NULL AS extra FROM public.t1 WHERE public.t1.a = 'x' UNION ALL SELECT public.t2.a, 'OTHER' AS label, NULL AS extra FROM public.t2 WHERE public.t2.a = 'x') sub"),
            ("set-op branches recurse",   ab_ab(), "SELECT * FROM (SELECT a, b FROM t1) sub1 WHERE sub1.a = 'x' UNION ALL SELECT * FROM (SELECT a, b FROM t2) sub2 WHERE sub2.a = 'y'",
             "SELECT sub1.a, sub1.b FROM (SELECT public.t1.a, public.t1.b FROM public.t1 WHERE public.t1.a = 'x') sub1 UNION ALL SELECT sub2.a, sub2.b FROM (SELECT public.t2.a, public.t2.b FROM public.t2 WHERE public.t2.a = 'y') sub2"),
        ];
        #[rustfmt::skip]
        let kept = vec![
            ("literal output column", ab(),    "SELECT * FROM (SELECT a, 42 AS val FROM t1) sub WHERE sub.val = 99"),
            ("GROUP BY",              ab(),    "SELECT * FROM (SELECT a, count(b) AS cnt FROM t1 GROUP BY a) sub WHERE sub.a = 'x'"),
            ("window function",       ab(),    "SELECT * FROM (SELECT a, row_number() OVER (ORDER BY b) AS rn FROM t1) sub WHERE sub.a = 'x'"),
            ("no derived table",      ab(),    "SELECT a, b FROM t1 WHERE a = 'x'"),
            ("subquery predicate",    ab_ab(), "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a IN (SELECT a FROM t2)"),
            ("LIMIT in the branch",   ab(),    "SELECT * FROM (SELECT a, b FROM t1 LIMIT 10) sub WHERE sub.a = 'x'"),
        ];
        (pushed, kept)
    }

    /// `pushdown` (a rewrite as its rendered SQL, or `None`) over the
    /// render cases.
    fn render_cases_check(pushdown: impl Fn(&ResolvedQuery) -> Option<String>) {
        let (pushed, kept) = render_cases();
        for (label, catalog, sql, expected) in &pushed {
            let result = pushdown(&bound(sql, catalog))
                .unwrap_or_else(|| panic!("{label}: expected a pushdown of {sql}"));
            assert_eq!(result, *expected, "{label}");
        }
        for (label, catalog, sql) in &kept {
            assert!(
                pushdown(&bound(sql, catalog)).is_none(),
                "{label}: {sql} keeps its WHERE"
            );
        }
    }

    #[test]
    fn test_pushdown_render_cases() {
        render_cases_check(|bound| query_predicate_pushdown(bound).map(|pushed| pushed.sql()));
    }
}
