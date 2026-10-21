//! Predicate pushdown (ADR-012) on the bound query: when a SELECT's sole
//! FROM source is a derived table, each outer WHERE conjunct that refers
//! only to that table and can be restated against every branch's own
//! columns moves into the branches, so population fetches and the cache
//! holds only the rows the predicate admits.
//!
//! The rewritten tree is built over the base (ADR-057): every subtree the
//! rewrite does not touch is kept whole; the candidate SELECT, its derived
//! source and each branch SELECT are re-emitted with their origins so
//! their bindings follow; a moved conjunct is a copy with its column
//! references replaced by the branch's own column node, or by a fresh
//! reference bound to the column a star stood for.

use ecow::EcoString;
use smallvec::SmallVec;

use crate::query::ast::rope::{
    Body, Name, NodeKind, NodeRef, Query, Replacement, Replacements, Scalar, ScalarKind, Select,
    SelectItem, Source, Star, TreeBuilder, View, Where,
};
use crate::query::ast::{BinaryOp, NodeId};
use crate::query::resolve::{
    Bindings, BindingsBuilder, ColumnBinding, ColumnTarget, ResolvedQuery, SlotId, StarItem,
};

/// What a pushed predicate's column reference becomes in one branch: the
/// branch's own column node, placed again so its binding follows, or a
/// reference built for a column a star expanded to, bound once placed.
#[derive(Clone, Copy)]
enum BranchColumn<'t> {
    Node(NodeRef<'t>),
    Star(ColumnBinding),
}

/// One branch SELECT of a derived table and what each of its output
/// positions projects.
struct Branch<'t> {
    select: Select<'t>,
    /// Each output position as the column it projects; `None` where the
    /// output is not a plain column.
    outputs: Vec<Option<BranchColumn<'t>>>,
}

/// A candidate SELECT and what moves out of its WHERE.
struct Move<'t> {
    select: Select<'t>,
    source: Source<'t>,
    /// The derived table's output names, by position: what a pushed
    /// conjunct's columns name.
    names: Vec<Option<EcoString>>,
    pushed: Vec<Where<'t>>,
    remaining: Vec<Where<'t>>,
    branches: Vec<Branch<'t>>,
}

impl<'t> Move<'t> {
    /// What `column` of a pushed conjunct becomes in `branch`.
    fn branch_column(&self, branch: &Branch<'t>, column: &str) -> Option<BranchColumn<'t>> {
        let position = self
            .names
            .iter()
            .position(|name| name.as_deref() == Some(column))?;
        branch.outputs.get(position).copied().flatten()
    }
}

/// Push the outer WHERE conjuncts of every sole-derived-table SELECT into
/// the derived table's branches, where the predicate can be restated.
/// `None` when nothing moves.
pub fn query_predicate_pushdown(bound: &ResolvedQuery) -> Option<ResolvedQuery> {
    let analysis = Analysis {
        bound,
        bindings: bound.bindings(),
    };
    let mut moves = Vec::new();
    analysis.moves_collect(bound.tree().root(), &mut moves);
    if moves.is_empty() {
        return None;
    }
    let mut emit = Emit {
        builder: TreeBuilder::over(bound.tree()),
        bindings: bound.bindings(),
        moves: &moves,
        bound_references: Vec::new(),
    };
    emit.query_emit(bound.tree().root());
    let (tree, remap) = emit.builder.finish();
    let mut builder = BindingsBuilder::from_remapped(bound.bindings(), &remap);
    for (node, binding) in emit.bound_references {
        builder.column_bind(node, ColumnTarget::Column(binding), false);
    }
    Some(ResolvedQuery::new(tree, builder.finish()))
}

struct Analysis<'a> {
    bound: &'a ResolvedQuery,
    bindings: &'a Bindings,
}

impl<'t> Analysis<'t> {
    /// Candidates in pre-order: a query's SELECT, or each branch of its
    /// set operation.
    fn moves_collect(&self, query: Query<'t>, out: &mut Vec<Move<'t>>) {
        match query.body() {
            Body::Select(select) => out.extend(self.select_move(select)),
            Body::SetOp(set_op) => {
                self.moves_collect(set_op.left(), out);
                self.moves_collect(set_op.right(), out);
            }
            Body::Values(_) => {}
        }
    }

    /// One SELECT: its sole FROM source must be a derived table without
    /// LIMIT, and it must have a WHERE to push from.
    fn select_move(&self, select: Select<'t>) -> Option<Move<'t>> {
        let where_clause = select.where_clause()?;
        let mut from = select.from();
        let (Some(source), None) = (from.next(), from.next()) else {
            return None;
        };
        let (derived, body) = match source {
            Source::Subquery(subquery) => (subquery.id(), subquery.query()),
            Source::CteRef(cte_ref) => (cte_ref.id(), cte_ref.query()),
            Source::Table(_) | Source::Join(_) => return None,
        };
        if body.has_limit() {
            return None;
        }
        let slot = self.bindings.source(derived)?;
        let mut branches = Vec::new();
        self.branches_collect(body, &mut branches)?;
        let mut planned = Move {
            select,
            source,
            names: self.bound.output_names(body),
            pushed: Vec::new(),
            remaining: Vec::new(),
            branches,
        };
        for conjunct in where_clause.conjuncts() {
            if self.conjunct_targets(conjunct, slot) && planned.conjunct_restatable(conjunct) {
                planned.pushed.push(conjunct);
            } else {
                planned.remaining.push(conjunct);
            }
        }
        (!planned.pushed.is_empty()).then_some(planned)
    }

    /// The branch SELECTs of a derived table in pre-order; `None` when a
    /// branch can take no predicate, which keeps every conjunct outside.
    fn branches_collect(&self, query: Query<'t>, out: &mut Vec<Branch<'t>>) -> Option<()> {
        match query.body() {
            Body::Select(select) => {
                if !branch_is_safe(select) {
                    return None;
                }
                out.push(Branch {
                    select,
                    outputs: self.branch_outputs(select),
                });
                Some(())
            }
            Body::SetOp(set_op) => {
                self.branches_collect(set_op.left(), out)?;
                self.branches_collect(set_op.right(), out)
            }
            Body::Values(_) => None,
        }
    }

    /// Each output position of a branch as the column it projects, stars
    /// expanded; `None` where the output is not a plain column.
    fn branch_outputs(&self, branch: Select<'t>) -> Vec<Option<BranchColumn<'t>>> {
        let Some(items) = branch.items() else {
            return Vec::new();
        };
        let mut outputs = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Expr(expr) => outputs.push(self.plain_column(expr.expr())),
                SelectItem::Star(star) => self.star_outputs(star, &mut outputs),
            }
        }
        outputs
    }

    /// The columns a star expanded to; a merged column is not a plain one.
    fn star_outputs(&self, star: Star<'t>, outputs: &mut Vec<Option<BranchColumn<'t>>>) {
        for item in self.bindings.star_by_id(star.id()).unwrap_or(&[]) {
            outputs.push(match item {
                StarItem::Column(binding) => Some(BranchColumn::Star(*binding)),
                StarItem::Merged(_) => None,
            });
        }
    }

    /// `expr` when it is a column reference bound to a column.
    fn plain_column(&self, expr: Scalar<'t>) -> Option<BranchColumn<'t>> {
        let ScalarKind::Column(column) = expr.kind() else {
            return None;
        };
        matches!(
            self.bindings.column_by_id(column.id()),
            Some(ColumnTarget::Column(_))
        )
        .then(|| BranchColumn::Node(column.node_ref()))
    }

    /// A conjunct can move when every column it references binds to the
    /// derived table, it references at least one, and it holds no subquery.
    fn conjunct_targets(&self, conjunct: Where<'t>, slot: SlotId) -> bool {
        let mut referenced = false;
        for position in conjunct.node_ref().subtree() {
            if let NodeKind::Column { .. } = position.kind() {
                referenced = true;
                if !matches!(
                    self.bindings.column_by_id(position.id()),
                    Some(ColumnTarget::Column(binding)) if binding.slot == slot
                ) {
                    return false;
                }
            } else if matches!(
                position.kind(),
                NodeKind::WhereSubquery { .. } | NodeKind::ScalarSubquery
            ) {
                return false;
            }
        }
        referenced
    }
}

impl<'t> Move<'t> {
    /// Whether `conjunct` can be restated against every branch: its form
    /// is one the rewrite copies, and each of its columns has a plain
    /// column behind it in each branch.
    fn conjunct_restatable(&self, conjunct: Where<'t>) -> bool {
        conjunct_is_restatable(conjunct)
            && self.branches.iter().all(|branch| {
                conjunct.node_ref().subtree().all(|position| {
                    if let NodeKind::Column { column, .. } = position.kind() {
                        self.branch_column(branch, column).is_some()
                    } else {
                        true
                    }
                })
            })
    }
}

/// The expression forms the rewrite restates: comparisons, logic and
/// multi-operand predicates over columns, literals and arrays.
fn conjunct_is_restatable(conjunct: Where<'_>) -> bool {
    conjunct.node_ref().subtree().all(|position| {
        matches!(
            position.kind(),
            NodeKind::Unary { .. }
                | NodeKind::Binary { .. }
                | NodeKind::Multi { .. }
                | NodeKind::Column { .. }
                | NodeKind::Literal(_)
                | NodeKind::Array { .. }
        )
    })
}

/// A branch can take a predicate unless grouping, HAVING or a window
/// function in its select list would change what the predicate means.
fn branch_is_safe(branch: Select<'_>) -> bool {
    if branch.group_by().len() > 0 || branch.having().is_some() {
        return false;
    }
    branch.items().is_none_or(|mut items| {
        !items.any(|item| match item {
            SelectItem::Expr(expr) => {
                matches!(expr.expr().kind(), ScalarKind::Function(function) if function.over().is_some())
            }
            SelectItem::Star(_) => false,
        })
    })
}

/// One operand of a fresh AND chain.
enum AndItem<'a, 't> {
    Keep(Where<'t>),
    Copy(Where<'t>, &'a Branch<'t>, &'a Move<'t>),
}

type AndChain<'a, 't> = SmallVec<[AndItem<'a, 't>; 4]>;

struct Emit<'a, 't> {
    builder: TreeBuilder<'t>,
    bindings: &'a Bindings,
    moves: &'a [Move<'t>],
    /// References built for star columns, by placed position.
    bound_references: Vec<(NodeId, ColumnBinding)>,
}

impl<'a, 't> Emit<'a, 't> {
    fn move_within(&self, node: NodeRef<'t>) -> bool {
        let (start, end) = (node.id().index(), node.end() as usize);
        self.moves
            .iter()
            .any(|m| (start..end).contains(&m.select.id().index()))
    }

    fn move_for(&self, select: Select<'t>) -> Option<&'a Move<'t>> {
        self.moves.iter().find(|m| m.select.id() == select.id())
    }

    fn query_emit(&mut self, query: Query<'t>) {
        if !self.move_within(query.node_ref()) {
            self.builder.keep(query.id());
            return;
        }
        let open = self
            .builder
            .open_from(query.node_ref().kind().clone(), query.id());
        for cte in query.ctes() {
            self.builder.keep(cte.id());
        }
        match query.body() {
            Body::Select(select) => match self.move_for(select) {
                Some(m) => self.candidate_emit(m),
                None => {
                    self.builder.keep(select.id());
                }
            },
            Body::SetOp(set_op) => {
                let open = self
                    .builder
                    .open_from(set_op.node_ref().kind().clone(), set_op.id());
                self.query_emit(set_op.left());
                self.query_emit(set_op.right());
                self.builder.close(open);
            }
            Body::Values(values) => {
                self.builder.keep(values.id());
            }
        }
        self.query_tail_emit(query);
        self.builder.close(open);
    }

    /// A query's ORDER BY and LIMIT literals, kept.
    fn query_tail_emit(&mut self, query: Query<'t>) {
        for order_by in query.order_by() {
            self.builder.keep(order_by.id());
        }
        if let Some(count) = query.limit_count() {
            self.builder.keep(count.id());
        }
        if let Some(offset) = query.limit_offset() {
            self.builder.keep(offset.id());
        }
    }

    /// The candidate SELECT with its remaining conjuncts as its WHERE and
    /// its derived source rebuilt around the pushed branches.
    fn candidate_emit(&mut self, m: &'a Move<'t>) {
        let select = m.select;
        let open = self.builder.open_from(
            select_kind_with_where(select, !m.remaining.is_empty()),
            select.id(),
        );
        self.items_keep(select);
        self.derived_source_emit(m);
        let remaining: AndChain<'a, 't> = m.remaining.iter().map(|c| AndItem::Keep(*c)).collect();
        self.and_chain_emit(&remaining);
        self.select_tail_keep(select);
        self.builder.close(open);
    }

    /// The derived source re-emitted around its pushed query: its alias
    /// names kept, the node itself standing in for the original so its
    /// slot follows.
    fn derived_source_emit(&mut self, m: &'a Move<'t>) {
        let (names, query): (Vec<Name<'t>>, Query<'t>) = match m.source {
            Source::Subquery(subquery) => (subquery.alias_columns().collect(), subquery.query()),
            Source::CteRef(cte_ref) => (
                cte_ref
                    .column_aliases()
                    .chain(cte_ref.alias_columns())
                    .collect(),
                cte_ref.query(),
            ),
            Source::Table(_) | Source::Join(_) => {
                debug_assert!(false, "a pushdown candidate's source is a derived table");
                self.builder.keep(m.source.id());
                return;
            }
        };
        let open = self
            .builder
            .open_from(m.source.node_ref().kind().clone(), m.source.id());
        for name in names {
            self.builder.keep(name.id());
        }
        self.pushed_query_emit(query, m);
        self.builder.close(open);
    }

    /// The derived table's query with each branch SELECT taking the pushed
    /// conjuncts.
    fn pushed_query_emit(&mut self, query: Query<'t>, m: &'a Move<'t>) {
        let open = self
            .builder
            .open_from(query.node_ref().kind().clone(), query.id());
        for cte in query.ctes() {
            self.builder.keep(cte.id());
        }
        match query.body() {
            Body::Select(select) => {
                let branch = m
                    .branches
                    .iter()
                    .find(|branch| branch.select.id() == select.id());
                match branch {
                    Some(branch) => self.branch_emit(branch, m),
                    None => {
                        debug_assert!(false, "every branch of a pushed derived table is planned");
                        self.builder.keep(select.id());
                    }
                }
            }
            Body::SetOp(set_op) => {
                let open = self
                    .builder
                    .open_from(set_op.node_ref().kind().clone(), set_op.id());
                self.pushed_query_emit(set_op.left(), m);
                self.pushed_query_emit(set_op.right(), m);
                self.builder.close(open);
            }
            Body::Values(values) => {
                self.builder.keep(values.id());
            }
        }
        self.query_tail_emit(query);
        self.builder.close(open);
    }

    /// A branch SELECT with the pushed conjuncts AND-ed onto its own WHERE,
    /// in the order they were pushed.
    fn branch_emit(&mut self, branch: &'a Branch<'t>, m: &'a Move<'t>) {
        let select = branch.select;
        let open = self
            .builder
            .open_from(select_kind_with_where(select, true), select.id());
        self.items_keep(select);
        for source in select.from() {
            self.builder.keep(source.id());
        }
        let mut items = AndChain::new();
        items.extend(select.where_clause().map(AndItem::Keep));
        items.extend(m.pushed.iter().map(|c| AndItem::Copy(*c, branch, m)));
        self.and_chain_emit(&items);
        self.select_tail_keep(select);
        self.builder.close(open);
    }

    fn items_keep(&mut self, select: Select<'t>) {
        for item in select.items().into_iter().flatten() {
            self.builder.keep(item.id());
        }
    }

    fn select_tail_keep(&mut self, select: Select<'t>) {
        for column in select.group_by() {
            self.builder.keep(column.id());
        }
        if let Some(having) = select.having() {
            self.builder.keep(having.id());
        }
    }

    /// `items` as a left-associated AND chain; nothing for no items.
    fn and_chain_emit(&mut self, items: &[AndItem<'a, 't>]) {
        let Some((last, init)) = items.split_last() else {
            return;
        };
        if init.is_empty() {
            self.and_item_emit(last);
            return;
        }
        let open = self.builder.open(NodeKind::Binary { op: BinaryOp::And });
        self.and_chain_emit(init);
        self.and_item_emit(last);
        self.builder.close(open);
    }

    fn and_item_emit(&mut self, item: &AndItem<'a, 't>) {
        match item {
            AndItem::Keep(conjunct) => {
                self.builder.keep(conjunct.id());
            }
            AndItem::Copy(conjunct, branch, m) => self.conjunct_copy(*conjunct, branch, m),
        }
    }

    /// `conjunct` restated against `branch`: a copy with every column
    /// reference replaced.
    fn conjunct_copy(&mut self, conjunct: Where<'t>, branch: &Branch<'t>, m: &Move<'t>) {
        let mut replacements = Replacements::new();
        let mut star_references: SmallVec<[(usize, ColumnBinding); 4]> = SmallVec::new();
        for position in conjunct.node_ref().subtree() {
            let NodeKind::Column { column, .. } = position.kind() else {
                continue;
            };
            let Some(replacement) = m.branch_column(branch, column) else {
                debug_assert!(false, "a pushed conjunct's columns are all planned");
                continue;
            };
            let replacement = match replacement {
                BranchColumn::Node(node) => Replacement::Subtree(node.id()),
                BranchColumn::Star(binding) => {
                    star_references.push((replacements.len(), binding));
                    Replacement::Leaf(self.star_reference_kind(binding))
                }
            };
            replacements.push(position.id(), replacement);
        }
        self.builder.copy(conjunct.node_ref(), &mut replacements);
        for (index, binding) in star_references {
            match replacements.placed(index) {
                Some(node) => self.bound_references.push((node, binding)),
                None => debug_assert!(false, "a replaced column is under the copied conjunct"),
            }
        }
    }

    /// A reference to a column a star stood for, qualified by its slot.
    fn star_reference_kind(&self, binding: ColumnBinding) -> NodeKind {
        let (table, column) = match (
            self.bindings.slot(binding.slot),
            self.bindings.binding_column(&binding),
        ) {
            (Some(slot), Some(column)) => (EcoString::from(slot.qualifier()), column.name.clone()),
            _ => {
                debug_assert!(false, "star item without a slot or column");
                (EcoString::new(), EcoString::new())
            }
        };
        NodeKind::Column {
            table: Some(table),
            column,
        }
    }
}

/// `select`'s node with its WHERE flag set to `has_where`.
fn select_kind_with_where(select: Select<'_>, has_where: bool) -> NodeKind {
    let mut kind = select.node_ref().kind().clone();
    if let NodeKind::Select {
        has_where: flag, ..
    } = &mut kind
    {
        *flag = has_where;
    }
    kind
}

#[cfg(test)]
pub(crate) mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::resolve::query_tree_resolve;

    /// A catalog of text-column tables, given as (name, columns), with
    /// relation OIDs from 1001 in order.
    fn tables(specs: &[(&str, &[&str])]) -> Catalog {
        let mut tables = BiHashMap::new();
        for (oid, (name, columns)) in (1001..).zip(specs) {
            let store = columns
                .iter()
                .enumerate()
                .map(|(i, column)| ColumnMetadata {
                    name: (*column).into(),
                    position: ColumnPosition::from_index(i).expect("column position in range"),
                    type_oid: TypeOid::from_raw(25),
                    data_type: Type::TEXT,
                    type_name: "text".into(),
                    cache_type_name: "text".into(),
                    is_primary_key: i == 0,
                });
            tables.insert_overwrite(TableMetadata::for_test(name, oid, store).into());
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
    pub(crate) const CORPUS: &[(&str, bool)] = &[
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
        ResolvedQuery::parse_resolve(sql, catalog, &["public"]).expect("resolve")
    }

    /// Every corpus query moves its predicate or keeps it as expected, and
    /// a moved predicate's carried bindings are the ones the resolver makes
    /// for the rewritten tree.
    #[test]
    fn test_pushdown_bindings_match_resolver() {
        let catalog = two_tables();
        for (sql, moves) in CORPUS {
            let pushed = query_predicate_pushdown(&bound(sql, &catalog));
            assert_eq!(pushed.is_some(), *moves, "{sql}");
            let Some(pushed) = pushed else {
                continue;
            };
            let resolved = query_tree_resolve(pushed.tree(), &catalog, &["public"])
                .unwrap_or_else(|e| panic!("{sql}: the pushed-down tree resolves: {e:?}"));
            let mut carried = pushed.bindings().describe();
            let mut expected = resolved.describe();
            carried.sort();
            expected.sort();
            assert_eq!(carried, expected, "{sql}");
        }
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
