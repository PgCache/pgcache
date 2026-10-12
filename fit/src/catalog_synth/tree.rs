//! The synthesis walk over the rope: `catalog_synth`'s owned walk read
//! through views, feeding the same facts in the same order.

use std::collections::HashSet;

use ecow::EcoString;
use pgcache_lib::query::ast::LiteralValue;
use pgcache_lib::query::ast::rope::{
    Body, JoinQual, NodeKind, NodeRef, Query, QueryTree, ScalarKind, Select, SelectItem, Source,
    View, Where, WhereKind,
};

use super::{Scope, ScopeEntry, SynthCatalog, Synthesizer};

/// [`super::catalog_synthesize`] over rope trees.
pub fn catalog_synthesize_tree<'a>(
    queries: impl IntoIterator<Item = &'a QueryTree>,
) -> SynthCatalog {
    let mut synthesizer = Synthesizer::default();
    for query in queries {
        let mut scopes = Vec::new();
        synthesizer.query_walk_tree(query.root(), &mut scopes);
    }
    synthesizer.finish()
}

/// A column reference as `(qualifier, column)`.
fn column_of(position: NodeRef<'_>) -> Option<(Option<&EcoString>, &EcoString)> {
    match position.kind() {
        NodeKind::Column { table, column } => Some((table.as_ref(), column)),
        _ => None,
    }
}

impl Synthesizer {
    fn query_walk_tree(&mut self, query: Query<'_>, scopes: &mut Vec<Scope>) {
        // CTE bodies are walked once at their definition; references in FROM
        // are treated as derived sources.
        for cte in query.ctes() {
            let mut fresh = Vec::new();
            self.query_walk_tree(cte.query(), &mut fresh);
        }
        match query.body() {
            Body::Select(select) => self.select_walk_tree(select, query, scopes),
            Body::SetOp(set_op) => {
                self.query_walk_tree(set_op.left(), scopes);
                self.query_walk_tree(set_op.right(), scopes);
            }
            Body::Values(_) => {}
        }
    }

    fn select_walk_tree(&mut self, select: Select<'_>, query: Query<'_>, scopes: &mut Vec<Scope>) {
        let mut scope = Scope::default();
        let mut join_quals: Vec<Where<'_>> = Vec::new();
        for source in select.from() {
            self.source_collect_tree(source, &mut scope, scopes, &mut join_quals);
        }
        scopes.push(scope);

        let output_aliases: HashSet<&EcoString> = select
            .items()
            .into_iter()
            .flatten()
            .filter_map(|item| match item {
                SelectItem::Expr(expr) => expr.alias(),
                SelectItem::Star(_) => None,
            })
            .collect();

        for item in select.items().into_iter().flatten() {
            if let SelectItem::Expr(expr) = item {
                self.expr_attribute_tree(expr.expr().node_ref(), scopes);
            }
        }
        for qual in join_quals {
            self.expr_attribute_tree(qual.node_ref(), scopes);
            self.where_infer_tree(qual, scopes);
        }
        if let Some(where_clause) = select.where_clause() {
            self.expr_attribute_tree(where_clause.node_ref(), scopes);
            self.where_infer_tree(where_clause, scopes);
        }
        for column in select.group_by() {
            if column.table().is_none() && output_aliases.contains(column.column()) {
                continue;
            }
            self.column_attribute(column.table(), column.column(), scopes);
        }
        if let Some(having) = select.having() {
            self.expr_attribute_tree(having.node_ref(), scopes);
        }
        for order in query.order_by() {
            match order.expr().kind() {
                // ORDER BY <output alias> and ORDER BY <position> reference
                // the select list, not table columns.
                ScalarKind::Column(column)
                    if column.table().is_none() && output_aliases.contains(column.column()) => {}
                ScalarKind::Literal(_) => {}
                _ => self.expr_attribute_tree(order.expr().node_ref(), scopes),
            }
        }

        scopes.pop();
    }

    fn source_collect_tree<'t>(
        &mut self,
        source: Source<'t>,
        scope: &mut Scope,
        scopes: &mut Vec<Scope>,
        join_quals: &mut Vec<Where<'t>>,
    ) {
        match source {
            Source::Table(table) => {
                let key =
                    self.table_key(table.schema().map(EcoString::from).as_ref(), table.name());
                self.table_ensure(&key);
                let alias = table.alias().unwrap_or_else(|| table.name()).clone();
                scope.entries.push(ScopeEntry {
                    alias,
                    key: Some(key),
                });
            }
            Source::Subquery(subquery) => {
                scope.has_derived = true;
                if let Some(alias) = subquery.alias() {
                    scope.entries.push(ScopeEntry {
                        alias: alias.clone(),
                        key: None,
                    });
                }
                self.query_walk_tree(subquery.query(), scopes);
            }
            Source::CteRef(cte_ref) => {
                scope.has_derived = true;
                let alias = cte_ref
                    .alias()
                    .unwrap_or_else(|| cte_ref.cte_name())
                    .clone();
                scope.entries.push(ScopeEntry { alias, key: None });
            }
            Source::Join(join) => {
                self.source_collect_tree(join.left(), scope, scopes, join_quals);
                self.source_collect_tree(join.right(), scope, scopes, join_quals);
                if let JoinQual::On(qual) = join.qual() {
                    join_quals.push(qual);
                }
            }
        }
    }

    /// Attribute the columns of an expression: its nested subqueries walked
    /// first with their own scopes, then the columns at its own level.
    fn expr_attribute_tree(&mut self, expr: NodeRef<'_>, scopes: &mut Vec<Scope>) {
        for nested in own_level(expr, true) {
            if let NodeKind::Query { .. } = nested.kind() {
                self.query_walk_tree(Query::from_ref(nested), scopes);
            }
        }
        for position in own_level(expr, false) {
            if let Some((table, column)) = column_of(position) {
                self.column_attribute(table, column, scopes);
            }
        }
    }

    /// Record literal-comparison type evidence from WHERE/JOIN predicates.
    fn where_infer_tree(&mut self, expr: Where<'_>, scopes: &[Scope]) {
        match expr.kind() {
            WhereKind::Binary(binary) => {
                if binary.op().is_comparison() {
                    let sides = [
                        (where_column(binary.lexpr()), where_literal(binary.rexpr())),
                        (where_column(binary.rexpr()), where_literal(binary.lexpr())),
                    ];
                    for (column, literal) in sides {
                        if let (Some((table, column)), Some(literal)) = (column, literal) {
                            self.evidence_note(table, column, literal, scopes);
                            return;
                        }
                    }
                }
                self.where_infer_tree(binary.lexpr(), scopes);
                self.where_infer_tree(binary.rexpr(), scopes);
            }
            WhereKind::Multi(multi) => {
                // col IN (v1, v2, ...) / col BETWEEN a AND b shapes.
                let mut operands = multi.exprs();
                if let Some((table, column)) = operands.next().and_then(where_column) {
                    let literals: Vec<&LiteralValue> = operands.filter_map(where_literal).collect();
                    if !literals.is_empty() && literals.len() == multi.exprs().len() - 1 {
                        for literal in literals {
                            self.evidence_note(table, column, literal, scopes);
                        }
                        return;
                    }
                }
                for inner in multi.exprs() {
                    self.where_infer_tree(inner, scopes);
                }
            }
            WhereKind::Unary(unary) => self.where_infer_tree(unary.expr(), scopes),
            WhereKind::Scalar(_) | WhereKind::Subquery(_) => {}
        }
    }
}

/// The positions of `expr`'s own level in pre-order: `nested` selects the
/// roots of the subqueries under it (each skipped as a whole) instead.
fn own_level(expr: NodeRef<'_>, nested: bool) -> Vec<NodeRef<'_>> {
    let mut out = Vec::new();
    let end = expr.end();
    let mut next = u32::try_from(expr.id().index()).unwrap_or(u32::MAX);
    while next < end {
        let position = expr.at_position(pgcache_lib::query::ast::NodeId::from_index(next));
        let is_query = matches!(position.kind(), NodeKind::Query { .. });
        if is_query && position.id().index() != expr.id().index() {
            if nested {
                out.push(position);
            }
            next = position.end();
            continue;
        }
        if !nested {
            out.push(position);
        }
        next += 1;
    }
    out
}

fn where_column(expr: Where<'_>) -> Option<(Option<&EcoString>, &EcoString)> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => match scalar.kind() {
            ScalarKind::Column(column) => Some((column.table(), column.column())),
            _ => None,
        },
        _ => None,
    }
}

fn where_literal(expr: Where<'_>) -> Option<&LiteralValue> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => match scalar.kind() {
            ScalarKind::Literal(literal) => Some(literal.value()),
            _ => None,
        },
        _ => None,
    }
}
