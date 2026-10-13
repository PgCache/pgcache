//! The synthesis walk: every relation and column reference a corpus of
//! trees makes, read through views and fed to the synthesizer's facts.

use std::collections::HashSet;

use ecow::EcoString;
use pgcache_lib::query::ast::rope::{
    Binary, Body, JoinQual, Multi, NodeKind, NodeRef, Query, QueryTree, ScalarKind, Select,
    SelectItem, Source, View, Where, WhereKind,
};
use pgcache_lib::query::ast::{LiteralValue, NodeId};

use super::{Scope, ScopeEntry, SynthCatalog, Synthesizer};

/// The catalog a corpus of queries implies.
pub fn catalog_synthesize<'a>(queries: impl IntoIterator<Item = &'a QueryTree>) -> SynthCatalog {
    let mut synthesizer = Synthesizer::default();
    for query in queries {
        let mut scopes = Vec::new();
        synthesizer.query_walk(query.root(), &mut scopes);
    }
    synthesizer.finish()
}

/// A column reference as `(qualifier, column)`.
type ColumnName<'t> = (Option<&'t EcoString>, &'t EcoString);

fn column_of(position: NodeRef<'_>) -> Option<ColumnName<'_>> {
    match position.kind() {
        NodeKind::Column { table, column } => Some((table.as_ref(), column)),
        _ => None,
    }
}

/// A FROM clause as collected: the scope its sources define and the ON
/// conditions of its joins, attributed once the scope is in place.
#[derive(Default)]
struct FromClause<'t> {
    scope: Scope,
    join_quals: Vec<Where<'t>>,
}

/// The output aliases a select list defines, which GROUP BY and ORDER BY
/// may name instead of a table column.
fn output_aliases(select: Select<'_>) -> HashSet<&EcoString> {
    select
        .items()
        .into_iter()
        .flatten()
        .filter_map(|item| match item {
            SelectItem::Expr(expr) => expr.alias(),
            SelectItem::Star(_) => None,
        })
        .collect()
}

impl Synthesizer {
    fn query_walk(&mut self, query: Query<'_>, scopes: &mut Vec<Scope>) {
        // CTE bodies are walked once at their definition; references in FROM
        // are treated as derived sources.
        for cte in query.ctes() {
            let mut fresh = Vec::new();
            self.query_walk(cte.query(), &mut fresh);
        }
        match query.body() {
            Body::Select(select) => self.select_walk(select, query, scopes),
            Body::SetOp(set_op) => {
                self.query_walk(set_op.left(), scopes);
                self.query_walk(set_op.right(), scopes);
            }
            Body::Values(_) => {}
        }
    }

    fn select_walk(&mut self, select: Select<'_>, query: Query<'_>, scopes: &mut Vec<Scope>) {
        let mut from = FromClause::default();
        for source in select.from() {
            self.source_collect(source, &mut from, scopes);
        }
        scopes.push(from.scope);
        let aliases = output_aliases(select);

        self.select_list_attribute(select, scopes);
        for predicate in from.join_quals.into_iter().chain(select.where_clause()) {
            self.expr_attribute(predicate.node_ref(), scopes);
            self.where_infer(predicate, scopes);
        }
        self.group_by_attribute(select, &aliases, scopes);
        if let Some(having) = select.having() {
            self.expr_attribute(having.node_ref(), scopes);
        }
        self.order_by_attribute(query, &aliases, scopes);

        scopes.pop();
    }

    fn select_list_attribute(&mut self, select: Select<'_>, scopes: &mut Vec<Scope>) {
        for item in select.items().into_iter().flatten() {
            if let SelectItem::Expr(expr) = item {
                self.expr_attribute(expr.expr().node_ref(), scopes);
            }
        }
    }

    /// GROUP BY columns that name a table column; a bare output alias
    /// references the select list instead.
    fn group_by_attribute(
        &mut self,
        select: Select<'_>,
        aliases: &HashSet<&EcoString>,
        scopes: &[Scope],
    ) {
        for column in select.group_by() {
            if column.table().is_some() || !aliases.contains(column.column()) {
                self.column_attribute(column.table(), column.column(), scopes);
            }
        }
    }

    /// ORDER BY items that name a table column; an output alias or a
    /// position references the select list instead.
    fn order_by_attribute(
        &mut self,
        query: Query<'_>,
        aliases: &HashSet<&EcoString>,
        scopes: &mut Vec<Scope>,
    ) {
        for order in query.order_by() {
            let references_output = match order.expr().kind() {
                ScalarKind::Column(column) => {
                    column.table().is_none() && aliases.contains(column.column())
                }
                ScalarKind::Literal(_) => true,
                _ => false,
            };
            if !references_output {
                self.expr_attribute(order.expr().node_ref(), scopes);
            }
        }
    }

    fn source_collect<'t>(
        &mut self,
        source: Source<'t>,
        from: &mut FromClause<'t>,
        scopes: &mut Vec<Scope>,
    ) {
        match source {
            Source::Table(table) => {
                let key =
                    self.table_key(table.schema().map(EcoString::from).as_ref(), table.name());
                self.table_ensure(&key);
                let alias = table.alias().unwrap_or_else(|| table.name()).clone();
                from.scope.entries.push(ScopeEntry {
                    alias,
                    key: Some(key),
                });
            }
            Source::Subquery(subquery) => {
                from.scope.has_derived = true;
                if let Some(alias) = subquery.alias() {
                    from.scope.entries.push(ScopeEntry {
                        alias: alias.clone(),
                        key: None,
                    });
                }
                self.query_walk(subquery.query(), scopes);
            }
            Source::CteRef(cte_ref) => {
                from.scope.has_derived = true;
                let alias = cte_ref
                    .alias()
                    .unwrap_or_else(|| cte_ref.cte_name())
                    .clone();
                from.scope.entries.push(ScopeEntry { alias, key: None });
            }
            Source::Join(join) => {
                self.source_collect(join.left(), from, scopes);
                self.source_collect(join.right(), from, scopes);
                if let JoinQual::On(qual) = join.qual() {
                    from.join_quals.push(qual);
                }
            }
        }
    }

    /// Attribute the columns of an expression: its nested subqueries walked
    /// first with their own scopes, then the columns at its own level.
    fn expr_attribute(&mut self, expr: NodeRef<'_>, scopes: &mut Vec<Scope>) {
        let (nested, own) = levels(expr);
        for subquery in nested {
            self.query_walk(Query::from_ref(subquery), scopes);
        }
        for (table, column) in own.into_iter().filter_map(column_of) {
            self.column_attribute(table, column, scopes);
        }
    }

    /// Record literal-comparison type evidence from WHERE/JOIN predicates.
    fn where_infer(&mut self, expr: Where<'_>, scopes: &[Scope]) {
        match expr.kind() {
            WhereKind::Binary(binary) => self.binary_infer(binary, scopes),
            WhereKind::Multi(multi) => self.multi_infer(multi, scopes),
            WhereKind::Unary(unary) => self.where_infer(unary.expr(), scopes),
            WhereKind::Scalar(_) | WhereKind::Subquery(_) => {}
        }
    }

    /// `col op literal` on either side is evidence; anything else recurses.
    fn binary_infer(&mut self, binary: Binary<'_>, scopes: &[Scope]) {
        if let Some(((table, column), literal)) = comparison_evidence(binary) {
            self.evidence_note(table, column, literal, scopes);
            return;
        }
        self.where_infer(binary.lexpr(), scopes);
        self.where_infer(binary.rexpr(), scopes);
    }

    /// `col IN (v1, v2, ...)` and `col BETWEEN a AND b` with every value a
    /// literal are evidence for each value; anything else recurses.
    fn multi_infer(&mut self, multi: Multi<'_>, scopes: &[Scope]) {
        let mut operands = multi.exprs();
        let subject = operands.next().and_then(where_column);
        let values = operands.len();
        let literals: Vec<&LiteralValue> = operands.filter_map(where_literal).collect();
        let every_value_literal = values > 0 && literals.len() == values;
        if let Some((table, column)) = subject
            && every_value_literal
        {
            for literal in literals {
                self.evidence_note(table, column, literal, scopes);
            }
            return;
        }
        for inner in multi.exprs() {
            self.where_infer(inner, scopes);
        }
    }
}

/// The column and literal of a `col op literal` or `literal op col`
/// comparison.
fn comparison_evidence(binary: Binary<'_>) -> Option<(ColumnName<'_>, &LiteralValue)> {
    if !binary.op().is_comparison() {
        return None;
    }
    let sides = [
        (where_column(binary.lexpr()), where_literal(binary.rexpr())),
        (where_column(binary.rexpr()), where_literal(binary.lexpr())),
    ];
    sides
        .into_iter()
        .find_map(|(column, literal)| Some((column?, literal?)))
}

/// The positions under `expr` in pre-order, split into the roots of the
/// nested subqueries (each skipped as a whole) and the positions of
/// `expr`'s own level.
fn levels(expr: NodeRef<'_>) -> (Vec<NodeRef<'_>>, Vec<NodeRef<'_>>) {
    let mut nested = Vec::new();
    let mut own = Vec::new();
    let start = u32::try_from(expr.id().index()).unwrap_or(u32::MAX);
    let mut next = start;
    while next < expr.end() {
        let position = expr.at_position(NodeId::from_index(next));
        if next != start && matches!(position.kind(), NodeKind::Query { .. }) {
            nested.push(position);
            next = position.end();
        } else {
            own.push(position);
            next += 1;
        }
    }
    (nested, own)
}

fn where_column(expr: Where<'_>) -> Option<ColumnName<'_>> {
    let WhereKind::Scalar(scalar) = expr.kind() else {
        return None;
    };
    match scalar.kind() {
        ScalarKind::Column(column) => Some((column.table(), column.column())),
        _ => None,
    }
}

fn where_literal(expr: Where<'_>) -> Option<&LiteralValue> {
    let WhereKind::Scalar(scalar) = expr.kind() else {
        return None;
    };
    match scalar.kind() {
        ScalarKind::Literal(literal) => Some(literal.value()),
        _ => None,
    }
}
