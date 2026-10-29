//! Render a rope tree as SQL: one walk over views, with a context that
//! decides what the references in it render as, so the plain tree and a
//! bound tree share it (ADR-057).

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use super::tree::QueryTree;
use super::view::{
    Binary, Body, Case, Column, Conjuncts, Cte, CteRef, Frame, FrameBound, Function, Join,
    JoinQual, Multi, OrderBy, Query, Run, Scalar, ScalarKind, Select, SelectItem, Source, Star,
    Subquery, Table, Unary, Values, Where, WhereKind, Window,
};
use crate::query::ast::{
    BinaryOp, Clause, CteMaterialization, Deparse, LiteralValue, MultiOp, SubLinkType, UnaryOp,
};
use crate::query::cast::cast_target_deparse;

/// What the walk asks its context at each reference. Every hook defaults
/// to the node as written.
pub trait DeparseContext: Sized {
    /// A table reference in FROM, with its leading space and alias.
    fn table_deparse(&mut self, table: Table<'_>, buf: &mut String) {
        table_plain_deparse(table, buf);
    }

    fn column_deparse(&mut self, column: Column<'_>, buf: &mut String) {
        column_plain_deparse(column, buf);
    }

    /// A select-list star. An expansion into several columns separates
    /// them with `, ` and takes no leading space.
    fn star_deparse(&mut self, star: Star<'_>, buf: &mut String) {
        star_plain_deparse(star, buf);
    }

    /// A CTE reference in FROM, with its leading space and alias.
    fn cte_ref_deparse(&mut self, cte_ref: CteRef<'_>, buf: &mut String) {
        cte_ref_plain_deparse(cte_ref, buf);
    }

    /// Whether a query's `WITH` clause renders.
    fn with_clause_renders(&self) -> bool {
        true
    }

    /// The columns a `NATURAL` join merges: `None` keeps the keyword as
    /// written, an empty list renders `CROSS JOIN`, otherwise `USING (…)`.
    fn natural_join_columns(&mut self, _join: Join<'_>) -> Option<Vec<EcoString>> {
        None
    }

    /// The name of a function call, before its parenthesised arguments.
    fn function_name_deparse(&mut self, function: Function<'_>, buf: &mut String) {
        function_name_plain_deparse(function, buf);
    }

    fn literal_deparse(&mut self, literal: &LiteralValue, buf: &mut String) {
        literal.deparse(buf);
    }

    /// Whether a top-level conjunct of a WHERE, HAVING or ON renders. A
    /// clause left with none is omitted, or `ON TRUE`.
    fn conjunct_renders(&self, _conjunct: Where<'_>) -> bool {
        true
    }

    fn clause_enter(&mut self, _clause: Clause) {}

    fn clause_leave(&mut self, _clause: Clause) {}
}

/// The context that renders a tree as written.
pub struct Plain;

impl DeparseContext for Plain {}

impl QueryTree {
    /// The tree as written, appended to `buf`.
    pub fn deparse_into(&self, buf: &mut String) {
        query_deparse_with(self.root(), &mut Plain, buf);
    }

    pub fn sql(&self) -> String {
        let mut buf = String::with_capacity(256);
        self.deparse_into(&mut buf);
        buf
    }
}

// --- plain renderings of references ---------------------------------------

/// ` schema.name alias(columns)`.
pub fn table_plain_deparse(table: Table<'_>, buf: &mut String) {
    buf.push(' ');
    if let Some(schema) = table.schema() {
        schema.deparse(buf);
        buf.push('.');
    }
    table.name().deparse(buf);
    table_alias_deparse(table, buf);
}

/// ` alias(columns)` when the table has an alias.
pub fn table_alias_deparse(table: Table<'_>, buf: &mut String) {
    if let Some(alias) = table.alias() {
        buf.push(' ');
        alias_deparse(alias, table.alias_columns(), buf);
    }
}

/// `name(c1, c2)`: an alias with its column list when it has one.
fn alias_deparse(name: &EcoString, columns: Run<'_, super::view::Name<'_>>, buf: &mut String) {
    name.deparse(buf);
    if columns.len() > 0 {
        buf.push('(');
        names_deparse(columns, ", ", buf);
        buf.push(')');
    }
}

/// Names as identifiers, `separator` between them.
pub fn names_deparse(names: Run<'_, super::view::Name<'_>>, separator: &str, buf: &mut String) {
    let mut sep = "";
    for name in names {
        buf.push_str(sep);
        name.text().deparse(buf);
        sep = separator;
    }
}

pub fn column_plain_deparse(column: Column<'_>, buf: &mut String) {
    if let Some(table) = column.table() {
        table.deparse(buf);
        buf.push('.');
    }
    column.column().deparse(buf);
}

pub fn star_plain_deparse(star: Star<'_>, buf: &mut String) {
    if let Some(qualifier) = star.qualifier() {
        buf.push_str(qualifier);
        buf.push('.');
    }
    buf.push('*');
}

pub fn cte_ref_plain_deparse(cte_ref: CteRef<'_>, buf: &mut String) {
    buf.push(' ');
    cte_ref.cte_name().deparse(buf);
    if let Some(alias) = cte_ref.alias() {
        buf.push(' ');
        alias_deparse(alias, cte_ref.alias_columns(), buf);
    }
}

/// `schema.NAME`: the name upper-cased.
pub fn function_name_plain_deparse(function: Function<'_>, buf: &mut String) {
    if let Some(schema) = function.schema() {
        buf.push_str(schema);
        buf.push('.');
    }
    buf.push_str(&function.name().to_uppercase());
}

// --- the walk --------------------------------------------------------------

pub fn query_deparse_with<C: DeparseContext>(query: Query<'_>, cx: &mut C, buf: &mut String) {
    query_unlimited_deparse_with(query, cx, buf);
    if query.has_limit() {
        cx.clause_enter(Clause::Limit);
        if let Some(count) = query.limit_count() {
            buf.push_str(" LIMIT ");
            cx.literal_deparse(count.value(), buf);
        }
        if let Some(offset) = query.limit_offset() {
            buf.push_str(" OFFSET ");
            cx.literal_deparse(offset.value(), buf);
        }
        cx.clause_leave(Clause::Limit);
    }
}

/// The query without its LIMIT/OFFSET.
pub fn query_unlimited_deparse_with<C: DeparseContext>(
    query: Query<'_>,
    cx: &mut C,
    buf: &mut String,
) {
    let ctes = query.ctes();
    if ctes.len() > 0 && cx.with_clause_renders() {
        cx.clause_enter(Clause::With);
        buf.push_str("WITH ");
        let mut sep = "";
        for cte in ctes {
            buf.push_str(sep);
            cte_deparse_with(cte, cx, buf);
            sep = ", ";
        }
        buf.push(' ');
        cx.clause_leave(Clause::With);
    }
    body_deparse_with(query.body(), cx, buf);
    let order_by = query.order_by();
    if order_by.len() > 0 {
        cx.clause_enter(Clause::OrderBy);
        buf.push_str(" ORDER BY ");
        order_by_list_deparse_with(order_by, cx, buf);
        cx.clause_leave(Clause::OrderBy);
    }
}

fn cte_deparse_with<C: DeparseContext>(cte: Cte<'_>, cx: &mut C, buf: &mut String) {
    cte.name().deparse(buf);
    let aliases = cte.column_aliases();
    if aliases.len() > 0 {
        buf.push('(');
        names_deparse(aliases, ", ", buf);
        buf.push(')');
    }
    buf.push_str(" AS ");
    match cte.materialization() {
        CteMaterialization::Default => {}
        CteMaterialization::Materialized => buf.push_str("MATERIALIZED "),
        CteMaterialization::NotMaterialized => buf.push_str("NOT MATERIALIZED "),
    }
    buf.push('(');
    query_deparse_with(cte.query(), cx, buf);
    buf.push(')');
}

fn body_deparse_with<C: DeparseContext>(body: Body<'_>, cx: &mut C, buf: &mut String) {
    match body {
        Body::Select(select) => select_deparse_with(select, cx, buf),
        Body::Values(values) => values_deparse_with(values, cx, buf),
        Body::SetOp(set_op) => {
            query_deparse_with(set_op.left(), cx, buf);
            buf.push(' ');
            set_op.op().deparse(buf);
            if set_op.all() {
                buf.push_str(" ALL");
            }
            buf.push(' ');
            query_deparse_with(set_op.right(), cx, buf);
        }
    }
}

pub fn values_deparse_with<C: DeparseContext>(values: Values<'_>, cx: &mut C, buf: &mut String) {
    cx.clause_enter(Clause::Values);
    buf.push_str("VALUES ");
    let mut row_sep = "";
    buf.push('(');
    for row in values.rows() {
        buf.push_str(row_sep);
        let mut sep = "";
        for literal in row.literals() {
            buf.push_str(sep);
            cx.literal_deparse(literal.value(), buf);
            sep = ", ";
        }
        row_sep = "), (";
    }
    buf.push(')');
    cx.clause_leave(Clause::Values);
}

pub fn select_deparse_with<C: DeparseContext>(select: Select<'_>, cx: &mut C, buf: &mut String) {
    buf.push_str("SELECT");
    if select.distinct() {
        buf.push_str(" DISTINCT");
    }
    cx.clause_enter(Clause::SelectList);
    select_items_deparse_with(select, cx, buf);
    cx.clause_leave(Clause::SelectList);
    from_deparse_with(select.from(), cx, buf);
    where_clause_deparse_with(select.where_clause(), Clause::Where, " WHERE ", cx, buf);
    group_by_deparse_with(select.group_by(), cx, buf);
    where_clause_deparse_with(select.having(), Clause::Having, " HAVING ", cx, buf);
}

/// ` FROM a, b` when the select has sources.
pub fn from_deparse_with<C: DeparseContext>(
    from: Run<'_, Source<'_>>,
    cx: &mut C,
    buf: &mut String,
) {
    if from.len() == 0 {
        return;
    }
    cx.clause_enter(Clause::From);
    buf.push_str(" FROM");
    let mut sep = "";
    for source in from {
        buf.push_str(sep);
        source_deparse_with(source, cx, buf);
        sep = ",";
    }
    cx.clause_leave(Clause::From);
}

/// `keyword` then `expr`, when present, reported to the context as `clause`.
pub fn where_clause_deparse_with<C: DeparseContext>(
    expr: Option<Where<'_>>,
    clause: Clause,
    keyword: &str,
    cx: &mut C,
    buf: &mut String,
) {
    let Some(expr) = expr else {
        return;
    };
    let kept = conjuncts_kept(expr, cx);
    if kept.is_empty() {
        return;
    }
    cx.clause_enter(clause);
    buf.push_str(keyword);
    conjuncts_deparse_with(&kept, cx, buf);
    cx.clause_leave(clause);
}

/// The top-level conjuncts of `expr` the context renders.
fn conjuncts_kept<'t, C: DeparseContext>(expr: Where<'t>, cx: &C) -> Conjuncts<'t> {
    expr.conjuncts()
        .into_iter()
        .filter(|conjunct| cx.conjunct_renders(*conjunct))
        .collect()
}

/// `conjuncts` joined by `AND`, as the AND they were split from renders;
/// one alone renders as the whole predicate it is.
fn conjuncts_deparse_with<C: DeparseContext>(
    conjuncts: &[Where<'_>],
    cx: &mut C,
    buf: &mut String,
) {
    if let [conjunct] = conjuncts {
        return where_deparse_with(*conjunct, cx, buf);
    }
    let mut sep = "";
    for &conjunct in conjuncts {
        buf.push_str(sep);
        where_parenthesized_deparse_with(
            conjunct,
            binary_child_needs_parens(BinaryOp::And, conjunct),
            cx,
            buf,
        );
        sep = " AND ";
    }
}

/// ` GROUP BY a, b` when the select groups.
pub fn group_by_deparse_with<C: DeparseContext>(
    group_by: Run<'_, Column<'_>>,
    cx: &mut C,
    buf: &mut String,
) {
    if group_by.len() == 0 {
        return;
    }
    cx.clause_enter(Clause::GroupBy);
    buf.push_str(" GROUP BY ");
    let mut sep = "";
    for column in group_by {
        buf.push_str(sep);
        cx.column_deparse(column, buf);
        sep = ", ";
    }
    cx.clause_leave(Clause::GroupBy);
}

/// `SELECT <projection>` over a branch's FROM, WHERE, GROUP BY and
/// HAVING: the select list, ORDER BY and LIMIT never render, so a CDC row
/// standing in for a table is evaluated for membership alone.
pub fn branch_select_into<C: DeparseContext>(
    cx: &mut C,
    buf: &mut String,
    select: Select<'_>,
    projection: &str,
) {
    buf.push_str("SELECT ");
    buf.push_str(projection);
    from_deparse_with(select.from(), cx, buf);
    where_clause_deparse_with(select.where_clause(), Clause::Where, " WHERE ", cx, buf);
    group_by_deparse_with(select.group_by(), cx, buf);
    where_clause_deparse_with(select.having(), Clause::Having, " HAVING ", cx, buf);
}

/// The select list; an item that renders to nothing (an empty star
/// expansion) is dropped with its separator.
fn select_items_deparse_with<C: DeparseContext>(select: Select<'_>, cx: &mut C, buf: &mut String) {
    let Some(items) = select.items() else {
        buf.push(' ');
        return;
    };
    let mut sep = "";
    for item in items {
        let start = buf.len();
        buf.push_str(sep);
        buf.push(' ');
        match item {
            SelectItem::Expr(expr) => {
                scalar_deparse_with(expr.expr(), cx, buf);
                if let Some(alias) = expr.alias() {
                    buf.push_str(" AS ");
                    alias.deparse(buf);
                }
            }
            SelectItem::Star(star) => cx.star_deparse(star, buf),
        }
        if buf.len() == start + sep.len() + 1 {
            buf.truncate(start);
        } else {
            sep = ",";
        }
    }
}

fn order_by_list_deparse_with<C: DeparseContext>(
    order_by: Run<'_, OrderBy<'_>>,
    cx: &mut C,
    buf: &mut String,
) {
    let mut sep = "";
    for clause in order_by {
        buf.push_str(sep);
        order_by_deparse_with(clause, cx, buf);
        sep = ", ";
    }
}

pub fn order_by_deparse_with<C: DeparseContext>(clause: OrderBy<'_>, cx: &mut C, buf: &mut String) {
    scalar_deparse_with(clause.expr(), cx, buf);
    buf.push(' ');
    clause.direction().deparse(buf);
    clause.null_order().deparse(buf);
}

fn source_deparse_with<C: DeparseContext>(source: Source<'_>, cx: &mut C, buf: &mut String) {
    match source {
        Source::Table(table) => cx.table_deparse(table, buf),
        Source::Subquery(subquery) => subquery_deparse_with(subquery, cx, buf),
        Source::Join(join) => join_deparse_with(join, cx, buf),
        Source::CteRef(cte_ref) => cx.cte_ref_deparse(cte_ref, buf),
    }
}

fn subquery_deparse_with<C: DeparseContext>(subquery: Subquery<'_>, cx: &mut C, buf: &mut String) {
    buf.push(' ');
    if subquery.lateral() {
        buf.push_str("LATERAL ");
    }
    buf.push('(');
    query_deparse_with(subquery.query(), cx, buf);
    buf.push(')');
    if let Some(alias) = subquery.alias() {
        buf.push(' ');
        alias_deparse(alias, subquery.alias_columns(), buf);
    }
}

/// How a join's qualification renders once the context has had its say
/// on a `NATURAL` join.
enum JoinQualText<'t> {
    On(Where<'t>),
    Using(Run<'t, super::view::Name<'t>>),
    UsingNames(Vec<EcoString>),
    Natural,
    Cross,
}

fn join_deparse_with<C: DeparseContext>(join: Join<'_>, cx: &mut C, buf: &mut String) {
    source_deparse_with(join.left(), cx, buf);
    let qual = match join.qual() {
        JoinQual::Natural => match cx.natural_join_columns(join) {
            Some(columns) if columns.is_empty() => JoinQualText::Cross,
            Some(columns) => JoinQualText::UsingNames(columns),
            None => JoinQualText::Natural,
        },
        JoinQual::On(condition) => JoinQualText::On(condition),
        JoinQual::Using(names) => JoinQualText::Using(names),
        JoinQual::Cross => JoinQualText::Cross,
    };
    match &qual {
        JoinQualText::Cross => buf.push_str(" CROSS JOIN"),
        JoinQualText::Natural => {
            buf.push_str(" NATURAL");
            buf.push_str(join.join_type().join_keyword());
        }
        JoinQualText::On(_) | JoinQualText::Using(_) | JoinQualText::UsingNames(_) => {
            buf.push_str(join.join_type().join_keyword());
        }
    }
    // A join on the right binds its own qualification before the outer
    // one does; the grammar allows it bare, the parentheses make it read.
    if let Source::Join(inner) = join.right() {
        let mut nested = String::new();
        join_deparse_with(inner, cx, &mut nested);
        buf.push_str(" (");
        buf.push_str(nested.trim_start());
        buf.push(')');
    } else {
        source_deparse_with(join.right(), cx, buf);
    }
    match qual {
        JoinQualText::On(condition) => {
            cx.clause_enter(Clause::JoinOn);
            buf.push_str(" ON ");
            let kept = conjuncts_kept(condition, cx);
            if kept.is_empty() {
                buf.push_str("TRUE");
            } else {
                conjuncts_deparse_with(&kept, cx, buf);
            }
            cx.clause_leave(Clause::JoinOn);
        }
        JoinQualText::Using(names) => {
            buf.push_str(" USING (");
            let mut sep = "";
            for name in names {
                buf.push_str(sep);
                buf.push_str(name.text());
                sep = ", ";
            }
            buf.push(')');
        }
        JoinQualText::UsingNames(names) => {
            buf.push_str(" USING (");
            let mut sep = "";
            for name in &names {
                buf.push_str(sep);
                buf.push_str(name);
                sep = ", ";
            }
            buf.push(')');
        }
        JoinQualText::Natural | JoinQualText::Cross => {}
    }
}

// --- expressions -----------------------------------------------------------

pub fn where_deparse_with<C: DeparseContext>(expr: Where<'_>, cx: &mut C, buf: &mut String) {
    match expr.kind() {
        WhereKind::Scalar(scalar) => scalar_deparse_with(scalar, cx, buf),
        WhereKind::Unary(unary) => unary_deparse_with(unary, cx, buf),
        WhereKind::Binary(binary) => binary_deparse_with(binary, cx, buf),
        WhereKind::Multi(multi) => multi_deparse_with(multi, cx, buf),
        WhereKind::Subquery(subquery) => {
            let opener = match (subquery.sublink_type(), subquery.test_expr()) {
                (SubLinkType::Exists, _) => "EXISTS (",
                (SubLinkType::Any, Some(test)) => {
                    scalar_deparse_with(test, cx, buf);
                    " IN ("
                }
                (SubLinkType::All, Some(test)) => {
                    scalar_deparse_with(test, cx, buf);
                    " <> ALL ("
                }
                (SubLinkType::All, None) => "ALL (",
                (SubLinkType::Any | SubLinkType::Expr, _) => "(",
            };
            buf.push_str(opener);
            query_deparse_with(subquery.query(), cx, buf);
            buf.push(')');
        }
    }
}

fn unary_deparse_with<C: DeparseContext>(unary: Unary<'_>, cx: &mut C, buf: &mut String) {
    let op = unary.op();
    let operand = unary.expr();
    match op {
        UnaryOp::Not => {
            let needs_parens = matches!(
                operand.kind(),
                WhereKind::Binary(child) if child.op().is_logical()
            );
            op.deparse(buf);
            buf.push(' ');
            where_parenthesized_deparse_with(operand, needs_parens, cx, buf);
        }
        _ => {
            where_deparse_with(operand, cx, buf);
            buf.push(' ');
            op.deparse(buf);
        }
    }
}

fn binary_deparse_with<C: DeparseContext>(binary: Binary<'_>, cx: &mut C, buf: &mut String) {
    let op = binary.op();
    let (lexpr, rexpr) = (binary.lexpr(), binary.rexpr());
    where_parenthesized_deparse_with(lexpr, binary_child_needs_parens(op, lexpr), cx, buf);
    buf.push(' ');
    op.deparse(buf);
    buf.push(' ');
    where_parenthesized_deparse_with(rexpr, binary_child_needs_parens(op, rexpr), cx, buf);
}

/// An `OR` under an `AND` is parenthesised; nothing else is.
fn binary_child_needs_parens(op: BinaryOp, child: Where<'_>) -> bool {
    match child.kind() {
        WhereKind::Binary(child) => matches!((op, child.op()), (BinaryOp::And, BinaryOp::Or)),
        _ => false,
    }
}

fn where_parenthesized_deparse_with<C: DeparseContext>(
    expr: Where<'_>,
    parens: bool,
    cx: &mut C,
    buf: &mut String,
) {
    if parens {
        buf.push('(');
    }
    where_deparse_with(expr, cx, buf);
    if parens {
        buf.push(')');
    }
}

fn multi_deparse_with<C: DeparseContext>(multi: Multi<'_>, cx: &mut C, buf: &mut String) {
    let mut exprs = multi.exprs();
    let Some(first) = exprs.next() else {
        return;
    };
    where_deparse_with(first, cx, buf);
    let op = multi.op();
    match op {
        MultiOp::In => buf.push_str(" IN ("),
        MultiOp::NotIn => buf.push_str(" NOT IN ("),
        MultiOp::Between
        | MultiOp::NotBetween
        | MultiOp::BetweenSymmetric
        | MultiOp::NotBetweenSymmetric => {
            buf.push(' ');
            op.deparse(buf);
            buf.push(' ');
            let mut sep = "";
            for expr in exprs {
                buf.push_str(sep);
                where_deparse_with(expr, cx, buf);
                sep = " AND ";
            }
            return;
        }
        MultiOp::Any { .. } | MultiOp::All { .. } => {
            buf.push(' ');
            op.deparse(buf);
            buf.push_str(" (");
        }
    }
    let mut sep = "";
    for expr in exprs {
        buf.push_str(sep);
        where_deparse_with(expr, cx, buf);
        sep = ", ";
    }
    buf.push(')');
}

pub fn scalar_deparse_with<C: DeparseContext>(expr: Scalar<'_>, cx: &mut C, buf: &mut String) {
    match expr.kind() {
        ScalarKind::Column(column) => cx.column_deparse(column, buf),
        ScalarKind::Function(function) => function_deparse_with(function, cx, buf),
        ScalarKind::Literal(literal) => cx.literal_deparse(literal.value(), buf),
        ScalarKind::Case(case) => case_deparse_with(case, cx, buf),
        ScalarKind::Arithmetic(arithmetic) => {
            buf.push('(');
            scalar_deparse_with(arithmetic.left(), cx, buf);
            buf.push(' ');
            buf.push_str(arithmetic.op().as_ref());
            buf.push(' ');
            scalar_deparse_with(arithmetic.right(), cx, buf);
            buf.push(')');
        }
        ScalarKind::Subquery(subquery) => {
            buf.push('(');
            query_deparse_with(subquery.query(), cx, buf);
            buf.push(')');
        }
        ScalarKind::Array(array) => {
            buf.push_str("ARRAY[");
            scalar_list_deparse_with(array.elements(), ", ", cx, buf);
            buf.push(']');
        }
        ScalarKind::TypeCast(cast) => {
            buf.push('(');
            scalar_deparse_with(cast.expr(), cx, buf);
            buf.push_str(")::");
            buf.push_str(cast_target_deparse(cast.target()));
        }
    }
}

fn scalar_list_deparse_with<C: DeparseContext>(
    exprs: Run<'_, Scalar<'_>>,
    separator: &str,
    cx: &mut C,
    buf: &mut String,
) {
    let mut sep = "";
    for expr in exprs {
        buf.push_str(sep);
        scalar_deparse_with(expr, cx, buf);
        sep = separator;
    }
}

fn case_deparse_with<C: DeparseContext>(case: Case<'_>, cx: &mut C, buf: &mut String) {
    buf.push_str("CASE");
    if let Some(arg) = case.arg() {
        buf.push(' ');
        scalar_deparse_with(arg, cx, buf);
    }
    for when in case.whens() {
        buf.push_str(" WHEN ");
        where_deparse_with(when.condition(), cx, buf);
        buf.push_str(" THEN ");
        scalar_deparse_with(when.result(), cx, buf);
    }
    if let Some(default) = case.default() {
        buf.push_str(" ELSE ");
        scalar_deparse_with(default, cx, buf);
    }
    buf.push_str(" END");
}

fn function_deparse_with<C: DeparseContext>(function: Function<'_>, cx: &mut C, buf: &mut String) {
    cx.function_name_deparse(function, buf);
    buf.push('(');
    if function.agg_distinct() {
        buf.push_str("DISTINCT ");
    }
    if function.agg_star() {
        buf.push('*');
    } else {
        scalar_list_deparse_with(function.args(), ", ", cx, buf);
    }
    let agg_order = function.agg_order();
    if agg_order.len() > 0 {
        buf.push_str(" ORDER BY ");
        order_by_list_deparse_with(agg_order, cx, buf);
    }
    buf.push(')');
    if let Some(filter) = function.filter() {
        buf.push_str(" FILTER (WHERE ");
        where_deparse_with(filter, cx, buf);
        buf.push(')');
    }
    if let Some(window) = function.over() {
        buf.push_str(" OVER ");
        window_deparse_with(window, cx, buf);
    }
}

fn window_deparse_with<C: DeparseContext>(window: Window<'_>, cx: &mut C, buf: &mut String) {
    buf.push('(');
    let mut parts = 0u32;
    let partition_by = window.partition_by();
    if partition_by.len() > 0 {
        window_part_separate(&mut parts, buf);
        buf.push_str("PARTITION BY ");
        scalar_list_deparse_with(partition_by, ", ", cx, buf);
    }
    let order_by = window.order_by();
    if order_by.len() > 0 {
        window_part_separate(&mut parts, buf);
        buf.push_str("ORDER BY ");
        order_by_list_deparse_with(order_by, cx, buf);
    }
    if let Some(frame) = window.frame() {
        window_part_separate(&mut parts, buf);
        frame_deparse_with(frame, cx, buf);
    }
    buf.push(')');
}

/// A space before every window part but the first.
fn window_part_separate(parts: &mut u32, buf: &mut String) {
    if *parts > 0 {
        buf.push(' ');
    }
    *parts += 1;
}

fn frame_deparse_with<C: DeparseContext>(frame: Frame<'_>, cx: &mut C, buf: &mut String) {
    frame.mode().deparse(buf);
    buf.push_str(" BETWEEN ");
    frame_bound_deparse_with(frame.start(), cx, buf);
    buf.push_str(" AND ");
    frame_bound_deparse_with(frame.end(), cx, buf);
    frame.exclusion().deparse(buf);
}

fn frame_bound_deparse_with<C: DeparseContext>(
    bound: FrameBound<'_>,
    cx: &mut C,
    buf: &mut String,
) {
    match bound {
        FrameBound::UnboundedPreceding => buf.push_str("UNBOUNDED PRECEDING"),
        FrameBound::CurrentRow => buf.push_str("CURRENT ROW"),
        FrameBound::UnboundedFollowing => buf.push_str("UNBOUNDED FOLLOWING"),
        FrameBound::OffsetPreceding(offset) => {
            scalar_deparse_with(offset, cx, buf);
            buf.push_str(" PRECEDING");
        }
        FrameBound::OffsetFollowing(offset) => {
            scalar_deparse_with(offset, cx, buf);
            buf.push_str(" FOLLOWING");
        }
    }
}
