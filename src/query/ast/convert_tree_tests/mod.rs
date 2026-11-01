//! The converter's tests, read through the rope's views: what each SQL
//! shape converts to (`tables`, `subqueries`, `clauses`, `expressions`), what it renders back to
//! and what is rejected (`render`). The helpers here are shared by all.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

mod clauses;
mod expressions;
mod index;
mod render;
mod subqueries;
mod tables;

use std::collections::hash_map::DefaultHasher;
use std::hash::Hasher;

pub(super) use ecow::EcoString;
pub(super) use ordered_float::NotNan;

pub(super) use crate::cache::{SubqueryKind, UpdateQuerySource};
pub(super) use crate::query::ast::rope::{
    Body, Function, Join, JoinQual, NodeKind, QueryTree, Scalar, ScalarKind, Select, SelectItem,
    Source, Table, View, Where, WhereKind,
};
pub(super) use crate::query::ast::{
    ArithmeticOp, BinaryOp, CteMaterialization, Deparse, JoinType, LiteralValue, MultiOp,
    OrderDirection, SetOpType, SubLinkType, UnaryOp, query_tree_parse,
};
pub(super) use crate::query::cast::cast_target_deparse;
pub(super) use crate::query::update::select_branches_with_source;

pub(super) fn parse(sql: &str) -> QueryTree {
    query_tree_parse(sql).unwrap_or_else(|e| panic!("{sql}: {e}"))
}

/// The SELECT a plain query's body is.
pub(super) fn select_of(tree: &QueryTree) -> Select<'_> {
    match tree.root().body() {
        Body::Select(select) => select,
        other => panic!("expected a SELECT body, got {:?}", other_kind(other)),
    }
}

pub(super) fn other_kind(body: Body<'_>) -> &'static str {
    match body {
        Body::Select(_) => "a SELECT",
        Body::Values(_) => "VALUES",
        Body::SetOp(_) => "a set operation",
    }
}

/// The select list, which the test expects to be explicit items.
pub(super) fn items(select: Select<'_>) -> Vec<SelectItem<'_>> {
    select.items().expect("explicit select items").collect()
}

/// The expression of a non-star select item.
pub(super) fn expr_of(item: SelectItem<'_>) -> Scalar<'_> {
    match item {
        SelectItem::Expr(expr) => expr.expr(),
        SelectItem::Star(_) => panic!("expected an expression item, got a star"),
    }
}

pub(super) fn alias_of(item: SelectItem<'_>) -> Option<&EcoString> {
    match item {
        SelectItem::Expr(expr) => expr.alias(),
        SelectItem::Star(_) => None,
    }
}

pub(super) fn only_table(select: Select<'_>) -> Table<'_> {
    let mut from = select.from();
    match (from.next(), from.next()) {
        (Some(Source::Table(table)), None) => table,
        _ => panic!("expected a single table source"),
    }
}

pub(super) fn only_join(select: Select<'_>) -> Join<'_> {
    let mut from = select.from();
    match (from.next(), from.next()) {
        (Some(Source::Join(join)), None) => join,
        _ => panic!("expected a single join"),
    }
}

/// The (qualifier, column) of every select-list column reference.
pub(super) fn column_refs(select: Select<'_>) -> Vec<(Option<&str>, &str)> {
    items(select)
        .into_iter()
        .map(|item| match expr_of(item).kind() {
            ScalarKind::Column(column) => (
                column.table().map(EcoString::as_str),
                column.column().as_str(),
            ),
            _ => panic!("expected a column reference"),
        })
        .collect()
}

/// The function call `sql` selects as its only item.
pub(super) fn only_function(sql: &str) -> (QueryTree, impl Fn(&QueryTree) -> Function<'_>) {
    (parse(sql), |tree: &QueryTree| {
        let item_list = items(select_of(tree));
        let [item] = item_list.as_slice() else {
            panic!("expected exactly one item");
        };
        match expr_of(*item).kind() {
            ScalarKind::Function(function) => function,
            _ => panic!("expected a function"),
        }
    })
}

pub(super) fn where_of(tree: &QueryTree) -> Where<'_> {
    select_of(tree).where_clause().expect("a WHERE clause")
}

/// Every node kind in pre-order.
pub(super) fn kinds(tree: &QueryTree) -> impl Iterator<Item = &NodeKind> {
    tree.nodes().map(|node| &node.kind)
}

/// The (schema, name, alias) of every base table in pre-order.
pub(super) fn tables(tree: &QueryTree) -> Vec<(Option<&str>, &str, Option<&str>)> {
    kinds(tree)
        .filter_map(|kind| match kind {
            NodeKind::Table {
                schema,
                name,
                alias,
                ..
            } => Some((
                (!schema.is_empty()).then_some(schema.as_str()),
                name.as_str(),
                (!alias.is_empty()).then_some(alias.as_str()),
            )),
            _ => None,
        })
        .collect()
}

/// The (qualifier, column) of every column reference under `view` in
/// pre-order.
pub(super) fn columns_under<'t>(view: impl View<'t>) -> Vec<(Option<&'t str>, &'t str)> {
    view.node_ref()
        .subtree()
        .filter_map(|position| match position.kind() {
            NodeKind::Column { table, column } => {
                Some((table.as_ref().map(EcoString::as_str), column.as_str()))
            }
            _ => None,
        })
        .collect()
}

pub(super) fn literals_under<'t>(view: impl View<'t>) -> Vec<&'t LiteralValue> {
    view.node_ref()
        .subtree()
        .filter_map(|position| match position.kind() {
            NodeKind::Literal(value) => Some(value),
            _ => None,
        })
        .collect()
}

/// Whether a query node sits under `select`.
pub(super) fn has_subqueries(select: Select<'_>) -> bool {
    select
        .node_ref()
        .subtree()
        .any(|position| matches!(position.kind(), NodeKind::Query { .. }))
}

pub(super) fn single_table(tree: &QueryTree) -> bool {
    let mut from = select_of(tree).from();
    matches!((from.next(), from.next()), (Some(Source::Table(_)), None))
}

pub(super) fn session_hash(tree: &QueryTree) -> u64 {
    let mut hasher = DefaultHasher::new();
    tree.session_hash(&mut hasher);
    hasher.finish()
}

pub(super) fn directions<'t>(
    clauses: impl Iterator<Item = crate::query::ast::rope::OrderBy<'t>>,
) -> Vec<OrderDirection> {
    clauses.map(|clause| clause.direction()).collect()
}

pub(super) fn branch_count(tree: &QueryTree) -> usize {
    select_branches_with_source(tree.root()).len()
}
