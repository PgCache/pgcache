//! Views on queries, select lists, VALUES and set operations.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use super::{
    Column, Cte, EMPTY_NAME, Literal, Name, NodeRef, OrderBy, Query, Run, Scalar, Select,
    SelectExpr, SetOp, Source, Star, Table, Values, ValuesRow, View, Where, mismatch,
};
use crate::query::ast::rope::node::NodeKind;
use crate::query::ast::{CteMaterialization, NullOrder, OrderDirection, SetOpType};

// --- Query -----------------------------------------------------------------

#[derive(Default, Clone, Copy)]
struct QueryLayout {
    cte_count: u32,
    order_by_count: u32,
    has_limit_count: bool,
    has_limit_offset: bool,
}

impl<'t> Query<'t> {
    fn layout(&self) -> QueryLayout {
        match *self.0.kind() {
            NodeKind::Query {
                cte_count,
                order_by_count,
                has_limit_count,
                has_limit_offset,
            } => QueryLayout {
                cte_count,
                order_by_count,
                has_limit_count,
                has_limit_offset,
            },
            _ => mismatch("query"),
        }
    }

    pub fn ctes(&self) -> Run<'t, Cte<'t>> {
        self.0.run(0, self.layout().cte_count)
    }

    pub fn body(&self) -> Body<'t> {
        self.0.nth(self.layout().cte_count)
    }

    pub fn order_by(&self) -> Run<'t, OrderBy<'t>> {
        let layout = self.layout();
        self.0.run(layout.cte_count + 1, layout.order_by_count)
    }

    pub fn limit_count(&self) -> Option<Literal<'t>> {
        let layout = self.layout();
        layout
            .has_limit_count
            .then(|| self.0.nth(layout.cte_count + 1 + layout.order_by_count))
    }

    pub fn limit_offset(&self) -> Option<Literal<'t>> {
        let layout = self.layout();
        layout.has_limit_offset.then(|| {
            self.0.nth(
                layout.cte_count + 1 + layout.order_by_count + u32::from(layout.has_limit_count),
            )
        })
    }

    pub fn has_limit(&self) -> bool {
        let layout = self.layout();
        layout.has_limit_count || layout.has_limit_offset
    }
}

/// A query's body.
#[derive(Clone, Copy)]
pub enum Body<'t> {
    Select(Select<'t>),
    Values(Values<'t>),
    SetOp(SetOp<'t>),
}

impl<'t> View<'t> for Body<'t> {
    fn from_ref(node: NodeRef<'t>) -> Self {
        match node.kind() {
            NodeKind::Values { .. } => Body::Values(Values(node)),
            NodeKind::SetOp { .. } => Body::SetOp(SetOp(node)),
            NodeKind::Select { .. } => Body::Select(Select(node)),
            _ => {
                debug_assert!(false, "a query body is a select, values or set operation");
                Body::Select(Select(node))
            }
        }
    }

    fn node_ref(&self) -> NodeRef<'t> {
        match self {
            Body::Select(select) => select.0,
            Body::Values(values) => values.0,
            Body::SetOp(set_op) => set_op.0,
        }
    }
}

impl<'t> Cte<'t> {
    fn layout(&self) -> (&'t EcoString, u32, CteMaterialization) {
        match self.0.kind() {
            NodeKind::Cte {
                name,
                alias_count,
                materialization,
            } => (name, *alias_count, *materialization),
            _ => mismatch::<(Option<&EcoString>, u32, Option<CteMaterialization>)>("cte")
                .0
                .map_or((&EMPTY_NAME, 0, CteMaterialization::Default), |name| {
                    (name, 0, CteMaterialization::Default)
                }),
        }
    }

    pub fn name(&self) -> &'t EcoString {
        self.layout().0
    }

    pub fn column_aliases(&self) -> Run<'t, Name<'t>> {
        self.0.run(0, self.layout().1)
    }

    pub fn materialization(&self) -> CteMaterialization {
        self.layout().2
    }

    pub fn query(&self) -> Query<'t> {
        self.0.nth(self.layout().1)
    }
}

// --- Select, Values, SetOp -------------------------------------------------

#[derive(Default, Clone, Copy)]
struct SelectLayout {
    distinct: bool,
    item_count: Option<u32>,
    from_count: u32,
    has_where: bool,
    group_by_count: u32,
    has_having: bool,
}

impl SelectLayout {
    fn items(&self) -> u32 {
        self.item_count.unwrap_or(0)
    }
}

impl<'t> Select<'t> {
    fn layout(&self) -> SelectLayout {
        match *self.0.kind() {
            NodeKind::Select {
                distinct,
                item_count,
                from_count,
                has_where,
                group_by_count,
                has_having,
            } => SelectLayout {
                distinct,
                item_count,
                from_count,
                has_where,
                group_by_count,
                has_having,
            },
            _ => mismatch("select"),
        }
    }

    pub fn distinct(&self) -> bool {
        self.layout().distinct
    }

    /// The select list; `None` when the query has none at all.
    pub fn items(&self) -> Option<Run<'t, SelectItem<'t>>> {
        self.layout().item_count.map(|count| self.0.run(0, count))
    }

    pub fn from(&self) -> Run<'t, Source<'t>> {
        let layout = self.layout();
        self.0.run(layout.items(), layout.from_count)
    }

    pub fn where_clause(&self) -> Option<Where<'t>> {
        let layout = self.layout();
        layout
            .has_where
            .then(|| self.0.nth(layout.items() + layout.from_count))
    }

    pub fn group_by(&self) -> Run<'t, Column<'t>> {
        let layout = self.layout();
        self.0.run(
            layout.items() + layout.from_count + u32::from(layout.has_where),
            layout.group_by_count,
        )
    }

    /// The tables directly in FROM, through joins but not into subqueries
    /// or CTE references.
    pub fn direct_tables(&self) -> Vec<Table<'t>> {
        let mut tables = Vec::new();
        for source in self.from() {
            source_direct_tables_collect(source, &mut tables);
        }
        tables
    }

    pub fn having(&self) -> Option<Where<'t>> {
        let layout = self.layout();
        layout.has_having.then(|| {
            self.0.nth(
                layout.items()
                    + layout.from_count
                    + u32::from(layout.has_where)
                    + layout.group_by_count,
            )
        })
    }
}

/// One entry of a select list.
#[derive(Clone, Copy)]
pub enum SelectItem<'t> {
    Expr(SelectExpr<'t>),
    Star(Star<'t>),
}

impl<'t> View<'t> for SelectItem<'t> {
    fn from_ref(node: NodeRef<'t>) -> Self {
        match node.kind() {
            NodeKind::Star { .. } => SelectItem::Star(Star(node)),
            _ => SelectItem::Expr(SelectExpr(node)),
        }
    }

    fn node_ref(&self) -> NodeRef<'t> {
        match self {
            SelectItem::Expr(expr) => expr.0,
            SelectItem::Star(star) => star.0,
        }
    }
}

impl<'t> SelectExpr<'t> {
    pub fn expr(&self) -> Scalar<'t> {
        self.0.nth(0)
    }

    pub fn alias(&self) -> Option<&'t EcoString> {
        match self.0.kind() {
            NodeKind::SelectExpr { alias } => alias.as_ref(),
            _ => mismatch("select expression"),
        }
    }
}

impl<'t> Star<'t> {
    pub fn qualifier(&self) -> Option<&'t EcoString> {
        match self.0.kind() {
            NodeKind::Star { qualifier } => qualifier.as_ref(),
            _ => mismatch("star"),
        }
    }
}

impl<'t> Values<'t> {
    pub fn rows(&self) -> Run<'t, ValuesRow<'t>> {
        let count = match *self.0.kind() {
            NodeKind::Values { row_count } => row_count,
            _ => mismatch("values"),
        };
        self.0.run(0, count)
    }
}

impl<'t> ValuesRow<'t> {
    pub fn literals(&self) -> Run<'t, Literal<'t>> {
        let width = match *self.0.kind() {
            NodeKind::ValuesRow { width } => width,
            _ => mismatch("values row"),
        };
        self.0.run(0, width)
    }
}

impl<'t> SetOp<'t> {
    fn layout(&self) -> (SetOpType, bool) {
        match *self.0.kind() {
            NodeKind::SetOp { op, all } => (op, all),
            _ => mismatch::<(Option<SetOpType>, bool)>("set operation")
                .0
                .map_or((SetOpType::Union, false), |op| (op, false)),
        }
    }

    pub fn op(&self) -> SetOpType {
        self.layout().0
    }

    pub fn all(&self) -> bool {
        self.layout().1
    }

    pub fn left(&self) -> Query<'t> {
        self.0.nth(0)
    }

    pub fn right(&self) -> Query<'t> {
        self.0.nth(1)
    }
}

impl<'t> OrderBy<'t> {
    fn layout(&self) -> (OrderDirection, NullOrder) {
        match self.0.kind() {
            NodeKind::OrderBy {
                direction,
                null_order,
            } => (direction.clone(), *null_order),
            _ => mismatch::<(Option<OrderDirection>, Option<NullOrder>)>("order by")
                .0
                .map_or((OrderDirection::Asc, NullOrder::Default), |d| {
                    (d, NullOrder::Default)
                }),
        }
    }

    pub fn expr(&self) -> Scalar<'t> {
        self.0.nth(0)
    }

    pub fn direction(&self) -> OrderDirection {
        self.layout().0
    }

    pub fn null_order(&self) -> NullOrder {
        self.layout().1
    }
}

fn source_direct_tables_collect<'t>(source: Source<'t>, tables: &mut Vec<Table<'t>>) {
    match source {
        Source::Table(table) => tables.push(table),
        Source::Join(join) => {
            source_direct_tables_collect(join.left(), tables);
            source_direct_tables_collect(join.right(), tables);
        }
        Source::Subquery(_) | Source::CteRef(_) => {}
    }
}
