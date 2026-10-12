//! The owned MV shape classifier: the oracle for `mv_shape.rs` while the
//! owned tree lasts.

#![allow(clippy::wildcard_enum_match_arm)]

use std::collections::HashSet;

use ecow::EcoString;

use super::mv_shape::ShapeGate;
use crate::query::ast::SetOpType;
use crate::query::ast::{
    QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectColumns, SelectNode, TableSource,
};
use crate::query::resolve::owned::{order_by_position_owned, output_names_owned};
use crate::query::resolve::{Bindings, ColumnTarget};

/// Every top-level ORDER BY item can be served against the MV: for a
/// SELECT, each names an output position; for a set operation, each names
/// an output of the leftmost branch by name.
fn order_by_serve_viable_owned(query: &QueryExpr, bindings: &Bindings) -> bool {
    if query.order_by.is_empty() {
        return true;
    }
    match &query.body {
        QueryBody::Select(select) => query
            .order_by
            .iter()
            .all(|clause| order_by_position_owned(bindings, select, clause).is_some()),
        QueryBody::SetOp(_) => {
            let Some(leftmost) = leftmost_query_owned(query) else {
                return false;
            };
            let names = output_names_owned(bindings, leftmost);
            query.order_by.iter().all(|clause| match &clause.expr {
                ScalarExpr::Column(column)
                    if matches!(bindings.column(column), Some(ColumnTarget::Output)) =>
                {
                    names
                        .iter()
                        .any(|name| name.as_ref() == Some(&column.column))
                }
                ScalarExpr::Column(_)
                | ScalarExpr::Function(_)
                | ScalarExpr::Literal(_)
                | ScalarExpr::Case(_)
                | ScalarExpr::Arithmetic(_)
                | ScalarExpr::Subquery(_)
                | ScalarExpr::Array(_)
                | ScalarExpr::TypeCast { .. } => false,
            })
        }
        QueryBody::Values(_) => false,
    }
}

/// Down the left side of a (possibly nested) set operation to the query
/// whose body is a SELECT; `None` when it is `VALUES`.
fn leftmost_query_owned(query: &QueryExpr) -> Option<&QueryExpr> {
    match &query.body {
        QueryBody::Select(_) => Some(query),
        QueryBody::SetOp(set_op) => leftmost_query_owned(&set_op.left),
        QueryBody::Values(_) => None,
    }
}

/// Classify a query's shape to decide whether it's a materialization
/// candidate. Runs at registration on the decorrelated form, so the shape
/// classified is the shape population and rebuild see.
///
/// Classification is **top-level only** — scalar subqueries in the SELECT
/// list and subqueries in FROM or WHERE don't change the output shape.
///
/// `aggregate_functions` is the set of aggregate function names loaded from
/// `pg_proc` at writer startup (`catalog::aggregate_functions_load`).
pub(super) fn shape_classify_owned(
    tree: &QueryExpr,
    bindings: &Bindings,
    aggregate_functions: &HashSet<EcoString>,
) -> ShapeGate {
    let shape = match &tree.body {
        // VALUES is already literal — nothing to materialize that we don't
        // already emit inline on every serve.
        QueryBody::Values(_) => return ShapeGate::Skip,

        QueryBody::SetOp(set_op) => {
            // UNION ALL is strictly additive — no dedup, no compute. Branches
            // are already cached; concatenating them at serve time is two seq
            // scans, cheap. MV would duplicate storage without saving anything.
            if set_op.op == SetOpType::Union && set_op.all {
                return ShapeGate::Skip;
            }
            // UNION (dedup), INTERSECT [ALL], EXCEPT [ALL] do real dedup work and
            // can reduce the result — the build-time gates decide.
            ShapeGate::Gated
        }

        QueryBody::Select(select) => {
            // Any non-trivial recompute — window, aggregate/GROUP BY/HAVING/
            // DISTINCT, or a join — can earn an MV. The two build-time gates
            // (row reduction OR compute avoidance) decide whether it actually
            // does; classification only fences off shapes that can never benefit.
            let computes = columns_any_owned(&select.columns, &ScalarExpr::has_window)
                || select.distinct
                || !select.group_by.is_empty()
                || select.having.is_some()
                || columns_any_owned(&select.columns, &|e| e.has_aggregate(aggregate_functions))
                || select_has_join_owned(select);
            if computes {
                ShapeGate::Gated
            } else {
                // Single-table plain filter/projection — source-row cache
                // already stores exactly these rows; MV would duplicate it.
                return ShapeGate::Skip;
            }
        }
    };

    // MV-specific viability: every top-level ORDER BY expression must be
    // serveable against the MV. If not, downgrade to Skip — serving without
    // the ORDER BY would give arbitrary rows for user LIMIT < max_limit.
    if !order_by_serve_viable_owned(tree, bindings) {
        return ShapeGate::Skip;
    }

    shape
}

/// Top-level FROM joins two or more base tables (explicit JOIN or comma
/// list). Subqueries in FROM are not descended into.
fn select_has_join_owned(select: &SelectNode) -> bool {
    select.from.len() > 1
        || select
            .from
            .iter()
            .any(|source| matches!(source, TableSource::Join(_)))
}

/// Whether any top-level column expression of the SELECT list satisfies
/// `pred`; a star is never one.
fn columns_any_owned<P>(columns: &SelectColumns, pred: &P) -> bool
where
    P: Fn(&ScalarExpr) -> bool,
{
    match columns {
        SelectColumns::None => false,
        SelectColumns::Columns(columns) => columns.iter().any(|column| match column {
            SelectColumn::Expr { expr, .. } => pred(expr),
            SelectColumn::Star(_) => false,
        }),
    }
}
