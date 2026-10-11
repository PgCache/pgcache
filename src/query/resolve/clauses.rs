//! Bind the clauses hanging off a SELECT: the target list (including `*`
//! expansion), ORDER BY and GROUP BY.

use super::bindings::{BindingsBuilder, ColumnBinding, ColumnTarget, StarItem};
use super::column::column_resolve;
use super::expr::scalar_expr_resolve;
use super::scope::{OutputColumn, ResolutionScope, ScopeEntry};
use crate::query::ast::{
    ColumnNode, OrderByClause, ScalarExpr, SelectColumn, SelectColumns, StarNode,
};
use crate::query::resolve::ResolveResult;

/// Bind the SELECT list, returning its output columns. A star (`*` or
/// `t1.*`) stands for all columns of the matching tables in scope.
pub(super) fn select_columns_resolve(
    columns: &SelectColumns,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Vec<OutputColumn>> {
    let SelectColumns::Columns(cols) = columns else {
        return Ok(Vec::new());
    };
    let mut outputs = Vec::new();
    for col in cols {
        match col {
            SelectColumn::Star(star) => star_expand(star, scope, &mut outputs),
            SelectColumn::Expr { expr, alias } => {
                let source = scalar_expr_resolve(expr, scope)?;
                let name = alias
                    .clone()
                    .or_else(|| source.as_ref().map(|s| s.name.clone()));
                outputs.push(OutputColumn {
                    name,
                    source: source.and_then(|s| s.binding),
                });
            }
        }
    }
    Ok(outputs)
}

/// Expand `*` or `qualifier.*` into the matching tables' columns.
///
/// Unqualified `*` over a USING/NATURAL join emits the merged join column(s)
/// once and first (as Postgres does), then each table's remaining (non-join)
/// columns. Qualified `t.*` is that table's columns verbatim (join column
/// included).
fn star_expand(star: &StarNode, scope: &mut ResolutionScope<'_>, outputs: &mut Vec<OutputColumn>) {
    let qualifier = star.qualifier.as_deref();
    let mut items = Vec::new();
    if qualifier.is_none() {
        for merged in &scope.merged_columns {
            outputs.push(OutputColumn {
                name: Some(merged.name.clone()),
                // An outer join's merged value is `COALESCE`, not a column.
                source: (!merged.outer).then_some(merged.left),
            });
            items.push(StarItem::Merged(merged.merged));
        }
    }
    for entry in &scope.entries {
        if qualifier.is_none_or(|q| entry.qualifier_matches(q)) {
            entry_columns_expand(entry, qualifier.is_none(), scope, outputs, &mut items);
        }
    }
    scope.bindings.star_bind(star.id, items);
}

/// One table's columns for `*`. Under unqualified `*`, columns merged into a
/// USING/NATURAL join column are skipped (they were emitted once already).
fn entry_columns_expand(
    entry: &ScopeEntry<'_>,
    unqualified: bool,
    scope: &ResolutionScope<'_>,
    outputs: &mut Vec<OutputColumn>,
    items: &mut Vec<StarItem>,
) {
    let key = entry.qualifier_key();
    for column_metadata in entry.metadata().columns.iter() {
        if unqualified && merged_join_column(scope, key, &column_metadata.name) {
            continue;
        }
        let binding = ColumnBinding {
            slot: entry.slot(),
            position: column_metadata.position,
        };
        outputs.push(OutputColumn {
            name: Some(column_metadata.name.clone()),
            source: Some(binding),
        });
        items.push(StarItem::Column(binding));
    }
}

/// Whether `column` of the table keyed `key` was merged into a USING/NATURAL
/// join column.
fn merged_join_column(scope: &ResolutionScope<'_>, key: &str, column: &str) -> bool {
    scope
        .merged_consumed
        .iter()
        .any(|(k, c)| k == key && c == column)
}

/// Bind ORDER BY clauses. With `outputs`, an unqualified name that matches
/// a SELECT-list output name binds to that output — PostgreSQL's
/// precedence rule that ORDER BY matches output names before falling back
/// to column lookup.
pub(super) fn order_by_resolve(
    order_by: &[OrderByClause],
    scope: &mut ResolutionScope<'_>,
    outputs: Option<&[OutputColumn]>,
) -> ResolveResult<()> {
    for clause in order_by {
        match order_by_output_match(&clause.expr, outputs) {
            Some(col) => scope
                .bindings
                .column_bind(col.id, ColumnTarget::Output, false),
            None => {
                scalar_expr_resolve(&clause.expr, scope)?;
            }
        }
    }
    Ok(())
}

/// The unqualified column reference `clause_expr` is, when its name matches
/// a SELECT-list output name.
fn order_by_output_match<'e>(
    clause_expr: &'e ScalarExpr,
    outputs: Option<&[OutputColumn]>,
) -> Option<&'e ColumnNode> {
    let ScalarExpr::Column(col) = clause_expr else {
        return None;
    };
    if col.table.is_some() {
        return None;
    }
    outputs?
        .iter()
        .any(|output| output.name.as_deref() == Some(col.column.as_str()))
        .then_some(col)
}

/// Bind ORDER BY clauses of a set operation or VALUES, whose column
/// references name output columns, not table columns.
pub(super) fn order_by_as_outputs(order_by: &[OrderByClause], bindings: &mut BindingsBuilder) {
    for clause in order_by {
        scalar_expr_bind_outputs(&clause.expr, bindings);
    }
}

/// Bind every column reference of an output-position expression as an
/// output reference, through function arguments, arithmetic, arrays and
/// casts. CASE and subqueries are not entered: an output ORDER BY over
/// them has no column references of its own to bind.
fn scalar_expr_bind_outputs(expr: &ScalarExpr, bindings: &mut BindingsBuilder) {
    match expr {
        ScalarExpr::Column(col) => bindings.column_bind(col.id, ColumnTarget::Output, false),
        ScalarExpr::Literal(_) | ScalarExpr::Case(_) | ScalarExpr::Subquery(_) => {}
        ScalarExpr::Function(func) => {
            for arg in &func.args {
                scalar_expr_bind_outputs(arg, bindings);
            }
        }
        ScalarExpr::Arithmetic(arith) => {
            scalar_expr_bind_outputs(&arith.left, bindings);
            scalar_expr_bind_outputs(&arith.right, bindings);
        }
        ScalarExpr::Array(elems) => {
            for elem in elems {
                scalar_expr_bind_outputs(elem, bindings);
            }
        }
        ScalarExpr::TypeCast { expr, .. } => scalar_expr_bind_outputs(expr, bindings),
    }
}

/// Bind GROUP BY columns.
pub(super) fn group_by_resolve(
    group_by: &[ColumnNode],
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<()> {
    for col in group_by {
        column_resolve(col, scope)?;
    }
    Ok(())
}
