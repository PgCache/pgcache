//! Bind the clauses hanging off a SELECT: the select list (including `*`
//! expansion), ORDER BY and GROUP BY.

use super::ResolveResult;
use super::bindings::{BindingsBuilder, ColumnBinding, ColumnTarget, StarItem};
use super::column::column_resolve;
use super::expr::scalar_resolve;
use super::scope::{OutputColumn, ResolutionScope, ScopeEntry};
use crate::query::ast::NodeId;
use crate::query::ast::rope::{Column, OrderBy, Run, Scalar, ScalarKind, Select, SelectItem};

/// Bind the select list, returning its output columns. A star (`*` or
/// `t1.*`) stands for all columns of the matching tables in scope.
pub(super) fn select_items_resolve<'a>(
    select: Select<'a>,
    scope: &mut ResolutionScope<'a>,
) -> ResolveResult<Vec<OutputColumn>> {
    let Some(items) = select.items() else {
        return Ok(Vec::new());
    };
    let mut outputs = Vec::new();
    for item in items {
        match item {
            SelectItem::Star(star) => star_expand(
                star.id(),
                star.qualifier().map(|qualifier| qualifier.as_str()),
                scope,
                &mut outputs,
            ),
            SelectItem::Expr(expr) => {
                let source = scalar_resolve(expr.expr(), scope)?;
                let name = expr
                    .alias()
                    .cloned()
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
fn star_expand(
    star: NodeId,
    qualifier: Option<&str>,
    scope: &mut ResolutionScope<'_>,
    outputs: &mut Vec<OutputColumn>,
) {
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
    scope.bindings.star_bind(star, items);
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
/// a select-list output name binds to that output — PostgreSQL's
/// precedence rule that ORDER BY matches output names before falling back
/// to column lookup.
pub(super) fn order_by_resolve<'a>(
    order_by: Run<'a, OrderBy<'a>>,
    scope: &mut ResolutionScope<'a>,
    outputs: Option<&[OutputColumn]>,
) -> ResolveResult<()> {
    for clause in order_by {
        match order_by_output_match(clause.expr(), outputs) {
            Some(column) => scope
                .bindings
                .column_bind(column.id(), ColumnTarget::Output, false),
            None => {
                scalar_resolve(clause.expr(), scope)?;
            }
        }
    }
    Ok(())
}

/// The unqualified column reference `expr` is, when its name matches a
/// select-list output name.
fn order_by_output_match<'a>(
    expr: Scalar<'a>,
    outputs: Option<&[OutputColumn]>,
) -> Option<Column<'a>> {
    let ScalarKind::Column(column) = expr.kind() else {
        return None;
    };
    if column.table().is_some() {
        return None;
    }
    outputs?
        .iter()
        .any(|output| output.name.as_deref() == Some(column.column().as_str()))
        .then_some(column)
}

/// Bind ORDER BY clauses of a set operation or VALUES, whose column
/// references name output columns, not table columns.
pub(super) fn order_by_as_outputs<'a>(
    order_by: Run<'a, OrderBy<'a>>,
    bindings: &mut BindingsBuilder,
) {
    for clause in order_by {
        scalar_bind_outputs(clause.expr(), bindings);
    }
}

/// Bind every column reference of an output-position expression as an
/// output reference, through function arguments, arithmetic, arrays and
/// casts. CASE and subqueries are not entered: an output ORDER BY over
/// them has no column references of its own to bind.
fn scalar_bind_outputs(expr: Scalar<'_>, bindings: &mut BindingsBuilder) {
    match expr.kind() {
        ScalarKind::Column(column) => {
            bindings.column_bind(column.id(), ColumnTarget::Output, false);
        }
        ScalarKind::Literal(_) | ScalarKind::Case(_) | ScalarKind::Subquery(_) => {}
        ScalarKind::Function(function) => {
            for arg in function.args() {
                scalar_bind_outputs(arg, bindings);
            }
        }
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_bind_outputs(arithmetic.left(), bindings);
            scalar_bind_outputs(arithmetic.right(), bindings);
        }
        ScalarKind::Array(array) => {
            for element in array.elements() {
                scalar_bind_outputs(element, bindings);
            }
        }
        ScalarKind::TypeCast(cast) => scalar_bind_outputs(cast.expr(), bindings),
    }
}

/// Bind GROUP BY columns.
pub(super) fn group_by_resolve<'a>(
    group_by: Run<'a, Column<'a>>,
    scope: &mut ResolutionScope<'a>,
) -> ResolveResult<()> {
    for column in group_by {
        column_resolve(
            column.id(),
            column.table().map(|table| table.as_str()),
            column.column(),
            scope,
        )?;
    }
    Ok(())
}
