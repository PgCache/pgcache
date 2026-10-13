//! Bind expressions: WHERE predicates, scalar expressions, and the window
//! specifications that hang off a window function call.

use ecow::EcoString;

use super::ResolveResult;
use super::bindings::{ColumnBinding, ColumnTarget};
use super::clauses::order_by_resolve;
use super::column::{ColumnLookup, column_resolve};
use super::scope::ResolutionScope;
use crate::query::ast::rope::{
    Column, Frame, FrameBound, Scalar, ScalarKind, Where, WhereKind, Window,
};

/// What a SELECT-list expression contributes to its output column: the name
/// a bare column reference gives it, and the catalog column it carries
/// through when it is one.
#[derive(Debug, Clone)]
pub(super) struct OutputSource {
    pub(super) name: EcoString,
    pub(super) binding: Option<ColumnBinding>,
}

/// Bind a WHERE expression.
pub(super) fn where_resolve<'a>(
    expr: Where<'a>,
    scope: &mut ResolutionScope<'a>,
) -> ResolveResult<()> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => {
            scalar_resolve(scalar, scope)?;
        }
        WhereKind::Unary(unary) => where_resolve(unary.expr(), scope)?,
        WhereKind::Binary(binary) => {
            where_resolve(binary.lexpr(), scope)?;
            where_resolve(binary.rexpr(), scope)?;
        }
        WhereKind::Multi(multi) => {
            for operand in multi.exprs() {
                where_resolve(operand, scope)?;
            }
        }
        WhereKind::Subquery(subquery) => {
            // The test expression (the left-hand side of IN/ANY/ALL) binds
            // in the outer scope; the inner query may correlate with it.
            if let Some(test_expr) = subquery.test_expr() {
                scalar_resolve(test_expr, scope)?;
            }
            scope.subquery_resolve(subquery.query())?;
        }
    }
    Ok(())
}

/// Bind a scalar expression, returning what it contributes to an output
/// column when it is a bare column reference.
pub(super) fn scalar_resolve<'a>(
    expr: Scalar<'a>,
    scope: &mut ResolutionScope<'a>,
) -> ResolveResult<Option<OutputSource>> {
    match expr.kind() {
        ScalarKind::Column(column) => column_resolve_output(column, scope),
        ScalarKind::Literal(_) => Ok(None),
        ScalarKind::Function(function) => {
            for arg in function.args() {
                scalar_resolve(arg, scope)?;
            }
            // Aggregate ORDER BY (e.g. `string_agg(x, ',' ORDER BY y)`) has no
            // access to SELECT-list aliases — it's evaluated per row within the
            // aggregate's input, not against the output.
            order_by_resolve(function.agg_order(), scope, None)?;
            if let Some(filter) = function.filter() {
                where_resolve(filter, scope)?;
            }
            if let Some(window) = function.over() {
                window_resolve(window, scope)?;
            }
            Ok(None)
        }
        ScalarKind::Case(case) => {
            if let Some(arg) = case.arg() {
                scalar_resolve(arg, scope)?;
            }
            for when in case.whens() {
                where_resolve(when.condition(), scope)?;
                scalar_resolve(when.result(), scope)?;
            }
            if let Some(default) = case.default() {
                scalar_resolve(default, scope)?;
            }
            Ok(None)
        }
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_resolve(arithmetic.left(), scope)?;
            scalar_resolve(arithmetic.right(), scope)?;
            Ok(None)
        }
        ScalarKind::Subquery(subquery) => {
            scope.subquery_resolve(subquery.query())?;
            Ok(None)
        }
        ScalarKind::Array(array) => {
            for element in array.elements() {
                scalar_resolve(element, scope)?;
            }
            Ok(None)
        }
        ScalarKind::TypeCast(cast) => {
            scalar_resolve(cast.expr(), scope)?;
            Ok(None)
        }
    }
}

/// Bind a column reference, returning what it contributes to an output
/// column.
fn column_resolve_output<'a>(
    column: Column<'a>,
    scope: &mut ResolutionScope<'a>,
) -> ResolveResult<Option<OutputSource>> {
    // An unqualified reference to a USING/NATURAL merged column is
    // the single merged value (the left column for an inner join,
    // `COALESCE(left, right)` for an outer one), not an ambiguous
    // per-side lookup. Qualified `t.c` still reaches the base table.
    if column.table().is_none()
        && let Some(merged) = scope.merged_column_find(column.column())
    {
        let source = (!merged.outer).then(|| OutputSource {
            name: merged.name.clone(),
            binding: Some(merged.left),
        });
        let id = merged.merged;
        scope
            .bindings
            .column_bind(column.id(), ColumnTarget::Merged(id), false);
        return Ok(source);
    }
    let lookup = column_resolve(
        column.id(),
        column.table().map(|table| table.as_str()),
        column.column(),
        scope,
    )?;
    Ok(Some(match lookup {
        ColumnLookup::Column { binding, .. } => OutputSource {
            name: column.column().clone(),
            binding: Some(binding),
        },
        ColumnLookup::Merged { .. } => OutputSource {
            name: column.column().clone(),
            binding: None,
        },
    }))
}

fn window_resolve<'a>(window: Window<'a>, scope: &mut ResolutionScope<'a>) -> ResolveResult<()> {
    for partition in window.partition_by() {
        scalar_resolve(partition, scope)?;
    }
    for clause in window.order_by() {
        scalar_resolve(clause.expr(), scope)?;
    }
    if let Some(frame) = window.frame() {
        frame_resolve(frame, scope)?;
    }
    Ok(())
}

fn frame_resolve<'a>(frame: Frame<'a>, scope: &mut ResolutionScope<'a>) -> ResolveResult<()> {
    frame_bound_resolve(frame.start(), scope)?;
    frame_bound_resolve(frame.end(), scope)
}

fn frame_bound_resolve<'a>(
    bound: FrameBound<'a>,
    scope: &mut ResolutionScope<'a>,
) -> ResolveResult<()> {
    match bound {
        FrameBound::UnboundedPreceding
        | FrameBound::CurrentRow
        | FrameBound::UnboundedFollowing => Ok(()),
        FrameBound::OffsetPreceding(offset) | FrameBound::OffsetFollowing(offset) => {
            scalar_resolve(offset, scope)?;
            Ok(())
        }
    }
}
