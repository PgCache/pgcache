//! Bind expressions: WHERE predicates, scalar expressions, and the window
//! specifications / frames that hang off a window function call.

use ecow::EcoString;

use super::bindings::{ColumnBinding, ColumnTarget};
use super::clauses::order_by_resolve;
use super::column::{ColumnLookup, column_resolve};
use super::scope::ResolutionScope;
use crate::query::ast::{FrameBound, ScalarExpr, WhereExpr, WindowFrame, WindowSpec};
use crate::query::resolve::ResolveResult;

/// What a SELECT-list expression contributes to its output column: the name
/// a bare column reference gives it, and the catalog column it carries
/// through when it is one.
#[derive(Debug, Clone)]
pub(super) struct OutputSource {
    pub(super) name: EcoString,
    pub(super) binding: Option<ColumnBinding>,
}

/// Bind a WHERE expression.
pub(super) fn where_expr_resolve(
    expr: &WhereExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<()> {
    match expr {
        WhereExpr::Scalar(scalar) => {
            scalar_expr_resolve(scalar, scope)?;
        }
        WhereExpr::Unary(unary) => where_expr_resolve(&unary.expr, scope)?,
        WhereExpr::Binary(binary) => {
            where_expr_resolve(&binary.lexpr, scope)?;
            where_expr_resolve(&binary.rexpr, scope)?;
        }
        WhereExpr::Multi(multi) => {
            for e in &multi.exprs {
                where_expr_resolve(e, scope)?;
            }
        }
        WhereExpr::Subquery {
            query, test_expr, ..
        } => {
            // The test expression (the left-hand side of IN/ANY/ALL) binds
            // in the outer scope; the inner query may correlate with it.
            if let Some(e) = test_expr {
                scalar_expr_resolve(e, scope)?;
            }
            scope.subquery_resolve(query)?;
        }
    }
    Ok(())
}

/// Bind a scalar expression, returning what it contributes to an output
/// column when it is a bare column reference.
pub(super) fn scalar_expr_resolve(
    expr: &ScalarExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<Option<OutputSource>> {
    match expr {
        ScalarExpr::Column(col) => {
            // An unqualified reference to a USING/NATURAL merged column is
            // the single merged value (the left column for an inner join,
            // `COALESCE(left, right)` for an outer one), not an ambiguous
            // per-side lookup. Qualified `t.c` still reaches the base table.
            if col.table.is_none()
                && let Some(merged) = scope.merged_column_find(col.column.as_str())
            {
                let source = (!merged.outer).then(|| OutputSource {
                    name: merged.name.clone(),
                    binding: Some(merged.left),
                });
                let id = merged.merged;
                scope
                    .bindings
                    .column_bind(col.id, ColumnTarget::Merged(id), false);
                return Ok(source);
            }
            let lookup = column_resolve(col.id, col.table.as_deref(), &col.column, scope)?;
            Ok(Some(match lookup {
                ColumnLookup::Column { binding, .. } => OutputSource {
                    name: col.column.clone(),
                    binding: Some(binding),
                },
                ColumnLookup::Merged { .. } => OutputSource {
                    name: col.column.clone(),
                    binding: None,
                },
            }))
        }
        ScalarExpr::Literal(_) => Ok(None),
        ScalarExpr::Function(func) => {
            for arg in &func.args {
                scalar_expr_resolve(arg, scope)?;
            }
            // Aggregate ORDER BY (e.g. `string_agg(x, ',' ORDER BY y)`) has no
            // access to SELECT-list aliases — it's evaluated per row within the
            // aggregate's input, not against the output.
            order_by_resolve(&func.agg_order, scope, None)?;
            if let Some(filter) = &func.agg_filter {
                where_expr_resolve(filter, scope)?;
            }
            if let Some(window) = &func.over {
                window_spec_resolve(window, scope)?;
            }
            Ok(None)
        }
        ScalarExpr::Case(case) => {
            if let Some(arg) = &case.arg {
                scalar_expr_resolve(arg, scope)?;
            }
            for when in &case.whens {
                where_expr_resolve(&when.condition, scope)?;
                scalar_expr_resolve(&when.result, scope)?;
            }
            if let Some(default) = &case.default {
                scalar_expr_resolve(default, scope)?;
            }
            Ok(None)
        }
        ScalarExpr::Arithmetic(arith) => {
            scalar_expr_resolve(&arith.left, scope)?;
            scalar_expr_resolve(&arith.right, scope)?;
            Ok(None)
        }
        ScalarExpr::Subquery(query) => {
            scope.subquery_resolve(query)?;
            Ok(None)
        }
        ScalarExpr::Array(elems) => {
            for e in elems {
                scalar_expr_resolve(e, scope)?;
            }
            Ok(None)
        }
        ScalarExpr::TypeCast { expr, .. } => {
            scalar_expr_resolve(expr, scope)?;
            Ok(None)
        }
    }
}

/// Bind a window specification.
fn window_spec_resolve(
    window_spec: &WindowSpec,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<()> {
    for col in &window_spec.partition_by {
        scalar_expr_resolve(col, scope)?;
    }
    for clause in &window_spec.order_by {
        scalar_expr_resolve(&clause.expr, scope)?;
    }
    if let Some(frame) = &window_spec.frame {
        window_frame_resolve(frame, scope)?;
    }
    Ok(())
}

fn window_frame_resolve(frame: &WindowFrame, scope: &mut ResolutionScope<'_>) -> ResolveResult<()> {
    frame_bound_resolve(&frame.start, scope)?;
    frame_bound_resolve(&frame.end, scope)
}

fn frame_bound_resolve(bound: &FrameBound, scope: &mut ResolutionScope<'_>) -> ResolveResult<()> {
    match bound {
        FrameBound::UnboundedPreceding
        | FrameBound::CurrentRow
        | FrameBound::UnboundedFollowing => Ok(()),
        FrameBound::OffsetPreceding(e) | FrameBound::OffsetFollowing(e) => {
            scalar_expr_resolve(e, scope)?;
            Ok(())
        }
    }
}
