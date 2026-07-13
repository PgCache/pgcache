//! Resolve expressions: WHERE predicates, scalar expressions, and the window
//! specifications / frames that hang off a window function call.

use crate::query::ast::{FrameBound, ScalarExpr, WhereExpr, WindowFrame, WindowSpec};
use crate::query::resolved::{
    ResolveResult, ResolvedArithmeticExpr, ResolvedBinaryExpr, ResolvedCaseExpr, ResolvedCaseWhen,
    ResolvedFrameBound, ResolvedFunctionCall, ResolvedMultiExpr, ResolvedOrderByClause,
    ResolvedScalarExpr, ResolvedUnaryExpr, ResolvedWhereExpr, ResolvedWindowFrame,
    ResolvedWindowSpec,
};

use super::clauses::order_by_resolve;
use super::column::column_resolve;
use super::scope::ResolutionScope;

/// Resolve a WHERE expression
pub(super) fn where_expr_resolve(
    expr: &WhereExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWhereExpr> {
    match expr {
        WhereExpr::Scalar(scalar) => {
            let resolved = scalar_expr_resolve(scalar, scope)?;
            Ok(ResolvedWhereExpr::Scalar(resolved))
        }
        WhereExpr::Unary(unary) => {
            let resolved_expr = where_expr_resolve(&unary.expr, scope)?;
            Ok(ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
                op: unary.op,
                expr: Box::new(resolved_expr),
            }))
        }
        WhereExpr::Binary(binary) => {
            let resolved_left = where_expr_resolve(&binary.lexpr, scope)?;
            let resolved_right = where_expr_resolve(&binary.rexpr, scope)?;
            Ok(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: binary.op,
                lexpr: Box::new(resolved_left),
                rexpr: Box::new(resolved_right),
            }))
        }
        WhereExpr::Multi(multi) => {
            let mut resolved_exprs = Vec::with_capacity(multi.exprs.len());
            for e in &multi.exprs {
                resolved_exprs.push(where_expr_resolve(e, scope)?);
            }
            Ok(ResolvedWhereExpr::Multi(ResolvedMultiExpr {
                op: multi.op,
                exprs: resolved_exprs,
            }))
        }
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            // Resolve the test expression (left-hand side for IN/ANY/ALL) in the outer scope
            let resolved_test = match test_expr {
                Some(e) => Some(Box::new(scalar_expr_resolve(e, scope)?)),
                None => None,
            };

            // Resolve the inner query, collecting any correlated outer references
            let (resolved_query, outer_refs) = scope.subquery_resolve(query)?;

            Ok(ResolvedWhereExpr::Subquery {
                query: Box::new(resolved_query),
                sublink_type: *sublink_type,
                test_expr: resolved_test,
                outer_refs,
            })
        }
    }
}

/// Resolve a column expression in SELECT list
pub(super) fn scalar_expr_resolve(
    expr: &ScalarExpr,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedScalarExpr> {
    match expr {
        ScalarExpr::Column(col) => {
            // An unqualified reference to a USING/NATURAL merged column
            // resolves to the single merged value (the left column for
            // an inner join, `COALESCE(left, right)` for an outer one),
            // not an ambiguous per-side lookup. Qualified `t.c` still
            // reaches the base table.
            if col.table.is_none()
                && let Some(expr) = scope
                    .merged_column_find(col.column.as_str())
                    .map(|m| m.expr.clone())
            {
                return Ok(expr);
            }
            let resolved = column_resolve(col, scope)?;
            Ok(ResolvedScalarExpr::Column(resolved))
        }
        ScalarExpr::Literal(lit) => Ok(ResolvedScalarExpr::Literal(lit.clone())),
        ScalarExpr::Function(func) => {
            let mut resolved_args = Vec::with_capacity(func.args.len());
            for arg in &func.args {
                resolved_args.push(scalar_expr_resolve(arg, scope)?);
            }
            // Aggregate ORDER BY (e.g. `string_agg(x, ',' ORDER BY y)`) has no
            // access to SELECT-list aliases — it's evaluated per row within the
            // aggregate's input, not against the output.
            let resolved_agg_order = order_by_resolve(&func.agg_order, scope, None)?;
            let resolved_agg_filter = match &func.agg_filter {
                Some(f) => Some(Box::new(where_expr_resolve(f, scope)?)),
                None => None,
            };
            let resolved_over = match &func.over {
                Some(w) => Some(window_spec_resolve(w, scope)?),
                None => None,
            };
            Ok(ResolvedScalarExpr::Function(ResolvedFunctionCall {
                name: func.name.as_str().into(),
                args: resolved_args,
                agg_star: func.agg_star,
                agg_distinct: func.agg_distinct,
                agg_order: resolved_agg_order,
                agg_filter: resolved_agg_filter,
                over: resolved_over,
            }))
        }
        ScalarExpr::Case(case) => {
            let arg = match &case.arg {
                Some(a) => Some(Box::new(scalar_expr_resolve(a, scope)?)),
                None => None,
            };
            let mut whens = Vec::with_capacity(case.whens.len());
            for w in &case.whens {
                let condition = where_expr_resolve(&w.condition, scope)?;
                let result = scalar_expr_resolve(&w.result, scope)?;
                whens.push(ResolvedCaseWhen { condition, result });
            }
            let default = match &case.default {
                Some(d) => Some(Box::new(scalar_expr_resolve(d, scope)?)),
                None => None,
            };
            Ok(ResolvedScalarExpr::Case(ResolvedCaseExpr {
                arg,
                whens,
                default,
            }))
        }
        ScalarExpr::Arithmetic(arith) => {
            let left = scalar_expr_resolve(&arith.left, scope)?;
            let right = scalar_expr_resolve(&arith.right, scope)?;
            Ok(ResolvedScalarExpr::Arithmetic(ResolvedArithmeticExpr {
                left: Box::new(left),
                op: arith.op,
                right: Box::new(right),
            }))
        }
        ScalarExpr::Subquery(query) => {
            // Resolve the scalar subquery, collecting any correlated outer references
            let (resolved_query, outer_refs) = scope.subquery_resolve(query)?;
            Ok(ResolvedScalarExpr::Subquery(
                Box::new(resolved_query),
                outer_refs,
            ))
        }
        ScalarExpr::Array(elems) => {
            let mut resolved = Vec::with_capacity(elems.len());
            for e in elems {
                resolved.push(scalar_expr_resolve(e, scope)?);
            }
            Ok(ResolvedScalarExpr::Array(resolved))
        }
        ScalarExpr::TypeCast { expr, target } => {
            let inner = scalar_expr_resolve(expr, scope)?;
            Ok(ResolvedScalarExpr::TypeCast {
                expr: Box::new(inner),
                target: target.clone(),
            })
        }
    }
}

/// Resolve a window specification
pub(super) fn window_spec_resolve(
    window_spec: &WindowSpec,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWindowSpec> {
    let mut partition_by = Vec::with_capacity(window_spec.partition_by.len());
    for col in &window_spec.partition_by {
        partition_by.push(scalar_expr_resolve(col, scope)?);
    }
    let mut order_by = Vec::with_capacity(window_spec.order_by.len());
    for clause in &window_spec.order_by {
        let resolved_expr = scalar_expr_resolve(&clause.expr, scope)?;
        order_by.push(ResolvedOrderByClause {
            expr: resolved_expr,
            direction: clause.direction.clone(),
            null_order: clause.null_order,
        });
    }
    let frame = match &window_spec.frame {
        Some(frame) => Some(window_frame_resolve(frame, scope)?),
        None => None,
    };
    Ok(ResolvedWindowSpec {
        partition_by,
        order_by,
        frame,
    })
}

pub(super) fn window_frame_resolve(
    frame: &WindowFrame,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedWindowFrame> {
    Ok(ResolvedWindowFrame {
        mode: frame.mode,
        start: frame_bound_resolve(&frame.start, scope)?,
        end: frame_bound_resolve(&frame.end, scope)?,
        exclusion: frame.exclusion,
    })
}

pub(super) fn frame_bound_resolve(
    bound: &FrameBound,
    scope: &mut ResolutionScope<'_>,
) -> ResolveResult<ResolvedFrameBound> {
    Ok(match bound {
        FrameBound::UnboundedPreceding => ResolvedFrameBound::UnboundedPreceding,
        FrameBound::CurrentRow => ResolvedFrameBound::CurrentRow,
        FrameBound::UnboundedFollowing => ResolvedFrameBound::UnboundedFollowing,
        FrameBound::OffsetPreceding(e) => {
            ResolvedFrameBound::OffsetPreceding(Box::new(scalar_expr_resolve(e, scope)?))
        }
        FrameBound::OffsetFollowing(e) => {
            ResolvedFrameBound::OffsetFollowing(Box::new(scalar_expr_resolve(e, scope)?))
        }
    })
}
