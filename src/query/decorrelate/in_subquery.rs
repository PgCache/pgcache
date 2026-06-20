use rootcause::Report;

use crate::query::ast::{BinaryOp, JoinType, UnaryOp};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedJoinQual, ResolvedQueryExpr,
    ResolvedScalarExpr, ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableSource,
    ResolvedUnaryExpr, ResolvedWhereExpr,
};

use super::predicate::*;
use super::{CorrelationPredicates, DecorrelateError, DecorrelateResult};

/// Extract the single output column from an IN subquery's SELECT list.
///
/// IN subqueries must produce exactly one column, and for decorrelation
/// it must be a simple column reference (not an expression).
fn in_any_inner_output_column(
    inner_select: &ResolvedSelectNode,
) -> DecorrelateResult<ResolvedColumnNode> {
    let columns = match &inner_select.columns {
        ResolvedSelectColumns::Columns(cols) => cols,
        ResolvedSelectColumns::None => {
            return Err(DecorrelateError::NonDecorrelatable {
                reason: "IN subquery has no output columns".to_owned(),
            }
            .into());
        }
    };

    match columns.as_slice() {
        [col] => match &col.expr {
            ResolvedScalarExpr::Column(col_node) => Ok(col_node.clone()),
            ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(..)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. } => Err(DecorrelateError::NonDecorrelatable {
                reason: "IN subquery output is not a simple column reference".to_owned(),
            }
            .into()),
        },
        _ => Err(DecorrelateError::NonDecorrelatable {
            reason: "IN subquery must have exactly one output column".to_owned(),
        }
        .into()),
    }
}

/// IN → INNER JOIN + DISTINCT (semi-join).
///
/// Merges inner FROM sources into a JOIN with the outer FROM, using both
/// the IN predicate (test_expr = inner_output_column) and correlation predicates
/// as the ON condition. Sets DISTINCT to preserve semi-join semantics.
/// Residual inner predicates are merged into the outer WHERE.
fn subquery_in_any_decorrelate(
    select: &ResolvedSelectNode,
    inner_query: &ResolvedQueryExpr,
    predicates: &CorrelationPredicates,
    residual: Option<ResolvedWhereExpr>,
    test_expr: &ResolvedScalarExpr,
    inner_output_column: &ResolvedColumnNode,
) -> Option<ResolvedSelectNode> {
    let inner_select = inner_query_select(inner_query)?;

    // Build IN predicate: test_expr = inner_output_column
    let in_predicate = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
        op: BinaryOp::Equal,
        lexpr: Box::new(ResolvedWhereExpr::Scalar(test_expr.clone())),
        rexpr: boxed_scalar_column(inner_output_column.clone()),
    });

    // Build correlation condition from WHERE predicates and combine with IN predicate
    let correlation_condition = correlation_predicates_to_condition(predicates);
    let join_condition = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
        op: BinaryOp::And,
        lexpr: Box::new(in_predicate),
        rexpr: Box::new(correlation_condition),
    });

    let right = from_sources_to_table_source(&inner_select.from)?;
    let left = from_sources_to_table_source(&select.from)?;

    let join = ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
        join_type: JoinType::Inner,
        left,
        right,
        qual: ResolvedJoinQual::On(join_condition),
    }));

    // Merge residual inner predicates into outer WHERE
    let new_where = merge_where_clauses(&select.where_clause, &residual);

    Some(ResolvedSelectNode {
        distinct: true,
        columns: select.columns.clone(),
        from: vec![join],
        where_clause: new_where,
        group_by: select.group_by.clone(),
        having: select.having.clone(),
    })
}

/// NOT IN → LEFT JOIN + IS NULL (anti-join).
///
/// Merges inner FROM sources into a LEFT JOIN with the outer FROM. The IN predicate
/// (test_expr = inner_output_column), correlation predicates, AND residual inner
/// predicates all go into the ON clause to preserve LEFT JOIN semantics. An IS NULL
/// check on the inner output column is added to the outer WHERE for anti-join filtering.
fn subquery_not_in_all_decorrelate(
    select: &ResolvedSelectNode,
    inner_query: &ResolvedQueryExpr,
    predicates: &CorrelationPredicates,
    residual: Option<ResolvedWhereExpr>,
    test_expr: &ResolvedScalarExpr,
    inner_output_column: &ResolvedColumnNode,
) -> Option<ResolvedSelectNode> {
    let inner_select = inner_query_select(inner_query)?;

    // Build IN predicate: test_expr = inner_output_column
    let in_predicate = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
        op: BinaryOp::Equal,
        lexpr: Box::new(ResolvedWhereExpr::Scalar(test_expr.clone())),
        rexpr: boxed_scalar_column(inner_output_column.clone()),
    });

    // Build correlation condition from WHERE predicates and combine with IN predicate
    let correlation_condition = correlation_predicates_to_condition(predicates);
    let mut on_condition = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
        op: BinaryOp::And,
        lexpr: Box::new(in_predicate),
        rexpr: Box::new(correlation_condition),
    });

    // For NOT IN (anti-join), residual predicates go into the ON clause (not outer WHERE)
    // to preserve LEFT JOIN semantics — placing them in WHERE would filter out the
    // NULL-padded rows that represent non-matching outer rows.
    if let Some(res) = residual {
        on_condition = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(on_condition),
            rexpr: Box::new(res),
        });
    }

    let right = from_sources_to_table_source(&inner_select.from)?;
    let left = from_sources_to_table_source(&select.from)?;

    let join = ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
        join_type: JoinType::Left,
        left,
        right,
        qual: ResolvedJoinQual::On(on_condition),
    }));

    // IS NULL check on inner output column (anti-join filter)
    let is_null_check = ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
        op: UnaryOp::IsNull,
        expr: boxed_scalar_column(inner_output_column.clone()),
    });

    // Merge IS NULL with existing outer WHERE
    let new_where = match &select.where_clause {
        Some(existing) => Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(existing.clone()),
            rexpr: Box::new(is_null_check),
        })),
        None => Some(is_null_check),
    };

    Some(ResolvedSelectNode {
        distinct: select.distinct,
        columns: select.columns.clone(),
        from: vec![join],
        where_clause: new_where,
        group_by: select.group_by.clone(),
        having: select.having.clone(),
    })
}

/// Attempt to decorrelate a correlated IN subquery conjunct.
///
/// Returns `Ok(Some(new_select))` on success, `Ok(None)` if the conjunct can't be
/// decorrelated, or `Err` for unsupported patterns.
pub(super) fn conjunct_in_any_try_decorrelate(
    current_select: &ResolvedSelectNode,
    query: &ResolvedQueryExpr,
    outer_refs: &[ResolvedColumnNode],
    test_expr: &ResolvedScalarExpr,
) -> DecorrelateResult<Option<ResolvedSelectNode>> {
    let Some(inner_select) = inner_query_select(query) else {
        return Ok(None);
    };

    // Strip GROUP BY/HAVING/LIMIT — safe for IN (membership check, conservative invalidation).
    // Same rationale as EXISTS: IN checks set membership, so these clauses don't change
    // which outer rows are affected, and stripping them is conservatively correct for CDC.
    let (cleaned_query, cleaned_select) = correlated_exists_inner_prepare(query, inner_select);

    let Some(inner_where) = &cleaned_select.where_clause else {
        return Ok(None);
    };

    // Extract the single output column (must be a simple column reference)
    let inner_output_column = in_any_inner_output_column(&cleaned_select)?;

    let (predicates, residual) = where_clause_correlation_partition(inner_where, outer_refs)
        .ok_or_else(|| {
            Report::from(DecorrelateError::NonDecorrelatable {
                reason: "unsupported correlation pattern in IN".to_owned(),
            })
        })?;

    Ok(subquery_in_any_decorrelate(
        current_select,
        &cleaned_query,
        &predicates,
        residual,
        test_expr,
        &inner_output_column,
    ))
}

/// Attempt to decorrelate a correlated NOT IN (ALL) subquery conjunct.
///
/// Returns `Ok(Some(new_select))` on success, `Ok(None)` if the conjunct can't be
/// decorrelated, or `Err` for unsupported patterns (including GROUP BY/HAVING).
pub(super) fn conjunct_not_in_all_try_decorrelate(
    current_select: &ResolvedSelectNode,
    query: &ResolvedQueryExpr,
    outer_refs: &[ResolvedColumnNode],
    test_expr: &ResolvedScalarExpr,
) -> DecorrelateResult<Option<ResolvedSelectNode>> {
    let Some(inner_select) = inner_query_select(query) else {
        return Ok(None);
    };

    // Strip LIMIT (safe — boolean check); reject GROUP BY/HAVING (unsafe for anti-join,
    // same reasoning as NOT EXISTS: the anti-join tests for zero matching rows in the
    // flattened join, but the original tests for no rows surviving the filter).
    let (cleaned_query, cleaned_select) = correlated_not_exists_inner_prepare(query, inner_select)?;

    let Some(inner_where) = &cleaned_select.where_clause else {
        return Ok(None);
    };

    // Extract the single output column (must be a simple column reference)
    let inner_output_column = in_any_inner_output_column(&cleaned_select)?;

    let (predicates, residual) = where_clause_correlation_partition(inner_where, outer_refs)
        .ok_or_else(|| {
            Report::from(DecorrelateError::NonDecorrelatable {
                reason: "unsupported correlation pattern in NOT IN".to_owned(),
            })
        })?;

    Ok(subquery_not_in_all_decorrelate(
        current_select,
        &cleaned_query,
        &predicates,
        residual,
        test_expr,
        &inner_output_column,
    ))
}
