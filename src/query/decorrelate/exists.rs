use rootcause::Report;

use crate::query::ast::{BinaryOp, JoinType, UnaryOp};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedJoinQual, ResolvedQueryExpr,
    ResolvedSelectNode, ResolvedTableSource, ResolvedUnaryExpr, ResolvedWhereExpr,
};

use super::predicate::*;
use super::{CorrelationPredicates, DecorrelateError, DecorrelateResult};

/// EXISTS → INNER JOIN + DISTINCT (semi-join).
///
/// Merges inner FROM sources into a JOIN with the outer FROM, using correlation
/// predicates as the ON condition. Sets DISTINCT on the result to preserve
/// semi-join semantics. Residual inner predicates are merged into the outer WHERE.
fn subquery_exists_decorrelate(
    select: &ResolvedSelectNode,
    inner_query: &ResolvedQueryExpr,
    predicates: &CorrelationPredicates,
    residual: Option<ResolvedWhereExpr>,
) -> Option<ResolvedSelectNode> {
    let inner_select = inner_query_select(inner_query)?;

    let join_condition = correlation_predicates_to_condition(predicates);

    // Build the right side of the JOIN from the inner query's FROM sources
    let right = from_sources_to_table_source(&inner_select.from)?;

    // Build the left side from the outer query's FROM sources
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

/// NOT EXISTS → LEFT JOIN + IS NULL (anti-join).
///
/// Merges inner FROM sources into a LEFT JOIN with the outer FROM. Correlation
/// predicates AND residual inner predicates both go into the ON clause (not the
/// outer WHERE) to preserve LEFT JOIN semantics. An IS NULL check on one of the
/// inner correlation columns is added to the outer WHERE.
fn subquery_not_exists_decorrelate(
    select: &ResolvedSelectNode,
    inner_query: &ResolvedQueryExpr,
    predicates: &CorrelationPredicates,
    residual: Option<ResolvedWhereExpr>,
) -> Option<ResolvedSelectNode> {
    let inner_select = inner_query_select(inner_query)?;

    // For NOT EXISTS, residual predicates go into the ON clause
    let correlation_condition = correlation_predicates_to_condition(predicates);
    let on_condition = match residual {
        Some(res) => ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(correlation_condition),
            rexpr: Box::new(res),
        }),
        None => correlation_condition,
    };

    let right = from_sources_to_table_source(&inner_select.from)?;
    let left = from_sources_to_table_source(&select.from)?;

    let join = ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
        join_type: JoinType::Left,
        left,
        right,
        qual: ResolvedJoinQual::On(on_condition),
    }));

    // Add IS NULL check on the first inner correlation column.
    let first_predicate = &predicates.first;
    let is_null_check = ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
        op: UnaryOp::IsNull,
        expr: boxed_scalar_column(first_predicate.inner_column.clone()),
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

/// Attempt to decorrelate a correlated EXISTS subquery conjunct.
///
/// Returns `Ok(Some(new_select))` on success, `Ok(None)` if the conjunct can't be
/// decorrelated (caller should keep as residual), or `Err` for unsupported patterns.
pub(super) fn conjunct_exists_try_decorrelate(
    current_select: &ResolvedSelectNode,
    query: &ResolvedQueryExpr,
    outer_refs: &[ResolvedColumnNode],
) -> DecorrelateResult<Option<ResolvedSelectNode>> {
    let Some(inner_select) = inner_query_select(query) else {
        return Ok(None);
    };

    // Strip GROUP BY/HAVING/LIMIT — safe for EXISTS (conservative invalidation)
    let (cleaned_query, cleaned_select) = correlated_exists_inner_prepare(query, inner_select);

    let Some(inner_where) = &cleaned_select.where_clause else {
        return Ok(None);
    };

    let (predicates, residual) = where_clause_correlation_partition(inner_where, outer_refs)
        .ok_or_else(|| {
            Report::from(DecorrelateError::NonDecorrelatable {
                reason: "unsupported correlation pattern in EXISTS".to_owned(),
            })
        })?;

    Ok(subquery_exists_decorrelate(
        current_select,
        &cleaned_query,
        &predicates,
        residual,
    ))
}

/// Attempt to decorrelate a correlated NOT EXISTS subquery conjunct.
///
/// Returns `Ok(Some(new_select))` on success, `Ok(None)` if the conjunct can't be
/// decorrelated, or `Err` for unsupported patterns (including GROUP BY/HAVING).
pub(super) fn conjunct_not_exists_try_decorrelate(
    current_select: &ResolvedSelectNode,
    query: &ResolvedQueryExpr,
    outer_refs: &[ResolvedColumnNode],
) -> DecorrelateResult<Option<ResolvedSelectNode>> {
    let Some(inner_select) = inner_query_select(query) else {
        return Ok(None);
    };

    // Strip LIMIT (safe); reject GROUP BY/HAVING (unsafe for anti-join)
    let (cleaned_query, cleaned_select) = correlated_not_exists_inner_prepare(query, inner_select)?;

    let Some(inner_where) = &cleaned_select.where_clause else {
        return Ok(None);
    };

    let (predicates, residual) = where_clause_correlation_partition(inner_where, outer_refs)
        .ok_or_else(|| {
            Report::from(DecorrelateError::NonDecorrelatable {
                reason: "unsupported correlation pattern in NOT EXISTS".to_owned(),
            })
        })?;

    Ok(subquery_not_exists_decorrelate(
        current_select,
        &cleaned_query,
        &predicates,
        residual,
    ))
}
