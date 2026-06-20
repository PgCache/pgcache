use std::collections::HashSet;

use ecow::EcoString;
use rootcause::Report;
use tokio_postgres::types::Type;

use crate::cache::SubqueryKind;
use crate::catalog::ColumnMetadata;
use crate::query::ast::{BinaryOp, JoinType, SubLinkType, TableAlias};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedJoinQual, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedTableSource, ResolvedTableSubqueryNode, ResolvedUnaryExpr,
    ResolvedWhereExpr,
};

use super::predicate::*;
use super::{
    CorrelationPredicate, DecorrelateError, DecorrelateResult, DecorrelateState,
    ScalarDecorrelateResult,
};

/// Validate and clean an inner query for scalar subquery decorrelation.
///
/// Requirements:
/// - Must be a simple SELECT (not SetOp/Values)
/// - Must have exactly one output column
/// - Must have a WHERE clause (needed for correlation extraction)
/// - Non-aggregate inner query WITH LIMIT is rejected (can't safely strip LIMIT)
///
/// Returns the cleaned select node (LIMIT/ORDER BY stripped) and the validated
/// WHERE clause (guaranteed to exist).
fn scalar_inner_prepare(
    inner_query: &ResolvedQueryExpr,
    agg_fns: &HashSet<EcoString>,
) -> DecorrelateResult<(ResolvedSelectNode, ResolvedWhereExpr)> {
    let inner_select = inner_query_select(inner_query).ok_or_else(|| {
        Report::from(DecorrelateError::NonDecorrelatable {
            reason: "scalar subquery is not a simple SELECT".to_owned(),
        })
    })?;

    // Must have exactly one output column
    let columns = match &inner_select.columns {
        ResolvedSelectColumns::Columns(cols) => cols,
        ResolvedSelectColumns::None => &Vec::new(),
    };
    let scalar_expr = match columns.as_slice() {
        [col] => &col.expr,
        _ => {
            return Err(DecorrelateError::NonDecorrelatable {
                reason: "scalar subquery must have exactly one output column".to_owned(),
            }
            .into());
        }
    };

    // Must have WHERE (needed for correlation predicates)
    let inner_where = inner_select.where_clause.clone().ok_or_else(|| {
        Report::from(DecorrelateError::NonDecorrelatable {
            reason: "scalar subquery has no WHERE clause".to_owned(),
        })
    })?;

    let has_aggregate = scalar_expr.has_aggregate(agg_fns);

    // Reject non-aggregate with LIMIT (can't safely strip LIMIT without aggregate dedup)
    if !has_aggregate && inner_query.limit.is_some() {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "non-aggregate scalar subquery with LIMIT".to_owned(),
        }
        .into());
    }

    // Strip LIMIT and ORDER BY
    let cleaned_select = inner_select.clone();

    Ok((cleaned_select, inner_where))
}

/// Build a synthetic `ResolvedColumnNode` for a derived table column.
fn synthetic_column_node(
    derived_alias: &str,
    column_name: EcoString,
    column_metadata: ColumnMetadata,
) -> ResolvedColumnNode {
    ResolvedColumnNode {
        schema: EcoString::from(""),
        table: EcoString::from(derived_alias),
        table_alias: Some(EcoString::from(derived_alias)),
        column: column_name,
        column_metadata,
    }
}

/// Synthetic column metadata for derived table columns (TEXT type, non-primary-key).
fn synthetic_text_metadata(name: &str, position: i16) -> ColumnMetadata {
    ColumnMetadata {
        name: EcoString::from(name),
        position,
        type_oid: 25,
        data_type: Type::TEXT,
        type_name: EcoString::from("text"),
        cache_type_name: EcoString::from("text"),
        is_primary_key: false,
    }
}

/// Core scalar subquery decorrelation: converts a correlated scalar subquery into a
/// LEFT JOIN with a derived table.
///
/// Input: inner query, outer_refs, mutable state for alias generation.
/// Output: `ScalarDecorrelateResult` containing the derived table, join condition,
/// and a column reference to the scalar result.
pub(super) fn subquery_scalar_decorrelate(
    inner_query: &ResolvedQueryExpr,
    outer_refs: &[ResolvedColumnNode],
    state: &mut DecorrelateState<'_>,
) -> DecorrelateResult<ScalarDecorrelateResult> {
    let (cleaned_select, inner_where) =
        scalar_inner_prepare(inner_query, state.aggregate_functions)?;

    let (predicates, residual) = where_clause_correlation_partition(&inner_where, outer_refs)
        .ok_or_else(|| {
            Report::from(DecorrelateError::NonDecorrelatable {
                reason: "unsupported correlation pattern in scalar subquery".to_owned(),
            })
        })?;

    // Extract the scalar expression from the single output column
    // Safety: scalar_inner_prepare validated exactly one column
    let columns = match &cleaned_select.columns {
        ResolvedSelectColumns::Columns(cols) => cols,
        ResolvedSelectColumns::None => &Vec::new(),
    };
    let scalar_expr = match columns.as_slice() {
        [col] => &col.expr,
        _ => {
            return Err(DecorrelateError::NonDecorrelatable {
                reason: "scalar subquery must have exactly one output column".to_owned(),
            }
            .into());
        }
    };

    let has_aggregate = scalar_expr.has_aggregate(state.aggregate_functions);

    // Generate aliases
    let derived_alias = state.next_derived_alias();
    let scalar_alias = state.next_scalar_alias();

    // Build derived table SELECT columns: correlation key columns + scalar expr
    let mut derived_columns: Vec<ResolvedSelectColumn> = predicates
        .iter()
        .map(|p| ResolvedSelectColumn {
            expr: ResolvedScalarExpr::Column(p.inner_column.clone()),
            alias: None,
        })
        .collect();

    derived_columns.push(ResolvedSelectColumn {
        expr: scalar_expr.clone(),
        alias: Some(scalar_alias.clone()),
    });

    // GROUP BY on correlation key columns if the scalar expression has an aggregate
    let group_by = if has_aggregate {
        predicates.iter().map(|p| p.inner_column.clone()).collect()
    } else {
        Vec::new()
    };

    // Build inner select for the derived table
    let derived_select = ResolvedSelectNode {
        distinct: false,
        columns: ResolvedSelectColumns::Columns(derived_columns),
        from: cleaned_select.from.clone(),
        where_clause: residual,
        group_by,
        having: None,
    };

    let derived_query = ResolvedQueryExpr {
        body: ResolvedQueryBody::Select(Box::new(derived_select)),
        order_by: Vec::new(),
        limit: None,
    };

    let derived_table = ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
        query: Box::new(derived_query),
        alias: TableAlias {
            name: EcoString::from(derived_alias.as_str()),
            columns: Vec::new(),
        },
        subquery_kind: SubqueryKind::Scalar,
    });

    // Build JOIN ON: _dcN.inner_col = outer_col for each correlation predicate
    let predicate_to_join_eq = |p: &CorrelationPredicate| {
        let derived_col = synthetic_column_node(
            &derived_alias,
            p.inner_column.column.clone(),
            p.inner_column.column_metadata.clone(),
        );
        ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: boxed_scalar_column(derived_col),
            rexpr: boxed_scalar_column(p.outer_column.clone()),
        })
    };

    let join_condition = predicates.rest.iter().map(predicate_to_join_eq).fold(
        predicate_to_join_eq(&predicates.first),
        |acc, next| {
            ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(acc),
                rexpr: Box::new(next),
            })
        },
    );

    // Build scalar column ref: _dcN._dsN
    let scalar_column_ref = synthetic_column_node(
        &derived_alias,
        scalar_alias.clone(),
        synthetic_text_metadata(scalar_alias.as_str(), 1),
    );

    Ok(ScalarDecorrelateResult {
        derived_table,
        join_condition,
        scalar_column_ref,
    })
}

/// LEFT JOIN a derived table onto the current select's FROM sources.
pub(super) fn left_join_derived(
    from: &[ResolvedTableSource],
    derived_table: ResolvedTableSource,
    join_condition: ResolvedWhereExpr,
) -> Option<Vec<ResolvedTableSource>> {
    let left = from_sources_to_table_source(from)?;
    Some(vec![ResolvedTableSource::Join(Box::new(
        ResolvedJoinNode {
            join_type: JoinType::Left,
            left,
            right: derived_table,
            qual: ResolvedJoinQual::On(join_condition),
        },
    ))])
}

/// Recursively walk a WHERE expression, decorrelating scalar subqueries
/// (`Subquery { sublink_type: Expr, outer_refs non-empty }`) by replacing them
/// with column references to LEFT JOINed derived tables.
///
/// Returns `(new_expr, was_transformed)`. Adds LEFT JOINs to `select.from`.
pub(super) fn conjunct_scalar_decorrelate(
    expr: &ResolvedWhereExpr,
    select: &mut ResolvedSelectNode,
    state: &mut DecorrelateState<'_>,
) -> DecorrelateResult<(ResolvedWhereExpr, bool)> {
    match expr {
        ResolvedWhereExpr::Subquery {
            sublink_type: SubLinkType::Expr,
            outer_refs,
            query,
            ..
        } if !outer_refs.is_empty() => {
            let result = subquery_scalar_decorrelate(query, outer_refs, state)?;
            match left_join_derived(&select.from, result.derived_table, result.join_condition) {
                Some(new_from) => {
                    select.from = new_from;
                    Ok((
                        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(
                            result.scalar_column_ref,
                        )),
                        true,
                    ))
                }
                None => Ok((expr.clone(), false)),
            }
        }
        ResolvedWhereExpr::Binary(binary) => {
            let (new_left, left_transformed) =
                conjunct_scalar_decorrelate(&binary.lexpr, select, state)?;
            let (new_right, right_transformed) =
                conjunct_scalar_decorrelate(&binary.rexpr, select, state)?;
            if left_transformed || right_transformed {
                Ok((
                    ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                        op: binary.op,
                        lexpr: Box::new(new_left),
                        rexpr: Box::new(new_right),
                    }),
                    true,
                ))
            } else {
                Ok((expr.clone(), false))
            }
        }
        ResolvedWhereExpr::Unary(unary) => {
            let (new_inner, was_transformed) =
                conjunct_scalar_decorrelate(&unary.expr, select, state)?;
            if was_transformed {
                Ok((
                    ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
                        op: unary.op,
                        expr: Box::new(new_inner),
                    }),
                    true,
                ))
            } else {
                Ok((expr.clone(), false))
            }
        }
        ResolvedWhereExpr::Scalar(_)
        | ResolvedWhereExpr::Multi(_)
        | ResolvedWhereExpr::Subquery { .. } => Ok((expr.clone(), false)),
    }
}
