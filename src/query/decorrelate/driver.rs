use std::collections::HashSet;

use ecow::EcoString;
use rootcause::Report;

use crate::query::ast::{BinaryOp, SubLinkType, UnaryOp};
use crate::query::resolved::{
    ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedSetOpNode, ResolvedWhereExpr,
};
use crate::query::transform::{where_expr_conjuncts_join, where_expr_conjuncts_split};

use super::exists::*;
use super::in_subquery::*;
use super::predicate::*;
use super::scalar::*;
use super::{DecorrelateError, DecorrelateOutcome, DecorrelateResult, DecorrelateState};

impl<'a> DecorrelateState<'a> {
    fn new(aggregate_functions: &'a HashSet<EcoString>) -> Self {
        Self {
            derived_table_counter: 0,
            scalar_column_counter: 0,
            aggregate_functions,
        }
    }

    pub(super) fn next_derived_alias(&mut self) -> String {
        self.derived_table_counter += 1;
        format!("_dc{}", self.derived_table_counter)
    }

    pub(super) fn next_scalar_alias(&mut self) -> EcoString {
        self.scalar_column_counter += 1;
        EcoString::from(format!("_ds{}", self.scalar_column_counter))
    }
}

/// Decorrelate correlated scalar subqueries in SELECT columns.
///
/// Replaces each correlated scalar subquery with a column reference to a LEFT JOINed
/// derived table. Rejects nested correlation in non-subquery column expressions.
fn select_columns_decorrelate(
    select: &mut ResolvedSelectNode,
    state: &mut DecorrelateState<'_>,
) -> DecorrelateResult<bool> {
    let ResolvedSelectColumns::Columns(cols) = &select.columns else {
        return Ok(false);
    };

    let mut new_cols = Vec::with_capacity(cols.len());
    let mut transformed = false;

    for col in cols {
        match &col.expr {
            ResolvedScalarExpr::Subquery(query, outer_refs) if !outer_refs.is_empty() => {
                let result = subquery_scalar_decorrelate(query, outer_refs, state)?;
                match left_join_derived(&select.from, result.derived_table, result.join_condition) {
                    Some(new_from) => {
                        select.from = new_from;
                        new_cols.push(ResolvedSelectColumn {
                            expr: ResolvedScalarExpr::Column(result.scalar_column_ref),
                            alias: col.alias.clone(),
                        });
                        transformed = true;
                    }
                    None => {
                        new_cols.push(col.clone());
                    }
                }
            }
            other @ (ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(..)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. }) => {
                // Reject nested correlation in non-Subquery exprs (e.g., CASE with
                // correlated subquery) — we don't walk into arbitrary column exprs.
                if scalar_expr_has_correlation(other) {
                    return Err(DecorrelateError::NonDecorrelatable {
                        reason: "correlated subquery nested in SELECT expression".to_owned(),
                    }
                    .into());
                }
                new_cols.push(col.clone());
            }
        }
    }

    select.columns = ResolvedSelectColumns::Columns(new_cols);
    Ok(transformed)
}

/// Apply a join-based decorrelation result to the iteration state.
///
/// On `Some`: replaces current_select with the new node, re-splits its WHERE into
/// remaining_conjuncts so subsequent iterations build on top. Returns true.
/// On `None`: pushes the original conjunct as residual. Returns false.
fn join_result_apply(
    result: Option<ResolvedSelectNode>,
    conjunct: ResolvedWhereExpr,
    current_select: &mut ResolvedSelectNode,
    remaining_conjuncts: &mut Vec<ResolvedWhereExpr>,
) -> bool {
    match result {
        Some(new_select) => {
            *current_select = new_select;
            remaining_conjuncts.clear();
            if let Some(w) = &current_select.where_clause {
                *remaining_conjuncts = where_expr_conjuncts_split(w.clone());
            }
            current_select.where_clause = None;
            true
        }
        None => {
            remaining_conjuncts.push(conjunct);
            false
        }
    }
}

/// Main entry point: decorrelate correlated subqueries in a single SELECT node.
///
/// Walks SELECT columns and WHERE conjuncts looking for correlated subqueries,
/// and flattens them into JOINs. Non-correlated subqueries and non-subquery
/// predicates are left unchanged.
///
/// For EXISTS and IN, inner GROUP BY/HAVING/LIMIT are stripped before decorrelation —
/// this is safe because it produces conservative (over-) invalidation.
/// For IN and NOT IN, the test expression equality with the inner output column is
/// added to the JOIN ON condition alongside correlation predicates from the inner WHERE.
/// For NOT EXISTS and NOT IN, LIMIT is stripped (boolean check, irrelevant), but
/// GROUP BY/HAVING are rejected because the anti-join would under-invalidate.
/// For scalar subqueries, a LEFT JOIN + derived table is used (SELECT list and WHERE).
///
/// Returns `Err(NonDecorrelatable)` if a correlated subquery is found in an
/// unsupported position (HAVING, OR-connected), or if a NOT EXISTS/NOT IN
/// inner subquery has GROUP BY/HAVING.
fn select_node_decorrelate(
    select: &ResolvedSelectNode,
    state: &mut DecorrelateState<'_>,
) -> DecorrelateResult<(ResolvedSelectNode, bool)> {
    let mut current_select = select.clone();
    let mut transformed = false;

    // Phase 1: Decorrelate scalar subqueries in SELECT columns
    transformed |= select_columns_decorrelate(&mut current_select, state)?;

    // Reject correlated subqueries in HAVING
    if let Some(having) = &current_select.having
        && where_expr_has_correlation(having)
    {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated subquery in HAVING clause".to_owned(),
        }
        .into());
    }

    // Phase 2: Decorrelate subqueries in WHERE conjuncts
    let Some(where_clause) = &current_select.where_clause else {
        return Ok((current_select, transformed));
    };

    let conjuncts = where_expr_conjuncts_split(where_clause.clone());
    let mut remaining_conjuncts = Vec::new();

    for conjunct in conjuncts {
        match &conjunct {
            // EXISTS (SELECT ... WHERE correlated)
            ResolvedWhereExpr::Subquery {
                sublink_type: SubLinkType::Exists,
                outer_refs,
                query,
                ..
            } if !outer_refs.is_empty() => {
                current_select.where_clause =
                    where_expr_conjuncts_join(remaining_conjuncts.clone());
                let result = conjunct_exists_try_decorrelate(&current_select, query, outer_refs)?;
                transformed |= join_result_apply(
                    result,
                    conjunct,
                    &mut current_select,
                    &mut remaining_conjuncts,
                );
            }

            // NOT EXISTS (SELECT ... WHERE correlated)
            ResolvedWhereExpr::Unary(unary)
                if unary.op == UnaryOp::Not
                    && matches!(
                        unary.expr.as_ref(),
                        ResolvedWhereExpr::Subquery {
                            sublink_type: SubLinkType::Exists,
                            outer_refs,
                            ..
                        } if !outer_refs.is_empty()
                    ) =>
            {
                let ResolvedWhereExpr::Subquery {
                    outer_refs, query, ..
                } = unary.expr.as_ref()
                else {
                    unreachable!()
                };

                current_select.where_clause =
                    where_expr_conjuncts_join(remaining_conjuncts.clone());
                let result =
                    conjunct_not_exists_try_decorrelate(&current_select, query, outer_refs)?;
                transformed |= join_result_apply(
                    result,
                    conjunct,
                    &mut current_select,
                    &mut remaining_conjuncts,
                );
            }

            // Correlated subquery inside OR — reject
            ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::Or => {
                if where_expr_has_correlation(&conjunct) {
                    return Err(DecorrelateError::NonDecorrelatable {
                        reason: "correlated subquery inside OR".to_owned(),
                    }
                    .into());
                }
                remaining_conjuncts.push(conjunct);
            }

            // IN (SELECT ... WHERE correlated) → INNER JOIN + DISTINCT (semi-join)
            ResolvedWhereExpr::Subquery {
                sublink_type: SubLinkType::Any,
                outer_refs,
                query,
                test_expr,
            } if !outer_refs.is_empty() => {
                let test = test_expr.as_deref().ok_or_else(|| {
                    Report::from(DecorrelateError::NonDecorrelatable {
                        reason: "correlated IN without test expression".to_owned(),
                    })
                })?;
                current_select.where_clause =
                    where_expr_conjuncts_join(remaining_conjuncts.clone());
                let result =
                    conjunct_in_any_try_decorrelate(&current_select, query, outer_refs, test)?;
                transformed |= join_result_apply(
                    result,
                    conjunct,
                    &mut current_select,
                    &mut remaining_conjuncts,
                );
            }

            // NOT IN / ALL (SELECT ... WHERE correlated) → LEFT JOIN + IS NULL (anti-join)
            ResolvedWhereExpr::Subquery {
                sublink_type: SubLinkType::All,
                outer_refs,
                query,
                test_expr,
            } if !outer_refs.is_empty() => {
                let test = test_expr.as_deref().ok_or_else(|| {
                    Report::from(DecorrelateError::NonDecorrelatable {
                        reason: "correlated NOT IN without test expression".to_owned(),
                    })
                })?;
                current_select.where_clause =
                    where_expr_conjuncts_join(remaining_conjuncts.clone());
                let result =
                    conjunct_not_in_all_try_decorrelate(&current_select, query, outer_refs, test)?;
                transformed |= join_result_apply(
                    result,
                    conjunct,
                    &mut current_select,
                    &mut remaining_conjuncts,
                );
            }

            // NOT IN (SELECT ... WHERE correlated) parsed as NOT(ANY) → LEFT JOIN + IS NULL
            ResolvedWhereExpr::Unary(unary)
                if unary.op == UnaryOp::Not
                    && matches!(
                        unary.expr.as_ref(),
                        ResolvedWhereExpr::Subquery {
                            sublink_type: SubLinkType::Any,
                            outer_refs,
                            ..
                        } if !outer_refs.is_empty()
                    ) =>
            {
                let ResolvedWhereExpr::Subquery {
                    outer_refs,
                    query,
                    test_expr,
                    ..
                } = unary.expr.as_ref()
                else {
                    unreachable!();
                };
                let test = test_expr.as_deref().ok_or_else(|| {
                    Report::from(DecorrelateError::NonDecorrelatable {
                        reason: "correlated NOT IN without test expression".to_owned(),
                    })
                })?;
                current_select.where_clause =
                    where_expr_conjuncts_join(remaining_conjuncts.clone());
                let result =
                    conjunct_not_in_all_try_decorrelate(&current_select, query, outer_refs, test)?;
                transformed |= join_result_apply(
                    result,
                    conjunct,
                    &mut current_select,
                    &mut remaining_conjuncts,
                );
            }

            // NOT wrapping a correlated non-EXISTS/non-IN subquery
            ResolvedWhereExpr::Unary(unary) if unary.op == UnaryOp::Not => {
                if matches!(
                    unary.expr.as_ref(),
                    ResolvedWhereExpr::Subquery { outer_refs, .. } if !outer_refs.is_empty()
                ) {
                    return Err(DecorrelateError::NonDecorrelatable {
                        reason: "correlated NOT-wrapped non-EXISTS subquery".to_owned(),
                    }
                    .into());
                }
                remaining_conjuncts.push(conjunct);
            }

            // Catch-all: non-correlated or non-subquery predicates, and scalar
            // correlated subqueries embedded in expressions (e.g., col > (SELECT ...))
            ResolvedWhereExpr::Scalar(_)
            | ResolvedWhereExpr::Unary(_)
            | ResolvedWhereExpr::Binary(_)
            | ResolvedWhereExpr::Multi(_)
            | ResolvedWhereExpr::Subquery { .. } => {
                if where_expr_has_correlation(&conjunct) {
                    let (new_conjunct, was_transformed) =
                        conjunct_scalar_decorrelate(&conjunct, &mut current_select, state)?;
                    remaining_conjuncts.push(new_conjunct);
                    transformed |= was_transformed;
                } else {
                    remaining_conjuncts.push(conjunct);
                }
            }
        }
    }

    current_select.where_clause = where_expr_conjuncts_join(remaining_conjuncts);
    Ok((current_select, transformed))
}

/// Top-level entry: decorrelate correlated subqueries in a resolved query expression.
///
/// Handles SELECT bodies directly, and recursively processes SetOp branches.
/// Returns `DecorrelateOutcome` with the (possibly transformed) query and a
/// flag indicating whether any transformation occurred.
///
/// `aggregate_functions` is the set of aggregate function names from pg_proc,
/// used to decide whether derived tables need GROUP BY during scalar decorrelation.
pub fn query_expr_decorrelate(
    resolved: &ResolvedQueryExpr,
    aggregate_functions: &HashSet<EcoString>,
) -> DecorrelateResult<DecorrelateOutcome> {
    let mut state = DecorrelateState::new(aggregate_functions);
    query_expr_decorrelate_inner(resolved, &mut state)
}

fn query_expr_decorrelate_inner(
    resolved: &ResolvedQueryExpr,
    state: &mut DecorrelateState<'_>,
) -> DecorrelateResult<DecorrelateOutcome> {
    match &resolved.body {
        ResolvedQueryBody::Select(select) => {
            let (new_select, transformed) = select_node_decorrelate(select, state)?;
            Ok(DecorrelateOutcome {
                resolved: ResolvedQueryExpr {
                    body: ResolvedQueryBody::Select(Box::new(new_select)),
                    order_by: resolved.order_by.clone(),
                    limit: resolved.limit.clone(),
                },
                transformed,
            })
        }
        ResolvedQueryBody::SetOp(set_op) => {
            let left_outcome = query_expr_decorrelate_inner(&set_op.left, state)?;
            let right_outcome = query_expr_decorrelate_inner(&set_op.right, state)?;
            let transformed = left_outcome.transformed || right_outcome.transformed;
            Ok(DecorrelateOutcome {
                resolved: ResolvedQueryExpr {
                    body: ResolvedQueryBody::SetOp(ResolvedSetOpNode {
                        op: set_op.op,
                        all: set_op.all,
                        left: Box::new(left_outcome.resolved),
                        right: Box::new(right_outcome.resolved),
                    }),
                    order_by: resolved.order_by.clone(),
                    limit: resolved.limit.clone(),
                },
                transformed,
            })
        }
        ResolvedQueryBody::Values(_) => Ok(DecorrelateOutcome {
            resolved: resolved.clone(),
            transformed: false,
        }),
    }
}
