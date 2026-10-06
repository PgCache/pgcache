use std::collections::HashSet;

use ecow::EcoString;
use rootcause::Report;

use super::exists::{conjunct_exists_try_decorrelate, conjunct_not_exists_try_decorrelate};
use super::in_subquery::{conjunct_in_any_try_decorrelate, conjunct_not_in_all_try_decorrelate};
use super::predicate::{scalar_expr_has_correlation, where_expr_has_correlation};
use super::scalar::{conjunct_scalar_decorrelate, left_join_derived, subquery_scalar_decorrelate};
use super::{DecorrelateError, DecorrelateOutcome, DecorrelateResult, DecorrelateState};
use crate::query::ast::{BinaryOp, SubLinkType, UnaryOp};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumn, ResolvedSelectColumns, ResolvedSelectNode, ResolvedSetOpNode,
    ResolvedTableSource, ResolvedWhereExpr,
};
use crate::query::transform::{where_expr_conjuncts_join, where_expr_conjuncts_split};

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
        let (new_col, col_transformed) = select_column_decorrelate(col, &mut select.from, state)?;
        new_cols.push(new_col);
        transformed |= col_transformed;
    }

    select.columns = ResolvedSelectColumns::Columns(new_cols);
    Ok(transformed)
}

/// One SELECT column: a correlated scalar subquery becomes a column of a LEFT
/// JOINed derived table added to `from`; any other correlated expression is
/// rejected; everything else is kept.
fn select_column_decorrelate(
    col: &ResolvedSelectColumn,
    from: &mut Vec<ResolvedTableSource>,
    state: &mut DecorrelateState<'_>,
) -> DecorrelateResult<(ResolvedSelectColumn, bool)> {
    if let ResolvedScalarExpr::Subquery(query, outer_refs) = &col.expr
        && !outer_refs.is_empty()
    {
        let result = subquery_scalar_decorrelate(query, outer_refs, state)?;
        let Some(new_from) = left_join_derived(from, result.derived_table, result.join_condition)
        else {
            return Ok((col.clone(), false));
        };
        *from = new_from;
        let column = ResolvedSelectColumn {
            expr: ResolvedScalarExpr::Column(result.scalar_column_ref),
            alias: col.alias.clone(),
        };
        return Ok((column, true));
    }
    // Reject nested correlation in non-Subquery exprs (e.g., CASE with
    // correlated subquery) — we don't walk into arbitrary column exprs.
    if scalar_expr_has_correlation(&col.expr) {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated subquery nested in SELECT expression".to_owned(),
        }
        .into());
    }
    Ok((col.clone(), false))
}

/// A WHERE conjunct classified as a join-based decorrelation target, with the
/// inner query / outer refs / test expression borrowed out of the conjunct. This
/// is the family that flattens to a JOIN (semi-join for EXISTS/IN, anti-join for
/// NOT EXISTS/NOT IN); scalar subqueries and residual predicates are handled
/// separately by the caller.
enum JoinDecorrelation<'a> {
    /// EXISTS → INNER JOIN + DISTINCT (semi-join).
    Exists {
        query: &'a ResolvedQueryExpr,
        outer_refs: &'a [ResolvedColumnNode],
    },
    /// NOT EXISTS → LEFT JOIN + IS NULL (anti-join).
    NotExists {
        query: &'a ResolvedQueryExpr,
        outer_refs: &'a [ResolvedColumnNode],
    },
    /// IN / ANY → INNER JOIN + DISTINCT (semi-join).
    InAny {
        query: &'a ResolvedQueryExpr,
        outer_refs: &'a [ResolvedColumnNode],
        test: &'a ResolvedScalarExpr,
    },
    /// NOT IN / ALL → LEFT JOIN + IS NULL (anti-join).
    NotInAll {
        query: &'a ResolvedQueryExpr,
        outer_refs: &'a [ResolvedColumnNode],
        test: &'a ResolvedScalarExpr,
    },
}

impl JoinDecorrelation<'_> {
    /// Dispatch to the family-specific conjunct decorrelation against the current
    /// outer SELECT.
    fn try_decorrelate(
        &self,
        current_select: &ResolvedSelectNode,
    ) -> DecorrelateResult<Option<ResolvedSelectNode>> {
        match *self {
            JoinDecorrelation::Exists { query, outer_refs } => {
                conjunct_exists_try_decorrelate(current_select, query, outer_refs)
            }
            JoinDecorrelation::NotExists { query, outer_refs } => {
                conjunct_not_exists_try_decorrelate(current_select, query, outer_refs)
            }
            JoinDecorrelation::InAny {
                query,
                outer_refs,
                test,
            } => conjunct_in_any_try_decorrelate(current_select, query, outer_refs, test),
            JoinDecorrelation::NotInAll {
                query,
                outer_refs,
                test,
            } => conjunct_not_in_all_try_decorrelate(current_select, query, outer_refs, test),
        }
    }
}

/// A correlated subquery predicate, borrowed out of a WHERE expression.
#[derive(Clone, Copy)]
struct CorrelatedSubquery<'a> {
    sublink_type: SubLinkType,
    query: &'a ResolvedQueryExpr,
    outer_refs: &'a [ResolvedColumnNode],
    test_expr: Option<&'a ResolvedScalarExpr>,
}

impl<'a> CorrelatedSubquery<'a> {
    /// `expr` as a subquery predicate that references the outer query.
    fn of(expr: &'a ResolvedWhereExpr) -> Option<Self> {
        let ResolvedWhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
            outer_refs,
        } = expr
        else {
            return None;
        };
        (!outer_refs.is_empty()).then(|| Self {
            sublink_type: *sublink_type,
            query,
            outer_refs,
            test_expr: test_expr.as_deref(),
        })
    }

    /// The IN / NOT IN test expression, which a join rewrite requires.
    fn test(&self, label: &str) -> DecorrelateResult<&'a ResolvedScalarExpr> {
        self.test_expr.ok_or_else(|| {
            Report::from(DecorrelateError::NonDecorrelatable {
                reason: format!("correlated {label} without test expression"),
            })
        })
    }
}

/// Classify a WHERE conjunct as a join-based decorrelation target.
///
/// Returns `Ok(Some(_))` for a correlated EXISTS / NOT EXISTS / IN / NOT IN
/// conjunct (including the `NOT(EXISTS)` / `NOT(ANY)` spellings), `Ok(None)` for
/// anything else (the caller's OR/scalar/residual path), and `Err` when an
/// IN/NOT IN subquery is missing its test expression.
fn conjunct_join_classify(
    conjunct: &ResolvedWhereExpr,
) -> DecorrelateResult<Option<JoinDecorrelation<'_>>> {
    let (negated, predicate) = match conjunct {
        ResolvedWhereExpr::Unary(unary) if unary.op == UnaryOp::Not => (true, unary.expr.as_ref()),
        ResolvedWhereExpr::Scalar(_)
        | ResolvedWhereExpr::Unary(_)
        | ResolvedWhereExpr::Binary(_)
        | ResolvedWhereExpr::Multi(_)
        | ResolvedWhereExpr::Subquery { .. } => (false, conjunct),
    };
    let Some(subquery) = CorrelatedSubquery::of(predicate) else {
        return Ok(None);
    };
    let CorrelatedSubquery {
        query, outer_refs, ..
    } = subquery;
    let kind = match (negated, subquery.sublink_type) {
        (false, SubLinkType::Exists) => JoinDecorrelation::Exists { query, outer_refs },
        (true, SubLinkType::Exists) => JoinDecorrelation::NotExists { query, outer_refs },
        (false, SubLinkType::Any) => JoinDecorrelation::InAny {
            query,
            outer_refs,
            test: subquery.test("IN")?,
        },
        // `x NOT IN (...)` parses as NOT(ANY); `x <> ALL (...)` — the only ALL
        // form AST conversion admits — is the same predicate spelled as a bare ALL.
        (true, SubLinkType::Any) | (false, SubLinkType::All) => JoinDecorrelation::NotInAll {
            query,
            outer_refs,
            test: subquery.test("NOT IN")?,
        },
        (false, SubLinkType::Expr) | (true, SubLinkType::All | SubLinkType::Expr) => {
            return Ok(None);
        }
    };
    Ok(Some(kind))
}

/// What the residual path does with a conjunct that isn't join-family.
enum Residual {
    Keep,
    /// A correlated scalar subquery embedded in an expression (e.g.
    /// `col > (SELECT ...)`), decorrelated through a LEFT JOIN.
    ScalarDecorrelate,
}

/// Classify a non-join conjunct, rejecting correlation under OR or under a
/// NOT the join family doesn't cover.
fn residual_classify(conjunct: &ResolvedWhereExpr) -> DecorrelateResult<Residual> {
    let correlated = where_expr_has_correlation(conjunct);
    match conjunct {
        ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::Or => {
            if correlated {
                return Err(DecorrelateError::NonDecorrelatable {
                    reason: "correlated subquery inside OR".to_owned(),
                }
                .into());
            }
            Ok(Residual::Keep)
        }
        ResolvedWhereExpr::Unary(unary) if unary.op == UnaryOp::Not => {
            if CorrelatedSubquery::of(&unary.expr).is_some() {
                return Err(DecorrelateError::NonDecorrelatable {
                    reason: "correlated NOT-wrapped non-EXISTS subquery".to_owned(),
                }
                .into());
            }
            Ok(Residual::Keep)
        }
        ResolvedWhereExpr::Scalar(_)
        | ResolvedWhereExpr::Unary(_)
        | ResolvedWhereExpr::Binary(_)
        | ResolvedWhereExpr::Multi(_)
        | ResolvedWhereExpr::Subquery { .. } => Ok(if correlated {
            Residual::ScalarDecorrelate
        } else {
            Residual::Keep
        }),
    }
}

/// The WHERE-conjunct pass over one SELECT: the node as rewritten so far, the
/// conjuncts still to rejoin as its WHERE, and whether anything changed.
struct WhereDecorrelation {
    select: ResolvedSelectNode,
    remaining: Vec<ResolvedWhereExpr>,
    transformed: bool,
}

impl WhereDecorrelation {
    /// Flatten a join-family conjunct against the node built so far. On success
    /// the rewritten node's WHERE is re-split into `remaining` so later
    /// conjuncts build on top; otherwise the conjunct stays as a residual.
    fn join_apply(
        &mut self,
        kind: &JoinDecorrelation<'_>,
        conjunct: &ResolvedWhereExpr,
    ) -> DecorrelateResult<()> {
        self.select.where_clause = where_expr_conjuncts_join(self.remaining.clone());
        match kind.try_decorrelate(&self.select)? {
            Some(new_select) => {
                self.select = new_select;
                self.remaining = self
                    .select
                    .where_clause
                    .take()
                    .map(where_expr_conjuncts_split)
                    .unwrap_or_default();
                self.transformed = true;
            }
            None => self.remaining.push(conjunct.clone()),
        }
        Ok(())
    }

    fn residual_apply(
        &mut self,
        conjunct: ResolvedWhereExpr,
        state: &mut DecorrelateState<'_>,
    ) -> DecorrelateResult<()> {
        match residual_classify(&conjunct)? {
            Residual::Keep => self.remaining.push(conjunct),
            Residual::ScalarDecorrelate => {
                let (new_conjunct, was_transformed) =
                    conjunct_scalar_decorrelate(&conjunct, &mut self.select, state)?;
                self.remaining.push(new_conjunct);
                self.transformed |= was_transformed;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> (ResolvedSelectNode, bool) {
        self.select.where_clause = where_expr_conjuncts_join(self.remaining);
        (self.select, self.transformed)
    }
}

fn having_correlation_reject(select: &ResolvedSelectNode) -> DecorrelateResult<()> {
    if let Some(having) = &select.having
        && where_expr_has_correlation(having)
    {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated subquery in HAVING clause".to_owned(),
        }
        .into());
    }
    Ok(())
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

    // Phase 1: Decorrelate scalar subqueries in SELECT columns
    let columns_transformed = select_columns_decorrelate(&mut current_select, state)?;
    having_correlation_reject(&current_select)?;

    // Phase 2: Decorrelate subqueries in WHERE conjuncts. EXISTS / NOT EXISTS /
    // IN / NOT IN flatten to a JOIN; everything else takes the residual path.
    let Some(where_clause) = current_select.where_clause.clone() else {
        return Ok((current_select, columns_transformed));
    };
    let mut pass = WhereDecorrelation {
        select: current_select,
        remaining: Vec::new(),
        transformed: columns_transformed,
    };
    for conjunct in where_expr_conjuncts_split(where_clause) {
        match conjunct_join_classify(&conjunct)? {
            Some(kind) => pass.join_apply(&kind, &conjunct)?,
            None => pass.residual_apply(conjunct, state)?,
        }
    }
    Ok(pass.finish())
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
