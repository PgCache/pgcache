use std::collections::HashSet;

use ecow::EcoString;

use crate::query::ast::{BinaryOp, JoinType};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedJoinQual, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectNode, ResolvedTableSource,
    ResolvedWhereExpr,
};
use crate::query::transform::{where_expr_conjuncts_join, where_expr_conjuncts_split};

use super::{CorrelationPredicate, CorrelationPredicates, DecorrelateError, DecorrelateResult};

/// Wrap a resolved column node as a boxed `WhereExpr::Scalar(Column(...))` leaf.
pub(super) fn boxed_scalar_column(col: ResolvedColumnNode) -> Box<ResolvedWhereExpr> {
    Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)))
}

impl CorrelationPredicates {
    pub(super) fn iter(&self) -> impl Iterator<Item = &CorrelationPredicate> {
        std::iter::once(&self.first).chain(&self.rest)
    }
}

/// Effective table identifier for matching: uses alias when present, otherwise table name.
///
/// Self-joins alias the same table differently (e.g., `departments d` / `departments d2`),
/// so the alias is needed to distinguish outer refs from inner columns.
fn effective_table(col: &ResolvedColumnNode) -> &str {
    col.table_alias
        .as_ref()
        .map(EcoString::as_str)
        .unwrap_or(col.table.as_str())
}

/// Build a lookup set of `(schema, effective_table, column)` tuples from outer_refs.
fn outer_ref_keys(outer_refs: &[ResolvedColumnNode]) -> HashSet<(&str, &str, &str)> {
    outer_refs
        .iter()
        .map(|col| {
            (
                col.schema.as_str(),
                effective_table(col),
                col.column.as_str(),
            )
        })
        .collect()
}

/// Check if a column matches any entry in the outer_refs key set.
fn column_matches_outer_ref(
    col: &ResolvedColumnNode,
    outer_keys: &HashSet<(&str, &str, &str)>,
) -> bool {
    outer_keys.contains(&(
        col.schema.as_str(),
        effective_table(col),
        col.column.as_str(),
    ))
}

/// Partition an inner subquery's WHERE clause into correlation predicates and residual predicates.
///
/// Correlation predicates are equalities where one side matches an outer_ref and the other
/// references an inner table. Returns `None` if no correlation predicates are found, or if
/// an unsupported correlation pattern is detected (e.g., non-equality with an outer ref).
pub(super) fn where_clause_correlation_partition(
    where_clause: &ResolvedWhereExpr,
    outer_refs: &[ResolvedColumnNode],
) -> Option<(CorrelationPredicates, Option<ResolvedWhereExpr>)> {
    let outer_keys = outer_ref_keys(outer_refs);
    let conjuncts = where_expr_conjuncts_split(where_clause.clone());

    let mut correlation_predicates = Vec::new();
    let mut residual = Vec::new();

    for conjunct in conjuncts {
        match &conjunct {
            ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::Equal => {
                // Check if this is a Column = Column with one side being an outer ref
                match (binary.lexpr.as_ref(), binary.rexpr.as_ref()) {
                    (
                        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(left)),
                        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(right)),
                    ) => {
                        let left_is_outer = column_matches_outer_ref(left, &outer_keys);
                        let right_is_outer = column_matches_outer_ref(right, &outer_keys);

                        if left_is_outer && !right_is_outer {
                            correlation_predicates.push(CorrelationPredicate {
                                outer_column: left.clone(),
                                inner_column: right.clone(),
                            });
                        } else if right_is_outer && !left_is_outer {
                            correlation_predicates.push(CorrelationPredicate {
                                outer_column: right.clone(),
                                inner_column: left.clone(),
                            });
                        } else {
                            // Both outer or both inner — treat as residual
                            residual.push(conjunct);
                        }
                    }
                    _ => {
                        // Non-column equality — check if it references outer refs
                        if conjunct_references_outer_ref(&conjunct, &outer_keys) {
                            // Non-column-to-column correlation — unsupported
                            return None;
                        }
                        residual.push(conjunct);
                    }
                }
            }
            ResolvedWhereExpr::Scalar(_)
            | ResolvedWhereExpr::Unary(_)
            | ResolvedWhereExpr::Binary(_)
            | ResolvedWhereExpr::Multi(_)
            | ResolvedWhereExpr::Subquery { .. } => {
                // Non-equality predicate — check if it references outer refs
                if conjunct_references_outer_ref(&conjunct, &outer_keys) {
                    // Non-equality correlation — unsupported
                    return None;
                }
                residual.push(conjunct);
            }
        }
    }

    let mut iter = correlation_predicates.into_iter();
    let first = iter.next()?;
    let rest = iter.collect();

    let residual_where = where_expr_conjuncts_join(residual);
    Some((CorrelationPredicates { first, rest }, residual_where))
}

/// Check whether a WHERE expression references any column matching the outer_ref keys.
fn conjunct_references_outer_ref(
    expr: &ResolvedWhereExpr,
    outer_keys: &HashSet<(&str, &str, &str)>,
) -> bool {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => scalar_references_outer_ref(scalar, outer_keys),
        ResolvedWhereExpr::Unary(u) => conjunct_references_outer_ref(&u.expr, outer_keys),
        ResolvedWhereExpr::Binary(b) => {
            conjunct_references_outer_ref(&b.lexpr, outer_keys)
                || conjunct_references_outer_ref(&b.rexpr, outer_keys)
        }
        ResolvedWhereExpr::Multi(m) => m
            .exprs
            .iter()
            .any(|e| conjunct_references_outer_ref(e, outer_keys)),
        ResolvedWhereExpr::Subquery { .. } => false,
    }
}

fn scalar_references_outer_ref(
    expr: &ResolvedScalarExpr,
    outer_keys: &HashSet<(&str, &str, &str)>,
) -> bool {
    match expr {
        ResolvedScalarExpr::Column(col) => column_matches_outer_ref(col, outer_keys),
        ResolvedScalarExpr::Function(func) => func
            .args
            .iter()
            .any(|a| scalar_references_outer_ref(a, outer_keys)),
        ResolvedScalarExpr::Arithmetic(arith) => {
            scalar_references_outer_ref(&arith.left, outer_keys)
                || scalar_references_outer_ref(&arith.right, outer_keys)
        }
        ResolvedScalarExpr::Case(case) => {
            case.arg
                .as_ref()
                .is_some_and(|a| scalar_references_outer_ref(a, outer_keys))
                || case.whens.iter().any(|w| {
                    conjunct_references_outer_ref(&w.condition, outer_keys)
                        || scalar_references_outer_ref(&w.result, outer_keys)
                })
                || case
                    .default
                    .as_ref()
                    .is_some_and(|d| scalar_references_outer_ref(d, outer_keys))
        }
        ResolvedScalarExpr::Array(elems) => elems
            .iter()
            .any(|e| scalar_references_outer_ref(e, outer_keys)),
        ResolvedScalarExpr::TypeCast { expr, .. } => scalar_references_outer_ref(expr, outer_keys),
        ResolvedScalarExpr::Literal(_)
        | ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Subquery(_, _) => false,
    }
}

/// Build a JOIN ON condition from correlation predicates.
pub(super) fn correlation_predicates_to_condition(
    predicates: &CorrelationPredicates,
) -> ResolvedWhereExpr {
    let first = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
        op: BinaryOp::Equal,
        lexpr: boxed_scalar_column(predicates.first.inner_column.clone()),
        rexpr: boxed_scalar_column(predicates.first.outer_column.clone()),
    });

    predicates.rest.iter().fold(first, |acc, p| {
        let condition = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: boxed_scalar_column(p.inner_column.clone()),
            rexpr: boxed_scalar_column(p.outer_column.clone()),
        });
        ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(acc),
            rexpr: Box::new(condition),
        })
    })
}

/// Extract the inner SELECT node and its FROM sources from a subquery's ResolvedQueryExpr.
/// Returns None if the inner query isn't a simple SELECT.
pub(super) fn inner_query_select(query: &ResolvedQueryExpr) -> Option<&ResolvedSelectNode> {
    match &query.body {
        ResolvedQueryBody::Select(select) => Some(select),
        ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
    }
}

/// Combine a list of FROM sources into a single ResolvedTableSource.
///
/// If there's exactly one source, returns it directly. Multiple sources are
/// combined into a chain of cross joins (no condition).
/// Returns `None` if `sources` is empty.
pub(super) fn from_sources_to_table_source(
    sources: &[ResolvedTableSource],
) -> Option<ResolvedTableSource> {
    sources.iter().cloned().reduce(|acc, next| {
        ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
            join_type: JoinType::Inner,
            left: acc,
            right: next,
            qual: ResolvedJoinQual::Cross,
        }))
    })
}

/// Merge two optional WHERE clauses with AND.
pub(super) fn merge_where_clauses(
    outer: &Option<ResolvedWhereExpr>,
    inner: &Option<ResolvedWhereExpr>,
) -> Option<ResolvedWhereExpr> {
    match (outer, inner) {
        (Some(o), Some(i)) => Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(o.clone()),
            rexpr: Box::new(i.clone()),
        })),
        (Some(o), None) => Some(o.clone()),
        (None, Some(i)) => Some(i.clone()),
        (None, None) => None,
    }
}

/// Check whether a ResolvedScalarExpr contains any correlated subquery (non-empty outer_refs).
pub(super) fn scalar_expr_has_correlation(expr: &ResolvedScalarExpr) -> bool {
    match expr {
        ResolvedScalarExpr::Subquery(_, outer_refs) => !outer_refs.is_empty(),
        ResolvedScalarExpr::Function(func) => func.args.iter().any(scalar_expr_has_correlation),
        ResolvedScalarExpr::Case(case) => {
            case.arg
                .as_ref()
                .is_some_and(|a| scalar_expr_has_correlation(a))
                || case.whens.iter().any(|w| {
                    where_expr_has_correlation(&w.condition)
                        || scalar_expr_has_correlation(&w.result)
                })
                || case
                    .default
                    .as_ref()
                    .is_some_and(|d| scalar_expr_has_correlation(d))
        }
        ResolvedScalarExpr::Arithmetic(arith) => {
            scalar_expr_has_correlation(&arith.left) || scalar_expr_has_correlation(&arith.right)
        }
        ResolvedScalarExpr::Array(elems) => elems.iter().any(scalar_expr_has_correlation),
        ResolvedScalarExpr::TypeCast { expr, .. } => scalar_expr_has_correlation(expr),
        ResolvedScalarExpr::Column(_)
        | ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Literal(_) => false,
    }
}

/// Check whether a ResolvedWhereExpr contains any correlated subquery.
pub(super) fn where_expr_has_correlation(expr: &ResolvedWhereExpr) -> bool {
    match expr {
        ResolvedWhereExpr::Subquery { outer_refs, .. } => !outer_refs.is_empty(),
        ResolvedWhereExpr::Scalar(scalar) => scalar_expr_has_correlation(scalar),
        ResolvedWhereExpr::Unary(u) => where_expr_has_correlation(&u.expr),
        ResolvedWhereExpr::Binary(b) => {
            where_expr_has_correlation(&b.lexpr) || where_expr_has_correlation(&b.rexpr)
        }
        ResolvedWhereExpr::Multi(m) => m.exprs.iter().any(where_expr_has_correlation),
    }
}

/// Prepare a correlated EXISTS inner query for decorrelation by stripping
/// clauses that are safe to remove for update query (CDC invalidation) purposes.
///
/// EXISTS is a boolean "does any row match?" check. For invalidation we only need
/// to know which outer rows *could* be affected — so stripping GROUP BY, HAVING,
/// and LIMIT produces a conservative (over-invalidation) result that is always safe.
///
/// Returns the cleaned inner query and select node.
pub(super) fn correlated_exists_inner_prepare(
    inner_query: &ResolvedQueryExpr,
    inner_select: &ResolvedSelectNode,
) -> (ResolvedQueryExpr, ResolvedSelectNode) {
    let cleaned_select = ResolvedSelectNode {
        group_by: Vec::new(),
        having: None,
        ..inner_select.clone()
    };
    let cleaned_query = ResolvedQueryExpr {
        limit: None,
        body: ResolvedQueryBody::Select(Box::new(cleaned_select.clone())),
        order_by: inner_query.order_by.clone(),
    };
    (cleaned_query, cleaned_select)
}

/// Prepare a correlated NOT EXISTS inner query for decorrelation.
///
/// LIMIT is safe to strip (NOT EXISTS is a boolean check, LIMIT doesn't change the answer).
/// GROUP BY and HAVING are NOT safe to strip for NOT EXISTS — the anti-join pattern
/// (LEFT JOIN + IS NULL) would under-invalidate because it tests for zero matching rows,
/// while the original NOT EXISTS tests for no rows surviving the GROUP BY/HAVING filter.
pub(super) fn correlated_not_exists_inner_prepare(
    inner_query: &ResolvedQueryExpr,
    inner_select: &ResolvedSelectNode,
) -> DecorrelateResult<(ResolvedQueryExpr, ResolvedSelectNode)> {
    if !inner_select.group_by.is_empty() {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated NOT EXISTS with GROUP BY".to_owned(),
        }
        .into());
    }
    if inner_select.having.is_some() {
        return Err(DecorrelateError::NonDecorrelatable {
            reason: "correlated NOT EXISTS with HAVING".to_owned(),
        }
        .into());
    }
    let cleaned_query = ResolvedQueryExpr {
        limit: None,
        body: inner_query.body.clone(),
        order_by: inner_query.order_by.clone(),
    };
    Ok((cleaned_query, inner_select.clone()))
}
