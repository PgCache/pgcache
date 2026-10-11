//! EXISTS and IN as semi-joins (INNER JOIN + DISTINCT), NOT EXISTS and
//! NOT IN as anti-joins (LEFT JOIN + IS NULL), ADR-014 and ADR-016/017.

use super::scope::{
    Scope, correlated_subquery, correlations_condition, scalar_moved, source_moved, sources_joined,
    where_and, where_correlation_partition, where_merge, where_moved,
};
use super::{Decorrelate, rejected};
use crate::query::ast::{
    ColumnNode, JoinNode, JoinQual, JoinType, NodeId, QueryBody, QueryExpr, ScalarExpr,
    SelectColumn, SelectColumns, SelectNode, SubLinkType, TableSource, UnaryExpr, UnaryOp,
    WhereExpr,
};
use crate::query::decorrelate::DecorrelateResult;

/// A WHERE conjunct that flattens to a join, with the inner query and (for
/// IN / NOT IN) the test expression borrowed out of it.
pub(super) enum JoinDecorrelation<'a> {
    Exists {
        query: &'a QueryExpr,
    },
    NotExists {
        query: &'a QueryExpr,
    },
    InAny {
        query: &'a QueryExpr,
        test: &'a ScalarExpr,
    },
    NotInAll {
        query: &'a QueryExpr,
        test: &'a ScalarExpr,
    },
}

impl<'a> JoinDecorrelation<'a> {
    /// Classify a conjunct: correlated EXISTS / NOT EXISTS / IN / NOT IN
    /// (including the `NOT(EXISTS)` / `NOT(ANY)` spellings); `None` for
    /// anything else; `Err` when an IN / NOT IN lacks its test expression.
    pub(super) fn classify(
        state: &Decorrelate<'_>,
        conjunct: &'a WhereExpr,
    ) -> DecorrelateResult<Option<Self>> {
        let (negated, predicate) = match conjunct {
            WhereExpr::Unary(unary) if unary.op == UnaryOp::Not => (true, unary.expr.as_ref()),
            WhereExpr::Scalar(_)
            | WhereExpr::Unary(_)
            | WhereExpr::Binary(_)
            | WhereExpr::Multi(_)
            | WhereExpr::Subquery { .. } => (false, conjunct),
        };
        let Some(subquery) = correlated_subquery(state, predicate) else {
            return Ok(None);
        };
        let query = subquery.query;
        let test = |label: &str| {
            subquery
                .test_expr
                .ok_or_else(|| rejected(&format!("correlated {label} without test expression")))
        };
        Ok(Some(match (negated, subquery.sublink_type) {
            (false, SubLinkType::Exists) => Self::Exists { query },
            (true, SubLinkType::Exists) => Self::NotExists { query },
            (false, SubLinkType::Any) => Self::InAny {
                query,
                test: test("IN")?,
            },
            // `x NOT IN (...)` parses as NOT(ANY); `x <> ALL (...)` is the
            // same predicate spelled as a bare ALL.
            (true, SubLinkType::Any) | (false, SubLinkType::All) => Self::NotInAll {
                query,
                test: test("NOT IN")?,
            },
            (false, SubLinkType::Expr) | (true, SubLinkType::All | SubLinkType::Expr) => {
                return Ok(None);
            }
        }))
    }

    /// The outer SELECT with this conjunct flattened into its FROM;
    /// `Ok(None)` when the inner shape has no join form (the conjunct stays).
    pub(super) fn try_decorrelate(
        &self,
        state: &mut Decorrelate<'_>,
        outer: &SelectNode,
    ) -> DecorrelateResult<Option<SelectNode>> {
        let (query, anti, test) = match *self {
            Self::Exists { query } => (query, false, None),
            Self::NotExists { query } => (query, true, None),
            Self::InAny { query, test } => (query, false, Some(test)),
            Self::NotInAll { query, test } => (query, true, Some(test)),
        };
        let Some(inner) = inner_select(query) else {
            return Ok(None);
        };
        // A semi-join may drop GROUP BY / HAVING / LIMIT (it can only
        // over-invalidate); an anti-join would under-invalidate, so it
        // keeps nothing it cannot honour.
        if anti {
            if !inner.group_by.is_empty() {
                return Err(rejected("correlated NOT EXISTS with GROUP BY"));
            }
            if inner.having.is_some() {
                return Err(rejected("correlated NOT EXISTS with HAVING"));
            }
        }
        let Some(inner_where) = &inner.where_clause else {
            return Ok(None);
        };
        let output = match test {
            Some(_) => Some(inner_output_column(inner)?.clone()),
            None => None,
        };
        let scope = Scope::of_query(state, query);
        let label = match (anti, test) {
            (false, None) => "EXISTS",
            (true, None) => "NOT EXISTS",
            (false, Some(_)) => "IN",
            (true, Some(_)) => "NOT IN",
        };
        let (predicates, residual) = where_correlation_partition(state, &scope, inner_where)
            .ok_or_else(|| rejected(&format!("unsupported correlation pattern in {label}")))?;

        let destination = Scope::of_select(state, outer);
        let correlation = correlations_condition(state, &predicates, &destination);
        let condition = match (test, &output) {
            (Some(test), Some(output)) => where_and(
                super::scope::where_equal(
                    scalar_moved(state, test, &destination),
                    super::scope::column_moved(state, output, &destination),
                ),
                correlation,
            ),
            _ => correlation,
        };
        let residual = residual.map(|residual| where_moved(state, &residual, &destination));
        let (on, where_clause) = if anti {
            // Residual inner predicates go into the ON clause to keep the
            // LEFT JOIN's NULL-padded rows; the IS NULL on an inner
            // correlation column (or the IN output) filters to them.
            let null_test = match &output {
                Some(output) => super::scope::column_moved(state, output, &destination),
                None => super::scope::column_moved(state, &predicates.first.inner, &destination),
            };
            let is_null = WhereExpr::Unary(UnaryExpr {
                id: NodeId::PENDING,
                op: UnaryOp::IsNull,
                expr: Box::new(WhereExpr::Scalar(null_test)),
            });
            (
                where_merge(Some(condition), residual).unwrap_or_else(|| {
                    debug_assert!(false, "an anti-join always has a condition");
                    WhereExpr::Unary(UnaryExpr {
                        id: NodeId::PENDING,
                        op: UnaryOp::Not,
                        expr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
                            crate::query::ast::LiteralValue::Boolean(true),
                        ))),
                    })
                }),
                Some(
                    where_merge(outer.where_clause.clone(), Some(is_null)).unwrap_or(
                        WhereExpr::Scalar(ScalarExpr::Literal(
                            crate::query::ast::LiteralValue::Boolean(true),
                        )),
                    ),
                ),
            )
        } else {
            (condition, where_merge(outer.where_clause.clone(), residual))
        };

        let inner_sources: Vec<TableSource> = inner
            .from
            .iter()
            .map(|source| source_moved(state, source, &destination))
            .collect();
        let (Some(right), Some(left)) = (
            sources_joined(inner_sources),
            sources_joined(outer.from.iter().cloned().collect()),
        ) else {
            return Ok(None);
        };
        let join = TableSource::Join(JoinNode {
            id: NodeId::PENDING,
            join_type: if anti {
                JoinType::Left
            } else {
                JoinType::Inner
            },
            left: Box::new(left),
            right: Box::new(right),
            qual: JoinQual::On(on),
        });
        Ok(Some(SelectNode {
            distinct: outer.distinct || !anti,
            from: [join].into_iter().collect(),
            where_clause,
            ..outer.clone()
        }))
    }
}

/// The inner SELECT of a subquery, when it is a plain SELECT.
pub(super) fn inner_select(query: &QueryExpr) -> Option<&SelectNode> {
    match &query.body {
        QueryBody::Select(select) => Some(select),
        QueryBody::Values(_) | QueryBody::SetOp(_) => None,
    }
}

/// The single output column of an IN subquery, which must be a plain
/// column reference.
fn inner_output_column(inner: &SelectNode) -> DecorrelateResult<&ColumnNode> {
    let SelectColumns::Columns(columns) = &inner.columns else {
        return Err(rejected("IN subquery has no output columns"));
    };
    match columns.as_slice() {
        [
            SelectColumn::Expr {
                expr: ScalarExpr::Column(column),
                ..
            },
        ] => Ok(column),
        [SelectColumn::Expr { .. } | SelectColumn::Star(_)] => Err(rejected(
            "IN subquery output is not a simple column reference",
        )),
        [] | [_, _, ..] => Err(rejected("IN subquery must have exactly one output column")),
    }
}
