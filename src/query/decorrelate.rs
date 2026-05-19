use std::collections::HashSet;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;
use tokio_postgres::types::Type;

use crate::cache::SubqueryKind;
use crate::catalog::ColumnMetadata;
use crate::query::ast::{BinaryOp, JoinType, SubLinkType, TableAlias, UnaryOp};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedJoinQual, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedSetOpNode, ResolvedTableSource, ResolvedTableSubqueryNode,
    ResolvedUnaryExpr, ResolvedWhereExpr,
};
use crate::query::transform::{where_expr_conjuncts_join, where_expr_conjuncts_split};

/// Wrap a resolved column node as a boxed `WhereExpr::Scalar(Column(...))` leaf.
fn boxed_scalar_column(col: ResolvedColumnNode) -> Box<ResolvedWhereExpr> {
    Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)))
}

error_set! {
    DecorrelateError := {
        #[display("Non-decorrelatable correlated subquery: {reason}")]
        NonDecorrelatable { reason: String },
    }
}

pub type DecorrelateResult<T> = Result<T, Report<DecorrelateError>>;

/// Outcome of decorrelation: the (possibly transformed) resolved query.
pub struct DecorrelateOutcome {
    pub resolved: ResolvedQueryExpr,
    pub transformed: bool,
}

/// A correlation predicate extracted from an inner subquery WHERE.
struct CorrelationPredicate {
    /// Column from the outer query scope
    outer_column: ResolvedColumnNode,
    /// Column from the inner subquery scope
    inner_column: ResolvedColumnNode,
}

/// Non-empty collection of correlation predicates.
/// Constructed by `where_clause_correlation_partition` which returns `None`
/// when no predicates are found, so existence of this type guarantees at
/// least one predicate.
struct CorrelationPredicates {
    first: CorrelationPredicate,
    rest: Vec<CorrelationPredicate>,
}

impl CorrelationPredicates {
    fn iter(&self) -> impl Iterator<Item = &CorrelationPredicate> {
        std::iter::once(&self.first).chain(&self.rest)
    }
}

/// Mutable state threaded through decorrelation to generate unique aliases.
struct DecorrelateState<'a> {
    /// Counter for derived table aliases: _dc1, _dc2, ...
    derived_table_counter: u32,
    /// Counter for scalar column aliases: _ds1, _ds2, ...
    scalar_column_counter: u32,
    /// Aggregate function names from pg_proc (lowercase).
    aggregate_functions: &'a HashSet<String>,
}

impl<'a> DecorrelateState<'a> {
    fn new(aggregate_functions: &'a HashSet<String>) -> Self {
        Self {
            derived_table_counter: 0,
            scalar_column_counter: 0,
            aggregate_functions,
        }
    }

    fn next_derived_alias(&mut self) -> String {
        self.derived_table_counter += 1;
        format!("_dc{}", self.derived_table_counter)
    }

    fn next_scalar_alias(&mut self) -> EcoString {
        self.scalar_column_counter += 1;
        EcoString::from(format!("_ds{}", self.scalar_column_counter))
    }
}

/// Result of decorrelating a single scalar subquery.
struct ScalarDecorrelateResult {
    /// The derived table to LEFT JOIN onto the outer query.
    derived_table: ResolvedTableSource,
    /// The JOIN ON condition (correlation predicates mapped to derived table columns).
    join_condition: ResolvedWhereExpr,
    /// A column reference pointing to the scalar result column inside the derived table.
    scalar_column_ref: ResolvedColumnNode,
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
fn where_clause_correlation_partition(
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
fn correlation_predicates_to_condition(predicates: &CorrelationPredicates) -> ResolvedWhereExpr {
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
fn inner_query_select(query: &ResolvedQueryExpr) -> Option<&ResolvedSelectNode> {
    match &query.body {
        ResolvedQueryBody::Select(select) => Some(select),
        ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
    }
}

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

/// Combine a list of FROM sources into a single ResolvedTableSource.
///
/// If there's exactly one source, returns it directly. Multiple sources are
/// combined into a chain of cross joins (no condition).
/// Returns `None` if `sources` is empty.
fn from_sources_to_table_source(sources: &[ResolvedTableSource]) -> Option<ResolvedTableSource> {
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
fn merge_where_clauses(
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
fn scalar_expr_has_correlation(expr: &ResolvedScalarExpr) -> bool {
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
fn where_expr_has_correlation(expr: &ResolvedWhereExpr) -> bool {
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
fn correlated_exists_inner_prepare(
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
fn correlated_not_exists_inner_prepare(
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
    agg_fns: &HashSet<String>,
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
fn subquery_scalar_decorrelate(
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
fn left_join_derived(
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
fn conjunct_scalar_decorrelate(
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

/// Attempt to decorrelate a correlated EXISTS subquery conjunct.
///
/// Returns `Ok(Some(new_select))` on success, `Ok(None)` if the conjunct can't be
/// decorrelated (caller should keep as residual), or `Err` for unsupported patterns.
fn conjunct_exists_try_decorrelate(
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
fn conjunct_not_exists_try_decorrelate(
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

/// Attempt to decorrelate a correlated IN subquery conjunct.
///
/// Returns `Ok(Some(new_select))` on success, `Ok(None)` if the conjunct can't be
/// decorrelated, or `Err` for unsupported patterns.
fn conjunct_in_any_try_decorrelate(
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
fn conjunct_not_in_all_try_decorrelate(
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
    aggregate_functions: &HashSet<String>,
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

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{Deparse, JoinType, query_expr_convert};
    use crate::query::resolved::query_expr_resolve;

    use super::*;

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: u32, column_names: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, col_name)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*col_name).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![column_names[0].to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn test_tables() -> BiHashMap<TableMetadata> {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table(
            "employees",
            1,
            &["id", "name", "dept_id", "status", "manager_id"],
        ));
        tables.insert_overwrite(test_table(
            "orders",
            2,
            &["id", "emp_id", "status", "total", "customer_id"],
        ));
        tables.insert_overwrite(test_table(
            "departments",
            3,
            &["id", "name", "location", "budget"],
        ));
        tables.insert_overwrite(test_table(
            "customers",
            4,
            &["id", "name", "region", "emp_id"],
        ));
        tables.insert_overwrite(test_table("users", 5, &["id", "name", "email", "status"]));
        tables.insert_overwrite(test_table("active_users", 6, &["id", "user_id"]));
        tables.insert_overwrite(test_table(
            "projects",
            7,
            &["id", "name", "dept_id", "status"],
        ));
        tables
    }

    fn test_aggregate_functions() -> HashSet<String> {
        [
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "array_agg",
            "string_agg",
            "bool_and",
            "bool_or",
        ]
        .into_iter()
        .map(String::from)
        .collect()
    }

    /// Parse SQL → resolve → decorrelate → return outcome.
    fn resolve_and_decorrelate(
        sql: &str,
        tables: &BiHashMap<TableMetadata>,
    ) -> DecorrelateResult<DecorrelateOutcome> {
        let parsed = pg_query::parse(sql).expect("parse SQL");
        let ast = query_expr_convert(&parsed).expect("convert to AST");
        let resolved = query_expr_resolve(&ast, tables, &["public"]).expect("resolve query");
        query_expr_decorrelate(&resolved, &test_aggregate_functions())
    }

    fn as_select(query: &ResolvedQueryExpr) -> &ResolvedSelectNode {
        match &query.body {
            ResolvedQueryBody::Select(s) => s,
            _ => panic!("expected Select"),
        }
    }

    // ==================== Test Helpers ====================

    /// Flatten all JoinNodes from the FROM clause by recursively walking the source tree.
    fn select_joins(select: &ResolvedSelectNode) -> Vec<&ResolvedJoinNode> {
        fn collect_from_source<'a>(
            source: &'a ResolvedTableSource,
            out: &mut Vec<&'a ResolvedJoinNode>,
        ) {
            match source {
                ResolvedTableSource::Table(_) | ResolvedTableSource::Subquery(_) => {}
                ResolvedTableSource::Join(join) => {
                    out.push(join);
                    collect_from_source(&join.left, out);
                    collect_from_source(&join.right, out);
                }
            }
        }
        let mut joins = Vec::new();
        for source in &select.from {
            collect_from_source(source, &mut joins);
        }
        joins
    }

    /// Collect all table names referenced in the FROM clause (through joins and subqueries).
    fn from_table_names(select: &ResolvedSelectNode) -> Vec<&str> {
        fn collect_from_source<'a>(source: &'a ResolvedTableSource, out: &mut Vec<&'a str>) {
            match source {
                ResolvedTableSource::Table(t) => out.push(&t.name),
                ResolvedTableSource::Subquery(_) => {}
                ResolvedTableSource::Join(join) => {
                    collect_from_source(&join.left, out);
                    collect_from_source(&join.right, out);
                }
            }
        }
        let mut names = Vec::new();
        for source in &select.from {
            collect_from_source(source, &mut names);
        }
        names
    }

    /// Collect aliases of derived tables (subqueries) in the FROM clause.
    fn from_derived_aliases(select: &ResolvedSelectNode) -> Vec<&str> {
        fn collect_from_source<'a>(source: &'a ResolvedTableSource, out: &mut Vec<&'a str>) {
            match source {
                ResolvedTableSource::Table(_) => {}
                ResolvedTableSource::Subquery(sub) => out.push(&sub.alias.name),
                ResolvedTableSource::Join(join) => {
                    collect_from_source(&join.left, out);
                    collect_from_source(&join.right, out);
                }
            }
        }
        let mut aliases = Vec::new();
        for source in &select.from {
            collect_from_source(source, &mut aliases);
        }
        aliases
    }

    /// Recursively check if any Subquery variant exists in a WHERE expression.
    fn where_has_subquery(expr: &Option<ResolvedWhereExpr>) -> bool {
        fn check(expr: &ResolvedWhereExpr) -> bool {
            match expr {
                ResolvedWhereExpr::Subquery { .. } => true,
                ResolvedWhereExpr::Unary(u) => check(&u.expr),
                ResolvedWhereExpr::Binary(b) => check(&b.lexpr) || check(&b.rexpr),
                ResolvedWhereExpr::Multi(m) => m.exprs.iter().any(check),
                _ => false,
            }
        }
        expr.as_ref().is_some_and(check)
    }

    /// Recursively check if an IS NULL expression exists in a WHERE expression.
    fn where_has_is_null(expr: &Option<ResolvedWhereExpr>) -> bool {
        fn check(expr: &ResolvedWhereExpr) -> bool {
            match expr {
                ResolvedWhereExpr::Unary(u) if u.op == UnaryOp::IsNull => true,
                ResolvedWhereExpr::Unary(u) => check(&u.expr),
                ResolvedWhereExpr::Binary(b) => check(&b.lexpr) || check(&b.rexpr),
                ResolvedWhereExpr::Multi(m) => m.exprs.iter().any(check),
                _ => false,
            }
        }
        expr.as_ref().is_some_and(check)
    }

    /// Collect scalar column aliases from derived table subquery SELECT lists.
    /// These are the aliased columns inside the derived table (e.g., `_ds1` in
    /// `(SELECT count(*) AS _ds1 ...) AS _dc1`).
    fn derived_scalar_aliases(select: &ResolvedSelectNode) -> Vec<&str> {
        fn collect_from_source<'a>(source: &'a ResolvedTableSource, out: &mut Vec<&'a str>) {
            match source {
                ResolvedTableSource::Table(_) => {}
                ResolvedTableSource::Subquery(sub) => {
                    if let ResolvedQueryBody::Select(inner) = &sub.query.body
                        && let ResolvedSelectColumns::Columns(cols) = &inner.columns
                    {
                        for col in cols {
                            if let Some(alias) = &col.alias {
                                out.push(alias);
                            }
                        }
                    }
                }
                ResolvedTableSource::Join(join) => {
                    collect_from_source(&join.left, out);
                    collect_from_source(&join.right, out);
                }
            }
        }
        let mut aliases = Vec::new();
        for source in &select.from {
            collect_from_source(source, &mut aliases);
        }
        aliases
    }

    /// Check if any derived table subquery has a non-empty GROUP BY.
    fn derived_has_group_by(select: &ResolvedSelectNode) -> bool {
        fn check_source(source: &ResolvedTableSource) -> bool {
            match source {
                ResolvedTableSource::Table(_) => false,
                ResolvedTableSource::Subquery(sub) => {
                    if let ResolvedQueryBody::Select(inner) = &sub.query.body {
                        !inner.group_by.is_empty()
                    } else {
                        false
                    }
                }
                ResolvedTableSource::Join(join) => {
                    check_source(&join.left) || check_source(&join.right)
                }
            }
        }
        select.from.iter().any(check_source)
    }

    /// Deparse a single WHERE expression for targeted predicate checks.
    fn deparse_expr(expr: &ResolvedWhereExpr) -> String {
        let mut buf = String::new();
        expr.deparse(&mut buf);
        buf
    }

    // ==================== No-op Cases ====================

    #[test]
    fn test_no_subqueries_unchanged() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT id, name FROM employees WHERE status = 'active'",
            &tables,
        )
        .unwrap();

        assert!(!outcome.transformed);
    }

    #[test]
    fn test_non_correlated_exists_unchanged() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT id FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE status = 'active')",
            &tables,
        )
        .unwrap();

        assert!(!outcome.transformed);
        let select = as_select(&outcome.resolved);
        assert!(
            where_has_subquery(&select.where_clause),
            "subquery should remain"
        );
    }

    // ==================== EXISTS Decorrelation ====================

    #[test]
    fn test_exists_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, e.name FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].join_type, JoinType::Inner);
        assert!(select.distinct);

        let on = deparse_expr(joins[0].predicate().unwrap());
        assert!(on.contains("o.emp_id = e.id"), "correlation in ON: {on}");
    }

    #[test]
    fn test_exists_multiple_correlation_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = e.status)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        let on = deparse_expr(joins[0].predicate().unwrap());
        assert!(on.contains("o.emp_id = e.id"), "first correlation: {on}");
        assert!(
            on.contains("o.status = e.status"),
            "second correlation: {on}"
        );
    }

    #[test]
    fn test_exists_with_residual_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = 'active')",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
        assert!(
            where_str.contains("o.status = 'active'"),
            "residual in WHERE: {where_str}"
        );
    }

    #[test]
    fn test_exists_outer_query_already_has_joins() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             JOIN departments d ON d.id = e.dept_id \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        let tables = from_table_names(select);
        assert!(tables.contains(&"departments"), "original join table");
        assert!(tables.contains(&"orders"), "decorrelated join table");
    }

    #[test]
    fn test_exists_inner_query_has_joins() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 JOIN customers c ON c.id = o.customer_id \
                 WHERE o.emp_id = e.id \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        let tables = from_table_names(select);
        assert!(tables.contains(&"customers"), "inner join table preserved");
        assert!(tables.contains(&"orders"), "inner table preserved");
    }

    #[test]
    fn test_multiple_exists_in_where() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
             AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        let tables = from_table_names(select);
        assert!(tables.contains(&"orders"), "first subquery table");
        assert!(tables.contains(&"projects"), "second subquery table");
    }

    #[test]
    fn test_exists_sets_distinct() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        let select = as_select(&outcome.resolved);
        assert!(select.distinct, "EXISTS decorrelation should set DISTINCT");
    }

    // ==================== NOT EXISTS Decorrelation ====================

    #[test]
    fn test_not_exists_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id, d.name FROM departments d \
             WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "NOT EXISTS should be removed"
        );
        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].join_type, JoinType::Left);
        assert!(where_has_is_null(&select.where_clause));
    }

    #[test]
    fn test_not_exists_residual_in_on_clause() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.status = 'active' \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].join_type, JoinType::Left);
        let on = deparse_expr(joins[0].predicate().unwrap());
        assert!(on.contains("e.dept_id = d.id"), "correlation in ON: {on}");
        assert!(on.contains("e.status = 'active'"), "residual in ON: {on}");
        assert!(where_has_is_null(&select.where_clause));
    }

    #[test]
    fn test_not_exists_adds_is_null_check() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
            &tables,
        )
        .unwrap();

        let select = as_select(&outcome.resolved);
        assert!(
            where_has_is_null(&select.where_clause),
            "should have IS NULL in WHERE"
        );
    }

    // ==================== Rejection Cases ====================

    #[test]
    fn test_correlated_non_equality_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id > e.id)",
            &tables,
        );

        assert!(
            result.is_err(),
            "non-equality correlation should be rejected"
        );
    }

    #[test]
    fn test_correlated_in_subquery_decorrelated() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed, "correlated IN should be decorrelated");
        let select = as_select(&outcome.resolved);
        assert!(
            !where_has_subquery(&select.where_clause),
            "IN should be removed"
        );
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        assert!(select.distinct);
    }

    #[test]
    fn test_correlated_scalar_subquery_decorrelated() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "correlated scalar subquery in SELECT should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "derived table alias _dc1"
        );
        assert!(
            derived_scalar_aliases(select).contains(&"_ds1"),
            "scalar column alias _ds1"
        );
    }

    #[test]
    fn test_exists_inside_or_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.status = 'active' \
                OR EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        );

        assert!(result.is_err(), "EXISTS inside OR should be rejected");
    }

    // ==================== EXISTS: Inner Clause Stripping ====================

    #[test]
    fn test_exists_with_inner_group_by_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 WHERE o.emp_id = e.id \
                 GROUP BY o.status \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "EXISTS with GROUP BY should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        assert!(select.group_by.is_empty(), "GROUP BY should be stripped");
    }

    #[test]
    fn test_exists_with_inner_having_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 WHERE o.emp_id = e.id \
                 GROUP BY o.status \
                 HAVING count(*) > 5 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "EXISTS with HAVING should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
        assert!(select.having.is_none(), "HAVING should be stripped");
    }

    #[test]
    fn test_exists_with_inner_limit_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS ( \
                 SELECT 1 FROM orders o \
                 WHERE o.emp_id = e.id \
                 LIMIT 1 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "EXISTS with LIMIT should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be removed"
        );
    }

    // ==================== NOT EXISTS: Inner Clause Handling ====================

    #[test]
    fn test_not_exists_with_inner_group_by_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e \
                 WHERE e.dept_id = d.id \
                 GROUP BY e.status \
             )",
            &tables,
        );

        assert!(
            result.is_err(),
            "NOT EXISTS with GROUP BY should be rejected"
        );
    }

    #[test]
    fn test_not_exists_with_inner_having_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e \
                 WHERE e.dept_id = d.id \
                 GROUP BY e.status \
                 HAVING count(*) > 5 \
             )",
            &tables,
        );

        assert!(result.is_err(), "NOT EXISTS with HAVING should be rejected");
    }

    #[test]
    fn test_not_exists_with_inner_limit_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id, d.name FROM departments d \
             WHERE NOT EXISTS ( \
                 SELECT 1 FROM employees e \
                 WHERE e.dept_id = d.id \
                 LIMIT 1 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "NOT EXISTS with LIMIT should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            !where_has_subquery(&select.where_clause),
            "NOT EXISTS should be removed"
        );
    }

    // ==================== Mixed Cases ====================

    #[test]
    fn test_mixed_correlated_and_non_correlated() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
             AND EXISTS (SELECT 1 FROM departments d WHERE d.name = 'Engineering')",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        // Correlated EXISTS decorrelated into JOIN
        assert!(
            from_table_names(select).contains(&"orders"),
            "correlated subquery decorrelated"
        );
        // Non-correlated EXISTS should remain as subquery
        assert!(
            where_has_subquery(&select.where_clause),
            "non-correlated EXISTS should remain"
        );
    }

    #[test]
    fn test_exists_with_outer_where_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.status = 'active' \
             AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
        assert!(
            where_str.contains("e.status = 'active'"),
            "outer predicate preserved: {where_str}"
        );
        assert!(
            !select_joins(select).is_empty(),
            "correlation should be JOIN"
        );
    }

    // ==================== Scalar SELECT List Decorrelation ====================

    #[test]
    fn test_scalar_select_count_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "derived table alias"
        );
        assert!(
            derived_scalar_aliases(select).contains(&"_ds1"),
            "scalar column alias"
        );
        assert!(derived_has_group_by(select), "aggregate needs GROUP BY");
    }

    #[test]
    fn test_scalar_select_avg_with_residual() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT avg(o.total) FROM orders o WHERE o.emp_id = e.id AND o.status = 'done') AS avg_total \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(derived_has_group_by(select), "aggregate needs GROUP BY");
        assert!(
            !from_derived_aliases(select).is_empty(),
            "should have derived table"
        );
    }

    #[test]
    fn test_scalar_select_multiple_correlations() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id AND o.status = e.status) AS cnt \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(derived_has_group_by(select), "aggregate needs GROUP BY");
    }

    #[test]
    fn test_scalar_select_multiple_subqueries() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count, \
                 (SELECT count(*) FROM projects p WHERE p.dept_id = e.dept_id) AS project_count \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let derived = from_derived_aliases(select);
        assert!(derived.contains(&"_dc1"), "first derived table");
        assert!(derived.contains(&"_dc2"), "second derived table");
        let scalar = derived_scalar_aliases(select);
        assert!(scalar.contains(&"_ds1"), "first scalar column");
        assert!(scalar.contains(&"_ds2"), "second scalar column");
    }

    #[test]
    fn test_scalar_select_non_aggregate_no_group_by() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept_name \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            select.group_by.is_empty(),
            "non-aggregate should not have GROUP BY"
        );
    }

    #[test]
    fn test_scalar_select_non_aggregate_with_limit_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT d.name FROM departments d WHERE d.id = e.dept_id LIMIT 1) AS dept_name \
             FROM employees e",
            &tables,
        );

        assert!(
            result.is_err(),
            "non-aggregate scalar with LIMIT should be rejected"
        );
    }

    #[test]
    fn test_scalar_select_mixed_with_exists() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
             FROM employees e \
             WHERE EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        // Scalar subquery decorrelated as LEFT JOIN with derived table
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "scalar derived table"
        );
        // EXISTS decorrelated as INNER JOIN + DISTINCT
        assert!(select.distinct, "EXISTS should set DISTINCT");
        assert!(
            !where_has_subquery(&select.where_clause),
            "EXISTS should be decorrelated"
        );
    }

    // ==================== Scalar WHERE Clause Decorrelation ====================

    #[test]
    fn test_scalar_where_comparison() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT d.id, d.name FROM departments d \
             WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "derived table alias"
        );
        assert!(
            derived_scalar_aliases(select).contains(&"_ds1"),
            "scalar column alias"
        );
    }

    #[test]
    fn test_scalar_where_equality() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.id = (SELECT max(o.emp_id) FROM orders o WHERE o.status = e.status)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "derived table alias"
        );
    }

    #[test]
    fn test_scalar_where_multiple_subqueries() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.id > (SELECT min(o.emp_id) FROM orders o WHERE o.status = e.status) \
             AND e.id < (SELECT max(o.emp_id) FROM orders o WHERE o.status = e.status)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let derived = from_derived_aliases(select);
        assert!(derived.contains(&"_dc1"), "first derived table");
        assert!(derived.contains(&"_dc2"), "second derived table");
    }

    #[test]
    fn test_scalar_where_non_aggregate() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            select.group_by.is_empty(),
            "non-aggregate should not have GROUP BY"
        );
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "derived table alias"
        );
    }

    // ==================== Self-Join / Alias Handling ====================

    #[test]
    fn test_scalar_select_self_join() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM employees e2 WHERE e2.manager_id = e.id) AS report_count \
             FROM employees e",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(
            from_derived_aliases(select).contains(&"_dc1"),
            "derived table alias"
        );
        assert!(derived_has_group_by(select), "aggregate needs GROUP BY");
    }

    // ==================== SetOp with Scalar Subqueries ====================

    #[test]
    fn test_scalar_setop_unique_aliases() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, \
                 (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS cnt \
             FROM employees e \
             UNION ALL \
             SELECT d.id, \
                 (SELECT count(*) FROM projects p WHERE p.dept_id = d.id) AS cnt \
             FROM departments d",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        // Extract both branches from the SetOp
        let set_op = match &outcome.resolved.body {
            ResolvedQueryBody::SetOp(s) => s,
            _ => panic!("expected SetOp"),
        };
        let left = as_select(&set_op.left);
        let right = as_select(&set_op.right);

        assert!(
            from_derived_aliases(left).contains(&"_dc1"),
            "first branch derived table"
        );
        assert!(
            from_derived_aliases(right).contains(&"_dc2"),
            "second branch derived table"
        );
        assert!(
            derived_scalar_aliases(left).contains(&"_ds1"),
            "first branch scalar column"
        );
        assert!(
            derived_scalar_aliases(right).contains(&"_ds2"),
            "second branch scalar column"
        );
    }

    // ==================== IN Decorrelation ====================

    #[test]
    fn test_in_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, e.name FROM employees e \
             WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "IN should be removed"
        );
        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].join_type, JoinType::Inner);
        assert!(select.distinct);
        let on = deparse_expr(joins[0].predicate().unwrap());
        assert!(on.contains("e.dept_id = d.id"), "IN predicate in ON: {on}");
        assert!(on.contains("d.name = e.name"), "correlation in ON: {on}");
    }

    #[test]
    fn test_in_with_residual_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id IN ( \
                 SELECT d.id FROM departments d \
                 WHERE d.name = e.name AND d.location = 'NYC' \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "IN should be removed"
        );
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
        assert!(
            where_str.contains("d.location = 'NYC'"),
            "residual in WHERE: {where_str}"
        );
    }

    #[test]
    fn test_in_with_outer_where_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.status = 'active' \
             AND e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
        assert!(
            where_str.contains("e.status = 'active'"),
            "outer predicate preserved: {where_str}"
        );
        assert!(!select_joins(select).is_empty(), "should have JOIN");
    }

    #[test]
    fn test_in_sets_distinct() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        let select = as_select(&outcome.resolved);
        assert!(select.distinct, "IN decorrelation should set DISTINCT");
    }

    #[test]
    fn test_in_with_inner_group_by_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id IN ( \
                 SELECT d.id FROM departments d \
                 WHERE d.name = e.name \
                 GROUP BY d.id \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "IN with GROUP BY should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        assert!(
            !where_has_subquery(&select.where_clause),
            "IN should be removed"
        );
        assert!(select.group_by.is_empty(), "GROUP BY should be stripped");
    }

    #[test]
    fn test_in_with_inner_limit_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id IN ( \
                 SELECT d.id FROM departments d \
                 WHERE d.name = e.name \
                 LIMIT 10 \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed, "IN with LIMIT should be decorrelated");
        let select = as_select(&outcome.resolved);
        assert!(!select_joins(select).is_empty(), "should have JOIN");
        assert!(
            !where_has_subquery(&select.where_clause),
            "IN should be removed"
        );
    }

    #[test]
    fn test_in_outer_query_has_joins() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             JOIN orders o ON o.emp_id = e.id \
             WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "IN should be removed"
        );
        let tables = from_table_names(select);
        assert!(tables.contains(&"orders"), "original join preserved");
        assert!(tables.contains(&"departments"), "IN table joined");
    }

    #[test]
    fn test_in_mixed_with_exists() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name) \
             AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "subqueries should be removed"
        );
        let tables = from_table_names(select);
        assert!(tables.contains(&"departments"), "IN table");
        assert!(tables.contains(&"orders"), "EXISTS table");
    }

    #[test]
    fn test_non_correlated_in_unchanged() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.location = 'NYC')",
            &tables,
        )
        .unwrap();

        assert!(!outcome.transformed);
        let select = as_select(&outcome.resolved);
        assert!(
            where_has_subquery(&select.where_clause),
            "non-correlated IN should remain"
        );
    }

    // ==================== NOT IN Decorrelation ====================

    #[test]
    fn test_not_in_single_correlation() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id, e.name FROM employees e \
             WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].join_type, JoinType::Left);
        assert!(where_has_is_null(&select.where_clause));
        let on = deparse_expr(joins[0].predicate().unwrap());
        assert!(on.contains("e.dept_id = d.id"), "IN predicate in ON: {on}");
        assert!(on.contains("d.name = e.name"), "correlation in ON: {on}");
    }

    #[test]
    fn test_not_in_all_syntax() {
        let tables = test_tables();
        // <> ALL is the same as NOT IN at the SubLinkType level
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id <> ALL (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(where_has_is_null(&select.where_clause));
    }

    #[test]
    fn test_not_in_with_residual_in_on_clause() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id NOT IN ( \
                 SELECT d.id FROM departments d \
                 WHERE d.name = e.name AND d.location = 'NYC' \
             )",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let joins = select_joins(select);
        assert_eq!(joins.len(), 1);
        assert_eq!(joins[0].join_type, JoinType::Left);
        // Residual should be in ON clause (not outer WHERE) for LEFT JOIN semantics
        let on = deparse_expr(joins[0].predicate().unwrap());
        assert!(
            on.contains("d.location = 'NYC'"),
            "residual in ON clause: {on}"
        );
        assert!(where_has_is_null(&select.where_clause));
    }

    #[test]
    fn test_not_in_with_outer_where_predicates() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.status = 'active' \
             AND e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
        assert!(
            where_str.contains("e.status = 'active'"),
            "outer predicate preserved: {where_str}"
        );
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
        assert!(where_has_is_null(&select.where_clause));
    }

    #[test]
    fn test_not_in_does_not_set_distinct() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        let select = as_select(&outcome.resolved);
        assert!(
            !select.distinct,
            "NOT IN decorrelation should NOT set DISTINCT"
        );
    }

    #[test]
    fn test_not_in_with_inner_group_by_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id NOT IN ( \
                 SELECT d.id FROM departments d \
                 WHERE d.name = e.name \
                 GROUP BY d.id \
             )",
            &tables,
        );

        assert!(result.is_err(), "NOT IN with GROUP BY should be rejected");
    }

    #[test]
    fn test_not_in_with_inner_having_rejected() {
        let tables = test_tables();
        let result = resolve_and_decorrelate(
            "SELECT d.id FROM departments d \
             WHERE d.id NOT IN ( \
                 SELECT e.dept_id FROM employees e \
                 WHERE e.name = d.name \
                 GROUP BY e.dept_id \
                 HAVING count(*) > 5 \
             )",
            &tables,
        );

        assert!(result.is_err(), "NOT IN with HAVING should be rejected");
    }

    #[test]
    fn test_not_in_with_inner_limit_stripped() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id NOT IN ( \
                 SELECT d.id FROM departments d \
                 WHERE d.name = e.name \
                 LIMIT 10 \
             )",
            &tables,
        )
        .unwrap();

        assert!(
            outcome.transformed,
            "NOT IN with LIMIT should be decorrelated"
        );
        let select = as_select(&outcome.resolved);
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
    }

    #[test]
    fn test_not_in_outer_query_has_joins() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             JOIN orders o ON o.emp_id = e.id \
             WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        let tables = from_table_names(select);
        assert!(tables.contains(&"orders"), "original join preserved");
        assert!(tables.contains(&"departments"), "NOT IN table joined");
        let joins = select_joins(select);
        assert!(
            joins.iter().any(|j| j.join_type == JoinType::Left),
            "should have LEFT JOIN"
        );
    }

    #[test]
    fn test_not_in_mixed_with_in_and_exists() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name) \
             AND e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status) \
             AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
            &tables,
        )
        .unwrap();

        assert!(outcome.transformed);
        let select = as_select(&outcome.resolved);

        assert!(
            !where_has_subquery(&select.where_clause),
            "all subqueries should be removed"
        );
        let tables = from_table_names(select);
        assert!(tables.contains(&"departments"), "NOT IN table");
        assert!(tables.contains(&"orders"), "IN table");
        assert!(tables.contains(&"projects"), "EXISTS table");
    }

    #[test]
    fn test_non_correlated_not_in_unchanged() {
        let tables = test_tables();
        let outcome = resolve_and_decorrelate(
            "SELECT e.id FROM employees e \
             WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.location = 'NYC')",
            &tables,
        )
        .unwrap();

        assert!(!outcome.transformed);
        let select = as_select(&outcome.resolved);
        assert!(
            where_has_subquery(&select.where_clause),
            "non-correlated NOT IN should remain"
        );
    }
}
