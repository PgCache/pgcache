//! A correlated scalar subquery as a LEFT JOINed derived table (ADR-015):
//! `_dcN` selects the correlation key columns and the scalar as `_dsN`,
//! grouped by the keys when the scalar aggregates; the outer reference
//! becomes `_dcN._dsN`.

use std::collections::HashSet;

use ecow::EcoString;
use postgres_types::Type;
use smallvec::SmallVec;

use super::join::inner_select;
use super::scope::{
    Scope, column_moved, scalar_has_correlation, sources_joined, where_and,
    where_correlation_partition, where_equal,
};
use super::{Decorrelate, rejected};
use crate::cache::SubqueryKind;
use crate::catalog::{ColumnMetadata, ColumnPosition};
use crate::oid::TypeOid;
use crate::query::ast::{
    BinaryExpr, ColumnNode, JoinNode, JoinQual, JoinType, NodeId, QueryBody, QueryExpr, ScalarExpr,
    SelectColumn, SelectColumns, SelectNode, SubLinkType, TableAlias, TableSource,
    TableSubqueryNode, UnaryExpr, WhereExpr,
};
use crate::query::decorrelate::DecorrelateResult;
use crate::query::resolve::{ColumnBinding, ColumnTarget};

/// What one scalar subquery becomes.
struct ScalarJoin {
    derived: TableSource,
    condition: WhereExpr,
    scalar: ColumnNode,
}

/// Whether a scalar expression calls an aggregate, looking through CASE
/// arms, arithmetic, arrays and casts but not into subqueries.
fn scalar_has_aggregate(expr: &ScalarExpr, aggregates: &HashSet<EcoString>) -> bool {
    match expr {
        ScalarExpr::Function(func) => {
            aggregates.contains(func.name.as_str())
                || func
                    .args
                    .iter()
                    .any(|a| scalar_has_aggregate(a, aggregates))
        }
        ScalarExpr::Case(case) => {
            case.arg
                .as_ref()
                .is_some_and(|a| scalar_has_aggregate(a, aggregates))
                || case
                    .whens
                    .iter()
                    .any(|w| scalar_has_aggregate(&w.result, aggregates))
                || case
                    .default
                    .as_ref()
                    .is_some_and(|d| scalar_has_aggregate(d, aggregates))
        }
        ScalarExpr::Arithmetic(arithmetic) => {
            scalar_has_aggregate(&arithmetic.left, aggregates)
                || scalar_has_aggregate(&arithmetic.right, aggregates)
        }
        ScalarExpr::Array(elements) => elements.iter().any(|e| scalar_has_aggregate(e, aggregates)),
        ScalarExpr::TypeCast { expr, .. } => scalar_has_aggregate(expr, aggregates),
        ScalarExpr::Column(_) | ScalarExpr::Literal(_) | ScalarExpr::Subquery(_) => false,
    }
}

/// The binding an inner correlation column refers to: its own, or the left
/// column of a merged inner-join column.
fn inner_binding(state: &Decorrelate<'_>, column: &ColumnNode) -> Option<ColumnBinding> {
    match state.bound.column(column)? {
        ColumnTarget::Column(binding) => Some(*binding),
        ColumnTarget::Merged(id) => state
            .bound
            .bindings()
            .merged(*id)
            .filter(|merged| !merged.outer)
            .map(|merged| merged.left),
        ColumnTarget::Output => None,
    }
}

fn subquery_scalar_decorrelate(
    state: &mut Decorrelate<'_>,
    query: &QueryExpr,
    destination: &Scope,
) -> DecorrelateResult<ScalarJoin> {
    let inner =
        inner_select(query).ok_or_else(|| rejected("scalar subquery is not a simple SELECT"))?;
    let columns = match &inner.columns {
        SelectColumns::Columns(columns) => columns.as_slice(),
        SelectColumns::None => &[],
    };
    let [SelectColumn::Expr { expr: scalar, .. }] = columns else {
        return Err(rejected(
            "scalar subquery must have exactly one output column",
        ));
    };
    let inner_where = inner
        .where_clause
        .as_ref()
        .ok_or_else(|| rejected("scalar subquery has no WHERE clause"))?;
    let has_aggregate = scalar_has_aggregate(scalar, state.aggregate_functions);
    if !has_aggregate && query.limit.is_some() {
        return Err(rejected("non-aggregate scalar subquery with LIMIT"));
    }
    let scope = Scope::of_query(state, query);
    let (predicates, residual) = where_correlation_partition(state, &scope, inner_where)
        .ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))?;

    let derived_alias = state.next_derived_alias();
    let scalar_alias = state.next_scalar_alias();

    // The derived table keeps the inner query's own scope: its columns and
    // FROM stand as they are, only the correlation predicates leave.
    let mut derived_columns = Vec::with_capacity(predicates.rest.len() + 2);
    let mut metadata = Vec::with_capacity(predicates.rest.len() + 2);
    for (position, predicate) in predicates.iter().enumerate() {
        let binding = inner_binding(state, &predicate.inner)
            .ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))?;
        let column = state
            .bound
            .bindings()
            .binding_column(&binding)
            .ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))?;
        let position = ColumnPosition::from_index(position)
            .ok_or_else(|| rejected("scalar subquery has too many correlation columns"))?;
        metadata.push(ColumnMetadata {
            position,
            ..column.clone()
        });
        derived_columns.push(SelectColumn::Expr {
            expr: ScalarExpr::Column(predicate.inner.clone()),
            alias: None,
        });
    }
    let scalar_position = ColumnPosition::from_index(derived_columns.len())
        .ok_or_else(|| rejected("scalar subquery has too many correlation columns"))?;
    metadata.push(ColumnMetadata {
        name: scalar_alias.clone(),
        position: scalar_position,
        type_oid: TypeOid::from_raw(25),
        data_type: Type::TEXT,
        type_name: EcoString::from("text"),
        cache_type_name: EcoString::from("text"),
        is_primary_key: false,
    });
    derived_columns.push(SelectColumn::Expr {
        expr: scalar.clone(),
        alias: Some(scalar_alias.clone()),
    });
    let group_by: Vec<ColumnNode> = if has_aggregate {
        predicates.iter().map(|p| p.inner.clone()).collect()
    } else {
        Vec::new()
    };
    let derived_query = QueryExpr {
        id: NodeId::PENDING,
        ctes: Vec::new(),
        body: QueryBody::Select(Box::new(SelectNode {
            id: NodeId::PENDING,
            distinct: false,
            columns: SelectColumns::Columns(derived_columns),
            from: inner.from.clone(),
            where_clause: residual,
            group_by,
            having: None,
        })),
        order_by: SmallVec::new(),
        limit: None,
    };
    let (derived, node) = state.derived_push(&derived_alias, metadata);
    let derived_table = TableSource::Subquery(TableSubqueryNode {
        id: node,
        lateral: false,
        query: Box::new(derived_query),
        alias: Some(TableAlias {
            name: derived_alias.clone(),
            columns: Vec::new(),
        }),
        kind: SubqueryKind::Scalar,
    });

    // `_dcN.key = outer` for every correlation predicate.
    let mut condition = None;
    for (position, predicate) in predicates.iter().enumerate() {
        let key = state.derived_column(
            derived,
            &derived_alias,
            predicate.inner.column.clone(),
            position,
        );
        let equality = where_equal(
            ScalarExpr::Column(key),
            column_moved(state, &predicate.outer, destination),
        );
        condition = Some(match condition {
            Some(acc) => where_and(acc, equality),
            None => equality,
        });
    }
    let Some(condition) = condition else {
        return Err(rejected(
            "unsupported correlation pattern in scalar subquery",
        ));
    };
    let scalar = state.derived_column(
        derived,
        &derived_alias,
        scalar_alias,
        scalar_position.index(),
    );
    Ok(ScalarJoin {
        derived: derived_table,
        condition,
        scalar,
    })
}

/// `from` LEFT JOINed with the derived table; `None` for an empty FROM.
fn left_join_derived(
    from: &SmallVec<[TableSource; 1]>,
    join: ScalarJoin,
) -> Option<(SmallVec<[TableSource; 1]>, ColumnNode)> {
    let left = sources_joined(from.iter().cloned().collect())?;
    let joined = TableSource::Join(JoinNode {
        id: NodeId::PENDING,
        join_type: JoinType::Left,
        left: Box::new(left),
        right: Box::new(join.derived),
        qual: JoinQual::On(join.condition),
    });
    Some(([joined].into_iter().collect(), join.scalar))
}

/// One select column: a correlated scalar subquery becomes a column of a
/// LEFT JOINed derived table added to `from`; any other correlated
/// expression is rejected; everything else is kept.
pub(super) fn select_column_decorrelate(
    state: &mut Decorrelate<'_>,
    column: &SelectColumn,
    from: &mut SmallVec<[TableSource; 1]>,
) -> DecorrelateResult<(SelectColumn, bool)> {
    let SelectColumn::Expr { expr, alias } = column else {
        return Ok((column.clone(), false));
    };
    if let ScalarExpr::Subquery(query) = expr
        && super::scope::query_is_correlated(state, query)
    {
        let destination = Scope::of_select_sources(state, from);
        let join = subquery_scalar_decorrelate(state, query, &destination)?;
        let Some((joined, scalar)) = left_join_derived(from, join) else {
            return Ok((column.clone(), false));
        };
        *from = joined;
        return Ok((
            SelectColumn::Expr {
                expr: ScalarExpr::Column(scalar),
                alias: alias.clone(),
            },
            true,
        ));
    }
    if scalar_has_correlation(state, expr) {
        return Err(rejected("correlated subquery nested in SELECT expression"));
    }
    Ok((column.clone(), false))
}

/// A WHERE conjunct with each correlated scalar subquery replaced by a
/// reference to a LEFT JOINed derived table added to `select.from`.
pub(super) fn conjunct_scalar_decorrelate(
    state: &mut Decorrelate<'_>,
    expr: &WhereExpr,
    select: &mut SelectNode,
) -> DecorrelateResult<(WhereExpr, bool)> {
    match expr {
        WhereExpr::Subquery {
            sublink_type: SubLinkType::Expr,
            query,
            ..
        } if super::scope::query_is_correlated(state, query) => {
            let destination = Scope::of_select(state, select);
            let join = subquery_scalar_decorrelate(state, query, &destination)?;
            match left_join_derived(&select.from, join) {
                Some((joined, scalar)) => {
                    select.from = joined;
                    Ok((WhereExpr::Scalar(ScalarExpr::Column(scalar)), true))
                }
                None => Ok((expr.clone(), false)),
            }
        }
        WhereExpr::Binary(binary) => {
            let (left, left_transformed) =
                conjunct_scalar_decorrelate(state, &binary.lexpr, select)?;
            let (right, right_transformed) =
                conjunct_scalar_decorrelate(state, &binary.rexpr, select)?;
            if left_transformed || right_transformed {
                Ok((
                    WhereExpr::Binary(BinaryExpr {
                        lexpr: Box::new(left),
                        rexpr: Box::new(right),
                        ..binary.clone()
                    }),
                    true,
                ))
            } else {
                Ok((expr.clone(), false))
            }
        }
        WhereExpr::Unary(unary) => {
            let (inner, transformed) = conjunct_scalar_decorrelate(state, &unary.expr, select)?;
            if transformed {
                Ok((
                    WhereExpr::Unary(UnaryExpr {
                        expr: Box::new(inner),
                        ..unary.clone()
                    }),
                    true,
                ))
            } else {
                Ok((expr.clone(), false))
            }
        }
        WhereExpr::Scalar(_) | WhereExpr::Multi(_) | WhereExpr::Subquery { .. } => {
            Ok((expr.clone(), false))
        }
    }
}
