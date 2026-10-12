//! The cacheability rules read from the rope: `cacheability.rs` over views,
//! one function per rule, with the same verdicts in the same order.

use ecow::EcoString;

use super::{CacheabilityError, FunctionVolatilityMap};
use crate::catalog::FunctionVolatility;
use crate::query::ast::rope::{
    Body, Case, Function, Join, JoinQual, NodeKind, Query, QueryTree, Scalar, ScalarKind, Select,
    SelectItem, Source, Where, WhereKind,
};
use crate::query::ast::{BinaryOp, JoinType};

/// Reject queries that reference a PostgreSQL system catalog: a `pg_`
/// prefix on the schema or the table name, anywhere in the tree.
pub(super) fn references_system_catalog_tree(tree: &QueryTree) -> Result<(), CacheabilityError> {
    for node in tree.nodes() {
        let NodeKind::Table { schema, name, .. } = &node.kind else {
            continue;
        };
        let schema = (!schema.is_empty()).then_some(schema);
        if schema.is_some_and(|schema| pg_prefixed(schema)) || pg_prefixed(name) {
            let relation = match schema {
                Some(schema) => EcoString::from(format!("{schema}.{name}")),
                None => name.clone(),
            };
            return Err(CacheabilityError::SystemCatalogReference { relation });
        }
    }
    Ok(())
}

/// Whether `s` begins with PostgreSQL's reserved `pg_` prefix, ASCII
/// case-insensitively and without allocating.
fn pg_prefixed(s: &str) -> bool {
    s.as_bytes()
        .get(..3)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"pg_"))
}

/// Where in the query tree a cacheability check is being evaluated.
///
/// Functions in the SELECT list are safe (re-evaluated against cached rows);
/// immutable functions are safe in any context; non-immutable functions in
/// a WHERE (or a FILTER) are rejected.
#[derive(Clone, Copy)]
enum ExprContext {
    WhereClause,
    SelectList,
}

/// A query body: SELECT nodes and set-operation branches recursively.
pub(super) fn body_cacheable(
    body: Body<'_>,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match body {
        Body::Select(select) => select_cacheable(select, fv),
        Body::Values(_) => Err(CacheabilityError::UnsupportedQueryType { kind: "VALUES" }),
        Body::SetOp(set_op) => {
            // LIMIT/OFFSET on branches makes them non-cacheable
            if set_op.left().has_limit() || set_op.right().has_limit() {
                return Err(CacheabilityError::HasLimit);
            }
            body_cacheable(set_op.left().body(), fv)?;
            body_cacheable(set_op.right().body(), fv)
        }
    }
}

fn select_cacheable(
    select: Select<'_>,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    from_supported(select, fv)?;
    if let Some(where_clause) = select.where_clause() {
        where_cacheable(where_clause, ExprContext::WhereClause, fv)?;
    }
    for item in select.items().into_iter().flatten() {
        if let SelectItem::Expr(expr) = item {
            scalar_cacheable(expr.expr(), ExprContext::SelectList, fv)?;
        }
    }
    Ok(())
}

fn from_supported(select: Select<'_>, fv: &FunctionVolatilityMap) -> Result<(), CacheabilityError> {
    let mut from = select.from();
    match (from.next(), from.next()) {
        (Some(Source::Join(join)), None) => join_supported(join, fv),
        (Some(Source::Table(_)), None) => Ok(()),
        (Some(Source::Subquery(subquery)), None) => {
            // LATERAL subqueries reference the outer scope.
            if subquery.lateral() {
                return Err(CacheabilityError::UnsupportedSubquery { kind: "LATERAL" });
            }
            if subquery.alias().is_none() {
                return Err(CacheabilityError::UnsupportedSubquery {
                    kind: "derived table without alias",
                });
            }
            // LIMIT in a derived table is valid SQL, so only the body is checked.
            body_cacheable(subquery.query().body(), fv)
        }
        (Some(Source::CteRef(cte_ref)), None) => body_cacheable(cte_ref.query().body(), fv),
        (None, _) => Err(CacheabilityError::UnsupportedFrom {
            construct: "no FROM clause",
        }),
        (Some(_), Some(_)) => Err(CacheabilityError::UnsupportedFrom {
            construct: "comma join",
        }),
    }
}

fn source_supported(
    source: Source<'_>,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match source {
        Source::Join(nested) => join_supported(nested, fv),
        Source::Table(_) => Ok(()),
        Source::Subquery(subquery) => {
            if subquery.lateral() {
                return Err(CacheabilityError::UnsupportedSubquery { kind: "LATERAL" });
            }
            if subquery.alias().is_none() {
                return Err(CacheabilityError::UnsupportedSubquery {
                    kind: "derived table without alias",
                });
            }
            body_cacheable(subquery.query().body(), fv)
        }
        Source::CteRef(cte_ref) => body_cacheable(cte_ref.query().body(), fv),
    }
}

fn join_supported(join: Join<'_>, fv: &FunctionVolatilityMap) -> Result<(), CacheabilityError> {
    // FULL OUTER JOINs are not cacheable: both sides are optional.
    if join.join_type() == JoinType::Full {
        return Err(CacheabilityError::UnsupportedFrom {
            construct: "FULL JOIN",
        });
    }
    // ON must be equality / AND of equalities. USING/NATURAL resolve to
    // the same equi-join shape; CROSS (no predicate) is fine.
    let condition_valid = match join.qual() {
        JoinQual::On(condition) => join_condition_valid(condition),
        JoinQual::Using(_) | JoinQual::Natural | JoinQual::Cross => true,
    };
    if !condition_valid {
        return Err(CacheabilityError::UnsupportedFrom {
            construct: "non-equi join condition",
        });
    }
    source_supported(join.left(), fv)?;
    source_supported(join.right(), fv)
}

/// Whether a join condition is an equality or an AND of equalities.
fn join_condition_valid(expr: Where<'_>) -> bool {
    match expr.kind() {
        WhereKind::Binary(binary) => match binary.op() {
            BinaryOp::Equal => true,
            BinaryOp::And => {
                join_condition_valid(binary.lexpr()) && join_condition_valid(binary.rexpr())
            }
            BinaryOp::Or
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => false,
        },
        WhereKind::Scalar(_)
        | WhereKind::Unary(_)
        | WhereKind::Multi(_)
        | WhereKind::Subquery(_) => false,
    }
}

/// A predicate: comparisons, AND/OR, LIKE, multi-operand forms, unary
/// tests and subqueries, each over cacheable operands.
fn where_cacheable(
    expr: Where<'_>,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match expr.kind() {
        WhereKind::Binary(binary) => {
            where_cacheable(binary.lexpr(), ctx, fv)?;
            where_cacheable(binary.rexpr(), ctx, fv)
        }
        WhereKind::Scalar(scalar) => scalar_cacheable(scalar, ctx, fv),
        WhereKind::Multi(multi) => multi
            .exprs()
            .try_for_each(|operand| where_cacheable(operand, ctx, fv)),
        WhereKind::Unary(unary) => where_cacheable(unary.expr(), ctx, fv),
        WhereKind::Subquery(subquery) => {
            subquery_inner_cacheable(subquery.query(), fv)?;
            if let Some(test) = subquery.test_expr() {
                scalar_cacheable(test, ctx, fv)?;
            }
            Ok(())
        }
    }
}

/// A subquery's inner query; LIMIT is allowed, as in a derived table.
fn subquery_inner_cacheable(
    query: Query<'_>,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    body_cacheable(query.body(), fv)
}

fn scalar_cacheable(
    expr: Scalar<'_>,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match expr.kind() {
        ScalarKind::Column(_) | ScalarKind::Literal(_) => Ok(()),
        ScalarKind::Function(function) => function_cacheable(function, ctx, fv),
        ScalarKind::Case(case) => case_cacheable(case, ctx, fv),
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_cacheable(arithmetic.left(), ctx, fv)?;
            scalar_cacheable(arithmetic.right(), ctx, fv)
        }
        ScalarKind::Subquery(subquery) => subquery_inner_cacheable(subquery.query(), fv),
        ScalarKind::Array(array) => array
            .elements()
            .try_for_each(|element| scalar_cacheable(element, ctx, fv)),
        // A cast is pure coercion: the wrapped expression decides.
        ScalarKind::TypeCast(cast) => scalar_cacheable(cast.expr(), ctx, fv),
    }
}

/// A function call: immutable everywhere, any volatility in the SELECT list
/// (re-evaluated at serve time); its arguments and FILTER must be cacheable.
fn function_cacheable(
    function: Function<'_>,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    if let Some(schema) = function.schema()
        && schema != "pg_catalog"
    {
        return Err(CacheabilityError::SchemaQualifiedFunction {
            function: format!("{schema}.{}", function.name()).into(),
        });
    }
    let name = function.name().to_lowercase();
    let is_immutable = matches!(fv.get(name.as_str()), Some(FunctionVolatility::Immutable));
    if !is_immutable && !matches!(ctx, ExprContext::SelectList) {
        return Err(CacheabilityError::NonImmutableFunction { function: name });
    }
    for arg in function.args() {
        scalar_cacheable(arg, ctx, fv)?;
    }
    // FILTER predicates run per input row, so a volatile function there is
    // rejected as in a WHERE.
    if let Some(filter) = function.filter() {
        where_cacheable(filter, ExprContext::WhereClause, fv)?;
    }
    Ok(())
}

fn case_cacheable(
    case: Case<'_>,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    if let Some(arg) = case.arg() {
        scalar_cacheable(arg, ctx, fv)?;
    }
    for when in case.whens() {
        where_cacheable(when.condition(), ctx, fv)?;
        scalar_cacheable(when.result(), ctx, fv)?;
    }
    if let Some(default) = case.default() {
        scalar_cacheable(default, ctx, fv)?;
    }
    Ok(())
}

/// Whether the query contains any function that can modify the database:
/// a VOLATILE function, or one absent from the volatility map (unknown, so
/// conservatively volatile). Every function anywhere in the tree counts.
pub fn query_tree_has_volatile_function(tree: &QueryTree, fv: &FunctionVolatilityMap) -> bool {
    tree.nodes().any(|node| {
        let NodeKind::Function { name, .. } = &node.kind else {
            return false;
        };
        !matches!(
            fv.get(name.to_lowercase().as_str()),
            Some(FunctionVolatility::Immutable | FunctionVolatility::Stable)
        )
    })
}
