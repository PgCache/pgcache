//! The cacheability rules: which query shapes, FROM sources, joins,
//! predicates and functions a cached query may contain.

use std::ops::ControlFlow;

use ecow::EcoString;

use super::{CacheabilityError, FunctionVolatilityMap};
use crate::catalog::FunctionVolatility;
use crate::query::ast::{
    AstNode, BinaryOp, CaseExpr, CteRefNode, FunctionCall, JoinNode, JoinQual, JoinType, MultiOp,
    QueryBody, QueryExpr, ScalarExpr, SelectNode, SetOpNode, SubLinkType, TableNode, TableSource,
    TableSubqueryNode, WhereExpr,
};

/// Reject queries that reference a PostgreSQL system catalog.
///
/// The `pg_` prefix is reserved by PostgreSQL for system schemas and system
/// relation names, so a match in either the schema or the table name marks a
/// catalog reference. Covers nested subqueries and CTEs via full-tree traversal.
pub(super) fn references_system_catalog(query: &QueryExpr) -> Result<(), CacheabilityError> {
    // Break on the first catalog table, carrying its name for the error.
    let found = query.try_for_each_node::<TableNode, EcoString>(&mut |table| {
        let is_catalog =
            table.schema.as_deref().is_some_and(pg_prefixed) || pg_prefixed(&table.name);
        if is_catalog {
            ControlFlow::Break(match &table.schema {
                Some(schema) => EcoString::from(format!("{schema}.{}", table.name)),
                None => table.name.clone(),
            })
        } else {
            ControlFlow::Continue(())
        }
    });
    match found {
        ControlFlow::Break(relation) => Err(CacheabilityError::SystemCatalogReference { relation }),
        ControlFlow::Continue(()) => Ok(()),
    }
}

/// Whether `s` begins with PostgreSQL's reserved `pg_` prefix, ASCII
/// case-insensitively and without allocating (vs `to_lowercase().starts_with`).
fn pg_prefixed(s: &str) -> bool {
    s.as_bytes()
        .get(..3)
        .is_some_and(|p| p.eq_ignore_ascii_case(b"pg_"))
}

/// Check if a query body is cacheable.
/// Recursively validates SELECT nodes and set operation branches.
pub(super) fn is_cacheable_body(
    body: &QueryBody,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match body {
        QueryBody::Select(node) => is_cacheable_select(node, fv),
        QueryBody::Values(_) => {
            // VALUES clauses are not cacheable as standalone queries
            Err(CacheabilityError::UnsupportedQueryType { kind: "VALUES" })
        }
        QueryBody::SetOp(set_op) => is_cacheable_set_op(set_op, fv),
    }
}

/// Check if a SELECT node is cacheable.
fn is_cacheable_select(
    node: &SelectNode,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    // Check FROM clause (tables, joins, subqueries)
    is_supported_from(node, ExprContext::FromClause, fv)?;

    // Check WHERE clause (including any subqueries)
    is_cacheable_where(node, ExprContext::WhereClause, fv)?;

    // Check SELECT list for subqueries
    is_cacheable_select_list(node, ExprContext::SelectList, fv)?;

    Ok(())
}

/// Check if a set operation (UNION/INTERSECT/EXCEPT) is cacheable.
/// Both branches must be cacheable.
fn is_cacheable_set_op(
    set_op: &SetOpNode,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    // LIMIT/OFFSET on branches makes them non-cacheable
    if set_op.left.limit.is_some() || set_op.right.limit.is_some() {
        return Err(CacheabilityError::HasLimit);
    }

    // Recursively validate both branches (subquery cacheability checked recursively)
    is_cacheable_body(&set_op.left.body, fv)?;
    is_cacheable_body(&set_op.right.body, fv)?;

    Ok(())
}

fn is_supported_from(
    select: &SelectNode,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match select.from.as_slice() {
        [TableSource::Join(join)] => is_supported_join(join, ctx, fv),
        [TableSource::Table(_)] => Ok(()),
        [TableSource::Subquery(sub)] => is_cacheable_table_subquery(sub, ctx, fv),
        [TableSource::CteRef(cte_ref)] => is_cacheable_cte_ref(cte_ref, ctx, fv),
        [] => Err(CacheabilityError::UnsupportedFrom {
            construct: "no FROM clause",
        }),
        _ => Err(CacheabilityError::UnsupportedFrom {
            construct: "comma join",
        }),
    }
}

/// Check if a table subquery (derived table) is cacheable.
fn is_cacheable_table_subquery(
    subquery: &TableSubqueryNode,
    _ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    // LATERAL subqueries are not supported (they reference outer scope)
    if subquery.lateral {
        return Err(CacheabilityError::UnsupportedSubquery { kind: "LATERAL" });
    }

    // Subquery must have an alias
    if subquery.alias.is_none() {
        return Err(CacheabilityError::UnsupportedSubquery {
            kind: "derived table without alias",
        });
    }

    // Inner query must be cacheable
    // Note: We check the inner query for cacheability but don't check for LIMIT
    // since LIMIT in a derived table subquery is valid SQL
    is_cacheable_body(&subquery.query.body, fv)
}

fn is_supported_join(
    join: &JoinNode,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    // FULL OUTER JOINs are not cacheable — both sides are optional
    if join.join_type == JoinType::Full {
        return Err(CacheabilityError::UnsupportedFrom {
            construct: "FULL JOIN",
        });
    }

    // ON must be equality / AND of equalities. USING/NATURAL resolve to
    // the same equi-join shape; CROSS (no predicate) is fine.
    let condition_valid = match &join.qual {
        JoinQual::On(expr) => join_condition_is_valid(expr),
        JoinQual::Using(_) | JoinQual::Natural | JoinQual::Cross => true,
    };

    if !condition_valid {
        return Err(CacheabilityError::UnsupportedFrom {
            construct: "non-equi join condition",
        });
    }

    // Recursively validate nested joins/tables/subqueries
    is_supported_table_source(&join.left, ctx, fv)?;
    is_supported_table_source(&join.right, ctx, fv)?;

    Ok(())
}

/// Check if a join condition contains only equalities or AND of equalities.
fn join_condition_is_valid(expr: &WhereExpr) -> bool {
    match expr {
        WhereExpr::Binary(b) => match b.op {
            BinaryOp::Equal => true,
            BinaryOp::And => join_condition_is_valid(&b.lexpr) && join_condition_is_valid(&b.rexpr),
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
        WhereExpr::Scalar(_)
        | WhereExpr::Unary(_)
        | WhereExpr::Multi(_)
        | WhereExpr::Subquery { .. } => false,
    }
}

/// Check if a table source (in a join) is supported.
fn is_supported_table_source(
    source: &TableSource,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match source {
        TableSource::Join(nested) => is_supported_join(nested, ctx, fv),
        TableSource::Table(_) => Ok(()),
        TableSource::Subquery(sub) => is_cacheable_table_subquery(sub, ctx, fv),
        TableSource::CteRef(cte_ref) => is_cacheable_cte_ref(cte_ref, ctx, fv),
    }
}

/// Check if a CTE reference is cacheable.
fn is_cacheable_cte_ref(
    cte_ref: &CteRefNode,
    _ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    is_cacheable_body(&cte_ref.query.body, fv)
}

/// Check if a SELECT's WHERE clause can be efficiently cached.
///
/// Supports:
/// - Simple equality, AND of equalities, OR of equalities in WHERE
/// - GROUP BY and HAVING (aggregation performed on cached rows at retrieval time)
/// - Non-correlated subqueries (EXISTS, IN, scalar)
///
fn is_cacheable_where(
    select: &SelectNode,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match &select.where_clause {
        Some(where_expr) => is_cacheable_expr(where_expr, ctx, fv),
        None => Ok(()), // No WHERE clause is always cacheable
    }
}

/// Where in the query tree a cacheability check is being evaluated.
///
/// Functions in the SELECT list (e.g. CASE WHEN conditions) are safe — they're
/// re-evaluated against cached rows. Immutable functions are safe in any context.
/// Non-immutable functions in WHERE/FROM are rejected.
#[derive(Clone, Copy)]
enum ExprContext {
    /// Expression in the FROM clause — only immutable functions allowed
    FromClause,
    /// Expression in the WHERE clause — only immutable functions allowed
    WhereClause,
    /// Expression in the SELECT list — all functions allowed
    SelectList,
}

/// Determine if a WHERE expression can be efficiently cached.
/// Supports simple comparisons, AND/OR of comparisons, and non-correlated subqueries.
/// Functions are gated by volatility: immutable allowed everywhere, others only in SelectList.
fn is_cacheable_expr(
    expr: &WhereExpr,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match expr {
        WhereExpr::Binary(binary_expr) => match binary_expr.op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => {
                // Recursively check both sides are cacheable
                is_cacheable_expr(&binary_expr.lexpr, ctx, fv)?;
                is_cacheable_expr(&binary_expr.rexpr, ctx, fv)?;
                // Allow comparisons where both sides are cacheable:
                // - (Column, Value) - simple comparisons for cache filtering
                // - (Column, Column) - join conditions in subqueries
                // - (Column, Subquery) - scalar subquery comparisons
                Ok(())
            }
            BinaryOp::And | BinaryOp::Or => {
                is_cacheable_expr(&binary_expr.lexpr, ctx, fv)?;
                is_cacheable_expr(&binary_expr.rexpr, ctx, fv)
            }
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                is_cacheable_expr(&binary_expr.lexpr, ctx, fv)?;
                is_cacheable_expr(&binary_expr.rexpr, ctx, fv)
            }
        },
        WhereExpr::Scalar(scalar) => is_cacheable_scalar_expr(scalar, ctx, fv),
        WhereExpr::Multi(multi_expr) => match multi_expr.op {
            MultiOp::In
            | MultiOp::NotIn
            | MultiOp::Between
            | MultiOp::NotBetween
            | MultiOp::BetweenSymmetric
            | MultiOp::NotBetweenSymmetric
            | MultiOp::Any { .. }
            | MultiOp::All { .. } => {
                for e in &multi_expr.exprs {
                    is_cacheable_expr(e, ctx, fv)?;
                }
                Ok(())
            }
        },
        WhereExpr::Unary(unary_expr) => is_cacheable_expr(&unary_expr.expr, ctx, fv),
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            // Check the inner query is cacheable
            is_cacheable_subquery_inner(query, fv)?;

            // Check test_expr (left-hand side for IN/ANY/ALL) is cacheable
            if let Some(test) = test_expr {
                is_cacheable_scalar_expr(test, ctx, fv)?;
            }

            // All supported sublink types are cacheable if inner query is cacheable
            match sublink_type {
                SubLinkType::Exists | SubLinkType::Any | SubLinkType::Expr => Ok(()),
                SubLinkType::All => {
                    // ALL subqueries can be complex - allow for now
                    Ok(())
                }
            }
        }
    }
}

/// Check if a subquery's inner query is cacheable.
/// For subqueries, we allow LIMIT since it's valid in derived tables.
fn is_cacheable_subquery_inner(
    query: &QueryExpr,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    // Check the query body (SELECT, VALUES, or SetOp)
    is_cacheable_body(&query.body, fv)
}

/// Check if a SELECT list contains cacheable expressions.
fn is_cacheable_select_list(
    select: &SelectNode,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    use crate::query::ast::{SelectColumn, SelectColumns};

    match &select.columns {
        SelectColumns::None => Ok(()),
        SelectColumns::Columns(cols) => {
            for col in cols {
                if let SelectColumn::Expr { expr, .. } = col {
                    is_cacheable_scalar_expr(expr, ctx, fv)?;
                }
            }
            Ok(())
        }
    }
}

/// Check if a column expression is cacheable.
fn is_cacheable_scalar_expr(
    expr: &ScalarExpr,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    match expr {
        ScalarExpr::Column(_) | ScalarExpr::Literal(_) => Ok(()),
        ScalarExpr::Function(func) => function_call_cacheable(func, ctx, fv),
        ScalarExpr::Case(case) => case_cacheable(case, ctx, fv),
        ScalarExpr::Arithmetic(arith) => {
            is_cacheable_scalar_expr(&arith.left, ctx, fv)?;
            is_cacheable_scalar_expr(&arith.right, ctx, fv)
        }
        ScalarExpr::Subquery(query) => is_cacheable_subquery_inner(query, fv),
        ScalarExpr::Array(elems) => elems
            .iter()
            .try_for_each(|e| is_cacheable_scalar_expr(e, ctx, fv)),
        ScalarExpr::TypeCast { expr, .. } => {
            // Type cast is pure coercion — defer to the wrapped expression.
            is_cacheable_scalar_expr(expr, ctx, fv)
        }
    }
}

/// A function call: immutable everywhere, any volatility in the SELECT list
/// (re-evaluated at serve time); its arguments and FILTER must be cacheable too.
fn function_call_cacheable(
    func: &FunctionCall,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    let function = func.name.to_lowercase();
    let is_immutable = matches!(
        fv.get(function.as_str()),
        Some(FunctionVolatility::Immutable)
    );
    if !is_immutable && !matches!(ctx, ExprContext::SelectList) {
        return Err(CacheabilityError::NonImmutableFunction { function });
    }
    for arg in &func.args {
        is_cacheable_scalar_expr(arg, ctx, fv)?;
    }
    // FILTER predicates run per input row, so use WhereClause context
    // to reject volatile functions inside FILTER.
    if let Some(filter) = &func.agg_filter {
        is_cacheable_expr(filter, ExprContext::WhereClause, fv)?;
    }
    Ok(())
}

fn case_cacheable(
    case: &CaseExpr,
    ctx: ExprContext,
    fv: &FunctionVolatilityMap,
) -> Result<(), CacheabilityError> {
    if let Some(arg) = &case.arg {
        is_cacheable_scalar_expr(arg, ctx, fv)?;
    }
    for when in &case.whens {
        is_cacheable_expr(&when.condition, ctx, fv)?;
        is_cacheable_scalar_expr(&when.result, ctx, fv)?;
    }
    if let Some(default) = &case.default {
        is_cacheable_scalar_expr(default, ctx, fv)?;
    }
    Ok(())
}

/// Whether the query contains any function that can modify the database.
///
/// Only VOLATILE functions can write; IMMUTABLE and STABLE cannot. A function
/// absent from the volatility map is unknown, so it is conservatively treated
/// as volatile. Used to classify an uncacheable forwarded SELECT (e.g. a
/// data-modifying-CTE-free `SELECT nextval(...)`) as a potential write for
/// read-after-write tracking (PGC-124). The full-tree walk catches functions
/// nested inside other functions and inside WHERE, which the cacheability
/// check's short-circuit on the first non-immutable function would miss.
pub fn query_has_volatile_function(query: &QueryExpr, fv: &FunctionVolatilityMap) -> bool {
    query
        .try_for_each_node::<FunctionCall, ()>(&mut |func| {
            match fv.get(func.name.to_lowercase().as_str()) {
                Some(FunctionVolatility::Immutable | FunctionVolatility::Stable) => {
                    ControlFlow::Continue(())
                }
                // Volatile or unknown: assume it may write.
                _ => ControlFlow::Break(()),
            }
        })
        .is_break()
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use std::collections::HashMap;

    use super::*;
    use crate::cache::query::CacheableQuery;
    use crate::query::ast::query_expr_parse;

    /// Build a test volatility map with common functions.
    fn test_func_volatility() -> FunctionVolatilityMap {
        let mut map = HashMap::new();
        for name in [
            "lower",
            "upper",
            "length",
            "abs",
            "concat",
            "trim",
            "btrim",
            "ltrim",
            "rtrim",
            "replace",
            "substring",
            "date_trunc",
        ] {
            map.insert(name.into(), FunctionVolatility::Immutable);
        }
        for name in ["now", "current_timestamp"] {
            map.insert(name.into(), FunctionVolatility::Stable);
        }
        map.insert("random".into(), FunctionVolatility::Volatile);
        map
    }

    /// Parse SQL and check cacheability using the test volatility map.
    fn check_cacheable(sql: &str) -> Result<CacheableQuery, CacheabilityError> {
        let fv = test_func_volatility();
        let query_expr = query_expr_parse(sql).expect("convert");
        CacheableQuery::try_new(query_expr, &fv)
    }

    #[test]
    fn test_cacheability_error_names_offending_item() {
        let display = |sql: &str| check_cacheable(sql).expect_err("not cacheable").to_string();
        assert_eq!(
            display("SELECT now()"),
            "Unsupported FROM clause: no FROM clause"
        );
        assert_eq!(
            display("SELECT id FROM a, b"),
            "Unsupported FROM clause: comma join"
        );
        assert_eq!(
            display("SELECT a.id FROM a FULL JOIN b ON a.id = b.a_id"),
            "Unsupported FROM clause: FULL JOIN"
        );
        assert_eq!(
            display("SELECT id FROM a WHERE score > RANDOM()"),
            "Non-immutable function: random"
        );
        assert_eq!(
            display("SELECT relname FROM pg_catalog.pg_class"),
            "System catalog reference: pg_catalog.pg_class"
        );
        assert_eq!(
            display("SELECT * FROM (SELECT 1)"),
            "Unsupported subquery: derived table without alias"
        );
    }

    #[test]
    fn test_two_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "2-table inner join should be cacheable");
    }

    #[test]
    fn test_three_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "3-table inner join should be cacheable");
    }

    #[test]
    fn test_four_table_join_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "4-table inner join should be cacheable");
    }

    #[test]
    fn test_left_join_cacheable() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "LEFT JOIN should be cacheable");
    }

    #[test]
    fn test_right_join_cacheable() {
        let sql = "SELECT * FROM a RIGHT JOIN b ON a.id = b.id WHERE b.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "RIGHT JOIN should be cacheable");
    }

    #[test]
    fn test_mixed_join_types_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Mixed join types (INNER + LEFT) should be cacheable"
        );
    }

    #[test]
    fn test_chained_left_joins_cacheable() {
        let sql =
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Chained LEFT JOINs should be cacheable");
    }

    #[test]
    fn test_non_terminal_left_join_cacheable() {
        let sql = "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE b.status = 'active'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Non-terminal LEFT JOIN should be cacheable (CDC handles correctness)"
        );
    }

    #[test]
    fn test_full_join_not_cacheable() {
        let sql = "SELECT * FROM a FULL JOIN b ON a.id = b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom { .. })),
            "FULL JOIN should not be cacheable"
        );
    }

    #[test]
    fn test_join_and_condition_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id = b.id AND a.tenant = b.tenant WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "AND of equalities in join condition should be cacheable"
        );
    }

    #[test]
    fn test_left_join_and_condition_cacheable() {
        let sql =
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND a.tenant = b.tenant WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "LEFT JOIN with AND condition should be cacheable"
        );
    }

    #[test]
    fn test_join_with_non_equality_condition_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id > b.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom { .. })),
            "Non-equality join condition should not be cacheable"
        );
    }

    #[test]
    fn test_nested_join_with_non_equality_not_cacheable() {
        let sql = "SELECT * FROM a JOIN b ON a.id > b.id JOIN c ON b.id = c.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::UnsupportedFrom { .. })),
            "Nested join with non-equality condition should not be cacheable"
        );
    }

    #[test]
    fn test_group_by_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "GROUP BY should be cacheable");
    }

    #[test]
    fn test_group_by_multiple_columns_cacheable() {
        let sql =
            "SELECT status, category FROM orders WHERE tenant_id = 1 GROUP BY status, category";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "GROUP BY with multiple columns should be cacheable"
        );
    }

    #[test]
    fn test_having_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status HAVING status = 'active'";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "HAVING should be cacheable");
    }

    #[test]
    fn test_limit_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "LIMIT should be cacheable");
    }

    #[test]
    fn test_offset_cacheable() {
        let sql = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "OFFSET should be cacheable");
    }

    #[test]
    fn test_group_by_with_limit_cacheable() {
        let sql = "SELECT status FROM orders WHERE tenant_id = 1 GROUP BY status LIMIT 5";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "GROUP BY with LIMIT should be cacheable");
    }

    // ==================== Subquery Tests ====================

    #[test]
    fn test_subquery_in_select_cacheable() {
        // Scalar subqueries in SELECT list are now cacheable
        let sql = "SELECT id, (SELECT x FROM other WHERE id = 1) FROM t WHERE id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Scalar subquery in SELECT list should be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_from_cacheable() {
        // Derived tables (non-LATERAL subqueries) in FROM are now cacheable
        let sql = "SELECT * FROM (SELECT id FROM users) sub WHERE id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Subquery in FROM clause should be cacheable"
        );
    }

    #[test]
    fn test_subquery_in_join_cacheable() {
        // Subqueries in JOIN are now cacheable
        let sql = "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id WHERE a.id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Subquery in JOIN should be cacheable");
    }

    #[test]
    fn test_subquery_in_where_cacheable() {
        // IN subqueries in WHERE are now cacheable
        let sql = "SELECT * FROM t WHERE id IN (SELECT id FROM other)";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Subquery in WHERE clause should be cacheable"
        );
    }

    #[test]
    fn test_subquery_exists_cacheable() {
        let sql = "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "EXISTS subquery should be cacheable, got: {:?}",
            result
        );
    }

    #[test]
    fn test_subquery_scalar_in_where_cacheable() {
        let sql = "SELECT * FROM users WHERE id > (SELECT AVG(id) FROM users)";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Scalar subquery in WHERE should be cacheable, got: {:?}",
            result
        );
    }

    #[test]
    fn test_correlated_scalar_subquery_in_select_cacheable() {
        let sql = "SELECT e.name, \
                   (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
                   FROM employees e ORDER BY e.name";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Correlated scalar subquery in SELECT should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_subquery_nested_cacheable() {
        let sql = "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Nested subqueries should be cacheable");
    }

    #[test]
    fn test_subquery_with_limit_in_outer_cacheable() {
        let sql = "SELECT * FROM (SELECT id FROM users) sub LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Outer LIMIT on simple SELECT should be cacheable"
        );
    }

    #[test]
    fn test_function_in_select_cacheable() {
        let sql = "SELECT COUNT(*), SUM(amount) FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Functions in SELECT should be cacheable");
    }

    // ==================== Set Operation Tests ====================

    #[test]
    fn test_union_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 UNION SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION should be cacheable");
    }

    #[test]
    fn test_union_all_cacheable() {
        let sql =
            "SELECT id FROM a WHERE tenant_id = 1 UNION ALL SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION ALL should be cacheable");
    }

    #[test]
    fn test_intersect_cacheable() {
        let sql =
            "SELECT id FROM a WHERE tenant_id = 1 INTERSECT SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "INTERSECT should be cacheable");
    }

    #[test]
    fn test_except_cacheable() {
        let sql =
            "SELECT id FROM a WHERE tenant_id = 1 EXCEPT SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "EXCEPT should be cacheable");
    }

    #[test]
    fn test_nested_union_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 \
                   UNION SELECT id FROM b WHERE tenant_id = 1 \
                   UNION SELECT id FROM c WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "Nested UNION should be cacheable");
    }

    #[test]
    fn test_union_with_join_cacheable() {
        let sql = "SELECT a.id FROM a JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
                   UNION \
                   SELECT c.id FROM c WHERE c.tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION with JOIN should be cacheable");
    }

    #[test]
    fn test_union_with_outer_limit_cacheable() {
        let sql = "SELECT id FROM a WHERE tenant_id = 1 \
                   UNION SELECT id FROM b WHERE tenant_id = 1 \
                   LIMIT 10";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "UNION with outer LIMIT should be cacheable — \
             all rows are populated per-branch, LIMIT applied at serve time"
        );
    }

    #[test]
    fn test_union_with_branch_limit_not_cacheable() {
        let sql = "(SELECT id FROM a WHERE tenant_id = 1 LIMIT 5) \
                   UNION SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::HasLimit)),
            "UNION with LIMIT in branch should not be cacheable"
        );
    }

    #[test]
    fn test_union_with_subquery_cacheable() {
        // Subqueries in UNION branches are now cacheable
        let sql = "SELECT id FROM a WHERE id IN (SELECT id FROM other) \
                   UNION SELECT id FROM b WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION with subquery should be cacheable");
    }

    #[test]
    fn test_union_with_left_join_cacheable() {
        let sql = "SELECT a.id FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
                   UNION SELECT id FROM c WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "UNION with LEFT JOIN should be cacheable");
    }

    // CTE cacheability tests

    #[test]
    fn test_cte_simple_cacheable() {
        let sql = "WITH x AS (SELECT id FROM users WHERE id = 1) SELECT * FROM x";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "simple CTE should be cacheable: {result:?}");
    }

    #[test]
    fn test_cte_with_join_cacheable() {
        let sql = "WITH active AS (SELECT id, name FROM users WHERE active = true) \
                    SELECT u.id, a.name FROM users u JOIN active a ON u.id = a.id WHERE u.id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "CTE in join should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_cte_multiple_cacheable() {
        let sql = "WITH a AS (SELECT id FROM users WHERE id = 1), \
                    b AS (SELECT id FROM products WHERE id = 2) \
                    SELECT * FROM a JOIN b ON a.id = b.id";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "multiple CTEs should be cacheable: {result:?}"
        );
    }

    // ==================== Function in CASE WHEN Tests ====================

    #[test]
    fn test_case_with_function_in_condition_cacheable() {
        let sql = "SELECT CASE WHEN date_trunc('day', created_at) = '2024-01-01' THEN 'yes' ELSE 'no' END FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "CASE with function in condition should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_case_with_nested_function_cacheable() {
        let sql = "SELECT CASE WHEN date_trunc('day', now()) = '2024-01-01' THEN 'yes' ELSE 'no' END FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "CASE with nested function calls should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_immutable_function_in_where_cacheable() {
        let sql = "SELECT * FROM orders WHERE date_trunc('day', created_at) = '2024-01-01'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Immutable function in WHERE clause should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_immutable_lower_in_where_cacheable() {
        let sql = "SELECT * FROM users WHERE lower(name) = 'foo'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "lower() in WHERE should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_nested_immutable_functions_in_where_cacheable() {
        let sql = "SELECT * FROM users WHERE lower(upper(name)) = 'foo'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Nested immutable functions in WHERE should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_immutable_in_select_and_where_cacheable() {
        let sql = "SELECT lower(name) FROM users WHERE lower(name) = 'foo'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Immutable function in both SELECT and WHERE should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_stable_function_in_where_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE now() > created_at";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::NonImmutableFunction { .. })),
            "Stable function in WHERE should not be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_volatile_function_in_where_not_cacheable() {
        let sql = "SELECT * FROM orders WHERE random() > 0.5";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::NonImmutableFunction { .. })),
            "Volatile function in WHERE should not be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_unknown_function_in_where_not_cacheable() {
        let sql = "SELECT * FROM users WHERE unknown_func(col) = 1";
        let result = check_cacheable(sql);
        assert!(
            matches!(result, Err(CacheabilityError::NonImmutableFunction { .. })),
            "Unknown function in WHERE should not be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_stable_function_in_select_list_cacheable() {
        // Functions in SELECT list are always allowed (re-evaluated at serve time)
        let sql = "SELECT now() FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "Stable function in SELECT list should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_immutable_function_case_insensitive() {
        // pg_query preserves case; lookup should be case-insensitive
        let sql = "SELECT * FROM users WHERE LOWER(name) = 'foo'";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "LOWER (uppercase) in WHERE should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_type_cast_cacheable() {
        // TypeCast on non-literal SELECT expressions is pure coercion; the
        // wrapper must not block cacheability.
        let cases = [
            "SELECT COUNT(*)::INT FROM a",
            "SELECT col::text FROM a",
            "SELECT (col + 1)::numeric(10,2) FROM a",
            "SELECT a.col::int FROM a",
            "SELECT COUNT(*)::INT, SUM(col)::NUMERIC(18,2) FROM a GROUP BY col",
        ];
        for sql in cases {
            let result = check_cacheable(sql);
            assert!(
                result.is_ok(),
                "should be cacheable: {sql} (err: {result:?})"
            );
        }
    }

    #[test]
    fn test_system_catalog_reference_uncacheable() {
        // psql's \d and friends query system catalogs, which can't be logically
        // replicated or registered against the cache db.
        let cases = [
            "SELECT * FROM pg_catalog.pg_class",
            "SELECT relname FROM pg_class",
            "SELECT * FROM pg_namespace WHERE nspname = 'public'",
            "SELECT * FROM pg_catalog.pg_attribute a JOIN pg_class c ON a.attrelid = c.oid",
            "SELECT * FROM users WHERE id IN (SELECT oid FROM pg_class)",
            "SELECT n.nspname FROM PG_NAMESPACE n",
        ];
        for sql in cases {
            let result = check_cacheable(sql);
            assert!(
                matches!(
                    result,
                    Err(CacheabilityError::SystemCatalogReference { .. })
                ),
                "should reject system catalog reference: {sql} (got {result:?})"
            );
        }
    }

    #[test]
    fn test_non_catalog_table_cacheable() {
        // A user table whose name merely contains "pg" is not a catalog reference.
        let sql = "SELECT * FROM pgbench_accounts WHERE aid = 1";
        let result = check_cacheable(sql);
        assert!(
            result.is_ok(),
            "table name containing 'pg' should be cacheable: {result:?}"
        );
    }

    #[test]
    fn test_locking_clause_uncacheable() {
        // All locking clause variants should be rejected at the AST level
        let cases = [
            "SELECT * FROM orders WHERE id = 1 FOR UPDATE",
            "SELECT * FROM orders WHERE id = 1 FOR SHARE",
            "SELECT * FROM orders WHERE id = 1 FOR NO KEY UPDATE",
            "SELECT * FROM orders WHERE id = 1 FOR KEY SHARE",
            "SELECT * FROM orders WHERE id = 1 FOR UPDATE NOWAIT",
            "SELECT * FROM orders WHERE id = 1 FOR UPDATE SKIP LOCKED",
        ];

        for sql in cases {
            let result = query_expr_parse(sql);
            assert!(result.is_err(), "should reject locking clause: {sql}");
        }
    }

    #[test]
    fn test_locking_clause_in_subquery_uncacheable() {
        let cases = [
            // FROM subquery
            "SELECT * FROM (SELECT * FROM orders FOR UPDATE) sub WHERE sub.id = 1",
            // WHERE EXISTS subquery
            "SELECT * FROM orders o WHERE EXISTS (SELECT 1 FROM items i WHERE i.order_id = o.id FOR UPDATE)",
            // WHERE IN subquery
            "SELECT * FROM orders WHERE id IN (SELECT id FROM orders FOR SHARE)",
            // Scalar subquery in SELECT list
            "SELECT (SELECT count(*) FROM items FOR UPDATE) FROM orders WHERE id = 1",
        ];

        for sql in cases {
            let result = query_expr_parse(sql);
            assert!(
                result.is_err(),
                "should reject locking clause in subquery: {sql}"
            );
        }
    }
}
