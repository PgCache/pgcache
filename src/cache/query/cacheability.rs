//! The cacheability rules: which query shapes, FROM sources, joins,
//! predicates and functions a cached query may contain.

use ecow::EcoString;

use super::{CacheabilityError, FunctionVolatilityMap};
use crate::catalog::FunctionVolatility;
use crate::query::ast::rope::{
    Body, Case, Function, Join, JoinQual, NodeKind, Query, QueryTree, Scalar, ScalarKind, Select,
    SelectItem, Source, Where, WhereKind,
};
use crate::query::ast::{BinaryOp, JoinType, UnaryOp};

/// Reject queries that reference a PostgreSQL system catalog: a `pg_`
/// prefix on the schema or the table name, anywhere in the tree.
pub(super) fn references_system_catalog(tree: &QueryTree) -> Result<(), CacheabilityError> {
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

/// Whether a join condition is an equality, a null test on a column (the
/// soft-delete idiom, PGC-510), or an AND of those.
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
        WhereKind::Unary(unary) => match unary.op() {
            UnaryOp::IsNull | UnaryOp::IsNotNull => matches!(
                unary.expr().kind(),
                WhereKind::Scalar(scalar) if matches!(scalar.kind(), ScalarKind::Column(_))
            ),
            UnaryOp::Not
            | UnaryOp::IsTrue
            | UnaryOp::IsNotTrue
            | UnaryOp::IsFalse
            | UnaryOp::IsNotFalse => false,
        },
        WhereKind::Scalar(_) | WhereKind::Multi(_) | WhereKind::Subquery(_) => false,
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

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use std::collections::HashMap;

    use super::*;
    use crate::cache::query::CacheableQuery;
    use crate::query::ast::query_tree_parse;

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
        let tree = query_tree_parse(sql).expect("convert");
        CacheableQuery::try_new(tree, &fv)
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

    /// A null test on a column beside the join's equalities is cacheable in
    /// an inner, LEFT or RIGHT join; FULL stays out, as does a null test on
    /// anything but a column, or any other unary test.
    #[test]
    fn test_join_null_test_condition_cacheability() {
        let cacheable = [
            "SELECT * FROM a JOIN b ON a.id = b.a_id AND b.deleted_at IS NULL WHERE a.id = 1",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.deleted_at IS NULL WHERE a.id = 1",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.deleted_at IS NOT NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND a.archived_at IS NULL",
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id AND a.deleted_at IS NULL WHERE b.id = 1",
            "SELECT * FROM a LEFT JOIN b ON b.deleted_at IS NULL AND a.id = b.a_id AND a.x = b.x",
        ];
        for sql in cacheable {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
        let rejected = [
            "SELECT * FROM a FULL JOIN b ON a.id = b.a_id AND b.deleted_at IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND lower(b.name) IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.deleted_at + 1 IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.active IS TRUE",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND NOT b.deleted_at IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id OR b.deleted_at IS NULL",
        ];
        for sql in rejected {
            assert!(
                matches!(
                    check_cacheable(sql),
                    Err(CacheabilityError::UnsupportedFrom { .. })
                ),
                "{sql}"
            );
        }
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
    fn test_pg_catalog_qualified_function_cacheable() {
        assert!(check_cacheable("SELECT id FROM users WHERE pg_catalog.lower(name) = 'x'").is_ok());
        assert!(check_cacheable("SELECT pg_catalog.lower(name) FROM users").is_ok());
    }

    #[test]
    fn test_other_schema_qualified_function_not_cacheable() {
        for sql in [
            "SELECT myschema.lower(name) FROM users",
            "SELECT id FROM users WHERE myschema.lower(name) = 'x'",
        ] {
            let err = check_cacheable(sql).expect_err("schema-qualified function rejected");
            assert!(
                matches!(&err, CacheabilityError::SchemaQualifiedFunction { function } if function == "myschema.lower"),
                "{sql}: {err}"
            );
        }
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
            let result = query_tree_parse(sql);
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
            let result = query_tree_parse(sql);
            assert!(
                result.is_err(),
                "should reject locking clause in subquery: {sql}"
            );
        }
    }
}
