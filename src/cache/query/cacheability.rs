//! The cacheability rules: which query shapes, FROM sources, joins,
//! predicates and functions a cached query may contain.

use ecow::EcoString;

use super::CacheabilityError;
#[cfg(feature = "proxy")]
use crate::catalog::{FunctionCatalog, schema_is_builtin};
use crate::catalog::{FunctionFacts, FunctionVolatility, is_settings_stable};
use crate::query::ast::rope::{
    Body, Case, FrameBound, Function, Join, JoinQual, NodeKind, Operator, Query, QueryTree, Scalar,
    ScalarKind, Select, SelectItem, Source, Where, WhereKind, Window,
};
use crate::query::ast::{Deparse, JoinType, LiteralValue, ScalarOp};
use crate::query::transform::{scalar_is_term_fragment, where_subquery_is_term_fragment};

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
/// An immutable function is cacheable wherever it stands. A volatile one
/// never is: it may answer differently per call or have side effects, which
/// no replay on the cache database reproduces. A stable one is cacheable in
/// the query's own SELECT list alone, and only an allowlisted builtin whose
/// result the serve recomputes per hit from the clock and the session's
/// settings (PGC-589); elsewhere it would decide membership or order, a
/// subquery's or derived table's SELECT list included, since its value
/// feeds the enclosing query. A join's ON also rejects subqueries:
/// decorrelation reads only WHERE, so a correlated one there would register
/// in its correlated form.
#[derive(Clone, Copy)]
enum ExprContext {
    WhereClause,
    JoinOn,
    SelectList,
}

/// A query body: SELECT nodes and set-operation branches recursively. The
/// branches of a set operation share the query's output, so they keep its
/// `output` context.
fn body_cacheable(
    body: Body<'_>,
    output: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    match body {
        Body::Select(select) => select_cacheable(select, output, functions),
        Body::Values(_) => Err(CacheabilityError::UnsupportedQueryType { kind: "VALUES" }),
        Body::SetOp(set_op) => {
            // LIMIT/OFFSET on branches makes them non-cacheable
            if set_op.left().has_limit() || set_op.right().has_limit() {
                return Err(CacheabilityError::HasLimit);
            }
            query_cacheable_at(set_op.left(), output, functions)?;
            query_cacheable_at(set_op.right(), output, functions)
        }
    }
}

/// The statement's query: its SELECT list is the output the serve
/// recomputes per hit, the one place a stable call is admitted.
pub(super) fn query_cacheable(
    query: Query<'_>,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    query_cacheable_at(query, ExprContext::SelectList, functions)
}

/// A query whose SELECT list feeds the enclosing query: a subquery, a
/// derived table or a CTE. Its items decide membership or order there, so
/// they are judged like a predicate.
fn nested_query_cacheable(
    query: Query<'_>,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    query_cacheable_at(query, ExprContext::WhereClause, functions)
}

/// A query: its body, with `output` the context its SELECT list is judged
/// in, and its ORDER BY, whose expressions decide the order served and are
/// judged like a predicate.
fn query_cacheable_at(
    query: Query<'_>,
    output: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    body_cacheable(query.body(), output, functions)?;
    for clause in query.order_by() {
        scalar_cacheable(clause.expr(), ExprContext::WhereClause, functions)?;
    }
    Ok(())
}

fn select_cacheable(
    select: Select<'_>,
    output: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    from_supported(select, functions)?;
    if let Some(where_clause) = select.where_clause() {
        where_cacheable(where_clause, ExprContext::WhereClause, functions)?;
    }
    for item in select.items().into_iter().flatten() {
        if let SelectItem::Expr(expr) = item {
            scalar_cacheable(expr.expr(), output, functions)?;
        }
    }
    // GROUP BY admits column references only (the converter refuses
    // expressions there); HAVING filters groups like a WHERE filters rows.
    if let Some(having) = select.having() {
        where_cacheable(having, ExprContext::WhereClause, functions)?;
    }
    Ok(())
}

fn from_supported(
    select: Select<'_>,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    let mut from = select.from();
    match (from.next(), from.next()) {
        (Some(Source::Join(join)), None) => join_supported(join, functions),
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
            nested_query_cacheable(subquery.query(), functions)
        }
        (Some(Source::CteRef(cte_ref)), None) => nested_query_cacheable(cte_ref.query(), functions),
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
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    match source {
        Source::Join(nested) => join_supported(nested, functions),
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
            nested_query_cacheable(subquery.query(), functions)
        }
        Source::CteRef(cte_ref) => nested_query_cacheable(cte_ref.query(), functions),
    }
}

fn join_supported(join: Join<'_>, functions: &dyn FunctionFacts) -> Result<(), CacheabilityError> {
    // FULL OUTER JOINs are not cacheable: both sides are optional.
    if join.join_type() == JoinType::Full {
        return Err(CacheabilityError::UnsupportedFrom {
            construct: "FULL JOIN",
        });
    }
    // Any predicate WHERE accepts is accepted in ON: the per-reference
    // predicate model (ADR-059) keeps what it cannot read as opaque.
    if let JoinQual::On(condition) = join.qual() {
        where_cacheable(condition, ExprContext::JoinOn, functions)?;
    }
    source_supported(join.left(), functions)?;
    source_supported(join.right(), functions)
}

/// A predicate: comparisons, AND/OR, LIKE, multi-operand forms, unary
/// tests and subqueries, each over cacheable operands.
fn where_cacheable(
    expr: Where<'_>,
    ctx: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    match expr.kind() {
        WhereKind::Binary(binary) => {
            where_cacheable(binary.lexpr(), ctx, functions)?;
            where_cacheable(binary.rexpr(), ctx, functions)
        }
        WhereKind::Scalar(scalar) => scalar_cacheable(scalar, ctx, functions),
        WhereKind::Multi(multi) => multi
            .exprs()
            .try_for_each(|operand| where_cacheable(operand, ctx, functions)),
        WhereKind::Unary(unary) => where_cacheable(unary.expr(), ctx, functions),
        WhereKind::Subquery(subquery) => {
            // The FROM-less wrapper over a session term folds to a literal
            // (PGC-580), like the scalar form.
            if where_subquery_is_term_fragment(subquery) {
                return Ok(());
            }
            subquery_inner_cacheable(subquery.query(), ctx, functions)?;
            if let Some(test) = subquery.test_expr() {
                scalar_cacheable(test, ctx, functions)?;
            }
            Ok(())
        }
    }
}

/// A subquery's inner query; LIMIT is allowed, as in a derived table.
fn subquery_inner_cacheable(
    query: Query<'_>,
    ctx: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    if matches!(ctx, ExprContext::JoinOn) {
        return Err(CacheabilityError::UnsupportedFrom {
            construct: "subquery in join condition",
        });
    }
    nested_query_cacheable(query, functions)
}

fn scalar_cacheable(
    expr: Scalar<'_>,
    ctx: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    // A session-term fragment folds to a literal under the session's values
    // before the query is keyed (PGC-580), so it is cacheable wherever a
    // literal is; a term the fold cannot reach falls through and refuses.
    if scalar_is_term_fragment(expr) {
        return Ok(());
    }
    match expr.kind() {
        ScalarKind::Column(_) => Ok(()),
        // A session term is a placeholder for a value origin would read from
        // the session; until something substitutes it, the query forwards.
        ScalarKind::Literal(literal) => {
            if let LiteralValue::SessionTerm(term) = literal.value() {
                let mut text = String::new();
                term.deparse(&mut text);
                return Err(CacheabilityError::SessionTerm { term: text.into() });
            }
            Ok(())
        }
        ScalarKind::Function(function) => function_cacheable(function, ctx, functions),
        ScalarKind::Case(case) => case_cacheable(case, ctx, functions),
        ScalarKind::Operator(operator) => {
            operator_volatility_check(operator, ctx)?;
            scalar_cacheable(operator.left(), ctx, functions)?;
            scalar_cacheable(operator.right(), ctx, functions)
        }
        ScalarKind::Subquery(subquery) => {
            subquery_inner_cacheable(subquery.query(), ctx, functions)
        }
        ScalarKind::Array(array) => array
            .elements()
            .try_for_each(|element| scalar_cacheable(element, ctx, functions)),
        // A cast is pure coercion: the wrapped expression decides.
        ScalarKind::TypeCast(cast) => scalar_cacheable(cast.expr(), ctx, functions),
    }
}

/// An operator whose every overload is immutable passes anywhere; one with a
/// STABLE overload (`@@`, `||`) only in the SELECT list, as a stable
/// function does: its rendering depends on the session's settings alone.
fn operator_volatility_check(
    operator: Operator<'_>,
    ctx: ExprContext,
) -> Result<(), CacheabilityError> {
    if let ScalarOp::Json(op) = operator.op()
        && !op.is_immutable()
        && !matches!(ctx, ExprContext::SelectList)
    {
        return Err(CacheabilityError::NonImmutableOperator {
            operator: op.as_ref().into(),
        });
    }
    Ok(())
}

/// A function call: immutable, or an allowlisted stable builtin in the
/// SELECT list, with cacheable arguments, aggregate ORDER BY, FILTER and
/// window. Judged by what `functions` knows of the call: the proxy's
/// catalog-only view, or the writer's with the search_path in force. A call
/// no row answers is non-immutable, conservatively.
fn function_cacheable(
    function: Function<'_>,
    ctx: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    let schema = function.schema().map(EcoString::as_str);
    let name = function.name();
    let arg_count = u32::try_from(function.args().len()).unwrap_or(u32::MAX);
    match functions.call_volatility(schema, name, arg_count) {
        Some(FunctionVolatility::Immutable) => {}
        Some(FunctionVolatility::Volatile) => {
            return Err(CacheabilityError::VolatileFunction {
                function: name.to_lowercase(),
            });
        }
        Some(FunctionVolatility::Stable)
            if matches!(ctx, ExprContext::SelectList)
                && schema.is_none_or(|schema| schema.eq_ignore_ascii_case("pg_catalog"))
                && is_settings_stable(name)
                && functions.call_only_builtins(schema, name, arg_count) => {}
        Some(FunctionVolatility::Stable) | None => {
            return Err(CacheabilityError::NonImmutableFunction {
                function: name.to_lowercase(),
            });
        }
    }
    for arg in function.args() {
        scalar_cacheable(arg, ctx, functions)?;
    }
    // An aggregate's ORDER BY decides the value it builds, a FILTER which
    // rows it sees: both are judged as in a WHERE.
    for clause in function.agg_order() {
        scalar_cacheable(clause.expr(), ExprContext::WhereClause, functions)?;
    }
    if let Some(filter) = function.filter() {
        where_cacheable(filter, ExprContext::WhereClause, functions)?;
    }
    if let Some(window) = function.over() {
        window_cacheable(window, functions)?;
    }
    Ok(())
}

/// A window's PARTITION BY, ORDER BY and frame offsets decide which rows a
/// window function sees, so they are judged like a predicate.
fn window_cacheable(
    window: Window<'_>,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    for expr in window.partition_by() {
        scalar_cacheable(expr, ExprContext::WhereClause, functions)?;
    }
    for clause in window.order_by() {
        scalar_cacheable(clause.expr(), ExprContext::WhereClause, functions)?;
    }
    if let Some(frame) = window.frame() {
        for bound in [frame.start(), frame.end()] {
            if let FrameBound::OffsetPreceding(offset) | FrameBound::OffsetFollowing(offset) = bound
            {
                scalar_cacheable(offset, ExprContext::WhereClause, functions)?;
            }
        }
    }
    Ok(())
}

fn case_cacheable(
    case: Case<'_>,
    ctx: ExprContext,
    functions: &dyn FunctionFacts,
) -> Result<(), CacheabilityError> {
    if let Some(arg) = case.arg() {
        scalar_cacheable(arg, ctx, functions)?;
    }
    for when in case.whens() {
        where_cacheable(when.condition(), ctx, functions)?;
        scalar_cacheable(when.result(), ctx, functions)?;
    }
    if let Some(default) = case.default() {
        scalar_cacheable(default, ctx, functions)?;
    }
    Ok(())
}

/// Whether the query calls a stable function or operator: one the serve
/// must evaluate per hit, since a materialized result or a memoized
/// response would freeze its value (PGC-589). On a cacheable query such a
/// call stands in the SELECT list, the one place the rule admits it.
#[cfg(any(feature = "proxy", test))]
pub(crate) fn query_tree_has_stable_call(tree: &QueryTree, functions: &dyn FunctionFacts) -> bool {
    tree.nodes().any(|node| {
        if let NodeKind::Function {
            schema,
            name,
            arg_count,
            ..
        } = &node.kind
        {
            return functions.call_volatility(schema.as_deref(), name, *arg_count)
                == Some(FunctionVolatility::Stable);
        }
        matches!(&node.kind, NodeKind::Operator { op, .. } if matches!(op, ScalarOp::Json(op) if !op.is_immutable()))
    })
}

/// Whether the query contains any function that can modify the database:
/// a VOLATILE function, or one absent from the catalog (unknown, so
/// conservatively volatile). Every function anywhere in the tree counts,
/// under the worst of the name's overloads in any schema. The built-in
/// calls of `exempt` are left out: `set_config` is volatile yet writes no
/// table, and its effect on the session is tracked by name, so a SELECT
/// calling nothing else volatile is no write for the read-after-write gate.
#[cfg(feature = "proxy")]
pub(crate) fn query_tree_has_volatile_function_except(
    tree: &QueryTree,
    functions: &FunctionCatalog,
    exempt: Option<&str>,
) -> bool {
    tree.nodes().any(|node| {
        let NodeKind::Function { schema, name, .. } = &node.kind else {
            return false;
        };
        if exempt.is_some_and(|exempt| name.eq_ignore_ascii_case(exempt))
            && schema_is_builtin(schema.as_deref())
        {
            return false;
        }
        !matches!(
            functions.name_worst_volatility(name),
            Some(FunctionVolatility::Immutable | FunctionVolatility::Stable)
        )
    })
}

/// Whether any call in the tree may run code that is not PostgreSQL's own
/// (see [`FunctionCatalog::call_may_run_user_code`]): such code can change the
/// session's settings or role without the statement text saying so.
#[cfg(feature = "proxy")]
pub(crate) fn query_tree_has_user_function(tree: &QueryTree, functions: &FunctionCatalog) -> bool {
    tree.nodes().any(|node| {
        let NodeKind::Function { schema, name, .. } = &node.kind else {
            return false;
        };
        functions.call_may_run_user_code(schema.as_deref(), name)
    })
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use super::*;
    use crate::cache::query::CacheableQuery;
    use crate::catalog::{FunctionRow, PathFunctions};
    use crate::query::ast::query_tree_parse;

    /// A test catalog of common builtins.
    fn test_function_catalog() -> FunctionCatalog {
        let immutable = [
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
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "row_number",
            "string_agg",
        ]
        .into_iter()
        .map(|name| (name, FunctionVolatility::Immutable));
        let rows = immutable
            .chain([
                ("now", FunctionVolatility::Stable),
                ("to_char", FunctionVolatility::Stable),
                ("pg_backend_pid", FunctionVolatility::Stable),
                ("random", FunctionVolatility::Volatile),
            ])
            .map(|(name, volatility)| FunctionRow::test_builtin(name, volatility))
            .chain([
                FunctionRow::test_user("app", "tenant_of", FunctionVolatility::Immutable),
                FunctionRow::test_user("app", "tenant_clock", FunctionVolatility::Stable),
                FunctionRow::test_user("app", "lower", FunctionVolatility::Stable),
            ])
            .collect();
        FunctionCatalog::from_rows(rows)
    }

    /// Parse SQL and check cacheability against the test catalog.
    fn check_cacheable(sql: &str) -> Result<CacheableQuery, CacheabilityError> {
        let functions = test_function_catalog();
        let tree = query_tree_parse(sql).expect("convert");
        CacheableQuery::try_new(tree, &functions)
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
            "Volatile function: random"
        );
        assert_eq!(
            display("SELECT id FROM a WHERE created_at > now()"),
            "Non-immutable function: now"
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

    /// Any predicate WHERE accepts is accepted in a join's ON, in inner,
    /// LEFT and RIGHT joins alike (PGC-534); FULL stays out.
    #[test]
    fn test_join_condition_predicates_cacheable() {
        let cacheable = [
            "SELECT * FROM a JOIN b ON a.id > b.id WHERE a.id = 1",
            "SELECT * FROM a JOIN b ON a.id > b.id JOIN c ON b.id = c.id WHERE a.id = 1",
            "SELECT * FROM a JOIN b ON a.id = b.a_id AND b.amount > 5",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.amount BETWEEN 1 AND 5",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.name LIKE 'x%'",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.status IN ('a', 'b')",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND a.status <> 'closed'",
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id AND a.amount >= b.amount WHERE b.id = 1",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND (b.amount > 5 OR b.flag)",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id OR b.deleted_at IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND NOT b.deleted_at IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.active IS TRUE",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND lower(b.name) IS NULL",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.deleted_at + 1 IS NULL",
            "SELECT * FROM a LEFT JOIN b ON lower(a.name) = lower(b.name)",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.kind = ANY(ARRAY['x', 'y'])",
        ];
        for sql in cacheable {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
        let full = "SELECT * FROM a FULL JOIN b ON a.id = b.a_id AND b.amount > 5";
        assert!(
            matches!(
                check_cacheable(full),
                Err(CacheabilityError::UnsupportedFrom { .. })
            ),
            "{full}"
        );
    }

    /// A null test on a column beside the join's equalities is cacheable in
    /// an inner, LEFT or RIGHT join; FULL stays out.
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
        let full = "SELECT * FROM a FULL JOIN b ON a.id = b.a_id AND b.deleted_at IS NULL";
        assert!(
            matches!(
                check_cacheable(full),
                Err(CacheabilityError::UnsupportedFrom { .. })
            ),
            "{full}"
        );
    }

    /// A join's ON follows the WHERE rules for functions and session terms:
    /// a non-immutable function there is rejected, which the equality-only
    /// gate let through. Subqueries in ON are rejected until decorrelation
    /// reads them.
    #[test]
    fn test_join_condition_rejections() {
        let non_immutable = [
            "SELECT * FROM a JOIN b ON random() = a.id",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.created_at > now()",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id AND b.seq = nextval('s')",
        ];
        for sql in non_immutable {
            assert!(
                matches!(
                    check_cacheable(sql),
                    Err(CacheabilityError::NonImmutableFunction { .. }
                        | CacheabilityError::VolatileFunction { .. })
                ),
                "{sql}"
            );
        }
        let subqueries = [
            "SELECT * FROM a JOIN b ON a.id = b.a_id AND b.x IN (SELECT x FROM c)",
            "SELECT * FROM a JOIN b ON a.id = b.a_id AND EXISTS (SELECT 1 FROM c WHERE c.b_id = b.id)",
            "SELECT * FROM a JOIN b ON a.id = (SELECT max(id) FROM c)",
        ];
        for sql in subqueries {
            assert!(
                matches!(
                    check_cacheable(sql),
                    Err(CacheabilityError::UnsupportedFrom {
                        construct: "subquery in join condition"
                    })
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
    fn test_case_with_nested_stable_function_cacheable_in_select_list() {
        let sql = "SELECT CASE WHEN date_trunc('day', now()) = '2024-01-01' THEN 'yes' ELSE 'no' END FROM orders WHERE tenant_id = 1";
        let result = check_cacheable(sql);
        assert!(result.is_ok(), "{result:?}");
    }

    #[test]
    fn test_pg_catalog_qualified_function_cacheable() {
        assert!(check_cacheable("SELECT id FROM users WHERE pg_catalog.lower(name) = 'x'").is_ok());
        assert!(check_cacheable("SELECT pg_catalog.lower(name) FROM users").is_ok());
    }

    /// A user function in WHERE passes here on its best overload, for the
    /// writer to admit or refuse with the search_path and the cache PG in
    /// hand (PGC-578); one with no immutable overload is refused outright. A
    /// user overload beside a builtin does not drag the builtin down.
    #[test]
    fn test_user_functions_pass_on_their_best_case() {
        assert!(check_cacheable("SELECT id FROM users WHERE tenant_of(id) = 1").is_ok());
        let err = check_cacheable("SELECT id FROM users WHERE tenant_clock(id) = 1")
            .expect_err("stable user function in WHERE");
        assert!(
            matches!(err, CacheabilityError::NonImmutableFunction { .. }),
            "{err}"
        );
        assert!(check_cacheable("SELECT id FROM users WHERE lower(name) = 'x'").is_ok());
        assert!(check_cacheable("SELECT tenant_of(id) FROM users").is_ok());
    }

    /// `COALESCE`, `NULLIF`, `GREATEST` and `LEAST` are the parser's own
    /// forms, immutable wherever they stand.
    #[test]
    fn test_sql_forms_cacheable_in_where() {
        for sql in [
            "SELECT id FROM users WHERE COALESCE(name, '') = 'x'",
            "SELECT id FROM users WHERE NULLIF(name, '') = 'x'",
            "SELECT id FROM users WHERE GREATEST(id, 3) = 3 AND LEAST(id, 3) = 1",
        ] {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
    }

    /// A call qualified with another schema is judged by that schema's
    /// overloads, not the builtin's: unknown there it is refused in WHERE
    /// like any unknown function, immutable there it passes to the writer.
    #[test]
    fn test_other_schema_qualified_function_judged_by_its_schema() {
        let err = check_cacheable("SELECT id FROM users WHERE myschema.lower(name) = 'x'")
            .expect_err("unknown in that schema");
        assert!(
            matches!(&err, CacheabilityError::NonImmutableFunction { function } if function == "lower"),
            "{err}"
        );
        let err = check_cacheable("SELECT id FROM users WHERE app.lower(name) = 'x'")
            .expect_err("stable in app");
        assert!(
            matches!(&err, CacheabilityError::NonImmutableFunction { .. }),
            "{err}"
        );
        let err = check_cacheable("SELECT myschema.lower(name) FROM users")
            .expect_err("unknown in that schema, in the SELECT list too");
        assert!(
            matches!(&err, CacheabilityError::NonImmutableFunction { function } if function == "lower"),
            "{err}"
        );
        assert!(check_cacheable("SELECT id FROM users WHERE app.tenant_of(id) = 1").is_ok());
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
            matches!(result, Err(CacheabilityError::VolatileFunction { .. })),
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

    /// The json operators whose every overload is immutable pass anywhere
    /// (PGC-577); `@@` and `||`, which have STABLE overloads, follow the
    /// stable-function rule.
    #[test]
    fn test_json_operators_follow_the_function_volatility_rule() {
        for sql in [
            "SELECT id FROM docs WHERE data ->> 'kind' = 'invoice'",
            "SELECT id FROM docs WHERE data @> '{\"a\": 1}' AND data ? 'k'",
            "SELECT id FROM docs WHERE data #>> '{a,b}' IS NULL OR data @? '$.a'",
            "SELECT id FROM docs JOIN other ON other.data <@ docs.data",
            "SELECT data -> 'a', data #- '{a}', data || '{}', data @@ '$.a == 1' FROM docs",
            "SELECT count(*) FILTER (WHERE data ?| ARRAY['a']) FROM docs",
        ] {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
        for (sql, operator) in [
            ("SELECT id FROM docs WHERE data || '{}' = '{}'", "||"),
            ("SELECT id FROM docs WHERE data @@ '$.a == 1'", "@@"),
            ("SELECT id FROM docs ORDER BY data || '{}'", "||"),
            (
                "SELECT id FROM docs JOIN other ON other.data @@ docs.data",
                "@@",
            ),
            (
                "SELECT count(*) FILTER (WHERE data @@ '$.a') FROM docs",
                "@@",
            ),
        ] {
            let error = check_cacheable(sql).expect_err(sql);
            assert!(
                matches!(&error, CacheabilityError::NonImmutableOperator { operator: found } if found == operator),
                "{sql}: {error}"
            );
        }
        assert_eq!(
            check_cacheable("SELECT id FROM docs WHERE data @@ '$.a'")
                .expect_err("stable operator")
                .to_string(),
            "Non-immutable operator: @@"
        );
    }

    /// A stable builtin on the allowlist is cacheable in the SELECT list,
    /// where the serve recomputes it per hit; anywhere else it would decide
    /// membership or order (PGC-589).
    #[test]
    fn test_allowlisted_stable_function_cacheable_in_select_list_only() {
        for sql in [
            "SELECT now() FROM orders WHERE tenant_id = 1",
            "SELECT to_char(created_at, 'YYYY'), pg_catalog.now() FROM orders WHERE tenant_id = 1",
            "SELECT count(*), to_char(now(), 'HH24') FROM orders GROUP BY tenant_id",
            "SELECT now() FROM orders UNION ALL SELECT now() FROM archived",
        ] {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
        // A nested query's SELECT list feeds the enclosing query: a stable
        // value there decides the outer membership at population time.
        for sql in [
            "SELECT id FROM orders ORDER BY now()",
            "SELECT id FROM orders WHERE created_at > now()",
            "SELECT count(*) FROM orders GROUP BY tenant_id HAVING max(created_at) > now()",
            "SELECT row_number() OVER (ORDER BY to_char(created_at, 'YYYY')) FROM orders",
            "SELECT string_agg(name, ',' ORDER BY to_char(created_at, 'YYYY')) FROM orders",
            "SELECT id FROM orders WHERE created_at < (SELECT now() FROM b LIMIT 1)",
            "SELECT id FROM orders WHERE created_at IN (SELECT now() FROM b)",
            "SELECT s.at FROM (SELECT now() AS at, id FROM orders) s",
            "WITH c AS (SELECT now() AS at, id FROM orders) SELECT at FROM c",
            "SELECT (SELECT now() FROM b LIMIT 1) FROM orders",
        ] {
            let err = check_cacheable(sql).expect_err(sql);
            assert!(
                matches!(&err, CacheabilityError::NonImmutableFunction { .. }),
                "{sql}: {err}"
            );
        }
    }

    /// A stable call outside the allowlist forwards even in the SELECT
    /// list: a builtin reading connection state answers for the cache
    /// connection, and a user function reads tables the cache holds in
    /// part. A user overload of an allowlisted name is for the writer, with
    /// the search_path in hand.
    #[test]
    fn test_stable_function_outside_the_allowlist_not_cacheable() {
        for sql in [
            "SELECT pg_backend_pid() FROM orders WHERE id = 1",
            "SELECT app.tenant_clock(id) FROM orders WHERE id = 1",
            "SELECT tenant_clock(id) FROM orders WHERE id = 1",
        ] {
            let err = check_cacheable(sql).expect_err(sql);
            assert!(
                matches!(&err, CacheabilityError::NonImmutableFunction { .. }),
                "{sql}: {err}"
            );
        }
        let catalog = FunctionCatalog::from_rows(vec![
            FunctionRow::test_builtin("to_char", FunctionVolatility::Stable),
            FunctionRow::test_user("app", "to_char", FunctionVolatility::Stable),
        ]);
        let tree = query_tree_parse("SELECT to_char(n, '999') FROM a").expect("convert");
        assert!(
            CacheableQuery::cacheable(&tree, &catalog).is_ok(),
            "a builtin answers: the writer decides on the path"
        );
        let on_app = PathFunctions {
            catalog: &catalog,
            search_path: &["app", "public"],
        };
        let err = CacheableQuery::cacheable(&tree, &on_app).expect_err("user overload on the path");
        assert!(
            matches!(&err, CacheabilityError::NonImmutableFunction { .. }),
            "{err}"
        );
        let on_public = PathFunctions {
            catalog: &catalog,
            search_path: &["public"],
        };
        assert!(CacheableQuery::cacheable(&tree, &on_public).is_ok());
    }

    /// The scan the shape gate reads: a stable call or operator anywhere in
    /// a cacheable query means the serve must recompute the result per hit.
    #[test]
    fn test_query_tree_has_stable_call() {
        let functions = test_function_catalog();
        let has = |sql: &str| {
            query_tree_has_stable_call(&query_tree_parse(sql).expect("convert"), &functions)
        };
        assert!(has("SELECT now(), id FROM a"));
        assert!(has(
            "SELECT count(*), to_char(max(created_at), 'YYYY') FROM a"
        ));
        assert!(has("SELECT data || '{}' FROM docs"));
        assert!(!has("SELECT lower(name), count(*) FROM a GROUP BY name"));
        assert!(!has("SELECT data -> 'a' FROM docs"));
    }

    /// A session term stands inside a fragment the proxy folds to a literal
    /// under the session's values (PGC-580), so it is cacheable wherever a
    /// literal is: a predicate, the SELECT list, an argument, the
    /// documented FROM-less scalar subquery wrapper.
    #[test]
    fn test_session_term_fragments_are_cacheable() {
        for sql in [
            "SELECT id FROM a WHERE owner = current_user",
            "SELECT id FROM a WHERE tenant = current_setting('app.tenant', true)::int",
            "SELECT current_user, id FROM a WHERE id = 1",
            "SELECT id FROM a WHERE name = lower(current_setting('app.name'))",
            "SELECT id FROM a WHERE owner = (SELECT current_setting('app.who'))",
            "SELECT id FROM a WHERE owner = \
             (current_setting('request.jwt.claims', true)::jsonb ->> 'sub')",
        ] {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
        // A built-in setting changes under `SET` with no custom shadow to
        // fold from, so it stays a bare term and the query forwards.
        assert!(matches!(
            check_cacheable("SELECT id FROM a WHERE tz = current_setting('timezone')"),
            Err(CacheabilityError::SessionTerm { .. })
        ));
    }

    /// A volatile call is refused wherever it stands: it may answer
    /// differently per call or have side effects, which a replay on the
    /// cache database never reproduces (PGC-589). Every clause is judged,
    /// subqueries and set-operation branches included.
    #[test]
    fn test_volatile_function_refused_in_every_clause() {
        for sql in [
            "SELECT random(), id FROM a",
            "SELECT id FROM a ORDER BY random()",
            "SELECT count(*) FROM a GROUP BY id HAVING count(*) > random()",
            "SELECT row_number() OVER (ORDER BY random()) FROM a",
            "SELECT row_number() OVER (PARTITION BY random()) FROM a",
            "SELECT sum(id) OVER (ORDER BY id ROWS BETWEEN random()::int PRECEDING AND CURRENT ROW) FROM a",
            "SELECT id FROM (SELECT id FROM a ORDER BY random()) s",
            "SELECT id FROM a UNION SELECT id FROM b ORDER BY random()",
            "SELECT (SELECT id FROM b ORDER BY random() LIMIT 1) FROM a",
            "SELECT id FROM a WHERE EXISTS (SELECT 1 FROM b ORDER BY random())",
            "SELECT count(*) FILTER (WHERE random() > 0.5) FROM a",
            "SELECT string_agg(name, ',' ORDER BY random()) FROM a",
            "SELECT lower(random()::text) FROM a",
        ] {
            let err = check_cacheable(sql).expect_err(sql);
            assert!(
                matches!(&err, CacheabilityError::VolatileFunction { function } if function == "random"),
                "{sql}: {err}"
            );
        }
        for sql in [
            "SELECT count(*), sum(amount), max(id) FROM orders WHERE tenant_id = 1",
            "SELECT row_number() OVER (PARTITION BY tenant_id ORDER BY id) FROM orders",
            "SELECT id FROM orders ORDER BY lower(name)",
            "SELECT count(*) FROM orders GROUP BY tenant_id HAVING count(*) > 1",
        ] {
            assert!(check_cacheable(sql).is_ok(), "{sql}");
        }
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
