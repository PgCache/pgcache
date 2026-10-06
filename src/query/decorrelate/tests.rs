#![allow(clippy::wildcard_enum_match_arm)]

use iddqd::BiHashMap;
use postgres_types::Type;

use super::*;
use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::{Deparse, JoinType, UnaryOp, query_expr_parse};
use crate::query::resolved::{
    ResolvedJoinNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedTableSource, ResolvedTableSubqueryNode, ResolvedWhereExpr,
    query_expr_resolve,
};

/// Create test table metadata with given column names.
/// First column is the primary key (INT4), rest are TEXT.
fn test_table(name: &str, relation_oid: Oid, column_names: &[&str]) -> TableMetadata {
    let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, col_name)| {
        let is_pk = i == 0;
        ColumnMetadata {
            name: (*col_name).into(),
            position: i16::try_from(i + 1).expect("column position fits in i16"),
            type_oid: TypeOid::from_raw(if is_pk { 23 } else { 25 }),
            data_type: if is_pk { Type::INT4 } else { Type::TEXT },
            type_name: if is_pk { "int4" } else { "text" }.into(),
            cache_type_name: if is_pk { "int4" } else { "text" }.into(),
            is_primary_key: is_pk,
        }
    }));
    TableMetadata {
        replica_identity_full: false,
        relation_oid,
        name: name.into(),
        schema: "public".into(),
        primary_key_columns: vec![column_names[0].into()],
        columns,
        indexes: Vec::new(),
    }
}

fn test_tables() -> BiHashMap<TableMetadata> {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table(
        "employees",
        Oid::from_raw(1),
        &["id", "name", "dept_id", "status", "manager_id"],
    ));
    tables.insert_overwrite(test_table(
        "orders",
        Oid::from_raw(2),
        &["id", "emp_id", "status", "total", "customer_id"],
    ));
    tables.insert_overwrite(test_table(
        "departments",
        Oid::from_raw(3),
        &["id", "name", "location", "budget"],
    ));
    tables.insert_overwrite(test_table(
        "customers",
        Oid::from_raw(4),
        &["id", "name", "region", "emp_id"],
    ));
    tables.insert_overwrite(test_table(
        "users",
        Oid::from_raw(5),
        &["id", "name", "email", "status"],
    ));
    tables.insert_overwrite(test_table(
        "active_users",
        Oid::from_raw(6),
        &["id", "user_id"],
    ));
    tables.insert_overwrite(test_table(
        "projects",
        Oid::from_raw(7),
        &["id", "name", "dept_id", "status"],
    ));
    tables
}

fn test_aggregate_functions() -> HashSet<EcoString> {
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
    .map(EcoString::from)
    .collect()
}

/// Parse SQL → resolve → decorrelate → return outcome.
fn resolve_and_decorrelate(
    sql: &str,
    tables: &BiHashMap<TableMetadata>,
) -> DecorrelateResult<DecorrelateOutcome> {
    let ast = query_expr_parse(sql).expect("convert to AST");
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

/// Every FROM source in pre-order: each top-level source, and a join's
/// sides after the join itself. Derived-table bodies are not entered.
fn from_sources(select: &ResolvedSelectNode) -> Vec<&ResolvedTableSource> {
    fn collect<'a>(source: &'a ResolvedTableSource, out: &mut Vec<&'a ResolvedTableSource>) {
        out.push(source);
        if let ResolvedTableSource::Join(join) = source {
            collect(&join.left, out);
            collect(&join.right, out);
        }
    }
    let mut sources = Vec::new();
    for source in &select.from {
        collect(source, &mut sources);
    }
    sources
}

/// Flatten all JoinNodes from the FROM clause.
fn select_joins(select: &ResolvedSelectNode) -> Vec<&ResolvedJoinNode> {
    from_sources(select)
        .into_iter()
        .filter_map(|source| match source {
            ResolvedTableSource::Join(join) => Some(join.as_ref()),
            _ => None,
        })
        .collect()
}

/// All table names in the FROM clause (through joins, not into subqueries).
fn from_table_names(select: &ResolvedSelectNode) -> Vec<&str> {
    from_sources(select)
        .into_iter()
        .filter_map(|source| match source {
            ResolvedTableSource::Table(t) => Some(t.name.as_str()),
            _ => None,
        })
        .collect()
}

/// The derived tables (FROM subqueries) of the FROM clause.
fn derived_tables(select: &ResolvedSelectNode) -> Vec<&ResolvedTableSubqueryNode> {
    from_sources(select)
        .into_iter()
        .filter_map(|source| match source {
            ResolvedTableSource::Subquery(sub) => Some(sub),
            _ => None,
        })
        .collect()
}

/// Aliases of the derived tables in the FROM clause.
fn from_derived_aliases(select: &ResolvedSelectNode) -> Vec<&str> {
    derived_tables(select)
        .into_iter()
        .map(|sub| sub.alias.name.as_str())
        .collect()
}

/// Scalar column aliases from derived table SELECT lists (e.g. `_ds1` in
/// `(SELECT count(*) AS _ds1 ...) AS _dc1`).
fn derived_scalar_aliases(select: &ResolvedSelectNode) -> Vec<&str> {
    derived_tables(select)
        .into_iter()
        .filter_map(|sub| match &sub.query.body {
            ResolvedQueryBody::Select(inner) => match &inner.columns {
                ResolvedSelectColumns::Columns(cols) => Some(cols),
                ResolvedSelectColumns::None => None,
            },
            _ => None,
        })
        .flatten()
        .filter_map(|col| col.alias.as_deref())
        .collect()
}

/// Whether any derived table has a non-empty GROUP BY.
fn derived_has_group_by(select: &ResolvedSelectNode) -> bool {
    derived_tables(select).into_iter().any(|sub| {
        matches!(&sub.query.body, ResolvedQueryBody::Select(inner) if !inner.group_by.is_empty())
    })
}

/// Whether `matches` holds anywhere in a WHERE expression, looking through
/// NOT/IS, AND/OR and multi-operand expressions but not into subquery bodies.
fn where_any(expr: &Option<ResolvedWhereExpr>, matches: fn(&ResolvedWhereExpr) -> bool) -> bool {
    fn walk(expr: &ResolvedWhereExpr, matches: fn(&ResolvedWhereExpr) -> bool) -> bool {
        matches(expr)
            || match expr {
                ResolvedWhereExpr::Unary(u) => walk(&u.expr, matches),
                ResolvedWhereExpr::Binary(b) => walk(&b.lexpr, matches) || walk(&b.rexpr, matches),
                ResolvedWhereExpr::Multi(m) => m.exprs.iter().any(|e| walk(e, matches)),
                _ => false,
            }
    }
    expr.as_ref().is_some_and(|e| walk(e, matches))
}

fn where_has_subquery(expr: &Option<ResolvedWhereExpr>) -> bool {
    where_any(expr, |e| matches!(e, ResolvedWhereExpr::Subquery { .. }))
}

fn where_has_is_null(expr: &Option<ResolvedWhereExpr>) -> bool {
    where_any(
        expr,
        |e| matches!(e, ResolvedWhereExpr::Unary(u) if u.op == UnaryOp::IsNull),
    )
}

/// Decorrelate `sql` over the test catalog; the result as returned.
fn decorrelate(sql: &str) -> DecorrelateResult<DecorrelateOutcome> {
    resolve_and_decorrelate(sql, &test_tables())
}

/// Decorrelate `sql`, which must succeed with a rewrite, and return the
/// rewritten SELECT.
fn decorrelated(sql: &str) -> ResolvedSelectNode {
    let outcome = decorrelate(sql).unwrap_or_else(|e| panic!("{sql}: {e:?}"));
    assert!(outcome.transformed, "expected a rewrite of {sql}");
    as_select(&outcome.resolved).clone()
}

/// Deparse a single WHERE expression for targeted predicate checks.
fn deparse_expr(expr: &ResolvedWhereExpr) -> String {
    let mut buf = String::new();
    expr.deparse(&mut buf);
    buf
}

// ==================== No-op Cases ====================

// ==================== EXISTS Decorrelation ====================

#[test]
fn test_exists_single_correlation() {
    let select = decorrelated(
        "SELECT e.id, e.name FROM employees e \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "EXISTS should be removed"
    );
    let joins = select_joins(&select);
    assert_eq!(joins.len(), 1);
    assert_eq!(joins[0].join_type, JoinType::Inner);
    assert!(select.distinct);

    let on = deparse_expr(joins[0].predicate().unwrap());
    assert!(on.contains("o.emp_id = e.id"), "correlation in ON: {on}");
}

#[test]
fn test_exists_multiple_correlation_predicates() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = e.status)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "EXISTS should be removed"
    );
    let joins = select_joins(&select);
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
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = 'active')",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "EXISTS should be removed"
    );
    assert!(!select_joins(&select).is_empty(), "should have JOIN");
    let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
    assert!(
        where_str.contains("o.status = 'active'"),
        "residual in WHERE: {where_str}"
    );
}

#[test]
fn test_exists_outer_query_already_has_joins() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         JOIN departments d ON d.id = e.dept_id \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "EXISTS should be removed"
    );
    let tables = from_table_names(&select);
    assert!(tables.contains(&"departments"), "original join table");
    assert!(tables.contains(&"orders"), "decorrelated join table");
}

#[test]
fn test_exists_inner_query_has_joins() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE EXISTS ( \
             SELECT 1 FROM orders o \
             JOIN customers c ON c.id = o.customer_id \
             WHERE o.emp_id = e.id \
         )",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "EXISTS should be removed"
    );
    let tables = from_table_names(&select);
    assert!(tables.contains(&"customers"), "inner join table preserved");
    assert!(tables.contains(&"orders"), "inner table preserved");
}

#[test]
fn test_multiple_exists_in_where() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
         AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "EXISTS should be removed"
    );
    let tables = from_table_names(&select);
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
    let select = decorrelated(
        "SELECT d.id, d.name FROM departments d \
         WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "NOT EXISTS should be removed"
    );
    let joins = select_joins(&select);
    assert_eq!(joins.len(), 1);
    assert_eq!(joins[0].join_type, JoinType::Left);
    assert!(where_has_is_null(&select.where_clause));
}

#[test]
fn test_not_exists_residual_in_on_clause() {
    let select = decorrelated(
        "SELECT d.id FROM departments d \
         WHERE NOT EXISTS ( \
             SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.status = 'active' \
         )",
    );

    let joins = select_joins(&select);
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
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status)",
    );
    assert!(
        !where_has_subquery(&select.where_clause),
        "IN should be removed"
    );
    assert!(!select_joins(&select).is_empty(), "should have JOIN");
    assert!(select.distinct);
}

#[test]
fn test_correlated_scalar_subquery_decorrelated() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
         FROM employees e",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
        "derived table alias _dc1"
    );
    assert!(
        derived_scalar_aliases(&select).contains(&"_ds1"),
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

// ==================== NOT EXISTS: Inner Clause Handling ====================

// ==================== Mixed Cases ====================

#[test]
fn test_mixed_correlated_and_non_correlated() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) \
         AND EXISTS (SELECT 1 FROM departments d WHERE d.name = 'Engineering')",
    );

    // Correlated EXISTS decorrelated into JOIN
    assert!(
        from_table_names(&select).contains(&"orders"),
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
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.status = 'active' \
         AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
    );

    let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
    assert!(
        where_str.contains("e.status = 'active'"),
        "outer predicate preserved: {where_str}"
    );
    assert!(
        !select_joins(&select).is_empty(),
        "correlation should be JOIN"
    );
}

// ==================== Scalar SELECT List Decorrelation ====================

#[test]
fn test_scalar_select_count_single_correlation() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
         FROM employees e",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
        "derived table alias"
    );
    assert!(
        derived_scalar_aliases(&select).contains(&"_ds1"),
        "scalar column alias"
    );
    assert!(derived_has_group_by(&select), "aggregate needs GROUP BY");
}

#[test]
fn test_scalar_select_avg_with_residual() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT avg(o.total) FROM orders o WHERE o.emp_id = e.id AND o.status = 'done') AS avg_total \
         FROM employees e",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(derived_has_group_by(&select), "aggregate needs GROUP BY");
    assert!(
        !from_derived_aliases(&select).is_empty(),
        "should have derived table"
    );
}

#[test]
fn test_scalar_select_multiple_correlations() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT count(*) FROM orders o WHERE o.emp_id = e.id AND o.status = e.status) AS cnt \
         FROM employees e",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(derived_has_group_by(&select), "aggregate needs GROUP BY");
}

#[test]
fn test_scalar_select_multiple_subqueries() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count, \
             (SELECT count(*) FROM projects p WHERE p.dept_id = e.dept_id) AS project_count \
         FROM employees e",
    );
    let derived = from_derived_aliases(&select);
    assert!(derived.contains(&"_dc1"), "first derived table");
    assert!(derived.contains(&"_dc2"), "second derived table");
    let scalar = derived_scalar_aliases(&select);
    assert!(scalar.contains(&"_ds1"), "first scalar column");
    assert!(scalar.contains(&"_ds2"), "second scalar column");
}

#[test]
fn test_scalar_select_non_aggregate_no_group_by() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept_name \
         FROM employees e",
    );
    let joins = select_joins(&select);
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
fn test_scalar_select_mixed_with_exists() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count \
         FROM employees e \
         WHERE EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
    );
    // Scalar subquery decorrelated as LEFT JOIN with derived table
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
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
    let select = decorrelated(
        "SELECT d.id, d.name FROM departments d \
         WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location)",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
        "derived table alias"
    );
    assert!(
        derived_scalar_aliases(&select).contains(&"_ds1"),
        "scalar column alias"
    );
}

#[test]
fn test_scalar_where_equality() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.id = (SELECT max(o.emp_id) FROM orders o WHERE o.status = e.status)",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
        "derived table alias"
    );
}

#[test]
fn test_scalar_where_multiple_subqueries() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.id > (SELECT min(o.emp_id) FROM orders o WHERE o.status = e.status) \
         AND e.id < (SELECT max(o.emp_id) FROM orders o WHERE o.status = e.status)",
    );
    let derived = from_derived_aliases(&select);
    assert!(derived.contains(&"_dc1"), "first derived table");
    assert!(derived.contains(&"_dc2"), "second derived table");
}

#[test]
fn test_scalar_where_non_aggregate() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id)",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(
        select.group_by.is_empty(),
        "non-aggregate should not have GROUP BY"
    );
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
        "derived table alias"
    );
}

// ==================== Self-Join / Alias Handling ====================

#[test]
fn test_scalar_select_self_join() {
    let select = decorrelated(
        "SELECT e.id, \
             (SELECT count(*) FROM employees e2 WHERE e2.manager_id = e.id) AS report_count \
         FROM employees e",
    );
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
    assert!(
        from_derived_aliases(&select).contains(&"_dc1"),
        "derived table alias"
    );
    assert!(derived_has_group_by(&select), "aggregate needs GROUP BY");
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
    let select = decorrelated(
        "SELECT e.id, e.name FROM employees e \
         WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "IN should be removed"
    );
    let joins = select_joins(&select);
    assert_eq!(joins.len(), 1);
    assert_eq!(joins[0].join_type, JoinType::Inner);
    assert!(select.distinct);
    let on = deparse_expr(joins[0].predicate().unwrap());
    assert!(on.contains("e.dept_id = d.id"), "IN predicate in ON: {on}");
    assert!(on.contains("d.name = e.name"), "correlation in ON: {on}");
}

#[test]
fn test_in_with_residual_predicates() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.dept_id IN ( \
             SELECT d.id FROM departments d \
             WHERE d.name = e.name AND d.location = 'NYC' \
         )",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "IN should be removed"
    );
    assert!(!select_joins(&select).is_empty(), "should have JOIN");
    let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
    assert!(
        where_str.contains("d.location = 'NYC'"),
        "residual in WHERE: {where_str}"
    );
}

#[test]
fn test_in_with_outer_where_predicates() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.status = 'active' \
         AND e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
    );

    let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
    assert!(
        where_str.contains("e.status = 'active'"),
        "outer predicate preserved: {where_str}"
    );
    assert!(!select_joins(&select).is_empty(), "should have JOIN");
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
fn test_in_outer_query_has_joins() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         JOIN orders o ON o.emp_id = e.id \
         WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "IN should be removed"
    );
    let tables = from_table_names(&select);
    assert!(tables.contains(&"orders"), "original join preserved");
    assert!(tables.contains(&"departments"), "IN table joined");
}

#[test]
fn test_in_mixed_with_exists() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name) \
         AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "subqueries should be removed"
    );
    let tables = from_table_names(&select);
    assert!(tables.contains(&"departments"), "IN table");
    assert!(tables.contains(&"orders"), "EXISTS table");
}

// ==================== NOT IN Decorrelation ====================

#[test]
fn test_not_in_single_correlation() {
    let select = decorrelated(
        "SELECT e.id, e.name FROM employees e \
         WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
    );

    let joins = select_joins(&select);
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
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.dept_id NOT IN ( \
             SELECT d.id FROM departments d \
             WHERE d.name = e.name AND d.location = 'NYC' \
         )",
    );

    let joins = select_joins(&select);
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
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.status = 'active' \
         AND e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
    );

    let where_str = deparse_expr(select.where_clause.as_ref().unwrap());
    assert!(
        where_str.contains("e.status = 'active'"),
        "outer predicate preserved: {where_str}"
    );
    let joins = select_joins(&select);
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
fn test_not_in_outer_query_has_joins() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         JOIN orders o ON o.emp_id = e.id \
         WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
    );

    let tables = from_table_names(&select);
    assert!(tables.contains(&"orders"), "original join preserved");
    assert!(tables.contains(&"departments"), "NOT IN table joined");
    let joins = select_joins(&select);
    assert!(
        joins.iter().any(|j| j.join_type == JoinType::Left),
        "should have LEFT JOIN"
    );
}

#[test]
fn test_not_in_mixed_with_in_and_exists() {
    let select = decorrelated(
        "SELECT e.id FROM employees e \
         WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name) \
         AND e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status) \
         AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
    );

    assert!(
        !where_has_subquery(&select.where_clause),
        "all subqueries should be removed"
    );
    let tables = from_table_names(&select);
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

#[test]
fn test_rejected_cases() {
    // An anti-join can't drop an inner GROUP BY/HAVING (it would
    // under-invalidate), and a non-aggregate scalar with LIMIT has no join form.
    #[rustfmt::skip]
    let cases = [
        ("NOT EXISTS with GROUP BY",
         "SELECT d.id FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id GROUP BY e.status)"),
        ("NOT EXISTS with HAVING",
         "SELECT d.id FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id GROUP BY e.status HAVING count(*) > 5)"),
        ("NOT IN with GROUP BY",
         "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name GROUP BY d.id)"),
        ("NOT IN with HAVING",
         "SELECT d.id FROM departments d WHERE d.id NOT IN (SELECT e.dept_id FROM employees e WHERE e.name = d.name GROUP BY e.dept_id HAVING count(*) > 5)"),
        ("non-aggregate scalar with LIMIT",
         "SELECT e.id, (SELECT d.name FROM departments d WHERE d.id = e.dept_id LIMIT 1) AS dept_name FROM employees e"),
    ];
    for (label, sql) in cases {
        assert!(
            decorrelate(sql).is_err(),
            "{label} should be rejected: {sql}"
        );
    }
}

#[test]
fn test_unchanged_cases() {
    // (label, SQL, whether a subquery remains in WHERE)
    #[rustfmt::skip]
    let cases = [
        ("no subqueries",           "SELECT id, name FROM employees WHERE status = 'active'", false),
        ("non-correlated EXISTS",   "SELECT id FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE status = 'active')", true),
        ("non-correlated IN",       "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.location = 'NYC')", true),
    ];
    for (label, sql, subquery_remains) in cases {
        let outcome = decorrelate(sql).unwrap_or_else(|e| panic!("{label}: {e:?}"));
        assert!(
            !outcome.transformed,
            "{label}: should be left unchanged: {sql}"
        );
        let select = as_select(&outcome.resolved);
        assert_eq!(
            where_has_subquery(&select.where_clause),
            subquery_remains,
            "{label}: subquery in WHERE"
        );
    }
}

/// An inner clause a semi-/anti-join drops: harmless for EXISTS/IN (it can
/// only over-invalidate), and LIMIT is irrelevant to a boolean check.
#[derive(Clone, Copy, Debug)]
enum Stripped {
    GroupBy,
    Having,
    Limit,
}

#[test]
fn test_inner_clause_stripped_cases() {
    use Stripped::{GroupBy, Having, Limit};
    // (label, SQL, stripped clause, anti-join so a LEFT JOIN is expected)
    #[rustfmt::skip]
    let cases = [
        ("EXISTS GROUP BY",   "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id GROUP BY o.status)",                  GroupBy, false),
        ("EXISTS HAVING",     "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id GROUP BY o.status HAVING count(*) > 5)", Having,  false),
        ("EXISTS LIMIT",      "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id LIMIT 1)",                            Limit,   false),
        ("NOT EXISTS LIMIT",  "SELECT d.id, d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id LIMIT 1)",          Limit,   true),
        ("IN GROUP BY",       "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name GROUP BY d.id)",       GroupBy, false),
        ("IN LIMIT",          "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name LIMIT 10)",            Limit,   false),
        ("NOT IN LIMIT",      "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name LIMIT 10)",        Limit,   true),
    ];
    for (label, sql, stripped, anti_join) in cases {
        let select = decorrelated(sql);
        let joins = select_joins(&select);
        if anti_join {
            assert!(
                joins.iter().any(|j| j.join_type == JoinType::Left),
                "{label}: expected a LEFT JOIN"
            );
        } else {
            assert!(!joins.is_empty(), "{label}: expected a JOIN");
        }
        assert!(
            !where_has_subquery(&select.where_clause),
            "{label}: subquery should be removed"
        );
        match stripped {
            GroupBy => assert!(
                select.group_by.is_empty(),
                "{label}: GROUP BY should be stripped"
            ),
            Having => assert!(
                select.having.is_none(),
                "{label}: HAVING should be stripped"
            ),
            Limit => {}
        }
    }
}
