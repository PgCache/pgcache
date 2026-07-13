#![allow(clippy::wildcard_enum_match_arm)]

use iddqd::BiHashMap;
use tokio_postgres::types::Type;

use crate::oid::Oid;

use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use crate::query::ast::{QueryBody, query_expr_parse};
use crate::query::resolved::{ResolvedSelectNode, select_node_resolve};

use super::extract::analyze_query_constraints;
use super::subsume::table_constraints_subsumed;
use super::*;

// Helper function to parse SQL and resolve to ResolvedSelectNode
fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
    let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
    let QueryBody::Select(node) = query_expr.body else {
        panic!("expected SELECT");
    };
    select_node_resolve(&node, tables, &["public"]).expect("resolve")
}

// Helper function to create test table metadata
fn test_table_metadata(name: &str, relation_oid: Oid) -> TableMetadata {
    let columns = ColumnStore::new([
        ColumnMetadata {
            name: "id".into(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        },
        ColumnMetadata {
            name: "name".into(),
            position: 2,
            type_oid: 25,
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: false,
        },
    ]);

    TableMetadata {
        replica_identity_full: false,
        relation_oid,
        name: name.into(),
        schema: "public".into(),
        primary_key_columns: vec!["id".into()],
        columns,
        indexes: Vec::new(),
    }
}

/// Helper to check if table_constraints contains a specific (column, op, value) triple
fn has_constraint(
    constraints: &QueryConstraints,
    table: &str,
    column: &str,
    op: BinaryOp,
    value: LiteralValue,
) -> bool {
    constraints.table_constraints.get(table).is_some_and(|cs| {
        cs.iter().any(|tc| match tc {
            TableConstraint::Comparison(c, o, v) => c == column && *o == op && *v == value,
            TableConstraint::AnyOf(..) | TableConstraint::CastComparison(..) => false,
        })
    })
}

fn has_in_constraint(
    constraints: &QueryConstraints,
    table: &str,
    column: &str,
    values: &[LiteralValue],
) -> bool {
    constraints.table_constraints.get(table).is_some_and(|cs| {
        cs.iter().any(|tc| match tc {
            TableConstraint::AnyOf(c, vs) => {
                c == column && values.iter().all(|v| vs.contains(v)) && vs.len() == values.len()
            }
            TableConstraint::Comparison(..) | TableConstraint::CastComparison(..) => false,
        })
    })
}

// ========== Existing equality tests (updated for new tuple format) ==========

#[test]
fn test_simple_constraint() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 1);
    assert_eq!(constraints.table_constraints.get("users").unwrap().len(), 1);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
}

#[test]
fn test_join_propagation() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("test", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("test_map", Oid::from_raw(1002)));

    let sql = "SELECT * FROM test t JOIN test_map tm ON tm.id = t.id WHERE t.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Should propagate: t.id = 1 -> tm.id = 1
    assert_eq!(constraints.column_constraints.len(), 2);

    let test_constraints = constraints.table_constraints.get("test").unwrap();
    assert_eq!(test_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "test",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));

    let test_map_constraints = constraints.table_constraints.get("test_map").unwrap();
    assert_eq!(test_map_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "test_map",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));

    assert_eq!(constraints.equivalences.len(), 1);
}

#[test]
fn test_transitive_propagation() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("a", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("b", Oid::from_raw(1002)));

    tables.insert_overwrite(TableMetadata {
        replica_identity_full: false,
        relation_oid: Oid::from_raw(1003),
        name: "c".into(),
        schema: "public".into(),
        primary_key_columns: vec!["id".into()],
        columns: ColumnStore::new([ColumnMetadata {
            name: "id".into(),
            position: 1,
            type_oid: 23,
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        }]),
        indexes: Vec::new(),
    });

    let sql = "SELECT * FROM (a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id WHERE a.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Should propagate through: a.id = 1 -> b.id = 1 -> c.id = 1
    assert_eq!(constraints.column_constraints.len(), 3);

    assert!(has_constraint(
        &constraints,
        "a",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1)
    ));
    assert!(has_constraint(
        &constraints,
        "b",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1)
    ));
    assert!(has_constraint(
        &constraints,
        "c",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1)
    ));
}

#[test]
fn test_multiple_constraints() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id = 1 AND name = 'john'";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);

    let user_constraints = constraints.table_constraints.get("users").unwrap();
    assert_eq!(user_constraints.len(), 2);

    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::Equal,
        LiteralValue::String("john".into()),
    ));
}

#[test]
fn test_equivalence_in_where() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("a", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("b", Oid::from_raw(1002)));

    let sql = "SELECT * FROM a, b WHERE a.id = b.id AND a.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "a",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1)
    ));
    assert!(has_constraint(
        &constraints,
        "b",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1)
    ));
}

#[test]
fn test_no_propagation_with_or() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id = 1 OR id = 2";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
    assert_eq!(constraints.table_constraints.len(), 0);
}

#[test]
fn test_self_join() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("test", Oid::from_raw(1001)));

    let sql = "SELECT * FROM test t1 JOIN test t2 ON t1.id = t2.id WHERE t1.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Both t1.id and t2.id reference the same column (test.id)
    // So we get 1 unique column with constraint
    assert_eq!(constraints.column_constraints.len(), 1);

    let test_constraints = constraints.table_constraints.get("test").unwrap();
    assert_eq!(test_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "test",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
}

#[test]
fn test_subquery_extracts_outer_constraints() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("active_users", Oid::from_raw(1002)));

    let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
}

#[test]
fn test_derived_table_no_outer_constraints() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(
        constraints.column_constraints.is_empty(),
        "Derived table with no outer WHERE should have no constraints"
    );
}

#[test]
fn test_scalar_subquery_extracts_outer_constraints() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let sql = "SELECT id, (SELECT COUNT(*) FROM orders) FROM users WHERE id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
}

#[test]
fn test_subquery_multiple_outer_constraints() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("active_users", Oid::from_raw(1002)));

    let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1 AND name = 'alice'";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::Equal,
        LiteralValue::String("alice".into()),
    ));
}

// ========== Inequality tests ==========

#[test]
fn test_simple_inequality() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id > 5";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThan,
        LiteralValue::Integer(5),
    ));
}

#[test]
fn test_multiple_inequalities_same_column() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id > 5 AND id < 100";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThan,
        LiteralValue::Integer(5),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::LessThan,
        LiteralValue::Integer(100),
    ));
}

#[test]
fn test_reversed_operand() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // 5 < id is equivalent to id > 5
    let sql = "SELECT * FROM users WHERE 5 < id";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThan,
        LiteralValue::Integer(5),
    ));
}

#[test]
fn test_not_equal() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE name != 'deleted'";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::NotEqual,
        LiteralValue::String("deleted".into()),
    ));
}

#[test]
fn test_mixed_equality_and_inequality() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id = 1 AND name != 'deleted'";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::Integer(1),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::NotEqual,
        LiteralValue::String("deleted".into()),
    ));
}

#[test]
fn test_inequality_propagation_through_join() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("a", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("b", Oid::from_raw(1002)));

    let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id > 5";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Should propagate: a.id > 5 -> b.id > 5
    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "a",
        "id",
        BinaryOp::GreaterThan,
        LiteralValue::Integer(5),
    ));
    assert!(has_constraint(
        &constraints,
        "b",
        "id",
        BinaryOp::GreaterThan,
        LiteralValue::Integer(5),
    ));
}

#[test]
fn test_or_prevents_inequality_extraction() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id > 5 OR id < 2";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
}

// ========== BETWEEN tests ==========

#[test]
fn test_between() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id BETWEEN 100 AND 500";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThanOrEqual,
        LiteralValue::Integer(100),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::LessThanOrEqual,
        LiteralValue::Integer(500),
    ));
}

#[test]
fn test_between_with_and() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE name = 'alice' AND id BETWEEN 100 AND 500";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 3);
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::Equal,
        LiteralValue::String("alice".into()),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThanOrEqual,
        LiteralValue::Integer(100),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::LessThanOrEqual,
        LiteralValue::Integer(500),
    ));
}

#[test]
fn test_between_propagation_through_join() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("a", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("b", Oid::from_raw(1002)));

    let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id BETWEEN 1 AND 10";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Both tables should get the two BETWEEN constraints
    assert_eq!(constraints.column_constraints.len(), 4);
    assert!(has_constraint(
        &constraints,
        "a",
        "id",
        BinaryOp::GreaterThanOrEqual,
        LiteralValue::Integer(1)
    ));
    assert!(has_constraint(
        &constraints,
        "a",
        "id",
        BinaryOp::LessThanOrEqual,
        LiteralValue::Integer(10)
    ));
    assert!(has_constraint(
        &constraints,
        "b",
        "id",
        BinaryOp::GreaterThanOrEqual,
        LiteralValue::Integer(1)
    ));
    assert!(has_constraint(
        &constraints,
        "b",
        "id",
        BinaryOp::LessThanOrEqual,
        LiteralValue::Integer(10)
    ));
}

#[test]
fn test_not_between() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // NOT BETWEEN is an OR (id < 100 OR id > 500), so no constraints
    let sql = "SELECT * FROM users WHERE id NOT BETWEEN 100 AND 500";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
}

#[test]
fn test_between_symmetric_reversed_bounds() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // Bounds are reversed (500, 100) — should normalize to (100, 500)
    let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 500 AND 100";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThanOrEqual,
        LiteralValue::Integer(100),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::LessThanOrEqual,
        LiteralValue::Integer(500),
    ));
}

#[test]
fn test_between_symmetric_already_ordered() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // Bounds already in order — same result as reversed
    let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 100 AND 500";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::GreaterThanOrEqual,
        LiteralValue::Integer(100),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::LessThanOrEqual,
        LiteralValue::Integer(500),
    ));
}

#[test]
fn test_between_symmetric_with_parameter() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // Can't compare parameter with literal — skip extraction
    let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC $1 AND 500";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
}

#[test]
fn test_not_between_symmetric() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // NOT BETWEEN SYMMETRIC is still an OR — no constraints
    let sql = "SELECT * FROM users WHERE id NOT BETWEEN SYMMETRIC 500 AND 100";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
}

// ========== Subsumption tests ==========

#[test]
fn test_subsumption_cached_no_constraints() {
    // Cached: SELECT * FROM users (no WHERE) → full scan covers everything
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_same_equality() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_new_narrower() {
    // Cached has fewer equality constraints → new is narrower. Subsumed.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = 1 AND name = 'alice'",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_different_values() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_cached_has_extra_constraint() {
    // Cached is narrower than new → not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = 1 AND name = 'alice'",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_new_no_constraints() {
    // New has no constraints but cached does → new is broader, not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_range_tighter_lower() {
    // id > 10 implies id > 5 — subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 10", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_between_with_non_literal_bounds() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // Non-literal bound (column reference) — skip extraction
    let sql = "SELECT * FROM users WHERE id BETWEEN name AND 10";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
}

// ========== Range subsumption tests ==========

#[test]
fn test_subsumption_range_looser_lower() {
    // id > 1 does NOT imply id > 3 — not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 1", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_range_exclusive_tighter_than_inclusive() {
    // id > 3 (exclusive) is tighter than id >= 3 (inclusive) — subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id >= 3", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_range_same_inclusive_bound() {
    // id >= 3 subsumed by id >= 3
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id >= 3", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id >= 3", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_range_containment() {
    // id BETWEEN 5 AND 8 is contained in id >= 3 AND id <= 10
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id >= 3 AND id <= 10",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id BETWEEN 5 AND 8",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_range_missing_upper() {
    // id > 50 has no upper bound, but cached has id < 100 — not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id > 0 AND id < 100",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 50", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_point_in_range() {
    // id = 5 is within id > 3
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_point_outside_range() {
    // id = 2 is NOT within id > 3
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_equal_not_subsumed_by_range() {
    // Cached = 5 (single point), new wants id > 3 (a range) — not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_not_equal_by_different_equal() {
    // Cached != 5, new = 3. 3 ≠ 5, so new's result is within cached's — subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 3", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_not_equal_by_same_equal() {
    // Cached != 5, new = 5 — 5 is excluded by cached. Not subsumed.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_not_equal_by_excluding_range() {
    // Cached != 5, new id > 10 — entire range excludes 5. Subsumed.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 10", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_not_equal_by_including_range() {
    // Cached != 5, new id > 3 — range includes 5. Not subsumed.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_not_equal_same() {
    // Cached != 5, new != 5 — same exclusion. Subsumed.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_not_equal_different() {
    // Cached != 5, new != 3 — different exclusions. Not subsumed.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 3", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_mixed_columns() {
    // Both columns must be subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id > 3 AND name = 'alice'",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id > 5 AND name = 'alice'",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_mixed_columns_mismatch() {
    // id subsumed but name differs — not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id > 3 AND name = 'alice'",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id > 5 AND name = 'bob'",
        &tables,
    ));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_contradictory_new() {
    // New has contradictory constraints (= 5 AND > 10) → Empty → trivially subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = 5 AND id > 10",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_contradictory_cached() {
    // Cached has contradictory constraints → Empty → no data, not subsumed
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = 5 AND id > 10",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

// ========== IN extraction tests ==========

#[test]
fn test_in_extraction() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
    let resolved = resolve_sql(sql, &tables);
    let constraints = analyze_query_constraints(&resolved);

    assert!(has_in_constraint(
        &constraints,
        "users",
        "id",
        &[
            LiteralValue::Integer(1),
            LiteralValue::Integer(2),
            LiteralValue::Integer(3),
        ],
    ));
}

#[test]
fn test_in_with_and() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id IN (1, 2) AND name = 'alice'";
    let resolved = resolve_sql(sql, &tables);
    let constraints = analyze_query_constraints(&resolved);

    assert!(has_in_constraint(
        &constraints,
        "users",
        "id",
        &[LiteralValue::Integer(1), LiteralValue::Integer(2)],
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::Equal,
        LiteralValue::String("alice".into()),
    ));
}

#[test]
fn test_in_with_parameter_skipped() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id IN (1, $1)";
    let resolved = resolve_sql(sql, &tables);
    let constraints = analyze_query_constraints(&resolved);

    // Parameter in IN list → entire IN skipped
    assert!(constraints.table_constraints.is_empty());
}

#[test]
fn test_not_in_extraction() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let sql = "SELECT * FROM users WHERE id NOT IN (1, 2, 3)";
    let resolved = resolve_sql(sql, &tables);
    let constraints = analyze_query_constraints(&resolved);

    // NOT IN → individual NotEqual constraints
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::NotEqual,
        LiteralValue::Integer(1),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::NotEqual,
        LiteralValue::Integer(2),
    ));
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::NotEqual,
        LiteralValue::Integer(3),
    ));
}

#[test]
fn test_in_propagation_through_join() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("a", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("b", Oid::from_raw(1002)));

    let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id IN (1, 2)";
    let resolved = resolve_sql(sql, &tables);
    let constraints = analyze_query_constraints(&resolved);

    // Should propagate: a.id IN (1, 2) → b.id IN (1, 2)
    assert!(has_in_constraint(
        &constraints,
        "a",
        "id",
        &[LiteralValue::Integer(1), LiteralValue::Integer(2)],
    ));
    assert!(has_in_constraint(
        &constraints,
        "b",
        "id",
        &[LiteralValue::Integer(1), LiteralValue::Integer(2)],
    ));
}

// ========== IN subsumption tests ==========

#[test]
fn test_subsumption_in_subset() {
    // IN (1,2,3) subsumed by IN (1,2) — subset
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2)",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_in_point() {
    // IN (1,2,3) subsumed by = 2 — point in set
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_in_not_subset() {
    // IN (1,2,3) NOT subsumed by IN (1,4) — 4 not in set
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 4)",
        &tables,
    ));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_range_subsumes_in() {
    // id > 0 subsumed by IN (1,2,3) — all values > 0
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached =
        analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 0", &tables));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_in_not_subsumes_range() {
    // IN (1,2,3) NOT subsumed by id > 0 — set is finite, range is infinite
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 0", &tables));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_unconstrained_subsumes_in() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id IN (1, 2, 3)",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

// ========== IN + range intersection in column_range_build ==========

#[test]
fn test_in_set_with_range_filter() {
    // IN (1,2,3,4,5) AND id > 3 → InSet({4, 5})
    let tcs = [
        TableConstraint::AnyOf(
            "id".into(),
            vec![
                LiteralValue::Integer(1),
                LiteralValue::Integer(2),
                LiteralValue::Integer(3),
                LiteralValue::Integer(4),
                LiteralValue::Integer(5),
            ],
        ),
        TableConstraint::Comparison("id".into(), BinaryOp::GreaterThan, LiteralValue::Integer(3)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    match range {
        ColumnRange::InSet(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&LiteralValue::Integer(4)));
            assert!(set.contains(&LiteralValue::Integer(5)));
        }
        _ => panic!("expected InSet, got {range:?}"),
    }
}

#[test]
fn test_in_set_with_equality_match() {
    // IN (1,2,3) AND id = 2 → Equal(2)
    let tcs = [
        TableConstraint::AnyOf(
            "id".into(),
            vec![
                LiteralValue::Integer(1),
                LiteralValue::Integer(2),
                LiteralValue::Integer(3),
            ],
        ),
        TableConstraint::Comparison("id".into(), BinaryOp::Equal, LiteralValue::Integer(2)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    assert!(matches!(
        range,
        ColumnRange::Equal(LiteralValue::Integer(2))
    ));
}

#[test]
fn test_in_set_with_equality_mismatch() {
    // IN (1,2,3) AND id = 5 → Empty
    let tcs = [
        TableConstraint::AnyOf(
            "id".into(),
            vec![
                LiteralValue::Integer(1),
                LiteralValue::Integer(2),
                LiteralValue::Integer(3),
            ],
        ),
        TableConstraint::Comparison("id".into(), BinaryOp::Equal, LiteralValue::Integer(5)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    assert!(matches!(range, ColumnRange::Empty));
}

#[test]
fn test_in_set_with_not_equal() {
    // IN (1,2,3) AND id != 2 → InSet({1, 3})
    let tcs = [
        TableConstraint::AnyOf(
            "id".into(),
            vec![
                LiteralValue::Integer(1),
                LiteralValue::Integer(2),
                LiteralValue::Integer(3),
            ],
        ),
        TableConstraint::Comparison("id".into(), BinaryOp::NotEqual, LiteralValue::Integer(2)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    match range {
        ColumnRange::InSet(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&LiteralValue::Integer(1)));
            assert!(set.contains(&LiteralValue::Integer(3)));
        }
        _ => panic!("expected InSet, got {range:?}"),
    }
}

// ========== PGC-106: where_analysis_complete tracking ==========

#[test]
fn test_no_where_clause_is_complete() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    let resolved = resolve_sql("SELECT * FROM users", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(
        constraints.where_analysis_complete,
        "no WHERE clause is trivially complete (full table scan)"
    );
}

#[test]
fn test_simple_equality_is_complete() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(constraints.where_analysis_complete);
}

#[test]
fn test_in_clause_is_complete() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    let resolved = resolve_sql("SELECT * FROM users WHERE id IN (1, 2, 3)", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(constraints.where_analysis_complete);
}

#[test]
fn test_any_eq_clause_extracts_inset() {
    // PGC-106 (option C): `WHERE col = ANY(<array>)` is set membership;
    // extracted as `ColumnConstraint::InSet` so cached ANY-queries can
    // subsume narrower ANY-queries on the same column.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
        &tables,
    );

    let constraints = analyze_query_constraints(&resolved);

    assert!(
        constraints.where_analysis_complete,
        "ANY = is now extractable, analysis is complete"
    );
    let users_cs = constraints
        .table_constraints
        .get("users")
        .expect("constraint extracted for users");
    assert_eq!(users_cs.len(), 1);
    assert!(matches!(
        users_cs[0],
        TableConstraint::AnyOf(ref col, ref vs)
            if col == "id" && vs.len() == 3
    ));
}

#[test]
fn test_or_clause_marks_incomplete() {
    // OR is still in the unrecognized-expression bucket; subsumption
    // must continue to fall back to "not subsumed" until/unless the
    // analyzer learns to handle disjunctions.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1 OR id = 2", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(!constraints.where_analysis_complete);
}

#[test]
fn test_subsumption_any_subsumes_narrower_any() {
    // PGC-106 (option C) headline: cached `ANY([1,2,3])` should
    // subsume new `ANY([1])`.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[1])",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_any_does_not_subsume_disjoint_any() {
    // The PGC-106 option-B scenario stays correct under option C:
    // disjoint arrays can't subsume each other.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2])",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[3, 4, 5])",
        &tables,
    ));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_any_subsumes_equality() {
    // Cached `ANY([1,2,3])` should also subsume new `WHERE id = 2`.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_subsumption_full_scan_still_subsumes() {
    // Sanity check that the new gate doesn't regress the legitimate
    // "full-scan subsumes everything" case: cached query has no WHERE
    // clause, so analysis is complete AND `table_constraints` is empty.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

    assert!(cached.where_analysis_complete);
    assert!(
        table_constraints_subsumed(&new, &cached, "users"),
        "true full-scan cached query should still subsume"
    );
}

// ------------------------------------------------------------------
// PGC-149: identity TypeCast strip in constraint extraction
// ------------------------------------------------------------------

#[test]
fn test_identity_text_cast_extracts_comparison_constraint() {
    // `name::text = 'alice'` on a TEXT column must extract the same
    // ColumnConstraint::Comparison as `name = 'alice'` would.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::text = 'alice'",
        &tables,
    ));

    assert!(constraints.where_analysis_complete);
    assert!(has_constraint(
        &constraints,
        "users",
        "name",
        BinaryOp::Equal,
        LiteralValue::String("alice".into()),
    ));
}

#[test]
fn test_identity_text_cast_enables_subsumption() {
    // Two queries that only differ by a redundant `::text` on a TEXT
    // column should subsume each other.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name = 'alice'",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::text = 'alice'",
        &tables,
    ));

    assert!(cached.where_analysis_complete);
    assert!(new.where_analysis_complete);
    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_identity_text_cast_on_int_column_extracts_constraint() {
    // PGC-177: int → ::text is identity, so the constraint must be
    // extracted as `id = 42` for subsumption to work.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id::text = '42'",
        &tables,
    ));

    assert!(constraints.where_analysis_complete);
    assert!(has_constraint(
        &constraints,
        "users",
        "id",
        BinaryOp::Equal,
        LiteralValue::String("42".into()),
    ));
}

#[test]
fn test_non_identity_text_cast_keeps_analysis_incomplete() {
    // `name::numeric = 42` is a text → numeric coercion, not identity.
    // Analysis must remain incomplete so subsumption stays conservative.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::numeric = 42",
        &tables,
    ));

    assert!(
        !constraints.where_analysis_complete,
        "non-identity cast must leave analysis incomplete"
    );
}

// ---------------------------------------------------------------
// PGC-182: subsumption for non-identity casts via CastComparison.
// ---------------------------------------------------------------

fn has_cast_constraint(
    constraints: &QueryConstraints,
    table: &str,
    column: &str,
    cast: &CastTarget,
    op: BinaryOp,
    value: &LiteralValue,
) -> bool {
    constraints.table_constraints.get(table).is_some_and(|cs| {
        cs.iter().any(|tc| match tc {
            TableConstraint::CastComparison(c, k, o, v) => {
                c == column && k == cast && *o == op && v == value
            }
            _ => false,
        })
    })
}

#[test]
fn test_text_to_int4_extracts_cast_comparison() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));

    assert!(constraints.where_analysis_complete);
    assert!(has_cast_constraint(
        &constraints,
        "users",
        "name",
        &CastTarget::Int4,
        BinaryOp::Equal,
        &LiteralValue::Integer(42),
    ));
}

#[test]
fn test_cast_comparison_subsumes_self() {
    // Identical cast queries: both extract the same CastComparison,
    // subsumption holds.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));

    assert!(cached.where_analysis_complete);
    assert!(new.where_analysis_complete);
    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_cast_comparison_range_subsumes_tighter_range() {
    // `WHERE name::int4 > 100` (cached) subsumes `WHERE name::int4 > 200`
    // (new) — every row in the new is also in the cached.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 > 100",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 > 200",
        &tables,
    ));

    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_cast_comparison_range_does_not_subsume_looser_range() {
    // `WHERE name::int4 > 200` (cached) does NOT subsume
    // `WHERE name::int4 > 100` (new) — new is broader.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 > 200",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 > 100",
        &tables,
    ));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_different_casts_do_not_cross_subsume() {
    // `name::int4 = 42` and `name::int8 = 42` are separate value domains;
    // neither query subsumes the other even though the value matches.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int8 = 42",
        &tables,
    ));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_bare_and_cast_constraints_do_not_cross_subsume() {
    // `name = '42'` (bare text compare) and `name::int4 = 42` are
    // different predicates — bare bytes vs int value. Subsumption must
    // not cross domains.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name = '42'",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));

    assert!(!table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_cast_comparison_does_not_propagate_through_equivalence() {
    // `name::int4 = 5 AND name = other_name` must NOT yield
    // `other_name::int4 = 5` — the cast doesn't follow equivalences
    // without knowing the other column's storage type is also castable.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("users2", Oid::from_raw(1002)));

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users u JOIN users2 u2 ON u.name = u2.name \
         WHERE u.name::int4 = 5",
        &tables,
    ));

    // u.name::int4 = 5 is extracted, but u2.name has no cast constraint.
    assert!(has_cast_constraint(
        &constraints,
        "users",
        "name",
        &CastTarget::Int4,
        BinaryOp::Equal,
        &LiteralValue::Integer(5),
    ));
    assert!(!has_cast_constraint(
        &constraints,
        "users2",
        "name",
        &CastTarget::Int4,
        BinaryOp::Equal,
        &LiteralValue::Integer(5),
    ));
}
