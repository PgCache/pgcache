//! Tests for raw-tree write classification (`statement_convert_raw`), PGC-124.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use crate::query::ast::*;
use crate::query::write::{
    INSERT_MAX_ROWS, InsertStatement, RelationRef, TransactionBoundary, WriteClass,
};

/// Classify one SQL statement through the public entry point.
fn classify(sql: &str) -> Result<RawStatement, AstError> {
    pg_query::parse_raw_scoped(sql, |tree| unsafe { statement_convert_raw(tree) })
        .expect("parse sql")
}

fn classify_write(sql: &str) -> WriteClass {
    match classify(sql) {
        Ok(RawStatement::Write(class)) => class,
        other => panic!("expected Write for {sql:?}, got {other:?}"),
    }
}

fn insert_rows(sql: &str) -> std::sync::Arc<InsertStatement> {
    match classify_write(sql) {
        WriteClass::InsertRows(insert) => insert,
        other => panic!("expected InsertRows for {sql:?}, got {other:?}"),
    }
}

fn table_name(class: &WriteClass) -> &RelationRef {
    match class {
        WriteClass::Table(rel) => rel,
        other => panic!("expected Table, got {other:?}"),
    }
}

fn assert_read_only(sql: &str) {
    assert!(
        matches!(classify(sql), Ok(RawStatement::ReadOnlyUtility { .. })),
        "expected ReadOnlyUtility for {sql:?}"
    );
}

fn transaction_of(sql: &str) -> Option<TransactionBoundary> {
    match classify(sql) {
        Ok(RawStatement::ReadOnlyUtility { transaction }) => transaction,
        other => panic!("expected ReadOnlyUtility for {sql:?}, got {other:?}"),
    }
}

#[test]
fn test_transaction_boundaries_classified() {
    assert_eq!(transaction_of("BEGIN"), Some(TransactionBoundary::Begin));
    assert_eq!(
        transaction_of("START TRANSACTION"),
        Some(TransactionBoundary::Begin)
    );
    assert_eq!(transaction_of("COMMIT"), Some(TransactionBoundary::End));
    assert_eq!(transaction_of("END"), Some(TransactionBoundary::End));
    assert_eq!(transaction_of("ROLLBACK"), Some(TransactionBoundary::End));
    assert_eq!(transaction_of("ABORT"), Some(TransactionBoundary::End));
    assert_eq!(
        transaction_of("COMMIT AND CHAIN"),
        Some(TransactionBoundary::Begin)
    );
    assert_eq!(
        transaction_of("ROLLBACK AND CHAIN"),
        Some(TransactionBoundary::Begin)
    );
    // Savepoint operations leave the transaction state unchanged.
    assert_eq!(transaction_of("SAVEPOINT s"), None);
    assert_eq!(transaction_of("RELEASE SAVEPOINT s"), None);
    assert_eq!(transaction_of("ROLLBACK TO SAVEPOINT s"), None);
    // Non-transaction utilities carry no boundary.
    assert_eq!(transaction_of("SHOW search_path"), None);
}

// ---------- INSERT row extraction ----------

#[test]
fn test_insert_values_extracts_rows() {
    let insert = insert_rows("INSERT INTO t (a, b) VALUES (1, 'x')");
    assert_eq!(insert.relation.name, "t");
    assert_eq!(insert.relation.schema, None);
    assert_eq!(
        insert.columns,
        vec![EcoString::from("a"), EcoString::from("b")]
    );
    assert_eq!(insert.rows.len(), 1);
    assert_eq!(
        insert.rows[0].as_slice(),
        &[
            Some(LiteralValue::Integer(1)),
            Some(LiteralValue::String("x".into()))
        ]
    );
}

#[test]
fn test_insert_schema_qualified() {
    let insert = insert_rows("INSERT INTO sales.orders (id) VALUES (7)");
    assert_eq!(insert.relation.schema.as_deref(), Some("sales"));
    assert_eq!(insert.relation.name, "orders");
}

#[test]
fn test_insert_multi_row_and_default_cell() {
    let insert = insert_rows("INSERT INTO t (a, b) VALUES (1, 'x'), (2, DEFAULT)");
    assert_eq!(insert.rows.len(), 2);
    assert_eq!(insert.rows[1][0], Some(LiteralValue::Integer(2)));
    // DEFAULT is unknown, not NULL.
    assert_eq!(insert.rows[1][1], None);
}

#[test]
fn test_insert_null_and_negative_literals() {
    let insert = insert_rows("INSERT INTO t (a, b) VALUES (NULL, -5)");
    assert_eq!(insert.rows[0][0], Some(LiteralValue::Null));
    assert_eq!(insert.rows[0][1], Some(LiteralValue::Integer(-5)));
}

#[test]
fn test_insert_parameter_cells() {
    let insert = insert_rows("INSERT INTO t (a, b) VALUES ($1, $2)");
    assert_eq!(
        insert.rows[0][0],
        Some(LiteralValue::Parameter("$1".into()))
    );
    assert_eq!(
        insert.rows[0][1],
        Some(LiteralValue::Parameter("$2".into()))
    );
}

#[test]
fn test_insert_unknown_cells_are_none() {
    // Casts, expressions, and function calls are unknown values — never NULL.
    let insert = insert_rows("INSERT INTO t (a, b, c) VALUES ('2024-01-01'::date, 1 + 2, now())");
    assert_eq!(insert.rows[0].as_slice(), &[None, None, None]);
}

#[test]
fn test_insert_returning_stays_row_level() {
    let insert = insert_rows("INSERT INTO t (a) VALUES (1) RETURNING a");
    assert_eq!(insert.rows.len(), 1);
}

// ---------- INSERT degradation ladder ----------

#[test]
fn test_insert_degrades_to_table() {
    for sql in [
        // Omitted column list: needs catalog column order.
        "INSERT INTO t VALUES (1, 2)",
        // ON CONFLICT can touch existing rows.
        "INSERT INTO t (a) VALUES (1) ON CONFLICT DO NOTHING",
        "INSERT INTO t (a) VALUES (1) ON CONFLICT (a) DO UPDATE SET a = 2",
        // Not row-enumerable.
        "INSERT INTO t (a) SELECT b FROM s",
        "INSERT INTO t DEFAULT VALUES",
        // Indirection writes part of a value.
        "INSERT INTO t (a[1]) VALUES (1)",
        // SELECT-only CTE feeding INSERT...SELECT.
        "WITH src AS (SELECT 1 AS b) INSERT INTO t (a) SELECT b FROM src",
    ] {
        assert_eq!(table_name(&classify_write(sql)).name, "t", "for {sql:?}");
    }
}

#[test]
fn test_insert_row_cap_degrades_to_table() {
    let rows: Vec<String> = (0..=INSERT_MAX_ROWS).map(|i| format!("({i})")).collect();
    let sql = format!("INSERT INTO t (a) VALUES {}", rows.join(", "));
    assert_eq!(table_name(&classify_write(&sql)).name, "t");

    let at_cap: Vec<String> = (0..INSERT_MAX_ROWS).map(|i| format!("({i})")).collect();
    let sql = format!("INSERT INTO t (a) VALUES {}", at_cap.join(", "));
    assert_eq!(insert_rows(&sql).rows.len(), INSERT_MAX_ROWS);
}

#[test]
fn test_insert_with_dml_cte_is_connection_scope() {
    // The CTE writes a different table than the INSERT target.
    let sql = "WITH moved AS (DELETE FROM src RETURNING a) INSERT INTO t (a) SELECT a FROM moved";
    assert!(matches!(classify_write(sql), WriteClass::Connection));
}

// ---------- Other DML ----------

#[test]
fn test_update_delete_merge_are_table_scope() {
    for sql in [
        "UPDATE t SET a = 1 WHERE b = 2",
        "DELETE FROM t WHERE a = 1",
        "MERGE INTO t USING s ON t.a = s.a WHEN MATCHED THEN UPDATE SET b = s.b",
    ] {
        assert_eq!(table_name(&classify_write(sql)).name, "t", "for {sql:?}");
    }
}

#[test]
fn test_update_with_dml_cte_is_connection_scope() {
    let sql = "WITH x AS (INSERT INTO other (a) VALUES (1) RETURNING a) UPDATE t SET a = 1";
    assert!(matches!(classify_write(sql), WriteClass::Connection));
}

#[test]
fn test_copy_classification() {
    assert_eq!(table_name(&classify_write("COPY t FROM STDIN")).name, "t");
    assert_read_only("COPY t TO STDOUT");
    assert_read_only("COPY (SELECT * FROM t) TO STDOUT");
}

#[test]
fn test_truncate_classification() {
    assert_eq!(table_name(&classify_write("TRUNCATE t")).name, "t");
    assert!(matches!(
        classify_write("TRUNCATE a, b"),
        WriteClass::Connection
    ));
}

// ---------- Transaction control and utilities ----------

#[test]
fn test_transaction_control_is_read_only() {
    for sql in [
        "BEGIN",
        "START TRANSACTION",
        "COMMIT",
        "ROLLBACK",
        "SAVEPOINT sp",
        "RELEASE SAVEPOINT sp",
        "ROLLBACK TO SAVEPOINT sp",
    ] {
        assert_read_only(sql);
    }
}

#[test]
fn test_session_utilities_are_read_only() {
    for sql in [
        "SET search_path TO public",
        "SHOW search_path",
        "DISCARD ALL",
        "DEALLOCATE p",
        "CLOSE c",
        "FETCH ALL FROM c",
        "PREPARE p AS SELECT 1",
        "LISTEN chan",
        "UNLISTEN chan",
        "NOTIFY chan",
    ] {
        assert_read_only(sql);
    }
}

#[test]
fn test_two_phase_commit_classification() {
    // PREPARE TRANSACTION commits later — the entry must never be LSN-stamped.
    assert!(matches!(
        classify_write("PREPARE TRANSACTION 'gid'"),
        WriteClass::ConnectionUnstampable
    ));
    assert!(matches!(
        classify_write("COMMIT PREPARED 'gid'"),
        WriteClass::Connection
    ));
    assert!(matches!(
        classify_write("ROLLBACK PREPARED 'gid'"),
        WriteClass::Connection
    ));
}

#[test]
fn test_opaque_statements_are_connection_scope() {
    for sql in [
        // EXECUTE runs a prepared statement whose body may be DML.
        "EXECUTE p",
        // EXPLAIN ANALYZE executes its argument.
        "EXPLAIN ANALYZE INSERT INTO t (a) VALUES (1)",
        "EXPLAIN SELECT 1",
        "CALL do_things()",
        "DO $$ BEGIN NULL; END $$",
        "CREATE TABLE t (a int)",
        "ALTER TABLE t ADD COLUMN b int",
        "VACUUM t",
    ] {
        assert!(
            matches!(classify_write(sql), WriteClass::Connection),
            "for {sql:?}"
        );
    }
}

#[test]
fn test_multi_statement_errors() {
    assert!(matches!(
        classify("INSERT INTO t (a) VALUES (1); INSERT INTO t (a) VALUES (2)"),
        Err(AstError::MultipleStatements)
    ));
}

// ---------- SELECT roots ----------

#[test]
fn test_plain_select_has_no_write_class() {
    match classify("SELECT * FROM t WHERE a = 1") {
        Ok(RawStatement::Select {
            converted: Ok(_),
            cte_write: None,
        }) => {}
        other => panic!("expected converted Select, got {other:?}"),
    }
}

#[test]
fn test_select_with_dml_cte_is_a_write() {
    let sql = "WITH x AS (INSERT INTO t (a) VALUES (1) RETURNING a) SELECT * FROM x";
    match classify(sql) {
        Ok(RawStatement::Select {
            converted: Err(_),
            cte_write: Some(WriteClass::Table(rel)),
        }) => assert_eq!(rel.name, "t"),
        other => panic!("expected DML-CTE write, got {other:?}"),
    }
}

#[test]
fn test_select_with_two_dml_ctes_is_connection_scope() {
    let sql = "WITH x AS (INSERT INTO t (a) VALUES (1) RETURNING a), \
               y AS (DELETE FROM u RETURNING b) SELECT 1";
    match classify(sql) {
        Ok(RawStatement::Select {
            converted: Err(_),
            cte_write: Some(WriteClass::Connection),
        }) => {}
        other => panic!("expected connection-scope write, got {other:?}"),
    }
}

#[test]
fn test_unconvertible_select_with_select_ctes_is_not_a_write() {
    // WITH RECURSIVE fails conversion, but its CTEs are pure SELECTs.
    let sql = "WITH RECURSIVE r AS (SELECT 1 UNION ALL SELECT n + 1 FROM r) SELECT * FROM r";
    match classify(sql) {
        Ok(RawStatement::Select {
            converted: Err(_),
            cte_write: None,
        }) => {}
        other => panic!("expected non-write failed Select, got {other:?}"),
    }
}
