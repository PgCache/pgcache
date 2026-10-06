#![allow(clippy::wildcard_enum_match_arm)]

use iddqd::BiHashMap;
use postgres_types::Type;

use super::*;
use crate::catalog::{ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::{
    AstNode, BinaryOp, Deparse, JoinType, LiteralValue, OrderDirection, SelectNode, SubLinkType,
};
use crate::query::resolved::{
    ResolveError, ResolvedBinaryExpr, ResolvedColumnNode, ResolvedFunctionCall, ResolvedJoinNode,
    ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedSetOpNode, ResolvedTableNode,
    ResolvedTableSource, ResolvedWhereExpr,
};

/// Parse SQL and return a SelectNode (for tests using new types)
fn parse_select_node(sql: &str) -> SelectNode {
    use crate::query::ast::{QueryBody, query_expr_parse};
    let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
    match query_expr.body {
        QueryBody::Select(node) => *node,
        _ => panic!("expected SELECT"),
    }
}

/// Parse SQL and resolve to ResolvedSelectNode
fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
    let node = parse_select_node(sql);
    select_node_resolve(&node, tables, &["public"]).expect("resolve")
}

/// Parse SQL and resolve to ResolvedQueryExpr (for ORDER BY/LIMIT tests)
fn resolve_query(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
    use crate::query::ast::query_expr_parse;
    let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
    query_expr_resolve(&query_expr, tables, &["public"]).expect("resolve")
}

#[test]
fn test_resolved_table_node_construction() {
    let table_node = ResolvedTableNode {
        schema: "public".into(),
        name: "users".into(),
        alias: Some("u".into()),
        relation_oid: Oid::from_raw(12345),
    };

    assert_eq!(table_node.schema, "public");
    assert_eq!(table_node.name, "users");
    assert_eq!(table_node.alias.as_deref(), Some("u"));
    assert_eq!(table_node.relation_oid.get(), 12345);
}

#[test]
fn test_resolved_column_node_construction() {
    let col_node = ResolvedColumnNode {
        schema: "public".into(),
        table: "users".into(),
        table_alias: Some("u".into()),
        column: "id".into(),
        column_metadata: ColumnMetadata {
            name: "id".into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        },
    };

    assert_eq!(col_node.schema, "public");
    assert_eq!(col_node.table, "users");
    assert_eq!(col_node.table_alias.as_deref(), Some("u"));
    assert_eq!(col_node.column, "id");
    assert_eq!(col_node.column_metadata.type_name, "int4");
    assert_eq!(col_node.column_metadata.position, ColumnPosition::FIRST);
    assert!(col_node.column_metadata.is_primary_key);
}

#[test]
fn test_resolved_select_node_default() {
    let node = ResolvedSelectNode::default();
    assert!(matches!(node.columns, ResolvedSelectColumns::None));
    assert!(node.from.is_empty());
    assert!(node.where_clause.is_none());
    assert!(node.group_by.is_empty());
    assert!(node.having.is_none());
    assert!(!node.distinct);
}

// Helper function to create test table metadata
fn test_table_metadata(name: &str, relation_oid: Oid) -> TableMetadata {
    let columns = ColumnStore::new([
        ColumnMetadata {
            name: "id".into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        },
        ColumnMetadata {
            name: "name".into(),
            position: ColumnPosition::from_raw(2),
            type_oid: TypeOid::from_raw(25),
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

/// A catalog of `test_table_metadata` tables (`id int4`, `name text`), with
/// relation OIDs from 1001 in order.
fn catalog(names: &[&str]) -> BiHashMap<TableMetadata> {
    let mut tables = BiHashMap::new();
    for (oid, name) in (1001..).zip(names) {
        tables.insert_overwrite(test_table_metadata(name, Oid::from_raw(oid)));
    }
    tables
}

/// A catalog of all-text tables given as (name, columns), OIDs from 1001.
fn catalog_with_columns(specs: &[(&str, &[&str])]) -> BiHashMap<TableMetadata> {
    let mut tables = BiHashMap::new();
    for (oid, (name, columns)) in (1001..).zip(specs) {
        tables.insert_overwrite(test_table_metadata_with_columns(
            name,
            Oid::from_raw(oid),
            columns,
        ));
    }
    tables
}

fn only_join(resolved: &ResolvedSelectNode) -> &ResolvedJoinNode {
    match resolved.from.as_slice() {
        [ResolvedTableSource::Join(join)] => join,
        other => panic!("expected a single join source, got {other:?}"),
    }
}

fn as_table(source: &ResolvedTableSource) -> &ResolvedTableNode {
    let ResolvedTableSource::Table(table) = source else {
        panic!("expected a table source, got {source:?}");
    };
    table
}

fn as_binary(expr: &ResolvedWhereExpr) -> &ResolvedBinaryExpr {
    let ResolvedWhereExpr::Binary(binary) = expr else {
        panic!("expected a binary expression, got {expr:?}");
    };
    binary
}

fn as_column(expr: &ResolvedWhereExpr) -> &ResolvedColumnNode {
    let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(column)) = expr else {
        panic!("expected a column, got {expr:?}");
    };
    column
}

fn column_ref(expr: &ResolvedWhereExpr) -> (&str, &str) {
    let column = as_column(expr);
    (column.table.as_str(), column.column.as_str())
}

/// The outer references of the first subquery: the WHERE predicate's, or the
/// first SELECT-list scalar subquery's.
fn subquery_outer_refs(resolved: &ResolvedSelectNode) -> Vec<(&str, &str)> {
    let refs = match &resolved.where_clause {
        Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) => outer_refs,
        _ => {
            let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
                panic!("expected a subquery in WHERE or the SELECT list");
            };
            cols.iter()
                .find_map(|col| match &col.expr {
                    ResolvedScalarExpr::Subquery(_, outer_refs) => Some(outer_refs),
                    _ => None,
                })
                .expect("a SELECT-list subquery")
        }
    };
    refs.iter()
        .map(|c| (c.table.as_str(), c.column.as_str()))
        .collect()
}

fn deparsed(node: &impl Deparse) -> String {
    let mut buf = String::new();
    node.deparse(&mut buf);
    buf
}

#[test]
fn test_table_and_column_resolve_cases() {
    let tables = catalog(&["users"]);
    // (label, SQL, expected FROM alias, WHERE lhs column, its type name if checked)
    #[rustfmt::skip]
    let cases = [
        ("bare table, qualified column",   "SELECT * FROM users WHERE users.id = 1",          None,      "id",   Some("int4")),
        ("aliased table, aliased column",  "SELECT * FROM users u WHERE u.name = 'john'",     Some("u"), "name", Some("text")),
        ("bare table, unqualified column", "SELECT * FROM users WHERE id = 1",                None,      "id",   None),
    ];
    for (label, sql, alias, column, type_name) in cases {
        let resolved = resolve_sql(sql, &tables);
        let [ResolvedTableSource::Table(table)] = resolved.from.as_slice() else {
            panic!("{label}: expected a single table source in {sql:?}");
        };
        let actual = (
            table.schema.as_str(),
            table.name.as_str(),
            table.alias.as_deref(),
            table.relation_oid.get(),
        );
        assert_eq!(
            actual,
            ("public", "users", alias, 1001),
            "{label}: table of {sql:?}"
        );
        let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause else {
            panic!("{label}: expected a binary WHERE in {sql:?}");
        };
        let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr else {
            panic!("{label}: expected a column on the left of the WHERE in {sql:?}");
        };
        let actual = (col.schema.as_str(), col.table.as_str(), col.column.as_str());
        assert_eq!(
            actual,
            ("public", "users", column),
            "{label}: WHERE column of {sql:?}"
        );
        if let Some(type_name) = type_name {
            assert_eq!(
                col.column_metadata.type_name, type_name,
                "{label}: column type of {sql:?}"
            );
        }
    }
}

/// Create test table metadata with custom column names (all text type, first is PK).
fn test_table_metadata_with_columns(
    name: &str,
    relation_oid: Oid,
    column_names: &[&str],
) -> TableMetadata {
    let columns =
        ColumnStore::new(
            column_names
                .iter()
                .enumerate()
                .map(|(i, col_name)| ColumnMetadata {
                    name: (*col_name).into(),
                    position: ColumnPosition::from_index(i).expect("column position in range"),
                    type_oid: TypeOid::from_raw(25),
                    data_type: Type::TEXT,
                    type_name: "text".into(),
                    cache_type_name: "text".into(),
                    is_primary_key: i == 0,
                }),
        );
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

#[test]
fn test_table_resolve_not_found() {
    let tables = BiHashMap::new();
    let node = parse_select_node("SELECT * FROM users");
    let result = select_node_resolve(&node, &tables, &["public"]);

    assert!(matches!(
        result.map_err(|e| e.into_current_context()),
        Err(ResolveError::TableNotFound { .. })
    ));
}

#[test]
fn test_column_resolve_ambiguous() {
    let tables = catalog(&["users", "orders"]);

    // Both tables have 'id' column, unqualified reference is ambiguous
    let node = parse_select_node("SELECT * FROM users, orders WHERE id = 1");
    let result = select_node_resolve(&node, &tables, &["public"]);

    assert!(matches!(
        result.map_err(|e| e.into_current_context()),
        Err(ResolveError::AmbiguousColumn { .. })
    ));
}

#[test]
fn test_select_star_expansion() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT * FROM users", &tables);

    // Check that SELECT * was expanded to all columns
    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 2);
    let ResolvedScalarExpr::Column(col) = &cols[0].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "id");
    assert_eq!(col.table, "users");
    let ResolvedScalarExpr::Column(col) = &cols[1].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "name");
    assert_eq!(col.table, "users");
}

#[test]
fn test_select_specific_columns() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT id, name FROM users", &tables);

    // Check that specific columns were resolved
    if let ResolvedSelectColumns::Columns(cols) = &resolved.columns {
        assert_eq!(cols.len(), 2);

        if let ResolvedScalarExpr::Column(col) = &cols[0].expr {
            assert_eq!(col.column, "id");
            assert_eq!(col.table, "users");
        } else {
            panic!("Expected column expression");
        }

        if let ResolvedScalarExpr::Column(col) = &cols[1].expr {
            assert_eq!(col.column, "name");
            assert_eq!(col.table, "users");
        } else {
            panic!("Expected column expression");
        }
    } else {
        panic!("Expected Columns");
    }
}

#[test]
fn test_select_star_with_column() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT *, name FROM users", &tables);

    // Star expands to all columns, then the explicit column follows
    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 3); // id, name (from *), name (explicit)

    let ResolvedScalarExpr::Column(col) = &cols[0].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "id");

    let ResolvedScalarExpr::Column(col) = &cols[1].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "name");

    let ResolvedScalarExpr::Column(col) = &cols[2].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "name");
}

#[test]
fn test_select_qualified_star_with_column() {
    let tables = catalog(&["users", "orders"]);

    let resolved = resolve_sql(
        "SELECT u.*, o.name FROM users u JOIN orders o ON o.id = u.id",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    // u.* expands to users.id, users.name, then o.name
    assert_eq!(cols.len(), 3);

    let ResolvedScalarExpr::Column(col) = &cols[0].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "id");
    assert_eq!(col.table, "users");

    let ResolvedScalarExpr::Column(col) = &cols[1].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "name");
    assert_eq!(col.table, "users");

    let ResolvedScalarExpr::Column(col) = &cols[2].expr else {
        panic!("Expected column expression");
    };
    assert_eq!(col.column, "name");
    assert_eq!(col.table, "orders");
}

/// The column node of a resolved select column, panicking otherwise.
fn select_column_node(col: &ResolvedSelectColumn) -> &ResolvedColumnNode {
    let ResolvedScalarExpr::Column(node) = &col.expr else {
        panic!("expected column expression, got {:?}", col.expr);
    };
    node
}

/// Assert a star-expanded derived-table column: empty schema, synthetic
/// table named after the alias, alias set — the same node shape an
/// explicit `alias.column` reference resolves to.
fn derived_column_assert(col: &ResolvedSelectColumn, alias: &str, column: &str) {
    let node = select_column_node(col);
    assert_eq!(node.schema, "");
    assert_eq!(node.table, alias);
    assert_eq!(node.table_alias.as_deref(), Some(alias));
    assert_eq!(node.column, column);
}

/// PGC-359: `*` over a USING join of two derived tables expands to the
/// merged join column plus each side's remaining columns.
#[test]
fn test_select_star_derived_using_inner() {
    let tables = catalog(&["users", "orders"]);

    let resolved = resolve_sql(
        "SELECT * FROM (SELECT id, name FROM users) a \
         JOIN (SELECT id, name FROM orders) b USING (id)",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 3); // merged id, a.name, b.name
    assert_eq!(cols[0].alias.as_deref(), Some("id"));
    derived_column_assert(&cols[0], "a", "id"); // inner merge = left column
    derived_column_assert(&cols[1], "a", "name");
    derived_column_assert(&cols[2], "b", "name");
}

/// PGC-359: `*` over a NATURAL LEFT JOIN of derived tables — merged
/// column is COALESCE, remaining columns follow in FROM order.
#[test]
fn test_select_star_derived_natural_left() {
    let tables = catalog(&["users", "orders"]);

    let resolved = resolve_sql(
        "SELECT * FROM (SELECT name, id AS a_id FROM users) a \
         NATURAL LEFT JOIN (SELECT name, id AS b_id FROM orders) b",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 3); // merged name, a.a_id, b.b_id
    assert_eq!(cols[0].alias.as_deref(), Some("name"));
    let ResolvedScalarExpr::Function(f) = &cols[0].expr else {
        panic!("expected COALESCE for outer-join merged column");
    };
    assert_eq!(f.name, "coalesce");
    derived_column_assert(&cols[1], "a", "a_id");
    derived_column_assert(&cols[2], "b", "b_id");
}

/// PGC-359: mixed base-then-derived USING join expands both sides.
/// PGC-359: mixed derived-then-base USING join — `*` expands in FROM
/// order (derived side's columns before the base table's).
/// PGC-359: qualified `derived.*` expands that side verbatim — join
/// column included, no merged-column injection.
/// PGC-359 (latent case): `*` over a single derived table expands to
/// the subquery's full output, not zero columns.
#[test]
fn test_select_star_single_derived() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT * FROM (SELECT id, name FROM users) d", &tables);

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 2);
    derived_column_assert(&cols[0], "d", "id");
    derived_column_assert(&cols[1], "d", "name");
}

/// PGC-359: multi-column USING over derived tables — each merged column
/// emitted once in USING order, both sides' consumed columns suppressed.
#[test]
fn test_select_star_derived_using_multi_column() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "t1",
        Oid::from_raw(1001),
        &["id", "name", "x"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "t2",
        Oid::from_raw(1002),
        &["id", "name", "y"],
    ));

    let resolved = resolve_sql(
        "SELECT * FROM (SELECT id, name, x FROM t1) a \
         JOIN (SELECT id, name, y FROM t2) b USING (id, name)",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 4); // merged id, merged name, a.x, b.y
    assert_eq!(cols[0].alias.as_deref(), Some("id"));
    assert_eq!(cols[1].alias.as_deref(), Some("name"));
    derived_column_assert(&cols[2], "a", "x");
    derived_column_assert(&cols[3], "b", "y");
}

#[test]
fn test_join_resolution() {
    let tables = catalog(&["users", "orders"]);
    let resolved = resolve_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.id",
        &tables,
    );
    let join = only_join(&resolved);
    assert_eq!(join.join_type, JoinType::Inner);
    assert_eq!(as_table(&join.left).name, "users");
    assert_eq!(as_table(&join.right).name, "orders");
    let cond = as_binary(join.predicate().expect("join condition"));
    assert_eq!(column_ref(&cond.lexpr), ("users", "id"));
    assert_eq!(column_ref(&cond.rexpr), ("orders", "id"));
}

#[test]
fn test_join_with_aliases() {
    let tables = catalog(&["users", "orders"]);
    let resolved = resolve_sql(
        "SELECT * FROM users u JOIN orders o ON u.id = o.id",
        &tables,
    );
    let join = only_join(&resolved);
    let (left, right) = (as_table(&join.left), as_table(&join.right));
    assert_eq!(
        (left.name.as_str(), left.alias.as_deref()),
        ("users", Some("u"))
    );
    assert_eq!(
        (right.name.as_str(), right.alias.as_deref()),
        ("orders", Some("o"))
    );
    // The condition's columns resolve to the real tables even though the
    // aliases were used.
    let cond = as_binary(join.predicate().expect("join condition"));
    assert_eq!(column_ref(&cond.lexpr), ("users", "id"));
    assert_eq!(column_ref(&cond.rexpr), ("orders", "id"));
}

#[test]
fn test_where_expr_complex() {
    let tables = catalog(&["users"]);
    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id = 1 AND name = 'john'",
        &tables,
    );
    let and_expr = as_binary(resolved.where_clause.as_ref().expect("WHERE"));
    assert_eq!(and_expr.op, BinaryOp::And);
    for (side, column) in [(&and_expr.lexpr, "id"), (&and_expr.rexpr, "name")] {
        let comparison = as_binary(side);
        assert_eq!(comparison.op, BinaryOp::Equal);
        assert_eq!(as_column(&comparison.lexpr).column, column);
    }
}

#[test]
fn test_order_by_simple() {
    let tables = catalog(&["users"]);

    // `SELECT *` expands `name` into the output list, so the unqualified
    // ORDER BY matches the output name and resolves to `Identifier` — PG's
    // output-first precedence rule.
    let resolved = resolve_query(
        "SELECT users.name FROM users ORDER BY users.name ASC",
        &tables,
    );

    assert_eq!(resolved.order_by.len(), 1);
    assert_eq!(resolved.order_by[0].direction, OrderDirection::Asc);

    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
        assert_eq!(col.schema, "public");
        assert_eq!(col.table, "users");
        assert_eq!(col.column, "name");
        assert_eq!(col.column_metadata.type_name, "text");
    } else {
        panic!("Expected column expression in ORDER BY");
    }
}

#[test]
fn test_order_by_multiple_columns() {
    let tables = catalog(&["users"]);

    let resolved = resolve_query(
        "SELECT users.name, users.id FROM users ORDER BY users.name ASC, users.id DESC",
        &tables,
    );

    assert_eq!(resolved.order_by.len(), 2);

    assert_eq!(resolved.order_by[0].direction, OrderDirection::Asc);
    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
        assert_eq!(col.column, "name");
        assert_eq!(col.table, "users");
    } else {
        panic!("Expected column expression");
    }

    assert_eq!(resolved.order_by[1].direction, OrderDirection::Desc);
    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[1].expr {
        assert_eq!(col.column, "id");
        assert_eq!(col.table, "users");
    } else {
        panic!("Expected column expression");
    }
}

#[test]
fn test_order_by_qualified_column() {
    let tables = catalog(&["users"]);

    let resolved = resolve_query("SELECT * FROM users u ORDER BY u.name DESC", &tables);

    // Check ORDER BY was resolved with qualified column
    assert_eq!(resolved.order_by.len(), 1);
    assert_eq!(resolved.order_by[0].direction, OrderDirection::Desc);

    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
        // Should resolve to actual table name, not alias
        assert_eq!(col.table, "users");
        assert_eq!(col.column, "name");
        assert_eq!(col.schema, "public");
    } else {
        panic!("Expected column expression");
    }
}

#[test]
fn test_order_by_select_alias() {
    let tables = catalog(&["users"]);

    let sql = "SELECT id, name AS display_name FROM users ORDER BY display_name DESC";
    let resolved = resolve_query(sql, &tables);

    assert_eq!(resolved.order_by.len(), 1);
    assert_eq!(resolved.order_by[0].direction, OrderDirection::Desc);
    match &resolved.order_by[0].expr {
        ResolvedScalarExpr::Identifier(name) => assert_eq!(name, "display_name"),
        other => panic!("expected Identifier for alias, got {other:?}"),
    }
}

#[test]
fn test_order_by_aggregate_alias() {
    let tables = catalog(&["orders"]);

    // Aggregate functions produce no column-derivable output name, so only an
    // explicit alias lets ORDER BY reference them — this is the key demo case.
    let sql = "SELECT id, SUM(id) AS total FROM orders GROUP BY id ORDER BY total DESC";
    let resolved = resolve_query(sql, &tables);

    assert_eq!(resolved.order_by.len(), 1);
    match &resolved.order_by[0].expr {
        ResolvedScalarExpr::Identifier(name) => assert_eq!(name, "total"),
        other => panic!("expected Identifier for alias, got {other:?}"),
    }
}

#[test]
fn test_order_by_qualified_does_not_match_alias() {
    let tables = catalog(&["users"]);

    // `u.name` is qualified — must resolve through the column path even if
    // an alias of the same name existed.
    let sql = "SELECT id, name AS display FROM users u ORDER BY u.name";
    let resolved = resolve_query(sql, &tables);

    assert_eq!(resolved.order_by.len(), 1);
    match &resolved.order_by[0].expr {
        ResolvedScalarExpr::Column(col) => {
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
        }
        other => panic!("expected Column for qualified ref, got {other:?}"),
    }
}

#[test]
fn test_order_by_with_join() {
    let tables = catalog(&["users", "orders"]);

    let sql = "SELECT * FROM users u JOIN orders o ON u.id = o.id ORDER BY u.name ASC, o.id DESC";
    let resolved = resolve_query(sql, &tables);

    // Check ORDER BY was resolved across joined tables
    assert_eq!(resolved.order_by.len(), 2);

    // First: u.name ASC
    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
        assert_eq!(col.table, "users");
        assert_eq!(col.column, "name");
    } else {
        panic!("Expected column expression");
    }

    // Second: o.id DESC
    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[1].expr {
        assert_eq!(col.table, "orders");
        assert_eq!(col.column, "id");
    } else {
        panic!("Expected column expression");
    }
}

#[test]
fn test_order_by_unqualified_column() {
    let tables = catalog(&["users"]);

    // Select a column whose name doesn't appear in the output list to force
    // the unqualified ORDER BY through column resolution.
    let resolved = resolve_query("SELECT id FROM users ORDER BY name", &tables);

    assert_eq!(resolved.order_by.len(), 1);
    if let ResolvedScalarExpr::Column(col) = &resolved.order_by[0].expr {
        assert_eq!(col.table, "users");
        assert_eq!(col.column, "name");
    } else {
        panic!("Expected column expression");
    }
}

#[test]
fn test_order_by_column_not_found() {
    use crate::query::ast::query_expr_parse;

    let tables = catalog(&["users"]);

    let sql = "SELECT * FROM users ORDER BY nonexistent_column ASC";
    let query_expr = query_expr_parse(sql).unwrap();

    let result = query_expr_resolve(&query_expr, &tables, &["public"]);

    // Should fail with column not found error
    assert!(matches!(
        result.map_err(|e| e.into_current_context()),
        Err(ResolveError::ColumnNotFound { .. })
    ));
}

// ==================== Deparse Tests ====================

fn id_column_metadata() -> ColumnMetadata {
    ColumnMetadata {
        name: "id".into(),
        position: ColumnPosition::from_raw(1),
        type_oid: TypeOid::from_raw(23),
        data_type: Type::INT4,
        type_name: "int4".into(),
        cache_type_name: "int4".into(),
        is_primary_key: true,
    }
}

/// PGC-262: lowercase reserved keywords are identifiers too — they must
/// deparse quoted, and an embedded `"` must be doubled.
#[test]
fn test_resolved_column_equality_ignores_alias() {
    // Two columns with same schema/table/column but different aliases should be equal
    let col1 = ResolvedColumnNode {
        schema: "public".into(),
        table: "users".into(),
        table_alias: Some("u".into()),
        column: "id".into(),
        column_metadata: id_column_metadata(),
    };

    let col2 = ResolvedColumnNode {
        schema: "public".into(),
        table: "users".into(),
        table_alias: Some("u2".into()), // Different alias
        column: "id".into(),
        column_metadata: id_column_metadata(),
    };

    assert_eq!(col1, col2);

    // Hash should also be equal
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher1 = DefaultHasher::new();
    col1.hash(&mut hasher1);
    let hash1 = hasher1.finish();

    let mut hasher2 = DefaultHasher::new();
    col2.hash(&mut hasher2);
    let hash2 = hasher2.finish();

    assert_eq!(hash1, hash2);
}

#[test]
fn test_complexity_ordering() {
    let tables = catalog(&["users", "orders"]);

    // Simple query: SELECT * FROM users
    let resolved1 = resolve_sql("SELECT * FROM users", &tables);

    // Query with WHERE: SELECT * FROM users WHERE id = 1
    let resolved2 = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

    // Query with JOIN: SELECT * FROM users JOIN orders ON ...
    let resolved3 = resolve_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.id",
        &tables,
    );

    // Verify ordering: simple < with_where < with_join
    assert!(resolved1.complexity() < resolved2.complexity());
    assert!(resolved2.complexity() < resolved3.complexity());
}

#[test]
fn test_complexity_subquery_depth() {
    let tables = catalog(&["users", "orders"]);

    // No subquery: complexity = 1 predicate
    let flat = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);
    assert_eq!(flat.subquery_depth(), 0);

    // One level of subquery: depth 1
    let one_deep = resolve_sql(
        "SELECT * FROM users WHERE id IN (SELECT id FROM orders)",
        &tables,
    );
    assert_eq!(one_deep.subquery_depth(), 1);

    // Subquery adds 5 per depth level, so one_deep > flat
    assert!(
        one_deep.complexity() > flat.complexity(),
        "subquery should increase complexity: {} > {}",
        one_deep.complexity(),
        flat.complexity()
    );
}

#[test]
fn test_complexity_nested_subquery_depth() {
    let tables = catalog(&["products", "stores", "regions"]);

    // Double-nested: depth 2
    let double_nested = resolve_sql(
        "SELECT * FROM products WHERE id IN (SELECT id FROM stores WHERE id IN (SELECT id FROM regions))",
        &tables,
    );
    assert_eq!(double_nested.subquery_depth(), 2);

    // Single-nested: depth 1
    let single_nested = resolve_sql(
        "SELECT * FROM stores WHERE id IN (SELECT id FROM regions)",
        &tables,
    );
    assert_eq!(single_nested.subquery_depth(), 1);

    // Inner query (no subqueries): depth 0
    let inner = resolve_sql("SELECT * FROM regions", &tables);
    assert_eq!(inner.subquery_depth(), 0);

    // Verify ordering: inner < single_nested < double_nested
    assert!(
        inner.complexity() < single_nested.complexity(),
        "inner ({}) < single_nested ({})",
        inner.complexity(),
        single_nested.complexity()
    );
    assert!(
        single_nested.complexity() < double_nested.complexity(),
        "single_nested ({}) < double_nested ({})",
        single_nested.complexity(),
        double_nested.complexity()
    );
}

#[test]
fn test_group_by_resolve_single_column() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT name FROM users GROUP BY name", &tables);

    assert_eq!(resolved.group_by.len(), 1);
    assert_eq!(resolved.group_by[0].schema, "public");
    assert_eq!(resolved.group_by[0].table, "users");
    assert_eq!(resolved.group_by[0].column, "name");
}

#[test]
fn test_group_by_resolve_multiple_columns() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT id, name FROM users GROUP BY id, name", &tables);

    assert_eq!(resolved.group_by.len(), 2);
    assert_eq!(resolved.group_by[0].column, "id");
    assert_eq!(resolved.group_by[1].column, "name");
}

#[test]
fn test_group_by_resolve_qualified_column() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql("SELECT u.name FROM users u GROUP BY u.name", &tables);

    assert_eq!(resolved.group_by.len(), 1);
    assert_eq!(resolved.group_by[0].table, "users");
    assert_eq!(resolved.group_by[0].table_alias.as_deref(), Some("u"));
    assert_eq!(resolved.group_by[0].column, "name");
}

#[test]
fn test_having_resolve() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql(
        "SELECT name FROM users GROUP BY name HAVING name = 'alice'",
        &tables,
    );

    assert!(resolved.having.is_some());
    if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.having {
        if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
            assert_eq!(col.column, "name");
        } else {
            panic!("Expected column in HAVING clause");
        }
    } else {
        panic!("Expected binary expression in HAVING clause");
    }
}

#[test]
fn test_combined_group_by_having_limit_resolve() {
    let tables = catalog(&["users"]);

    let sql = "SELECT name FROM users GROUP BY name HAVING name != 'test' ORDER BY name LIMIT 10";
    let resolved = resolve_query(sql, &tables);

    // GROUP BY and HAVING are on the select body
    let ResolvedQueryBody::Select(select) = &resolved.body else {
        panic!("Expected SELECT body");
    };
    assert_eq!(select.group_by.len(), 1);
    assert!(select.having.is_some());

    // ORDER BY and LIMIT are on the QueryExpr
    assert!(!resolved.order_by.is_empty());
    assert!(resolved.limit.is_some());
    assert_eq!(
        resolved.limit.unwrap().count,
        Some(LiteralValue::Integer(10))
    );
}

#[test]
fn test_resolved_window_function() {
    let tables = catalog(&["users"]);

    // Use columns that exist in test_table_metadata: id, name
    let resolved = resolve_sql(
        "SELECT sum(id) OVER (PARTITION BY name ORDER BY id) FROM users",
        &tables,
    );

    let ResolvedSelectColumns::Columns(columns) = &resolved.columns else {
        panic!("expected columns");
    };

    let ResolvedSelectColumn {
        expr: ResolvedScalarExpr::Function(func),
        ..
    } = &columns[0]
    else {
        panic!("expected function");
    };

    assert_eq!(func.name, "sum");
    assert!(func.over.is_some(), "should have OVER clause");

    let window_spec = func.over.as_ref().unwrap();
    assert_eq!(window_spec.partition_by.len(), 1);
    assert_eq!(window_spec.order_by.len(), 1);
}

#[test]
fn test_resolved_window_function_deparse() {
    let tables = catalog(&["users"]);

    // Use columns that exist in test_table_metadata: id, name
    let resolved = resolve_sql(
        "SELECT sum(id) OVER (ORDER BY name DESC) FROM users",
        &tables,
    );

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    // Should contain the window function with OVER clause
    assert!(
        buf.contains("OVER"),
        "deparsed SQL should contain OVER: {}",
        buf
    );
    assert!(
        buf.contains("ORDER BY"),
        "deparsed SQL should contain ORDER BY: {}",
        buf
    );
}

#[test]
fn test_select_nodes_simple_select() {
    let query_expr = ResolvedQueryExpr {
        body: ResolvedQueryBody::Select(Box::default()),
        order_by: vec![],
        limit: None,
    };

    let branches = query_expr.select_nodes();
    assert_eq!(branches.len(), 1, "simple SELECT should have one branch");
}

#[test]
fn test_select_nodes_union() {
    use crate::query::ast::SetOpType;

    let left_select = ResolvedSelectNode {
        from: vec![ResolvedTableSource::Table(ResolvedTableNode {
            schema: "public".into(),
            name: "a".into(),
            alias: None,
            relation_oid: Oid::from_raw(1001),
        })],
        ..Default::default()
    };

    let right_select = ResolvedSelectNode {
        from: vec![ResolvedTableSource::Table(ResolvedTableNode {
            schema: "public".into(),
            name: "b".into(),
            alias: None,
            relation_oid: Oid::from_raw(1002),
        })],
        ..Default::default()
    };

    let set_op = ResolvedSetOpNode {
        op: SetOpType::Union,
        all: false,
        left: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::new(left_select)),
            order_by: vec![],
            limit: None,
        }),
        right: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::new(right_select)),
            order_by: vec![],
            limit: None,
        }),
    };

    let query_expr = ResolvedQueryExpr {
        body: ResolvedQueryBody::SetOp(set_op),
        order_by: vec![],
        limit: None,
    };

    let branches = query_expr.select_nodes();
    assert_eq!(branches.len(), 2, "UNION should have two branches");

    // Verify each branch has the correct table
    assert_eq!(branches[0].from.len(), 1);
    assert_eq!(branches[1].from.len(), 1);

    if let ResolvedTableSource::Table(t) = &branches[0].from[0] {
        assert_eq!(t.name, "a");
    } else {
        panic!("Expected table source");
    }

    if let ResolvedTableSource::Table(t) = &branches[1].from[0] {
        assert_eq!(t.name, "b");
    } else {
        panic!("Expected table source");
    }
}

#[test]
fn test_select_nodes_nested_union() {
    use crate::query::ast::SetOpType;

    // Build: (SELECT FROM a UNION SELECT FROM b) UNION SELECT FROM c
    let a_select = ResolvedSelectNode {
        from: vec![ResolvedTableSource::Table(ResolvedTableNode {
            schema: "public".into(),
            name: "a".into(),
            alias: None,
            relation_oid: Oid::from_raw(1001),
        })],
        ..Default::default()
    };

    let b_select = ResolvedSelectNode {
        from: vec![ResolvedTableSource::Table(ResolvedTableNode {
            schema: "public".into(),
            name: "b".into(),
            alias: None,
            relation_oid: Oid::from_raw(1002),
        })],
        ..Default::default()
    };

    let c_select = ResolvedSelectNode {
        from: vec![ResolvedTableSource::Table(ResolvedTableNode {
            schema: "public".into(),
            name: "c".into(),
            alias: None,
            relation_oid: Oid::from_raw(1003),
        })],
        ..Default::default()
    };

    let inner_union = ResolvedSetOpNode {
        op: SetOpType::Union,
        all: false,
        left: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::new(a_select)),
            order_by: vec![],
            limit: None,
        }),
        right: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::new(b_select)),
            order_by: vec![],
            limit: None,
        }),
    };

    let outer_union = ResolvedSetOpNode {
        op: SetOpType::Union,
        all: false,
        left: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::SetOp(inner_union),
            order_by: vec![],
            limit: None,
        }),
        right: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(Box::new(c_select)),
            order_by: vec![],
            limit: None,
        }),
    };

    let query_expr = ResolvedQueryExpr {
        body: ResolvedQueryBody::SetOp(outer_union),
        order_by: vec![],
        limit: None,
    };

    let branches = query_expr.select_nodes();
    assert_eq!(branches.len(), 3, "nested UNION should have three branches");
}

// ==========================================================================
// Subquery Resolution Tests
// ==========================================================================

#[test]
fn test_where_subquery_in_resolution() {
    // Test resolving WHERE ... IN (SELECT ...) subquery
    let tables = catalog(&["users", "active_users"]);

    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
        &tables,
    );

    // Should have resolved WHERE clause with subquery
    let where_clause = resolved
        .where_clause
        .as_ref()
        .expect("should have WHERE clause");

    match where_clause {
        ResolvedWhereExpr::Subquery {
            sublink_type,
            test_expr,
            query,
            ..
        } => {
            assert_eq!(
                *sublink_type,
                SubLinkType::Any,
                "IN should resolve as SubLinkType::Any"
            );
            assert!(test_expr.is_some(), "IN should have test expression");

            // Verify inner query was resolved
            match &query.body {
                ResolvedQueryBody::Select(inner_select) => {
                    assert_eq!(inner_select.from.len(), 1);
                    if let ResolvedTableSource::Table(t) = &inner_select.from[0] {
                        assert_eq!(t.name, "active_users");
                        assert_eq!(t.relation_oid.get(), 1002);
                    } else {
                        panic!("Expected table source");
                    }
                }
                _ => panic!("Expected SELECT body in subquery"),
            }
        }
        _ => panic!(
            "Expected ResolvedWhereExpr::Subquery, got {:?}",
            where_clause
        ),
    }
}

#[test]
fn test_where_subquery_exists_resolution() {
    // Test resolving WHERE EXISTS (SELECT ...) subquery
    let tables = catalog(&["orders", "items"]);

    let resolved = resolve_sql(
        "SELECT * FROM orders WHERE EXISTS (SELECT id FROM items)",
        &tables,
    );

    let where_clause = resolved
        .where_clause
        .as_ref()
        .expect("should have WHERE clause");

    match where_clause {
        ResolvedWhereExpr::Subquery {
            sublink_type,
            test_expr,
            ..
        } => {
            assert_eq!(
                *sublink_type,
                SubLinkType::Exists,
                "EXISTS should resolve correctly"
            );
            assert!(
                test_expr.is_none(),
                "EXISTS should not have test expression"
            );
        }
        _ => panic!("Expected ResolvedWhereExpr::Subquery"),
    }
}

#[test]
fn test_where_subquery_scalar_resolution() {
    // Test resolving scalar subquery in WHERE clause
    let tables = catalog(&["users"]);

    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id > (SELECT id FROM users)",
        &tables,
    );

    let where_clause = resolved
        .where_clause
        .as_ref()
        .expect("should have WHERE clause");

    // The scalar subquery should be on the right side of the > comparison
    match where_clause {
        ResolvedWhereExpr::Binary(binary) => match binary.rexpr.as_ref() {
            ResolvedWhereExpr::Subquery { sublink_type, .. } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Expr,
                    "Scalar subquery should be SubLinkType::Expr"
                );
            }
            _ => panic!("Expected ResolvedWhereExpr::Subquery on right side"),
        },
        _ => panic!("Expected ResolvedWhereExpr::Binary"),
    }
}

#[test]
fn test_table_subquery_resolution() {
    // Test resolving subquery in FROM clause (derived table)
    let tables = catalog(&["users"]);

    // Note: Column resolution from subqueries is limited, but the subquery itself should resolve
    let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");
    let result = select_node_resolve(&node, &tables, &["public"]);

    // Should resolve successfully
    let resolved = result.expect("should resolve table subquery");
    assert_eq!(resolved.from.len(), 1);

    match &resolved.from[0] {
        ResolvedTableSource::Subquery(sub) => {
            assert_eq!(sub.alias.name, "sub", "Should preserve alias");

            // Verify inner query was resolved
            match &sub.query.body {
                ResolvedQueryBody::Select(inner_select) => {
                    assert_eq!(inner_select.from.len(), 1);
                    if let ResolvedTableSource::Table(t) = &inner_select.from[0] {
                        assert_eq!(t.name, "users");
                    } else {
                        panic!("Expected table source in inner query");
                    }
                }
                _ => panic!("Expected SELECT body"),
            }
        }
        _ => panic!("Expected ResolvedTableSource::Subquery"),
    }
}

#[test]
fn test_table_subquery_requires_alias() {
    // Test that table subquery without alias fails resolution
    let tables = catalog(&["users"]);

    // Parse a query with subquery without alias
    // Note: PostgreSQL parser typically requires alias, but we should still handle the error
    // gracefully if it somehow gets through
    let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");

    // This should succeed since it has an alias
    let result = select_node_resolve(&node, &tables, &["public"]);
    assert!(result.is_ok());
}

// ==========================================================================
// Direct Table Nodes Tests (population uses these, not nodes())
// ==========================================================================

#[test]
fn test_direct_table_nodes_excludes_where_subquery() {
    let tables = catalog(&["users", "active_users"]);

    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
        &tables,
    );

    // nodes() finds both tables (full traversal)
    let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(all_tables.len(), 2);

    // direct_table_nodes() only finds the FROM-clause table
    let direct_tables = resolved.direct_table_nodes();
    assert_eq!(direct_tables.len(), 1, "Should only find direct FROM table");
    assert_eq!(direct_tables[0].name, "users");
}

#[test]
fn test_direct_table_nodes_with_join_and_subquery() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "items",
        Oid::from_raw(1001),
        &["id", "name", "category_id"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "inventory",
        Oid::from_raw(1002),
        &["id", "item_id", "quantity"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "categories",
        Oid::from_raw(1003),
        &["id", "name", "active"],
    ));

    let resolved = resolve_sql(
        "SELECT i.name FROM items i \
         JOIN inventory inv ON i.id = inv.item_id \
         WHERE i.category_id IN (SELECT c.id FROM categories c WHERE c.active = true) \
         ORDER BY i.name",
        &tables,
    );

    // nodes() finds all 3 tables
    let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(all_tables.len(), 3);

    // direct_table_nodes() only finds the 2 JOIN tables, not the WHERE subquery table
    let direct_tables = resolved.direct_table_nodes();
    assert_eq!(
        direct_tables.len(),
        2,
        "Should find items and inventory but not categories"
    );
    let names: Vec<&str> = direct_tables.iter().map(|t| t.name.as_str()).collect();
    assert!(names.contains(&"items"));
    assert!(names.contains(&"inventory"));
    assert!(!names.contains(&"categories"));
}

#[test]
fn test_direct_table_nodes_derived_table() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql(
        "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
        &tables,
    );

    // nodes() finds the table inside the derived table
    let all_tables: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(all_tables.len(), 1);

    // direct_table_nodes() finds nothing — the derived table is a subquery, not a direct table
    let direct_tables = resolved.direct_table_nodes();
    assert_eq!(
        direct_tables.len(),
        0,
        "Derived table should not appear in direct_table_nodes"
    );
}

// ==========================================================================
// Correlated Subquery Tests
// ==========================================================================

#[test]
fn test_doubly_nested_correlated_subquery() {
    // Grandchild subquery references the grandparent scope
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "departments",
        Oid::from_raw(1001),
        &["id", "name"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "employees",
        Oid::from_raw(1002),
        &["id", "dept_id"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "projects",
        Oid::from_raw(1003),
        &["id", "employee_id"],
    ));

    // departments d → employees e (correlated on d.id) → projects (correlated on e.id)
    let node = parse_select_node(
        "SELECT d.id FROM departments d \
         WHERE EXISTS (\
           SELECT 1 FROM employees e WHERE e.dept_id = d.id AND EXISTS (\
             SELECT 1 FROM projects WHERE employee_id = e.id\
           )\
         )",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    // Resolution must succeed; the outer EXISTS subquery is correlated on d.id
    assert!(
        result.is_ok(),
        "doubly-nested correlated subquery should resolve, got: {:?}",
        result
    );
    let resolved = result.unwrap();
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert!(
        !outer_refs.is_empty(),
        "outer EXISTS should be correlated on departments.id"
    );
    assert_eq!(outer_refs[0].table, "departments");
    assert_eq!(outer_refs[0].column, "id");
}

#[test]
fn test_unqualified_column_not_in_any_scope() {
    // `nonexistent` doesn't exist in any table — should remain ColumnNotFound
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "users",
        Oid::from_raw(1001),
        &["id", "name"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "total"],
    ));

    let node = parse_select_node(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE nonexistent = 1)",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    assert!(
        matches!(
            result.as_ref().map_err(|e| e.current_context()),
            Err(ResolveError::ColumnNotFound { .. })
        ),
        "Column not in any scope should remain ColumnNotFound, got: {:?}",
        result
    );
}

// ---------------------------------------------------------------
// PGC-123: HAVING aggregate metadata must survive resolution and
// resolved-side deparse.
// ---------------------------------------------------------------

fn resolved_having_lhs_function(node: &ResolvedSelectNode) -> &ResolvedFunctionCall {
    let having = node.having.as_ref().expect("resolved HAVING present");
    let ResolvedWhereExpr::Binary(binary) = having else {
        panic!("expected Binary HAVING, got {having:?}");
    };
    let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Function(func)) = binary.lexpr.as_ref()
    else {
        panic!("expected Scalar(Function) on LHS, got {:?}", binary.lexpr);
    };
    func
}

#[test]
fn test_having_filter_resolved_deparse_contains_filter() {
    let tables = catalog(&["users"]);

    let resolved = resolve_sql(
        "SELECT name FROM users GROUP BY name \
         HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
        &tables,
    );

    let mut buf = String::new();
    resolved.deparse(&mut buf);
    assert!(
        buf.contains("FILTER (WHERE "),
        "resolved deparse must keep FILTER, got: {buf}"
    );
}

#[test]
fn test_resolved_column_node_deparse_cases() {
    // (label, schema, table, alias, column, expected)
    #[rustfmt::skip]
    let cases = [
        ("alias wins",              "public", "users", Some("u"), "id",        "u.id"),
        ("schema-qualified",        "public", "users", None,      "id",        "public.users.id"),
        ("mixed case is quoted",    "Public", "Users", None,      "firstName", "\"Public\".\"Users\".\"firstName\""),
        ("keywords are quoted",     "public", "order", None,      "user",      "public.\"order\".\"user\""),
        ("embedded quote doubled",  "public", "users", None,      "we\"ird",   "public.users.\"we\"\"ird\""),
    ];
    for (label, schema, table, alias, column, expected) in cases {
        let node = ResolvedColumnNode {
            schema: schema.into(),
            table: table.into(),
            table_alias: alias.map(Into::into),
            column: column.into(),
            column_metadata: id_column_metadata(),
        };
        assert_eq!(deparsed(&node), expected, "{label}");
    }
}

#[test]
fn test_resolved_table_node_deparse_cases() {
    // (label, schema, name, alias, expected)
    #[rustfmt::skip]
    let cases = [
        ("with alias",           "public", "users", Some("u"), " public.users u"),
        ("without alias",        "public", "users", None,      " public.users"),
        ("mixed case is quoted", "Public", "Users", None,      " \"Public\".\"Users\""),
    ];
    for (label, schema, name, alias, expected) in cases {
        let node = ResolvedTableNode {
            schema: schema.into(),
            name: name.into(),
            alias: alias.map(Into::into),
            relation_oid: Oid::from_raw(1001),
        };
        assert_eq!(deparsed(&node), expected, "{label}");
    }
}

#[test]
fn test_resolved_query_deparse_cases() {
    // `*` expands to explicit columns; references are fully qualified unless
    // aliased.
    #[rustfmt::skip]
    let cases = [
        ("WHERE",          "SELECT * FROM users WHERE id = 1",
                           "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1"),
        ("table alias",    "SELECT u.id, u.name FROM users u WHERE u.id = 1",
                           "SELECT u.id, u.name FROM public.users u WHERE u.id = 1"),
        ("join",           "SELECT u.id, o.name FROM users u JOIN orders o ON u.id = o.id WHERE u.id = 1",
                           "SELECT u.id, o.name FROM public.users u JOIN public.orders o ON u.id = o.id WHERE u.id = 1"),
        ("ORDER BY",       "SELECT id FROM users u ORDER BY name DESC",
                           "SELECT u.id FROM public.users u ORDER BY u.name DESC"),
        ("count(*)",       "SELECT COUNT(*) FROM users WHERE id = 1",
                           "SELECT count(*) FROM public.users WHERE public.users.id = 1"),
        ("count DISTINCT", "SELECT COUNT(DISTINCT name) FROM users WHERE id = 1",
                           "SELECT count(DISTINCT public.users.name) FROM public.users WHERE public.users.id = 1"),
        ("CASE",           "SELECT CASE WHEN name = 'admin' THEN 1 ELSE 0 END FROM users WHERE id = 1",
                           "SELECT CASE WHEN public.users.name = 'admin' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = 1"),
    ];
    let tables = catalog(&["users", "orders"]);
    for (label, sql, expected) in cases {
        assert_eq!(
            deparsed(&resolve_query(sql, &tables)),
            expected,
            "{label}: {sql}"
        );
    }
}

#[test]
fn test_complexity_cases() {
    // Each predicate counts 1, each join 3, each subquery level 5.
    #[rustfmt::skip]
    let cases = [
        ("one table, no WHERE",        "SELECT * FROM users",                                                   0, 0),
        ("one table, one predicate",   "SELECT * FROM users WHERE id = 1",                                      1, 0),
        ("one table, two predicates",  "SELECT * FROM users WHERE id = 1 AND name = 'john'",                    2, 0),
        ("join, no WHERE",             "SELECT * FROM users JOIN orders ON users.id = orders.id",               3, 0),
        ("join plus one predicate",    "SELECT * FROM users JOIN orders ON users.id = orders.id WHERE users.id = 1", 4, 0),
    ];
    let tables = catalog(&["users", "orders"]);
    for (label, sql, complexity, depth) in cases {
        let resolved = resolve_sql(sql, &tables);
        assert_eq!(
            (resolved.complexity(), resolved.subquery_depth()),
            (complexity, depth),
            "{label}: (complexity, subquery depth) of {sql}"
        );
    }
    // A FROM subquery counts as one level of depth.
    let from_sub = resolve_sql(
        "SELECT * FROM (SELECT * FROM users WHERE id = 1) sub",
        &tables,
    );
    assert_eq!(from_sub.subquery_depth(), 1, "FROM subquery depth");
}

#[test]
fn test_limit_resolve_cases() {
    use LiteralValue::{Integer, Parameter};
    // Parameterized values are preserved through resolution.
    #[rustfmt::skip]
    let cases = [
        ("count only",         "SELECT * FROM users LIMIT 10",            Some((Some(Integer(10)), None))),
        ("offset only",        "SELECT * FROM users OFFSET 5",            Some((None, Some(Integer(5))))),
        ("count and offset",   "SELECT * FROM users LIMIT 10 OFFSET 20",  Some((Some(Integer(10)), Some(Integer(20))))),
        ("parameterized",      "SELECT * FROM users LIMIT $1 OFFSET $2",  Some((Some(Parameter("$1".into())), Some(Parameter("$2".into()))))),
        ("no LIMIT",           "SELECT * FROM users",                     None),
    ];
    let tables = catalog(&["users"]);
    for (label, sql, expected) in cases {
        let actual = resolve_query(sql, &tables)
            .limit
            .map(|l| (l.count, l.offset));
        assert_eq!(actual, expected, "{label}: (count, offset) of {sql}");
    }
}

#[test]
fn test_subquery_nodes_traversal_cases() {
    // nodes() reaches tables in WHERE subqueries, FROM subqueries, SELECT-list
    // scalar subqueries and nested subqueries.
    #[rustfmt::skip]
    let cases: [(&str, &[&str], &str, &[&str]); 4] = [
        ("WHERE subquery",  &["users", "active_users"], "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
                            &["active_users", "users"]),
        ("FROM subquery",   &["users"],                 "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
                            &["users"]),
        ("scalar subquery", &["orders", "users"],       "SELECT id, (SELECT COUNT(*) FROM users) AS user_count FROM orders WHERE id = 1",
                            &["orders", "users"]),
        ("nested",          &["a", "b", "c"],           "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
                            &["a", "b", "c"]),
    ];
    for (label, names, sql, expected) in cases {
        let resolved = resolve_sql(sql, &catalog(names));
        let mut found: Vec<&str> = resolved
            .nodes::<ResolvedTableNode>()
            .map(|t| t.name.as_str())
            .collect();
        found.sort_unstable();
        assert_eq!(found, expected, "{label}: tables reached in {sql}");
    }
}

#[test]
fn test_correlated_subquery_outer_refs_cases() {
    let users_orders = || catalog(&["users", "orders"]);
    // Unqualified columns fall back to the outer scope only when the inner
    // scope lacks them; a column in both scopes binds to the inner one.
    #[rustfmt::skip]
    let cases = [
        ("EXISTS",                     catalog(&["orders", "items"]),
         "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.id = orders.id)",                   vec![("orders", "id")]),
        ("IN",                         users_orders(),
         "SELECT * FROM users WHERE id IN (SELECT id FROM orders WHERE orders.name = users.name)",               vec![("users", "name")]),
        ("SELECT-list scalar",         users_orders(),
         "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.id = users.id) FROM users",                       vec![("users", "id")]),
        ("outer table alias",          users_orders(),
         "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = u.id)",                     vec![("users", "id")]),
        ("unqualified, outer only",    catalog_with_columns(&[("users", &["id", "email"]), ("orders", &["id", "user_id", "total"])]),
         "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE email = 'test@example.com')",             vec![("users", "email")]),
        ("unqualified, in both",       catalog_with_columns(&[("users", &["id", "email"]), ("orders", &["id", "user_id"])]),
         "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = id)",                      vec![]),
        ("unqualified, scalar",        catalog_with_columns(&[("users", &["id", "status"]), ("orders", &["id", "amount"])]),
         "SELECT id, (SELECT COUNT(*) FROM orders WHERE status = 'active') FROM users",                          vec![("users", "status")]),
        ("not correlated",             catalog(&["users", "active_users"]),
         "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",                                         vec![]),
        ("mixed inner and outer",      catalog_with_columns(&[("departments", &["id", "region"]), ("employees", &["id", "dept_id", "region"])]),
         "SELECT d.id FROM departments d WHERE EXISTS (SELECT 1 FROM employees WHERE dept_id = d.id)",           vec![("departments", "id")]),
    ];
    for (label, tables, sql, expected) in cases {
        let node = parse_select_node(sql);
        let resolved = select_node_resolve(&node, &tables, &["public"])
            .unwrap_or_else(|e| panic!("{label}: {sql} should resolve: {e:?}"));
        assert_eq!(
            subquery_outer_refs(&resolved),
            expected,
            "{label}: outer refs of {sql}"
        );
    }
}

/// An expected `*`-expansion output column.
#[derive(Debug, Clone, Copy)]
enum StarColumn {
    /// A USING/NATURAL merged join column, emitted under its name.
    Merged(&'static str),
    /// A base-table column (table, column).
    Base(&'static str, &'static str),
    /// A derived-table column (derived alias, column).
    Derived(&'static str, &'static str),
}

fn star_column_assert(col: &ResolvedSelectColumn, expected: StarColumn, context: &str) {
    match expected {
        StarColumn::Merged(name) => {
            assert_eq!(col.alias.as_deref(), Some(name), "{context}: merged column")
        }
        StarColumn::Base(table, column) => {
            let node = select_column_node(col);
            assert_eq!(
                (node.table.as_str(), node.column.as_str()),
                (table, column),
                "{context}"
            );
        }
        StarColumn::Derived(alias, column) => derived_column_assert(col, alias, column),
    }
}

#[test]
fn test_select_star_mixed_derived_cases() {
    use StarColumn::{Base, Derived, Merged};
    let mut tables = catalog(&["users"]);
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "total"],
    ));
    // Unqualified `*` emits the merged USING column first, then each side's
    // remaining columns in FROM order; qualified `o.*` is that side verbatim.
    #[rustfmt::skip]
    let cases: [(&str, &str, &[StarColumn]); 3] = [
        ("base table first",    "SELECT * FROM users u JOIN (SELECT id, total FROM orders) o USING (id)",
                                &[Merged("id"), Base("users", "name"), Derived("o", "total")]),
        ("derived table first", "SELECT * FROM (SELECT id, total FROM orders) o JOIN users u USING (id)",
                                &[Merged("id"), Derived("o", "total"), Base("users", "name")]),
        ("qualified o.*",       "SELECT o.* FROM (SELECT id, total FROM orders) o JOIN users u USING (id)",
                                &[Derived("o", "id"), Derived("o", "total")]),
    ];
    for (label, sql, expected) in cases {
        let resolved = resolve_sql(sql, &tables);
        let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
            panic!("{label}: expected Columns for {sql}");
        };
        assert_eq!(cols.len(), expected.len(), "{label}: column count of {sql}");
        for (i, (col, want)) in cols.iter().zip(expected).enumerate() {
            star_column_assert(col, *want, &format!("{label}: column {i} of {sql}"));
        }
    }
}

/// A predicate over a resolved HAVING aggregate.
type AggregateCheck = fn(&ResolvedFunctionCall) -> bool;

#[test]
fn test_having_aggregate_decorations_resolve_cases() {
    // FILTER, aggregate ORDER BY and DISTINCT on a HAVING aggregate must
    // survive resolution.
    #[rustfmt::skip]
    let cases: [(&str, &str, AggregateCheck); 3] = [
        ("count(*) FILTER", "SELECT name FROM users GROUP BY name HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
         |f| f.name == "count" && f.agg_star && f.agg_filter.is_some()),
        ("string_agg ORDER BY", "SELECT id FROM users GROUP BY id HAVING string_agg(name, ',' ORDER BY name) <> ''",
         |f| f.name == "string_agg" && !f.agg_order.is_empty()),
        ("count DISTINCT", "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1",
         |f| f.agg_distinct),
    ];
    let tables = catalog(&["users"]);
    for (label, sql, check) in cases {
        let resolved = resolve_sql(sql, &tables);
        let func = resolved_having_lhs_function(&resolved);
        assert!(
            check(func),
            "{label}: HAVING aggregate of {sql} resolved to {func:?}"
        );
    }
}
