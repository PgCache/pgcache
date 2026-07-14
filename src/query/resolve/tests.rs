#![allow(clippy::wildcard_enum_match_arm)]

use iddqd::BiHashMap;
use tokio_postgres::types::Type;

use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use crate::oid::Oid;
use crate::query::ast::{
    AstNode, BinaryOp, Deparse, JoinType, LiteralValue, OrderDirection, SelectNode, SubLinkType,
};
use crate::query::resolved::{
    ResolveError, ResolvedColumnNode, ResolvedFunctionCall, ResolvedQueryBody, ResolvedQueryExpr,
    ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns, ResolvedSelectNode,
    ResolvedSetOpNode, ResolvedTableNode, ResolvedTableSource, ResolvedWhereExpr,
};

use super::*;

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
            position: 1,
            type_oid: 23,
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
    assert_eq!(col_node.column_metadata.position, 1);
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
                    position: i16::try_from(i + 1).expect("column position fits in i16"),
                    type_oid: 25,
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
fn test_table_resolve_simple() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users", &tables);

    assert_eq!(resolved.from.len(), 1);
    if let ResolvedTableSource::Table(table) = &resolved.from[0] {
        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "users");
        assert_eq!(table.alias, None);
        assert_eq!(table.relation_oid.get(), 1001);
    } else {
        panic!("Expected table source");
    }
}

#[test]
fn test_table_resolve_with_alias() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users u", &tables);

    assert_eq!(resolved.from.len(), 1);
    if let ResolvedTableSource::Table(table) = &resolved.from[0] {
        assert_eq!(table.schema, "public");
        assert_eq!(table.name, "users");
        assert_eq!(table.alias.as_deref(), Some("u"));
        assert_eq!(table.relation_oid.get(), 1001);
    } else {
        panic!("Expected table source");
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
fn test_column_resolve_qualified() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users WHERE users.id = 1", &tables);

    // Check WHERE clause resolved correctly
    if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
        if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
            assert_eq!(col.schema, "public");
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "id");
            assert_eq!(col.column_metadata.type_name, "int4");
        } else {
            panic!("Expected column in binary expression");
        }
    } else {
        panic!("Expected binary WHERE expression");
    }
}

#[test]
fn test_column_resolve_with_alias() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users u WHERE u.name = 'john'", &tables);

    // Check WHERE clause resolved correctly
    if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
        if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
            assert_eq!(col.schema, "public");
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "name");
            assert_eq!(col.column_metadata.type_name, "text");
        } else {
            panic!("Expected column in binary expression");
        }
    } else {
        panic!("Expected binary WHERE expression");
    }
}

#[test]
fn test_column_resolve_unqualified() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

    // Check WHERE clause resolved correctly
    if let Some(ResolvedWhereExpr::Binary(binary)) = &resolved.where_clause {
        if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*binary.lexpr {
            assert_eq!(col.schema, "public");
            assert_eq!(col.table, "users");
            assert_eq!(col.column, "id");
        } else {
            panic!("Expected column in binary expression");
        }
    } else {
        panic!("Expected binary WHERE expression");
    }
}

#[test]
fn test_column_resolve_ambiguous() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
#[test]
fn test_select_star_mixed_base_first() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "total"],
    ));

    let resolved = resolve_sql(
        "SELECT * FROM users u JOIN (SELECT id, total FROM orders) o USING (id)",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 3); // merged id, u.name, o.total
    assert_eq!(cols[0].alias.as_deref(), Some("id"));
    let base = select_column_node(&cols[1]);
    assert_eq!(base.table, "users");
    assert_eq!(base.column, "name");
    derived_column_assert(&cols[2], "o", "total");
}

/// PGC-359: mixed derived-then-base USING join — `*` expands in FROM
/// order (derived side's columns before the base table's).
#[test]
fn test_select_star_mixed_derived_first() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "total"],
    ));

    let resolved = resolve_sql(
        "SELECT * FROM (SELECT id, total FROM orders) o JOIN users u USING (id)",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 3); // merged id, o.total, u.name
    assert_eq!(cols[0].alias.as_deref(), Some("id"));
    derived_column_assert(&cols[1], "o", "total");
    let base = select_column_node(&cols[2]);
    assert_eq!(base.table, "users");
    assert_eq!(base.column, "name");
}

/// PGC-359: qualified `derived.*` expands that side verbatim — join
/// column included, no merged-column injection.
#[test]
fn test_select_qualified_star_derived() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "total"],
    ));

    let resolved = resolve_sql(
        "SELECT o.* FROM (SELECT id, total FROM orders) o JOIN users u USING (id)",
        &tables,
    );

    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 2); // o.id, o.total
    derived_column_assert(&cols[0], "o", "id");
    derived_column_assert(&cols[1], "o", "total");
}

/// PGC-359 (latent case): `*` over a single derived table expands to
/// the subquery's full output, not zero columns.
#[test]
fn test_select_star_single_derived() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.id",
        &tables,
    );

    // Check that JOIN was resolved
    assert_eq!(resolved.from.len(), 1);
    if let ResolvedTableSource::Join(join) = &resolved.from[0] {
        assert_eq!(join.join_type, JoinType::Inner);

        // Check left side
        if let ResolvedTableSource::Table(left) = &join.left {
            assert_eq!(left.name, "users");
        } else {
            panic!("Expected table on left side");
        }

        // Check right side
        if let ResolvedTableSource::Table(right) = &join.right {
            assert_eq!(right.name, "orders");
        } else {
            panic!("Expected table on right side");
        }

        // Check join condition
        if let Some(ResolvedWhereExpr::Binary(cond)) = join.predicate() {
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(left_col)) = &*cond.lexpr {
                assert_eq!(left_col.table, "users");
                assert_eq!(left_col.column, "id");
            }
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(right_col)) = &*cond.rexpr {
                assert_eq!(right_col.table, "orders");
                assert_eq!(right_col.column, "id");
            }
        } else {
            panic!("Expected binary join condition");
        }
    } else {
        panic!("Expected join source");
    }
}

#[test]
fn test_join_with_aliases() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT * FROM users u JOIN orders o ON u.id = o.id",
        &tables,
    );

    // Check that JOIN with aliases was resolved
    if let ResolvedTableSource::Join(join) = &resolved.from[0] {
        // Check left side has alias
        if let ResolvedTableSource::Table(left) = &join.left {
            assert_eq!(left.name, "users");
            assert_eq!(left.alias.as_deref(), Some("u"));
        } else {
            panic!("Expected table on left side");
        }

        // Check right side has alias
        if let ResolvedTableSource::Table(right) = &join.right {
            assert_eq!(right.name, "orders");
            assert_eq!(right.alias.as_deref(), Some("o"));
        } else {
            panic!("Expected table on right side");
        }

        // Check join condition uses aliases
        if let Some(ResolvedWhereExpr::Binary(cond)) = join.predicate() {
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(left_col)) = &*cond.lexpr {
                // Should resolve to 'users' table even though alias 'u' was used
                assert_eq!(left_col.table, "users");
                assert_eq!(left_col.column, "id");
            }
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(right_col)) = &*cond.rexpr {
                // Should resolve to 'orders' table even though alias 'o' was used
                assert_eq!(right_col.table, "orders");
                assert_eq!(right_col.column, "id");
            }
        }
    } else {
        panic!("Expected join source");
    }
}

#[test]
fn test_where_expr_complex() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id = 1 AND name = 'john'",
        &tables,
    );

    // Check that complex WHERE was resolved
    if let Some(ResolvedWhereExpr::Binary(and_expr)) = &resolved.where_clause {
        assert_eq!(and_expr.op, BinaryOp::And);

        // Left side: id = 1
        if let ResolvedWhereExpr::Binary(left_binary) = &*and_expr.lexpr {
            assert_eq!(left_binary.op, BinaryOp::Equal);
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*left_binary.lexpr
            {
                assert_eq!(col.column, "id");
            }
        } else {
            panic!("Expected binary expression on left");
        }

        // Right side: name = 'john'
        if let ResolvedWhereExpr::Binary(right_binary) = &*and_expr.rexpr {
            assert_eq!(right_binary.op, BinaryOp::Equal);
            if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = &*right_binary.lexpr
            {
                assert_eq!(col.column, "name");
            }
        } else {
            panic!("Expected binary expression on right");
        }
    } else {
        panic!("Expected binary WHERE expression");
    }
}

#[test]
fn test_order_by_simple() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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

    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
        position: 1,
        type_oid: 23,
        data_type: Type::INT4,
        type_name: "int4".into(),
        cache_type_name: "int4".into(),
        is_primary_key: true,
    }
}

#[test]
fn test_resolved_column_node_deparse_with_alias() {
    let mut buf = String::new();

    // Column with alias - should use alias
    ResolvedColumnNode {
        schema: "public".into(),
        table: "users".into(),
        table_alias: Some("u".into()),
        column: "id".into(),
        column_metadata: id_column_metadata(),
    }
    .deparse(&mut buf);
    assert_eq!(buf, "u.id");
}

#[test]
fn test_resolved_column_node_deparse_without_alias() {
    let mut buf = String::new();

    // Column without alias - should use schema.table
    ResolvedColumnNode {
        schema: "public".into(),
        table: "users".into(),
        table_alias: None,
        column: "id".into(),
        column_metadata: id_column_metadata(),
    }
    .deparse(&mut buf);
    assert_eq!(buf, "public.users.id");
}

#[test]
fn test_resolved_column_node_deparse_quoting() {
    let mut buf = String::new();

    // Column without alias - should use schema.table
    ResolvedColumnNode {
        schema: "Public".into(),
        table: "Users".into(),
        table_alias: None,
        column: "firstName".into(),
        column_metadata: id_column_metadata(),
    }
    .deparse(&mut buf);
    assert_eq!(buf, "\"Public\".\"Users\".\"firstName\"");
}

#[test]
fn test_resolved_table_node_deparse_with_alias() {
    let mut buf = String::new();

    ResolvedTableNode {
        schema: "public".into(),
        name: "users".into(),
        alias: Some("u".into()),
        relation_oid: Oid::from_raw(1001),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " public.users u");
}

#[test]
fn test_resolved_table_node_deparse_without_alias() {
    let mut buf = String::new();

    ResolvedTableNode {
        schema: "public".into(),
        name: "users".into(),
        alias: None,
        relation_oid: Oid::from_raw(1001),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " public.users");
}

#[test]
fn test_resolved_table_node_deparse_quoting() {
    let mut buf = String::new();

    ResolvedTableNode {
        schema: "Public".into(),
        name: "Users".into(),
        alias: None,
        relation_oid: Oid::from_raw(1001),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " \"Public\".\"Users\"");
}

#[test]
fn test_resolved_select_deparse_with_where() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    // SELECT * is expanded to explicit columns, table and column references are fully qualified
    assert_eq!(
        buf,
        "SELECT public.users.id, public.users.name FROM public.users WHERE public.users.id = 1"
    );
}

#[test]
fn test_resolved_select_deparse_with_alias() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT u.id, u.name FROM users u WHERE u.id = 1", &tables);

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    // With alias, uses alias.column
    assert_eq!(
        buf,
        "SELECT u.id, u.name FROM public.users u WHERE u.id = 1"
    );
}

#[test]
fn test_resolved_select_deparse_join() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT u.id, o.name FROM users u JOIN orders o ON u.id = o.id WHERE u.id = 1",
        &tables,
    );

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    assert_eq!(
        buf,
        "SELECT u.id, o.name FROM public.users u JOIN public.orders o ON u.id = o.id WHERE u.id = 1"
    );
}

#[test]
fn test_resolved_query_deparse_order_by() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_query("SELECT id FROM users u ORDER BY name DESC", &tables);

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    assert_eq!(buf, "SELECT u.id FROM public.users u ORDER BY u.name DESC");
}

#[test]
fn test_resolved_select_deparse_count_star() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT COUNT(*) FROM users WHERE id = 1", &tables);

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    assert_eq!(
        buf,
        "SELECT count(*) FROM public.users WHERE public.users.id = 1"
    );
}

#[test]
fn test_resolved_select_deparse_count_distinct() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT COUNT(DISTINCT name) FROM users WHERE id = 1",
        &tables,
    );

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    assert_eq!(
        buf,
        "SELECT count(DISTINCT public.users.name) FROM public.users WHERE public.users.id = 1"
    );
}

#[test]
fn test_resolved_select_deparse_case() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT CASE WHEN name = 'admin' THEN 1 ELSE 0 END FROM users WHERE id = 1",
        &tables,
    );

    let mut buf = String::new();
    resolved.deparse(&mut buf);

    assert_eq!(
        buf,
        "SELECT CASE WHEN public.users.name = 'admin' THEN 1 ELSE 0 END FROM public.users WHERE public.users.id = 1"
    );
}

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
fn test_complexity_single_table_no_where() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users", &tables);

    // Single table, no predicates = complexity 0
    assert_eq!(resolved.complexity(), 0);
}

#[test]
fn test_complexity_single_table_with_where() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

    // Single table, 1 predicate = complexity 1
    assert_eq!(resolved.complexity(), 1);
}

#[test]
fn test_complexity_single_table_multiple_predicates() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id = 1 AND name = 'john'",
        &tables,
    );

    // Single table, 2 predicates = complexity 2
    assert_eq!(resolved.complexity(), 2);
}

#[test]
fn test_complexity_join_no_where() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.id",
        &tables,
    );

    // 2 tables (1 join) * 3 = 3, no WHERE predicates
    assert_eq!(resolved.complexity(), 3);
}

#[test]
fn test_complexity_join_with_where() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT * FROM users JOIN orders ON users.id = orders.id WHERE users.id = 1",
        &tables,
    );

    // 2 tables (1 join) * 3 = 3, plus 1 WHERE predicate = 4
    assert_eq!(resolved.complexity(), 4);
}

#[test]
fn test_complexity_ordering() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("products", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("stores", Oid::from_raw(1002)));
    tables.insert_overwrite(test_table_metadata("regions", Oid::from_raw(1003)));

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
fn test_complexity_from_subquery_depth() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // FROM subquery: depth 1
    let from_sub = resolve_sql(
        "SELECT * FROM (SELECT * FROM users WHERE id = 1) sub",
        &tables,
    );
    assert_eq!(from_sub.subquery_depth(), 1);
}

#[test]
fn test_group_by_resolve_single_column() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT name FROM users GROUP BY name", &tables);

    assert_eq!(resolved.group_by.len(), 1);
    assert_eq!(resolved.group_by[0].schema, "public");
    assert_eq!(resolved.group_by[0].table, "users");
    assert_eq!(resolved.group_by[0].column, "name");
}

#[test]
fn test_group_by_resolve_multiple_columns() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT id, name FROM users GROUP BY id, name", &tables);

    assert_eq!(resolved.group_by.len(), 2);
    assert_eq!(resolved.group_by[0].column, "id");
    assert_eq!(resolved.group_by[1].column, "name");
}

#[test]
fn test_group_by_resolve_qualified_column() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql("SELECT u.name FROM users u GROUP BY u.name", &tables);

    assert_eq!(resolved.group_by.len(), 1);
    assert_eq!(resolved.group_by[0].table, "users");
    assert_eq!(resolved.group_by[0].table_alias.as_deref(), Some("u"));
    assert_eq!(resolved.group_by[0].column, "name");
}

#[test]
fn test_having_resolve() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
fn test_limit_resolve_count_only() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_query("SELECT * FROM users LIMIT 10", &tables);

    let limit = resolved.limit.unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
    assert_eq!(limit.offset, None);
}

#[test]
fn test_limit_resolve_offset_only() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_query("SELECT * FROM users OFFSET 5", &tables);

    let limit = resolved.limit.unwrap();
    assert_eq!(limit.count, None);
    assert_eq!(limit.offset, Some(LiteralValue::Integer(5)));
}

#[test]
fn test_limit_resolve_count_and_offset() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_query("SELECT * FROM users LIMIT 10 OFFSET 20", &tables);

    let limit = resolved.limit.unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
    assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
}

#[test]
fn test_limit_resolve_parameterized() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_query("SELECT * FROM users LIMIT $1 OFFSET $2", &tables);

    // Parameterized values are preserved through resolution
    let limit = resolved.limit.unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".into())));
    assert_eq!(limit.offset, Some(LiteralValue::Parameter("$2".into())));
}

#[test]
fn test_no_limit_clause() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_query("SELECT * FROM users", &tables);

    assert!(resolved.limit.is_none());
}

#[test]
fn test_combined_group_by_having_limit_resolve() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("active_users", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("items", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    // Parse a query with subquery without alias
    // Note: PostgreSQL parser typically requires alias, but we should still handle the error
    // gracefully if it somehow gets through
    let node = parse_select_node("SELECT * FROM (SELECT id FROM users) AS sub");

    // This should succeed since it has an alias
    let result = select_node_resolve(&node, &tables, &["public"]);
    assert!(result.is_ok());
}

#[test]
fn test_subquery_nodes_traversal() {
    // Test that nodes() traverses into subqueries to find all tables
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("active_users", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id IN (SELECT id FROM active_users)",
        &tables,
    );

    // Should find both outer table and inner table via nodes() traversal
    let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(
        table_nodes.len(),
        2,
        "Should find tables in both outer and inner query"
    );

    let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"users"), "Should find outer table");
    assert!(
        table_names.contains(&"active_users"),
        "Should find inner table"
    );
}

#[test]
fn test_subquery_nodes_traversal_derived_table() {
    // Test that nodes() traverses into FROM subqueries
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub",
        &tables,
    );

    // Should find the table inside the derived table
    let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(table_nodes.len(), 1, "Should find table in FROM subquery");
    assert_eq!(table_nodes[0].name, "users");
}

#[test]
fn test_subquery_nodes_traversal_scalar() {
    // Test that nodes() traverses into scalar subqueries in SELECT list
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1002)));

    let resolved = resolve_sql(
        "SELECT id, (SELECT COUNT(*) FROM users) AS user_count FROM orders WHERE id = 1",
        &tables,
    );

    // Should find both tables
    let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(
        table_nodes.len(),
        2,
        "Should find tables in outer and scalar subquery"
    );

    let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"orders"), "Should find outer table");
    assert!(
        table_names.contains(&"users"),
        "Should find scalar subquery table"
    );
}

#[test]
fn test_subquery_nodes_traversal_nested() {
    // Test that nodes() traverses into nested subqueries
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("a", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("b", Oid::from_raw(1002)));
    tables.insert_overwrite(test_table_metadata("c", Oid::from_raw(1003)));

    let resolved = resolve_sql(
        "SELECT * FROM a WHERE id IN (SELECT id FROM b WHERE id IN (SELECT id FROM c))",
        &tables,
    );

    // Should find all three tables
    let table_nodes: Vec<&ResolvedTableNode> = resolved.nodes().collect();
    assert_eq!(
        table_nodes.len(),
        3,
        "Should find all tables in nested subqueries"
    );

    let table_names: Vec<&str> = table_nodes.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"a"), "Should find outermost table");
    assert!(table_names.contains(&"b"), "Should find middle table");
    assert!(table_names.contains(&"c"), "Should find innermost table");
}

// ==========================================================================
// Direct Table Nodes Tests (population uses these, not nodes())
// ==========================================================================

#[test]
fn test_direct_table_nodes_excludes_where_subquery() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("active_users", Oid::from_raw(1002)));

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
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
fn test_correlated_exists_subquery_resolves() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("items", Oid::from_raw(1002)));

    let node = parse_select_node(
        "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.id = orders.id)",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("correlated EXISTS should resolve successfully");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert_eq!(outer_refs.len(), 1, "should have one outer ref");
    assert_eq!(outer_refs[0].table, "orders");
    assert_eq!(outer_refs[0].column, "id");
}

#[test]
fn test_correlated_in_subquery_resolves() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let node = parse_select_node(
        "SELECT * FROM users WHERE id IN (SELECT id FROM orders WHERE orders.name = users.name)",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("correlated IN should resolve successfully");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert_eq!(outer_refs.len(), 1, "should have one outer ref");
    assert_eq!(outer_refs[0].table, "users");
    assert_eq!(outer_refs[0].column, "name");
}

#[test]
fn test_correlated_scalar_subquery_resolves() {
    // Scalar correlated subquery in SELECT list
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let node = parse_select_node(
        "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.id = users.id) FROM users",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("correlated scalar subquery should resolve successfully");
    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("expected Columns");
    };
    let outer_refs = cols
        .iter()
        .find_map(|col| match &col.expr {
            ResolvedScalarExpr::Subquery(_, outer_refs) => Some(outer_refs),
            _ => None,
        })
        .expect("subquery column");
    assert_eq!(outer_refs.len(), 1, "should have one outer ref");
    assert_eq!(outer_refs[0].table, "users");
    assert_eq!(outer_refs[0].column, "id");
}

#[test]
fn test_correlated_subquery_with_alias_resolves() {
    // Table alias in outer scope should be resolved to the aliased table
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("orders", Oid::from_raw(1002)));

    let node = parse_select_node(
        "SELECT * FROM users u WHERE EXISTS (SELECT 1 FROM orders WHERE orders.id = u.id)",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("correlated subquery with alias should resolve successfully");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert_eq!(outer_refs.len(), 1, "should have one outer ref");
    assert_eq!(outer_refs[0].table, "users");
    assert_eq!(outer_refs[0].column, "id");
}

#[test]
fn test_correlated_unqualified_column_in_where() {
    // `email` only exists on `users`, not `orders` — bare `email` in the subquery
    // is an implicit correlated reference resolved via outer scope fallback
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "users",
        Oid::from_raw(1001),
        &["id", "email"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "user_id", "total"],
    ));

    let node = parse_select_node(
        "SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE email = 'test@example.com')",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("unqualified outer column should resolve successfully");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert_eq!(outer_refs.len(), 1, "should have one outer ref");
    assert_eq!(outer_refs[0].table, "users");
    assert_eq!(outer_refs[0].column, "email");
}

#[test]
fn test_correlated_unqualified_column_in_select_list() {
    // `id` exists in both `users` and `orders` — resolves to inner scope (non-correlated)
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "users",
        Oid::from_raw(1001),
        &["id", "email"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "user_id"],
    ));

    let node = parse_select_node(
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM orders WHERE user_id = id)",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("column present in both scopes should resolve to inner scope");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    // `id` resolves to `orders.id` in the inner scope — not a correlated reference
    assert!(
        outer_refs.is_empty(),
        "inner-scope column should not appear in outer_refs"
    );
}

#[test]
fn test_correlated_unqualified_column_scalar_subquery() {
    // `status` only exists on `users`, bare reference in SELECT-list scalar subquery
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "users",
        Oid::from_raw(1001),
        &["id", "status"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "orders",
        Oid::from_raw(1002),
        &["id", "amount"],
    ));

    let node = parse_select_node(
        "SELECT id, (SELECT COUNT(*) FROM orders WHERE status = 'active') FROM users",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("unqualified outer column in scalar subquery should resolve");
    let ResolvedSelectColumns::Columns(cols) = &resolved.columns else {
        panic!("expected Columns");
    };
    let outer_refs = cols
        .iter()
        .find_map(|col| match &col.expr {
            ResolvedScalarExpr::Subquery(_, outer_refs) => Some(outer_refs),
            _ => None,
        })
        .expect("subquery column");
    assert_eq!(outer_refs.len(), 1, "should have one outer ref");
    assert_eq!(outer_refs[0].table, "users");
    assert_eq!(outer_refs[0].column, "status");
}

#[test]
fn test_non_correlated_subquery_has_empty_outer_refs() {
    // Non-correlated subquery should have outer_refs: []
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));
    tables.insert_overwrite(test_table_metadata("active_users", Oid::from_raw(1002)));

    let node = parse_select_node("SELECT * FROM users WHERE id IN (SELECT id FROM active_users)");
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("non-correlated subquery should resolve successfully");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert!(
        outer_refs.is_empty(),
        "non-correlated subquery must have empty outer_refs"
    );
}

#[test]
fn test_correlated_mixed_inner_and_outer_columns() {
    // Same predicate references both an inner-scope column and an outer-scope column.
    // The inner column resolves normally; the outer column goes into outer_refs.
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata_with_columns(
        "departments",
        Oid::from_raw(1001),
        &["id", "region"],
    ));
    tables.insert_overwrite(test_table_metadata_with_columns(
        "employees",
        Oid::from_raw(1002),
        &["id", "dept_id", "region"],
    ));

    // `employees.dept_id = departments.id` — dept_id is inner, departments.id is outer
    let node = parse_select_node(
        "SELECT d.id FROM departments d \
         WHERE EXISTS (SELECT 1 FROM employees WHERE dept_id = d.id)",
    );
    let result = select_node_resolve(&node, &tables, &["public"]);

    let resolved = result.expect("mixed inner/outer predicate should resolve");
    let Some(ResolvedWhereExpr::Subquery { outer_refs, .. }) = &resolved.where_clause else {
        panic!("expected Subquery WHERE");
    };
    assert_eq!(
        outer_refs.len(),
        1,
        "only the outer-scope column should be in outer_refs"
    );
    assert_eq!(outer_refs[0].table, "departments");
    assert_eq!(outer_refs[0].column, "id");
}

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
fn test_having_filter_resolves() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT name FROM users GROUP BY name \
         HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
        &tables,
    );

    let func = resolved_having_lhs_function(&resolved);
    assert_eq!(func.name, "count");
    assert!(func.agg_star);
    assert!(
        func.agg_filter.is_some(),
        "FILTER (WHERE ...) must survive resolution"
    );
}

#[test]
fn test_having_distinct_resolves() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1",
        &tables,
    );

    let func = resolved_having_lhs_function(&resolved);
    assert!(func.agg_distinct, "DISTINCT must survive resolution");
}

#[test]
fn test_having_aggregate_order_by_resolves() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

    let resolved = resolve_sql(
        "SELECT id FROM users GROUP BY id \
         HAVING string_agg(name, ',' ORDER BY name) <> ''",
        &tables,
    );

    let func = resolved_having_lhs_function(&resolved);
    assert_eq!(func.name, "string_agg");
    assert!(
        !func.agg_order.is_empty(),
        "aggregate ORDER BY must survive resolution"
    );
}

#[test]
fn test_having_filter_resolved_deparse_contains_filter() {
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(test_table_metadata("users", Oid::from_raw(1001)));

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
