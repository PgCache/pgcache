#![allow(clippy::wildcard_enum_match_arm)]

use std::{collections::HashSet, panic};

use ecow::EcoString;
use ordered_float::NotNan;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::query::ast::*;

/// Parse SQL and return a QueryExpr (new type)
fn parse_query(sql: &str) -> QueryExpr {
    query_expr_parse(sql).expect("convert to QueryExpr")
}

/// Parse SQL and return a SelectNode (for tests that need direct access)
fn parse_select(sql: &str) -> SelectNode {
    let query = parse_query(sql);
    match query.body {
        QueryBody::Select(node) => *node,
        _ => panic!("expected SELECT"),
    }
}

/// The query's deparsed SQL.
fn deparsed(sql: &str) -> String {
    deparsed_node(&parse_query(sql))
}

fn deparsed_node(node: &impl Deparse) -> String {
    let mut buf = String::new();
    node.deparse(&mut buf);
    buf
}

/// The SELECT list, which the test expects to be explicit columns.
fn select_columns(select: &SelectNode) -> &[SelectColumn] {
    let SelectColumns::Columns(columns) = &select.columns else {
        panic!("expected columns, got {:?}", select.columns);
    };
    columns
}

/// Every (schema, name) table reference in the query.
fn table_refs(ast: &QueryExpr) -> HashSet<(Option<&str>, &str)> {
    ast.nodes::<TableNode>()
        .map(|t| (t.schema.as_deref(), t.name.as_str()))
        .collect()
}

fn only_table(select: &SelectNode) -> &TableNode {
    let [TableSource::Table(table)] = select.from.as_slice() else {
        panic!("expected a single table source, got {:?}", select.from);
    };
    table
}

fn only_join(select: &SelectNode) -> &JoinNode {
    let [TableSource::Join(join)] = select.from.as_slice() else {
        panic!("expected a single join, got {:?}", select.from);
    };
    join
}

/// The (qualifier, column) of every SELECT-list column reference.
fn column_refs(select: &SelectNode) -> Vec<(Option<&str>, &str)> {
    select_columns(select)
        .iter()
        .map(|col| match col.expr() {
            Some(ScalarExpr::Column(c)) => (c.table.as_deref(), c.column.as_str()),
            other => panic!("expected a column reference, got {other:?}"),
        })
        .collect()
}

/// The function call `sql` selects as its only column.
fn only_function(sql: &str) -> FunctionCall {
    let select = parse_select(sql);
    let [column] = select_columns(&select) else {
        panic!("expected exactly one column in {sql}");
    };
    let Some(ScalarExpr::Function(func)) = column.expr() else {
        panic!("expected a function in {sql}, got {column:?}");
    };
    func.clone()
}

fn nested_function_name(arg: &ScalarExpr) -> Option<&str> {
    let ScalarExpr::Function(inner) = arg else {
        return None;
    };
    Some(inner.name.as_str())
}

#[test]
fn test_query_expr_convert_simple_select() {
    let ast = parse_query("SELECT id, name FROM users WHERE id = 1");
    assert!(ast.is_single_table());
    assert!(ast.has_where_clause());
    assert_eq!(table_refs(&ast), HashSet::from([(None, "users")]));
}

/// PGC-183: `SELECT $1 FROM …` must parse — the SELECT-list converter
/// previously rejected ParamRef even though `node_convert_to_scalar_expr`
/// already supported it for WHERE-side use.
#[test]
fn test_query_expr_convert_select_paramref() {
    let select = parse_select("SELECT $1 FROM users");
    let SelectColumns::Columns(cols) = &select.columns else {
        panic!("expected explicit columns");
    };
    assert_eq!(cols.len(), 1);
    let SelectColumn::Expr { expr, .. } = &cols[0] else {
        panic!("expected scalar column expression");
    };
    assert!(
        matches!(expr, ScalarExpr::Literal(LiteralValue::Parameter(p)) if p == "$1"),
        "expected ScalarExpr::Literal(Parameter('$1')), got {expr:?}",
    );
}

#[test]
fn test_query_expr_convert_select_star() {
    let ast = parse_query("SELECT * FROM products");
    assert!(ast.is_single_table());
    assert!(!ast.has_where_clause());
    assert_eq!(table_refs(&ast), HashSet::from([(None, "products")]));
}

#[test]
fn test_query_expr_convert_where_clause() {
    let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND active = true");

    assert!(ast.has_where_clause());
    let where_clause = ast.where_clause().unwrap();

    // Should convert the same WHERE clause as before
    // (reusing existing WhereExpr conversion)
    assert!(matches!(where_clause, WhereExpr::Binary(_)));
}

#[test]
fn test_query_expr_convert_table_schema() {
    let select = parse_select("SELECT id, name FROM test.users WHERE active = true");
    let table = only_table(&select);
    assert_eq!(table.schema, Some(EcoString::from("test")));
    assert_eq!(table.name, "users");
    assert_eq!(table.alias, None);
    assert_eq!(column_refs(&select), [(None, "id"), (None, "name")]);
}

#[test]
fn test_query_expr_convert_table_alias() {
    let select = parse_select("SELECT u.id, u.name FROM users u WHERE u.active = true");
    let table = only_table(&select);
    assert_eq!(table.name, "users");
    let alias = table.alias.as_ref().expect("table alias");
    assert_eq!(alias.name, "u");
    assert!(alias.columns.is_empty());
    assert_eq!(
        column_refs(&select),
        [(Some("u"), "id"), (Some("u"), "name")]
    );
}

#[test]
fn test_query_expr_convert_column_alias() {
    let select = parse_select("SELECT id as user_id, name as full_name FROM users");

    if let SelectColumns::Columns(columns) = &select.columns {
        assert_eq!(columns.len(), 2);

        // First column: id as user_id
        assert_eq!(
            columns[0].alias().cloned(),
            Some(EcoString::from("user_id"))
        );
        if let ScalarExpr::Column(col_ref) = &columns[0].expr().expect("non-star SELECT column") {
            assert_eq!(col_ref.column, "id");
        }

        // Second column: name as full_name
        assert_eq!(
            columns[1].alias().cloned(),
            Some(EcoString::from("full_name"))
        );
        if let ScalarExpr::Column(col_ref) = &columns[1].expr().expect("non-star SELECT column") {
            assert_eq!(col_ref.column, "name");
        }
    }
}

#[test]
fn test_query_expr_convert_no_alias() {
    let select = parse_select("SELECT id, name FROM users");

    assert_eq!(select.from.len(), 1);

    let TableSource::Table(table) = &select.from[0] else {
        panic!("expected table");
    };

    // Table should have no alias
    assert_eq!(table.alias, None);

    // Columns should have no aliases
    if let SelectColumns::Columns(columns) = &select.columns {
        assert_eq!(columns[0].alias().cloned(), None);
        assert_eq!(columns[1].alias().cloned(), None);
    }
}

#[test]
fn test_query_expr_join() {
    let ast = parse_query("SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id");

    assert_eq!(
        ast.nodes::<TableNode>()
            .map(|t| (t.schema.as_deref(), t.name.as_str(), t.alias.as_ref()))
            .collect::<HashSet<_>>(),
        HashSet::<(Option<&str>, _, _)>::from([
            (None, "invoice", None),
            (
                None,
                "product",
                Some(&TableAlias {
                    name: EcoString::from("p"),
                    columns: Vec::new()
                })
            )
        ])
    );
}

#[test]
fn test_query_expr_multiple_joins_two_tables() {
    let ast =
        parse_query("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1");

    // Should parse successfully with three tables
    let tables: Vec<&TableNode> = ast.nodes().collect();
    assert_eq!(tables.len(), 3);
    assert_eq!(tables[0].name, "a");
    assert_eq!(tables[1].name, "b");
    assert_eq!(tables[2].name, "c");

    // Should have 2 join nodes (nested structure)
    let joins: Vec<&JoinNode> = ast.nodes().collect();
    assert_eq!(joins.len(), 2);
    assert_eq!(joins[0].join_type, JoinType::Inner);
    assert_eq!(joins[1].join_type, JoinType::Inner);

    // Verify each join's condition contains the expected columns
    // Note: We count each join's condition separately to avoid double-counting
    // columns from nested joins (since joins can be nested in the AST)
    let mut col_count = 0;
    for join in &joins {
        if let JoinQual::On(condition) = &join.qual {
            col_count += condition.nodes::<ColumnNode>().count();
        }
    }
    assert_eq!(col_count, 4); // a.id, b.id from first join + b.id, c.id from second join
}

#[test]
fn test_query_expr_multiple_joins_three_tables() {
    let ast = parse_query(
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id",
    );

    // Should parse successfully with four tables
    let tables: Vec<&TableNode> = ast.nodes().collect();
    assert_eq!(tables.len(), 4);
    assert_eq!(tables[0].name, "a");
    assert_eq!(tables[1].name, "b");
    assert_eq!(tables[2].name, "c");
    assert_eq!(tables[3].name, "d");

    // Should have 3 join nodes (deeply nested structure)
    let joins: Vec<&JoinNode> = ast.nodes().collect();
    assert_eq!(joins.len(), 3);
    assert!(joins.iter().all(|j| j.join_type == JoinType::Inner));
}

#[test]
fn test_query_expr_mixed_join_types() {
    let ast = parse_query(
        "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id",
    );

    // Should parse successfully with three tables
    let tables: Vec<&TableNode> = ast.nodes().collect();
    assert_eq!(tables.len(), 3);
    assert_eq!(tables[0].name, "users");
    assert_eq!(tables[1].name, "orders");
    assert_eq!(tables[2].name, "payments");

    // Should have 2 join nodes with different types
    // Note: Joins are returned in traversal order (outer join first, then nested joins)
    // For this query, PostgreSQL creates: (users INNER JOIN orders) LEFT JOIN payments
    // So the outer LEFT join is returned first, then the inner INNER join
    let joins: Vec<&JoinNode> = ast.nodes().collect();
    assert_eq!(joins.len(), 2);
    assert_eq!(joins[0].join_type, JoinType::Left); // Outer join
    assert_eq!(joins[1].join_type, JoinType::Inner); // Nested join
}

#[test]
fn test_query_expr_multiple_joins_deparse() {
    let ast = parse_query("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id");

    let buf = deparsed_node(&ast);

    // Parse the deparsed SQL to verify it's valid
    let ast2 = parse_query(&buf);

    // Should produce identical AST
    assert_eq!(ast, ast2);
}

#[test]
fn test_query_expr_select_subquery() {
    let select =
        parse_select("SELECT invoice.id, (SELECT x.data FROM x WHERE 1 = 1) as one FROM invoice");

    let SelectColumns::Columns(columns) = &select.columns else {
        panic!();
    };

    assert_eq!(
        columns[0],
        SelectColumn::Expr {
            expr: ScalarExpr::Column(ColumnNode {
                table: Some(EcoString::from("invoice")),
                column: EcoString::from("id")
            }),
            alias: None,
        }
    );

    assert!(matches!(
        columns[1].expr().expect("non-star SELECT column"),
        ScalarExpr::Subquery(_)
    ));
    assert_eq!(columns[1].alias().cloned(), Some(EcoString::from("one")));
}

#[test]
fn test_query_expr_table_subquery() {
    let select = parse_select("SELECT * FROM (SELECT * FROM invoice WHERE id = 2) inv");

    assert_eq!(select.from.len(), 1);

    let TableSource::Subquery(subquery) = &select.from[0] else {
        panic!("expected subquery");
    };

    assert!(!subquery.lateral);
    assert_eq!(subquery.alias.as_ref().unwrap().name, "inv");
    assert!(subquery.alias.as_ref().unwrap().columns.is_empty());
}

#[test]
fn test_query_expr_values() {
    let select = parse_select("SELECT * FROM (VALUES(1, 2, 'test'), (3, 4, 'a')) v");

    assert_eq!(select.from.len(), 1);

    let TableSource::Subquery(subquery) = &select.from[0] else {
        panic!("expected subquery");
    };

    assert!(!subquery.lateral);
    assert_eq!(subquery.alias.as_ref().unwrap().name, "v");

    let QueryBody::Values(values_clause) = &subquery.query.body else {
        panic!("expected VALUES clause in subquery");
    };
    assert_eq!(values_clause.rows.len(), 2);
    assert_eq!(values_clause.rows[0].len(), 3);
    assert_eq!(values_clause.rows[1].len(), 3);
}

#[test]
fn test_literal_value_deparse() {
    let mut buf = String::new();

    // String literal
    LiteralValue::String("hello".into()).deparse(&mut buf);
    assert_eq!(buf, "'hello'");
    buf.clear();

    // Integer literal
    LiteralValue::Integer(42).deparse(&mut buf);
    assert_eq!(buf, "42");
    buf.clear();

    // Float literal
    LiteralValue::Float(NotNan::new(3.25).unwrap()).deparse(&mut buf);
    assert_eq!(buf, "3.25");
    buf.clear();

    // Boolean literals
    LiteralValue::Boolean(true).deparse(&mut buf);
    assert_eq!(buf, "true");
    buf.clear();

    LiteralValue::Boolean(false).deparse(&mut buf);
    assert_eq!(buf, "false");
    buf.clear();

    // NULL literal
    LiteralValue::Null.deparse(&mut buf);
    assert_eq!(buf, "NULL");
    buf.clear();

    // Parameter
    LiteralValue::Parameter("$1".into()).deparse(&mut buf);
    assert_eq!(buf, "$1");
}

#[test]
fn test_table_node_deparse() {
    let mut buf = String::new();

    // Simple table
    TableNode {
        schema: None,
        name: EcoString::from("users"),
        alias: None,
    }
    .deparse(&mut buf);
    assert_eq!(buf, " users");
    buf.clear();

    // Qualified table with alias
    TableNode {
        schema: Some(EcoString::from("public")),
        name: EcoString::from("users"),
        alias: Some(TableAlias {
            name: EcoString::from("alias"),
            columns: vec![],
        }),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " public.users alias");
    buf.clear();

    // table requires quoting
    TableNode {
        schema: Some(EcoString::from("public")),
        name: EcoString::from("userAccount"),
        alias: Some(TableAlias {
            name: EcoString::from("usrAcc"),
            columns: vec![],
        }),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " public.\"userAccount\" \"usrAcc\"");
}

#[test]
fn test_column_ref_deparse() {
    let mut buf = String::new();

    // Simple column
    ColumnNode {
        table: None,
        column: EcoString::from("id"),
    }
    .deparse(&mut buf);
    assert_eq!(buf, "id");
    buf.clear();

    // Qualified column
    ColumnNode {
        table: Some(EcoString::from("users")),
        column: EcoString::from("name"),
    }
    .deparse(&mut buf);
    assert_eq!(buf, "users.name");
    buf.clear();

    // table and column require quoting
    ColumnNode {
        table: Some(EcoString::from("Users")),
        column: EcoString::from("firstName"),
    }
    .deparse(&mut buf);
    assert_eq!(buf, "\"Users\".\"firstName\"");
}

#[test]
fn test_select_column_alias_deparse() {
    let mut buf = String::new();

    // Simple column
    SelectColumn::Expr {
        expr: ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("id"),
        }),
        alias: Some(EcoString::from("alias")),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " id AS alias");
    buf.clear();

    // Qualified column
    SelectColumn::Expr {
        expr: ScalarExpr::Column(ColumnNode {
            table: Some(EcoString::from("users")),
            column: EcoString::from("name"),
        }),
        alias: Some(EcoString::from("alias")),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " users.name AS alias");
    buf.clear();

    // alias requires quoting
    SelectColumn::Expr {
        expr: ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("id"),
        }),
        alias: Some(EcoString::from("Alias")),
    }
    .deparse(&mut buf);
    assert_eq!(buf, " id AS \"Alias\"");
}

#[test]
fn test_unary_op_deparse() {
    let mut buf = String::new();

    UnaryOp::Not.deparse(&mut buf);
    assert_eq!(buf, "NOT");
    buf.clear();

    UnaryOp::IsNull.deparse(&mut buf);
    assert_eq!(buf, "IS NULL");
    buf.clear();

    UnaryOp::IsNotNull.deparse(&mut buf);
    assert_eq!(buf, "IS NOT NULL");
    buf.clear();

    UnaryOp::IsTrue.deparse(&mut buf);
    assert_eq!(buf, "IS TRUE");
    buf.clear();

    UnaryOp::IsNotTrue.deparse(&mut buf);
    assert_eq!(buf, "IS NOT TRUE");
    buf.clear();

    UnaryOp::IsFalse.deparse(&mut buf);
    assert_eq!(buf, "IS FALSE");
    buf.clear();

    UnaryOp::IsNotFalse.deparse(&mut buf);
    assert_eq!(buf, "IS NOT FALSE");
}

#[test]
fn test_binary_op_deparse() {
    let mut buf = String::new();

    // Comparison operators
    BinaryOp::Equal.deparse(&mut buf);
    assert_eq!(buf, "=");
    buf.clear();

    BinaryOp::NotEqual.deparse(&mut buf);
    assert_eq!(buf, "!=");
    buf.clear();

    BinaryOp::LessThan.deparse(&mut buf);
    assert_eq!(buf, "<");
    buf.clear();

    BinaryOp::GreaterThanOrEqual.deparse(&mut buf);
    assert_eq!(buf, ">=");
    buf.clear();

    // Pattern matching
    BinaryOp::Like.deparse(&mut buf);
    assert_eq!(buf, "LIKE");
    buf.clear();

    BinaryOp::NotLike.deparse(&mut buf);
    assert_eq!(buf, "NOT LIKE");
    buf.clear();

    // Logical operators
    BinaryOp::And.deparse(&mut buf);
    assert_eq!(buf, "AND");
    buf.clear();

    BinaryOp::Or.deparse(&mut buf);
    assert_eq!(buf, "OR");
}

#[test]
fn test_multi_op_deparse() {
    let mut buf = String::new();

    MultiOp::In.deparse(&mut buf);
    assert_eq!(buf, "IN");
    buf.clear();

    MultiOp::NotIn.deparse(&mut buf);
    assert_eq!(buf, "NOT IN");
}

#[test]
fn test_binary_expr_deparse() {
    let mut buf = String::new();

    // Simple equality: id = 1
    let expr = BinaryExpr {
        op: BinaryOp::Equal,
        lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("id"),
        }))),
        rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
            LiteralValue::Integer(1),
        ))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "id = 1");
    buf.clear();

    // Complex expression: users.name = 'john'
    let expr = BinaryExpr {
        op: BinaryOp::Equal,
        lexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: Some(EcoString::from("users")),
            column: EcoString::from("name"),
        }))),
        rexpr: Box::new(WhereExpr::Scalar(ScalarExpr::Literal(
            LiteralValue::String("john".into()),
        ))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "users.name = 'john'");
}

#[test]
fn test_unary_expr_deparse() {
    let mut buf = String::new();

    // NOT active (prefix operator)
    let expr = UnaryExpr {
        op: UnaryOp::Not,
        expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("active"),
        }))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "NOT active");
    buf.clear();

    // deleted_at IS NULL (postfix operator)
    let expr = UnaryExpr {
        op: UnaryOp::IsNull,
        expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("deleted_at"),
        }))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "deleted_at IS NULL");
    buf.clear();

    // name IS NOT NULL (postfix operator)
    let expr = UnaryExpr {
        op: UnaryOp::IsNotNull,
        expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("name"),
        }))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "name IS NOT NULL");
    buf.clear();

    // active IS TRUE (postfix operator)
    let expr = UnaryExpr {
        op: UnaryOp::IsTrue,
        expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("active"),
        }))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "active IS TRUE");
    buf.clear();

    // active IS NOT FALSE (postfix operator)
    let expr = UnaryExpr {
        op: UnaryOp::IsNotFalse,
        expr: Box::new(WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("active"),
        }))),
    };

    expr.deparse(&mut buf);
    assert_eq!(buf, "active IS NOT FALSE");
}

#[test]
fn test_round_trip() {
    fn round_trip(sql: &str) {
        // Parse original
        let ast1 = query_expr_parse(sql).unwrap();

        // Deparse to string
        let deparsed = deparsed_node(&ast1);

        // Parse deparsed version
        let ast2 = query_expr_parse(&deparsed).unwrap();

        // Should be equivalent
        assert_eq!(ast1, ast2);
    }

    round_trip("SELECT id, name FROM users WHERE active = true");
    round_trip(
        "SELECT id, name \
        FROM users JOIN address a on a.is = users.address_id \
        WHERE active = true",
    );
    round_trip(
        "SELECT id, name \
        FROM (SELECT * FROM users) u \
        WHERE active = true",
    );
}

#[test]
fn test_literal_empty_string() {
    let mut buf = String::new();
    LiteralValue::String("".into()).deparse(&mut buf);
    assert_eq!(buf, "''");
}

#[test]
fn test_literal_string_with_quotes() {
    let mut buf = String::new();
    LiteralValue::String("test'quote".into()).deparse(&mut buf);
    // postgres-protocol should properly escape the quote
    assert_eq!(buf, "'test''quote'");
}

#[test]
fn test_literal_string_with_backslashes() {
    let mut buf = String::new();
    LiteralValue::String("test\\path".into()).deparse(&mut buf);
    // postgres-protocol should use E'' syntax for backslashes
    assert_eq!(buf, "E'test\\\\path'");
}

#[test]
fn test_nodes_table_extraction() {
    let ast = parse_query(
        "SELECT first_name, last_name, film_id \
                FROM actor a \
                JOIN public.film_actor fa ON a.actor_id = fa.actor_id \
                WHERE a.actor_id = 1",
    );

    // Test extracting TableNode instances using the generic nodes function
    let tables = ast.nodes::<TableNode>().collect::<Vec<_>>();

    assert_eq!(tables.len(), 2);

    assert_eq!(tables[0].schema, None);
    assert_eq!(tables[0].name, "actor");
    assert_eq!(
        tables[0].alias,
        Some(TableAlias {
            name: EcoString::from("a"),
            columns: vec![]
        })
    );

    assert_eq!(tables[1].schema, Some(EcoString::from("public")));
    assert_eq!(tables[1].name, "film_actor");
    assert_eq!(
        tables[1].alias,
        Some(TableAlias {
            name: EcoString::from("fa"),
            columns: vec![]
        })
    );
}

#[test]
fn test_nodes_table_nodes() {
    let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

    // Test getting TableNode instances using the nodes function
    let table_nodes: Vec<&TableNode> = ast.nodes().collect();
    assert_eq!(table_nodes.len(), 2);

    assert_eq!(table_nodes[0].name, "users");
    assert_eq!(table_nodes[0].alias.as_ref().unwrap().name, "u");

    assert_eq!(table_nodes[1].name, "orders");
    assert_eq!(table_nodes[1].alias.as_ref().unwrap().name, "o");
}

#[test]
fn test_nodes_join_nodes() {
    let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

    // Test getting JoinNode instances using the nodes function
    let join_nodes: Vec<&JoinNode> = ast.nodes().collect();
    assert_eq!(join_nodes.len(), 1);

    assert_eq!(join_nodes[0].join_type, JoinType::Inner);
    assert!(matches!(join_nodes[0].qual, JoinQual::On(_)));
}

#[test]
fn test_nodes_mixed_types() {
    let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

    // Should find both table nodes and join nodes independently
    let table_nodes: Vec<&TableNode> = ast.nodes().collect();
    let join_nodes: Vec<&JoinNode> = ast.nodes().collect();

    assert_eq!(table_nodes.len(), 2);
    assert_eq!(join_nodes.len(), 1);

    // Verify the types are different queries but same AST
    assert_eq!(table_nodes[0].name, "users");
    assert_eq!(table_nodes[1].name, "orders");
    assert_eq!(join_nodes[0].join_type, JoinType::Inner);
}

#[test]
fn test_where_expr_nodes_column() {
    let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND age > 25");

    let where_clause = ast.where_clause().unwrap();
    let columns: Vec<&ColumnNode> = where_clause.nodes().collect();

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].column, "name");
    assert_eq!(columns[1].column, "age");
}

#[test]
fn test_where_expr_nodes_literal() {
    let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND age > 25 AND active = true");

    let where_clause = ast.where_clause().unwrap();
    let literals: Vec<&LiteralValue> = where_clause.nodes().collect();

    assert_eq!(literals.len(), 3);
    assert_eq!(literals[0], &LiteralValue::String("john".into()));
    assert_eq!(literals[1], &LiteralValue::Integer(25));
    assert_eq!(literals[2], &LiteralValue::Boolean(true));
}

#[test]
fn test_where_expr_nodes_binary() {
    let ast = parse_query("SELECT * FROM users WHERE name = 'john' AND age > 25");

    let where_clause = ast.where_clause().unwrap();
    let binary_exprs: Vec<&BinaryExpr> = where_clause.nodes().collect();

    assert_eq!(binary_exprs.len(), 3); // AND, =, >
    assert_eq!(binary_exprs[0].op, BinaryOp::And);
    assert_eq!(binary_exprs[1].op, BinaryOp::Equal);
    assert_eq!(binary_exprs[2].op, BinaryOp::GreaterThan);
}

#[test]
fn test_where_expr_nodes_whole_expr() {
    let ast = parse_query("SELECT * FROM users WHERE name = 'john'");

    let where_clause = ast.where_clause().unwrap();
    let where_exprs: Vec<&WhereExpr> = where_clause.nodes().collect();

    // Should find the root expression plus all child expressions
    assert_eq!(where_exprs.len(), 3); // Binary(name = 'john'), Column(name), Value('john')
}

#[test]
fn test_where_expr_nodes_nested() {
    let ast =
        parse_query("SELECT * FROM users WHERE (name = 'john' OR name = 'jane') AND age > 18");

    let where_clause = ast.where_clause().unwrap();
    let columns: Vec<&ColumnNode> = where_clause.nodes().collect();

    // Should find all column references in nested structure
    assert_eq!(columns.len(), 3); // name, name, age
    assert_eq!(columns[0].column, "name");
    assert_eq!(columns[1].column, "name");
    assert_eq!(columns[2].column, "age");
}

#[test]
fn test_join_condition_nodes() {
    let ast = parse_query("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");

    // Get the join node from the query
    let join_nodes: Vec<&JoinNode> = ast.nodes().collect();
    assert_eq!(join_nodes.len(), 1);

    let join_node = join_nodes[0];

    // Test that we can extract column nodes from the join condition
    let columns: Vec<&ColumnNode> = join_node.nodes().collect();

    // Should find only the condition columns (u.id, o.user_id)
    // since we're specifically collecting ColumnNode instances
    assert_eq!(columns.len(), 2);

    // Verify the condition columns
    assert_eq!(columns[0].table, Some(EcoString::from("u")));
    assert_eq!(columns[0].column, "id");
    assert_eq!(columns[1].table, Some(EcoString::from("o")));
    assert_eq!(columns[1].column, "user_id");
}

#[test]
fn test_query_body_nodes() {
    let ast = parse_query("SELECT * FROM users WHERE id = 1");

    // Test that QueryBody::nodes() delegates to SelectNode
    let tables: Vec<&TableNode> = ast.body.nodes().collect();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "users");
}

#[test]
fn test_select_columns_nodes() {
    let ast = parse_query("SELECT id, name FROM users");

    // Test that we can extract ColumnNode through SelectColumns
    let columns: Vec<&ColumnNode> = ast.nodes().collect();
    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0].column, "id");
    assert_eq!(columns[1].column, "name");
}

#[test]
fn test_scalar_expr_nodes() {
    let ast = parse_query("SELECT id, name FROM users WHERE active = true");

    // Test that ScalarExpr::nodes() can traverse through to ColumnNode
    let columns: Vec<&ColumnNode> = ast.nodes().collect();
    assert_eq!(columns.len(), 3); // id, name, active
    assert_eq!(columns[0].column, "id");
    assert_eq!(columns[1].column, "name");
    assert_eq!(columns[2].column, "active");
}

#[test]
fn test_function_call_nodes() {
    let func = FunctionCall {
        name: EcoString::from("COUNT"),
        args: vec![ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("id"),
        })],
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    };

    // Test that FunctionCall::nodes() can extract ColumnNode from args
    let columns: Vec<&ColumnNode> = func.nodes().collect();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].column, "id");
}

#[test]
fn test_table_node_nodes() {
    let ast = parse_query("SELECT * FROM public.users");

    // Test that TableNode::nodes() returns itself as a leaf node
    let tables: Vec<&TableNode> = ast.nodes().collect();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].schema, Some(EcoString::from("public")));
    assert_eq!(tables[0].name, "users");
}

#[test]
fn test_table_subquery_node_nodes() {
    let ast = parse_query("SELECT * FROM (SELECT id FROM users) sub");

    // try_for_each_node traverses into subqueries
    let subqueries: Vec<&TableSubqueryNode> = ast.nodes().collect();
    assert_eq!(subqueries.len(), 1);

    // Inner table is also reachable
    let tables: Vec<&TableNode> = ast.nodes().collect();
    assert_eq!(tables.len(), 1);
    assert_eq!(tables[0].name, "users");
}

#[test]
fn test_scalar_expr_subquery_nodes() {
    // Scalar subquery in SELECT list: ScalarExpr::Subquery should traverse into inner query
    let ast = parse_query("SELECT id, (SELECT COUNT(*) FROM orders) FROM users");

    // Should find both tables: users (FROM) and orders (inside scalar subquery)
    let tables: Vec<&TableNode> = ast.nodes().collect();
    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(
        table_names.contains(&"users"),
        "should find outer table 'users'"
    );
    assert!(
        table_names.contains(&"orders"),
        "should find inner table 'orders' via ScalarExpr::Subquery traversal"
    );
    assert_eq!(tables.len(), 2);
}

#[test]
fn test_unary_expr_nodes() {
    let ast = parse_query("SELECT * FROM users WHERE NOT active");

    // Test that UnaryExpr::nodes() returns itself
    let unary_exprs: Vec<&UnaryExpr> = ast.nodes().collect();
    assert_eq!(unary_exprs.len(), 1);
    assert_eq!(unary_exprs[0].op, UnaryOp::Not);
}

#[test]
fn test_binary_expr_nodes() {
    let ast = parse_query("SELECT * FROM users WHERE name = 'john'");

    // Test that BinaryExpr::nodes() returns itself
    let binary_exprs: Vec<&BinaryExpr> = ast.nodes().collect();
    assert_eq!(binary_exprs.len(), 1);
    assert_eq!(binary_exprs[0].op, BinaryOp::Equal);
}

#[test]
fn test_multi_expr_nodes() {
    // Test MultiExpr::nodes() with a manually constructed MultiExpr
    let multi = MultiExpr {
        op: MultiOp::In,
        exprs: vec![
            WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("id"),
            })),
            WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(1))),
            WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(2))),
        ],
    };

    // Test that MultiExpr::nodes() returns itself
    let multi_exprs: Vec<&MultiExpr> = multi.nodes().collect();
    assert_eq!(multi_exprs.len(), 1);
    assert_eq!(multi_exprs[0].op, MultiOp::In);

    // Test that it can extract child nodes
    let columns: Vec<&ColumnNode> = multi.nodes().collect();
    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0].column, "id");
}

#[test]
fn test_in_clause_parse_and_deparse() {
    // Test that IN clause round-trips through parse and deparse
    let select = parse_select("SELECT * FROM t WHERE status IN ('active', 'pending')");

    let where_clause = select.where_clause.as_ref().unwrap();

    let mut buf = String::new();
    where_clause.deparse(&mut buf);
    assert_eq!(buf, "status IN ('active', 'pending')");
}

#[test]
fn test_order_by_multiple_columns() {
    let ast = parse_query("SELECT * FROM users ORDER BY last_name ASC, first_name DESC");

    assert_eq!(ast.order_by.len(), 2);

    // First ORDER BY clause
    assert_eq!(ast.order_by[0].direction, OrderDirection::Asc);
    if let ScalarExpr::Column(col) = &ast.order_by[0].expr {
        assert_eq!(col.column, "last_name");
    } else {
        panic!("Expected column expression");
    }

    // Second ORDER BY clause
    assert_eq!(ast.order_by[1].direction, OrderDirection::Desc);
    if let ScalarExpr::Column(col) = &ast.order_by[1].expr {
        assert_eq!(col.column, "first_name");
    } else {
        panic!("Expected column expression");
    }
}

#[test]
fn test_order_by_qualified_column() {
    let ast = parse_query("SELECT * FROM users u ORDER BY u.name ASC");

    assert_eq!(ast.order_by.len(), 1);

    if let ScalarExpr::Column(col) = &ast.order_by[0].expr {
        assert_eq!(col.table, Some(EcoString::from("u")));
        assert_eq!(col.column, "name");
    } else {
        panic!("Expected column expression");
    }
}

#[test]
fn test_order_by_with_where() {
    let ast = parse_query("SELECT * FROM users WHERE active = true ORDER BY name ASC");

    assert!(ast.has_where_clause());
    assert_eq!(ast.order_by.len(), 1);
    assert_eq!(ast.order_by[0].direction, OrderDirection::Asc);
}

#[test]
fn test_order_by_round_trip() {
    let queries = vec![
        "SELECT * FROM users ORDER BY name ASC",
        "SELECT * FROM users ORDER BY age DESC",
        "SELECT id, name FROM users WHERE active = true ORDER BY created_at DESC",
        "SELECT * FROM users ORDER BY last_name ASC, first_name ASC, id DESC",
    ];

    for sql in queries {
        // Parse original
        let ast1 = parse_query(sql);

        // Deparse to string
        let deparsed = deparsed_node(&ast1);

        // Parse deparsed version
        let ast2 = parse_query(&deparsed);

        // Should be equivalent
        assert_eq!(ast1, ast2, "Round trip failed for: {sql}");
    }
}

#[test]
fn test_order_by_nodes_extraction() {
    let ast = parse_query("SELECT * FROM users ORDER BY u.name ASC, age DESC");

    // Extract OrderByClause nodes
    let order_clauses: Vec<&OrderByClause> = ast.nodes().collect();
    assert_eq!(order_clauses.len(), 2);

    // Extract ColumnNode from ORDER BY
    let columns: Vec<&ColumnNode> = ast.nodes().collect();
    // Should find columns in ORDER BY clause
    assert!(columns.iter().any(|c| c.column == "name"));
    assert!(columns.iter().any(|c| c.column == "age"));
}

#[test]
fn test_group_by_single_column() {
    let select = parse_select("SELECT status FROM orders GROUP BY status");

    assert_eq!(select.group_by.len(), 1);
    assert_eq!(select.group_by[0].column, "status");
    assert_eq!(select.group_by[0].table, None);
}

#[test]
fn test_group_by_multiple_columns() {
    let select = parse_select("SELECT status, category FROM orders GROUP BY status, category");

    assert_eq!(select.group_by.len(), 2);
    assert_eq!(select.group_by[0].column, "status");
    assert_eq!(select.group_by[1].column, "category");
}

#[test]
fn test_group_by_qualified_column() {
    let select = parse_select("SELECT o.status FROM orders o GROUP BY o.status");

    assert_eq!(select.group_by.len(), 1);
    assert_eq!(select.group_by[0].column, "status");
    assert_eq!(select.group_by[0].table, Some(EcoString::from("o")));
}

#[test]
fn test_having_simple() {
    let select = parse_select("SELECT status FROM orders GROUP BY status HAVING status = 'active'");

    assert!(select.having.is_some());
}

#[test]
fn test_having_with_and() {
    let select = parse_select(
        "SELECT category FROM sales GROUP BY category HAVING category = 'electronics' AND category != 'toys'",
    );

    assert!(!select.group_by.is_empty());
    assert!(select.having.is_some());
}

#[test]
fn test_limit_only() {
    let ast = parse_query("SELECT * FROM users LIMIT 10");

    let limit = ast.limit.as_ref().unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
    assert_eq!(limit.offset, None);
}

#[test]
fn test_offset_only() {
    let ast = parse_query("SELECT * FROM users OFFSET 20");

    let limit = ast.limit.as_ref().unwrap();
    assert_eq!(limit.count, None);
    assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
}

#[test]
fn test_limit_and_offset() {
    let ast = parse_query("SELECT * FROM users LIMIT 10 OFFSET 20");

    let limit = ast.limit.as_ref().unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
    assert_eq!(limit.offset, Some(LiteralValue::Integer(20)));
}

#[test]
fn test_no_limit() {
    let ast = parse_query("SELECT * FROM users");

    assert!(ast.limit.is_none());
}

#[test]
fn test_no_group_by() {
    let select = parse_select("SELECT * FROM users WHERE id = 1");

    assert!(select.group_by.is_empty());
    assert!(select.having.is_none());
}

#[test]
fn test_combined_group_by_having_limit() {
    let ast = parse_query(
        "SELECT status FROM orders GROUP BY status HAVING status != 'cancelled' ORDER BY status DESC LIMIT 10",
    );

    let select = match &ast.body {
        QueryBody::Select(s) => s,
        _ => panic!("expected SELECT"),
    };
    assert_eq!(select.group_by.len(), 1);
    assert!(select.having.is_some());
    assert!(ast.limit.is_some());
    assert!(!ast.order_by.is_empty());
}

#[test]
fn test_limit_parameterized() {
    let ast = parse_query("SELECT * FROM users LIMIT $1");

    let limit = ast.limit.as_ref().unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".into())));
    assert_eq!(limit.offset, None);
}

#[test]
fn test_limit_and_offset_parameterized() {
    let ast = parse_query("SELECT * FROM users LIMIT $1 OFFSET $2");

    let limit = ast.limit.as_ref().unwrap();
    assert_eq!(limit.count, Some(LiteralValue::Parameter("$1".into())));
    assert_eq!(limit.offset, Some(LiteralValue::Parameter("$2".into())));
}

#[test]
fn test_function_with_alias() {
    let select = parse_select("SELECT COUNT(*) as total FROM users");

    let columns = select_columns(&select);

    assert_eq!(columns[0].alias().cloned(), Some(EcoString::from("total")));
}

#[test]
fn test_function_mixed_with_columns() {
    let select = parse_select("SELECT id, name, COUNT(*) as cnt FROM users GROUP BY id, name");

    let columns = select_columns(&select);

    assert_eq!(columns.len(), 3);
    assert!(matches!(
        columns[0].expr().expect("non-star SELECT column"),
        ScalarExpr::Column(_)
    ));
    assert!(matches!(
        columns[1].expr().expect("non-star SELECT column"),
        ScalarExpr::Column(_)
    ));
    assert!(matches!(
        columns[2].expr().expect("non-star SELECT column"),
        ScalarExpr::Function(_)
    ));
}

#[test]
fn test_literal_in_select() {
    let select = parse_select("SELECT 42 as answer, 'hello' as greeting FROM t");

    let columns = select_columns(&select);

    assert_eq!(columns.len(), 2);
    assert!(matches!(
        columns[0].expr().expect("non-star SELECT column"),
        ScalarExpr::Literal(LiteralValue::Integer(42))
    ));
    assert!(matches!(
        &columns[1].expr().expect("non-star SELECT column"),
        ScalarExpr::Literal(LiteralValue::String(s)) if s == "hello"
    ));
}

#[test]
fn test_function_deparse_count_star() {
    let func = FunctionCall {
        name: EcoString::from("count"),
        args: vec![],
        agg_star: true,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    };
    let mut buf = String::new();
    func.deparse(&mut buf);
    assert_eq!(buf, "COUNT(*)");
}

#[test]
fn test_function_deparse_count_distinct() {
    let func = FunctionCall {
        name: EcoString::from("count"),
        args: vec![ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("status"),
        })],
        agg_star: false,
        agg_distinct: true,
        agg_order: vec![],
        agg_filter: None,
        over: None,
    };
    let mut buf = String::new();
    func.deparse(&mut buf);
    assert_eq!(buf, "COUNT(DISTINCT status)");
}

#[test]
fn test_case_deparse() {
    let sql = "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1";
    let ast = parse_query(sql);

    let mut buf = String::new();
    ast.deparse(&mut buf);
    assert_eq!(
        buf,
        "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items WHERE id = 1"
    );
}

#[test]
fn test_case_has_subqueries() {
    // CASE with subquery in WHEN condition
    let select =
        parse_select("SELECT CASE WHEN id IN (SELECT id FROM other) THEN 1 ELSE 0 END FROM items");

    assert!(
        select.has_subqueries(),
        "CASE with subquery should have sublink"
    );
}

#[test]
fn test_window_function_deparse() {
    let func = FunctionCall {
        name: EcoString::from("sum"),
        args: vec![ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("amount"),
        })],
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![],
        agg_filter: None,
        over: Some(WindowSpec {
            partition_by: vec![ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("category"),
            })],
            order_by: vec![OrderByClause {
                expr: ScalarExpr::Column(ColumnNode {
                    table: None,
                    column: EcoString::from("date"),
                }),
                direction: OrderDirection::Asc,
                null_order: NullOrder::Default,
            }],
            frame: None,
            ref_name: None,
        }),
    };
    let mut buf = String::new();
    func.deparse(&mut buf);
    assert_eq!(
        buf,
        "SUM(amount) OVER (PARTITION BY category ORDER BY date ASC)"
    );
}

/// Deparse the OVER clause of the first SELECT-list function in `sql`.
fn parse_over_deparse(sql: &str) -> String {
    let select = parse_select(sql);
    let columns = select_columns(&select);
    let ScalarExpr::Function(func) = &columns[0].expr().expect("non-star column") else {
        panic!("expected function");
    };
    let mut buf = String::new();
    func.over.as_ref().expect("OVER clause").deparse(&mut buf);
    buf
}

#[test]
fn test_window_frame_rows_unbounded_roundtrip() {
    // The single-bound shorthand normalizes to the explicit BETWEEN form.
    assert_eq!(
        parse_over_deparse("SELECT sum(a) OVER (ORDER BY b ROWS UNBOUNDED PRECEDING) FROM t",),
        "(ORDER BY b ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)"
    );
}

#[test]
fn test_window_frame_rows_between_following_roundtrip() {
    assert_eq!(
        parse_over_deparse(
            "SELECT sum(a) OVER (ORDER BY b ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) FROM t",
        ),
        "(ORDER BY b ASC ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING)"
    );
}

#[test]
fn test_window_frame_range_full_partition_roundtrip() {
    assert_eq!(
        parse_over_deparse(
            "SELECT last_value(a) OVER (ORDER BY b \
             RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) FROM t",
        ),
        "(ORDER BY b ASC RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    );
}

#[test]
fn test_window_frame_groups_exclude_roundtrip() {
    assert_eq!(
        parse_over_deparse(
            "SELECT count(*) OVER (ORDER BY b \
             GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE TIES) FROM t",
        ),
        "(ORDER BY b ASC GROUPS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING EXCLUDE TIES)"
    );
}

#[test]
fn test_window_default_frame_emits_nothing() {
    // No explicit frame → no frame text (PostgreSQL's default applies).
    assert_eq!(
        parse_over_deparse("SELECT sum(a) OVER (ORDER BY b) FROM t"),
        "(ORDER BY b ASC)"
    );
}

#[test]
fn test_named_window_resolves_to_definition() {
    // `OVER w` is inlined from the WINDOW clause (PGC-280).
    assert_eq!(
        parse_over_deparse("SELECT sum(a) OVER w FROM t WINDOW w AS (PARTITION BY b ORDER BY c)",),
        "(PARTITION BY b ORDER BY c ASC)"
    );
}

#[test]
fn test_named_window_with_frame_resolves() {
    assert_eq!(
        parse_over_deparse(
            "SELECT last_value(a) OVER w FROM t \
             WINDOW w AS (ORDER BY b ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)",
        ),
        "(ORDER BY b ASC ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)"
    );
}

#[test]
fn test_undefined_window_reference_forwards() {
    // No WINDOW clause defines `w` → conversion fails so the query forwards
    // rather than silently deparsing to `OVER ()`.
    assert!(query_expr_parse("SELECT rank() OVER w FROM t").is_err());
}

#[test]
fn test_aggregate_order_by_deparse() {
    let sql = "SELECT string_agg(name, ', ' ORDER BY name ASC) FROM t";
    let ast = parse_query(sql);

    let mut buf = String::new();
    ast.deparse(&mut buf);
    assert!(buf.contains("ORDER BY"), "deparsed should contain ORDER BY");
    assert!(
        buf.contains("STRING_AGG"),
        "deparsed should contain STRING_AGG"
    );
}

/// Two FILTER aggregates with different predicates must round-trip to
/// distinct function calls — they must not collapse to plain count(*).
#[test]
fn test_filter_aggregate_distinct_predicates_roundtrip() {
    let sql = "SELECT count(*) FILTER (WHERE posttypeid = 1) AS questions, \
               count(*) FILTER (WHERE posttypeid = 2) AS answers FROM posts";
    let select = parse_select(sql);

    let columns = select_columns(&select);
    assert_eq!(columns.len(), 2);

    let mut buf = String::new();
    for (i, col) in columns.iter().enumerate() {
        let ScalarExpr::Function(func) = &col.expr().expect("non-star SELECT column") else {
            panic!("expected function in column {i}");
        };
        assert!(
            func.agg_filter.is_some(),
            "column {i} should retain its FILTER predicate"
        );
        buf.clear();
        func.deparse(&mut buf);
        assert!(
            buf.contains("FILTER (WHERE"),
            "column {i} should deparse FILTER clause; got `{buf}`"
        );
    }

    // The two columns must deparse differently — collapsing them was the bug.
    let mut a = String::new();
    let ScalarExpr::Function(f0) = &columns[0].expr().expect("non-star SELECT column") else {
        unreachable!()
    };
    f0.deparse(&mut a);
    let mut b = String::new();
    let ScalarExpr::Function(f1) = &columns[1].expr().expect("non-star SELECT column") else {
        unreachable!()
    };
    f1.deparse(&mut b);
    assert_ne!(a, b, "FILTER predicates 1 and 2 must deparse differently");
}

#[test]
fn test_aggregate_order_by_deparse_roundtrip() {
    let func = FunctionCall {
        name: EcoString::from("string_agg"),
        args: vec![
            ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("name"),
            }),
            ScalarExpr::Literal(LiteralValue::String(", ".into())),
        ],
        agg_star: false,
        agg_distinct: false,
        agg_order: vec![OrderByClause {
            expr: ScalarExpr::Column(ColumnNode {
                table: None,
                column: EcoString::from("name"),
            }),
            direction: OrderDirection::Asc,
            null_order: NullOrder::Default,
        }],
        agg_filter: None,
        over: None,
    };
    let mut buf = String::new();
    func.deparse(&mut buf);
    assert_eq!(buf, "STRING_AGG(name, ', ' ORDER BY name ASC)");
}

#[test]
fn test_arithmetic_multiply_parse() {
    let sql = "SELECT amount * 2 FROM t";
    let select = parse_select(sql);

    let columns = select_columns(&select);

    let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column") else {
        panic!("expected arithmetic expression");
    };

    assert_eq!(arith.op, ArithmeticOp::Multiply);
}

#[test]
fn test_arithmetic_multiply_negative() {
    let sql = "SELECT amount * -1 FROM t";
    let select = parse_select(sql);

    let columns = select_columns(&select);

    let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column") else {
        panic!("expected arithmetic expression");
    };

    assert_eq!(arith.op, ArithmeticOp::Multiply);
    // Right side should be -1 (negative literal)
    assert!(matches!(
        arith.right.as_ref(),
        ScalarExpr::Literal(LiteralValue::Integer(-1))
    ));
}

#[test]
fn test_arithmetic_add() {
    let sql = "SELECT price + tax FROM t";
    let select = parse_select(sql);

    let columns = select_columns(&select);

    let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column") else {
        panic!("expected arithmetic expression");
    };

    assert_eq!(arith.op, ArithmeticOp::Add);
}

#[test]
fn test_arithmetic_subtract() {
    let sql = "SELECT total - discount FROM t";
    let select = parse_select(sql);

    let columns = select_columns(&select);

    let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column") else {
        panic!("expected arithmetic expression");
    };

    assert_eq!(arith.op, ArithmeticOp::Subtract);
}

#[test]
fn test_arithmetic_divide() {
    let sql = "SELECT total / count FROM t";
    let select = parse_select(sql);

    let columns = select_columns(&select);

    let ScalarExpr::Arithmetic(arith) = &columns[0].expr().expect("non-star SELECT column") else {
        panic!("expected arithmetic expression");
    };

    assert_eq!(arith.op, ArithmeticOp::Divide);
}

#[test]
fn test_arithmetic_deparse() {
    let arith = ArithmeticExpr {
        left: Box::new(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from("amount"),
        })),
        op: ArithmeticOp::Multiply,
        right: Box::new(ScalarExpr::Literal(LiteralValue::Integer(-1))),
    };
    let mut buf = String::new();
    arith.deparse(&mut buf);
    assert_eq!(buf, "(amount * -1)");
}

#[test]
fn test_arithmetic_nested() {
    // (a + b) * c
    let sql = "SELECT (a + b) * c FROM t";
    let select = parse_select(sql);

    let columns = select_columns(&select);

    let ScalarExpr::Arithmetic(outer) = &columns[0].expr().expect("non-star SELECT column") else {
        panic!("expected arithmetic expression");
    };

    assert_eq!(outer.op, ArithmeticOp::Multiply);

    // Left side should be another arithmetic expression (a + b)
    let ScalarExpr::Arithmetic(inner) = outer.left.as_ref() else {
        panic!("expected nested arithmetic expression");
    };
    assert_eq!(inner.op, ArithmeticOp::Add);
}

// ========================================================================
// TypeCast tests
// ========================================================================

/// Parse `sql` and return the first SELECT column's expr.
fn parse_first_column(sql: &str) -> ScalarExpr {
    let select = parse_select(sql);
    let SelectColumns::Columns(columns) = select.columns else {
        panic!("expected columns");
    };
    let SelectColumn::Expr { expr, .. } = columns.into_iter().next().expect("at least one column")
    else {
        panic!("expected Expr column");
    };
    expr
}

/// Parse `sql` and unwrap the first column as a `(inner, canonical_target)` cast.
fn parse_first_typecast(sql: &str) -> (ScalarExpr, EcoString) {
    use crate::query::cast::cast_target_deparse;
    match parse_first_column(sql) {
        ScalarExpr::TypeCast { expr, target } => {
            (*expr, EcoString::from(cast_target_deparse(&target)))
        }
        other => panic!("expected type cast, got {other:?}"),
    }
}

#[test]
fn test_type_cast_count_star_int() {
    let (expr, target_type) = parse_first_typecast("SELECT COUNT(*)::INT FROM t");
    assert_eq!(target_type.as_str(), "int4");
    assert!(matches!(expr, ScalarExpr::Function(f) if f.name == "count"));
}

#[test]
fn test_type_cast_column_text() {
    let (expr, target_type) = parse_first_typecast("SELECT col::text FROM t");
    assert_eq!(target_type.as_str(), "text");
    assert!(matches!(expr, ScalarExpr::Column(c) if c.column == "col"));
}

#[test]
fn test_type_cast_arithmetic_numeric_typmods() {
    let (expr, target_type) = parse_first_typecast("SELECT (a + b)::numeric(10,2) FROM t");
    assert_eq!(target_type.as_str(), "numeric(10,2)");
    assert!(matches!(expr, ScalarExpr::Arithmetic(_)));
}

#[test]
fn test_type_cast_qualified_column() {
    let (expr, target_type) = parse_first_typecast("SELECT t.col::int FROM t");
    assert_eq!(target_type.as_str(), "int4");
    let ScalarExpr::Column(col) = expr else {
        panic!("expected column");
    };
    assert_eq!(col.table.as_deref(), Some("t"));
    assert_eq!(col.column.as_str(), "col");
}

#[test]
fn test_type_cast_literal_date_still_parses() {
    let (expr, target_type) = parse_first_typecast("SELECT '2024-01-01'::DATE FROM t");
    assert_eq!(target_type.as_str(), "date");
    assert!(matches!(expr, ScalarExpr::Literal(LiteralValue::String(_))));
}

#[test]
fn test_type_cast_deparse_round_trip() {
    let sql = "SELECT COUNT(*)::INT, SUM(amount)::NUMERIC(18,2) FROM t";
    let select = parse_select(sql);

    let mut buf = String::new();
    select.deparse(&mut buf);
    assert!(
        buf.contains("(COUNT(*))::int4"),
        "expected count cast in: {buf}"
    );
    assert!(
        buf.contains("(SUM(amount))::numeric(18,2)"),
        "expected sum cast in: {buf}"
    );
}

#[test]
fn test_type_cast_aliased() {
    let select = parse_select("SELECT COUNT(*)::INT AS n FROM t");
    let columns = select_columns(&select);
    assert_eq!(columns[0].alias().cloned().as_deref(), Some("n"));
    assert!(matches!(
        &columns[0].expr().expect("non-star SELECT column"),
        ScalarExpr::TypeCast { .. }
    ));
}

// ========================================================================
// QueryExpr (new type hierarchy) tests
// ========================================================================

#[test]
fn test_query_expr_simple_select() {
    let sql = "SELECT id, name FROM users WHERE id = 1";
    let query_expr = query_expr_parse(sql).unwrap();

    assert!(query_expr.is_single_table());
    assert!(query_expr.has_where_clause());

    let select = query_expr.as_select().unwrap();
    assert_eq!(select.from.len(), 1);
    assert!(matches!(select.columns, SelectColumns::Columns(_)));
}

#[test]
fn test_query_expr_select_star() {
    let sql = "SELECT * FROM products";
    let query_expr = query_expr_parse(sql).unwrap();

    assert!(query_expr.is_single_table());
    assert!(!query_expr.has_where_clause());

    let SelectColumns::Columns(cols) = &query_expr.as_select().unwrap().columns else {
        panic!("Expected Columns");
    };
    assert_eq!(cols.len(), 1);
    assert!(matches!(&cols[0], SelectColumn::Star(None)));
}

#[test]
fn test_query_expr_select_star_with_column() {
    let select = parse_select("SELECT *, col FROM test");
    let [star, column] = select_columns(&select) else {
        panic!("expected two columns");
    };
    assert!(matches!(star, SelectColumn::Star(None)));
    assert!(matches!(column.expr(), Some(ScalarExpr::Column(c)) if c.column == "col"));
}

#[test]
fn test_query_expr_select_qualified_star() {
    let select = parse_select("SELECT t1.*, t2.col FROM test t1 JOIN test2 t2 ON t2.id = t1.id");
    let [star, column] = select_columns(&select) else {
        panic!("expected two columns");
    };
    assert!(matches!(star, SelectColumn::Star(Some(t)) if t == "t1"));
    assert!(matches!(column.expr(),
        Some(ScalarExpr::Column(c)) if c.column == "col" && c.table.as_deref() == Some("t2")));
}

#[test]
fn test_query_expr_select_star_deparse() {
    // Bare star
    let sql = "SELECT * FROM products";
    let query_expr = query_expr_parse(sql).unwrap();
    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert!(
        buf.contains("SELECT *"),
        "should deparse as SELECT *: {buf}"
    );

    // Qualified star with column
    let sql = "SELECT t1.*, t2.name FROM a t1 JOIN b t2 ON t2.id = t1.id";
    let query_expr = query_expr_parse(sql).unwrap();
    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert!(buf.contains("t1.*"), "should deparse qualified star: {buf}");
    assert!(buf.contains("t2.name"), "should deparse column: {buf}");
}

#[test]
fn test_query_expr_values_clause() {
    let sql = "VALUES (1, 'a'), (2, 'b')";
    let query_expr = query_expr_parse(sql).unwrap();

    let QueryBody::Values(values) = &query_expr.body else {
        panic!("expected VALUES clause");
    };

    assert_eq!(values.rows.len(), 2);
    assert_eq!(values.rows[0].len(), 2);
    assert_eq!(values.rows[1].len(), 2);

    // Check first row values
    assert!(matches!(values.rows[0][0], LiteralValue::Integer(1)));
    assert!(matches!(&values.rows[0][1], LiteralValue::String(s) if s == "a"));
}

#[test]
fn test_query_expr_union() {
    let sql = "SELECT a FROM t1 UNION SELECT b FROM t2";
    let query_expr = query_expr_parse(sql).unwrap();

    let QueryBody::SetOp(set_op) = &query_expr.body else {
        panic!("expected set operation");
    };

    assert_eq!(set_op.op, SetOpType::Union);
    assert!(!set_op.all);

    // Check left side is a SELECT
    assert!(set_op.left.as_select().is_some());
    // Check right side is a SELECT
    assert!(set_op.right.as_select().is_some());
}

#[test]
fn test_query_expr_union_all() {
    let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2";
    let query_expr = query_expr_parse(sql).unwrap();

    let QueryBody::SetOp(set_op) = &query_expr.body else {
        panic!("expected set operation");
    };

    assert_eq!(set_op.op, SetOpType::Union);
    assert!(set_op.all);
}

#[test]
fn test_query_expr_union_with_order_by() {
    let sql = "SELECT a FROM t1 UNION SELECT b FROM t2 ORDER BY 1";
    let query_expr = query_expr_parse(sql).unwrap();

    // ORDER BY should be at the top level
    assert_eq!(query_expr.order_by.len(), 1);

    let QueryBody::SetOp(set_op) = &query_expr.body else {
        panic!("expected set operation");
    };
    assert_eq!(set_op.op, SetOpType::Union);

    // Sub-queries should not have ORDER BY
    assert!(set_op.left.order_by.is_empty());
    assert!(set_op.right.order_by.is_empty());
}

#[test]
fn test_query_expr_intersect() {
    let sql = "SELECT a FROM t1 INTERSECT SELECT b FROM t2";
    let query_expr = query_expr_parse(sql).unwrap();

    let QueryBody::SetOp(set_op) = &query_expr.body else {
        panic!("expected set operation");
    };

    assert_eq!(set_op.op, SetOpType::Intersect);
}

#[test]
fn test_query_expr_except() {
    let sql = "SELECT a FROM t1 EXCEPT SELECT b FROM t2";
    let query_expr = query_expr_parse(sql).unwrap();

    let QueryBody::SetOp(set_op) = &query_expr.body else {
        panic!("expected set operation");
    };

    assert_eq!(set_op.op, SetOpType::Except);
}

#[test]
fn test_query_expr_chained_union() {
    let sql = "SELECT a FROM t1 UNION SELECT b FROM t2 UNION SELECT c FROM t3";
    let query_expr = query_expr_parse(sql).unwrap();

    let QueryBody::SetOp(outer_set_op) = &query_expr.body else {
        panic!("expected set operation");
    };
    assert_eq!(outer_set_op.op, SetOpType::Union);

    // The nested structure: (t1 UNION t2) UNION t3
    let QueryBody::SetOp(inner_set_op) = &outer_set_op.left.body else {
        panic!("expected nested set operation");
    };
    assert_eq!(inner_set_op.op, SetOpType::Union);

    // Right side of outer should be simple SELECT
    assert!(outer_set_op.right.as_select().is_some());
}

#[test]
fn test_query_expr_deparse_simple_select() {
    let sql = "SELECT id FROM users WHERE id = 1";
    let query_expr = query_expr_parse(sql).unwrap();

    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert_eq!(buf, "SELECT id FROM users WHERE id = 1");
}

#[test]
fn test_query_expr_deparse_values() {
    let sql = "VALUES (1, 'a')";
    let query_expr = query_expr_parse(sql).unwrap();

    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert_eq!(buf, "VALUES (1, 'a')");
}

#[test]
fn test_query_expr_deparse_union() {
    let sql = "SELECT a FROM t1 UNION SELECT b FROM t2";
    let query_expr = query_expr_parse(sql).unwrap();

    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert_eq!(buf, "SELECT a FROM t1 UNION SELECT b FROM t2");
}

#[test]
fn test_query_expr_deparse_union_all() {
    let sql = "SELECT a FROM t1 UNION ALL SELECT b FROM t2";
    let query_expr = query_expr_parse(sql).unwrap();

    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert_eq!(buf, "SELECT a FROM t1 UNION ALL SELECT b FROM t2");
}

#[test]
fn test_query_expr_order_by_limit() {
    let sql = "SELECT a FROM t ORDER BY a LIMIT 10";
    let query_expr = query_expr_parse(sql).unwrap();

    assert_eq!(query_expr.order_by.len(), 1);
    assert!(query_expr.limit.is_some());

    let mut buf = String::new();
    query_expr.deparse(&mut buf);
    assert_eq!(buf, "SELECT a FROM t ORDER BY a ASC LIMIT 10");
}

#[test]
fn test_select_nodes_union() {
    let sql = "SELECT id FROM users UNION SELECT id FROM admins";
    let query_expr = query_expr_parse(sql).unwrap();

    let branches = query_expr.select_nodes();
    assert_eq!(branches.len(), 2, "UNION should have two branches");

    // Verify each branch has a FROM clause with different tables
    let tables: Vec<_> = branches
        .iter()
        .filter_map(|b| b.from.first())
        .filter_map(|ts| {
            if let TableSource::Table(t) = ts {
                Some(t.name.as_str())
            } else {
                None
            }
        })
        .collect();

    assert!(tables.contains(&"users"));
    assert!(tables.contains(&"admins"));
}

// ==========================================================================
// WHERE Subquery Parsing Tests
// ==========================================================================

#[test]
fn test_where_subquery_in_parsed() {
    // Test that IN subquery is properly parsed
    let select = parse_select("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");

    let where_clause = select
        .where_clause
        .as_ref()
        .expect("should have WHERE clause");

    match where_clause {
        WhereExpr::Subquery {
            query,
            sublink_type,
            test_expr,
        } => {
            assert_eq!(
                *sublink_type,
                SubLinkType::Any,
                "IN should parse as SubLinkType::Any"
            );
            assert!(test_expr.is_some(), "IN should have test expression (id)");

            // Verify the inner query structure
            match &query.body {
                QueryBody::Select(inner_select) => {
                    let tables: Vec<&TableNode> = inner_select.nodes().collect();
                    assert_eq!(tables.len(), 1);
                    assert_eq!(tables[0].name, "active_users");
                }
                _ => panic!("Expected inner SELECT"),
            }
        }
        _ => panic!("Expected WhereExpr::Subquery, got {:?}", where_clause),
    }
}

/// PGC-361: only equality ANY sublinks are IN-equivalent; a quantified
/// range comparison must be rejected (→ forwarded), not silently
/// converted with its operator dropped.
#[test]
fn test_where_subquery_any_operator_check() {
    // Explicit `= ANY (SELECT ...)` is IN — converts.
    let select =
        parse_select("SELECT * FROM users WHERE id = ANY (SELECT user_id FROM active_users)");
    assert!(matches!(
        select.where_clause.as_ref().expect("where"),
        WhereExpr::Subquery {
            sublink_type: SubLinkType::Any,
            ..
        }
    ));

    // Non-equality quantified comparisons are rejected.
    for sql in [
        "SELECT * FROM users WHERE id > ANY (SELECT user_id FROM active_users)",
        "SELECT * FROM users WHERE id <= ANY (SELECT user_id FROM active_users)",
        "SELECT * FROM users WHERE id <> ANY (SELECT user_id FROM active_users)",
    ] {
        assert!(query_expr_parse(sql).is_err(), "should be rejected: {sql}");
    }

    // ALL is unchanged: `<> ALL` (NOT IN) converts, `< ALL` rejects.
    let select =
        parse_select("SELECT * FROM users WHERE id <> ALL (SELECT user_id FROM active_users)");
    assert!(matches!(
        select.where_clause.as_ref().expect("where"),
        WhereExpr::Subquery {
            sublink_type: SubLinkType::All,
            ..
        }
    ));
    assert!(
        query_expr_parse("SELECT * FROM users WHERE id < ALL (SELECT user_id FROM active_users)")
            .is_err()
    );
}

#[test]
fn test_where_subquery_exists_parsed() {
    // Test that EXISTS subquery is properly parsed
    let select = parse_select(
        "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)",
    );

    let where_clause = select
        .where_clause
        .as_ref()
        .expect("should have WHERE clause");

    match where_clause {
        WhereExpr::Subquery {
            sublink_type,
            test_expr,
            ..
        } => {
            assert_eq!(
                *sublink_type,
                SubLinkType::Exists,
                "EXISTS should parse as SubLinkType::Exists"
            );
            assert!(
                test_expr.is_none(),
                "EXISTS should not have test expression"
            );
        }
        _ => panic!("Expected WhereExpr::Subquery, got {:?}", where_clause),
    }
}

#[test]
fn test_where_subquery_scalar_parsed() {
    // Test that scalar subquery in WHERE is properly parsed
    let select = parse_select("SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)");

    let where_clause = select
        .where_clause
        .as_ref()
        .expect("should have WHERE clause");

    // The scalar subquery should be on the right side of the > comparison
    match where_clause {
        WhereExpr::Binary(binary) => match binary.rexpr.as_ref() {
            WhereExpr::Subquery {
                sublink_type,
                test_expr,
                ..
            } => {
                assert_eq!(
                    *sublink_type,
                    SubLinkType::Expr,
                    "Scalar subquery should parse as SubLinkType::Expr"
                );
                assert!(
                    test_expr.is_none(),
                    "Scalar subquery should not have test expression"
                );
            }
            _ => panic!("Expected WhereExpr::Subquery on right side"),
        },
        _ => panic!("Expected WhereExpr::Binary, got {:?}", where_clause),
    }
}

#[test]
fn test_where_subquery_not_in_parsed() {
    // NOT IN should also parse as SubLinkType::Any with proper negation
    let select =
        parse_select("SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)");

    // PostgreSQL parses NOT IN as a negated ANY sublink
    // The structure may vary, but we should be able to find the subquery
    assert!(select.where_clause.is_some(), "Should have WHERE clause");
    assert!(select.has_subqueries(), "Should detect sublink in NOT IN");
}

#[test]
fn test_where_subquery_has_subqueries_traversal() {
    // Verify nodes() traverses into subqueries to find TableNode
    let select = parse_select("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");

    let tables: Vec<&TableNode> = select.nodes().collect();

    // Should find both outer table (users) and inner table (active_users)
    assert_eq!(
        tables.len(),
        2,
        "Should find tables in both outer query and subquery"
    );

    let table_names: Vec<&str> = tables.iter().map(|t| t.name.as_str()).collect();
    assert!(table_names.contains(&"users"), "Should find outer table");
    assert!(
        table_names.contains(&"active_users"),
        "Should find subquery table"
    );
}

#[test]
fn test_where_subquery_deparse_in() {
    let select = parse_select("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");
    let mut buf = String::new();
    select.deparse(&mut buf);

    // The deparsed output should contain IN and the subquery
    assert!(buf.contains("IN"), "Deparsed should contain IN: {}", buf);
    assert!(
        buf.contains("active_users"),
        "Deparsed should contain subquery table: {}",
        buf
    );
}

#[test]
fn test_where_subquery_deparse_exists() {
    let select = parse_select("SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items)");
    let mut buf = String::new();
    select.deparse(&mut buf);

    assert!(
        buf.contains("EXISTS"),
        "Deparsed should contain EXISTS: {}",
        buf
    );
    assert!(
        buf.contains("items"),
        "Deparsed should contain subquery table: {}",
        buf
    );
}

// ====================================================================
// CTE tests
// ====================================================================

#[test]
fn test_cte_simple_parsed() {
    let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");

    // QueryExpr should have one CTE definition
    assert_eq!(query.ctes.len(), 1);
    assert_eq!(query.ctes[0].name, "x");
    assert_eq!(query.ctes[0].materialization, CteMaterialization::Default);

    // Body should reference CTE via CteRef, not Table
    let select = query.as_select().unwrap();
    assert_eq!(select.from.len(), 1);
    match &select.from[0] {
        TableSource::CteRef(cte_ref) => {
            assert_eq!(cte_ref.cte_name, "x");
            assert!(cte_ref.alias.is_none());
            // Inner query should reference "users"
            let inner_tables: Vec<_> = cte_ref
                .query
                .nodes::<TableNode>()
                .map(|t| t.name.as_str())
                .collect();
            assert_eq!(inner_tables, vec!["users"]);
        }
        other => panic!("expected CteRef, got {other:?}"),
    }
}

#[test]
fn test_cte_deparse() {
    let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");
    let mut buf = String::new();
    query.deparse(&mut buf);

    assert!(buf.contains("WITH"), "should contain WITH: {buf}");
    assert!(buf.contains("x AS"), "should contain CTE name: {buf}");
    // The FROM clause should reference x by name, not inline the body
    // The output should have the CTE body once (in WITH) and the name once (in FROM)
    let x_count = buf.matches(" x").count();
    assert!(
        x_count >= 2,
        "should reference x at least twice (definition + FROM): {buf}"
    );
}

#[test]
fn test_cte_column_aliases() {
    let query = parse_query("WITH x(a, b) AS (SELECT id, name FROM users) SELECT * FROM x");

    assert_eq!(query.ctes[0].column_aliases, vec!["a", "b"]);

    // CteRefNode should also have column aliases
    let select = query.as_select().unwrap();
    match &select.from[0] {
        TableSource::CteRef(cte_ref) => {
            assert_eq!(cte_ref.column_aliases, vec!["a", "b"]);
        }
        other => panic!("expected CteRef, got {other:?}"),
    }
}

#[test]
fn test_cte_reference_alias() {
    let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x AS y");

    let select = query.as_select().unwrap();
    match &select.from[0] {
        TableSource::CteRef(cte_ref) => {
            assert_eq!(cte_ref.cte_name, "x");
            assert_eq!(cte_ref.alias.as_ref().unwrap().name, "y");
        }
        other => panic!("expected CteRef, got {other:?}"),
    }
}

#[test]
fn test_cte_with_join() {
    let query = parse_query(
        "WITH active AS (SELECT id, name FROM users WHERE active = true) \
         SELECT u.id, a.name FROM users u JOIN active a ON u.id = a.id",
    );
    assert_eq!(query.ctes.len(), 1);
    // A join of a table (left) with the CTE reference (right)
    let join = only_join(query.as_select().expect("SELECT"));
    assert!(matches!(*join.left, TableSource::Table(_)));
    assert!(matches!(*join.right, TableSource::CteRef(_)));
}

#[test]
fn test_cte_referencing_cte() {
    let query = parse_query(
        "WITH a AS (SELECT id FROM users), \
         b AS (SELECT * FROM a) \
         SELECT * FROM b",
    );

    assert_eq!(query.ctes.len(), 2);
    assert_eq!(query.ctes[0].name, "a");
    assert_eq!(query.ctes[1].name, "b");

    // CTE b's body should contain a CteRef to a
    let b_select = query.ctes[1].query.as_select().unwrap();
    match &b_select.from[0] {
        TableSource::CteRef(cte_ref) => {
            assert_eq!(cte_ref.cte_name, "a");
        }
        other => panic!("expected CteRef in b's body, got {other:?}"),
    }
}

#[test]
fn test_cte_multiple_references() {
    let query = parse_query(
        "WITH x AS (SELECT id FROM users) \
         SELECT * FROM x a JOIN x b ON a.id = b.id",
    );
    assert_eq!(query.ctes.len(), 1);
    // Both join sides reference the CTE
    let join = only_join(query.as_select().expect("SELECT"));
    assert!(matches!(*join.left, TableSource::CteRef(_)));
    assert!(matches!(*join.right, TableSource::CteRef(_)));
}

#[test]
fn test_cte_in_union() {
    let query = parse_query(
        "WITH x AS (SELECT id FROM users) \
         SELECT * FROM x UNION ALL SELECT * FROM x",
    );

    assert_eq!(query.ctes.len(), 1);

    match &query.body {
        QueryBody::SetOp(set_op) => {
            // Both sides should have CteRef
            let left_select = set_op.left.as_select().unwrap();
            assert!(matches!(&left_select.from[0], TableSource::CteRef(_)));

            let right_select = set_op.right.as_select().unwrap();
            assert!(matches!(&right_select.from[0], TableSource::CteRef(_)));
        }
        _ => panic!("expected SetOp"),
    }
}

#[test]
fn test_cte_unreferenced() {
    let query = parse_query("WITH x AS (SELECT 1) SELECT * FROM users");

    // CTE definition should still be stored
    assert_eq!(query.ctes.len(), 1);
    assert_eq!(query.ctes[0].name, "x");

    // Body should reference users as a Table, not CteRef
    let select = query.as_select().unwrap();
    match &select.from[0] {
        TableSource::Table(table) => {
            assert_eq!(table.name, "users");
        }
        other => panic!("expected Table, got {other:?}"),
    }
}

#[test]
fn test_cte_schema_qualified_not_replaced() {
    let query = parse_query("WITH users AS (SELECT 1 AS id) SELECT * FROM public.users");

    assert_eq!(query.ctes.len(), 1);

    // public.users should remain as Table (schema-qualified)
    let select = query.as_select().unwrap();
    match &select.from[0] {
        TableSource::Table(table) => {
            assert_eq!(table.schema.as_deref(), Some("public"));
            assert_eq!(table.name, "users");
        }
        other => panic!("expected Table, got {other:?}"),
    }
}

#[test]
fn test_cte_recursive_rejected() {
    {
        let result =
            query_expr_parse("WITH RECURSIVE x AS (SELECT 1 UNION ALL SELECT 1) SELECT * FROM x");
        assert!(result.is_err(), "recursive CTE should be rejected");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("RECURSIVE"),
            "error should mention RECURSIVE: {err}"
        );
    }
}

#[test]
fn test_cte_materialized() {
    let query = parse_query("WITH x AS MATERIALIZED (SELECT id FROM users) SELECT * FROM x");

    assert_eq!(
        query.ctes[0].materialization,
        CteMaterialization::Materialized
    );

    let select = query.as_select().unwrap();
    match &select.from[0] {
        TableSource::CteRef(cte_ref) => {
            assert_eq!(cte_ref.materialization, CteMaterialization::Materialized);
        }
        other => panic!("expected CteRef, got {other:?}"),
    }
}

#[test]
fn test_cte_not_materialized() {
    let query = parse_query("WITH x AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM x");

    assert_eq!(
        query.ctes[0].materialization,
        CteMaterialization::NotMaterialized
    );
}

#[test]
fn test_cte_select_nodes() {
    let query = parse_query("WITH x AS (SELECT id FROM users WHERE active = true) SELECT * FROM x");

    let branches = query.select_nodes();
    // Outer SELECT + CteRef body (CTE collected via reference, not eagerly from definition)
    assert_eq!(branches.len(), 2);

    // All branches combined should reference "users"
    let table_names: Vec<_> = branches
        .iter()
        .flat_map(|b| b.direct_table_nodes())
        .map(|t| t.name.as_str())
        .collect();
    assert!(
        table_names.contains(&"users"),
        "branches should reference users: {table_names:?}"
    );
}

#[test]
fn test_cte_nodes_traversal() {
    let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");

    let table_names: Vec<_> = query
        .nodes::<TableNode>()
        .map(|t| t.name.as_str())
        .collect();
    assert!(
        table_names.contains(&"users"),
        "nodes traversal should find users: {table_names:?}"
    );
}

#[test]
fn test_cte_deparse_materialized() {
    let query = parse_query("WITH x AS MATERIALIZED (SELECT id FROM users) SELECT * FROM x");
    let mut buf = String::new();
    query.deparse(&mut buf);

    assert!(
        buf.contains("MATERIALIZED"),
        "deparse should contain MATERIALIZED: {buf}"
    );
}

#[test]
fn test_cte_deparse_not_materialized() {
    let query = parse_query("WITH x AS NOT MATERIALIZED (SELECT id FROM users) SELECT * FROM x");
    let mut buf = String::new();
    query.deparse(&mut buf);

    assert!(
        buf.contains("NOT MATERIALIZED"),
        "deparse should contain NOT MATERIALIZED: {buf}"
    );
}

#[test]
fn test_cte_multiple_definitions() {
    let query = parse_query(
        "WITH a AS (SELECT id FROM users), \
         b AS (SELECT id FROM products) \
         SELECT * FROM a JOIN b ON a.id = b.id",
    );

    assert_eq!(query.ctes.len(), 2);
    assert_eq!(query.ctes[0].name, "a");
    assert_eq!(query.ctes[1].name, "b");

    let select = query.as_select().unwrap();
    match &select.from[0] {
        TableSource::Join(join) => match (&*join.left, &*join.right) {
            (TableSource::CteRef(left), TableSource::CteRef(right)) => {
                assert_eq!(left.cte_name, "a");
                assert_eq!(right.cte_name, "b");
            }
            other => panic!("expected two CteRefs, got {other:?}"),
        },
        other => panic!("expected Join, got {other:?}"),
    }
}

// ==========================================================================
// select_nodes_with_source tests
// ==========================================================================

#[test]
fn test_select_nodes_with_source_cte() {
    let query = parse_query(
        "WITH active AS (SELECT id FROM users WHERE active = true) SELECT * FROM active",
    );
    let branches = query.select_nodes_with_source();

    // Outer SELECT (FromClause) + CteRef body (Subquery(Inclusion))
    // CTE definitions are NOT collected — only CteRef references are
    assert_eq!(branches.len(), 2);
    assert_eq!(branches[0].1, UpdateQuerySource::FromClause);
    assert_eq!(
        branches[1].1,
        UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
        "CTE body via CteRef should be Inclusion"
    );

    // The inner branch should reference "users" table
    let inner_tables = branches[1].0.direct_table_nodes();
    assert_eq!(inner_tables.len(), 1);
    assert_eq!(inner_tables[0].name, "users");
}

#[test]
fn test_deparse_and_inside_or_no_unnecessary_parentheses() {
    // AND inside OR doesn't need parentheses because AND already binds
    // tighter than OR. The parser produces the same AST with or without
    // parens, so the deparse should not add unnecessary ones.
    let sql = "SELECT * FROM t WHERE x = 1 OR (y = 2 AND z = 3)";
    let ast = parse_query(sql);

    let buf = deparsed_node(&ast);

    assert_eq!(
        buf, "SELECT * FROM t WHERE x = 1 OR y = 2 AND z = 3",
        "should not add unnecessary parentheses around AND inside OR"
    );
}

#[test]
fn test_select_nodes_cte_no_duplication() {
    // Both select_nodes() and select_nodes_with_source() collect CTE bodies
    // via CteRef references only, avoiding duplication from CTE definitions
    let query = parse_query("WITH x AS (SELECT id FROM users) SELECT * FROM x");

    let with_source = query.select_nodes_with_source();
    let without_source = query.select_nodes();

    // Both should have 2 (outer + CteRef body)
    assert_eq!(with_source.len(), 2);
    assert_eq!(without_source.len(), 2);
}

#[test]
fn test_fingerprint_excludes_limit() {
    let q1 = parse_query("SELECT * FROM t WHERE id = 1");
    let q2 = parse_query("SELECT * FROM t WHERE id = 1 LIMIT 10");
    let q3 = parse_query("SELECT * FROM t WHERE id = 1 LIMIT 10 OFFSET 20");
    let q4 = parse_query("SELECT * FROM t WHERE id = 1 OFFSET 5");

    let fp1 = query_expr_fingerprint(&q1);
    let fp2 = query_expr_fingerprint(&q2);
    let fp3 = query_expr_fingerprint(&q3);
    let fp4 = query_expr_fingerprint(&q4);

    assert_eq!(fp1, fp2, "LIMIT should not affect fingerprint");
    assert_eq!(fp1, fp3, "LIMIT+OFFSET should not affect fingerprint");
    assert_eq!(fp1, fp4, "OFFSET should not affect fingerprint");

    // Different base queries should still produce different fingerprints
    let q_different = parse_query("SELECT * FROM t WHERE id = 2");
    assert_ne!(
        fp1,
        query_expr_fingerprint(&q_different),
        "different WHERE should produce different fingerprint"
    );
}

#[test]
fn test_limit_clause_deparse() {
    let limit = LimitClause {
        count: Some(LiteralValue::Integer(10)),
        offset: Some(LiteralValue::Integer(5)),
    };
    let mut buf = String::new();
    limit.deparse(&mut buf);
    assert_eq!(buf, " LIMIT 10 OFFSET 5");

    let limit_only = LimitClause {
        count: Some(LiteralValue::Integer(10)),
        offset: None,
    };
    let mut buf = String::new();
    limit_only.deparse(&mut buf);
    assert_eq!(buf, " LIMIT 10");

    let offset_only = LimitClause {
        count: None,
        offset: Some(LiteralValue::Integer(5)),
    };
    let mut buf = String::new();
    offset_only.deparse(&mut buf);
    assert_eq!(buf, " OFFSET 5");
}

// ---------------------------------------------------------------
// PGC-123: HAVING aggregate metadata (DISTINCT / ORDER BY / FILTER)
//
// Predicates flow through `WhereExpr`, but the function leaf is wrapped in
// `WhereExpr::Scalar(ScalarExpr::Function(FunctionCall))` — `FunctionCall`
// already carries the aggregate metadata. These tests assert that the
// metadata round-trips through parse + deparse for HAVING.
// ---------------------------------------------------------------

/// Walk the LHS of a `f(...) <op> <rhs>` HAVING expression to the
/// `FunctionCall` leaf.
fn having_lhs_function(select: &SelectNode) -> &FunctionCall {
    let having = select.having.as_ref().expect("HAVING present");
    let WhereExpr::Binary(binary) = having else {
        panic!("expected Binary HAVING, got {having:?}");
    };
    let WhereExpr::Scalar(ScalarExpr::Function(func)) = binary.lexpr.as_ref() else {
        panic!("expected Scalar(Function) on LHS, got {:?}", binary.lexpr);
    };
    func
}

fn select_deparse(select: &SelectNode) -> String {
    let mut buf = String::new();
    select.deparse(&mut buf);
    buf
}

#[test]
fn test_having_count_distinct_preserved() {
    let select = parse_select("SELECT name FROM users GROUP BY name HAVING COUNT(DISTINCT id) > 1");
    let func = having_lhs_function(&select);
    assert_eq!(func.name, "count");
    assert!(func.agg_distinct, "DISTINCT must survive HAVING conversion");

    let out = select_deparse(&select);
    assert!(
        out.contains("COUNT(DISTINCT "),
        "deparsed HAVING must contain `COUNT(DISTINCT ...)`, got: {out}"
    );
}

#[test]
fn test_having_count_filter_preserved() {
    let select = parse_select(
        "SELECT name FROM users GROUP BY name \
         HAVING COUNT(*) FILTER (WHERE id > 0) > 5",
    );
    let func = having_lhs_function(&select);
    assert_eq!(func.name, "count");
    assert!(func.agg_star, "COUNT(*) star flag must survive");
    assert!(
        func.agg_filter.is_some(),
        "FILTER (WHERE ...) must survive HAVING conversion"
    );

    let out = select_deparse(&select);
    assert!(
        out.contains("FILTER (WHERE "),
        "deparsed HAVING must contain `FILTER (WHERE ...)`, got: {out}"
    );
}

#[test]
fn test_having_aggregate_order_by_preserved() {
    // `string_agg(name, ',' ORDER BY name)` — ORDER BY *inside* the aggregate.
    let select = parse_select(
        "SELECT id FROM users GROUP BY id \
         HAVING string_agg(name, ',' ORDER BY name) <> ''",
    );
    let func = having_lhs_function(&select);
    assert_eq!(func.name, "string_agg");
    assert!(
        !func.agg_order.is_empty(),
        "aggregate ORDER BY must survive HAVING conversion"
    );

    let out = select_deparse(&select);
    assert!(
        out.contains("ORDER BY"),
        "deparsed HAVING must keep aggregate ORDER BY, got: {out}"
    );
}

#[test]
fn test_having_combined_filter_distinct_preserved() {
    // Belt-and-suspenders: DISTINCT and FILTER on the same aggregate.
    let select = parse_select(
        "SELECT name FROM users GROUP BY name \
         HAVING COUNT(DISTINCT id) FILTER (WHERE id > 0) > 1",
    );
    let func = having_lhs_function(&select);
    assert!(func.agg_distinct, "DISTINCT must survive");
    assert!(func.agg_filter.is_some(), "FILTER must survive");

    let out = select_deparse(&select);
    assert!(out.contains("COUNT(DISTINCT "), "deparsed: {out}");
    assert!(out.contains("FILTER (WHERE "), "deparsed: {out}");
}

#[test]
fn test_having_aggregate_roundtrip_stable() {
    // Parse → deparse → parse → deparse must converge (fixed point).
    let sql = "SELECT name FROM users GROUP BY name \
               HAVING COUNT(*) FILTER (WHERE id > 0) > 5";
    let select1 = parse_select(sql);
    let deparsed1 = select_deparse(&select1);

    let select2 = parse_select(&deparsed1);
    let deparsed2 = select_deparse(&select2);

    assert_eq!(
        deparsed1, deparsed2,
        "HAVING aggregate-metadata deparse must round-trip stably"
    );
}

/// Converter errors name the construct that was met, not pg_query's numeric
/// node tag (PGC-409). One case per fall-through arm the workloads have hit.
#[test]
fn test_conversion_errors_name_the_construct() {
    let cases = [
        (
            "SELECT * FROM t TABLESAMPLE SYSTEM (10)",
            "TABLESAMPLE in FROM",
        ),
        (
            "SELECT a FROM t GROUP BY ROLLUP (a)",
            "GROUPING SETS / ROLLUP / CUBE in GROUP BY",
        ),
        (
            "SELECT arr[1] FROM t",
            "subscript or field access (x[1], (x).f) in a column expression",
        ),
        (
            "SELECT a AND b FROM t",
            "AND/OR/NOT expression in a column expression",
        ),
        (
            "SELECT a FROM t WHERE a IS DISTINCT FROM b",
            "IS DISTINCT FROM",
        ),
    ];
    for (sql, expected) in cases {
        let err = query_expr_parse(sql).expect_err(sql).to_string();
        assert!(err.contains(expected), "{sql}: {err}");
        assert!(
            !err.chars().last().is_some_and(|c| c.is_ascii_digit()),
            "{sql}: message ends in a raw number: {err}"
        );
    }
}

#[test]
fn test_round_trip_deparse_cases() {
    // Each statement deparses back to exactly its own text.
    #[rustfmt::skip]
    let cases = [
        ("query expr deparse simple", "SELECT id, name FROM users"),
        ("select deparse with where", "SELECT * FROM users WHERE id = 1"),
        ("select deparse distinct", "SELECT DISTINCT name FROM users"),
        ("select deparse multiple tables", "SELECT * FROM users, orders"),
        ("select deparse schema qualified", "SELECT * FROM public.users"),
        ("select deparse join", "SELECT first_name, last_name, film_id FROM actor a \
            JOIN film_actor fa ON a.actor_id = fa.actor_id \
            WHERE a.actor_id = 1"),
        ("select deparse left join", "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1"),
        ("select deparse right join", "SELECT a.id, b.name FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1"),
        ("select deparse mixed joins", "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1"),
        ("select deparse table values", "SELECT fa.actor_id \
        FROM (VALUES ('1', '2'), ('3', '4')) fa(actor_id, film_id) \
        WHERE a.actor_id = 1"),
        ("order by deparse asc", "SELECT * FROM users ORDER BY name ASC"),
        ("order by deparse desc", "SELECT * FROM users ORDER BY age DESC"),
        ("order by deparse multiple", "SELECT * FROM users ORDER BY last_name ASC, first_name DESC"),
    ];
    for (label, sql) in cases {
        assert_eq!(deparsed(sql), sql, "{label}");
    }
}

#[test]
fn test_deparse_keeps_parentheses_cases() {
    // NOT binds tighter than AND/OR, and AND tighter than OR, so grouping
    // parentheses must survive deparse or the predicate changes meaning.
    #[rustfmt::skip]
    let cases = [
        // NOT has higher precedence than AND, so NOT (a AND b) must keep parens.
        // Without them: NOT a AND b → (NOT a) AND b — different semantics.
        ("deparse not with and", "SELECT * FROM t WHERE NOT (x = 1 AND y = 2)", "NOT (x = 1 AND y = 2)"),
        // Same issue: NOT (a OR b) must keep parens.
        ("deparse not with or", "SELECT * FROM t WHERE NOT (x = 1 OR y = 2)", "NOT (x = 1 OR y = 2)"),
        // When an OR expression is nested inside an AND chain, parentheses must
        // be emitted to preserve semantics. Without them:
        //   a AND (b OR c) AND d  →  a AND b OR c AND d
        // which changes evaluation due to AND binding tighter than OR.
        ("deparse or inside and preserves parentheses", "SELECT * FROM t WHERE (x = 1 OR y = 2) AND z = 3", "(x = 1 OR y = 2)"),
    ];
    for (label, sql, fragment) in cases {
        let text = deparsed(sql);
        assert!(
            text.contains(fragment),
            "{label}: expected {fragment:?} in {text:?}"
        );
    }
}

#[test]
fn test_has_subqueries_cases() {
    #[rustfmt::skip]
    let cases = [
        ("has subqueries in select list", "SELECT id, (SELECT x FROM other WHERE id = 1) as val FROM t", true),
        ("has subqueries in from clause", "SELECT * FROM (SELECT id FROM users) sub", true),
        ("has subqueries in join", "SELECT * FROM a JOIN (SELECT id FROM b) sub ON a.id = sub.id", true),
        ("has subqueries in where clause", "SELECT * FROM t WHERE id IN (SELECT id FROM other)", true),
        ("has subqueries no subquery", "SELECT id, name FROM users WHERE active = true", false),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(
            parse_select(sql).has_subqueries(),
            expected,
            "{label}: {sql}"
        );
    }
}

#[test]
fn test_select_nodes_with_source_cases() {
    use UpdateQuerySource::{FromClause, Subquery};
    // The outer SELECT is FromClause; each subquery branch carries its kind.
    #[rustfmt::skip]
    let cases = [
        ("select nodes with source simple", "SELECT id FROM users WHERE id = 1", vec![FromClause]),
        // Both branches of a UNION are FromClause
        ("select nodes with source union", "SELECT id FROM users UNION SELECT id FROM admins", vec![FromClause, FromClause]),
        // Outer SELECT (FromClause) + IN subquery (Inclusion)
        ("select nodes with source where in", "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)", vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
        // NOT IN is parsed as SubLinkType::All (already Exclusion)
        // or as NOT wrapping Any (negated → Exclusion)
        ("select nodes with source not in", "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)", vec![FromClause, Subquery(SubqueryKind::Exclusion)]),
        ("select nodes with source exists", "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)", vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
        ("select nodes with source not exists", "SELECT * FROM orders WHERE NOT EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)", vec![FromClause, Subquery(SubqueryKind::Exclusion)]),
        ("select nodes with source scalar in where", "SELECT * FROM users WHERE age > (SELECT AVG(age) FROM users)", vec![FromClause, Subquery(SubqueryKind::Scalar)]),
        ("select nodes with source scalar in select", "SELECT id, (SELECT COUNT(*) FROM orders WHERE orders.user_id = users.id) FROM users", vec![FromClause, Subquery(SubqueryKind::Scalar)]),
        ("select nodes with source from subquery", "SELECT * FROM (SELECT id FROM users) sub", vec![FromClause, Subquery(SubqueryKind::Inclusion)]),
    ];
    for (label, sql, expected) in cases {
        let query = parse_query(sql);
        let sources: Vec<UpdateQuerySource> = query
            .select_nodes_with_source()
            .into_iter()
            .map(|(_, source)| source)
            .collect();
        assert_eq!(sources, expected, "{label}: {sql}");
    }
}

/// Expected properties of a parsed function call; `None` fields are not
/// checked.
#[derive(Default)]
struct FunctionExpect {
    name: Option<&'static str>,
    args: Option<usize>,
    agg_star: Option<bool>,
    agg_distinct: Option<bool>,
    has_filter: Option<bool>,
    agg_order: Option<&'static [OrderDirection]>,
    agg_order_len: Option<usize>,
    /// Name of the function the first argument calls.
    first_arg_function: Option<&'static str>,
    window: Option<WindowExpect>,
}

/// Expected OVER clause; `None` fields are not checked.
#[derive(Default)]
struct WindowExpect {
    partitions: Option<usize>,
    order: Option<&'static [OrderDirection]>,
    order_len: Option<usize>,
}

fn field_check<T: PartialEq + std::fmt::Debug>(
    context: &str,
    field: &str,
    expected: Option<T>,
    actual: T,
) {
    if let Some(expected) = expected {
        assert_eq!(actual, expected, "{context}: {field}");
    }
}

fn directions(clauses: &[OrderByClause]) -> Vec<OrderDirection> {
    clauses.iter().map(|c| c.direction.clone()).collect()
}

impl FunctionExpect {
    fn check(&self, func: &FunctionCall, context: &str) {
        field_check(context, "name", self.name, func.name.as_str());
        field_check(context, "argument count", self.args, func.args.len());
        field_check(context, "agg_star", self.agg_star, func.agg_star);
        field_check(
            context,
            "agg_distinct",
            self.agg_distinct,
            func.agg_distinct,
        );
        field_check(
            context,
            "FILTER",
            self.has_filter,
            func.agg_filter.is_some(),
        );
        field_check(
            context,
            "aggregate ORDER BY",
            self.agg_order.map(<[_]>::to_vec),
            directions(&func.agg_order),
        );
        field_check(
            context,
            "aggregate ORDER BY count",
            self.agg_order_len,
            func.agg_order.len(),
        );
        field_check(
            context,
            "first argument's function",
            self.first_arg_function,
            func.args
                .first()
                .and_then(nested_function_name)
                .unwrap_or(""),
        );
        if let Some(window) = &self.window {
            let over = func
                .over
                .as_ref()
                .unwrap_or_else(|| panic!("{context}: expected OVER"));
            field_check(
                context,
                "PARTITION BY count",
                window.partitions,
                over.partition_by.len(),
            );
            field_check(
                context,
                "window ORDER BY",
                window.order.map(<[_]>::to_vec),
                directions(&over.order_by),
            );
            field_check(
                context,
                "window ORDER BY count",
                window.order_len,
                over.order_by.len(),
            );
        }
    }
}

#[test]
fn test_select_function_cases() {
    use OrderDirection::{Asc, Desc};
    // COALESCE / GREATEST / LEAST / NULLIF parse as plain function calls.
    #[rustfmt::skip]
    let cases = [
        ("count(*)",                 "SELECT COUNT(*) FROM users",
         FunctionExpect { name: Some("count"), agg_star: Some(true), args: Some(0), ..Default::default() }),
        ("count(column)",            "SELECT COUNT(id) FROM users",
         FunctionExpect { name: Some("count"), agg_star: Some(false), args: Some(1), ..Default::default() }),
        ("count(DISTINCT)",          "SELECT COUNT(DISTINCT status) FROM orders",
         FunctionExpect { name: Some("count"), agg_distinct: Some(true), ..Default::default() }),
        ("sum",                      "SELECT SUM(amount) FROM orders WHERE tenant_id = 1",
         FunctionExpect { name: Some("sum"), ..Default::default() }),
        ("nested round(avg())",      "SELECT ROUND(AVG(value)) FROM data",
         FunctionExpect { name: Some("round"), args: Some(1), first_arg_function: Some("avg"), ..Default::default() }),
        ("coalesce",                 "SELECT COALESCE(name, 'unknown') FROM users",
         FunctionExpect { name: Some("coalesce"), args: Some(2), ..Default::default() }),
        ("coalesce(max(), 0)",       "SELECT COALESCE(MAX(value), 0) FROM data",
         FunctionExpect { name: Some("coalesce"), args: Some(2), first_arg_function: Some("max"), ..Default::default() }),
        ("greatest",                 "SELECT GREATEST(a, b, c) FROM t",
         FunctionExpect { name: Some("greatest"), args: Some(3), ..Default::default() }),
        ("least",                    "SELECT LEAST(a, b) FROM t",
         FunctionExpect { name: Some("least"), args: Some(2), ..Default::default() }),
        ("nullif",                   "SELECT NULLIF(status, 'deleted') FROM items",
         FunctionExpect { name: Some("nullif"), args: Some(2), ..Default::default() }),
        ("window ORDER BY",          "SELECT sum(amount) OVER (ORDER BY date) FROM orders",
         FunctionExpect { name: Some("sum"), window: Some(WindowExpect { partitions: Some(0), order_len: Some(1), ..Default::default() }), ..Default::default() }),
        ("window PARTITION BY",      "SELECT sum(amount) OVER (PARTITION BY category ORDER BY date) FROM orders",
         FunctionExpect { window: Some(WindowExpect { partitions: Some(1), order_len: Some(1), ..Default::default() }), ..Default::default() }),
        ("row_number()",             "SELECT row_number() OVER (ORDER BY id) FROM users",
         FunctionExpect { name: Some("row_number"), args: Some(0), window: Some(WindowExpect::default()), ..Default::default() }),
        ("window multi ORDER BY",    "SELECT sum(x) OVER (ORDER BY a ASC, b DESC) FROM t",
         FunctionExpect { window: Some(WindowExpect { order: Some(&[Asc, Desc]), ..Default::default() }), ..Default::default() }),
        ("aggregate ORDER BY",       "SELECT string_agg(name, ', ' ORDER BY name) FROM t",
         FunctionExpect { name: Some("string_agg"), agg_order: Some(&[Asc]), ..Default::default() }),
        ("DISTINCT with ORDER BY",   "SELECT string_agg(DISTINCT name, ', ' ORDER BY name) FROM t",
         FunctionExpect { agg_distinct: Some(true), agg_order_len: Some(1), ..Default::default() }),
        ("aggregate multi ORDER BY", "SELECT array_agg(x ORDER BY y ASC, z DESC) FROM t",
         FunctionExpect { name: Some("array_agg"), agg_order: Some(&[Asc, Desc]), ..Default::default() }),
        ("FILTER",                   "SELECT count(*) FILTER (WHERE x = 1) FROM t",
         FunctionExpect { agg_star: Some(true), has_filter: Some(true), ..Default::default() }),
    ];
    for (label, sql, expect) in cases {
        expect.check(&only_function(sql), &format!("{label}: {sql}"));
    }
}

#[test]
fn test_parameterized_where_cases() {
    use LiteralValue::{Boolean, Parameter};
    // WHERE literals in order; each statement also deparses back to itself.
    #[rustfmt::skip]
    let cases = [
        ("single parameter",       "SELECT * FROM users WHERE id = $1",                      vec![Parameter("$1".into())]),
        ("two parameters",         "SELECT * FROM users WHERE name = $1 AND age > $2",       vec![Parameter("$1".into()), Parameter("$2".into())]),
        ("parameter and literal",  "SELECT * FROM users WHERE name = $1 AND active = true",  vec![Parameter("$1".into()), Boolean(true)]),
    ];
    for (label, sql, expected) in cases {
        let ast = parse_query(sql);
        let where_clause = ast.where_clause().expect("WHERE");
        let literals: Vec<LiteralValue> = where_clause.nodes::<LiteralValue>().cloned().collect();
        assert_eq!(literals, expected, "{label}: WHERE literals of {sql}");
        assert_eq!(deparsed_node(&ast), sql, "{label}: deparse");
    }
}

#[test]
fn test_multi_expr_deparse_cases() {
    use LiteralValue::{Integer, String};
    #[rustfmt::skip]
    let cases = [
        ("IN",     MultiOp::In,    "status", vec![String("active".into()), String("pending".into())], "status IN ('active', 'pending')"),
        ("NOT IN", MultiOp::NotIn, "id",     vec![Integer(1), Integer(2), Integer(3)],                 "id NOT IN (1, 2, 3)"),
    ];
    for (label, op, column, values, expected) in cases {
        let mut exprs = vec![WhereExpr::Scalar(ScalarExpr::Column(ColumnNode {
            table: None,
            column: EcoString::from(column),
        }))];
        exprs.extend(
            values
                .into_iter()
                .map(|v| WhereExpr::Scalar(ScalarExpr::Literal(v))),
        );
        assert_eq!(deparsed_node(&MultiExpr { op, exprs }), expected, "{label}");
    }
}

#[test]
fn test_order_by_direction_cases() {
    use OrderDirection::{Asc, Desc};
    // An ORDER BY without a direction defaults to ASC.
    #[rustfmt::skip]
    let cases = [
        ("explicit ASC",  "SELECT * FROM users ORDER BY name ASC", Some("name"), Asc),
        ("explicit DESC", "SELECT * FROM users ORDER BY age DESC", Some("age"),  Desc),
        ("default",       "SELECT * FROM users ORDER BY name",     None,         Asc),
    ];
    for (label, sql, column, direction) in cases {
        let ast = parse_query(sql);
        let [clause] = ast.order_by.as_slice() else {
            panic!("{label}: expected one ORDER BY clause in {sql}");
        };
        assert_eq!(clause.direction, direction, "{label}: direction");
        if let Some(column) = column {
            let ScalarExpr::Column(col) = &clause.expr else {
                panic!("{label}: expected a column in {sql}");
            };
            assert_eq!(
                (col.table.as_deref(), col.column.as_str()),
                (None, column),
                "{label}"
            );
        }
    }
}

#[test]
fn test_case_shape_cases() {
    // Simple CASE carries the tested expression as `arg`; searched CASE has none.
    #[rustfmt::skip]
    let cases = [
        ("searched",     "SELECT CASE WHEN status = 'active' THEN 1 ELSE 0 END FROM items",                    false, 1, true),
        ("simple",       "SELECT CASE status WHEN 'active' THEN 1 WHEN 'pending' THEN 2 ELSE 0 END FROM items", true,  2, true),
        ("without ELSE", "SELECT CASE WHEN x > 0 THEN 'positive' END FROM items",                               false, 1, false),
    ];
    for (label, sql, has_arg, whens, has_default) in cases {
        let select = parse_select(sql);
        let Some(ScalarExpr::Case(case)) = select_columns(&select)[0].expr() else {
            panic!("{label}: expected a CASE in {sql}");
        };
        let actual = (case.arg.is_some(), case.whens.len(), case.default.is_some());
        assert_eq!(
            actual,
            (has_arg, whens, has_default),
            "{label}: (arg, WHENs, ELSE) of {sql}"
        );
    }
}

#[test]
fn test_select_nodes_branch_count_cases() {
    // Every SELECT a query contains: set-operation branches, FROM derived
    // tables, and WHERE / SELECT-list subqueries.
    #[rustfmt::skip]
    let cases = [
        ("simple SELECT",        "SELECT id FROM users WHERE id = 1",                                                  1),
        ("nested UNION",         "SELECT id FROM a UNION SELECT id FROM b UNION SELECT id FROM c",                     3),
        ("FROM subquery",        "SELECT * FROM (SELECT id FROM users) sub",                                           2),
        ("WHERE IN subquery",    "SELECT * FROM users WHERE id IN (SELECT user_id FROM active)",                      2),
        ("EXISTS subquery",      "SELECT * FROM orders WHERE EXISTS (SELECT 1 FROM items WHERE items.order_id = orders.id)", 2),
        ("SELECT-list subquery", "SELECT id, (SELECT name FROM users WHERE users.id = orders.user_id) FROM orders",    2),
        ("nested subqueries",    "SELECT * FROM (SELECT id FROM users WHERE id IN (SELECT user_id FROM active)) sub", 3),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(
            parse_query(sql).select_nodes().len(),
            expected,
            "{label}: {sql}"
        );
    }
}
