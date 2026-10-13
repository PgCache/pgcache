//! Tables, aliases, columns and joins: what each FROM shape converts to.

#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use super::*;

// ============================================================================
// Tables, aliases and columns
// ============================================================================

#[test]
fn test_simple_select() {
    let tree = parse("SELECT id, name FROM users WHERE id = 1");
    let shape = (
        single_table(&tree),
        matches!(where_of(&tree).kind(), WhereKind::Binary(_)),
        tables(&tree),
        column_refs(select_of(&tree)),
    );
    assert_eq!(
        shape,
        (
            true,
            true,
            vec![(None, "users", None)],
            vec![(None, "id"), (None, "name")]
        )
    );
}

/// PGC-183: `SELECT $1 FROM …` must parse.
#[test]
fn test_select_paramref() {
    let tree = parse("SELECT $1 FROM users");
    let item_list = items(select_of(&tree));
    let [item] = item_list.as_slice() else {
        panic!("expected one item");
    };
    assert!(matches!(
        expr_of(*item).kind(),
        ScalarKind::Literal(literal) if literal.value() == &LiteralValue::Parameter("$1".into())
    ));
}

#[test]
fn test_select_star() {
    let tree = parse("SELECT * FROM products");
    assert!(single_table(&tree));
    assert!(select_of(&tree).where_clause().is_none());
    assert_eq!(tables(&tree), [(None, "products", None)]);
    let item_list = items(select_of(&tree));
    let [item] = item_list.as_slice() else {
        panic!("expected one item");
    };
    assert!(matches!(item, SelectItem::Star(star) if star.qualifier().is_none()));
}

/// A select item as written: `qualifier.column`, `column`, `*` or
/// `qualifier.*`; `expr` for anything else.
fn item_described(item: SelectItem<'_>) -> String {
    let (qualifier, name) = match item {
        SelectItem::Star(star) => (star.qualifier(), "*"),
        SelectItem::Expr(expr) => match expr.expr().kind() {
            ScalarKind::Column(column) => (column.table(), column.column().as_str()),
            _ => return "expr".to_owned(),
        },
    };
    match qualifier {
        Some(qualifier) => format!("{qualifier}.{name}"),
        None => name.to_owned(),
    }
}

fn items_described(select: Select<'_>) -> Vec<String> {
    items(select).into_iter().map(item_described).collect()
}

#[test]
fn test_star_cases() {
    #[rustfmt::skip]
    let cases = [
        ("bare star",            "SELECT * FROM products",                                               vec!["*"]),
        ("star then column",     "SELECT *, col FROM test",                                              vec!["*", "col"]),
        ("qualified star",       "SELECT t1.*, t2.col FROM test t1 JOIN test2 t2 ON t2.id = t1.id",      vec!["t1.*", "t2.col"]),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        assert_eq!(items_described(select_of(&tree)), expected, "{label}");
        assert_eq!(tree.sql(), sql, "{label}: renders as written");
    }
}

#[test]
fn test_table_schema_and_alias_cases() {
    // (sql, schema, name, alias, alias columns, select-list column refs)
    #[rustfmt::skip]
    let cases = [
        ("schema",   "SELECT id, name FROM test.users WHERE active = true",      Some("test"), "users", None,      0, vec![(None, "id"), (None, "name")]),
        ("alias",    "SELECT u.id, u.name FROM users u WHERE u.active = true",   None,         "users", Some("u"), 0, vec![(Some("u"), "id"), (Some("u"), "name")]),
        ("no alias", "SELECT id, name FROM users",                               None,         "users", None,      0, vec![(None, "id"), (None, "name")]),
    ];
    for (label, sql, schema, name, alias, alias_columns, columns) in cases {
        let tree = parse(sql);
        let table = only_table(select_of(&tree));
        let actual = (
            table.schema().map(EcoString::as_str),
            table.name().as_str(),
            table.alias().map(EcoString::as_str),
            table.alias_columns().len(),
            column_refs(select_of(&tree)),
        );
        assert_eq!(
            actual,
            (schema, name, alias, alias_columns, columns),
            "{label}: {sql}"
        );
    }
}

#[test]
fn test_column_alias() {
    let tree = parse("SELECT id as user_id, name as full_name FROM users");
    let items = items(select_of(&tree));
    assert_eq!(items.len(), 2);
    let described: Vec<(Option<&str>, &str)> = items
        .iter()
        .map(|item| {
            let ScalarKind::Column(column) = expr_of(*item).kind() else {
                panic!("expected a column");
            };
            (
                alias_of(*item).map(EcoString::as_str),
                column.column().as_str(),
            )
        })
        .collect();
    assert_eq!(
        described,
        [(Some("user_id"), "id"), (Some("full_name"), "name")]
    );
}

/// The base tables of a query in pre-order, aliases included: joins,
/// derived tables, scalar and predicate subqueries and CTE bodies are
/// all reached.
#[test]
fn test_tables_in_preorder_cases() {
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<(Option<&str>, &str, Option<&str>)>); 9] = [
        ("join with alias",       "SELECT * FROM invoice JOIN product p ON p.id = invoice.product_id",
         vec![(None, "invoice", None), (None, "product", Some("p"))]),
        ("three tables",          "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",
         vec![(None, "a", None), (None, "b", None), (None, "c", None)]),
        ("four tables",           "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id",
         vec![(None, "a", None), (None, "b", None), (None, "c", None), (None, "d", None)]),
        ("schema and aliases",    "SELECT first_name FROM actor a JOIN public.film_actor fa ON a.actor_id = fa.actor_id WHERE a.actor_id = 1",
         vec![(None, "actor", Some("a")), (Some("public"), "film_actor", Some("fa"))]),
        ("qualified table",       "SELECT * FROM public.users",
         vec![(Some("public"), "users", None)]),
        ("derived table",         "SELECT * FROM (SELECT id FROM users) sub",
         vec![(None, "users", None)]),
        ("scalar subquery",       "SELECT id, (SELECT COUNT(*) FROM orders) FROM users",
         vec![(None, "orders", None), (None, "users", None)]),
        ("WHERE subquery",        "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",
         vec![(None, "users", None), (None, "active_users", None)]),
        ("CTE body",              "WITH x AS (SELECT id FROM users) SELECT * FROM x",
         vec![(None, "users", None), (None, "users", None)]),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(tables(&parse(sql)), expected, "{label}: {sql}");
    }
}

/// The joins of a query in pre-order: the outermost first, so a chain of
/// joins lists the last written join type first.
#[test]
fn test_joins_in_preorder_cases() {
    use JoinType::{Inner, Left};
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<JoinType>); 4] = [
        ("one join",     "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",                                   vec![Inner]),
        ("two joins",    "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1",                vec![Inner, Inner]),
        ("three joins",  "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id",         vec![Inner, Inner, Inner]),
        ("mixed types",  "SELECT * FROM users u INNER JOIN orders o ON u.id = o.user_id LEFT JOIN payments p ON o.id = p.order_id", vec![Left, Inner]),
    ];
    for (label, sql, expected) in cases {
        let tree = parse(sql);
        let joins: Vec<JoinType> = kinds(&tree)
            .filter_map(|kind| match kind {
                NodeKind::Join { join_type, .. } => Some(*join_type),
                _ => None,
            })
            .collect();
        assert_eq!(joins, expected, "{label}: {sql}");
    }
}

#[test]
fn test_join_condition_columns() {
    let tree = parse("SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id WHERE a.id = 1");
    let outer = only_join(select_of(&tree));
    let JoinQual::On(outer_condition) = outer.qual() else {
        panic!("expected ON");
    };
    assert_eq!(
        columns_under(outer_condition),
        [(Some("b"), "id"), (Some("c"), "id")]
    );
    let Source::Join(inner) = outer.left() else {
        panic!("expected a nested join");
    };
    let JoinQual::On(inner_condition) = inner.qual() else {
        panic!("expected ON");
    };
    assert_eq!(
        columns_under(inner_condition),
        [(Some("a"), "id"), (Some("b"), "id")]
    );

    let tree = parse("SELECT * FROM users u JOIN orders o ON u.id = o.user_id");
    let join = only_join(select_of(&tree));
    assert_eq!(join.join_type(), JoinType::Inner);
    let JoinQual::On(condition) = join.qual() else {
        panic!("expected ON");
    };
    assert_eq!(
        columns_under(condition),
        [(Some("u"), "id"), (Some("o"), "user_id")]
    );
}

/// Every column reference a clause holds, in pre-order.
#[test]
fn test_columns_in_preorder_cases() {
    #[rustfmt::skip]
    let cases: [(&str, &str, Vec<(Option<&str>, &str)>); 5] = [
        ("WHERE",            "SELECT * FROM users WHERE name = 'john' AND age > 25",                     vec![(None, "name"), (None, "age")]),
        ("nested WHERE",     "SELECT * FROM users WHERE (name = 'john' OR name = 'jane') AND age > 18",  vec![(None, "name"), (None, "name"), (None, "age")]),
        ("select list",      "SELECT id, name FROM users",                                               vec![(None, "id"), (None, "name")]),
        ("list and WHERE",   "SELECT id, name FROM users WHERE active = true",                           vec![(None, "id"), (None, "name"), (None, "active")]),
        ("function argument", "SELECT COUNT(id) FROM users",                                             vec![(None, "id")]),
    ];
    for (label, sql, expected) in cases {
        assert_eq!(columns_under(parse(sql).root()), expected, "{label}: {sql}");
    }
    let tree = parse("SELECT * FROM users ORDER BY u.name ASC, age DESC");
    assert_eq!(
        columns_under(tree.root()),
        [(Some("u"), "name"), (None, "age")]
    );
}

#[test]
fn test_where_nodes_in_preorder() {
    let tree = parse("SELECT * FROM users WHERE name = 'john' AND age > 25 AND active = true");
    assert_eq!(
        literals_under(where_of(&tree)),
        [
            &LiteralValue::String("john".into()),
            &LiteralValue::Integer(25),
            &LiteralValue::Boolean(true)
        ]
    );
    let tree = parse("SELECT * FROM users WHERE name = 'john' AND age > 25");
    let ops: Vec<BinaryOp> = where_of(&tree)
        .node_ref()
        .subtree()
        .filter_map(|position| match position.kind() {
            NodeKind::Binary { op } => Some(*op),
            _ => None,
        })
        .collect();
    assert_eq!(ops, [BinaryOp::And, BinaryOp::Equal, BinaryOp::GreaterThan]);
    // The comparison, its column and its literal.
    let tree = parse("SELECT * FROM users WHERE name = 'john'");
    assert_eq!(where_of(&tree).node_ref().subtree_len(), 3);
    let tree = parse("SELECT * FROM users WHERE NOT active");
    assert!(
        matches!(where_of(&tree).kind(), WhereKind::Unary(unary) if unary.op() == UnaryOp::Not)
    );
    let tree = parse("SELECT * FROM t WHERE id IN (1, 2)");
    let WhereKind::Multi(multi) = where_of(&tree).kind() else {
        panic!("expected IN");
    };
    assert_eq!(multi.op(), MultiOp::In);
    assert_eq!(columns_under(where_of(&tree)), [(None, "id")]);
    assert_eq!(multi.exprs().len(), 3);
}
