use crate::cache::{UpdateQuerySource, query::outer_join_optional_tables};
use crate::query::ast::LiteralValue;
use crate::query::resolved::{
    ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn,
    ResolvedSelectColumns, ResolvedTableNode,
};
use crate::query::transform::resolved_select_node_update_replace;

/// Generate queries used to check if a DML statement applies to a given table.
/// Returns one update query per (table, branch) pair, operating entirely in the
/// resolved AST domain.
///
/// For simple SELECT queries, each table gets one update query derived from that SELECT.
/// For set operations (UNION/INTERSECT/EXCEPT), each table gets an update query derived
/// from just the branch that contains it (not the full set operation).
///
/// This approach ensures that CDC handling only checks if a changed row matches the
/// specific branch conditions, not the entire set operation structure.
pub fn query_table_update_queries(
    resolved: &ResolvedQueryExpr,
) -> Vec<(&ResolvedTableNode, ResolvedQueryExpr, UpdateQuerySource)> {
    let mut result = Vec::new();

    let select_true = ResolvedSelectColumns::Columns(vec![ResolvedSelectColumn {
        expr: ResolvedScalarExpr::Literal(LiteralValue::Boolean(true)),
        alias: None,
    }]);

    for (branch, source) in resolved.select_nodes_with_source() {
        let (terminal, non_terminal) = outer_join_optional_tables(branch);

        // For each direct table in this branch (not inside subqueries).
        // Tables inside subqueries are handled by their own inner branch.
        for table in branch.direct_table_nodes() {
            // Create update query from just this branch (not full query)
            let update_select = resolved_select_node_update_replace(branch, select_true.clone());
            let update_query = ResolvedQueryExpr {
                body: ResolvedQueryBody::Select(Box::new(update_select)),
                order_by: vec![],
                limit: None,
            };

            // Outer join optional-side tables need specialized CDC handling.
            let effective_source = if source == UpdateQuerySource::FromClause {
                if non_terminal.contains(table.name.as_str()) {
                    UpdateQuerySource::OuterJoinOptional
                } else if terminal.contains(table.name.as_str()) {
                    UpdateQuerySource::OuterJoinTerminal
                } else {
                    source
                }
            } else {
                source
            };

            result.push((table, update_query, effective_source));
        }
    }

    result
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::cache::SubqueryKind;
    use crate::cache::query::CacheableQuery;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{Deparse, query_expr_parse};
    use crate::query::resolved::query_expr_resolve;

    use super::*;

    /// Helper to parse SQL and return a CacheableQuery
    fn parse_cacheable(sql: &str) -> CacheableQuery {
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        CacheableQuery::try_new(&query_expr, &HashMap::new()).expect("query to be cacheable")
    }

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: u32, column_names: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, col_name)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*col_name).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
                type_oid: if is_pk { 23 } else { 25 },
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![column_names[0].into()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Build a comprehensive table catalog covering all tables used in tests.
    fn test_tables() -> BiHashMap<TableMetadata> {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table(
            "a",
            1,
            &["id", "name", "val", "tenant_id", "status"],
        ));
        tables.insert_overwrite(test_table(
            "b",
            2,
            &[
                "id",
                "name",
                "a_id",
                "x",
                "val",
                "b_id",
                "user_id",
                "tenant_id",
                "status",
            ],
        ));
        tables.insert_overwrite(test_table("c", 3, &["id", "x", "b_id", "val", "tenant_id"]));
        tables.insert_overwrite(test_table("d", 4, &["id"]));
        tables.insert_overwrite(test_table(
            "users",
            10,
            &[
                "id",
                "name",
                "email",
                "user_id",
                "status",
                "active",
                "uuid",
                "age",
                "tenant_id",
            ],
        ));
        tables.insert_overwrite(test_table("location", 11, &["user_id"]));
        tables.insert_overwrite(test_table(
            "orders",
            12,
            &["id", "user_id", "total", "product_id"],
        ));
        tables.insert_overwrite(test_table("admins", 13, &["id", "tenant_id"]));
        tables.insert_overwrite(test_table("active_users", 14, &["id", "user_id"]));
        tables.insert_overwrite(test_table("banned_users", 15, &["id", "user_id"]));
        tables.insert_overwrite(test_table(
            "products",
            16,
            &["id", "store_id", "name", "category_id"],
        ));
        tables.insert_overwrite(test_table("stores", 17, &["id", "region_id"]));
        tables.insert_overwrite(test_table("regions", 18, &["id", "name"]));
        tables.insert_overwrite(test_table("excluded_regions", 19, &["id", "region_id"]));
        tables.insert_overwrite(test_table("categories", 20, &["id", "name"]));
        tables.insert_overwrite(test_table("excluded_categories", 21, &["id"]));
        tables.insert_overwrite(test_table(
            "blacklist",
            22,
            &["id", "product_id", "category_id"],
        ));
        tables
    }

    /// Parse SQL, resolve, and call query_table_update_queries.
    fn update_queries(sql: &str) -> Vec<(String, ResolvedQueryExpr, UpdateQuerySource)> {
        let tables = test_tables();
        let cacheable = parse_cacheable(sql);
        let resolved = query_expr_resolve(&cacheable.query, &tables, &["public"]).expect("resolve");
        query_table_update_queries(&resolved)
            .into_iter()
            .map(|(t, q, s)| (t.name.to_string(), q, s))
            .collect()
    }

    #[test]
    fn test_query_table_update_query_simple_select() {
        let result = update_queries("SELECT id, name, email FROM users WHERE id = 1");

        assert_eq!(result.len(), 1);

        let mut buf = String::new();
        let new_sql = result[0].1.deparse(&mut buf);

        assert_eq!(
            new_sql,
            "SELECT true FROM public.users WHERE public.users.id = 1"
        );
    }

    #[test]
    fn test_query_table_update_query_simple_join() {
        let result = update_queries(
            "SELECT id, name, email FROM users \
             JOIN location ON location.user_id = users.user_id \
             WHERE users.user_id = 1",
        );

        assert_eq!(result.len(), 2);

        let expected = "SELECT true FROM public.users JOIN public.location ON public.location.user_id = public.users.user_id WHERE public.users.user_id = 1";

        let mut update1 = String::new();
        result[0].1.deparse(&mut update1);
        assert_eq!(update1, expected);

        let mut update2 = String::new();
        result[1].1.deparse(&mut update2);
        assert_eq!(update2, expected);
    }

    #[test]
    fn test_query_table_update_query_three_table_join() {
        let result = update_queries(
            "SELECT * FROM users \
             JOIN location ON location.user_id = users.user_id \
             JOIN orders ON orders.user_id = users.user_id \
             WHERE users.user_id = 1",
        );

        assert_eq!(result.len(), 3, "Should have 3 update queries for 3 tables");

        // All three queries should produce the same SQL (the full join query with SELECT true)
        let mut update1 = String::new();
        result[0].1.deparse(&mut update1);
        assert!(update1.contains("SELECT true"));
        assert!(update1.contains("JOIN"));

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"location"));
        assert!(table_names.contains(&"orders"));
    }

    #[test]
    fn test_query_table_update_query_four_table_join() {
        let result = update_queries(
            "SELECT * FROM a \
             JOIN b ON a.id = b.id \
             JOIN c ON b.id = c.id \
             JOIN d ON c.id = d.id \
             WHERE a.id = 1",
        );

        assert_eq!(result.len(), 4, "Should have 4 update queries for 4 tables");

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));
        assert!(table_names.contains(&"d"));
    }

    // ==================== Set Operation Update Query Tests ====================
    //
    // Update queries for set operations are now branch-specific:
    // - Each table gets an update query derived from just its branch
    // - No UNION structure is preserved in update queries
    // - This enables CDC to check individual branch predicates

    #[test]
    fn test_query_table_update_query_union() {
        let result = update_queries(
            "SELECT id FROM users WHERE tenant_id = 1 \
             UNION SELECT id FROM admins WHERE tenant_id = 1",
        );

        assert_eq!(result.len(), 2, "Should have 2 update queries for 2 tables");

        // Verify each table is associated with an update query
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"users"));
        assert!(table_names.contains(&"admins"));

        // Update queries should be branch-specific (no UNION)
        for (table_name, query, _) in &result {
            let mut sql = String::new();
            query.deparse(&mut sql);
            assert!(
                !sql.contains("UNION"),
                "Update query for {table_name} should NOT contain UNION: {sql}",
            );
            assert!(
                sql.contains("SELECT true"),
                "Update query should have SELECT true: {sql}",
            );
            assert!(
                sql.contains(table_name.as_str()),
                "Update query for {table_name} should reference that table: {sql}",
            );
        }
    }

    #[test]
    fn test_query_table_update_query_nested_union() {
        let result = update_queries(
            "SELECT id FROM a WHERE tenant_id = 1 \
             UNION SELECT id FROM b WHERE tenant_id = 1 \
             UNION SELECT id FROM c WHERE tenant_id = 1",
        );

        assert_eq!(result.len(), 3, "Should have 3 update queries for 3 tables");

        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));

        // Verify each update query is branch-specific (no UNION)
        for (table_name, query, _) in &result {
            let mut sql = String::new();
            query.deparse(&mut sql);
            assert!(
                !sql.contains("UNION"),
                "Update query for {table_name} should NOT contain UNION: {sql}",
            );
        }
    }

    #[test]
    fn test_query_table_update_query_union_with_join() {
        let result = update_queries(
            "SELECT a.id FROM a JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 \
             UNION SELECT id FROM c WHERE tenant_id = 1",
        );

        assert_eq!(result.len(), 3, "Should have 3 update queries (a, b, c)");

        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(table_names.contains(&"a"));
        assert!(table_names.contains(&"b"));
        assert!(table_names.contains(&"c"));
    }

    // ==================== CTE Update Query Tests ====================

    #[test]
    fn test_cte_update_queries() {
        let result = update_queries(
            "WITH active_users AS (SELECT id, name FROM users WHERE status = 'active') \
             SELECT active_users.id, orders.total \
             FROM active_users \
             JOIN orders ON orders.user_id = active_users.id",
        );

        // The CTE body contains "users" and the main query joins with "orders".
        // select_nodes_with_source traverses into CTE bodies via CteRef,
        // so we should get tables from both the CTE body and the main query.
        let table_names: Vec<_> = result.iter().map(|(t, _, _)| t.as_str()).collect();
        assert!(
            table_names.contains(&"users"),
            "Should have update query for 'users' table from CTE body: {table_names:?}"
        );
        assert!(
            table_names.contains(&"orders"),
            "Should have update query for 'orders' table from main query: {table_names:?}"
        );
    }

    // ==================== UpdateQuerySource Tests ====================

    #[test]
    fn test_update_query_source_simple() {
        let result = update_queries("SELECT id FROM users WHERE id = 1");

        assert_eq!(result.len(), 1);
        assert_eq!(result[0].2, UpdateQuerySource::FromClause);
    }

    #[test]
    fn test_update_query_source_join() {
        let result = update_queries(
            "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id WHERE a.id = 1",
        );

        assert_eq!(result.len(), 2);
        // Both tables in a JOIN are FromClause
        assert!(
            result
                .iter()
                .all(|(_, _, src)| *src == UpdateQuerySource::FromClause),
            "All JOIN tables should be FromClause"
        );
    }

    #[test]
    fn test_update_query_source_where_in() {
        let result =
            update_queries("SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)");

        assert_eq!(result.len(), 2);

        let users = result.iter().find(|(t, _, _)| t == "users").unwrap();
        assert_eq!(users.2, UpdateQuerySource::FromClause);

        let active = result.iter().find(|(t, _, _)| t == "active_users").unwrap();
        assert_eq!(
            active.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion)
        );
    }

    #[test]
    fn test_update_query_source_not_in() {
        let result = update_queries(
            "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",
        );

        assert_eq!(result.len(), 2);

        let banned = result.iter().find(|(t, _, _)| t == "banned_users").unwrap();
        assert_eq!(
            banned.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT IN subquery table should be Exclusion"
        );
    }

    #[test]
    fn test_update_query_source_cte() {
        let result = update_queries(
            "WITH active AS (SELECT id FROM users WHERE active = true) \
             SELECT * FROM active",
        );

        // CTE body "users" should be Subquery(Inclusion)
        let users = result.iter().find(|(t, _, _)| t == "users").unwrap();
        assert_eq!(
            users.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "CTE body table should be Subquery(Inclusion)"
        );
    }

    #[test]
    fn test_update_query_source_from_subquery() {
        let result = update_queries("SELECT * FROM (SELECT id FROM users WHERE id = 1) sub");

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "FROM subquery table should be Subquery(Inclusion)"
        );
    }

    #[test]
    fn test_update_query_source_nested_in_in() {
        // Double-nested: IN inside IN — all inner tables are Inclusion
        let result = update_queries(
            "SELECT name FROM products \
             WHERE store_id IN ( \
                 SELECT id FROM stores \
                 WHERE region_id IN ( \
                     SELECT id FROM regions WHERE name = 'East' \
                 ) \
             )",
        );

        assert_eq!(result.len(), 3);

        let products = result.iter().find(|(t, _, _)| t == "products").unwrap();
        assert_eq!(products.2, UpdateQuerySource::FromClause);

        let stores = result.iter().find(|(t, _, _)| t == "stores").unwrap();
        assert_eq!(
            stores.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "IN subquery table should be Inclusion"
        );

        let regions = result.iter().find(|(t, _, _)| t == "regions").unwrap();
        assert_eq!(
            regions.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "Nested IN inside IN should still be Inclusion"
        );
    }

    #[test]
    fn test_update_query_source_nested_not_in_inside_in() {
        // NOT IN nested inside IN:
        // The outer IN context is not negated, so the inner NOT IN is Exclusion
        let result = update_queries(
            "SELECT name FROM products \
             WHERE store_id IN ( \
                 SELECT id FROM stores \
                 WHERE region_id NOT IN ( \
                     SELECT region_id FROM excluded_regions \
                 ) \
             )",
        );

        assert_eq!(result.len(), 3);

        let products = result.iter().find(|(t, _, _)| t == "products").unwrap();
        assert_eq!(products.2, UpdateQuerySource::FromClause);

        let stores = result.iter().find(|(t, _, _)| t == "stores").unwrap();
        assert_eq!(
            stores.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "Outer IN subquery table should be Inclusion"
        );

        let excluded = result
            .iter()
            .find(|(t, _, _)| t == "excluded_regions")
            .unwrap();
        assert_eq!(
            excluded.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT IN inside IN should be Exclusion"
        );
    }

    #[test]
    fn test_update_query_source_nested_in_inside_not_in() {
        // IN nested inside NOT IN:
        // The NOT IN sets negated=true, so the inner IN inherits negated → Exclusion
        let result = update_queries(
            "SELECT name FROM products \
             WHERE id NOT IN ( \
                 SELECT product_id FROM blacklist \
                 WHERE category_id IN ( \
                     SELECT id FROM categories WHERE name = 'Restricted' \
                 ) \
             )",
        );

        assert_eq!(result.len(), 3);

        let products = result.iter().find(|(t, _, _)| t == "products").unwrap();
        assert_eq!(products.2, UpdateQuerySource::FromClause);

        let blacklist = result.iter().find(|(t, _, _)| t == "blacklist").unwrap();
        assert_eq!(
            blacklist.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "NOT IN subquery table should be Exclusion"
        );

        let categories = result.iter().find(|(t, _, _)| t == "categories").unwrap();
        assert_eq!(
            categories.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "IN inside NOT IN should be Exclusion (negated flips Inclusion → Exclusion)"
        );
    }

    #[test]
    fn test_update_query_source_nested_not_in_inside_not_in() {
        // NOT IN inside NOT IN:
        // Outer NOT IN sets negated=true, inner NOT IN (ALL) with negated=true → Inclusion
        // Double negation: NOT IN under NOT IN flips back to Inclusion
        let result = update_queries(
            "SELECT name FROM products \
             WHERE id NOT IN ( \
                 SELECT product_id FROM blacklist \
                 WHERE category_id NOT IN ( \
                     SELECT id FROM excluded_categories \
                 ) \
             )",
        );

        assert_eq!(result.len(), 3);

        let blacklist = result.iter().find(|(t, _, _)| t == "blacklist").unwrap();
        assert_eq!(
            blacklist.2,
            UpdateQuerySource::Subquery(SubqueryKind::Exclusion),
            "Outer NOT IN table should be Exclusion"
        );

        let excluded = result
            .iter()
            .find(|(t, _, _)| t == "excluded_categories")
            .unwrap();
        assert_eq!(
            excluded.2,
            UpdateQuerySource::Subquery(SubqueryKind::Inclusion),
            "NOT IN inside NOT IN should be Inclusion (double negation)"
        );
    }

    // ==================== Outer Join Source Tests ====================

    #[test]
    fn test_update_query_source_terminal_left_join() {
        let result = update_queries(
            "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1",
        );

        assert_eq!(result.len(), 2);

        let a = result.iter().find(|(t, _, _)| t == "a").unwrap();
        assert_eq!(
            a.2,
            UpdateQuerySource::FromClause,
            "preserved side is FromClause"
        );

        let b = result.iter().find(|(t, _, _)| t == "b").unwrap();
        assert_eq!(
            b.2,
            UpdateQuerySource::OuterJoinTerminal,
            "terminal optional side is OuterJoinTerminal"
        );
    }

    #[test]
    fn test_update_query_source_non_terminal_left_join() {
        let result = update_queries(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status = 'active'",
        );

        assert_eq!(result.len(), 2);

        let a = result.iter().find(|(t, _, _)| t == "a").unwrap();
        assert_eq!(
            a.2,
            UpdateQuerySource::FromClause,
            "preserved side should be FromClause"
        );

        let b = result.iter().find(|(t, _, _)| t == "b").unwrap();
        assert_eq!(
            b.2,
            UpdateQuerySource::OuterJoinOptional,
            "non-terminal optional side should be OuterJoinOptional"
        );
    }

    #[test]
    fn test_update_query_source_chained_outer_joins() {
        let result = update_queries(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.x = c.x WHERE a.id = 1",
        );

        assert_eq!(result.len(), 3);

        let a = result.iter().find(|(t, _, _)| t == "a").unwrap();
        assert_eq!(a.2, UpdateQuerySource::FromClause);

        // b is non-terminal: its columns appear in the outer LEFT JOIN's condition
        let b = result.iter().find(|(t, _, _)| t == "b").unwrap();
        assert_eq!(
            b.2,
            UpdateQuerySource::OuterJoinOptional,
            "b is non-terminal (used in outer join condition)"
        );

        // c is terminal optional side: only appears in its own ON clause
        let c = result.iter().find(|(t, _, _)| t == "c").unwrap();
        assert_eq!(
            c.2,
            UpdateQuerySource::OuterJoinTerminal,
            "c is terminal optional side"
        );
    }

    #[test]
    fn test_update_query_source_mixed_inner_left() {
        let result = update_queries(
            "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1",
        );

        assert_eq!(result.len(), 3);

        let a = result.iter().find(|(t, _, _)| t == "a").unwrap();
        assert_eq!(a.2, UpdateQuerySource::FromClause, "a is INNER JOIN");

        let b = result.iter().find(|(t, _, _)| t == "b").unwrap();
        assert_eq!(b.2, UpdateQuerySource::FromClause, "b is INNER JOIN");

        let c = result.iter().find(|(t, _, _)| t == "c").unwrap();
        assert_eq!(
            c.2,
            UpdateQuerySource::OuterJoinTerminal,
            "c is terminal optional side"
        );
    }

    #[test]
    fn test_update_query_source_non_terminal_chained_join() {
        // b is optional side of LEFT JOIN, and b.val appears in downstream INNER JOIN
        let result = update_queries(
            "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.val = c.val WHERE a.id = 1",
        );

        assert_eq!(result.len(), 3);

        let b = result.iter().find(|(t, _, _)| t == "b").unwrap();
        assert_eq!(
            b.2,
            UpdateQuerySource::OuterJoinOptional,
            "b is non-terminal (used in downstream join condition)"
        );

        let a = result.iter().find(|(t, _, _)| t == "a").unwrap();
        assert_eq!(a.2, UpdateQuerySource::FromClause);

        let c = result.iter().find(|(t, _, _)| t == "c").unwrap();
        assert_eq!(
            c.2,
            UpdateQuerySource::FromClause,
            "c is not on optional side"
        );
    }
}
