use std::sync::Arc;

use smallvec::SmallVec;

use crate::cache::UpdateQuerySource;
use crate::cache::query::outer_join_optional_tables;
use crate::oid::Oid;
use crate::query::ast::{
    LiteralValue, NodeId, QueryBody, QueryExpr, ScalarExpr, SelectColumn, SelectColumns,
    SelectNode, TableNode, query_expr_refinalize,
};
use crate::query::resolve::{BindingsBuilder, ResolvedQuery};

/// One table's update query: the branch it sits in with the select list
/// replaced by `true`, as a query of its own.
pub struct TableUpdateQuery<'a> {
    pub table: &'a TableNode,
    pub relation_oid: Oid,
    pub bound: ResolvedQuery,
    pub source: UpdateQuerySource,
}

/// One update query per direct table of each branch: the branch with its
/// select list replaced by `true` and no ORDER BY or LIMIT, as the query CDC
/// evaluates a changed row against. The branch's FROM, WHERE, GROUP BY and
/// HAVING keep their nodes, so their bindings follow through the refinalize
/// remap (ADR-057).
pub fn query_table_update_queries(bound: &ResolvedQuery) -> Vec<TableUpdateQuery<'_>> {
    let mut result = Vec::new();
    for (branch, source) in bound.tree().select_nodes_with_source() {
        let (terminal, non_terminal) = outer_join_optional_tables(bound, branch);
        for table in branch.direct_table_nodes() {
            let relation_oid = bound
                .bindings()
                .source(table.id)
                .and_then(|slot| bound.bindings().slot(slot))
                .map(|slot| slot.table().relation_oid);
            let Some(relation_oid) = relation_oid else {
                debug_assert!(false, "a direct table of a resolved query has a slot");
                continue;
            };
            let mut tree = QueryExpr {
                id: NodeId::PENDING,
                ctes: Vec::new(),
                body: QueryBody::Select(Box::new(SelectNode {
                    id: NodeId::PENDING,
                    columns: SelectColumns::Columns(vec![SelectColumn::Expr {
                        expr: ScalarExpr::Literal(LiteralValue::Boolean(true)),
                        alias: None,
                    }]),
                    ..branch.clone()
                })),
                order_by: SmallVec::new(),
                limit: None,
            };
            let remap = query_expr_refinalize(&mut tree);
            let bindings = BindingsBuilder::from_remapped(bound.bindings(), &remap).finish();
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
            result.push(TableUpdateQuery {
                table,
                relation_oid,
                bound: ResolvedQuery::new(Arc::new(tree), bindings),
                source: effective_source,
            });
        }
    }
    result
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use std::collections::HashMap;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::cache::SubqueryKind;
    use crate::cache::query::CacheableQuery;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::query_expr_resolve;

    /// Helper to parse SQL and return a CacheableQuery
    fn parse_cacheable(sql: &str) -> CacheableQuery {
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        CacheableQuery::try_new(query_expr, &HashMap::new()).expect("query to be cacheable")
    }

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: Oid, column_names: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, col_name)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*col_name).into(),
                position: ColumnPosition::from_index(i).expect("column position in range"),
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

    /// Build a comprehensive table catalog covering all tables used in tests.
    fn test_tables() -> Catalog {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            test_table(
                "a",
                Oid::from_raw(1),
                &["id", "name", "val", "tenant_id", "status"],
            )
            .into(),
        );
        tables.insert_overwrite(
            test_table(
                "b",
                Oid::from_raw(2),
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
            )
            .into(),
        );
        tables.insert_overwrite(
            test_table(
                "c",
                Oid::from_raw(3),
                &["id", "x", "b_id", "val", "tenant_id"],
            )
            .into(),
        );
        tables.insert_overwrite(test_table("d", Oid::from_raw(4), &["id"]).into());
        tables.insert_overwrite(
            test_table(
                "users",
                Oid::from_raw(10),
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
            )
            .into(),
        );
        tables.insert_overwrite(test_table("location", Oid::from_raw(11), &["user_id"]).into());
        tables.insert_overwrite(
            test_table(
                "orders",
                Oid::from_raw(12),
                &["id", "user_id", "total", "product_id"],
            )
            .into(),
        );
        tables
            .insert_overwrite(test_table("admins", Oid::from_raw(13), &["id", "tenant_id"]).into());
        tables.insert_overwrite(
            test_table("active_users", Oid::from_raw(14), &["id", "user_id"]).into(),
        );
        tables.insert_overwrite(
            test_table("banned_users", Oid::from_raw(15), &["id", "user_id"]).into(),
        );
        tables.insert_overwrite(
            test_table(
                "products",
                Oid::from_raw(16),
                &["id", "store_id", "name", "category_id"],
            )
            .into(),
        );
        tables
            .insert_overwrite(test_table("stores", Oid::from_raw(17), &["id", "region_id"]).into());
        tables.insert_overwrite(test_table("regions", Oid::from_raw(18), &["id", "name"]).into());
        tables.insert_overwrite(
            test_table("excluded_regions", Oid::from_raw(19), &["id", "region_id"]).into(),
        );
        tables
            .insert_overwrite(test_table("categories", Oid::from_raw(20), &["id", "name"]).into());
        tables
            .insert_overwrite(test_table("excluded_categories", Oid::from_raw(21), &["id"]).into());
        tables.insert_overwrite(
            test_table(
                "blacklist",
                Oid::from_raw(22),
                &["id", "product_id", "category_id"],
            )
            .into(),
        );
        tables
    }

    /// Parse SQL, bind it, and derive the update queries as
    /// `(table, update SQL, source)`.
    fn update_queries(sql: &str) -> Vec<(String, String, UpdateQuerySource)> {
        let tables = test_tables();
        let cacheable = parse_cacheable(sql);
        let query = Arc::new(cacheable.query().clone());
        let bindings = query_expr_resolve(&query, &tables, &["public"]).expect("resolve");
        let bound = ResolvedQuery::new(query, bindings);
        query_table_update_queries(&bound)
            .into_iter()
            .map(|update| {
                (
                    update.table.name.to_string(),
                    update.bound.sql(),
                    update.source,
                )
            })
            .collect()
    }

    #[test]
    fn test_query_table_update_query_simple_select() {
        let result = update_queries("SELECT id, name, email FROM users WHERE id = 1");

        assert_eq!(result.len(), 1);

        assert_eq!(
            result[0].1,
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

        assert_eq!(result[0].1, expected);
        assert_eq!(result[1].1, expected);
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
        let update1 = &result[0].1;
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
        for (table_name, sql, _) in &result {
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
        for (table_name, sql, _) in &result {
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

    /// Every query the resolved derivation's tests exercise, plus subqueries
    /// whose truth feeds a scalar (PGC-107).
    const BOUND_CORPUS: &[&str] = &[
        "SELECT count(*) FILTER (WHERE EXISTS (SELECT 1 FROM b WHERE b.a_id = 1)) FROM a",
        "SELECT count(*) FILTER (WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.a_id = 1)) FROM a",
        "SELECT CASE WHEN EXISTS (SELECT 1 FROM b WHERE b.a_id = 1) THEN 1 ELSE 0 END FROM a",
        "SELECT CASE WHEN id IN (SELECT a_id FROM b) THEN 1 ELSE (SELECT count(*) FROM c) END FROM a",
        "SELECT id, name, email FROM users WHERE id = 1",
        "SELECT id, name, email FROM users JOIN location ON location.user_id = users.user_id WHERE users.user_id = 1",
        "SELECT * FROM users JOIN location ON location.user_id = users.user_id JOIN orders ON orders.user_id = users.user_id WHERE users.user_id = 1",
        "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.id = c.id JOIN d ON c.id = d.id WHERE a.id = 1",
        "SELECT id FROM users WHERE tenant_id = 1 UNION SELECT id FROM admins WHERE tenant_id = 1",
        "SELECT id FROM a WHERE tenant_id = 1 UNION SELECT id FROM b WHERE tenant_id = 1 UNION SELECT id FROM c WHERE tenant_id = 1",
        "SELECT a.id FROM a JOIN b ON a.id = b.a_id WHERE a.tenant_id = 1 UNION SELECT id FROM c WHERE tenant_id = 1",
        "WITH active_users AS (SELECT id, name FROM users WHERE status = 'active') SELECT active_users.id, orders.total FROM active_users JOIN orders ON orders.user_id = active_users.id",
        "SELECT id FROM users WHERE id = 1",
        "SELECT a.id FROM users a JOIN orders b ON a.id = b.user_id WHERE a.id = 1",
        "SELECT * FROM users WHERE id IN (SELECT user_id FROM active_users)",
        "SELECT * FROM users WHERE id NOT IN (SELECT user_id FROM banned_users)",
        "WITH active AS (SELECT id FROM users WHERE active = true) SELECT * FROM active",
        "SELECT * FROM (SELECT id FROM users WHERE id = 1) sub",
        "SELECT name FROM products WHERE store_id IN ( SELECT id FROM stores WHERE region_id IN ( SELECT id FROM regions WHERE name = 'East' ) )",
        "SELECT name FROM products WHERE store_id IN ( SELECT id FROM stores WHERE region_id NOT IN ( SELECT region_id FROM excluded_regions ) )",
        "SELECT name FROM products WHERE id NOT IN ( SELECT product_id FROM blacklist WHERE category_id IN ( SELECT id FROM categories WHERE name = 'Restricted' ) )",
        "SELECT name FROM products WHERE id NOT IN ( SELECT product_id FROM blacklist WHERE category_id NOT IN ( SELECT id FROM excluded_categories ) )",
        "SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status = 'active'",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.x = c.x WHERE a.id = 1",
        "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1",
        "SELECT * FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.val = c.val WHERE a.id = 1",
    ];

    fn bound(sql: &str, tables: &Catalog) -> ResolvedQuery {
        let cacheable = parse_cacheable(sql);
        let query = Arc::new(cacheable.query().clone());
        let bindings = query_expr_resolve(&query, tables, &["public"]).expect("resolve");
        ResolvedQuery::new(query, bindings)
    }

    /// An update query's remapped bindings are the ones the resolver makes
    /// for it as a query of its own, wherever it is one (a correlated
    /// subquery branch is not).
    #[test]
    fn test_bound_update_query_bindings_match_resolver() {
        let tables = test_tables();
        for sql in BOUND_CORPUS {
            for update in query_table_update_queries(&bound(sql, &tables)) {
                match query_expr_resolve(update.bound.tree(), &tables, &["public"]) {
                    Ok(expected) => {
                        assert_eq!(
                            update.bound.bindings().describe(),
                            expected.describe(),
                            "{sql}"
                        );
                    }
                    Err(e) => assert_ne!(
                        update.source,
                        UpdateQuerySource::FromClause,
                        "{sql}: a FROM-clause update query resolves on its own: {e:?}"
                    ),
                }
            }
        }
    }
}
