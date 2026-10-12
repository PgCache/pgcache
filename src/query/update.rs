use crate::cache::query::outer_join_optional_tables;
use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::oid::Oid;
use crate::query::ast::rope::{
    Body, JoinQual, Query, Scalar, ScalarKind, Select, SelectItem, Source, Table, Where, WhereKind,
};
use crate::query::ast::{SubLinkType, UnaryOp};
use crate::query::resolve::{QueryBranch, ResolvedQuery};

/// One table's update query: the branch it sits in, as the cache
/// evaluates a changed row of the table against it.
pub struct TableUpdateQuery<'a> {
    pub table: Table<'a>,
    pub relation_oid: Oid,
    pub branch: QueryBranch<'a>,
    pub source: UpdateQuerySource,
}

/// Every (table, branch) pair of the query: each SELECT, the root and
/// every subquery, once per table directly in its FROM.
pub fn query_table_update_queries(bound: &ResolvedQuery) -> Vec<TableUpdateQuery<'_>> {
    let mut result = Vec::new();
    for (select, source) in select_branches_with_source(bound.tree().root()) {
        let (terminal, non_terminal) = outer_join_optional_tables(bound, select);
        let branch = QueryBranch { bound, select };
        for table in select.direct_tables() {
            let relation_oid = bound
                .bindings()
                .source(table.id())
                .and_then(|slot| bound.bindings().slot(slot))
                .map(|slot| slot.table().relation_oid);
            let Some(relation_oid) = relation_oid else {
                debug_assert!(false, "a direct table of a resolved query has a slot");
                continue;
            };
            let effective_source = if source == UpdateQuerySource::FromClause {
                if non_terminal.contains(table.name().as_str()) {
                    UpdateQuerySource::OuterJoinOptional
                } else if terminal.contains(table.name().as_str()) {
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
                branch,
                source: effective_source,
            });
        }
    }
    result
}

/// Every SELECT of `query` with where it sits: the root body
/// (`FromClause`), or a subquery with its kind. CTE definitions are not
/// collected directly: their branches are collected through each reference,
/// inheriting the reference site's context.
pub fn select_branches_with_source(query: Query<'_>) -> Vec<(Select<'_>, UpdateQuerySource)> {
    let mut branches = Vec::new();
    query_branches_collect(query, &mut branches, UpdateQuerySource::FromClause, false);
    branches
}

/// `negated` tracks NOT-wrapping, which flips Inclusion and Exclusion.
fn query_branches_collect<'t>(
    query: Query<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
    outer_source: UpdateQuerySource,
    negated: bool,
) {
    match query.body() {
        Body::Select(select) => {
            branches.push((select, outer_source));
            for source in select.from() {
                source_branches_collect(source, branches, negated);
            }
            if let Some(where_clause) = select.where_clause() {
                where_branches_collect(where_clause, branches, negated);
            }
            if let Some(having) = select.having() {
                where_branches_collect(having, branches, negated);
            }
            if let Some(items) = select.items() {
                for item in items {
                    if let SelectItem::Expr(expr) = item {
                        scalar_branches_collect(expr.expr(), branches);
                    }
                }
            }
        }
        Body::SetOp(set_op) => {
            query_branches_collect(set_op.left(), branches, outer_source, negated);
            query_branches_collect(set_op.right(), branches, outer_source, negated);
        }
        Body::Values(_) => {}
    }
}

fn source_branches_collect<'t>(
    source: Source<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
    negated: bool,
) {
    match source {
        Source::Table(_) => {}
        Source::Subquery(subquery) => {
            let kind = match (subquery.kind(), negated) {
                (SubqueryKind::Scalar, _) => SubqueryKind::Scalar,
                (SubqueryKind::Inclusion, true) => SubqueryKind::Exclusion,
                (SubqueryKind::Exclusion, true) => SubqueryKind::Inclusion,
                (kind, false) => kind,
            };
            query_branches_collect(
                subquery.query(),
                branches,
                UpdateQuerySource::Subquery(kind),
                negated,
            );
        }
        Source::CteRef(cte_ref) => {
            let kind = if negated {
                SubqueryKind::Exclusion
            } else {
                SubqueryKind::Inclusion
            };
            query_branches_collect(
                cte_ref.query(),
                branches,
                UpdateQuerySource::Subquery(kind),
                negated,
            );
        }
        Source::Join(join) => {
            source_branches_collect(join.left(), branches, negated);
            source_branches_collect(join.right(), branches, negated);
            if let JoinQual::On(condition) = join.qual() {
                where_branches_collect(condition, branches, negated);
            }
        }
    }
}

fn where_branches_collect<'t>(
    expr: Where<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
    negated: bool,
) {
    match expr.kind() {
        WhereKind::Scalar(scalar) => scalar_branches_collect(scalar, branches),
        WhereKind::Binary(binary) => {
            where_branches_collect(binary.lexpr(), branches, negated);
            where_branches_collect(binary.rexpr(), branches, negated);
        }
        WhereKind::Unary(unary) => {
            let child_negated = if unary.op() == UnaryOp::Not {
                !negated
            } else {
                negated
            };
            where_branches_collect(unary.expr(), branches, child_negated);
        }
        WhereKind::Multi(multi) => {
            for operand in multi.exprs() {
                where_branches_collect(operand, branches, negated);
            }
        }
        WhereKind::Subquery(subquery) => {
            let kind = match subquery.sublink_type() {
                SubLinkType::Expr => SubqueryKind::Scalar,
                SubLinkType::Any | SubLinkType::Exists => {
                    if negated {
                        SubqueryKind::Exclusion
                    } else {
                        SubqueryKind::Inclusion
                    }
                }
                SubLinkType::All => {
                    if negated {
                        SubqueryKind::Inclusion
                    } else {
                        SubqueryKind::Exclusion
                    }
                }
            };
            query_branches_collect(
                subquery.query(),
                branches,
                UpdateQuerySource::Subquery(kind),
                negated,
            );
            if let Some(test) = subquery.test_expr() {
                scalar_branches_collect(test, branches);
            }
        }
    }
}

/// Like [`where_branches_collect`] but every collected subquery is
/// `SubqueryKind::Scalar` whatever its sublink type: its truth value feeds
/// an enclosing scalar (aggregate FILTER, CASE WHEN condition), so
/// shrink-skip rules are unsound (PGC-107).
fn where_branches_collect_as_scalar<'t>(
    expr: Where<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
) {
    let start = branches.len();
    where_branches_collect(expr, branches, false);
    for (_, source) in branches.iter_mut().skip(start) {
        if let UpdateQuerySource::Subquery(kind) = source {
            *kind = SubqueryKind::Scalar;
        }
    }
}

fn scalar_branches_collect<'t>(
    expr: Scalar<'t>,
    branches: &mut Vec<(Select<'t>, UpdateQuerySource)>,
) {
    match expr.kind() {
        ScalarKind::Column(_) | ScalarKind::Literal(_) => {}
        ScalarKind::Function(function) => {
            for arg in function.args() {
                scalar_branches_collect(arg, branches);
            }
            for clause in function.agg_order() {
                scalar_branches_collect(clause.expr(), branches);
            }
            if let Some(filter) = function.filter() {
                where_branches_collect_as_scalar(filter, branches);
            }
            if let Some(over) = function.over() {
                for partition in over.partition_by() {
                    scalar_branches_collect(partition, branches);
                }
                for clause in over.order_by() {
                    scalar_branches_collect(clause.expr(), branches);
                }
            }
        }
        ScalarKind::Case(case) => {
            if let Some(arg) = case.arg() {
                scalar_branches_collect(arg, branches);
            }
            for when in case.whens() {
                where_branches_collect_as_scalar(when.condition(), branches);
                scalar_branches_collect(when.result(), branches);
            }
            if let Some(default) = case.default() {
                scalar_branches_collect(default, branches);
            }
        }
        ScalarKind::Arithmetic(arithmetic) => {
            scalar_branches_collect(arithmetic.left(), branches);
            scalar_branches_collect(arithmetic.right(), branches);
        }
        ScalarKind::Subquery(subquery) => {
            let source = UpdateQuerySource::Subquery(SubqueryKind::Scalar);
            query_branches_collect(subquery.query(), branches, source, false);
        }
        ScalarKind::Array(array) => {
            for element in array.elements() {
                scalar_branches_collect(element, branches);
            }
        }
        ScalarKind::TypeCast(cast) => scalar_branches_collect(cast.expr(), branches),
    }
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
        let tree = cacheable.tree().clone();
        let bound = ResolvedQuery::resolve(tree, &tables, &["public"]).expect("resolve");
        query_table_update_queries(&bound)
            .into_iter()
            .map(|update| {
                (
                    update.table.name().to_string(),
                    update.branch.sql(),
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
}
