use ecow::EcoString;
use rootcause::Report;

use crate::cache::SubqueryKind;
use crate::catalog::TableMetadata;
use crate::query::ast::{LiteralValue, TableAlias, ValuesClause};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableSource, ResolvedTableSubqueryNode,
    ResolvedWhereExpr,
};

use super::{AstTransformError, AstTransformResult};

/// Replace `table_metadata`'s table source with a single-row `VALUES`
/// (the CDC row), aliased as the table, so the query can be evaluated
/// as an `EXISTS(...)` membership test for that row. Column references
/// to the table across every clause are rewritten to the alias.
pub fn resolved_select_node_table_replace_with_values(
    resolved: &ResolvedSelectNode,
    table_metadata: &TableMetadata,
    row_data: &[Option<String>],
) -> AstTransformResult<ResolvedSelectNode> {
    let mut resolved_new = resolved.clone();
    let relation_oid = table_metadata.relation_oid;

    // Find first matching table source by relation_oid and get the alias
    let alias = {
        let Some(first_from) = resolved_new.from.first() else {
            return Err(AstTransformError::MissingTable.into());
        };
        let mut frontier = vec![first_from];
        let mut found_alias: Option<EcoString> = None;
        while let Some(cur) = frontier.pop() {
            match cur {
                ResolvedTableSource::Join(join) => {
                    frontier.push(&join.left);
                    frontier.push(&join.right);
                }
                ResolvedTableSource::Table(table) => {
                    if table.relation_oid == relation_oid {
                        found_alias = Some(EcoString::from(
                            table.alias.as_deref().unwrap_or(&table_metadata.name),
                        ));
                        break;
                    }
                }
                _ => (),
            }
        }
        found_alias.ok_or_else(|| Report::from(AstTransformError::MissingTable))?
    };

    // Update all column references for this table to use the alias
    resolved_select_node_column_alias_update(
        &mut resolved_new,
        &table_metadata.schema,
        &table_metadata.name,
        &alias,
    );

    // Now replace the table source with a VALUES subquery
    let Some(first_from) = resolved_new.from.first_mut() else {
        return Err(AstTransformError::MissingTable.into());
    };
    let mut frontier = vec![first_from];
    let mut source_node: Option<&mut ResolvedTableSource> = None;
    while let Some(cur) = frontier.pop() {
        match cur {
            ResolvedTableSource::Join(join) => {
                frontier.push(&mut join.left);
                frontier.push(&mut join.right);
            }
            ResolvedTableSource::Table(table) => {
                if table.relation_oid == relation_oid {
                    source_node = Some(cur);
                    break;
                }
            }
            _ => (),
        }
    }

    let Some(source_node) = source_node else {
        return Err(AstTransformError::MissingTable.into());
    };

    // Build VALUES clause from row_data and collect column names
    let mut values = Vec::new();
    let mut column_names: Vec<EcoString> = Vec::new();
    for column_meta in &table_metadata.columns {
        let position = column_meta.index();
        if let Some(row_value) = row_data.get(position) {
            let value = row_value.as_deref().map_or_else(
                || LiteralValue::NullWithCast(column_meta.type_name.clone()),
                |v| LiteralValue::StringWithCast(v.to_owned(), column_meta.type_name.clone()),
            );
            values.push(value);
            column_names.push(EcoString::from(column_meta.name.as_str()));
        }
    }

    *source_node = ResolvedTableSource::Subquery(ResolvedTableSubqueryNode {
        query: Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Values(ValuesClause { rows: vec![values] }),
            order_by: vec![],
            limit: None,
        }),
        alias: TableAlias {
            name: alias,
            columns: column_names,
        },
        subquery_kind: SubqueryKind::Inclusion,
    });

    Ok(resolved_new)
}

/// Update all column references for a specific table to use an alias in a ResolvedSelectNode.
fn resolved_select_node_column_alias_update(
    resolved: &mut ResolvedSelectNode,
    schema: &str,
    table: &str,
    alias: &str,
) {
    // Update WHERE clause columns
    if let Some(where_clause) = &mut resolved.where_clause {
        resolved_where_expr_alias_update(where_clause, schema, table, alias);
    }
    // Update SELECT columns
    resolved_select_columns_alias_update(&mut resolved.columns, schema, table, alias);
    // GROUP BY / HAVING — same dangling-ref hazard as the JOIN case
    // below (PGC-145; same omission class as PGC-139).
    for col in &mut resolved.group_by {
        resolved_column_node_alias_update(col, schema, table, alias);
    }
    if let Some(having) = &mut resolved.having {
        resolved_where_expr_alias_update(having, schema, table, alias);
    }
    // Update JOIN ON conditions: the replaced table becomes an aliased
    // VALUES subquery, so refs to it in join conditions must use the
    // alias too — otherwise a schema-qualified `schema.table.col`
    // survives while the FROM entry is only `alias`, which Postgres
    // rejects as "invalid reference to FROM-clause entry" (PGC-139).
    for source in &mut resolved.from {
        resolved_table_source_alias_update(source, schema, table, alias);
    }
}

/// Rewrite a resolved column node referencing `schema.table` to use
/// `alias` (the VALUES-subquery name), so it deparses as `alias.col`.
fn resolved_column_node_alias_update(
    col: &mut ResolvedColumnNode,
    schema: &str,
    table: &str,
    alias: &str,
) {
    if col.schema == schema && col.table == table {
        col.table_alias = Some(EcoString::from(alias));
    }
}

/// Update column references for `table` to `alias` within a FROM source's
/// JOIN conditions, recursing through nested joins. Subquery sources are
/// left alone — a derived table is its own scope and cannot reference the
/// outer table being replaced.
fn resolved_table_source_alias_update(
    source: &mut ResolvedTableSource,
    schema: &str,
    table: &str,
    alias: &str,
) {
    match source {
        ResolvedTableSource::Join(join) => {
            resolved_table_source_alias_update(&mut join.left, schema, table, alias);
            resolved_table_source_alias_update(&mut join.right, schema, table, alias);
            if let Some(condition) = join.predicate_mut() {
                resolved_where_expr_alias_update(condition, schema, table, alias);
            }
        }
        ResolvedTableSource::Table(_) | ResolvedTableSource::Subquery(_) => {}
    }
}

/// Update column aliases in a ResolvedQueryExpr
fn resolved_query_expr_column_alias_update(
    query: &mut ResolvedQueryExpr,
    schema: &str,
    table: &str,
    alias: &str,
) {
    match &mut query.body {
        ResolvedQueryBody::Select(select_node) => {
            // Update WHERE clause columns
            if let Some(where_clause) = &mut select_node.where_clause {
                resolved_where_expr_alias_update(where_clause, schema, table, alias);
            }
            // Update SELECT columns
            resolved_select_columns_alias_update(&mut select_node.columns, schema, table, alias);
        }
        ResolvedQueryBody::Values(_) => {
            // VALUES clauses don't have column references to update
        }
        ResolvedQueryBody::SetOp(set_op) => {
            resolved_query_expr_column_alias_update(&mut set_op.left, schema, table, alias);
            resolved_query_expr_column_alias_update(&mut set_op.right, schema, table, alias);
        }
    }
    // Update ORDER BY columns at query level
    for order_by in &mut query.order_by {
        resolved_scalar_expr_alias_update(&mut order_by.expr, schema, table, alias);
    }
}

fn resolved_where_expr_alias_update(
    expr: &mut ResolvedWhereExpr,
    schema: &str,
    table: &str,
    alias: &str,
) {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => {
            resolved_scalar_expr_alias_update(scalar, schema, table, alias);
        }
        ResolvedWhereExpr::Unary(unary) => {
            resolved_where_expr_alias_update(&mut unary.expr, schema, table, alias);
        }
        ResolvedWhereExpr::Binary(binary) => {
            resolved_where_expr_alias_update(&mut binary.lexpr, schema, table, alias);
            resolved_where_expr_alias_update(&mut binary.rexpr, schema, table, alias);
        }
        ResolvedWhereExpr::Multi(multi) => {
            for e in &mut multi.exprs {
                resolved_where_expr_alias_update(e, schema, table, alias);
            }
        }
        ResolvedWhereExpr::Subquery {
            query, test_expr, ..
        } => {
            resolved_query_expr_column_alias_update(query, schema, table, alias);
            if let Some(test) = test_expr {
                resolved_scalar_expr_alias_update(test, schema, table, alias);
            }
        }
    }
}

fn resolved_select_columns_alias_update(
    columns: &mut ResolvedSelectColumns,
    schema: &str,
    table: &str,
    alias: &str,
) {
    match columns {
        ResolvedSelectColumns::Columns(cols) => {
            for col in cols {
                resolved_scalar_expr_alias_update(&mut col.expr, schema, table, alias);
            }
        }
        ResolvedSelectColumns::None => {}
    }
}

fn resolved_scalar_expr_alias_update(
    expr: &mut ResolvedScalarExpr,
    schema: &str,
    table: &str,
    alias: &str,
) {
    match expr {
        ResolvedScalarExpr::Column(col) => {
            resolved_column_node_alias_update(col, schema, table, alias);
        }
        ResolvedScalarExpr::Function(func) => {
            for arg in &mut func.args {
                resolved_scalar_expr_alias_update(arg, schema, table, alias);
            }
            for clause in &mut func.agg_order {
                resolved_scalar_expr_alias_update(&mut clause.expr, schema, table, alias);
            }
            if let Some(filter) = &mut func.agg_filter {
                resolved_where_expr_alias_update(filter, schema, table, alias);
            }
            if let Some(window_spec) = &mut func.over {
                for col in &mut window_spec.partition_by {
                    resolved_scalar_expr_alias_update(col, schema, table, alias);
                }
                for clause in &mut window_spec.order_by {
                    resolved_scalar_expr_alias_update(&mut clause.expr, schema, table, alias);
                }
            }
        }
        ResolvedScalarExpr::Case(case) => {
            if let Some(arg) = &mut case.arg {
                resolved_scalar_expr_alias_update(arg, schema, table, alias);
            }
            for when in &mut case.whens {
                resolved_where_expr_alias_update(&mut when.condition, schema, table, alias);
                resolved_scalar_expr_alias_update(&mut when.result, schema, table, alias);
            }
            if let Some(default) = &mut case.default {
                resolved_scalar_expr_alias_update(default, schema, table, alias);
            }
        }
        ResolvedScalarExpr::Arithmetic(arith) => {
            resolved_scalar_expr_alias_update(&mut arith.left, schema, table, alias);
            resolved_scalar_expr_alias_update(&mut arith.right, schema, table, alias);
        }
        ResolvedScalarExpr::Subquery(query, _) => {
            resolved_query_expr_column_alias_update(query, schema, table, alias);
        }
        ResolvedScalarExpr::Array(elems) => {
            for elem in elems {
                resolved_scalar_expr_alias_update(elem, schema, table, alias);
            }
        }
        ResolvedScalarExpr::TypeCast { expr, .. } => {
            resolved_scalar_expr_alias_update(expr, schema, table, alias);
        }
        ResolvedScalarExpr::Identifier(_) | ResolvedScalarExpr::Literal(_) => {}
    }
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{
        BinaryOp, Deparse, JoinType, LiteralValue, QueryBody, query_expr_convert,
    };
    use crate::query::resolve::select_node_resolve;
    use crate::query::resolved::{
        ResolvedBinaryExpr, ResolvedColumnNode, ResolvedJoinNode, ResolvedJoinQual,
        ResolvedSelectColumn, ResolvedTableNode, ResolvedTableSource, ResolvedWhereExpr,
    };
    use crate::query::transform::AstTransformError;

    use super::*;

    fn column_metadata(name: &str, position: i16, type_name: &str) -> ColumnMetadata {
        let (type_oid, data_type) = match type_name {
            "int4" => (23, Type::INT4),
            _ => (25, Type::TEXT),
        };
        ColumnMetadata {
            name: name.into(),
            position,
            type_oid,
            data_type,
            type_name: type_name.into(),
            cache_type_name: type_name.into(),
            is_primary_key: position == 1,
        }
    }

    fn table_metadata(name: &str, oid: u32, cols: &[(&str, &str)]) -> TableMetadata {
        let columns =
            ColumnStore::new(cols.iter().enumerate().map(|(i, &(col_name, type_name))| {
                column_metadata(
                    col_name,
                    i16::try_from(i + 1).expect("column position fits in i16"),
                    type_name,
                )
            }));
        TableMetadata {
            relation_oid: oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![cols.first().expect("at least one column").0.to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn resolved_table(name: &str, oid: u32, alias: Option<&str>) -> ResolvedTableSource {
        ResolvedTableSource::Table(ResolvedTableNode {
            schema: "public".into(),
            name: name.into(),
            alias: alias.map(|a| a.into()),
            relation_oid: oid,
        })
    }

    fn resolved_column(table: &str, column: &str, col_meta: ColumnMetadata) -> ResolvedColumnNode {
        ResolvedColumnNode {
            schema: "public".into(),
            table: table.into(),
            table_alias: None,
            column: column.into(),
            column_metadata: col_meta,
        }
    }

    /// Build a minimal SELECT node: `SELECT columns FROM table_source WHERE where_clause`
    fn select_node(
        from: Vec<ResolvedTableSource>,
        columns: ResolvedSelectColumns,
        where_clause: Option<ResolvedWhereExpr>,
    ) -> ResolvedSelectNode {
        ResolvedSelectNode {
            from,
            columns,
            where_clause,
            ..Default::default()
        }
    }

    /// PGC-139: when a join's CDC'd table is swapped for a VALUES
    /// subquery, refs to it in the JOIN ON condition must use the
    /// subquery alias. A surviving schema-qualified `public.j1_tbl.col`
    /// dangles against the aliased FROM entry and Postgres rejects it
    /// with "invalid reference to FROM-clause entry".
    #[test]
    fn test_join_on_condition_uses_subquery_alias() {
        let cols = &[("i", "int4"), ("j", "int4"), ("t", "text"), ("j1_pk", "int4")];
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(table_metadata("j1_tbl", 5001, cols));
        tables.insert_overwrite(table_metadata(
            "j2_tbl",
            5002,
            &[("i", "int4"), ("k", "int4"), ("j2_pk", "int4")],
        ));
        let j1 = table_metadata("j1_tbl", 5001, cols);
        let row = vec![
            Some("1".to_owned()),
            Some("4".to_owned()),
            Some("one".to_owned()),
            Some("1".to_owned()),
        ];

        let ast = pg_query::parse("SELECT * FROM j1_tbl JOIN j2_tbl ON (j1_tbl.i = j2_tbl.i)")
            .expect("parse");
        let qe = query_expr_convert(&ast).expect("convert");
        let QueryBody::Select(node) = qe.body else {
            panic!("expected SELECT");
        };
        let resolved = select_node_resolve(&node, &tables, &["public"]).expect("resolve");
        let replaced = resolved_select_node_table_replace_with_values(&resolved, &j1, &row)
            .expect("replace");
        let mut sql = String::new();
        Deparse::deparse(&replaced, &mut sql);

        assert!(
            sql.contains("ON j1_tbl.i = "),
            "ON condition must reference the subquery alias; got: {sql}"
        );
        assert!(
            !sql.contains("public.j1_tbl"),
            "no dangling schema-qualified ref to the replaced table; got: {sql}"
        );
    }

    /// PGC-145: GROUP BY / HAVING refs to the CDC'd table must also use
    /// the VALUES-subquery alias (same omission class as PGC-139). A
    /// surviving `public.onek.col` in GROUP BY/HAVING dangles against
    /// the aliased FROM entry → "invalid reference to FROM-clause
    /// entry" → writer crash on insert.
    #[test]
    fn test_group_by_having_uses_subquery_alias() {
        let cols = &[("unique1", "int4"), ("ten", "int4"), ("odd", "int4")];
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(table_metadata("onek", 6001, cols));
        let onek = table_metadata("onek", 6001, cols);
        let row = vec![
            Some("1".to_owned()),
            Some("7".to_owned()),
            Some("1".to_owned()),
        ];

        let ast = pg_query::parse(
            "SELECT ten, count(*) FILTER (WHERE odd = 1) AS c FROM onek \
             GROUP BY ten HAVING count(*) > 0",
        )
        .expect("parse");
        let qe = query_expr_convert(&ast).expect("convert");
        let QueryBody::Select(node) = qe.body else {
            panic!("expected SELECT");
        };
        let resolved = select_node_resolve(&node, &tables, &["public"]).expect("resolve");
        let replaced = resolved_select_node_table_replace_with_values(&resolved, &onek, &row)
            .expect("replace");
        let mut sql = String::new();
        Deparse::deparse(&replaced, &mut sql);

        assert!(
            sql.contains("GROUP BY onek.ten"),
            "GROUP BY must reference the subquery alias; got: {sql}"
        );
        assert!(
            !sql.contains("public.onek"),
            "no dangling schema-qualified ref to the replaced table; got: {sql}"
        );
    }

    /// USING/NATURAL deparse **verbatim** (so Postgres performs the
    /// column merge — matching origin's `SELECT *` width / unqualified
    /// refs), while carrying a synthesized equi-`predicate` for
    /// freshness analysis. CROSS deparses `CROSS JOIN` with no
    /// predicate. (PGC-142)
    #[test]
    fn test_join_qual_using_natural_preserved_with_predicate() {
        let cols1 = &[("i", "int4"), ("j", "int4"), ("t", "text"), ("j1_pk", "int4")];
        let cols2 = &[("i", "int4"), ("k", "int4"), ("j2_pk", "int4")];
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(table_metadata("j1_tbl", 5001, cols1));
        tables.insert_overwrite(table_metadata("j2_tbl", 5002, cols2));

        let resolve = |sql: &str| {
            let ast = pg_query::parse(sql).expect("parse");
            let qe = query_expr_convert(&ast).expect("convert");
            let QueryBody::Select(node) = qe.body else {
                panic!("expected SELECT");
            };
            select_node_resolve(&node, &tables, &["public"]).expect("resolve")
        };
        let deparsed = |n: &ResolvedSelectNode| {
            let mut s = String::new();
            Deparse::deparse(n, &mut s);
            s
        };
        let join_qual = |n: &ResolvedSelectNode| match n.from.first() {
            Some(ResolvedTableSource::Join(j)) => j.clone(),
            _ => panic!("expected a join in FROM"),
        };

        // CROSS → `CROSS JOIN`, no predicate.
        let cross = resolve("SELECT * FROM j1_tbl CROSS JOIN j2_tbl");
        let cross_sql = deparsed(&cross);
        assert!(
            cross_sql.contains(" CROSS JOIN public.j2_tbl") && !cross_sql.contains(" ON "),
            "CROSS must deparse as CROSS JOIN with no ON; got: {cross_sql}"
        );
        assert!(
            join_qual(&cross).predicate().is_none(),
            "CROSS has no analysis predicate"
        );

        // USING (i) → emitted verbatim (NOT rewritten to ON), with a
        // synthesized equi-predicate for analysis.
        let using = resolve("SELECT * FROM j1_tbl JOIN j2_tbl USING (i)");
        let using_sql = deparsed(&using);
        assert!(
            using_sql.contains(" JOIN public.j2_tbl USING (i)") && !using_sql.contains(" ON "),
            "USING must deparse verbatim, not as ON; got: {using_sql}"
        );
        assert!(
            join_qual(&using).predicate().is_some(),
            "USING must carry an equi-predicate for analysis"
        );

        // NATURAL (common column: i) → resolved to `USING (i)`.
        let natural = deparsed(&resolve("SELECT * FROM j1_tbl NATURAL JOIN j2_tbl"));
        assert!(
            natural.contains(" JOIN public.j2_tbl USING (i)") && !natural.contains(" ON "),
            "NATURAL must resolve to USING over common columns; got: {natural}"
        );

        // Outer + USING keeps the outer keyword and the USING clause.
        let left = deparsed(&resolve("SELECT * FROM j1_tbl LEFT JOIN j2_tbl USING (i)"));
        assert!(
            left.contains(" LEFT JOIN public.j2_tbl USING (i)"),
            "LEFT JOIN USING must deparse verbatim; got: {left}"
        );
    }

    // ==================== Happy Path Tests ====================

    #[test]
    fn test_simple_table_replaced_with_values() {
        let meta = table_metadata("users", 100, &[("id", "int4"), ("name", "text")]);
        let node = select_node(
            vec![resolved_table("users", 100, None)],
            ResolvedSelectColumns::None,
            None,
        );
        let row = vec![Some("42".to_owned()), Some("alice".to_owned())];

        let result =
            resolved_select_node_table_replace_with_values(&node, &meta, &row).expect("replace");

        // The FROM source should now be a subquery with VALUES
        let from = result.from.first().expect("at least one FROM source");
        let subquery = match from {
            ResolvedTableSource::Subquery(sq) => sq,
            ResolvedTableSource::Table(_) | ResolvedTableSource::Join(_) => {
                panic!("expected Subquery, got: {from:?}")
            }
        };

        // Alias should use the table name since no explicit alias was set
        assert_eq!(subquery.alias.name, "users");
        assert_eq!(subquery.alias.columns, vec!["id", "name"]);

        // VALUES should contain the row data with type casts
        let mut buf = String::new();
        result.deparse(&mut buf);
        assert!(
            buf.contains("VALUES ('42'::int4, 'alice'::text)"),
            "should contain VALUES with casts: {buf}"
        );
        assert!(
            buf.contains("users(id, name)"),
            "should contain alias with column names: {buf}"
        );
    }

    #[test]
    fn test_null_values_produce_null_with_cast() {
        let meta = table_metadata("users", 100, &[("id", "int4"), ("name", "text")]);
        let node = select_node(
            vec![resolved_table("users", 100, None)],
            ResolvedSelectColumns::None,
            None,
        );
        let row = vec![Some("1".to_owned()), None];

        let result =
            resolved_select_node_table_replace_with_values(&node, &meta, &row).expect("replace");

        let mut buf = String::new();
        result.deparse(&mut buf);
        assert!(
            buf.contains("'1'::int4, NULL::text"),
            "NULL column should produce NULL::type: {buf}"
        );
    }

    #[test]
    fn test_explicit_alias_preserved() {
        let meta = table_metadata("users", 100, &[("id", "int4")]);
        let node = select_node(
            vec![resolved_table("users", 100, Some("u"))],
            ResolvedSelectColumns::None,
            None,
        );
        let row = vec![Some("1".to_owned())];

        let result =
            resolved_select_node_table_replace_with_values(&node, &meta, &row).expect("replace");

        let from = result.from.first().expect("at least one FROM source");
        let subquery = match from {
            ResolvedTableSource::Subquery(sq) => sq,
            ResolvedTableSource::Table(_) | ResolvedTableSource::Join(_) => {
                panic!("expected Subquery, got: {from:?}")
            }
        };
        assert_eq!(subquery.alias.name, "u", "should preserve explicit alias");
    }

    // ==================== Error Tests ====================

    #[test]
    fn test_empty_from_returns_missing_table() {
        let meta = table_metadata("users", 100, &[("id", "int4")]);
        let node = select_node(vec![], ResolvedSelectColumns::None, None);
        let row = vec![Some("1".to_owned())];

        let err = resolved_select_node_table_replace_with_values(&node, &meta, &row)
            .map_err(|e| e.into_current_context())
            .expect_err("should fail");

        assert!(
            matches!(err, AstTransformError::MissingTable),
            "expected MissingTable, got: {err:?}"
        );
    }

    #[test]
    fn test_non_matching_oid_returns_missing_table() {
        let meta = table_metadata("users", 100, &[("id", "int4")]);
        // FROM has a table with OID 999, but metadata says OID 100
        let node = select_node(
            vec![resolved_table("users", 999, None)],
            ResolvedSelectColumns::None,
            None,
        );
        let row = vec![Some("1".to_owned())];

        let err = resolved_select_node_table_replace_with_values(&node, &meta, &row)
            .map_err(|e| e.into_current_context())
            .expect_err("should fail");

        assert!(
            matches!(err, AstTransformError::MissingTable),
            "expected MissingTable, got: {err:?}"
        );
    }

    // ==================== Column Alias Update Tests ====================

    #[test]
    fn test_where_column_alias_updated() {
        let id_meta = column_metadata("id", 1, "int4");
        let where_clause = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(
                resolved_column("users", "id", id_meta),
            ))),
            rexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(
                LiteralValue::Integer(42),
            ))),
        });

        let meta = table_metadata("users", 100, &[("id", "int4"), ("name", "text")]);
        let node = select_node(
            vec![resolved_table("users", 100, None)],
            ResolvedSelectColumns::None,
            Some(where_clause),
        );
        let row = vec![Some("42".to_owned()), Some("alice".to_owned())];

        let result =
            resolved_select_node_table_replace_with_values(&node, &meta, &row).expect("replace");

        // After replacement, column references to public.users should have table_alias set
        let mut buf = String::new();
        result.deparse(&mut buf);
        // With alias set, column deparses as "users.id" instead of "public.users.id"
        assert!(
            buf.contains("WHERE users.id = 42"),
            "column should use alias after replacement: {buf}"
        );
    }

    #[test]
    fn test_select_column_alias_updated() {
        let id_meta = column_metadata("id", 1, "int4");
        let columns = ResolvedSelectColumns::Columns(vec![ResolvedSelectColumn {
            expr: ResolvedScalarExpr::Column(resolved_column("users", "id", id_meta)),
            alias: None,
        }]);

        let meta = table_metadata("users", 100, &[("id", "int4")]);
        let node = select_node(vec![resolved_table("users", 100, None)], columns, None);
        let row = vec![Some("42".to_owned())];

        let result =
            resolved_select_node_table_replace_with_values(&node, &meta, &row).expect("replace");

        let mut buf = String::new();
        result.deparse(&mut buf);
        // SELECT column should deparse with alias
        assert!(
            buf.contains("SELECT users.id"),
            "select column should use alias: {buf}"
        );
        assert!(
            !buf.contains("public.users.id"),
            "should not have schema-qualified reference: {buf}"
        );
    }

    // ==================== JOIN Tests ====================

    #[test]
    fn test_table_inside_join_replaced() {
        let meta = table_metadata("orders", 200, &[("id", "int4"), ("total", "text")]);

        let join = ResolvedTableSource::Join(Box::new(ResolvedJoinNode {
            join_type: JoinType::Inner,
            left: resolved_table("users", 100, None),
            right: resolved_table("orders", 200, None),
            qual: ResolvedJoinQual::Cross,
        }));

        let node = select_node(vec![join], ResolvedSelectColumns::None, None);
        let row = vec![Some("7".to_owned()), Some("99.50".to_owned())];

        let result =
            resolved_select_node_table_replace_with_values(&node, &meta, &row).expect("replace");

        // The JOIN's right side should now be a subquery
        let from = result.from.first().expect("at least one FROM source");
        let join_node = match from {
            ResolvedTableSource::Join(j) => j,
            ResolvedTableSource::Table(_) | ResolvedTableSource::Subquery(_) => {
                panic!("expected Join, got: {from:?}")
            }
        };

        // Left side should remain a table
        assert!(
            matches!(&join_node.left, ResolvedTableSource::Table(t) if t.name == "users"),
            "left side should still be the users table"
        );

        // Right side should be replaced with VALUES subquery
        let subquery = match &join_node.right {
            ResolvedTableSource::Subquery(sq) => sq,
            ResolvedTableSource::Table(_) | ResolvedTableSource::Join(_) => {
                panic!(
                    "expected right side to be Subquery, got: {:?}",
                    join_node.right
                )
            }
        };
        assert_eq!(subquery.alias.name, "orders");
        assert_eq!(subquery.alias.columns, vec!["id", "total"]);
    }
}
