use std::collections::HashMap;

use ecow::EcoString;

use crate::query::ast::BinaryOp;
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedMultiExpr, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumns, ResolvedSelectNode,
    ResolvedSetOpNode, ResolvedTableSource, ResolvedUnaryExpr, ResolvedWhereExpr,
};

/// Entry point: apply predicate pushdown to the top-level query expression.
///
/// If the outer SELECT's sole FROM source is a subquery, pushable WHERE
/// conjuncts are pushed into the subquery's branches to reduce rows fetched
/// from origin during cache population and served from cache.
pub fn predicate_pushdown_apply(mut query: ResolvedQueryExpr) -> ResolvedQueryExpr {
    query.body = match query.body {
        ResolvedQueryBody::Select(select) => {
            ResolvedQueryBody::Select(Box::new(select_node_pushdown_apply(*select)))
        }
        // SetOp branches may themselves contain pushdown opportunities
        ResolvedQueryBody::SetOp(set_op) => ResolvedQueryBody::SetOp(ResolvedSetOpNode {
            left: Box::new(predicate_pushdown_apply(*set_op.left)),
            right: Box::new(predicate_pushdown_apply(*set_op.right)),
            ..set_op
        }),
        other @ ResolvedQueryBody::Values(_) => other,
    };
    query
}

/// Core pushdown logic for a single SELECT node.
///
/// Checks whether the SELECT's sole FROM source is a subquery (without
/// LIMIT/OFFSET), then attempts to push each AND-conjunct of the outer
/// WHERE into the subquery body.
fn select_node_pushdown_apply(mut select: ResolvedSelectNode) -> ResolvedSelectNode {
    // Guard: single FROM subquery, has WHERE, subquery has no LIMIT
    let [ResolvedTableSource::Subquery(subquery)] = select.from.as_mut_slice() else {
        return select;
    };
    if subquery.query.limit.is_some() {
        return select;
    }
    if let Some(where_clause) = select.where_clause {
        let alias = &subquery.alias.name;

        // Build output column name → position map from the subquery
        let output_names = subquery_output_column_names(&subquery.query);
        let name_to_position: HashMap<&str, usize> = output_names
            .iter()
            .enumerate()
            .map(|(i, name)| (name.as_str(), i))
            .collect();

        // Split outer WHERE into AND-conjuncts
        let conjuncts = where_expr_conjuncts_split(where_clause);

        let mut remaining = Vec::new();

        for conjunct in conjuncts {
            // Check: all column refs target the subquery alias, and no subquery expressions
            if !predicate_targets_subquery(&conjunct, alias)
                || conjunct.nodes::<ResolvedQueryExpr>().next().is_some()
            {
                remaining.push(conjunct);
                continue;
            }

            // Attempt to push into the subquery body
            match predicate_push_into_query(&subquery.query, &conjunct, &name_to_position) {
                Some(new_query) => {
                    *subquery.query = new_query;
                }
                None => {
                    remaining.push(conjunct);
                }
            }
        }

        select.where_clause = where_expr_conjuncts_join(remaining);
        select
    } else {
        select
    }
}

/// Flatten a WHERE expression into AND-conjuncts.
///
/// `a AND b AND c` becomes `[a, b, c]`. Non-AND expressions return a
/// single-element vec.
pub(crate) fn where_expr_conjuncts_split(expr: ResolvedWhereExpr) -> Vec<ResolvedWhereExpr> {
    match expr {
        ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::And => {
            let mut result = where_expr_conjuncts_split(*binary.lexpr);
            result.extend(where_expr_conjuncts_split(*binary.rexpr));
            result
        }
        other @ (ResolvedWhereExpr::Scalar(_)
        | ResolvedWhereExpr::Unary(_)
        | ResolvedWhereExpr::Binary(_)
        | ResolvedWhereExpr::Multi(_)
        | ResolvedWhereExpr::Subquery { .. }) => vec![other],
    }
}

/// Rebuild an AND tree from conjuncts. Returns None if the vec is empty.
pub(crate) fn where_expr_conjuncts_join(
    conjuncts: Vec<ResolvedWhereExpr>,
) -> Option<ResolvedWhereExpr> {
    let mut iter = conjuncts.into_iter();
    let first = iter.next()?;
    Some(iter.fold(first, |acc, next| {
        ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(acc),
            rexpr: Box::new(next),
        })
    }))
}

/// Check that all column references in the predicate target the given
/// subquery alias (schema must be empty for derived table columns).
fn predicate_targets_subquery(expr: &ResolvedWhereExpr, alias: &str) -> bool {
    let columns: Vec<&ResolvedColumnNode> = expr.nodes().collect();
    // Must have at least one column reference to be pushable
    !columns.is_empty()
        && columns
            .iter()
            .all(|col| col.schema.is_empty() && col.table == alias)
}

/// Extract output column names from the leftmost SELECT of a query.
///
/// For set operations (UNION, etc.), output columns are defined by the
/// leftmost branch. This mirrors `derived_table_columns_extract` logic
/// but only returns names.
fn subquery_output_column_names(query: &ResolvedQueryExpr) -> Vec<EcoString> {
    let select = match &query.body {
        ResolvedQueryBody::Select(select) => select,
        ResolvedQueryBody::SetOp(set_op) => return subquery_output_column_names(&set_op.left),
        ResolvedQueryBody::Values(_) => return Vec::new(),
    };

    match &select.columns {
        ResolvedSelectColumns::None => Vec::new(),
        ResolvedSelectColumns::Columns(cols) => cols
            .iter()
            .filter_map(|col| {
                if let Some(alias) = &col.alias {
                    Some(alias.clone())
                } else {
                    match &col.expr {
                        ResolvedScalarExpr::Column(c) => Some(c.column.clone()),
                        ResolvedScalarExpr::Identifier(ident) => Some(ident.clone()),
                        ResolvedScalarExpr::Function(_)
                        | ResolvedScalarExpr::Literal(_)
                        | ResolvedScalarExpr::Case(_)
                        | ResolvedScalarExpr::Arithmetic(_)
                        | ResolvedScalarExpr::Subquery(..)
                        | ResolvedScalarExpr::Array(_)
                        | ResolvedScalarExpr::TypeCast { .. } => None,
                    }
                }
            })
            .collect(),
    }
}

/// Check whether a SELECT branch is safe for predicate pushdown.
///
/// Returns false if the branch has GROUP BY, HAVING, or window functions
/// in the SELECT list — these change the semantics of WHERE predicates.
fn branch_pushdown_is_safe(select: &ResolvedSelectNode) -> bool {
    if !select.group_by.is_empty() || select.having.is_some() {
        return false;
    }

    // Check for window functions in SELECT columns
    if let ResolvedSelectColumns::Columns(cols) = &select.columns {
        for col in cols {
            if let ResolvedScalarExpr::Function(func) = &col.expr
                && func.over.is_some()
            {
                return false;
            }
        }
    }

    true
}

/// Push a predicate into a query body (SELECT or SetOp).
///
/// For SELECT: remap column references and AND into existing WHERE.
/// For SetOp: recursively push into both branches; if either fails, return None.
/// For Values: return None.
fn predicate_push_into_query(
    query: &ResolvedQueryExpr,
    predicate: &ResolvedWhereExpr,
    name_to_position: &HashMap<&str, usize>,
) -> Option<ResolvedQueryExpr> {
    let new_body = match &query.body {
        ResolvedQueryBody::Select(select) => {
            let new_select = predicate_push_into_select(select, predicate, name_to_position)?;
            ResolvedQueryBody::Select(Box::new(new_select))
        }
        ResolvedQueryBody::SetOp(set_op) => {
            let new_set_op = predicate_push_into_set_op(set_op, predicate, name_to_position)?;
            ResolvedQueryBody::SetOp(new_set_op)
        }
        ResolvedQueryBody::Values(_) => return None,
    };

    Some(ResolvedQueryExpr {
        body: new_body,
        order_by: query.order_by.clone(),
        limit: query.limit.clone(),
    })
}

/// Push a predicate into a single SELECT branch.
fn predicate_push_into_select(
    select: &ResolvedSelectNode,
    predicate: &ResolvedWhereExpr,
    name_to_position: &HashMap<&str, usize>,
) -> Option<ResolvedSelectNode> {
    if !branch_pushdown_is_safe(select) {
        return None;
    }

    let branch_columns = match &select.columns {
        ResolvedSelectColumns::Columns(cols) => cols,
        ResolvedSelectColumns::None => return None,
    };

    // Collect column names actually referenced by the predicate — only these
    // need to map to a simple Column in the branch. Non-referenced positions
    // (which may be literals, functions, etc.) are irrelevant.
    let predicate_columns: Vec<&str> = predicate
        .nodes::<ResolvedColumnNode>()
        .map(|col| col.column.as_str())
        .collect();

    let name_to_column: HashMap<&str, &ResolvedColumnNode> = predicate_columns
        .iter()
        .map(|&col_name| {
            let &pos = name_to_position.get(col_name)?;
            let col = branch_columns.get(pos)?;
            match &col.expr {
                ResolvedScalarExpr::Column(node) => Some((col_name, node)),
                ResolvedScalarExpr::Identifier(_)
                | ResolvedScalarExpr::Function(_)
                | ResolvedScalarExpr::Literal(_)
                | ResolvedScalarExpr::Case(_)
                | ResolvedScalarExpr::Arithmetic(_)
                | ResolvedScalarExpr::Subquery(..)
                | ResolvedScalarExpr::Array(_)
                | ResolvedScalarExpr::TypeCast { .. } => None,
            }
        })
        .collect::<Option<_>>()?;

    let remapped = where_expr_columns_remap(predicate, &name_to_column)?;

    // AND the remapped predicate into the branch's existing WHERE
    let new_where = match &select.where_clause {
        Some(existing) => ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(existing.clone()),
            rexpr: Box::new(remapped),
        }),
        None => remapped,
    };

    Some(ResolvedSelectNode {
        where_clause: Some(new_where),
        ..select.clone()
    })
}

/// Push a predicate into both branches of a set operation.
fn predicate_push_into_set_op(
    set_op: &ResolvedSetOpNode,
    predicate: &ResolvedWhereExpr,
    name_to_position: &HashMap<&str, usize>,
) -> Option<ResolvedSetOpNode> {
    let new_left = predicate_push_into_query(&set_op.left, predicate, name_to_position)?;
    let new_right = predicate_push_into_query(&set_op.right, predicate, name_to_position)?;

    Some(ResolvedSetOpNode {
        op: set_op.op,
        all: set_op.all,
        left: Box::new(new_left),
        right: Box::new(new_right),
    })
}

/// Remap column references in a predicate to point at a branch's actual columns.
///
/// Each `ResolvedColumnNode` is looked up by name in the provided map and
/// replaced with the corresponding branch column node.
fn where_expr_columns_remap(
    expr: &ResolvedWhereExpr,
    name_to_column: &HashMap<&str, &ResolvedColumnNode>,
) -> Option<ResolvedWhereExpr> {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => Some(ResolvedWhereExpr::Scalar(
            scalar_expr_columns_remap(scalar, name_to_column)?,
        )),
        ResolvedWhereExpr::Unary(u) => {
            let remapped = where_expr_columns_remap(&u.expr, name_to_column)?;
            Some(ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
                op: u.op,
                expr: Box::new(remapped),
            }))
        }
        ResolvedWhereExpr::Binary(b) => {
            let left = where_expr_columns_remap(&b.lexpr, name_to_column)?;
            let right = where_expr_columns_remap(&b.rexpr, name_to_column)?;
            Some(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: b.op,
                lexpr: Box::new(left),
                rexpr: Box::new(right),
            }))
        }
        ResolvedWhereExpr::Multi(m) => {
            let remapped: Option<Vec<_>> = m
                .exprs
                .iter()
                .map(|e| where_expr_columns_remap(e, name_to_column))
                .collect();
            Some(ResolvedWhereExpr::Multi(ResolvedMultiExpr {
                op: m.op,
                exprs: remapped?,
            }))
        }
        // Should not reach here — predicate_has_subquery filters these out
        ResolvedWhereExpr::Subquery { .. } => None,
    }
}

fn scalar_expr_columns_remap(
    expr: &ResolvedScalarExpr,
    name_to_column: &HashMap<&str, &ResolvedColumnNode>,
) -> Option<ResolvedScalarExpr> {
    match expr {
        ResolvedScalarExpr::Column(col) => {
            let replacement = name_to_column.get(col.column.as_str())?;
            Some(ResolvedScalarExpr::Column((*replacement).clone()))
        }
        ResolvedScalarExpr::Literal(v) => Some(ResolvedScalarExpr::Literal(v.clone())),
        ResolvedScalarExpr::Array(elems) => {
            let remapped: Option<Vec<_>> = elems
                .iter()
                .map(|e| scalar_expr_columns_remap(e, name_to_column))
                .collect();
            Some(ResolvedScalarExpr::Array(remapped?))
        }
        ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Function(_)
        | ResolvedScalarExpr::Case(_)
        | ResolvedScalarExpr::Arithmetic(_)
        | ResolvedScalarExpr::Subquery(_, _)
        | ResolvedScalarExpr::TypeCast { .. } => None,
    }
}

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{LiteralValue, query_expr_parse};
    use crate::query::resolved::query_expr_resolve;

    fn test_table_metadata_with_columns(
        name: &str,
        relation_oid: u32,
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
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            columns,
            primary_key_columns: vec![column_names[0].to_owned()],
            indexes: Vec::new(),
        }
    }

    fn resolve_and_pushdown(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedQueryExpr {
        let ast = query_expr_parse(sql).expect("convert to AST");
        let resolved = query_expr_resolve(&ast, tables, &["public"]).expect("resolve query");
        predicate_pushdown_apply(resolved)
    }

    // --- Navigation helpers ---

    fn as_select(query: &ResolvedQueryExpr) -> &ResolvedSelectNode {
        match &query.body {
            ResolvedQueryBody::Select(s) => s,
            ResolvedQueryBody::SetOp(_) => panic!("expected Select, got SetOp"),
            ResolvedQueryBody::Values(_) => panic!("expected Select, got Values"),
        }
    }

    fn as_set_op(query: &ResolvedQueryExpr) -> &ResolvedSetOpNode {
        match &query.body {
            ResolvedQueryBody::SetOp(s) => s,
            ResolvedQueryBody::Select(_) => panic!("expected SetOp, got Select"),
            ResolvedQueryBody::Values(_) => panic!("expected SetOp, got Values"),
        }
    }

    /// Extract the inner query from a SELECT with a single FROM subquery.
    fn from_subquery_body(select: &ResolvedSelectNode) -> &ResolvedQueryExpr {
        match select.from.as_slice() {
            [ResolvedTableSource::Subquery(sub)] => &sub.query,
            _ => panic!("expected single FROM subquery"),
        }
    }

    /// Check whether a WHERE expression tree contains `table.column = 'value'`.
    /// Walks through AND nodes to find the comparison anywhere in the tree.
    fn where_has_eq(expr: &ResolvedWhereExpr, table: &str, column: &str, value: &str) -> bool {
        match expr {
            ResolvedWhereExpr::Binary(b) if b.op == BinaryOp::Equal => {
                matches!(
                    (b.lexpr.as_ref(), b.rexpr.as_ref()),
                    (
                        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)),
                        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(LiteralValue::String(v)))
                    ) if col.table == table && col.column == column && v == value
                )
            }
            ResolvedWhereExpr::Binary(b) if b.op == BinaryOp::And => {
                where_has_eq(&b.lexpr, table, column, value)
                    || where_has_eq(&b.rexpr, table, column, value)
            }
            _ => false,
        }
    }

    // --- Tests ---

    #[test]
    fn test_union_pushdown_both_branches() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_none(),
            "outer WHERE should be removed"
        );

        let set_op = as_set_op(from_subquery_body(outer));
        let left = as_select(&set_op.left);
        let right = as_select(&set_op.right);
        assert!(where_has_eq(
            left.where_clause.as_ref().expect("left WHERE"),
            "t1",
            "a",
            "x"
        ));
        assert!(where_has_eq(
            right.where_clause.as_ref().expect("right WHERE"),
            "t2",
            "a",
            "x"
        ));
    }

    #[test]
    fn test_union_column_alias_remapping() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["c", "d"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT c, d FROM t2) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_none(),
            "outer WHERE should be removed"
        );

        let set_op = as_set_op(from_subquery_body(outer));
        let left = as_select(&set_op.left);
        let right = as_select(&set_op.right);
        // t1 branch: a → position 0 → t1.a
        assert!(where_has_eq(
            left.where_clause.as_ref().expect("left WHERE"),
            "t1",
            "a",
            "x"
        ));
        // t2 branch: a → position 0 → t2.c (remapped)
        assert!(where_has_eq(
            right.where_clause.as_ref().expect("right WHERE"),
            "t2",
            "c",
            "x"
        ));
    }

    #[test]
    fn test_existing_where_anded() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1 WHERE b = 'y' UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_none(),
            "outer WHERE should be removed"
        );

        let set_op = as_set_op(from_subquery_body(outer));
        let left = as_select(&set_op.left);
        let left_where = left.where_clause.as_ref().expect("left WHERE");
        // t1 should have both: existing b = 'y' AND pushed a = 'x'
        assert!(
            where_has_eq(left_where, "t1", "b", "y"),
            "existing WHERE preserved"
        );
        assert!(
            where_has_eq(left_where, "t1", "a", "x"),
            "pushed predicate added"
        );
    }

    #[test]
    fn test_non_scalar_expr_no_pushdown() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, 42 AS val FROM t1) sub WHERE sub.val = 99";
        let result = resolve_and_pushdown(sql, &tables);

        // Position 1 maps to a literal — cannot push
        let outer = as_select(&result);
        assert!(outer.where_clause.is_some(), "outer WHERE should remain");
    }

    #[test]
    fn test_group_by_no_pushdown() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));

        let sql =
            "SELECT * FROM (SELECT a, count(b) AS cnt FROM t1 GROUP BY a) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_some(),
            "outer WHERE should remain with GROUP BY"
        );
    }

    #[test]
    fn test_window_function_no_pushdown() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, row_number() OVER (ORDER BY b) AS rn FROM t1) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_some(),
            "outer WHERE should remain with window function"
        );
    }

    #[test]
    fn test_multiple_conjuncts_all_pushed() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1 UNION ALL SELECT a, b FROM t2) sub WHERE sub.a = 'x' AND sub.b = 'y'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_none(),
            "outer WHERE should be removed"
        );

        let set_op = as_set_op(from_subquery_body(outer));
        let left_where = as_select(&set_op.left)
            .where_clause
            .as_ref()
            .expect("left WHERE");
        let right_where = as_select(&set_op.right)
            .where_clause
            .as_ref()
            .expect("right WHERE");

        assert!(where_has_eq(left_where, "t1", "a", "x"));
        assert!(where_has_eq(left_where, "t1", "b", "y"));
        assert!(where_has_eq(right_where, "t2", "a", "x"));
        assert!(where_has_eq(right_where, "t2", "b", "y"));
    }

    #[test]
    fn test_plain_subquery_pushdown() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_none(),
            "outer WHERE should be removed"
        );

        let inner = as_select(from_subquery_body(outer));
        assert!(where_has_eq(
            inner.where_clause.as_ref().expect("inner WHERE"),
            "t1",
            "a",
            "x"
        ));
    }

    #[test]
    fn test_no_subquery_unchanged() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));

        let sql = "SELECT a, b FROM t1 WHERE a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        // No FROM subquery — WHERE should be preserved as-is
        let select = as_select(&result);
        assert!(where_has_eq(
            select.where_clause.as_ref().expect("WHERE preserved"),
            "t1",
            "a",
            "x"
        ));
    }

    #[test]
    fn test_predicate_with_subquery_not_pushed() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1) sub WHERE sub.a IN (SELECT a FROM t2)";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_some(),
            "outer WHERE should remain for subquery predicates"
        );
    }

    #[test]
    fn test_subquery_with_limit_not_pushed() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));

        let sql = "SELECT * FROM (SELECT a, b FROM t1 LIMIT 10) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_some(),
            "outer WHERE should remain when subquery has LIMIT"
        );
    }

    #[test]
    fn test_conjuncts_split_join_roundtrip() {
        let a = ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(LiteralValue::Boolean(true)));
        let b =
            ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(LiteralValue::Boolean(false)));
        let c = ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(LiteralValue::Null));

        let combined = ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::And,
            lexpr: Box::new(a.clone()),
            rexpr: Box::new(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
                op: BinaryOp::And,
                lexpr: Box::new(b.clone()),
                rexpr: Box::new(c.clone()),
            })),
        });

        let split = where_expr_conjuncts_split(combined);
        assert_eq!(split.len(), 3);
        assert_eq!(split[0], a);
        assert_eq!(split[1], b);
        assert_eq!(split[2], c);

        // Join back
        let joined = where_expr_conjuncts_join(split).expect("non-empty");
        let re_split = where_expr_conjuncts_split(joined);
        assert_eq!(re_split.len(), 3);
    }

    #[test]
    fn test_conjuncts_join_empty() {
        assert!(where_expr_conjuncts_join(Vec::new()).is_none());
    }

    #[test]
    fn test_literal_columns_dont_block_pushdown() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["a", "b"]));

        // Branches have literal columns (NULL, constants) alongside real columns.
        // Predicate only references a real column — should still push down.
        let sql = "\
            SELECT * FROM (\
                SELECT a, 'CONST' AS label, NULL AS extra FROM t1 \
                UNION ALL \
                SELECT a, 'OTHER' AS label, NULL AS extra FROM t2\
            ) sub WHERE sub.a = 'x'";
        let result = resolve_and_pushdown(sql, &tables);

        let outer = as_select(&result);
        assert!(
            outer.where_clause.is_none(),
            "outer WHERE should be removed"
        );

        let set_op = as_set_op(from_subquery_body(outer));
        let left = as_select(&set_op.left);
        let right = as_select(&set_op.right);
        assert!(where_has_eq(
            left.where_clause.as_ref().expect("left WHERE"),
            "t1",
            "a",
            "x"
        ));
        assert!(where_has_eq(
            right.where_clause.as_ref().expect("right WHERE"),
            "t2",
            "a",
            "x"
        ));
    }

    #[test]
    fn test_setop_branches_recurse() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata_with_columns("t1", 1001, &["a", "b"]));
        tables.insert_overwrite(test_table_metadata_with_columns("t2", 1002, &["a", "b"]));

        // Top-level UNION; each branch has a FROM subquery with a pushable outer WHERE
        let sql = "\
            SELECT * FROM (SELECT a, b FROM t1) sub1 WHERE sub1.a = 'x' \
            UNION ALL \
            SELECT * FROM (SELECT a, b FROM t2) sub2 WHERE sub2.a = 'y'";
        let result = resolve_and_pushdown(sql, &tables);

        let set_op = as_set_op(&result);

        let left_outer = as_select(&set_op.left);
        assert!(
            left_outer.where_clause.is_none(),
            "left outer WHERE should be removed"
        );
        let left_inner = as_select(from_subquery_body(left_outer));
        assert!(where_has_eq(
            left_inner.where_clause.as_ref().expect("left inner WHERE"),
            "t1",
            "a",
            "x"
        ));

        let right_outer = as_select(&set_op.right);
        assert!(
            right_outer.where_clause.is_none(),
            "right outer WHERE should be removed"
        );
        let right_inner = as_select(from_subquery_body(right_outer));
        assert!(where_has_eq(
            right_inner
                .where_clause
                .as_ref()
                .expect("right inner WHERE"),
            "t2",
            "a",
            "y"
        ));
    }
}
