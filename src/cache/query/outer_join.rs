//! Outer-join terminality: which optional-side tables of an outer join CDC can
//! update in place, and which force invalidation.

use std::collections::HashSet;

use ecow::EcoString;

use crate::query::ast::{AstNode, JoinType};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedJoinNode, ResolvedSelectNode, ResolvedTableSource,
    ResolvedWhereExpr,
};

/// Categorizes optional-side tables in outer joins as terminal or non-terminal.
///
/// - **Terminal**: columns don't appear in WHERE or other join conditions.
///   CDC INSERT/DELETE handled in place — the preserved side already has the row,
///   changes here only affect NULL-padded columns.
/// - **Non-terminal**: columns appear in WHERE or other join conditions.
///   CDC events trigger full query invalidation (conservative but correct).
///
/// Uses the resolved AST where column references carry the real table name
/// (not aliases), eliminating alias ambiguity.
pub fn outer_join_optional_tables(
    select: &ResolvedSelectNode,
) -> (HashSet<EcoString>, HashSet<EcoString>) {
    let join = match select.from.as_slice() {
        [ResolvedTableSource::Join(join)] => join,
        _ => return (HashSet::new(), HashSet::new()),
    };

    // Pass 1: collect real table names from WHERE clause column references.
    // GROUP BY, HAVING, and SELECT list are excluded — population queries strip
    // GROUP BY/HAVING, and all three are re-evaluated at retrieval time against
    // cached rows.
    let mut non_terminal_refs = HashSet::new();
    if let Some(where_clause) = &select.where_clause {
        resolved_column_table_refs_collect(where_clause, &mut non_terminal_refs);
    }

    // Pass 2: walk the join tree, collecting all optional-side tables and
    // identifying which are non-terminal
    let mut all_optional = HashSet::new();
    let mut non_terminal = HashSet::new();
    resolved_join_terminality_walk(
        join,
        &non_terminal_refs,
        &mut all_optional,
        &mut non_terminal,
    );

    let terminal = all_optional.difference(&non_terminal).cloned().collect();
    (terminal, non_terminal)
}

/// Collect real table names from all column references in a resolved WHERE expression.
fn resolved_column_table_refs_collect(expr: &ResolvedWhereExpr, tables: &mut HashSet<EcoString>) {
    for col in expr.nodes::<ResolvedColumnNode>() {
        tables.insert(col.table.clone());
    }
}

/// Collect the real table names from all table nodes in a resolved table source subtree.
/// Traverses JOINs but not subqueries.
fn resolved_source_table_names_collect(
    source: &ResolvedTableSource,
    names: &mut HashSet<EcoString>,
) {
    match source {
        ResolvedTableSource::Table(table) => {
            names.insert(table.name.clone());
        }
        ResolvedTableSource::Join(join) => {
            resolved_source_table_names_collect(&join.left, names);
            resolved_source_table_names_collect(&join.right, names);
        }
        ResolvedTableSource::Subquery(_) => {}
    }
}

/// Recursive walk of the resolved join tree to collect optional-side tables
/// and identify which are non-terminal.
///
/// `non_terminal_refs` accumulates: WHERE column table refs + ancestor join
/// condition column table refs. At each outer join, the optional side's tables
/// are checked against this set.
///
/// The current join's own ON condition is NOT in `non_terminal_refs` during the
/// check — it's only merged before recursing into children. This correctly
/// excludes a join's own condition from the terminal definition.
fn resolved_join_terminality_walk(
    join: &ResolvedJoinNode,
    non_terminal_refs: &HashSet<EcoString>,
    all_optional: &mut HashSet<EcoString>,
    non_terminal: &mut HashSet<EcoString>,
) {
    if let Some(optional_side) = optional_side(join) {
        let mut optional_tables = HashSet::new();
        resolved_source_table_names_collect(optional_side, &mut optional_tables);
        for table in optional_tables {
            if non_terminal_refs.contains(&table) {
                non_terminal.insert(table.clone());
            }
            all_optional.insert(table);
        }
    }

    // Before recursing, merge this join's condition refs so children see them
    // as "ancestor join conditions"
    let mut child_refs = non_terminal_refs.clone();
    if let Some(condition) = join.predicate() {
        resolved_column_table_refs_collect(condition, &mut child_refs);
    }

    // Recurse into nested joins
    for side in [&join.left, &join.right] {
        if let ResolvedTableSource::Join(nested) = side {
            resolved_join_terminality_walk(nested, &child_refs, all_optional, non_terminal);
        }
    }
}

/// The side of an outer join whose rows may be NULL-padded; none for INNER,
/// and FULL is rejected before this analysis runs.
fn optional_side(join: &ResolvedJoinNode) -> Option<&ResolvedTableSource> {
    match join.join_type {
        JoinType::Left => Some(&join.right),
        JoinType::Right => Some(&join.left),
        JoinType::Inner | JoinType::Full => None,
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::{QueryBody, query_expr_parse};
    use crate::query::resolved::select_node_resolve;

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: Oid, column_names: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, col_name)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*col_name).into(),
                position: i16::try_from(i + 1).expect("column position fits in i16"),
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

    /// Parse SQL and resolve the SELECT node using the given tables.
    fn resolve_select(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
        let query_expr = query_expr_parse(sql).expect("convert");
        let select = match query_expr.body {
            QueryBody::Select(s) => s,
            _ => panic!("expected SELECT"),
        };
        select_node_resolve(&select, tables, &["public"]).expect("resolve")
    }

    /// Terminal and non-terminal optional tables of `sql` over the standard
    /// terminality tables.
    fn terminality(sql: &str) -> (HashSet<EcoString>, HashSet<EcoString>) {
        outer_join_optional_tables(&resolve_select(sql, &terminality_test_tables()))
    }

    /// Create standard test tables for terminality tests:
    /// a(id, name, status), b(id, a_id, name, status, val, x), c(id, b_id, val, x)
    fn terminality_test_tables() -> BiHashMap<TableMetadata> {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table("a", Oid::from_raw(1), &["id", "name", "status"]));
        tables.insert_overwrite(test_table(
            "b",
            Oid::from_raw(2),
            &["id", "a_id", "name", "status", "val", "x"],
        ));
        tables.insert_overwrite(test_table(
            "c",
            Oid::from_raw(3),
            &["id", "b_id", "val", "x"],
        ));
        tables
    }

    #[test]
    fn test_terminal_left_join() {
        // b is terminal: only appears in its own ON clause and SELECT list
        let (terminal, non_terminal) =
            terminality("SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1");
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(terminal.contains("b"), "b should be terminal: {terminal:?}");
    }

    #[test]
    fn test_non_terminal_where_reference() {
        // b is non-terminal: b.status appears in WHERE
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status = 'active'");
        assert!(
            non_terminal.contains("b"),
            "b should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_non_terminal_chained_join() {
        // b is non-terminal: b.val appears in the downstream INNER JOIN condition
        // c is not on an outer join's optional side
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.val = c.val");
        assert!(
            non_terminal.contains("b"),
            "b should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_chained_outer_joins() {
        // a LEFT JOIN b ... LEFT JOIN c ON b.x = c.x
        // b is non-terminal: appears in the outer LEFT JOIN's condition (ancestor)
        // c is terminal: only appears in its own ON clause
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.x = c.x");
        assert!(
            non_terminal.contains("b"),
            "b should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.contains("c"), "c should be terminal: {terminal:?}");
    }

    #[test]
    fn test_terminal_right_join() {
        // a is terminal optional side (RIGHT JOIN makes left side optional)
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1");
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(terminal.contains("a"), "a should be terminal: {terminal:?}");
    }

    #[test]
    fn test_non_terminal_right_join() {
        // a is non-terminal optional side: a.status in WHERE
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.status = 'active'");
        assert!(
            non_terminal.contains("a"),
            "a should be non-terminal: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_inner_join_no_optional() {
        // INNER JOIN has no optional side
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a JOIN b ON a.id = b.id WHERE b.x = 1");
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
    }

    #[test]
    fn test_terminal_with_alias() {
        // Aliased table on optional side, terminal.
        // Resolved AST resolves alias "t" back to real table name "b".
        let (terminal, non_terminal) =
            terminality("SELECT a.id, t.name FROM a LEFT JOIN b t ON a.id = t.a_id WHERE a.id = 1");
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(
            terminal.contains("b"),
            "aliased b should be terminal by real name: {terminal:?}"
        );
    }

    #[test]
    fn test_non_terminal_with_alias() {
        // Aliased table on optional side, non-terminal (alias used in WHERE).
        // Resolved AST uses real table name "b" (not alias "t").
        let (terminal, non_terminal) =
            terminality("SELECT * FROM a LEFT JOIN b t ON a.id = t.a_id WHERE t.status = 'active'");
        assert!(
            non_terminal.contains("b"),
            "aliased b should be non-terminal by real name: {non_terminal:?}"
        );
        assert!(terminal.is_empty(), "no terminal: {terminal:?}");
    }

    #[test]
    fn test_mixed_inner_and_terminal_left() {
        // a JOIN b is inner, LEFT JOIN c is terminal
        let (terminal, non_terminal) = terminality(
            "SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1",
        );
        assert!(non_terminal.is_empty(), "no non-terminal: {non_terminal:?}");
        assert!(terminal.contains("c"), "c should be terminal: {terminal:?}");
    }

    #[test]
    fn test_no_join_no_optional() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table("users", Oid::from_raw(1), &["id", "name"]));
        let select = resolve_select("SELECT * FROM users WHERE id = 1", &tables);
        let (terminal, non_terminal) = outer_join_optional_tables(&select);
        assert!(terminal.is_empty());
        assert!(non_terminal.is_empty());
    }
}
