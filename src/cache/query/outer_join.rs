//! Outer-join terminality: which optional-side tables of an outer join CDC can
//! update in place, and which force invalidation.

use std::collections::HashSet;

use ecow::EcoString;

use crate::query::ast::JoinType;
use crate::query::ast::rope::{Join, JoinQual, NodeKind, NodeRef, Select, Source, View};
use crate::query::resolve::{ColumnTarget, ResolvedQuery};

/// The optional-side tables of a SELECT whose FROM is one outer join tree,
/// split into those no predicate references (terminal: a change there
/// cannot drop a cached row) and those one does (non-terminal).
pub fn outer_join_optional_tables(
    bound: &ResolvedQuery,
    select: Select<'_>,
) -> (HashSet<EcoString>, HashSet<EcoString>) {
    let mut from = select.from();
    let (Some(Source::Join(join)), None) = (from.next(), from.next()) else {
        return (HashSet::new(), HashSet::new());
    };
    let mut non_terminal_refs = HashSet::new();
    if let Some(where_clause) = select.where_clause() {
        column_table_refs_collect(bound, where_clause.node_ref(), &mut non_terminal_refs);
    }
    let mut all_optional = HashSet::new();
    let mut non_terminal = HashSet::new();
    join_terminality_walk(
        bound,
        join,
        &non_terminal_refs,
        &mut all_optional,
        &mut non_terminal,
    );
    let terminal = all_optional.difference(&non_terminal).cloned().collect();
    (terminal, non_terminal)
}

/// The tables the column references under `node` bind to: a merged column
/// counts as its left side.
fn column_table_refs_collect(
    bound: &ResolvedQuery,
    node: NodeRef<'_>,
    tables: &mut HashSet<EcoString>,
) {
    let bindings = bound.bindings();
    for position in node.subtree() {
        if !matches!(position.kind(), NodeKind::Column { .. }) {
            continue;
        }
        let binding = match bindings.column_by_id(position.id()) {
            Some(ColumnTarget::Column(binding)) => Some(*binding),
            Some(ColumnTarget::Merged(id)) => bindings.merged(*id).map(|merged| merged.left),
            Some(ColumnTarget::Output) | None => None,
        };
        if let Some(slot) = binding.and_then(|binding| bindings.slot(binding.slot)) {
            tables.insert(slot.table().name.clone());
        }
    }
}

fn source_table_names_collect(source: Source<'_>, names: &mut HashSet<EcoString>) {
    match source {
        Source::Table(table) => {
            names.insert(table.name().clone());
        }
        Source::Join(join) => {
            source_table_names_collect(join.left(), names);
            source_table_names_collect(join.right(), names);
        }
        Source::Subquery(_) | Source::CteRef(_) => {}
    }
}

/// Walk a join tree: each optional side's tables join `all_optional`, and
/// `non_terminal` when a predicate above the join references them; a
/// join's own condition counts for the joins nested inside it.
fn join_terminality_walk(
    bound: &ResolvedQuery,
    join: Join<'_>,
    non_terminal_refs: &HashSet<EcoString>,
    all_optional: &mut HashSet<EcoString>,
    non_terminal: &mut HashSet<EcoString>,
) {
    let optional_side = match join.join_type() {
        JoinType::Left => Some(join.right()),
        JoinType::Right => Some(join.left()),
        JoinType::Inner | JoinType::Full => None,
    };
    if let Some(optional_side) = optional_side {
        let mut optional_tables = HashSet::new();
        source_table_names_collect(optional_side, &mut optional_tables);
        for table in optional_tables {
            if non_terminal_refs.contains(&table) {
                non_terminal.insert(table.clone());
            }
            all_optional.insert(table);
        }
    }
    let mut child_refs = non_terminal_refs.clone();
    match join.qual() {
        JoinQual::On(condition) => {
            column_table_refs_collect(bound, condition.node_ref(), &mut child_refs);
        }
        JoinQual::Using(_) | JoinQual::Natural => {
            let bindings = bound.bindings();
            let join_id = join.id();
            for merged in bindings
                .merged_columns()
                .iter()
                .filter(|merged| merged.join.index() == join_id.index())
            {
                for binding in [&merged.left, &merged.right] {
                    if let Some(slot) = bindings.slot(binding.slot) {
                        child_refs.insert(slot.table().name.clone());
                    }
                }
            }
        }
        JoinQual::Cross => {}
    }
    for side in [join.left(), join.right()] {
        if let Source::Join(nested) = side {
            join_terminality_walk(bound, nested, &child_refs, all_optional, non_terminal);
        }
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::predicates::{OuterJoinRole, query_branch_predicates_build};
    use crate::query::resolve::ResolvedQuery;

    /// Create test table metadata with given column names.
    /// First column is the primary key (INT4), rest are TEXT.
    fn test_table(name: &str, relation_oid: Oid, column_names: &[&str]) -> TableMetadata {
        let columns = column_names.iter().enumerate().map(|(i, col_name)| {
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
        });
        TableMetadata::for_test(name, relation_oid.get(), columns)
    }

    /// Parse and bind SQL over the given tables.
    fn bind(sql: &str, tables: &Catalog) -> ResolvedQuery {
        ResolvedQuery::parse_resolve(sql, tables, &["public"]).expect("resolve")
    }

    /// Terminal and non-terminal optional tables of `sql` over the standard
    /// terminality tables.
    fn terminality(sql: &str) -> (HashSet<EcoString>, HashSet<EcoString>) {
        let bound = bind(sql, &terminality_test_tables());
        outer_join_optional_tables(&bound, bound.select().expect("a SELECT"))
    }

    /// Create standard test tables for terminality tests:
    /// a(id, name, status), b(id, a_id, name, status, val, x), c(id, b_id, val, x)
    fn terminality_test_tables() -> Catalog {
        let mut tables = BiHashMap::new();
        tables
            .insert_overwrite(test_table("a", Oid::from_raw(1), &["id", "name", "status"]).into());
        tables.insert_overwrite(
            test_table(
                "b",
                Oid::from_raw(2),
                &["id", "a_id", "name", "status", "val", "x"],
            )
            .into(),
        );
        tables.insert_overwrite(
            test_table("c", Oid::from_raw(3), &["id", "b_id", "val", "x"]).into(),
        );
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
        // The bindings name the real table "b" behind the alias "t".
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
        // The bindings name the real table "b", not the alias "t".
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
        tables.insert_overwrite(test_table("users", Oid::from_raw(1), &["id", "name"]).into());
        let bound = bind("SELECT * FROM users WHERE id = 1", &tables);
        let (terminal, non_terminal) =
            outer_join_optional_tables(&bound, bound.select().expect("a SELECT"));
        assert!(terminal.is_empty());
        assert!(non_terminal.is_empty());
    }

    /// The predicate model's outer-join role agrees with terminality,
    /// except that it keeps a table whose outer join a NULL-rejecting
    /// conjunct above reduces, where terminality still calls it optional.
    #[test]
    fn test_outer_join_role_agrees_with_terminality() {
        #[rustfmt::skip]
        let cases: &[(&str, &[&str])] = &[
            ("SELECT a.id, b.name FROM a LEFT JOIN b ON a.id = b.a_id WHERE a.id = 1", &[]),
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status = 'active'", &["b"]),
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id JOIN c ON b.val = c.val", &["b"]),
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id LEFT JOIN c ON b.x = c.x", &[]),
            ("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE b.id = 1", &[]),
            ("SELECT * FROM a RIGHT JOIN b ON a.id = b.a_id WHERE a.status = 'active'", &["a"]),
            ("SELECT * FROM a JOIN b ON a.id = b.id WHERE b.x = 1", &[]),
            ("SELECT a.id, t.name FROM a LEFT JOIN b t ON a.id = t.a_id WHERE a.id = 1", &[]),
            ("SELECT * FROM a LEFT JOIN b t ON a.id = t.a_id WHERE t.status = 'active'", &["b"]),
            ("SELECT * FROM a JOIN b ON a.id = b.a_id LEFT JOIN c ON b.id = c.b_id WHERE a.id = 1",
             &[]),
            ("SELECT * FROM a LEFT JOIN b ON a.id = b.a_id WHERE b.status IS NULL", &[]),
        ];
        for (sql, reduced) in cases {
            let bound = bind(sql, &terminality_test_tables());
            let select = bound.select().expect("a SELECT");
            let (terminal, non_terminal) = outer_join_optional_tables(&bound, select);
            let predicates = query_branch_predicates_build(&bound, select);
            for reference in predicates.table_references() {
                let table = bound
                    .bindings()
                    .slot(reference.slot)
                    .expect("slot")
                    .table()
                    .name
                    .clone();
                let expected = if reduced.contains(&table.as_str()) {
                    assert!(
                        non_terminal.contains(&table),
                        "{sql}: {table} optional today"
                    );
                    OuterJoinRole::Kept
                } else if terminal.contains(&table) {
                    OuterJoinRole::Terminal
                } else if non_terminal.contains(&table) {
                    OuterJoinRole::Optional
                } else {
                    OuterJoinRole::Kept
                };
                let role = predicates
                    .table_reference_predicates(reference.slot)
                    .expect("reference predicates")
                    .outer_join_role;
                assert_eq!(role, expected, "{sql}: {table}");
            }
        }
    }
}
