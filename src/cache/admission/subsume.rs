//! The subsumption-coverage decision, extracted from the writer's
//! `subsumption_check` (PGC-391). Pure over a [`SubsumerSource`]: the writer
//! supplies candidates from its per-relation index plus parent state,
//! pgcache-fit from its offline registry.

use std::collections::HashSet;

use super::{SubsumerCandidate, SubsumerSource};
use crate::oid::Oid;
use crate::query::ast::rope::Select;
use crate::query::constraints::{
    QueryConstraints, TableConstraint, query_constraints_analyze, table_constraints_subsumed,
};
use crate::query::resolve::ResolvedQuery;

/// The new query's constraints as subsumption may read them: those of
/// `select`, minus every table name that more than one FROM entry binds
/// (self-join, a table repeated in a subquery or CTE, one name across
/// schemas). Constraints are keyed by name, so a repeated name's set need not
/// hold for each occurrence; dropping it leaves only a full-table parent able
/// to cover that table (PGC-493).
pub fn subsumption_constraints(bound: &ResolvedQuery, select: Select<'_>) -> QueryConstraints {
    let mut constraints = query_constraints_analyze(bound, select);
    let mut names = HashSet::new();
    for slot in bound.bindings().slots() {
        let name = slot.table().name.as_str();
        if !names.insert(name) {
            constraints.table_constraints.remove(name);
        }
    }
    constraints
}

/// Whether every relation of the new query is covered by some registered
/// candidate whose constraints are implied by the new query's. Candidate
/// gates: no LIMIT, parent Ready.
///
/// `relations` are the new query's `(relation_oid, table_name)` pairs; an
/// empty list is never covered. The new query's constraints must come from a
/// plain SELECT — set operations require per-branch analysis and are
/// rejected by the caller.
pub fn subsumption_covered(
    new_constraints: &QueryConstraints,
    relations: &[(Oid, &str)],
    source: &impl SubsumerSource,
) -> bool {
    if relations.is_empty() {
        return false;
    }
    let empty: Vec<TableConstraint> = Vec::new();
    relations.iter().all(|(relation_oid, table_name)| {
        let new_table_constraints = new_constraints
            .table_constraints
            .get(*table_name)
            .unwrap_or(&empty);
        source
            .candidates(*relation_oid, new_table_constraints)
            .any(|candidate| {
                let SubsumerCandidate {
                    constraints,
                    has_limit,
                    ready,
                } = candidate;
                !has_limit
                    && ready
                    && table_constraints_subsumed(new_constraints, constraints, table_name)
            })
    })
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;

    fn column(name: &str, position: i16, data_type: Type, type_name: &str) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(data_type.oid()),
            data_type,
            type_name: type_name.into(),
            cache_type_name: type_name.into(),
            is_primary_key: position == 1,
        }
    }

    fn table(schema: &str, name: &str, oid: u32) -> TableMetadata {
        let mut table = TableMetadata::for_test(
            name,
            oid,
            [
                column("id", 1, Type::INT4, "int4"),
                column("name", 2, Type::TEXT, "text"),
            ],
        );
        table.schema = schema.into();
        table
    }

    fn catalog() -> Catalog {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(table("public", "t", 1001).into());
        tables.insert_overwrite(table("public", "u", 1002).into());
        tables.insert_overwrite(table("other", "t", 1003).into());
        tables
    }

    /// The table names whose constraints subsumption may read.
    fn constrained_tables(sql: &str) -> Vec<String> {
        let tables = catalog();
        let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve");
        let constraints = subsumption_constraints(&bound, bound.select().expect("a SELECT"));
        let mut names: Vec<String> = constraints
            .table_constraints
            .keys()
            .map(|name| name.to_string())
            .collect();
        names.sort();
        names
    }

    /// A table name bound more than once keeps no constraints; every other
    /// table keeps its own (PGC-493).
    #[test]
    fn test_subsumption_constraints_drop_repeated_table_names() {
        let cases: &[(&str, &[&str])] = &[
            ("SELECT id FROM t WHERE name = 'x'", &["t"]),
            (
                "SELECT a.id FROM t a JOIN t b ON b.id = a.id WHERE a.name = 'x'",
                &[],
            ),
            (
                "SELECT id FROM t WHERE name = 'x' \
                 AND id IN (SELECT id FROM t WHERE name = 'y')",
                &[],
            ),
            (
                "WITH c AS (SELECT id FROM t WHERE name = 'y') \
                 SELECT id FROM t WHERE name = 'x' AND id IN (SELECT id FROM c)",
                &[],
            ),
            (
                "SELECT a.id FROM public.t a JOIN other.t b ON b.id = a.id \
                 WHERE a.name = 'x'",
                &[],
            ),
            (
                "SELECT a.id FROM t a JOIN t b ON b.id = a.id \
                 JOIN u ON u.id = a.id WHERE a.name = 'x' AND u.name = 'z'",
                &["u"],
            ),
        ];
        for (sql, expected) in cases {
            assert_eq!(constrained_tables(sql), *expected, "{sql}");
        }
    }
}
