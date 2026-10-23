//! The subsumption-coverage decision, extracted from the writer's
//! `subsumption_check` (PGC-391). Pure over a [`SubsumerSource`]: the writer
//! supplies candidates from its per-relation index plus parent state,
//! pgcache-fit from its offline registry.

use std::collections::HashSet;

use super::{SubsumerCandidate, SubsumerSource};
use crate::oid::Oid;
use crate::query::constraints::{TableConstraint, TableRanges, table_ranges_subsumed};
use crate::query::predicates::query_branch_predicates_build;
use crate::query::resolve::{ResolvedQuery, TableSlot};
use crate::query::update::select_branches_with_source;

/// One table reference of a new query: its relation and the filters every
/// row it reads satisfies, as constraints for the source to key and as
/// ranges for the coverage check.
#[derive(Debug, Clone)]
pub struct SubsumptionReference {
    pub relation_oid: Oid,
    pub filters: Vec<TableConstraint>,
    pub ranges: TableRanges,
}

/// Every base table reference of `bound` across its SELECT branches (root,
/// set-operation arms, subqueries, derived tables, CTE bodies), each with
/// its own branch's filters. `None` when a table the query reads is no
/// branch's reference, so coverage can't skip one.
///
/// A set operation is covered when every arm is: each UNION row comes from
/// one arm, and INTERSECT/EXCEPT read both sides in full, so a missing row
/// on either would change the result (PGC-495).
pub fn subsumption_references(bound: &ResolvedQuery) -> Option<Vec<SubsumptionReference>> {
    let bindings = bound.bindings();
    let mut references = Vec::new();
    let mut seen = HashSet::new();
    for (select, _) in select_branches_with_source(bound.tree().root()) {
        let predicates = query_branch_predicates_build(bound, select);
        for reference in predicates.table_references() {
            let Some(TableSlot::Base { table, .. }) = bindings.slot(reference.slot) else {
                continue;
            };
            // A CTE body is a branch once per reference to it.
            if !seen.insert(reference.slot) {
                continue;
            }
            let relation = predicates.relation_predicates(bindings, reference.slot)?;
            references.push(SubsumptionReference {
                relation_oid: table.relation_oid,
                filters: relation.filters,
                ranges: relation.ranges,
            });
        }
    }
    let base_slots = bindings
        .slots()
        .iter()
        .filter(|slot| matches!(slot, TableSlot::Base { .. }))
        .count();
    (seen.len() == base_slots).then_some(references)
}

/// Whether every reference of the new query is covered by some registered
/// candidate on its relation whose region holds every row the reference's
/// filters admit. Candidate gates: no LIMIT, parent Ready. An empty list is
/// never covered.
pub fn subsumption_covered(
    references: &[SubsumptionReference],
    source: &impl SubsumerSource,
) -> bool {
    !references.is_empty()
        && references.iter().all(|reference| {
            source.candidates(reference).any(|candidate| {
                let SubsumerCandidate {
                    region,
                    has_limit,
                    ready,
                } = candidate;
                !has_limit && ready && table_ranges_subsumed(&reference.ranges, region)
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
    use crate::query::constraints::ColumnConstraintKind;

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

    /// Each reference's relation oid with its filters, sorted.
    fn reference_lines(sql: &str) -> Vec<String> {
        let tables = catalog();
        let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve");
        let mut lines: Vec<String> = subsumption_references(&bound)
            .expect("references")
            .iter()
            .map(|reference| {
                let oid = reference.relation_oid;
                let filters: Vec<String> = reference
                    .filters
                    .iter()
                    .map(|constraint| match &constraint.kind {
                        ColumnConstraintKind::Comparison { op, value } => {
                            format!("Comparison({:?}, {op:?}, {value:?})", constraint.column)
                        }
                        ColumnConstraintKind::AnyOf(_)
                        | ColumnConstraintKind::CastComparison { .. }
                        | ColumnConstraintKind::NullTest(_) => format!("{constraint:?}"),
                    })
                    .collect();
                format!("{oid:?} [{}]", filters.join(", "))
            })
            .collect();
        lines.sort();
        lines
    }

    /// Every reference keeps its own branch's filters, a repeated table
    /// included (PGC-493).
    #[test]
    fn test_subsumption_references_per_branch() {
        let t = format!("{:?}", Oid::from_raw(1001));
        let u = format!("{:?}", Oid::from_raw(1002));
        let other_t = format!("{:?}", Oid::from_raw(1003));
        let name = |value: &str| format!(r#"[Comparison("name", Equal, String("{value}"))]"#);
        let cases: Vec<(&str, Vec<String>)> = vec![
            (
                "SELECT id FROM t WHERE name = 'x'",
                vec![format!("{t} {}", name("x"))],
            ),
            (
                "SELECT a.id FROM t a JOIN t b ON b.id = a.id WHERE a.name = 'x'",
                vec![format!("{t} []"), format!("{t} {}", name("x"))],
            ),
            (
                "SELECT id FROM t WHERE name = 'x' \
                 AND id IN (SELECT id FROM t WHERE name = 'y')",
                vec![format!("{t} {}", name("x")), format!("{t} {}", name("y"))],
            ),
            (
                "WITH c AS (SELECT id FROM t WHERE name = 'y') \
                 SELECT id FROM t WHERE name = 'x' AND id IN (SELECT id FROM c)",
                vec![format!("{t} {}", name("x")), format!("{t} {}", name("y"))],
            ),
            (
                "SELECT a.id FROM public.t a JOIN other.t b ON b.id = a.id \
                 WHERE a.name = 'x'",
                vec![format!("{other_t} []"), format!("{t} {}", name("x"))],
            ),
            (
                "SELECT a.id FROM t a JOIN t b ON b.id = a.id \
                 JOIN u ON u.id = a.id WHERE a.name = 'x' AND u.name = 'z'",
                vec![
                    format!("{t} []"),
                    format!("{t} {}", name("x")),
                    format!("{u} {}", name("z")),
                ],
            ),
        ];
        for (sql, mut expected) in cases {
            expected.sort();
            assert_eq!(reference_lines(sql), expected, "{sql}");
        }
    }

    /// Each arm of a set operation is a reference of its own with its own
    /// filters, a repeated table included (PGC-495).
    #[test]
    fn test_subsumption_references_set_operation_per_arm() {
        let t = format!("{:?}", Oid::from_raw(1001));
        let u = format!("{:?}", Oid::from_raw(1002));
        let name = |value: &str| format!(r#"[Comparison("name", Equal, String("{value}"))]"#);
        assert_eq!(
            reference_lines(
                "SELECT id FROM t WHERE name = 'x' UNION SELECT id FROM u WHERE name = 'y'"
            ),
            vec![format!("{t} {}", name("x")), format!("{u} {}", name("y"))],
        );
        assert_eq!(
            reference_lines(
                "SELECT id FROM t WHERE name = 'x' INTERSECT SELECT id FROM t WHERE name = 'y'"
            ),
            vec![format!("{t} {}", name("x")), format!("{t} {}", name("y"))],
        );
        assert_eq!(
            reference_lines(
                "(SELECT id FROM t WHERE name = 'x' EXCEPT SELECT id FROM u) UNION SELECT id FROM t"
            ),
            vec![
                format!("{t} {}", name("x")),
                format!("{t} []"),
                format!("{u} []")
            ],
        );
    }
}
