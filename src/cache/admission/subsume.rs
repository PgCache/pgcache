//! The subsumption-coverage decision, extracted from the writer's
//! `subsumption_check` (PGC-391). Pure over a [`SubsumerSource`]: the writer
//! supplies candidates from its per-relation index plus parent state,
//! pgcache-fit from its offline registry.

use ecow::EcoString;

use super::{CandidateState, SubsumerCandidate, SubsumerSource};
use crate::oid::Oid;
use crate::query::Fingerprint;
use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{
    ColumnConstraintKind, TableConstraint, TableRanges, literal_value_canonical_order,
    table_ranges_subsumed,
};
use crate::query::join_region::JoinRegion;
use crate::query::resolve::ResolvedQuery;
use crate::query::update::query_base_references;

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
    let base = query_base_references(bound);
    if !base.uncovered.is_empty() {
        return None;
    }
    base.references
        .into_iter()
        .map(|reference| {
            let relation = reference.predicates?;
            Some(SubsumptionReference {
                relation_oid: reference.table.relation_oid,
                filters: relation.filters,
                ranges: relation.ranges,
            })
        })
        .collect()
}

/// Most pieces an in-set split probes; a reference whose split would need
/// more falls back to the single-parent rule (PGC-494).
const SUBSUMPTION_SPLIT_CAP: usize = 64;

/// Most in-set columns a split chooses among, smallest sets first. A set
/// has at least two values, so no more than this many columns can split
/// together within [`SUBSUMPTION_SPLIT_CAP`].
const SUBSUMPTION_SPLIT_COLUMNS: usize = SUBSUMPTION_SPLIT_CAP.ilog2() as usize;

/// How the new query's references were covered.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SubsumptionCover {
    /// Every reference held by one parent.
    Whole,
    /// Some reference was covered value by value: split on its in-set
    /// columns, each piece held by a parent of its own (PGC-494).
    Split,
    /// The whole join held by a cached join of the same shape whose WHERE
    /// the new query's implies (PGC-417).
    Join,
}

/// Whether the new query is covered: as a whole by a parent of its join
/// shape whose WHERE region holds every FROM row its own WHERE admits
/// ([`join_covered`]), else reference by reference on each relation, by
/// one registered candidate whose region holds every row the reference's
/// filters admit, or, failing that, piece by piece once the reference is
/// split on its in-set columns ([`reference_covered_split`]). Candidate
/// gates: no LIMIT, parent Ready. An empty list is never covered.
pub fn subsumption_covered(
    references: &[SubsumptionReference],
    join: Option<&JoinRegion>,
    source: &impl SubsumerSource,
) -> Option<SubsumptionCover> {
    cover_decide(references, join, source, None)
}

/// The one parent still populating whose Ready would cover the new query
/// as [`subsumption_covered`] requires (PGC-496): the same cover, allowed
/// to lean on a single populating candidate beside Ready ones. `None` when
/// Ready parents already cover it, when it would take a second populating
/// parent, or when only a split would cover a reference.
pub fn subsumption_pending_parent(
    references: &[SubsumptionReference],
    join: Option<&JoinRegion>,
    source: &impl SubsumerSource,
) -> Option<Fingerprint> {
    let mut pending = None;
    cover_decide(references, join, source, Some(&mut pending))?;
    pending
}

/// The cover, by a parent of the join's shape ([`join_covered`]) or
/// reference by reference; `pending` lets one populating parent hold.
fn cover_decide(
    references: &[SubsumptionReference],
    join: Option<&JoinRegion>,
    source: &impl SubsumerSource,
    mut pending: Option<&mut Option<Fingerprint>>,
) -> Option<SubsumptionCover> {
    if references.is_empty() {
        return None;
    }
    if join.is_some_and(|join| join_covered(join, source, pending.as_deref_mut())) {
        return Some(SubsumptionCover::Join);
    }
    let mut cover = SubsumptionCover::Whole;
    for reference in references {
        if candidate_holds(
            source.candidates(reference),
            &reference.ranges,
            pending.as_deref_mut(),
        ) {
            continue;
        }
        if !reference_covered_split(reference, source) {
            return None;
        }
        cover = SubsumptionCover::Split;
    }
    Some(cover)
}

/// Whether one Ready candidate on the reference's relation holds its whole
/// region.
fn reference_covered(reference: &SubsumptionReference, source: &impl SubsumerSource) -> bool {
    candidate_holds(source.candidates(reference), &reference.ranges, None)
}

/// Whether one parent of the join's shape holds every FROM row its WHERE
/// admits: the parent loaded every FROM row of the shape passing its own
/// WHERE, and the new query's WHERE constraints imply that WHERE column by
/// column, each column keyed by its slot, so a NULL-padded row answers like
/// any other. A row the cache assembles from the parent's rows is a FROM
/// row origin has, so serving the new query over them returns its result.
fn join_covered(
    join: &JoinRegion,
    source: &impl SubsumerSource,
    pending: Option<&mut Option<Fingerprint>>,
) -> bool {
    candidate_holds(source.join_candidates(join), &join.ranges, pending)
}

/// Whether a LIMIT-free candidate holds `ranges`: a Ready one, or, when
/// `pending` is given, a populating one, which `pending` records as the
/// parent the cover waits on. A cover leans on one such parent at most, so
/// a second populating candidate holds only when it is that parent.
fn candidate_holds<'a>(
    candidates: impl Iterator<Item = SubsumerCandidate<'a>>,
    ranges: &TableRanges,
    pending: Option<&mut Option<Fingerprint>>,
) -> bool {
    let mut populating: Vec<Fingerprint> = Vec::new();
    for candidate in candidates {
        if candidate.has_limit || !table_ranges_subsumed(ranges, candidate.region) {
            continue;
        }
        match candidate.state {
            CandidateState::Ready => return true,
            CandidateState::Populating => populating.push(candidate.fingerprint),
            CandidateState::Unavailable => {}
        }
    }
    let Some(pending) = pending else {
        return false;
    };
    match pending {
        Some(parent) => populating.contains(parent),
        None => match populating.first() {
            Some(first) => {
                *pending = Some(*first);
                true
            }
            None => false,
        },
    }
}

/// One in-set column a split fixes, with its values in a stable order.
type SplitColumn<'a> = (&'a EcoString, Vec<&'a LiteralValue>);

/// Whether the reference is covered value by value: split on some of its
/// in-set columns, every piece held by a candidate. Splitting on more
/// columns only narrows the pieces, so a piece a parent holds stays held;
/// each maximal set of columns whose piece count fits the cap is tried, and
/// the reference is covered when any one of them is.
fn reference_covered_split(reference: &SubsumptionReference, source: &impl SubsumerSource) -> bool {
    let mut columns: Vec<SplitColumn<'_>> = reference
        .ranges
        .set_columns()
        .filter(|(_, values)| values.len() > 1)
        .map(|(column, values)| {
            let mut values: Vec<&LiteralValue> = values.iter().collect();
            values.sort_by(|a, b| literal_value_canonical_order(a, b));
            (column, values)
        })
        .collect();
    columns.sort_by(|a, b| a.1.len().cmp(&b.1.len()).then_with(|| a.0.cmp(b.0)));
    columns.truncate(SUBSUMPTION_SPLIT_COLUMNS);
    split_maximal_subsets(&columns).into_iter().any(|split| {
        reference_pieces(reference, &split).all(|piece| reference_covered(&piece, source))
    })
}

/// Every subset of `columns` whose piece count fits [`SUBSUMPTION_SPLIT_CAP`]
/// and that no further column could join within it.
fn split_maximal_subsets<'a, 'b>(columns: &'b [SplitColumn<'a>]) -> Vec<Vec<&'b SplitColumn<'a>>> {
    let fits = |count: usize, column: &SplitColumn<'a>| {
        count
            .checked_mul(column.1.len())
            .is_some_and(|count| count <= SUBSUMPTION_SPLIT_CAP)
    };
    let mut subsets = Vec::new();
    let mut chosen: Vec<&'b SplitColumn<'a>> = Vec::new();
    // Over the bitmasks of a column list this short, a subset is maximal
    // when every column it leaves out would overflow the cap.
    for mask in 1u32..1u32 << columns.len() {
        chosen.clear();
        let mut count = 1usize;
        let mut feasible = true;
        for (index, column) in columns.iter().enumerate() {
            if mask & (1 << index) == 0 {
                continue;
            }
            if !fits(count, column) {
                feasible = false;
                break;
            }
            count *= column.1.len();
            chosen.push(column);
        }
        if !feasible {
            continue;
        }
        let maximal = columns
            .iter()
            .enumerate()
            .filter(|(index, _)| mask & (1 << index) == 0)
            .all(|(_, column)| !fits(count, column));
        if maximal {
            subsets.push(chosen.clone());
        }
    }
    subsets
}

/// The reference split value by value on `split`'s columns: one piece per
/// combination of values, the reference's filters with those columns fixed
/// to the combination. Every row of the reference falls in exactly one
/// piece, so the reference is covered when each piece is. Pieces are built
/// as they are consumed, so a failing probe stops the work early.
fn reference_pieces<'a>(
    reference: &'a SubsumptionReference,
    split: &'a [&'a SplitColumn<'a>],
) -> impl Iterator<Item = SubsumptionReference> + 'a {
    let count: usize = split.iter().map(|(_, values)| values.len()).product();
    (0..count).map(move |index| {
        let mut rest = index;
        let assignment: Vec<(&EcoString, &LiteralValue)> = split
            .iter()
            .filter_map(|(column, values)| {
                let value = values.get(rest % values.len())?;
                rest /= values.len();
                Some((*column, *value))
            })
            .collect();
        reference_piece(reference, &assignment)
    })
}

/// The reference with each column of `assignment` fixed to its value.
fn reference_piece(
    reference: &SubsumptionReference,
    assignment: &[(&EcoString, &LiteralValue)],
) -> SubsumptionReference {
    // The column's bare constraints reduce to the set, so the equality
    // replaces them; cast constraints stay in their domain.
    let mut filters: Vec<TableConstraint> = reference
        .filters
        .iter()
        .filter(|constraint| {
            constraint.kind.cast_target().is_some()
                || !assignment
                    .iter()
                    .any(|(column, _)| **column == constraint.column)
        })
        .cloned()
        .collect();
    for (column, value) in assignment {
        filters.push(TableConstraint {
            column: (*column).clone(),
            kind: ColumnConstraintKind::Comparison {
                op: BinaryOp::Equal,
                value: (*value).clone(),
            },
        });
    }
    SubsumptionReference {
        relation_oid: reference.relation_oid,
        ranges: TableRanges::build(&filters),
        filters,
    }
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
