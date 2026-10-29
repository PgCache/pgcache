//! A single-branch join's shape and the FROM rows its WHERE admits, for
//! same-shape subsumption (ADR-024, PGC-417): a cached join holds every
//! FROM row of its shape that passes its WHERE, so a join of the same shape
//! under a WHERE that implies it is served from those rows.

use std::hash::{DefaultHasher, Hash, Hasher};

use ecow::EcoString;

#[cfg(feature = "proxy")]
use crate::catalog::Catalog;
use crate::oid::Oid;
use crate::query::ast::rope::{NodeKind, View};
#[cfg(feature = "proxy")]
use crate::query::constraint_index::table_constraints_keyed;
use crate::query::constraints::{TableConstraint, TableRanges};
use crate::query::predicates::{ConjunctForm, ConjunctScope, query_branch_predicates_build};
use crate::query::resolve::{ColumnTarget, ResolvedQuery, SlotId, TableSlot};
use crate::query::update::select_branches_with_source;

/// Hash of a single-branch query's FROM clause with its tables as relation
/// oids and its columns as (slot, position): two queries of one shape read
/// the same FROM rows, whatever their aliases, WHERE, projection, grouping
/// or ordering. Literals in ON are part of the shape.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JoinShape(u64);

/// One WHERE constraint of a join, on a column of one of its slots.
#[derive(Debug, Clone)]
pub struct SlotConstraint {
    pub slot: SlotId,
    pub relation_oid: Oid,
    pub constraint: TableConstraint,
}

/// The FROM rows a single-branch join reads: those of its shape that pass
/// its WHERE, described by the WHERE's single-column constraints.
#[derive(Debug, Clone)]
pub struct JoinRegion {
    pub shape: JoinShape,
    pub constraints: Vec<SlotConstraint>,
    /// `constraints` as one table's ranges, each column keyed by its slot.
    pub ranges: TableRanges,
    /// Every WHERE conjunct is one of `constraints`, so the region is
    /// exactly what population loaded and the query can parent a join of
    /// its shape. A WHERE the model can't read whole still probes.
    pub exact: bool,
}

/// The join region of `bound`: `None` unless it is one SELECT over two or
/// more base tables with no subquery, CTE or set operation anywhere.
pub fn query_join_region(bound: &ResolvedQuery) -> Option<JoinRegion> {
    let root = bound.tree().root();
    let select = bound.select()?;
    if root.ctes().len() != 0 || select_branches_with_source(root).len() != 1 {
        return None;
    }
    let bindings = bound.bindings();
    let slots = bindings.slots();
    if slots.len() < 2
        || slots
            .iter()
            .any(|slot| !matches!(slot, TableSlot::Base { .. }))
    {
        return None;
    }

    let mut hasher = DefaultHasher::new();
    for source in select.from() {
        for node in source.node_ref().subtree() {
            let kind = node.kind();
            if matches!(kind, NodeKind::Table { .. }) {
                let slot = bindings.source(node.id())?;
                let table = bindings.slot(slot)?.table();
                (0u8, table.relation_oid, slot.index()).hash(&mut hasher);
            } else if matches!(kind, NodeKind::Column { .. }) {
                match bindings.column_by_id(node.id())? {
                    ColumnTarget::Column(binding) => {
                        (1u8, binding.slot.index(), binding.position.index()).hash(&mut hasher);
                    }
                    ColumnTarget::Merged(id) => {
                        let merged = bindings.merged(*id)?;
                        for binding in [merged.left, merged.right] {
                            (2u8, binding.slot.index(), binding.position.index()).hash(&mut hasher);
                        }
                    }
                    ColumnTarget::Output => return None,
                }
            } else {
                (3u8, kind).hash(&mut hasher);
            }
        }
    }
    let shape = JoinShape(hasher.finish());

    let predicates = query_branch_predicates_build(bound, select);
    let mut constraints = Vec::new();
    let mut exact = true;
    for conjunct in predicates.conjuncts() {
        if conjunct.scope != ConjunctScope::Where {
            continue;
        }
        // A constraint that reads as one column of one reference holds on a
        // FROM row exactly as its range says, NULL padding included; a
        // merged column under a FULL join is two columns and reads as none.
        let constraint = match &conjunct.form {
            ConjunctForm::Constraint(constraint)
                if !conjunct.external
                    && conjunct
                        .columns
                        .iter()
                        .all(|column| *column == constraint.column) =>
            {
                constraint
            }
            ConjunctForm::Constraint(_) | ConjunctForm::Equality(..) | ConjunctForm::Opaque => {
                exact = false;
                continue;
            }
        };
        let slot = constraint.column.slot;
        let name = bindings.binding_column(&constraint.column)?.name.clone();
        constraints.push(SlotConstraint {
            slot,
            relation_oid: bindings.slot(slot)?.table().relation_oid,
            constraint: constraint.clone().map_column(|_| name),
        });
    }
    let ranges = slot_constraints_ranges(constraints.iter().map(|c| (c.slot, &c.constraint)));
    Some(JoinRegion {
        shape,
        constraints,
        ranges,
        exact,
    })
}

/// `constraints` as one table's ranges, each column named by its slot.
fn slot_constraints_ranges<'a>(
    constraints: impl Iterator<Item = (SlotId, &'a TableConstraint)>,
) -> TableRanges {
    let keyed: Vec<TableConstraint> = constraints
        .map(|(slot, constraint)| TableConstraint {
            column: EcoString::from(format!("{}.{}", slot.index(), constraint.column)),
            kind: constraint.kind.clone(),
        })
        .collect();
    TableRanges::build(&keyed)
}

#[cfg(feature = "proxy")]
impl JoinRegion {
    /// `ranges` with every literal in its index-key form for its column's
    /// type (PGC-478), as the shape index is built and probed.
    pub fn keyed_ranges(&self, tables: &Catalog) -> TableRanges {
        let keyed: Vec<(SlotId, TableConstraint)> = self
            .constraints
            .iter()
            .map(|constraint| {
                let keyed = tables
                    .get1(&constraint.relation_oid)
                    .and_then(|table| {
                        table_constraints_keyed(std::slice::from_ref(&constraint.constraint), table)
                            .pop()
                    })
                    .unwrap_or_else(|| constraint.constraint.clone());
                (constraint.slot, keyed)
            })
            .collect();
        slot_constraints_ranges(keyed.iter().map(|(slot, constraint)| (*slot, constraint)))
    }
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::constraints::{ColumnConstraintKind, table_ranges_subsumed};

    fn catalog() -> Catalog {
        let int_column = |name: &str, position: i16| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: position == 1,
        };
        let mut tables = BiHashMap::new();
        for (oid, (schema, name)) in
            (1001..).zip([("public", "a"), ("public", "b"), ("other", "a")])
        {
            let mut table = TableMetadata::for_test(
                name,
                oid,
                [int_column("id", 1), int_column("x", 2), int_column("y", 3)],
            );
            table.schema = schema.into();
            tables.insert_overwrite(table.into());
        }
        tables
    }

    fn region(sql: &str) -> Option<JoinRegion> {
        let tables = catalog();
        let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve");
        query_join_region(&bound)
    }

    fn shape(sql: &str) -> JoinShape {
        region(sql).expect("a join region").shape
    }

    /// Each constraint as `slot.column kind`, in WHERE order.
    fn constraint_lines(sql: &str) -> (Vec<String>, bool) {
        let region = region(sql).expect("a join region");
        let lines = region
            .constraints
            .iter()
            .map(|constraint| {
                let kind = match &constraint.constraint.kind {
                    ColumnConstraintKind::Comparison { op, value } => format!("{op:?} {value:?}"),
                    ColumnConstraintKind::AnyOf(values) => format!("AnyOf {values:?}"),
                    ColumnConstraintKind::NullTest(test) => format!("{test:?}"),
                    ColumnConstraintKind::CastComparison { cast, op, value } => {
                        format!("{cast:?} {op:?} {value:?}")
                    }
                };
                format!(
                    "{}.{} {kind}",
                    constraint.slot.index(),
                    constraint.constraint.column
                )
            })
            .collect();
        (lines, region.exact)
    }

    /// Whether the WHERE region of `cached` holds every FROM row of `new`,
    /// the shape being the same.
    fn parent_covers(cached: &str, new: &str) -> bool {
        let parent = region(cached).expect("parent region");
        let child = region(new).expect("child region");
        assert_eq!(
            parent.shape, child.shape,
            "{cached} and {new} share a shape"
        );
        parent.exact && table_ranges_subsumed(&child.ranges, &parent.ranges)
    }

    /// The shape reads through aliases, WHERE, projection and ordering,
    /// and changes with the join type, the ON, the table order, an ON
    /// literal and the relation a name resolves to.
    #[test]
    fn test_join_shape_equivalence() {
        const BASE: &str = "SELECT a.id FROM a JOIN b ON b.id = a.id WHERE a.x = 1";
        let same = [
            "SELECT * FROM a p JOIN b q ON q.id = p.id WHERE p.y = 2 ORDER BY p.id",
            "SELECT a.id, count(*) FROM a JOIN b ON b.id = a.id GROUP BY a.id",
            "SELECT a.id FROM public.a JOIN public.b ON b.id = a.id",
            "SELECT a.id FROM a INNER JOIN b ON (b.id = a.id)",
        ];
        for sql in same {
            assert_eq!(shape(sql), shape(BASE), "{sql}");
        }
        let different = [
            "SELECT a.id FROM a LEFT JOIN b ON b.id = a.id WHERE a.x = 1",
            "SELECT a.id FROM a JOIN b ON b.id = a.x WHERE a.x = 1",
            "SELECT a.id FROM b JOIN a ON b.id = a.id WHERE a.x = 1",
            "SELECT a.id FROM a JOIN b ON b.id = a.id AND b.x > 5",
            "SELECT a.id FROM a JOIN b USING (id)",
            "SELECT a.id FROM other.a JOIN b ON b.id = a.id WHERE a.x = 1",
            "SELECT a.id FROM a, b WHERE b.id = a.id",
        ];
        for sql in different {
            assert_ne!(shape(sql), shape(BASE), "{sql}");
        }
        assert_ne!(
            shape("SELECT a.id FROM a JOIN b ON b.id = a.id AND b.x > 5"),
            shape("SELECT a.id FROM a JOIN b ON b.id = a.id AND b.x > 6"),
        );
        assert_eq!(
            shape("SELECT * FROM a p JOIN a q ON q.id = p.x"),
            shape("SELECT * FROM a q JOIN a p ON p.id = q.x"),
            "a self-join keys its slots by position"
        );
    }

    /// Only one SELECT over two or more base tables has a region.
    #[test]
    fn test_join_region_single_branch_only() {
        let none = [
            "SELECT * FROM a WHERE x = 1",
            "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x IN (SELECT id FROM b)",
            "SELECT * FROM a JOIN (SELECT * FROM b) d ON d.id = a.id",
            "WITH c AS (SELECT id FROM b) SELECT * FROM a JOIN c ON c.id = a.id",
            "SELECT a.id FROM a JOIN b ON b.id = a.id UNION SELECT id FROM a",
            "SELECT (SELECT count(*) FROM b) FROM a JOIN b ON b.id = a.id",
        ];
        for sql in none {
            assert!(region(sql).is_none(), "{sql}");
        }
        assert!(region("SELECT * FROM a JOIN b ON b.id = a.id").is_some());
    }

    /// The region holds the WHERE's single-column constraints keyed by
    /// slot, nothing from ON; a conjunct it can't read as one leaves the
    /// region inexact.
    #[test]
    fn test_join_region_where_constraints_by_slot() {
        let cases: &[(&str, &[&str], bool)] = &[
            (
                "SELECT * FROM a p JOIN a q ON q.id = p.x WHERE p.y = 1 AND q.y = 2",
                &["0.y Equal Integer(1)", "1.y Equal Integer(2)"],
                true,
            ),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id AND b.x > 5 WHERE a.y IN (1, 2)",
                &["0.y AnyOf [Integer(1), Integer(2)]"],
                true,
            ),
            (
                "SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL",
                &["1.x IsNull"],
                true,
            ),
            ("SELECT * FROM a JOIN b ON b.id = a.id", &[], true),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1 OR a.y = 2",
                &[],
                false,
            ),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = b.y AND a.y = 3",
                &["0.y Equal Integer(3)"],
                false,
            ),
            (
                "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1 AND b.y > a.y",
                &["0.x Equal Integer(1)"],
                false,
            ),
            (
                "SELECT * FROM a FULL JOIN b USING (id) WHERE id = 1",
                &[],
                false,
            ),
        ];
        for (sql, expected, exact) in cases {
            let expected: Vec<String> = expected.iter().map(ToString::to_string).collect();
            assert_eq!(constraint_lines(sql), (expected, *exact), "{sql}");
        }
    }

    /// A parent covers a child of its shape when the child's WHERE
    /// constraints imply the parent's on every column, slot by slot; an ON
    /// constraint is no WHERE constraint, and an inexact WHERE is no parent.
    #[test]
    fn test_join_region_cover() {
        const JOIN: &str = "SELECT * FROM a JOIN b ON b.id = a.id";
        const LEFT: &str = "SELECT * FROM a LEFT JOIN b ON b.id = a.id";
        const SELF: &str = "SELECT * FROM a p JOIN a q ON q.id = p.x";
        let cases = [
            (
                format!("{JOIN} WHERE a.x = 1"),
                format!("{JOIN} WHERE a.x = 1 AND b.y = 2"),
                true,
            ),
            (
                format!("{JOIN} WHERE a.x > 0"),
                format!("{JOIN} WHERE a.x > 1 AND a.y IN (1, 2)"),
                true,
            ),
            (JOIN.to_owned(), format!("{JOIN} WHERE b.y = 2"), true),
            (
                format!("{JOIN} WHERE a.x = 1"),
                format!("{JOIN} WHERE a.x = 2"),
                false,
            ),
            (format!("{JOIN} WHERE a.x = 1"), JOIN.to_owned(), false),
            (
                format!("{JOIN} WHERE a.x = 1 OR a.y = 2"),
                format!("{JOIN} WHERE a.x = 1 AND a.y = 2"),
                false,
            ),
            (
                format!("{LEFT} WHERE b.x IS NULL"),
                format!("{LEFT} WHERE b.x IS NULL AND a.y = 1"),
                true,
            ),
            (
                format!("{LEFT} WHERE b.x IS NULL"),
                format!("{LEFT} WHERE a.y = 1"),
                false,
            ),
            (
                format!("{LEFT} WHERE b.x IS NOT NULL"),
                format!("{LEFT} WHERE b.x = 1"),
                true,
            ),
            (
                format!("{LEFT} WHERE b.x = 1"),
                format!("{LEFT} WHERE b.x IS NOT NULL"),
                false,
            ),
            (
                format!("{SELF} WHERE p.y = 1"),
                format!("{SELF} WHERE p.y = 1 AND q.y = 2"),
                true,
            ),
            (
                format!("{SELF} WHERE p.y = 1"),
                format!("{SELF} WHERE q.y = 1"),
                false,
            ),
        ];
        for (cached, new, expected) in cases {
            assert_eq!(
                parent_covers(&cached, &new),
                expected,
                "cached {cached:?} covers new {new:?}"
            );
        }
    }

    /// The keyed ranges rewrite each literal for its own slot's column
    /// type, so a child spelled differently still probes the same key.
    #[test]
    fn test_join_region_keyed_ranges_per_slot() {
        let tables = catalog();
        let region = |sql: &str| {
            let bound = ResolvedQuery::parse_resolve(sql, &tables, &["public"]).expect("resolve");
            query_join_region(&bound).expect("a join region")
        };
        let parent = region("SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = 1 AND b.y = 2");
        let child = region("SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x = '1' AND b.y = 2");
        assert!(table_ranges_subsumed(
            &child.keyed_ranges(&tables),
            &parent.keyed_ranges(&tables)
        ));
    }
}
