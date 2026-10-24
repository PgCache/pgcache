//! A table's constraints reduced once to per-column ranges, and the
//! subsumption decision over them: does a cached query's region on a table
//! hold every row a new query's reference reads?

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use super::TableConstraint;
use super::range::{ColumnRange, column_range_build, column_range_subsumes};
use crate::query::ast::LiteralValue;
use crate::query::cast::CastTarget;

/// One column's ranges: its bare constraints reduced to one range, and one
/// range per cast whose output domain a constraint compares in. The two are
/// kept apart so each range reasons within a consistent value domain.
#[derive(Debug, Clone, Default)]
struct ColumnBuckets {
    bare: Option<ColumnRange>,
    casts: Vec<(CastTarget, ColumnRange)>,
}

/// A table's constraints reduced once to per-column ranges, as subsumption,
/// the constraint index and read-after-write consume them.
#[derive(Debug, Clone, Default)]
pub struct TableRanges {
    columns: HashMap<EcoString, ColumnBuckets>,
}

impl TableRanges {
    /// The ranges of `constraints`, AND-connected per column and cast.
    pub fn build(constraints: &[TableConstraint]) -> Self {
        let mut grouped: HashMap<(&EcoString, Option<&CastTarget>), Vec<&TableConstraint>> =
            HashMap::new();
        for constraint in constraints {
            grouped
                .entry((&constraint.column, constraint.kind.cast_target()))
                .or_default()
                .push(constraint);
        }
        let mut columns: HashMap<EcoString, ColumnBuckets> = HashMap::new();
        for ((column, cast), group) in grouped {
            let range = column_range_build(&group);
            let buckets = columns.entry(column.clone()).or_default();
            match cast {
                None => buckets.bare = Some(range),
                Some(cast) => buckets.casts.push((cast.clone(), range)),
            }
        }
        Self { columns }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    /// Every column some constraint names.
    pub fn columns(&self) -> impl Iterator<Item = &EcoString> {
        self.columns.keys()
    }

    /// Every column whose bare range is a finite set of values, with the set.
    pub(crate) fn set_columns(&self) -> impl Iterator<Item = (&EcoString, &HashSet<LiteralValue>)> {
        self.columns
            .iter()
            .filter_map(|(column, buckets)| match &buckets.bare {
                Some(ColumnRange::InSet(values)) => Some((column, values)),
                Some(
                    ColumnRange::Unknown
                    | ColumnRange::Unconstrained
                    | ColumnRange::Empty
                    | ColumnRange::Equal(_)
                    | ColumnRange::Null
                    | ColumnRange::Range { .. },
                )
                | None => None,
            })
    }

    /// `column`'s range as the constraint index reasons about it: its bare
    /// range, or `Unknown` once a cast constraint is on it, since the index
    /// can't reason across cast domains and leaves that to the precise check.
    pub(crate) fn index_range(&self, column: &str) -> ColumnRange {
        match self.columns.get(column) {
            None => ColumnRange::Unconstrained,
            Some(buckets) if !buckets.casts.is_empty() => ColumnRange::Unknown,
            Some(buckets) => buckets.bare.clone().unwrap_or(ColumnRange::Unconstrained),
        }
    }

    /// The bare range per column, for read-after-write disjointness: a cast
    /// comparison constrains a derived value, not the raw column a written
    /// row supplies, so it can't prove disjointness and is left out.
    #[cfg(feature = "proxy")]
    pub(crate) fn bare_columns(&self) -> HashMap<EcoString, ColumnRange> {
        self.columns
            .iter()
            .filter_map(|(column, buckets)| Some((column.clone(), buckets.bare.clone()?)))
            .collect()
    }
}

/// Whether a parent region `cached` holds every row of a reference whose
/// ranges are `new`: each range `cached` has must contain `new`'s on the
/// same column and cast, an absent one being unconstrained. An empty
/// `cached` is the whole table.
pub fn table_ranges_subsumed(new: &TableRanges, cached: &TableRanges) -> bool {
    cached.columns.iter().all(|(column, cached_buckets)| {
        let new_buckets = new.columns.get(column);
        let bare_covered = cached_buckets.bare.as_ref().is_none_or(|cached_range| {
            let new_range = new_buckets.and_then(|buckets| buckets.bare.as_ref());
            column_range_subsumes(
                cached_range,
                new_range.unwrap_or(&ColumnRange::Unconstrained),
            )
        });
        bare_covered
            && cached_buckets.casts.iter().all(|(cast, cached_range)| {
                let new_range = new_buckets
                    .and_then(|buckets| buckets.casts.iter().find(|(new_cast, _)| new_cast == cast))
                    .map(|(_, range)| range);
                column_range_subsumes(
                    cached_range,
                    new_range.unwrap_or(&ColumnRange::Unconstrained),
                )
            })
    })
}

/// [`table_ranges_subsumed`] over constraint lists, reducing both sides; a
/// test convenience, production compares stored ranges.
#[cfg(test)]
pub(crate) fn table_constraints_subsumed(
    new: &[TableConstraint],
    cached: &[TableConstraint],
) -> bool {
    table_ranges_subsumed(&TableRanges::build(new), &TableRanges::build(cached))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{BinaryOp, LiteralValue};
    use crate::query::constraints::ColumnConstraintKind;

    fn constraint(column: &str, kind: ColumnConstraintKind) -> TableConstraint {
        TableConstraint {
            column: column.into(),
            kind,
        }
    }

    fn comparison(column: &str, op: BinaryOp, value: i64) -> TableConstraint {
        constraint(
            column,
            ColumnConstraintKind::Comparison {
                op,
                value: LiteralValue::Integer(value),
            },
        )
    }

    fn cast_comparison(column: &str, op: BinaryOp, value: i64) -> TableConstraint {
        constraint(
            column,
            ColumnConstraintKind::CastComparison {
                cast: CastTarget::Int4,
                op,
                value: LiteralValue::Integer(value),
            },
        )
    }

    /// Bare constraints on a column reduce to one range; a cast constraint
    /// keeps its own, hides the column from the index and from
    /// read-after-write, and the column still counts as constrained.
    #[test]
    fn test_table_ranges_buckets_by_column_and_cast() {
        let ranges = TableRanges::build(&[
            comparison("id", BinaryOp::Equal, 5),
            comparison("id", BinaryOp::GreaterThan, 3),
            cast_comparison("name", BinaryOp::Equal, 42),
            comparison("x", BinaryOp::LessThan, 10),
        ]);
        let mut columns: Vec<&str> = ranges.columns().map(EcoString::as_str).collect();
        columns.sort_unstable();
        assert_eq!(columns, ["id", "name", "x"]);
        assert!(matches!(
            ranges.index_range("id"),
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
        assert!(matches!(ranges.index_range("name"), ColumnRange::Unknown));
        assert!(matches!(
            ranges.index_range("absent"),
            ColumnRange::Unconstrained
        ));
        assert!(TableRanges::default().is_empty());
    }

    /// Read-after-write sees only the bare ranges: a cast comparison can't
    /// prove disjointness against a raw written value.
    #[cfg(feature = "proxy")]
    #[test]
    fn test_table_ranges_bare_columns_drop_casts() {
        let ranges = TableRanges::build(&[
            comparison("id", BinaryOp::Equal, 5),
            cast_comparison("name", BinaryOp::Equal, 42),
            comparison("x", BinaryOp::LessThan, 10),
        ]);
        let bare = ranges.bare_columns();
        assert_eq!(bare.len(), 2);
        assert!(matches!(
            bare.get("id"),
            Some(ColumnRange::Equal(LiteralValue::Integer(5)))
        ));
        assert!(bare.contains_key("x") && !bare.contains_key("name"));
    }

    /// Coverage is decided per column and cast: a cast range covers only a
    /// cast range under the same cast, and a column the parent leaves alone
    /// is covered whatever the child does there.
    #[test]
    fn test_table_ranges_subsumed_per_bucket() {
        let parent = TableRanges::build(&[
            comparison("id", BinaryOp::GreaterThan, 3),
            cast_comparison("name", BinaryOp::GreaterThan, 100),
        ]);
        let child = TableRanges::build(&[
            comparison("id", BinaryOp::Equal, 5),
            cast_comparison("name", BinaryOp::GreaterThan, 200),
            comparison("x", BinaryOp::Equal, 1),
        ]);
        assert!(table_ranges_subsumed(&child, &parent));
        assert!(!table_ranges_subsumed(&parent, &child));
        let bare_name = TableRanges::build(&[
            comparison("id", BinaryOp::Equal, 5),
            comparison("name", BinaryOp::GreaterThan, 200),
        ]);
        assert!(
            !table_ranges_subsumed(&bare_name, &parent),
            "a bare range on the column does not satisfy the parent's cast range"
        );
        assert!(table_ranges_subsumed(&child, &TableRanges::default()));
        assert!(!table_ranges_subsumed(&TableRanges::default(), &child));
    }
}
