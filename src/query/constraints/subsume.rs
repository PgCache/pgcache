//! A table's constraints reduced once to per-column ranges, and the
//! subsumption decision over them: does a cached query's region on a table
//! hold every row a new query's reference reads?

use std::collections::HashSet;

use ecow::{EcoString, EcoVec};
use smallvec::SmallVec;

use super::TableConstraint;
use super::range::{ColumnRange, column_range_build, column_range_subsumes};
use crate::query::ast::LiteralValue;
use crate::query::cast::CastTarget;

/// One column's ranges: its bare constraints reduced to one range, and one
/// range per cast whose output domain a constraint compares in. The two are
/// kept apart so each range reasons within a consistent value domain.
#[derive(Debug, Clone)]
struct ColumnRanges {
    column: EcoString,
    bare: Option<ColumnRange>,
    casts: Vec<(CastTarget, ColumnRange)>,
}

/// A table's constraints reduced once to per-column ranges, as subsumption,
/// the constraint index and read-after-write consume them. One buffer
/// sorted by column, shared on clone: the update query, its subsumer region
/// and the read facts hold the same ranges.
#[derive(Debug, Clone, Default)]
pub struct TableRanges {
    columns: EcoVec<ColumnRanges>,
}

/// Constraints a build groups inline before spilling to the heap.
const INLINE_CONSTRAINTS: usize = 8;

impl TableRanges {
    /// The ranges of `constraints`, AND-connected per column and cast.
    pub fn build(constraints: &[TableConstraint]) -> Self {
        let mut sorted: SmallVec<[&TableConstraint; INLINE_CONSTRAINTS]> =
            constraints.iter().collect();
        sorted.sort_unstable_by(|a, b| a.column.cmp(&b.column));
        let distinct = sorted
            .iter()
            .zip(sorted.iter().skip(1))
            .filter(|(a, b)| a.column != b.column)
            .count()
            + usize::from(!sorted.is_empty());
        let mut columns = EcoVec::with_capacity(distinct);
        let mut rest: &[&TableConstraint] = &sorted;
        while let Some(first) = rest.first() {
            let run_len = rest
                .iter()
                .take_while(|constraint| constraint.column == first.column)
                .count();
            let (run, tail) = rest.split_at(run_len);
            columns.push(Self::column_ranges(run));
            rest = tail;
        }
        Self { columns }
    }

    /// One column's ranges from its constraints: the bare ones reduced
    /// together, then each cast's.
    fn column_ranges(run: &[&TableConstraint]) -> ColumnRanges {
        let column = run
            .first()
            .map_or_else(EcoString::new, |c| c.column.clone());
        let bare: SmallVec<[&TableConstraint; INLINE_CONSTRAINTS]> = run
            .iter()
            .copied()
            .filter(|constraint| constraint.kind.cast_target().is_none())
            .collect();
        let bare = (!bare.is_empty()).then(|| column_range_build(&bare));
        let mut casts: Vec<(CastTarget, ColumnRange)> = Vec::new();
        for constraint in run {
            let Some(cast) = constraint.kind.cast_target() else {
                continue;
            };
            if casts.iter().any(|(seen, _)| seen == cast) {
                continue;
            }
            let group: SmallVec<[&TableConstraint; INLINE_CONSTRAINTS]> = run
                .iter()
                .copied()
                .filter(|c| c.kind.cast_target() == Some(cast))
                .collect();
            casts.push((cast.clone(), column_range_build(&group)));
        }
        ColumnRanges {
            column,
            bare,
            casts,
        }
    }

    /// Bare ranges by column, for tests that state a reference's ranges.
    #[cfg(test)]
    pub(crate) fn from_bare(ranges: impl IntoIterator<Item = (EcoString, ColumnRange)>) -> Self {
        let mut columns: Vec<ColumnRanges> = ranges
            .into_iter()
            .map(|(column, range)| ColumnRanges {
                column,
                bare: Some(range),
                casts: Vec::new(),
            })
            .collect();
        columns.sort_by(|a, b| a.column.cmp(&b.column));
        Self {
            columns: columns.into_iter().collect(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.columns.is_empty()
    }

    fn column(&self, column: &str) -> Option<&ColumnRanges> {
        self.columns
            .binary_search_by(|entry| entry.column.as_str().cmp(column))
            .ok()
            .and_then(|index| self.columns.get(index))
    }

    /// Every column some constraint names, in column order.
    pub fn columns(&self) -> impl Iterator<Item = &EcoString> {
        self.columns.iter().map(|entry| &entry.column)
    }

    /// Every column whose bare range is a finite set of values, with the set.
    pub(crate) fn set_columns(&self) -> impl Iterator<Item = (&EcoString, &HashSet<LiteralValue>)> {
        self.columns.iter().filter_map(|entry| match &entry.bare {
            Some(ColumnRange::InSet(values)) => Some((&entry.column, values)),
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
        match self.column(column) {
            None => ColumnRange::Unconstrained,
            Some(entry) if !entry.casts.is_empty() => ColumnRange::Unknown,
            Some(entry) => entry.bare.clone().unwrap_or(ColumnRange::Unconstrained),
        }
    }

    /// The bare range on `column`, for read-after-write disjointness: a cast
    /// comparison constrains a derived value, not the raw column a written
    /// row supplies, so it can't prove disjointness and is left out.
    #[cfg(feature = "proxy")]
    pub(crate) fn bare_range(&self, column: &str) -> Option<&ColumnRange> {
        self.column(column)?.bare.as_ref()
    }

    /// Every column's bare range, in column order ([`Self::bare_range`]).
    #[cfg(feature = "proxy")]
    pub(crate) fn bare_ranges(&self) -> impl Iterator<Item = (&EcoString, &ColumnRange)> {
        self.columns
            .iter()
            .filter_map(|entry| Some((&entry.column, entry.bare.as_ref()?)))
    }
}

/// Whether a parent region `cached` holds every row of a reference whose
/// ranges are `new`: each range `cached` has must contain `new`'s on the
/// same column and cast, an absent one being unconstrained. An empty
/// `cached` is the whole table.
pub fn table_ranges_subsumed(new: &TableRanges, cached: &TableRanges) -> bool {
    cached.columns.iter().all(|cached_column| {
        let new_column = new.column(&cached_column.column);
        let bare_covered = cached_column.bare.as_ref().is_none_or(|cached_range| {
            let new_range = new_column.and_then(|column| column.bare.as_ref());
            column_range_subsumes(
                cached_range,
                new_range.unwrap_or(&ColumnRange::Unconstrained),
            )
        });
        bare_covered
            && cached_column.casts.iter().all(|(cast, cached_range)| {
                let new_range = new_column
                    .and_then(|column| column.casts.iter().find(|(new_cast, _)| new_cast == cast))
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
    fn test_table_ranges_bare_ranges_drop_casts() {
        let ranges = TableRanges::build(&[
            comparison("id", BinaryOp::Equal, 5),
            cast_comparison("name", BinaryOp::Equal, 42),
            comparison("x", BinaryOp::LessThan, 10),
        ]);
        let bare: Vec<&str> = ranges
            .bare_ranges()
            .map(|(column, _)| column.as_str())
            .collect();
        assert_eq!(bare, ["id", "x"]);
        assert!(matches!(
            ranges.bare_range("id"),
            Some(ColumnRange::Equal(LiteralValue::Integer(5)))
        ));
        assert!(ranges.bare_range("x").is_some() && ranges.bare_range("name").is_none());
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
