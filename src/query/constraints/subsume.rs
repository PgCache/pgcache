//! Constraint subsumption: does a cached query's constraint set on a table
//! imply a new query's?
//!
//! The bridge between the two vocabularies — reduces each side's
//! [`TableConstraint`]s to a per-column [`ColumnRange`](super::range::ColumnRange)
//! and compares them in the value domain.

use std::collections::HashMap;

use super::TableConstraint;
use super::range::{ColumnRange, column_range_build, column_range_subsumes};
use crate::query::cast::CastTarget;

// ============================================================================
// ColumnRange: per-column constraint reduction for subsumption
// ============================================================================

/// Bucket key for per-column range building. Bare-column constraints
/// (`Comparison`, `AnyOf`) and cast-column constraints (`CastComparison`)
/// live in separate buckets so subsumption compares within a consistent
/// value domain.
type ConstraintBucketKey<'a> = (&'a str, Option<&'a CastTarget>);

/// Group table constraints by (column name, optional cast) for per-bucket
/// range building.
pub(super) fn constraints_group_by_column<'a>(
    constraints: &'a [TableConstraint],
) -> HashMap<ConstraintBucketKey<'a>, Vec<&'a TableConstraint>> {
    let mut grouped: HashMap<ConstraintBucketKey<'a>, Vec<&'a TableConstraint>> = HashMap::new();
    for tc in constraints {
        let key: ConstraintBucketKey<'a> = match tc {
            TableConstraint::Comparison(col, _, _)
            | TableConstraint::AnyOf(col, _)
            | TableConstraint::NullTest(col, _) => (col.as_str(), None),
            TableConstraint::CastComparison(col, cast, _, _) => (col.as_str(), Some(cast)),
        };
        grouped.entry(key).or_default().push(tc);
    }
    grouped
}

/// Whether a parent region `cached` holds every row of a reference whose
/// filters are `new`: each column `cached` constrains must have a `new` range
/// inside the cached one. An empty `cached` is the whole table.
pub fn table_constraints_subsumed(new: &[TableConstraint], cached: &[TableConstraint]) -> bool {
    let cached_by_col = constraints_group_by_column(cached);
    let new_by_col = constraints_group_by_column(new);
    cached_by_col.iter().all(|(col, cached_col_cs)| {
        let cached_range = column_range_build(cached_col_cs.as_slice());
        let new_range = new_by_col
            .get(col)
            .map_or(ColumnRange::Unconstrained, |cs| {
                column_range_build(cs.as_slice())
            });
        column_range_subsumes(&cached_range, &new_range)
    })
}
