//! Constraint subsumption: does a cached query's constraint set on a table
//! imply a new query's?
//!
//! The bridge between the two vocabularies — reduces each side's
//! [`TableConstraint`]s to a per-column [`ColumnRange`](super::range::ColumnRange)
//! and compares them in the value domain.

use std::collections::HashMap;

use crate::query::cast::CastTarget;

use super::range::{ColumnRange, column_range_build, column_range_subsumes};
use super::{QueryConstraints, TableConstraint};

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
fn constraints_group_by_column<'a>(
    constraints: &'a [TableConstraint],
) -> HashMap<ConstraintBucketKey<'a>, Vec<&'a TableConstraint>> {
    let mut grouped: HashMap<ConstraintBucketKey<'a>, Vec<&'a TableConstraint>> = HashMap::new();
    for tc in constraints {
        let key: ConstraintBucketKey<'a> = match tc {
            TableConstraint::Comparison(col, _, _) | TableConstraint::AnyOf(col, _) => {
                (col.as_str(), None)
            }
            TableConstraint::CastComparison(col, cast, _, _) => (col.as_str(), Some(cast)),
        };
        grouped.entry(key).or_default().push(tc);
    }
    grouped
}

/// Returns true if the cached query's constraints on `table` are implied
/// by the new query's constraints. Per-column range reduction: each column
/// cached constrains must have a new range that fits within the cached range.
///
/// When the cached query has no constraints on a table, it loaded all rows — subsumed.
/// When the cached query has constraints but the new query doesn't for that table,
/// the new query is broader — not subsumed.
pub fn table_constraints_subsumed(
    new: &QueryConstraints,
    cached: &QueryConstraints,
    table: &str,
) -> bool {
    // PGC-106: if the analyzer couldn't fully understand the cached
    // query's WHERE clause, an empty `table_constraints` doesn't mean
    // "full table scan" — it means "we don't know what the cache holds".
    // Refuse to subsume in that case so the new query falls through and
    // gets its own cache entry.
    if !cached.where_analysis_complete {
        return false;
    }

    let cached_for_table = cached.table_constraints.get(table);
    let new_for_table = new.table_constraints.get(table);

    match (cached_for_table, new_for_table) {
        // Cached has no constraints on this table → full scan, all rows loaded. Subsumed.
        (None, _) => true,
        // Cached has constraints but new doesn't → new is broader than cached.
        (Some(_), None) => false,
        // Both have constraints → per-column range subsumption.
        (Some(cached_cs), Some(new_cs)) => {
            let cached_by_col = constraints_group_by_column(cached_cs);
            let new_by_col = constraints_group_by_column(new_cs);

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
    }
}
