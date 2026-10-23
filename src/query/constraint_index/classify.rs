//! Partition a table's ranges into its subsumption class: which columns it
//! constrains, and whether it is equality-pure (hash-indexable by value tuple)
//! or complex (routed to the containment structures).

use super::ColumnSet;
use super::value_key::ValueKey;
use crate::query::constraints::{ColumnRange, TableRanges};

pub(super) enum Classification {
    EqualityPure {
        columns: ColumnSet,
        values: Vec<ValueKey>,
    },
    Complex {
        columns: ColumnSet,
    },
}

impl Classification {
    pub(super) fn into_columns(self) -> ColumnSet {
        match self {
            Classification::EqualityPure { columns, .. } | Classification::Complex { columns } => {
                columns
            }
        }
    }
}

/// Classify a query's table ranges. Equality-pure iff every constrained
/// column reduces to one keyable value in the bare domain; a column under a
/// cast reads as `Unknown` and a value that can't form a `ValueKey` (an
/// `Equal(Null)`) can't sit in the equality hash bucket, so either falls
/// back to Complex for the opaque/range path.
pub(super) fn classify(ranges: &TableRanges) -> Classification {
    let columns = ColumnSet::new(ranges.columns().cloned().collect());
    let values: Option<Vec<ValueKey>> = columns
        .columns()
        .iter()
        .map(|column| match ranges.index_range(column) {
            ColumnRange::Equal(value) => ValueKey::try_new(&value),
            ColumnRange::Unknown
            | ColumnRange::Unconstrained
            | ColumnRange::Empty
            | ColumnRange::Null
            | ColumnRange::InSet(_)
            | ColumnRange::Range { .. } => None,
        })
        .collect();
    match values {
        Some(values) => Classification::EqualityPure { columns, values },
        None => Classification::Complex { columns },
    }
}

/// Enumerate all subsets of a column set, each as a sorted `ColumnSet`.
/// 2^|set| subsets, yielded lazily. Callers only take this branch when
/// 2^|set| is no larger than the class count (PGC-410), which keeps the
/// `u32` mask in range.
pub(super) fn column_set_powerset(set: &ColumnSet) -> impl Iterator<Item = ColumnSet> + '_ {
    let cols = set.columns();
    let n = cols.len();
    debug_assert!(
        n < u32::BITS as usize,
        "powerset branch taken for {n} columns"
    );
    (0u32..(1u32 << n)).map(move |mask| {
        let mut subset = Vec::with_capacity(mask.count_ones() as usize);
        for (i, col) in cols.iter().enumerate() {
            if mask & (1 << i) != 0 {
                subset.push(col.clone());
            }
        }
        // `cols` is sorted, so the subset stays sorted by construction.
        ColumnSet(subset)
    })
}

// ============================================================================
// V1 within-class complex index (PGC-129)
// ============================================================================

/// The range of each class column, in column order, as the index reasons
/// about it (see [`TableRanges::index_range`]).
pub(super) fn column_ranges(ranges: &TableRanges, columns: &ColumnSet) -> Vec<ColumnRange> {
    columns
        .columns()
        .iter()
        .map(|column| ranges.index_range(column))
        .collect()
}
