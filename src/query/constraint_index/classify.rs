//! Partition a constraint set into its subsumption class: which columns it
//! constrains, and whether it is equality-pure (hash-indexable by value tuple)
//! or complex (routed to the containment structures).

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use super::ColumnSet;
use super::value_key::ValueKey;
use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{
    ColumnConstraintKind, ColumnRange, TableConstraint, column_range_build,
};

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

/// Classify a query's table constraints. Equality-pure iff every constraint
/// is `Comparison(_, Equal, _)` and every constrained column has exactly one
/// such constraint with a consistent value.
pub(super) fn classify(constraints: &[TableConstraint]) -> Classification {
    let mut equality: HashMap<EcoString, LiteralValue> = HashMap::new();
    let mut all_columns: HashSet<EcoString> = HashSet::new();
    let mut complex = false;

    for tc in constraints {
        all_columns.insert(tc.column.clone());
        match &tc.kind {
            ColumnConstraintKind::Comparison {
                op: BinaryOp::Equal,
                value,
            } => match equality.get(&tc.column) {
                Some(prev) if prev == value => {}
                Some(_) => complex = true,
                None => {
                    equality.insert(tc.column.clone(), value.clone());
                }
            },
            // A cast comparison sits in a different value domain from bare
            // comparisons, so the equality-pure bucket can't index it by
            // `(column, value)` either; detailed subsumption handles the cast.
            ColumnConstraintKind::Comparison { .. }
            | ColumnConstraintKind::AnyOf(_)
            | ColumnConstraintKind::CastComparison { .. }
            | ColumnConstraintKind::NullTest(_) => complex = true,
        }
    }

    let columns = ColumnSet::new(all_columns.into_iter().collect());

    if !complex && equality.len() == columns.len() {
        // A value that can't form a `ValueKey` (e.g. an `Equal(Null)`) can't
        // sit in the equality hash bucket; fall back to Complex so the
        // opaque/range path handles it.
        let values: Option<Vec<ValueKey>> = columns
            .columns()
            .iter()
            .map(|c| equality.remove(c).as_ref().and_then(ValueKey::try_new))
            .collect();
        match values {
            Some(values) => Classification::EqualityPure { columns, values },
            None => Classification::Complex { columns },
        }
    } else {
        Classification::Complex { columns }
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

/// Build the per-column `ColumnRange` for each class column, in column
/// order, by reducing the constraints that name it. Reuses
/// `column_range_build` — the same reduction `table_constraints_subsumed`
/// runs — so the index and the precise check share one vocabulary.
///
/// A column carrying a cast comparison is reported as `Unknown`: the index
/// can't reason across cast domains, so it routes to the linear fallback and
/// the precise check handles the cast.
pub(super) fn column_ranges(
    constraints: &[TableConstraint],
    columns: &ColumnSet,
) -> Vec<ColumnRange> {
    columns
        .columns()
        .iter()
        .map(|col| {
            let mut bare: Vec<&TableConstraint> = Vec::new();
            let mut has_cast = false;
            for tc in constraints {
                if tc.column != *col {
                    continue;
                }
                if tc.kind.cast_target().is_some() {
                    has_cast = true;
                } else {
                    bare.push(tc);
                }
            }
            if has_cast {
                ColumnRange::Unknown
            } else {
                column_range_build(&bare)
            }
        })
        .collect()
}
