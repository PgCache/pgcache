//! Partition a constraint set into its subsumption class: which columns it
//! constrains, and whether it is equality-pure (hash-indexable by value tuple)
//! or complex (routed to the containment structures).

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{ColumnRange, TableConstraint, column_range_build};

use super::value_key::ValueKey;
use super::{ColumnKeys, ColumnSet};

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
        match tc {
            TableConstraint::Comparison(col, BinaryOp::Equal, val) => {
                all_columns.insert(col.clone());
                match equality.get(col) {
                    Some(prev) if prev == val => {}
                    Some(_) => complex = true,
                    None => {
                        equality.insert(col.clone(), val.clone());
                    }
                }
            }
            TableConstraint::Comparison(col, _, _) | TableConstraint::AnyOf(col, _) => {
                all_columns.insert(col.clone());
                complex = true;
            }
            // Cast constraints sit in a different value domain from bare
            // comparisons; the equality-pure fast bucket can't index them
            // by `(column, value)`. Mark as Complex so detailed subsumption
            // (`table_constraints_subsumed`) handles the cast logic.
            TableConstraint::CastComparison(col, _, _, _) => {
                all_columns.insert(col.clone());
                complex = true;
            }
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
            .map(|c| ValueKey::try_new(&equality.remove(c).expect("equality maps every column")))
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

/// Cartesian product of per-column key sets, for the point-probe equality
/// lookup. Empty input → one empty tuple (the unconstrained class). Each
/// column carries ≤3 forms and classes have few columns, so the product stays
/// tiny.
pub(super) fn value_key_product(key_sets: &[ColumnKeys]) -> Vec<Vec<ValueKey>> {
    let mut result: Vec<Vec<ValueKey>> = vec![Vec::new()];
    for ks in key_sets {
        let present = ks.iter().flatten().count();
        let mut next = Vec::with_capacity(result.len() * present);
        for prefix in &result {
            for k in ks.iter().flatten() {
                let mut tuple = prefix.clone();
                tuple.push(k.clone());
                next.push(tuple);
            }
        }
        result = next;
    }
    result
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
                let (tc_col, is_cast) = match tc {
                    TableConstraint::Comparison(c, _, _) | TableConstraint::AnyOf(c, _) => {
                        (c, false)
                    }
                    TableConstraint::CastComparison(c, _, _, _) => (c, true),
                };
                if tc_col != col {
                    continue;
                }
                if is_cast {
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
