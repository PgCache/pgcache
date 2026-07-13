//! Sub-linear per-relation constraint-containment index.
//!
//! Indexes entries (keyed by an id type `K`) by their per-table constraints,
//! and answers "which entries' constraints could contain a given query's
//! constraints" sub-linearly. Subsumption candidate lookup is the first
//! consumer (replacing the linear scan over `UpdateQueries.queries` previously
//! done by `subsumption_check`); see PGC-119 for V0 and PGC-129 for V1.
//!
//! For each table, entries are partitioned by their constraint-column set
//! ([`classify`]). Within a class, equality-pure entries are hash-indexed by
//! the joint value tuple. Entries with any non-equality constraint go to a
//! `ComplexIndex` ([`column_index`]): one `ColumnIndex` per class column, each
//! partitioning entries by constraint shape. [`index`] carries the operations.
//!
//! Lookup is **lossy-safe**: missed containment opportunities just mean we
//! populate from origin instead of stamping existing rows.

use std::collections::{HashMap, HashSet};

use ecow::EcoString;
use smallvec::SmallVec;

use crate::id_hash::{BuildIdHasher, IdHashable};
use crate::query::constraints::ColumnRange;

mod classify;
mod column_index;
mod index;
mod row_forms;
#[cfg(test)]
mod tests;
mod value_key;

use index::{Membership, SubsumptionClass};
use value_key::ValueKey;

pub(crate) use row_forms::row_value_forms;

/// `HashMap` keyed by an id type with the passthrough identity hasher.
type IdMap<K, V> = HashMap<K, V, BuildIdHasher<K>>;
/// `HashSet` of an id type with the passthrough identity hasher.
type IdSet<K> = HashSet<K, BuildIdHasher<K>>;

/// Per-column candidate forms a CDC row value can take: the literal string plus
/// optional float/bool reinterpretations. A fixed array (one slot per form) so
/// the per-row point probe never heap-allocates on this axis (PGC-341), and so
/// adding a fourth reinterpretation is a compile error (`[_; 3]` can't hold it)
/// — forcing a deliberate decision about the capacity rather than a silent
/// heap spill. Empty slots are `None`; iterate with `.iter().flatten()`.
pub(crate) type ColumnForms = [Option<ColumnRange>; 3];

/// Per-column `ValueKey`s extracted from a column's `Equal` forms — one slot per
/// `ColumnForms` slot (a form is keyable or it isn't), same fixed-array rules.
type ColumnKeys = [Option<ValueKey>; 3];

/// Per-class collection: one `ColumnForms` per column in the class. Unlike the
/// per-column forms (always ≤3), the column count is data-dependent and
/// unbounded (wide composite predicates), so this is a `SmallVec` — inline for
/// the common 1–2 column case, with a correct heap spill for wider classes,
/// rather than a fixed array (PGC-341).
type ClassForms = SmallVec<[ColumnForms; 2]>;

/// Per-class collection of `ColumnKeys`, same shape and rationale as `ClassForms`.
type ClassKeys = SmallVec<[ColumnKeys; 2]>;

/// Sorted, deduplicated set of column names — canonical key for a
/// subsumption class. Two queries constraining the same columns hash to
/// the same `ColumnSet` regardless of source order.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ColumnSet(Vec<EcoString>);

impl ColumnSet {
    pub fn new(mut cols: Vec<EcoString>) -> Self {
        cols.sort();
        cols.dedup();
        Self(cols)
    }

    pub fn columns(&self) -> &[EcoString] {
        &self.0
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }
}

/// Sub-linear per-relation constraint-containment index.
#[derive(Debug)]
pub struct ConstraintIndex<K> {
    classes: HashMap<ColumnSet, SubsumptionClass<K>>,
    /// Reverse lookup so `remove(id)` doesn't need to re-classify the
    /// caller's constraints.
    membership: IdMap<K, Membership>,
}

impl<K: IdHashable + Copy> Default for ConstraintIndex<K> {
    fn default() -> Self {
        Self {
            classes: HashMap::new(),
            membership: IdMap::default(),
        }
    }
}
