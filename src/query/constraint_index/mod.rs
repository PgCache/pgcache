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

use crate::id_hash::{BuildIdHasher, IdHashable};

mod classify;
mod column_index;
mod index;
#[cfg(feature = "proxy")]
mod point;
#[cfg(test)]
mod tests;
mod value_key;

use index::{Membership, SubsumptionClass};

#[cfg(feature = "proxy")]
pub(crate) use point::row_value_forms;

/// `HashMap` keyed by an id type with the passthrough identity hasher.
type IdMap<K, V> = HashMap<K, V, BuildIdHasher<K>>;
/// `HashSet` of an id type with the passthrough identity hasher.
type IdSet<K> = HashSet<K, BuildIdHasher<K>>;

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

    /// Whether every column of `self` is in `other`. Both sides are sorted
    /// and deduplicated by construction, so this is a single merge walk.
    pub fn is_subset_of(&self, other: &ColumnSet) -> bool {
        let mut other_iter = other.0.iter();
        self.0
            .iter()
            .all(|col| other_iter.by_ref().any(|other_col| other_col == col))
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
