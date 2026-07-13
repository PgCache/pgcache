//! The [`ConstraintIndex`](super::ConstraintIndex) operations: insert, remove,
//! and the two candidate lookups (constraint-set and point probe).

use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::id_hash::IdHashable;
use crate::query::constraints::TableConstraint;

use super::classify::{
    Classification, classify, column_ranges, column_set_powerset, project_values, value_key_product,
};
use super::column_index::ComplexIndex;
use super::value_key::ValueKey;
use super::{ClassForms, ClassKeys, ColumnForms, ColumnSet, ConstraintIndex, IdSet};
use crate::query::constraints::ColumnRange;

#[derive(Debug)]
pub(super) struct SubsumptionClass<K> {
    /// Entries whose constraints on every class column are pure `Equal(v)`.
    /// Keyed by joint value tuple in class-column order.
    equality: HashMap<Vec<ValueKey>, Vec<K>>,
    /// Entries with at least one non-equality constraint on any class
    /// column. Indexed per-column for sub-linear candidate lookup (PGC-129).
    complex: ComplexIndex<K>,
}

impl<K: IdHashable + Copy> SubsumptionClass<K> {
    fn new(num_columns: usize) -> Self {
        Self {
            equality: HashMap::new(),
            complex: ComplexIndex::new(num_columns),
        }
    }
}

#[derive(Debug)]
pub(super) struct Membership {
    columns: ColumnSet,
    payload: MembershipPayload,
}

#[derive(Debug)]
enum MembershipPayload {
    Equality(Vec<ValueKey>),
    /// Per-column ranges, in class-column order — lets `remove` locate the
    /// fingerprint in each `ColumnIndex` without re-classifying constraints.
    Complex(Vec<ColumnRange>),
}

impl<K: IdHashable + Copy> ConstraintIndex<K> {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index a query's constraints on a single table.
    ///
    /// Caller is responsible for skipping queries that should never be
    /// considered for subsumption (e.g. `has_limit=true`, multi-table
    /// parents). Those should not reach this method. Idempotent on the same
    /// `fingerprint`: previous membership is removed before re-indexing.
    pub fn insert(&mut self, fingerprint: K, table_constraints: &[TableConstraint]) {
        if self.membership.contains_key(&fingerprint) {
            self.remove(fingerprint);
        }
        match classify(table_constraints) {
            Classification::EqualityPure { columns, values } => {
                let bucket = self
                    .classes
                    .entry(columns.clone())
                    .or_insert_with(|| SubsumptionClass::new(columns.len()));
                bucket
                    .equality
                    .entry(values.clone())
                    .or_default()
                    .push(fingerprint);
                self.membership.insert(
                    fingerprint,
                    Membership {
                        columns,
                        payload: MembershipPayload::Equality(values),
                    },
                );
            }
            Classification::Complex { columns } => {
                let ranges = column_ranges(table_constraints, &columns);
                let bucket = self
                    .classes
                    .entry(columns.clone())
                    .or_insert_with(|| SubsumptionClass::new(columns.len()));
                bucket.complex.insert(fingerprint, &ranges);
                self.membership.insert(
                    fingerprint,
                    Membership {
                        columns,
                        payload: MembershipPayload::Complex(ranges),
                    },
                );
            }
        }
    }

    /// Remove a query's entry. O(1) plus the per-bucket vector retain.
    /// Removal is a cold path (failure cleanup, eviction).
    pub fn remove(&mut self, fingerprint: K) {
        let Some(membership) = self.membership.remove(&fingerprint) else {
            return;
        };
        let Some(bucket) = self.classes.get_mut(&membership.columns) else {
            return;
        };
        match membership.payload {
            MembershipPayload::Equality(values) => {
                if let Some(fps) = bucket.equality.get_mut(&values) {
                    fps.retain(|fp| *fp != fingerprint);
                    if fps.is_empty() {
                        bucket.equality.remove(&values);
                    }
                }
            }
            MembershipPayload::Complex(shapes) => {
                bucket.complex.remove(fingerprint, &shapes);
            }
        }
        if bucket.equality.is_empty() && bucket.complex.is_empty() {
            self.classes.remove(&membership.columns);
        }
    }

    /// Collect candidate parent fingerprints whose constraints might subsume
    /// the new query's constraints on this table. The caller runs the
    /// existing detailed `table_constraints_subsumed` check on each.
    ///
    /// Returns parents whose constraint-column set is a subset of new's.
    /// Equality-pure parents on a matching value tuple are short-circuited
    /// via hash lookup; complex-bucket parents are filtered per-column via
    /// `ComplexIndex` (PGC-129/189). Lossy-safe: may over-return, never
    /// under-returns a true subsumer.
    pub fn candidates(&self, new_constraints: &[TableConstraint]) -> IdSet<K> {
        let mut candidates = IdSet::default();
        let new_class = classify(new_constraints);
        let (new_columns, new_values_opt) = match &new_class {
            Classification::EqualityPure { columns, values } => (columns, Some(values)),
            Classification::Complex { columns } => (columns, None),
        };

        for subset in column_set_powerset(new_columns) {
            let Some(bucket) = self.classes.get(&subset) else {
                continue;
            };
            // Equality probe: when new is equality-pure on `subset`, parents
            // with exactly-matching values are candidates. Independently, the
            // empty subset always probes the empty-tuple key — that bucket
            // holds truly unconstrained parents, which subsume any new query
            // regardless of new's shape.
            let probe_values = if subset.columns().is_empty() {
                Some(Vec::new())
            } else {
                new_values_opt.and_then(|nv| project_values(new_columns, nv, &subset))
            };
            if let Some(values) = probe_values
                && let Some(fps) = bucket.equality.get(&values)
            {
                candidates.extend(fps);
            }
            // Complex probe: per-column containment lookup over the subset's
            // columns. `new` constrains every column of `subset` (subset is a
            // subset of new's columns), so the ranges are fully populated.
            let ranges = column_ranges(new_constraints, &subset);
            candidates.extend(bucket.complex.candidates(&ranges));
        }
        candidates
    }

    /// Candidate entries whose constraints a single row satisfies. Unlike
    /// [`candidates`](Self::candidates) — a region probe over a *query's*
    /// constraints — this is a point probe: the row is an `Equal`-on-every-
    /// column degenerate query, enumerated over the existing classes rather
    /// than the powerset of a (potentially wide) row.
    ///
    /// `col_forms` supplies, for a column, the row value's keyable
    /// interpretations as `Equal` forms (typically [`row_value_forms`]): a
    /// numeric wire value yields both its `String` and `Float` forms, since an
    /// entry on that column may be keyed under either (`val = 42` vs the
    /// identity-`::text`-stripped `val = '42'`). All forms are probed and
    /// unioned. `[ColumnRange::Unknown]` (SQL NULL, unchanged-TOAST, absent)
    /// is a wildcard that matches every entry constraining the column, so this
    /// **never under-returns** — load-bearing for the CDC/memo consumers,
    /// where a miss is a stale read, not just a lost optimization.
    /// Returning convenience wrapper over [`candidates_point_into`] — production
    /// CDC paths use the `_into` form to reuse a scratch set (PGC-341/344).
    #[cfg(test)]
    pub(crate) fn candidates_point<F>(&self, col_forms_fn: F) -> IdSet<K>
    where
        F: Fn(&str) -> ColumnForms,
    {
        let mut candidates = IdSet::default();
        self.candidates_point_into(col_forms_fn, &mut candidates);
        candidates
    }

    /// Like [`candidates_point`], but fills a caller-provided set (cleared first)
    /// instead of allocating a fresh one — lets the CDC hot path reuse a scratch
    /// set, retaining its (possibly large) capacity across probes (PGC-341/344).
    pub(crate) fn candidates_point_into<F>(&self, col_forms_fn: F, candidates: &mut IdSet<K>)
    where
        F: Fn(&str) -> ColumnForms,
    {
        candidates.clear();
        for (column_set, class) in &self.classes {
            let col_forms: ClassForms = column_set
                .columns()
                .iter()
                .map(|c| col_forms_fn(c.as_str()))
                .collect();
            // Equality-pure entries (in `class.equality`) are reachable only
            // through this bucket. Per column, collect the `ValueKey`s of its
            // `Equal` forms; an empty set (Unknown / non-keyable) is a wildcard
            // for that position. All columns keyed → probe the small cartesian
            // product of joint tuples; any wildcard → scan the bucket, matching
            // non-wildcard positions against their key sets.
            let key_sets: ClassKeys = col_forms
                .iter()
                .map(|forms| {
                    forms.each_ref().map(|slot| {
                        slot.as_ref().and_then(|r| match r {
                            ColumnRange::Equal(v) => ValueKey::try_new(v),
                            ColumnRange::Unknown
                            | ColumnRange::Unconstrained
                            | ColumnRange::Empty
                            | ColumnRange::InSet(_)
                            | ColumnRange::Range { .. } => None,
                        })
                    })
                })
                .collect();
            if key_sets.iter().all(|ks| ks.iter().any(Option::is_some)) {
                for tuple in value_key_product(&key_sets) {
                    if let Some(fps) = class.equality.get(&tuple) {
                        candidates.extend(fps);
                    }
                }
            } else {
                for (tuple, fps) in &class.equality {
                    let matches = key_sets.iter().zip(tuple).all(|(ks, t)| {
                        // No keyable form for this column → wildcard; else the
                        // tuple value must match one of the column's keys.
                        ks.iter().all(Option::is_none) || ks.iter().flatten().any(|k| k == t)
                    });
                    if matches {
                        candidates.extend(fps);
                    }
                }
            }
            candidates.extend(class.complex.candidates_point(&col_forms));
        }
    }

    /// Union of the columns any class consults — the columns a recovered old
    /// image must carry for `candidates_point_into` to probe at full precision
    /// (PGC-255). The unconstrained class contributes nothing; when this is
    /// empty, every entry is value-independent and old-image recovery buys
    /// nothing.
    pub fn columns(&self) -> impl Iterator<Item = &EcoString> {
        let mut seen: HashSet<&EcoString> = HashSet::new();
        self.classes
            .keys()
            .flat_map(ColumnSet::columns)
            .filter(move |c| seen.insert(c))
    }

    /// Number of column-set classes across all entries. Useful for metrics
    /// and for sanity-checking the partitioning fan-out.
    pub fn classes_len(&self) -> usize {
        self.classes.len()
    }

    /// Total fingerprints across all complex buckets (PGC-129 per-column
    /// index). Pair with `complex_fallback_total` to gauge how many are
    /// handled precisely vs. via the linear `opaque` fallback.
    pub fn complex_total(&self) -> usize {
        self.classes.values().map(|c| c.complex.len()).sum()
    }

    /// Fingerprints sitting in the per-column linear `opaque` fallback,
    /// summed across every class and column. A high ratio against
    /// `complex_total` flags column-set classes where the structured
    /// buckets aren't pulling their weight — the trigger to consider V2.
    pub fn complex_fallback_total(&self) -> usize {
        self.classes
            .values()
            .map(|c| c.complex.fallback_total())
            .sum()
    }
}
