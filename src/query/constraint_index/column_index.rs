//! The complex (non-equality) containment structures: one [`ColumnIndex`] per
//! class column, partitioned by constraint shape, and the [`ComplexIndex`] that
//! intersects their per-column match sets.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::id_hash::IdHashable;
use crate::query::ast::LiteralValue;
use crate::query::constraints::ColumnRange;

use super::value_key::{Placement, ValueKey, placement};
use super::{ColumnForms, IdSet};

/// Intersect per-column match sets, smallest first so the accumulator shrinks
/// as fast as possible, short-circuiting once it empties.
fn intersect_smallest_first<K: IdHashable + Copy>(mut per_column: Vec<Vec<K>>) -> Vec<K> {
    per_column.sort_by_key(|fps| fps.len());
    let mut iter = per_column.into_iter();
    let Some(first) = iter.next() else {
        return Vec::new();
    };
    let mut acc: IdSet<K> = first.into_iter().collect();
    for column in iter {
        if acc.is_empty() {
            break;
        }
        let other: IdSet<K> = column.into_iter().collect();
        acc.retain(|fp| other.contains(fp));
    }
    acc.into_iter().collect()
}

/// Per-class index over complex (non-equality) parents. Holds one
/// `ColumnIndex` per class column; `candidates` intersects their per-column
/// match sets.
#[derive(Debug)]
pub(super) struct ComplexIndex<K> {
    /// Parallel to the class's sorted column set.
    per_column: Vec<ColumnIndex<K>>,
    /// Distinct fingerprints indexed — each appears once per column.
    len: usize,
}

impl<K: IdHashable + Copy> ComplexIndex<K> {
    pub(super) fn new(num_columns: usize) -> Self {
        Self {
            per_column: (0..num_columns).map(|_| ColumnIndex::default()).collect(),
            len: 0,
        }
    }

    pub(super) fn len(&self) -> usize {
        self.len
    }

    pub(super) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Total fingerprints in the linear `opaque` fallback, summed over
    /// columns. Approaching `len * num_columns` signals that V2 (a real
    /// 2-D containment structure) is warranted for this class.
    pub(super) fn fallback_total(&self) -> usize {
        self.per_column.iter().map(|c| c.opaque.len()).sum()
    }

    pub(super) fn insert(&mut self, fingerprint: K, ranges: &[ColumnRange]) {
        for (column, range) in self.per_column.iter_mut().zip(ranges) {
            column.insert(fingerprint, range);
        }
        self.len += 1;
    }

    pub(super) fn remove(&mut self, fingerprint: K, ranges: &[ColumnRange]) {
        for (column, range) in self.per_column.iter_mut().zip(ranges) {
            column.remove(fingerprint, range);
        }
        self.len = self.len.saturating_sub(1);
    }

    /// Candidate parents whose constraint on every column could subsume the
    /// query's. Intersects the per-column match sets, smallest first.
    pub(super) fn candidates(&self, query_ranges: &[ColumnRange]) -> Vec<K> {
        if self.len == 0 {
            return Vec::new();
        }
        match (self.per_column.as_slice(), query_ranges) {
            ([], _) => Vec::new(),
            // Single-column class: the per-column match set is the answer
            // outright — no cross-column intersection, no `HashSet` pass.
            // The caller dedups into its own set.
            ([column], [range]) => column.containing(range),
            (columns, ranges) => {
                let per_column: Vec<Vec<K>> = columns
                    .iter()
                    .zip(ranges)
                    .map(|(column, range)| column.containing(range))
                    .collect();
                intersect_smallest_first(per_column)
            }
        }
    }

    /// Point-probe variant: each column supplies a list of `ColumnRange` forms
    /// (the row value's keyable interpretations). Union `containing` over a
    /// column's forms, then intersect across columns — mirrors `candidates`'
    /// smallest-first intersection. A `[Unknown]` column unions to every entry
    /// on that column (wildcard, no filtering).
    pub(super) fn candidates_point(&self, col_forms: &[ColumnForms]) -> Vec<K> {
        if self.len == 0 {
            return Vec::new();
        }
        match self.per_column.as_slice() {
            [] => Vec::new(),
            // Single-column class: concatenate the per-form match sets — the
            // caller dedups into its own set.
            [column] => col_forms
                .first()
                .into_iter()
                .flat_map(|forms| forms.iter().flatten())
                .flat_map(|form| column.containing(form))
                .collect(),
            columns => {
                let per_column: Vec<Vec<K>> = columns
                    .iter()
                    .zip(col_forms)
                    .map(|(column, forms)| {
                        let mut set: IdSet<K> = IdSet::default();
                        for form in forms.iter().flatten() {
                            set.extend(column.containing(form));
                        }
                        set.into_iter().collect::<Vec<K>>()
                    })
                    .collect();
                intersect_smallest_first(per_column)
            }
        }
    }
}

/// Index over one class column's complex parents, partitioned by constraint
/// shape so containment lookups avoid scanning the whole class.
#[derive(Debug)]
struct ColumnIndex<K> {
    /// `column = v` parents, keyed by value.
    eq: HashMap<ValueKey, Vec<K>>,
    /// `column IN (...)` parents — inverted: each member value maps to the
    /// parents whose set contains it.
    inset: HashMap<ValueKey, Vec<K>>,
    /// `column > v` / `>= v` parents, keyed by the lower bound.
    range_lower: BTreeMap<ValueKey, Vec<K>>,
    /// `column < v` / `<= v` parents, keyed by the upper bound.
    range_upper: BTreeMap<ValueKey, Vec<K>>,
    /// Two-sided range parents `(l, u)` (PGC-189), keyed by the lower bound
    /// with the upper bound inline so the lookup can filter during the walk
    /// — no intersection materialization.
    range_both: BTreeMap<ValueKey, Vec<(ValueKey, K)>>,
    /// Linear fallback for shapes the structured buckets can't place.
    opaque: Vec<K>,
}

// Hand-written so the bound is `K: ` nothing, not the `K: Default` a derive
// would demand — no field needs `K: Default`.
impl<K> Default for ColumnIndex<K> {
    fn default() -> Self {
        Self {
            eq: HashMap::new(),
            inset: HashMap::new(),
            range_lower: BTreeMap::new(),
            range_upper: BTreeMap::new(),
            range_both: BTreeMap::new(),
            opaque: Vec::new(),
        }
    }
}

impl<K: IdHashable + Copy> ColumnIndex<K> {
    fn insert(&mut self, fingerprint: K, range: &ColumnRange) {
        match placement(range) {
            Placement::Eq(key) => self.eq.entry(key).or_default().push(fingerprint),
            Placement::InSet(set) => {
                for v in set {
                    let key = ValueKey::try_new(v).expect("InSet members are keyable");
                    self.inset.entry(key).or_default().push(fingerprint);
                }
            }
            Placement::RangeLower(key) => {
                self.range_lower.entry(key).or_default().push(fingerprint);
            }
            Placement::RangeUpper(key) => {
                self.range_upper.entry(key).or_default().push(fingerprint);
            }
            Placement::RangeBoth { lower, upper } => {
                self.range_both
                    .entry(lower)
                    .or_default()
                    .push((upper, fingerprint));
            }
            Placement::Opaque => self.opaque.push(fingerprint),
        }
    }

    fn remove(&mut self, fingerprint: K, range: &ColumnRange) {
        match placement(range) {
            Placement::Eq(key) => map_vec_remove(&mut self.eq, &key, fingerprint),
            Placement::InSet(set) => {
                for v in set {
                    let key = ValueKey::try_new(v).expect("InSet members are keyable");
                    map_vec_remove(&mut self.inset, &key, fingerprint);
                }
            }
            Placement::RangeLower(key) => {
                btree_vec_remove(&mut self.range_lower, &key, fingerprint);
            }
            Placement::RangeUpper(key) => {
                btree_vec_remove(&mut self.range_upper, &key, fingerprint);
            }
            Placement::RangeBoth { lower, .. } => {
                btree_pair_remove(&mut self.range_both, &lower, fingerprint);
            }
            Placement::Opaque => self.opaque.retain(|fp| *fp != fingerprint),
        }
    }

    /// Parents on this column whose range could subsume `query`'s range on
    /// the same column. May over-return (lossy-safe); the caller's precise
    /// `table_constraints_subsumed` rejects false candidates.
    fn containing(&self, query: &ColumnRange) -> Vec<K> {
        let mut out: Vec<K> = self.opaque.clone();
        match query {
            // Can't reason (`Unknown`), query covers nothing (`Empty` —
            // subsumed by all) or everything (`Unconstrained`): return the
            // whole column bucket.
            ColumnRange::Unknown | ColumnRange::Empty | ColumnRange::Unconstrained => {
                self.extend_all(&mut out);
            }
            ColumnRange::Equal(v) => {
                if let Some(key) = ValueKey::try_new(v) {
                    if let Some(fps) = self.eq.get(&key) {
                        out.extend(fps);
                    }
                    if let Some(fps) = self.inset.get(&key) {
                        out.extend(fps);
                    }
                    // Range parents whose interval contains the point `v`.
                    self.extend_lower_covering(v, &mut out);
                    self.extend_upper_covering(v, &mut out);
                    self.extend_two_sided_lit(v, v, &mut out);
                } else {
                    // Non-keyable point (`Null` etc.) — can't reason; return
                    // the whole column bucket conservatively.
                    self.extend_all(&mut out);
                }
            }
            ColumnRange::InSet(set) => self.containing_inset(set, &mut out),
            ColumnRange::Range { lower, upper, .. } => {
                // A `range_lower` parent `(l, +inf)` covers the query only if
                // `l` is at or below the query's lower bound; a query
                // unbounded below admits no finite-`l` parent. Symmetric for
                // `range_upper` and the upper bound.
                if let Some(lb) = lower {
                    self.extend_lower_covering(&lb.value, &mut out);
                }
                if let Some(ub) = upper {
                    self.extend_upper_covering(&ub.value, &mut out);
                }
                // Two-sided parents only cover a query that is itself bounded
                // on both sides — a finite-upper parent cannot cover an
                // unbounded-above query, and symmetric for below.
                if let (Some(lb), Some(ub)) = (lower, upper) {
                    self.extend_two_sided_lit(&lb.value, &ub.value, &mut out);
                }
            }
        }
        out
    }

    /// All fingerprints stored in `range_both`, across every key.
    fn range_both_all(&self) -> impl Iterator<Item = K> + '_ {
        self.range_both
            .values()
            .flat_map(|v| v.iter().map(|(_, fp)| *fp))
    }

    /// Every fingerprint on this column, across all sub-indexes.
    fn extend_all(&self, out: &mut Vec<K>) {
        out.extend(self.eq.values().flatten());
        out.extend(self.inset.values().flatten());
        out.extend(self.range_lower.values().flatten());
        out.extend(self.range_upper.values().flatten());
        out.extend(self.range_both_all());
    }

    /// Two-sided-range parents whose interval covers `[qlo, qhi]`. Walks the
    /// `l <= qlo` prefix of `range_both`, inline-filtering each entry's
    /// stored upper bound against `qhi` — single-pass, no intersection
    /// materialization.
    ///
    /// The `range_both.is_empty()` early-out keeps V1 single-sided workloads
    /// from paying for V2's two-sided sub-index. Load-bearing for the V1
    /// midpoint bench.
    fn extend_two_sided(&self, qlo: &ValueKey, qhi: &ValueKey, out: &mut Vec<K>) {
        if self.range_both.is_empty() {
            return;
        }
        for (_, entries) in self.range_both.range(..=qlo.clone()) {
            for (upper, fp) in entries {
                if upper >= qhi {
                    out.push(*fp);
                }
            }
        }
    }

    /// `LiteralValue` entry point for `extend_two_sided`. Falls back to
    /// over-returning every two-sided parent if either bound isn't orderable.
    #[inline]
    fn extend_two_sided_lit(&self, qlo: &LiteralValue, qhi: &LiteralValue, out: &mut Vec<K>) {
        if self.range_both.is_empty() {
            return;
        }
        match (ValueKey::try_new(qlo), ValueKey::try_new(qhi)) {
            (Some(qlo_key), Some(qhi_key)) => self.extend_two_sided(&qlo_key, &qhi_key, out),
            _ => out.extend(self.range_both_all()),
        }
    }

    /// `range_lower` parents `(l, +inf)` with `l <= bound`. A non-orderable
    /// bound can't probe the `BTreeMap`, so return the whole bucket.
    fn extend_lower_covering(&self, bound: &LiteralValue, out: &mut Vec<K>) {
        match ValueKey::try_new(bound) {
            Some(key) => out.extend(self.range_lower.range(..=key).flat_map(|(_, f)| f)),
            None => out.extend(self.range_lower.values().flatten()),
        }
    }

    /// `range_upper` parents `(-inf, u)` with `u >= bound`.
    fn extend_upper_covering(&self, bound: &LiteralValue, out: &mut Vec<K>) {
        match ValueKey::try_new(bound) {
            Some(key) => out.extend(self.range_upper.range(key..).flat_map(|(_, f)| f)),
            None => out.extend(self.range_upper.values().flatten()),
        }
    }

    /// `InSet` query branch of `containing`: a parent subsumes it only if the
    /// parent's constraint covers every value in the set.
    fn containing_inset(&self, set: &HashSet<LiteralValue>, out: &mut Vec<K>) {
        // Any non-keyable member means we can't reason about the set
        // precisely — return the whole bucket conservatively.
        let Some(keys): Option<Vec<ValueKey>> = set.iter().map(ValueKey::try_new).collect() else {
            self.extend_all(out);
            return;
        };
        let mut iter = keys.iter();
        let Some(first) = iter.next() else {
            return;
        };
        // InSet parents: the parent's set must be a superset — intersect the
        // inverted-index lists over every query value.
        let mut members: IdSet<K> = self
            .inset
            .get(first)
            .map_or_else(IdSet::default, |fps| fps.iter().copied().collect());
        for v in iter {
            if members.is_empty() {
                break;
            }
            let present: IdSet<K> = self
                .inset
                .get(v)
                .map_or_else(IdSet::default, |fps| fps.iter().copied().collect());
            members.retain(|fp| present.contains(fp));
        }
        out.extend(members);
        // A single-value IN is an equality in disguise.
        if keys.len() == 1
            && let Some(fps) = self.eq.get(first)
        {
            out.extend(fps);
        }
        // Range parents must cover the closed interval [min, max] of the set.
        let min = keys.iter().min().expect("set is non-empty");
        let max = keys.iter().max().expect("set is non-empty");
        out.extend(self.range_lower.range(..=min.clone()).flat_map(|(_, f)| f));
        out.extend(self.range_upper.range(max.clone()..).flat_map(|(_, f)| f));
        self.extend_two_sided(min, max, out);
    }
}

/// Remove `fp` from a `HashMap`-backed posting list, dropping the key when
/// its list empties.
fn map_vec_remove<K: Copy + Eq>(map: &mut HashMap<ValueKey, Vec<K>>, key: &ValueKey, fp: K) {
    if let Some(fps) = map.get_mut(key) {
        fps.retain(|x| *x != fp);
        if fps.is_empty() {
            map.remove(key);
        }
    }
}

/// Remove `fp` from a `BTreeMap`-backed posting list, dropping the key when
/// its list empties.
fn btree_vec_remove<K: Copy + Eq>(map: &mut BTreeMap<ValueKey, Vec<K>>, key: &ValueKey, fp: K) {
    if let Some(fps) = map.get_mut(key) {
        fps.retain(|x| *x != fp);
        if fps.is_empty() {
            map.remove(key);
        }
    }
}

/// Remove `fp` from a `BTreeMap`-backed `(other_bound, fp)` posting list,
/// dropping the key when its list empties.
fn btree_pair_remove<K: Copy + Eq>(
    map: &mut BTreeMap<ValueKey, Vec<(ValueKey, K)>>,
    key: &ValueKey,
    fp: K,
) {
    if let Some(entries) = map.get_mut(key) {
        entries.retain(|(_, x)| *x != fp);
        if entries.is_empty() {
            map.remove(key);
        }
    }
}
