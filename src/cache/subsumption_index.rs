//! Sub-linear subsumption candidate lookup.
//!
//! Replaces the linear scan over `UpdateQueries.queries` previously done by
//! `subsumption_check`. See PGC-119 for V0 and PGC-129 for V1.
//!
//! For each table, queries are partitioned by their constraint-column set.
//! Within a class, equality-pure queries are hash-indexed by the joint
//! value tuple. Queries with any non-equality constraint (Range/IN/etc.) go
//! to a `ComplexIndex`: one `ColumnIndex` per class column, each
//! partitioning parents by constraint shape (`eq` / `inset` / `range_lower`
//! / `range_upper` / `range_both` / `opaque` fallback). `candidates` runs a
//! per-column containment lookup and intersects the results.
//!
//! Lookup is **lossy-safe**: missed subsumption opportunities just mean we
//! populate from origin instead of stamping existing rows.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{ColumnRange, TableConstraint, column_range_build};

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

/// Sub-linear subsumption candidate lookup.
#[derive(Debug, Default)]
pub struct SubsumptionIndex {
    classes: HashMap<ColumnSet, SubsumptionClass>,
    /// Reverse lookup so `remove(fingerprint)` doesn't need to re-classify
    /// the caller's constraints.
    membership: HashMap<u64, Membership>,
}

#[derive(Debug)]
struct SubsumptionClass {
    /// Fingerprints whose constraints on every class column are pure
    /// `Equal(v)`. Keyed by joint value tuple in class-column order.
    equality: HashMap<Vec<LiteralValue>, Vec<u64>>,
    /// Fingerprints with at least one non-equality constraint on any class
    /// column. Indexed per-column for sub-linear candidate lookup (PGC-129).
    complex: ComplexIndex,
}

impl SubsumptionClass {
    fn new(num_columns: usize) -> Self {
        Self {
            equality: HashMap::new(),
            complex: ComplexIndex::new(num_columns),
        }
    }
}

#[derive(Debug)]
struct Membership {
    columns: ColumnSet,
    payload: MembershipPayload,
}

#[derive(Debug)]
enum MembershipPayload {
    Equality(Vec<LiteralValue>),
    /// Per-column ranges, in class-column order — lets `remove` locate the
    /// fingerprint in each `ColumnIndex` without re-classifying constraints.
    Complex(Vec<ColumnRange>),
}

impl SubsumptionIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Index a query's constraints on a single table.
    ///
    /// Caller is responsible for skipping queries that should never be
    /// considered for subsumption (e.g. `has_limit=true`, multi-table
    /// parents). Those should not reach this method. Idempotent on the same
    /// `fingerprint`: previous membership is removed before re-indexing.
    pub fn insert(&mut self, fingerprint: u64, table_constraints: &[TableConstraint]) {
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
    pub fn remove(&mut self, fingerprint: u64) {
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
    pub fn candidates(&self, new_constraints: &[TableConstraint]) -> HashSet<u64> {
        let mut candidates = HashSet::new();
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

enum Classification {
    EqualityPure {
        columns: ColumnSet,
        values: Vec<LiteralValue>,
    },
    Complex {
        columns: ColumnSet,
    },
}

/// Classify a query's table constraints. Equality-pure iff every constraint
/// is `Comparison(_, Equal, _)` and every constrained column has exactly one
/// such constraint with a consistent value.
fn classify(constraints: &[TableConstraint]) -> Classification {
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
        let values: Vec<LiteralValue> = columns
            .columns()
            .iter()
            .map(|c| equality.remove(c).expect("equality maps every column"))
            .collect();
        Classification::EqualityPure { columns, values }
    } else {
        Classification::Complex { columns }
    }
}

/// Enumerate all subsets of a column set, each as a sorted `ColumnSet`.
/// Bounded by 2^|set| — typical |constraint_columns| ≤ 4 keeps this small.
fn column_set_powerset(set: &ColumnSet) -> Vec<ColumnSet> {
    let cols = set.columns();
    let n = cols.len();
    let mut subsets = Vec::with_capacity(1usize << n);
    for mask in 0u32..(1u32 << n) {
        let mut subset = Vec::with_capacity(mask.count_ones() as usize);
        for (i, col) in cols.iter().enumerate() {
            if mask & (1 << i) != 0 {
                subset.push(col.clone());
            }
        }
        // `cols` is sorted, so the subset stays sorted by construction.
        subsets.push(ColumnSet(subset));
    }
    subsets
}

/// Project a value tuple onto a subset of the original column set. Both
/// `full_columns` and `subset` are sorted; we walk in lockstep.
fn project_values(
    full_columns: &ColumnSet,
    full_values: &[LiteralValue],
    subset: &ColumnSet,
) -> Option<Vec<LiteralValue>> {
    let mut result = Vec::with_capacity(subset.len());
    let mut full_iter = full_columns.columns().iter().zip(full_values);
    for sub_col in subset.columns() {
        loop {
            let (col, val) = full_iter.next()?;
            if col == sub_col {
                result.push(val.clone());
                break;
            }
        }
    }
    Some(result)
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
fn column_ranges(constraints: &[TableConstraint], columns: &ColumnSet) -> Vec<ColumnRange> {
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

/// Which `ColumnIndex` sub-structure a column range belongs in.
enum Placement<'a> {
    Eq(&'a LiteralValue),
    InSet(&'a HashSet<LiteralValue>),
    RangeLower(LitKey),
    RangeUpper(LitKey),
    /// Two-sided range with orderable bounds (PGC-189).
    RangeBoth {
        lower: LitKey,
        upper: LitKey,
    },
    Opaque,
}

/// Classify a `ColumnRange` into its sub-index. Single-sided ranges with an
/// orderable bound get a structured bucket; two-sided ranges with orderable
/// bounds land in `range_both`; everything else — `Unknown`/`Empty`/
/// `Unconstrained`, non-orderable bounds — routes to the linear `opaque`
/// fallback.
fn placement(range: &ColumnRange) -> Placement<'_> {
    match range {
        ColumnRange::Equal(v) => Placement::Eq(v),
        ColumnRange::InSet(set) => Placement::InSet(set),
        ColumnRange::Range { lower, upper, .. } => match (lower, upper) {
            (Some(lb), None) => {
                LitKey::try_new(&lb.value).map_or(Placement::Opaque, Placement::RangeLower)
            }
            (None, Some(ub)) => {
                LitKey::try_new(&ub.value).map_or(Placement::Opaque, Placement::RangeUpper)
            }
            (Some(lb), Some(ub)) => {
                match (LitKey::try_new(&lb.value), LitKey::try_new(&ub.value)) {
                    (Some(lower), Some(upper)) => Placement::RangeBoth { lower, upper },
                    _ => Placement::Opaque,
                }
            }
            (None, None) => Placement::Opaque,
        },
        ColumnRange::Unknown | ColumnRange::Unconstrained | ColumnRange::Empty => Placement::Opaque,
    }
}

/// A `LiteralValue` wrapped with a total order, for use as a `BTreeMap` key.
/// Only constructible for orderable value kinds (`Integer`, `Float`,
/// `String`, `StringWithCast`). A column holds a single type, so all keys in
/// any one `BTreeMap` share a variant and compare meaningfully.
#[derive(Debug, Clone, PartialEq, Eq)]
struct LitKey(LiteralValue);

impl LitKey {
    fn try_new(v: &LiteralValue) -> Option<Self> {
        matches!(
            v,
            LiteralValue::Integer(_)
                | LiteralValue::Float(_)
                | LiteralValue::String(_)
                | LiteralValue::StringWithCast(..)
        )
        .then(|| Self(v.clone()))
    }
}

/// Stable discriminant for cross-type ordering — only exercised if a column
/// ever mixes types, which it should not.
fn lit_tag(v: &LiteralValue) -> u8 {
    match v {
        LiteralValue::Integer(_) => 0,
        LiteralValue::Float(_) => 1,
        LiteralValue::String(_) => 2,
        LiteralValue::StringWithCast(..) => 3,
        LiteralValue::Boolean(_)
        | LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_)
        | LiteralValue::Array(..) => 4,
    }
}

impl Ord for LitKey {
    fn cmp(&self, other: &Self) -> Ordering {
        match (&self.0, &other.0) {
            (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a.cmp(b),
            (LiteralValue::Float(a), LiteralValue::Float(b)) => a.cmp(b),
            (LiteralValue::String(a), LiteralValue::String(b)) => a.cmp(b),
            (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => a.cmp(b),
            _ => lit_tag(&self.0).cmp(&lit_tag(&other.0)),
        }
    }
}

impl PartialOrd for LitKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Per-class index over complex (non-equality) parents. Holds one
/// `ColumnIndex` per class column; `candidates` intersects their per-column
/// match sets.
#[derive(Debug)]
struct ComplexIndex {
    /// Parallel to the class's sorted column set.
    per_column: Vec<ColumnIndex>,
    /// Distinct fingerprints indexed — each appears once per column.
    len: usize,
}

impl ComplexIndex {
    fn new(num_columns: usize) -> Self {
        Self {
            per_column: (0..num_columns).map(|_| ColumnIndex::default()).collect(),
            len: 0,
        }
    }

    fn len(&self) -> usize {
        self.len
    }

    fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Total fingerprints in the linear `opaque` fallback, summed over
    /// columns. Approaching `len * num_columns` signals that V2 (a real
    /// 2-D containment structure) is warranted for this class.
    fn fallback_total(&self) -> usize {
        self.per_column.iter().map(|c| c.opaque.len()).sum()
    }

    fn insert(&mut self, fingerprint: u64, ranges: &[ColumnRange]) {
        for (column, range) in self.per_column.iter_mut().zip(ranges) {
            column.insert(fingerprint, range);
        }
        self.len += 1;
    }

    fn remove(&mut self, fingerprint: u64, ranges: &[ColumnRange]) {
        for (column, range) in self.per_column.iter_mut().zip(ranges) {
            column.remove(fingerprint, range);
        }
        self.len = self.len.saturating_sub(1);
    }

    /// Candidate parents whose constraint on every column could subsume the
    /// query's. Intersects the per-column match sets, smallest first.
    fn candidates(&self, query_ranges: &[ColumnRange]) -> Vec<u64> {
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
                let mut per_column: Vec<Vec<u64>> = columns
                    .iter()
                    .zip(ranges)
                    .map(|(column, range)| column.containing(range))
                    .collect();
                per_column.sort_by_key(|fps| fps.len());
                let mut iter = per_column.into_iter();
                let Some(first) = iter.next() else {
                    return Vec::new();
                };
                let mut acc: HashSet<u64> = first.into_iter().collect();
                for column in iter {
                    if acc.is_empty() {
                        break;
                    }
                    let other: HashSet<u64> = column.into_iter().collect();
                    acc.retain(|fp| other.contains(fp));
                }
                acc.into_iter().collect()
            }
        }
    }
}

/// Index over one class column's complex parents, partitioned by constraint
/// shape so containment lookups avoid scanning the whole class.
#[derive(Debug, Default)]
struct ColumnIndex {
    /// `column = v` parents, keyed by value.
    eq: HashMap<LiteralValue, Vec<u64>>,
    /// `column IN (...)` parents — inverted: each member value maps to the
    /// parents whose set contains it.
    inset: HashMap<LiteralValue, Vec<u64>>,
    /// `column > v` / `>= v` parents, keyed by the lower bound.
    range_lower: BTreeMap<LitKey, Vec<u64>>,
    /// `column < v` / `<= v` parents, keyed by the upper bound.
    range_upper: BTreeMap<LitKey, Vec<u64>>,
    /// Two-sided range parents `(l, u)` (PGC-189), keyed by the lower bound
    /// with the upper bound inline so the lookup can filter during the walk
    /// — no intersection materialization.
    range_both: BTreeMap<LitKey, Vec<(LitKey, u64)>>,
    /// Linear fallback for shapes the structured buckets can't place.
    opaque: Vec<u64>,
}

impl ColumnIndex {
    fn insert(&mut self, fingerprint: u64, range: &ColumnRange) {
        match placement(range) {
            Placement::Eq(v) => self.eq.entry(v.clone()).or_default().push(fingerprint),
            Placement::InSet(set) => {
                for v in set {
                    self.inset.entry(v.clone()).or_default().push(fingerprint);
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

    fn remove(&mut self, fingerprint: u64, range: &ColumnRange) {
        match placement(range) {
            Placement::Eq(v) => map_vec_remove(&mut self.eq, v, fingerprint),
            Placement::InSet(set) => {
                for v in set {
                    map_vec_remove(&mut self.inset, v, fingerprint);
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
    fn containing(&self, query: &ColumnRange) -> Vec<u64> {
        let mut out: Vec<u64> = self.opaque.clone();
        match query {
            // Can't reason (`Unknown`), query covers nothing (`Empty` —
            // subsumed by all) or everything (`Unconstrained`): return the
            // whole column bucket.
            ColumnRange::Unknown | ColumnRange::Empty | ColumnRange::Unconstrained => {
                self.extend_all(&mut out);
            }
            ColumnRange::Equal(v) => {
                if let Some(fps) = self.eq.get(v) {
                    out.extend(fps);
                }
                if let Some(fps) = self.inset.get(v) {
                    out.extend(fps);
                }
                // Range parents whose interval contains the point `v`.
                self.extend_lower_covering(v, &mut out);
                self.extend_upper_covering(v, &mut out);
                self.extend_two_sided_lit(v, v, &mut out);
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
    fn range_both_all(&self) -> impl Iterator<Item = u64> + '_ {
        self.range_both
            .values()
            .flat_map(|v| v.iter().map(|(_, fp)| *fp))
    }

    /// Every fingerprint on this column, across all sub-indexes.
    fn extend_all(&self, out: &mut Vec<u64>) {
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
    fn extend_two_sided(&self, qlo: &LitKey, qhi: &LitKey, out: &mut Vec<u64>) {
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
    fn extend_two_sided_lit(&self, qlo: &LiteralValue, qhi: &LiteralValue, out: &mut Vec<u64>) {
        if self.range_both.is_empty() {
            return;
        }
        match (LitKey::try_new(qlo), LitKey::try_new(qhi)) {
            (Some(qlo_key), Some(qhi_key)) => self.extend_two_sided(&qlo_key, &qhi_key, out),
            _ => out.extend(self.range_both_all()),
        }
    }

    /// `range_lower` parents `(l, +inf)` with `l <= bound`. A non-orderable
    /// bound can't probe the `BTreeMap`, so return the whole bucket.
    fn extend_lower_covering(&self, bound: &LiteralValue, out: &mut Vec<u64>) {
        match LitKey::try_new(bound) {
            Some(key) => out.extend(self.range_lower.range(..=key).flat_map(|(_, f)| f)),
            None => out.extend(self.range_lower.values().flatten()),
        }
    }

    /// `range_upper` parents `(-inf, u)` with `u >= bound`.
    fn extend_upper_covering(&self, bound: &LiteralValue, out: &mut Vec<u64>) {
        match LitKey::try_new(bound) {
            Some(key) => out.extend(self.range_upper.range(key..).flat_map(|(_, f)| f)),
            None => out.extend(self.range_upper.values().flatten()),
        }
    }

    /// `InSet` query branch of `containing`: a parent subsumes it only if the
    /// parent's constraint covers every value in the set.
    fn containing_inset(&self, set: &HashSet<LiteralValue>, out: &mut Vec<u64>) {
        let mut iter = set.iter();
        let Some(first) = iter.next() else {
            return;
        };
        // InSet parents: the parent's set must be a superset — intersect the
        // inverted-index lists over every query value.
        let mut members: HashSet<u64> = self
            .inset
            .get(first)
            .map_or_else(HashSet::new, |fps| fps.iter().copied().collect());
        for v in iter {
            if members.is_empty() {
                break;
            }
            let present: HashSet<u64> = self
                .inset
                .get(v)
                .map_or_else(HashSet::new, |fps| fps.iter().copied().collect());
            members.retain(|fp| present.contains(fp));
        }
        out.extend(members);
        // A single-value IN is an equality in disguise.
        if set.len() == 1
            && let Some(fps) = self.eq.get(first)
        {
            out.extend(fps);
        }
        // Range parents must cover the closed interval [min, max] of the set.
        let keys: Option<Vec<LitKey>> = set.iter().map(LitKey::try_new).collect();
        match keys {
            Some(keys) => {
                let min = keys.iter().min().expect("set is non-empty");
                let max = keys.iter().max().expect("set is non-empty");
                out.extend(self.range_lower.range(..=min.clone()).flat_map(|(_, f)| f));
                out.extend(self.range_upper.range(max.clone()..).flat_map(|(_, f)| f));
                self.extend_two_sided(min, max, out);
            }
            None => {
                out.extend(self.range_lower.values().flatten());
                out.extend(self.range_upper.values().flatten());
                out.extend(self.range_both_all());
            }
        }
    }
}

/// Remove `fp` from a `HashMap`-backed posting list, dropping the key when
/// its list empties.
fn map_vec_remove(map: &mut HashMap<LiteralValue, Vec<u64>>, key: &LiteralValue, fp: u64) {
    if let Some(fps) = map.get_mut(key) {
        fps.retain(|x| *x != fp);
        if fps.is_empty() {
            map.remove(key);
        }
    }
}

/// Remove `fp` from a `BTreeMap`-backed posting list, dropping the key when
/// its list empties.
fn btree_vec_remove(map: &mut BTreeMap<LitKey, Vec<u64>>, key: &LitKey, fp: u64) {
    if let Some(fps) = map.get_mut(key) {
        fps.retain(|x| *x != fp);
        if fps.is_empty() {
            map.remove(key);
        }
    }
}

/// Remove `fp` from a `BTreeMap`-backed `(other_bound, fp)` posting list,
/// dropping the key when its list empties.
fn btree_pair_remove(map: &mut BTreeMap<LitKey, Vec<(LitKey, u64)>>, key: &LitKey, fp: u64) {
    if let Some(entries) = map.get_mut(key) {
        entries.retain(|(_, x)| *x != fp);
        if entries.is_empty() {
            map.remove(key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::{BinaryOp, LiteralValue};
    use crate::query::cast::CastTarget;
    use ecow::EcoString;

    fn col(s: &str) -> EcoString {
        EcoString::from(s)
    }

    fn int(n: i64) -> LiteralValue {
        LiteralValue::Integer(n)
    }

    fn eq(c: &str, v: LiteralValue) -> TableConstraint {
        TableConstraint::Comparison(col(c), BinaryOp::Equal, v)
    }

    fn gt(c: &str, v: LiteralValue) -> TableConstraint {
        TableConstraint::Comparison(col(c), BinaryOp::GreaterThan, v)
    }

    fn lt(c: &str, v: LiteralValue) -> TableConstraint {
        TableConstraint::Comparison(col(c), BinaryOp::LessThan, v)
    }

    fn text(s: &str) -> LiteralValue {
        LiteralValue::String(s.into())
    }

    fn any_of(c: &str, vs: Vec<LiteralValue>) -> TableConstraint {
        TableConstraint::AnyOf(col(c), vs)
    }

    fn cast_eq(c: &str, cast: CastTarget, v: LiteralValue) -> TableConstraint {
        TableConstraint::CastComparison(col(c), cast, BinaryOp::Equal, v)
    }

    #[test]
    fn empty_index_has_no_candidates() {
        let idx = SubsumptionIndex::new();
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn equality_pure_exact_match() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);
        idx.insert(2, &[eq("id", int(99))]);

        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    #[test]
    fn equality_pure_different_value_misses() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);

        let candidates = idx.candidates(&[eq("id", int(99))]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn parent_broader_via_subset_filter() {
        let mut idx = SubsumptionIndex::new();
        // Parent constrains only category=5 — class {category}
        idx.insert(1, &[eq("category", int(5))]);

        // New constrains category=5 AND status='active' — class {category, status}
        // Subset enumeration should include {category} and find the parent.
        let new = vec![
            eq("category", int(5)),
            eq("status", LiteralValue::String("active".into())),
        ];
        let candidates = idx.candidates(&new);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn parent_with_unconstrained_column_finds_via_empty_class() {
        let mut idx = SubsumptionIndex::new();
        // Parent: full table scan, no constraints — class {}
        idx.insert(1, &[]);

        // New: WHERE id = 42 — class {id}
        // Empty subset of {id} should hit the empty class and pull the parent.
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn complex_constraint_lands_in_complex_bucket() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(100))]);
        idx.insert(
            2,
            &[any_of("status", vec![LiteralValue::String("a".into())])],
        );

        // New: WHERE id = 200 — pure equality on {id}.
        // Parent 1 is in class {id}.complex (gt is non-equality).
        // Parent 2 is in class {status}.complex.
        // Powerset of new's {id} = {{}, {id}}. Only {id} class will be hit;
        // parent 1 should be a candidate via complex scan.
        let candidates = idx.candidates(&[eq("id", int(200))]);
        assert!(candidates.contains(&1));
        assert!(!candidates.contains(&2)); // {status} ⊄ {id}, never visited
    }

    #[test]
    fn mixed_equality_and_complex_both_returned() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]); // class {id}.equality[(42,)]
        idx.insert(2, &[gt("id", int(0))]); // class {id}.complex

        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, [1, 2].into_iter().collect());
    }

    #[test]
    fn remove_drops_entry() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);
        idx.remove(1);

        assert!(idx.candidates(&[eq("id", int(42))]).is_empty());
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn remove_keeps_unrelated_entries() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(42))]);
        idx.insert(2, &[eq("id", int(42))]);
        idx.remove(1);

        assert_eq!(
            idx.candidates(&[eq("id", int(42))]),
            [2].into_iter().collect()
        );
    }

    #[test]
    fn column_order_does_not_affect_class_membership() {
        let mut idx = SubsumptionIndex::new();
        // Insert as [a, b]
        idx.insert(1, &[eq("a", int(1)), eq("b", int(2))]);
        // Lookup with [b, a] — same class.
        let candidates = idx.candidates(&[eq("b", int(2)), eq("a", int(1))]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    #[test]
    fn contradictory_equality_lands_in_complex() {
        let mut idx = SubsumptionIndex::new();
        // WHERE a=1 AND a=2 — same column, conflicting values. Classifier
        // falls back to complex.
        idx.insert(1, &[eq("a", int(1)), eq("a", int(2))]);
        // Probing equality lookup with a=1 must not return this entry from
        // the equality bucket (it's in complex). But complex scan finds it.
        let candidates = idx.candidates(&[eq("a", int(1))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn empty_new_query_finds_only_unconstrained_parents() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]); // unconstrained — class {}
        idx.insert(2, &[eq("id", int(42))]); // class {id}, not a subset of {}

        let candidates = idx.candidates(&[]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    #[test]
    fn powerset_bounded_by_column_count() {
        // 4 columns → 16 subsets. Just confirm we don't explode for a
        // realistic max.
        let cols = ColumnSet::new(vec![col("a"), col("b"), col("c"), col("d")]);
        assert_eq!(column_set_powerset(&cols).len(), 16);
    }

    // Regression: unconstrained parents must be findable by *complex* new
    // queries (range, IN, NOT IN). The fix was probing the empty-subset
    // equality bucket regardless of whether new is equality-pure.

    #[test]
    fn unconstrained_parent_subsumes_complex_new_range() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]); // unconstrained — class {}

        // New has a range constraint, not equality. Classified as Complex.
        let candidates = idx.candidates(&[gt("id", int(10))]);
        assert!(
            candidates.contains(&1),
            "unconstrained parent should subsume any range query"
        );
    }

    #[test]
    fn unconstrained_parent_subsumes_complex_new_inset() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);

        let candidates = idx.candidates(&[any_of("id", vec![int(1), int(2), int(3)])]);
        assert!(
            candidates.contains(&1),
            "unconstrained parent should subsume any IN-set query"
        );
    }

    #[test]
    fn unconstrained_parent_subsumes_mixed_new() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);

        // Mix of equality and non-equality across columns. Classified Complex
        // (any non-equality constraint demotes the whole query to Complex).
        let candidates = idx.candidates(&[eq("a", int(5)), gt("b", int(10))]);
        assert!(
            candidates.contains(&1),
            "unconstrained parent should subsume mixed new queries"
        );
    }

    #[test]
    fn unconstrained_new_finds_unconstrained_parent() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);
        idx.insert(2, &[eq("id", int(42))]);

        let candidates = idx.candidates(&[]);
        assert_eq!(candidates, [1].into_iter().collect());
    }

    // Idempotency: re-inserting the same fingerprint should replace the
    // previous indexing (not double-count).

    #[test]
    fn reinsert_same_fingerprint_replaces() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[eq("id", int(5))]);
        // Same fingerprint, different value — lookup of old value misses.
        idx.insert(1, &[eq("id", int(10))]);

        assert!(idx.candidates(&[eq("id", int(5))]).is_empty());
        assert_eq!(
            idx.candidates(&[eq("id", int(10))]),
            [1].into_iter().collect()
        );
    }

    #[test]
    fn reinsert_changing_shape_replaces() {
        let mut idx = SubsumptionIndex::new();
        // Start as Equality-pure on {id}.
        idx.insert(1, &[eq("id", int(5))]);
        // Re-insert with a range — now Complex on {id}.
        idx.insert(1, &[gt("id", int(0))]);

        // Lookup of id=42: equality bucket has no (42,) entry (we replaced
        // the (5,) entry with a complex one), but the complex bucket is
        // always scanned for visited subsets, so the range parent is found.
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, [1].into_iter().collect());
        // The (5,) equality bucket should be gone (no entry for fp=1 in it).
        // Confirm by counting classes — there's only the {id} class with one
        // complex entry, no leftover empty equality buckets.
        assert_eq!(idx.classes_len(), 1);
    }

    #[test]
    fn remove_unconstrained_drops_empty_class() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[]);
        idx.remove(1);

        assert_eq!(idx.classes_len(), 0);
        assert!(idx.candidates(&[]).is_empty());
    }

    // Documented limitation: parents in non-empty equality classes are only
    // probed when new is *fully* equality-pure on at least one matching
    // subset. When new is overall Complex (any non-equality constraint), the
    // equality probe is skipped for non-empty subsets — even if new has
    // matching equality on the subset's columns. This is a lossy-safe
    // false negative: we populate from origin rather than stamping, never
    // a wrong subsumption claim.
    //
    // The fingerprint can still be found via the complex bucket of the
    // matching subset class, so the only true miss is when the parent is in
    // an equality bucket of a non-empty class AND new is overall complex.
    #[test]
    fn known_limitation_equality_parent_missed_by_complex_new() {
        let mut idx = SubsumptionIndex::new();
        // Parent: WHERE a = 5 — lives in class {a}.equality[(5,)]
        idx.insert(1, &[eq("a", int(5))]);

        // New: WHERE a = 5 AND b > 10 — Complex overall, columns {a, b}
        let new = vec![eq("a", int(5)), gt("b", int(10))];
        let candidates = idx.candidates(&new);

        // The parent COULD subsume (parent's a=5 ⊇ new's a=5∧b>10 on column
        // a; parent has no constraint on b → covers all b). But the index
        // currently misses this — see comment above. Update this test if the
        // limitation is removed (per-column equality detection in candidates).
        assert!(
            !candidates.contains(&1),
            "limitation: equality parent in non-empty class not probed when new is Complex"
        );
    }

    // PGC-182: CastComparison constraints route through Complex classification
    // so the equality-pure fast-bucket doesn't try to index them by raw value
    // (their values live in the cast-output domain, not the column domain).

    #[test]
    fn cast_comparison_classifies_as_complex() {
        let constraint = cast_eq("name", CastTarget::Int4, int(42));
        let class = classify(&[constraint]);
        assert!(matches!(class, Classification::Complex { .. }));
    }

    #[test]
    fn cast_comparison_alongside_equality_classifies_as_complex() {
        // Mixed: a bare equality + a cast comparison. Cast presence forces Complex.
        let constraints = vec![eq("id", int(1)), cast_eq("name", CastTarget::Int4, int(42))];
        let class = classify(&constraints);
        assert!(matches!(class, Classification::Complex { .. }));
    }

    // PGC-129: complex-bucket subsumption contract. These assert that a
    // genuine subsumer is *returned* by `candidates()` — the invariant V1's
    // within-class index must preserve. V0 returns the whole complex bucket,
    // so they pass trivially today; they guard against V1 dropping a true
    // candidate. Precision (non-subsumers excluded) is asserted separately
    // once the V1 index lands, since V0 cannot satisfy it.

    #[test]
    fn range_parent_subsumes_equality_in_range() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(100))]);

        // New: WHERE id = 200 — inside the parent's (100, +inf) range.
        let candidates = idx.candidates(&[eq("id", int(200))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn range_parent_subsumes_narrower_range() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0))]);

        // New: WHERE id > 50 — narrower than the parent's id > 0.
        let candidates = idx.candidates(&[gt("id", int(50))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn inset_parent_subsumes_member_equality() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(
            1,
            &[any_of(
                "status",
                vec![
                    LiteralValue::String("a".into()),
                    LiteralValue::String("b".into()),
                ],
            )],
        );

        // New: WHERE status = 'a' — a member of the parent's set.
        let candidates = idx.candidates(&[eq("status", LiteralValue::String("a".into()))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn multi_column_complex_class_subsumer_returned() {
        let mut idx = SubsumptionIndex::new();
        // Parent: id > 10 AND region = 5 — class {id, region}, Complex.
        idx.insert(1, &[gt("id", int(10)), eq("region", int(5))]);

        // New: id > 20 AND region = 5 — narrower on id, same region.
        let candidates = idx.candidates(&[gt("id", int(20)), eq("region", int(5))]);
        assert!(candidates.contains(&1));
    }

    #[test]
    fn range_parent_in_subset_class_returned() {
        let mut idx = SubsumptionIndex::new();
        // Parent constrains only category — class {category}, Complex.
        idx.insert(1, &[gt("category", int(5))]);

        // New constrains category AND status — class {category, status}.
        // Subset enumeration must reach {category} and return the parent.
        let new = vec![
            gt("category", int(10)),
            eq("status", LiteralValue::String("x".into())),
        ];
        let candidates = idx.candidates(&new);
        assert!(candidates.contains(&1));
    }

    // PGC-129 V1 precision: the per-column index returns a *tight* candidate
    // set. These assert non-subsumers are *excluded* — V0's whole-bucket
    // scan could not satisfy them, so they land with the V1 index.

    #[test]
    fn range_parent_excludes_out_of_range_equality() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(100))]);

        // New: WHERE id = 50 — below the parent's (100, +inf) range.
        let candidates = idx.candidates(&[eq("id", int(50))]);
        assert!(!candidates.contains(&1));
    }

    #[test]
    fn range_parent_excludes_broader_range() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(50))]);

        // New: WHERE id > 10 — broader than the parent's id > 50.
        let candidates = idx.candidates(&[gt("id", int(10))]);
        assert!(!candidates.contains(&1));
    }

    #[test]
    fn upper_range_parent_bounds_both_ways() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[lt("id", int(100))]);

        assert!(idx.candidates(&[eq("id", int(50))]).contains(&1));
        assert!(!idx.candidates(&[eq("id", int(200))]).contains(&1));
    }

    #[test]
    fn inset_parent_excludes_non_member() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[any_of("status", vec![text("a"), text("b")])]);

        assert!(idx.candidates(&[eq("status", text("b"))]).contains(&1));
        assert!(!idx.candidates(&[eq("status", text("c"))]).contains(&1));
    }

    #[test]
    fn inset_parent_subsumes_subset_inset_only() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(
            1,
            &[any_of("status", vec![text("a"), text("b"), text("c")])],
        );

        // Subset IN — subsumed.
        assert!(
            idx.candidates(&[any_of("status", vec![text("a"), text("b")])])
                .contains(&1)
        );
        // IN with a non-member — not subsumed.
        assert!(
            !idx.candidates(&[any_of("status", vec![text("a"), text("d")])])
                .contains(&1)
        );
    }

    #[test]
    fn multi_column_excludes_when_one_column_misses() {
        let mut idx = SubsumptionIndex::new();
        // Parent: id > 10 AND region = 5.
        idx.insert(1, &[gt("id", int(10)), eq("region", int(5))]);

        // id is covered (20 > 10) but region mismatches — must be excluded.
        let candidates = idx.candidates(&[gt("id", int(20)), eq("region", int(9))]);
        assert!(!candidates.contains(&1));
    }

    #[test]
    fn two_sided_range_avoids_opaque_fallback() {
        // PGC-189: two-sided range parents go into `range_both`, not the
        // linear fallback. `complex_fallback_total` stays at zero.
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);

        assert_eq!(idx.complex_total(), 1);
        assert_eq!(idx.complex_fallback_total(), 0);
        assert!(idx.candidates(&[eq("id", int(50))]).contains(&1));
    }

    #[test]
    fn single_sided_range_avoids_fallback() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0))]);

        assert_eq!(idx.complex_total(), 1);
        assert_eq!(idx.complex_fallback_total(), 0);
    }

    #[test]
    fn remove_clears_range_parent() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(100))]);
        idx.remove(1);

        assert!(idx.candidates(&[eq("id", int(200))]).is_empty());
        assert_eq!(idx.complex_total(), 0);
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn remove_one_range_parent_keeps_sibling() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(10))]);
        idx.insert(2, &[gt("id", int(20))]);
        idx.remove(1);

        let candidates = idx.candidates(&[eq("id", int(30))]);
        assert!(!candidates.contains(&1));
        assert!(candidates.contains(&2));
        assert_eq!(idx.complex_total(), 1);
    }

    #[test]
    fn two_sided_column_does_not_mask_sibling() {
        // Parent: two-sided range on `id` (range_both), clean equality on
        // `region`. Region's precise filter must still apply across columns.
        let mut idx = SubsumptionIndex::new();
        idx.insert(
            1,
            &[gt("id", int(0)), lt("id", int(100)), eq("region", int(5))],
        );

        let hit = idx.candidates(&[eq("id", int(50)), eq("region", int(5))]);
        assert!(hit.contains(&1));
        let miss = idx.candidates(&[eq("id", int(50)), eq("region", int(9))]);
        assert!(!miss.contains(&1));
    }

    // PGC-189: two-sided range parents have their own sub-index. Precision
    // tests for the `range_both` code paths.

    #[test]
    fn two_sided_parent_subsumes_interior_equality() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);

        // Inside the interval — covered.
        assert!(idx.candidates(&[eq("id", int(50))]).contains(&1));
    }

    #[test]
    fn two_sided_parent_excludes_outside_equality() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);

        // Outside on either side — not covered.
        assert!(!idx.candidates(&[eq("id", int(200))]).contains(&1));
        assert!(!idx.candidates(&[eq("id", int(-10))]).contains(&1));
    }

    #[test]
    fn two_sided_parent_subsumes_narrower_two_sided_query() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);

        // Narrower interval — covered.
        let narrower = vec![gt("id", int(10)), lt("id", int(90))];
        assert!(idx.candidates(&narrower).contains(&1));
    }

    #[test]
    fn two_sided_parent_excludes_broader_two_sided_query() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(10)), lt("id", int(90))]);

        // Broader interval (parent narrower than query) — not covered.
        let broader = vec![gt("id", int(0)), lt("id", int(100))];
        assert!(!idx.candidates(&broader).contains(&1));
    }

    #[test]
    fn two_sided_parent_excludes_partial_overlap() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);

        // Overlaps on the right but extends past the parent — not covered.
        let shifted_right = vec![gt("id", int(50)), lt("id", int(200))];
        assert!(!idx.candidates(&shifted_right).contains(&1));

        // Overlaps on the left but extends past the parent — not covered.
        let shifted_left = vec![gt("id", int(-50)), lt("id", int(50))];
        assert!(!idx.candidates(&shifted_left).contains(&1));
    }

    #[test]
    fn two_sided_parent_does_not_cover_single_sided_query() {
        // A finite-bound parent cannot cover a half-infinite query interval.
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);

        // Query (50, +inf): parent's upper=100 < +inf — not covered.
        assert!(!idx.candidates(&[gt("id", int(50))]).contains(&1));
        // Query (-inf, 50): parent's lower=0 > -inf — not covered.
        assert!(!idx.candidates(&[lt("id", int(50))]).contains(&1));
    }

    #[test]
    fn two_sided_remove_clears_parent() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]);
        idx.remove(1);

        assert!(idx.candidates(&[eq("id", int(50))]).is_empty());
        assert_eq!(idx.complex_total(), 0);
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn two_sided_remove_one_keeps_sibling() {
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(50))]);
        idx.insert(2, &[gt("id", int(0)), lt("id", int(100))]);
        idx.remove(1);

        // Query at id=75 — only parent 2 (which has upper=100) covers it.
        let candidates = idx.candidates(&[eq("id", int(75))]);
        assert!(!candidates.contains(&1));
        assert!(candidates.contains(&2));
        assert_eq!(idx.complex_total(), 1);
    }

    #[test]
    fn two_sided_mixed_with_single_sided_class() {
        // Two-sided and single-sided parents coexisting on the same column.
        let mut idx = SubsumptionIndex::new();
        idx.insert(1, &[gt("id", int(0)), lt("id", int(100))]); // (0, 100)
        idx.insert(2, &[gt("id", int(20))]); // (20, +inf)

        // id=50 covered by both.
        let mid = idx.candidates(&[eq("id", int(50))]);
        assert!(mid.contains(&1));
        assert!(mid.contains(&2));

        // id=200 covered only by the single-sided parent.
        let high = idx.candidates(&[eq("id", int(200))]);
        assert!(!high.contains(&1));
        assert!(high.contains(&2));
    }
}
