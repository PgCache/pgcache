//! Sub-linear per-relation constraint-containment index.
//!
//! Indexes entries (keyed by an id type `K`) by their per-table constraints,
//! and answers "which entries' constraints could contain a given query's
//! constraints" sub-linearly. Subsumption candidate lookup is the first
//! consumer (replacing the linear scan over `UpdateQueries.queries` previously
//! done by `subsumption_check`); see PGC-119 for V0 and PGC-129 for V1.
//!
//! For each table, entries are partitioned by their constraint-column set.
//! Within a class, equality-pure entries are hash-indexed by the joint
//! value tuple. Entries with any non-equality constraint (Range/IN/etc.) go
//! to a `ComplexIndex`: one `ColumnIndex` per class column, each
//! partitioning entries by constraint shape (`eq` / `inset` / `range_lower`
//! / `range_upper` / `range_both` / `opaque` fallback). `candidates` runs a
//! per-column containment lookup and intersects the results.
//!
//! Lookup is **lossy-safe**: missed containment opportunities just mean we
//! populate from origin instead of stamping existing rows.

use crate::catalog::TableMetadata;
use crate::id_hash::{BuildIdHasher, IdHashable};
use crate::pg::protocol::ByteString;
use std::collections::{BTreeMap, HashMap, HashSet};

use ecow::EcoString;
use ordered_float::NotNan;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{ColumnRange, TableConstraint, column_range_build};
use crate::query::evaluate::pg_bool_parse;

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

#[derive(Debug)]
struct SubsumptionClass<K> {
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
struct Membership {
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
    pub(crate) fn candidates_point<F>(&self, col_forms_fn: F) -> IdSet<K>
    where
        F: Fn(&str) -> Vec<ColumnRange>,
    {
        let mut candidates = IdSet::default();
        for (column_set, class) in &self.classes {
            let col_forms: Vec<Vec<ColumnRange>> = column_set
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
            let key_sets: Vec<Vec<ValueKey>> = col_forms
                .iter()
                .map(|forms| {
                    forms
                        .iter()
                        .filter_map(|r| match r {
                            ColumnRange::Equal(v) => ValueKey::try_new(v),
                            ColumnRange::Unknown
                            | ColumnRange::Unconstrained
                            | ColumnRange::Empty
                            | ColumnRange::InSet(_)
                            | ColumnRange::Range { .. } => None,
                        })
                        .collect()
                })
                .collect();
            if key_sets.iter().all(|ks| !ks.is_empty()) {
                for tuple in value_key_product(&key_sets) {
                    if let Some(fps) = class.equality.get(&tuple) {
                        candidates.extend(fps);
                    }
                }
            } else {
                for (tuple, fps) in &class.equality {
                    let matches = key_sets
                        .iter()
                        .zip(tuple)
                        .all(|(ks, t)| ks.is_empty() || ks.contains(t));
                    if matches {
                        candidates.extend(fps);
                    }
                }
            }
            candidates.extend(class.complex.candidates_point(&col_forms));
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
        values: Vec<ValueKey>,
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
    full_values: &[ValueKey],
    subset: &ColumnSet,
) -> Option<Vec<ValueKey>> {
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

/// Cartesian product of per-column key sets, for the point-probe equality
/// lookup. Empty input → one empty tuple (the unconstrained class). Each
/// column carries ≤3 forms and classes have few columns, so the product stays
/// tiny.
fn value_key_product(key_sets: &[Vec<ValueKey>]) -> Vec<Vec<ValueKey>> {
    let mut result: Vec<Vec<ValueKey>> = vec![Vec::new()];
    for ks in key_sets {
        let mut next = Vec::with_capacity(result.len() * ks.len());
        for prefix in &result {
            for k in ks {
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
    Eq(ValueKey),
    InSet(&'a HashSet<LiteralValue>),
    RangeLower(ValueKey),
    RangeUpper(ValueKey),
    /// Two-sided range with orderable bounds (PGC-189).
    RangeBoth {
        lower: ValueKey,
        upper: ValueKey,
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
        ColumnRange::Equal(v) => ValueKey::try_new(v).map_or(Placement::Opaque, Placement::Eq),
        // All members must be keyable for the inverted `inset` index; one
        // unkeyable member routes the whole constraint to `opaque`
        // (deterministic, so insert and remove agree).
        ColumnRange::InSet(set) => {
            if set.iter().all(|v| ValueKey::try_new(v).is_some()) {
                Placement::InSet(set)
            } else {
                Placement::Opaque
            }
        }
        ColumnRange::Range { lower, upper, .. } => match (lower, upper) {
            (Some(lb), None) => {
                ValueKey::try_new(&lb.value).map_or(Placement::Opaque, Placement::RangeLower)
            }
            (None, Some(ub)) => {
                ValueKey::try_new(&ub.value).map_or(Placement::Opaque, Placement::RangeUpper)
            }
            (Some(lb), Some(ub)) => {
                match (ValueKey::try_new(&lb.value), ValueKey::try_new(&ub.value)) {
                    (Some(lower), Some(upper)) => Placement::RangeBoth { lower, upper },
                    _ => Placement::Opaque,
                }
            }
            (None, None) => Placement::Opaque,
        },
        ColumnRange::Unknown | ColumnRange::Unconstrained | ColumnRange::Empty => Placement::Opaque,
    }
}

/// Canonical, totally-ordered bucket key. Collapses `Integer(n)` and
/// `Float(n)` to a single numeric key so a row value coerced to either variant
/// probes the same bucket — load-bearing for the point probe, where a missed
/// entry is a stale read, not just a lost optimization. Integers past 2^53 may
/// share one `f64`, which only ever over-returns (the caller's precise check
/// rejects); it never drops a true match.
///
/// `String` and `StringWithCast` both key by their string content; non-keyable
/// values (`Null`, `Parameter`, `Array`) route to the `opaque` fallback.
/// Derived `Ord` orders by variant first (`Num` < `Str` < `Bool`), then by
/// value — `Bool` never reaches a range `BTreeMap`, and a single column never
/// mixes `Num`/`Str` meaningfully.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum ValueKey {
    Num(NotNan<f64>),
    Str(EcoString),
    Bool(bool),
}

impl ValueKey {
    fn try_new(v: &LiteralValue) -> Option<Self> {
        match v {
            // Integers past 2^53 may share an f64 — see the type doc; only
            // over-returns, never drops a true match.
            #[allow(clippy::cast_precision_loss)]
            LiteralValue::Integer(n) => {
                Some(ValueKey::Num(NotNan::new(*n as f64).expect("i64 as f64 is never NaN")))
            }
            LiteralValue::Float(f) => Some(ValueKey::Num(*f)),
            LiteralValue::String(s) | LiteralValue::StringWithCast(s, _) => {
                Some(ValueKey::Str(s.clone()))
            }
            LiteralValue::Boolean(b) => Some(ValueKey::Bool(*b)),
            LiteralValue::Null
            | LiteralValue::NullWithCast(_)
            | LiteralValue::Parameter(_)
            | LiteralValue::Array(..) => None,
        }
    }
}

/// Per-class index over complex (non-equality) parents. Holds one
/// `ColumnIndex` per class column; `candidates` intersects their per-column
/// match sets.
#[derive(Debug)]
struct ComplexIndex<K> {
    /// Parallel to the class's sorted column set.
    per_column: Vec<ColumnIndex<K>>,
    /// Distinct fingerprints indexed — each appears once per column.
    len: usize,
}

impl<K: IdHashable + Copy> ComplexIndex<K> {
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

    fn insert(&mut self, fingerprint: K, ranges: &[ColumnRange]) {
        for (column, range) in self.per_column.iter_mut().zip(ranges) {
            column.insert(fingerprint, range);
        }
        self.len += 1;
    }

    fn remove(&mut self, fingerprint: K, ranges: &[ColumnRange]) {
        for (column, range) in self.per_column.iter_mut().zip(ranges) {
            column.remove(fingerprint, range);
        }
        self.len = self.len.saturating_sub(1);
    }

    /// Candidate parents whose constraint on every column could subsume the
    /// query's. Intersects the per-column match sets, smallest first.
    fn candidates(&self, query_ranges: &[ColumnRange]) -> Vec<K> {
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
                let mut per_column: Vec<Vec<K>> = columns
                    .iter()
                    .zip(ranges)
                    .map(|(column, range)| column.containing(range))
                    .collect();
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
        }
    }

    /// Point-probe variant: each column supplies a list of `ColumnRange` forms
    /// (the row value's keyable interpretations). Union `containing` over a
    /// column's forms, then intersect across columns — mirrors `candidates`'
    /// smallest-first intersection. A `[Unknown]` column unions to every entry
    /// on that column (wildcard, no filtering).
    fn candidates_point(&self, col_forms: &[Vec<ColumnRange>]) -> Vec<K> {
        if self.len == 0 {
            return Vec::new();
        }
        match self.per_column.as_slice() {
            [] => Vec::new(),
            // Single-column class: concatenate the per-form match sets — the
            // caller dedups into its own set.
            [column] => col_forms
                .first()
                .map(Vec::as_slice)
                .unwrap_or(&[])
                .iter()
                .flat_map(|form| column.containing(form))
                .collect(),
            columns => {
                let mut per_column: Vec<Vec<K>> = columns
                    .iter()
                    .zip(col_forms)
                    .map(|(column, forms)| {
                        let mut set: IdSet<K> = IdSet::default();
                        for form in forms {
                            set.extend(column.containing(form));
                        }
                        set.into_iter().collect::<Vec<K>>()
                    })
                    .collect();
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
    fn extend_two_sided_lit(
        &self,
        qlo: &LiteralValue,
        qhi: &LiteralValue,
        out: &mut Vec<K>,
    ) {
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

/// Coerce a CDC row's value for `column` into the point-probe forms: every
/// keyable interpretation of the wire text, as `Equal` ranges. A present
/// value always yields its lexical `String` form, plus a `Float` form when it
/// parses numerically and a `Boolean` form when it is `t`/`f`. Probing all
/// forms (unioned) is what keeps the probe correct regardless of how the
/// matching entry's literal was typed — a numeric column can hold a
/// `String`-keyed entry via an identity `::text` cast (`val::text = '42'`
/// strips to `Comparison(val, Eq, String("42"))`), and a `String` row form
/// finds it while the `Float` form finds the ordinary `val = 42` entry.
///
/// An absent column, SQL NULL, or unchanged-TOAST yields `[Unknown]` — a
/// wildcard that matches every entry constraining the column (conservative,
/// never under-returns). The forms mirror `where_value_compare_string`'s row
/// interpretation, so the precise check downstream agrees.
pub(crate) fn row_value_forms(
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    column: &str,
) -> Vec<ColumnRange> {
    let Some(meta) = table_metadata.columns.get(column) else {
        return vec![ColumnRange::Unknown];
    };
    let Some(Some(bytes)) = row_data.get(meta.index()) else {
        return vec![ColumnRange::Unknown];
    };
    let text = bytes.as_str();
    let mut forms = Vec::with_capacity(2);
    forms.push(ColumnRange::Equal(LiteralValue::String(text.into())));
    if let Some(n) = text.parse::<f64>().ok().and_then(|x| NotNan::new(x).ok()) {
        forms.push(ColumnRange::Equal(LiteralValue::Float(n)));
    }
    if let Some(b) = pg_bool_parse(text) {
        forms.push(ColumnRange::Equal(LiteralValue::Boolean(b)));
    }
    forms
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, Oid, TableMetadata};
    use crate::query::ast::{BinaryOp, LiteralValue};
    use crate::query::cast::CastTarget;
    use tokio_postgres::types::Type;
    use crate::query::{Fingerprint, FingerprintSet};
    use ecow::EcoString;

    fn col(s: &str) -> EcoString {
        EcoString::from(s)
    }

    fn fp(n: u64) -> Fingerprint {
        Fingerprint::from_raw(n)
    }

    fn fps<const N: usize>(a: [u64; N]) -> FingerprintSet {
        a.into_iter().map(Fingerprint::from_raw).collect()
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

    fn float_lit(x: f64) -> LiteralValue {
        LiteralValue::Float(NotNan::new(x).unwrap())
    }

    fn bs(s: &str) -> Option<ByteString> {
        Some(ByteString::from_utf8(bytes::Bytes::copy_from_slice(s.as_bytes())).expect("utf8"))
    }

    /// `[id int4 (pk), name text, active bool]` — row layout `[id, name, active]`.
    fn point_table() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "integer".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "active".into(),
                position: 3,
                type_oid: 16,
                data_type: Type::BOOL,
                type_name: "boolean".into(),
                cache_type_name: "bool".into(),
                is_primary_key: false,
            },
        ]);
        TableMetadata {
            name: "t".into(),
            schema: "public".into(),
            relation_oid: Oid::from_raw(1),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    #[test]
    fn empty_index_has_no_candidates() {
        let idx = ConstraintIndex::<Fingerprint>::new();
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn equality_pure_exact_match() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(42))]);
        idx.insert(fp(2), &[eq("id", int(99))]);

        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, fps([1]));
    }

    #[test]
    fn equality_pure_different_value_misses() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(42))]);

        let candidates = idx.candidates(&[eq("id", int(99))]);
        assert!(candidates.is_empty());
    }

    #[test]
    fn parent_broader_via_subset_filter() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Parent constrains only category=5 — class {category}
        idx.insert(fp(1), &[eq("category", int(5))]);

        // New constrains category=5 AND status='active' — class {category, status}
        // Subset enumeration should include {category} and find the parent.
        let new = vec![
            eq("category", int(5)),
            eq("status", LiteralValue::String("active".into())),
        ];
        let candidates = idx.candidates(&new);
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn parent_with_unconstrained_column_finds_via_empty_class() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Parent: full table scan, no constraints — class {}
        idx.insert(fp(1), &[]);

        // New: WHERE id = 42 — class {id}
        // Empty subset of {id} should hit the empty class and pull the parent.
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn complex_constraint_lands_in_complex_bucket() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(100))]);
        idx.insert(
            fp(2),
            &[any_of("status", vec![LiteralValue::String("a".into())])],
        );

        // New: WHERE id = 200 — pure equality on {id}.
        // Parent 1 is in class {id}.complex (gt is non-equality).
        // Parent 2 is in class {status}.complex.
        // Powerset of new's {id} = {{}, {id}}. Only {id} class will be hit;
        // parent 1 should be a candidate via complex scan.
        let candidates = idx.candidates(&[eq("id", int(200))]);
        assert!(candidates.contains(&fp(1)));
        assert!(!candidates.contains(&fp(2))); // {status} ⊄ {id}, never visited
    }

    #[test]
    fn mixed_equality_and_complex_both_returned() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(42))]); // class {id}.equality[(42,)]
        idx.insert(fp(2), &[gt("id", int(0))]); // class {id}.complex

        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, fps([1, 2]));
    }

    #[test]
    fn remove_drops_entry() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(42))]);
        idx.remove(fp(1));

        assert!(idx.candidates(&[eq("id", int(42))]).is_empty());
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn remove_keeps_unrelated_entries() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(42))]);
        idx.insert(fp(2), &[eq("id", int(42))]);
        idx.remove(fp(1));

        assert_eq!(idx.candidates(&[eq("id", int(42))]), fps([2]));
    }

    #[test]
    fn column_order_does_not_affect_class_membership() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Insert as [a, b]
        idx.insert(fp(1), &[eq("a", int(1)), eq("b", int(2))]);
        // Lookup with [b, a] — same class.
        let candidates = idx.candidates(&[eq("b", int(2)), eq("a", int(1))]);
        assert_eq!(candidates, fps([1]));
    }

    #[test]
    fn contradictory_equality_lands_in_complex() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // WHERE a=1 AND a=2 — same column, conflicting values. Classifier
        // falls back to complex.
        idx.insert(fp(1), &[eq("a", int(1)), eq("a", int(2))]);
        // Probing equality lookup with a=1 must not return this entry from
        // the equality bucket (it's in complex). But complex scan finds it.
        let candidates = idx.candidates(&[eq("a", int(1))]);
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn empty_new_query_finds_only_unconstrained_parents() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[]); // unconstrained — class {}
        idx.insert(fp(2), &[eq("id", int(42))]); // class {id}, not a subset of {}

        let candidates = idx.candidates(&[]);
        assert_eq!(candidates, fps([1]));
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
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[]); // unconstrained — class {}

        // New has a range constraint, not equality. Classified as Complex.
        let candidates = idx.candidates(&[gt("id", int(10))]);
        assert!(
            candidates.contains(&fp(1)),
            "unconstrained parent should subsume any range query"
        );
    }

    #[test]
    fn unconstrained_parent_subsumes_complex_new_inset() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[]);

        let candidates = idx.candidates(&[any_of("id", vec![int(1), int(2), int(3)])]);
        assert!(
            candidates.contains(&fp(1)),
            "unconstrained parent should subsume any IN-set query"
        );
    }

    #[test]
    fn unconstrained_parent_subsumes_mixed_new() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[]);

        // Mix of equality and non-equality across columns. Classified Complex
        // (any non-equality constraint demotes the whole query to Complex).
        let candidates = idx.candidates(&[eq("a", int(5)), gt("b", int(10))]);
        assert!(
            candidates.contains(&fp(1)),
            "unconstrained parent should subsume mixed new queries"
        );
    }

    #[test]
    fn unconstrained_new_finds_unconstrained_parent() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[]);
        idx.insert(fp(2), &[eq("id", int(42))]);

        let candidates = idx.candidates(&[]);
        assert_eq!(candidates, fps([1]));
    }

    // Idempotency: re-inserting the same fingerprint should replace the
    // previous indexing (not double-count).

    #[test]
    fn reinsert_same_fingerprint_replaces() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(5))]);
        // Same fingerprint, different value — lookup of old value misses.
        idx.insert(fp(1), &[eq("id", int(10))]);

        assert!(idx.candidates(&[eq("id", int(5))]).is_empty());
        assert_eq!(idx.candidates(&[eq("id", int(10))]), fps([1]));
    }

    #[test]
    fn reinsert_changing_shape_replaces() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Start as Equality-pure on {id}.
        idx.insert(fp(1), &[eq("id", int(5))]);
        // Re-insert with a range — now Complex on {id}.
        idx.insert(fp(1), &[gt("id", int(0))]);

        // Lookup of id=42: equality bucket has no (42,) entry (we replaced
        // the (5,) entry with a complex one), but the complex bucket is
        // always scanned for visited subsets, so the range parent is found.
        let candidates = idx.candidates(&[eq("id", int(42))]);
        assert_eq!(candidates, fps([1]));
        // The (5,) equality bucket should be gone (no entry for fp=1 in it).
        // Confirm by counting classes — there's only the {id} class with one
        // complex entry, no leftover empty equality buckets.
        assert_eq!(idx.classes_len(), 1);
    }

    #[test]
    fn remove_unconstrained_drops_empty_class() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[]);
        idx.remove(fp(1));

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
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Parent: WHERE a = 5 — lives in class {a}.equality[(5,)]
        idx.insert(fp(1), &[eq("a", int(5))]);

        // New: WHERE a = 5 AND b > 10 — Complex overall, columns {a, b}
        let new = vec![eq("a", int(5)), gt("b", int(10))];
        let candidates = idx.candidates(&new);

        // The parent COULD subsume (parent's a=5 ⊇ new's a=5∧b>10 on column
        // a; parent has no constraint on b → covers all b). But the index
        // currently misses this — see comment above. Update this test if the
        // limitation is removed (per-column equality detection in candidates).
        assert!(
            !candidates.contains(&fp(1)),
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
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(100))]);

        // New: WHERE id = 200 — inside the parent's (100, +inf) range.
        let candidates = idx.candidates(&[eq("id", int(200))]);
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn range_parent_subsumes_narrower_range() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0))]);

        // New: WHERE id > 50 — narrower than the parent's id > 0.
        let candidates = idx.candidates(&[gt("id", int(50))]);
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn inset_parent_subsumes_member_equality() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(
            fp(1),
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
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn multi_column_complex_class_subsumer_returned() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Parent: id > 10 AND region = 5 — class {id, region}, Complex.
        idx.insert(fp(1), &[gt("id", int(10)), eq("region", int(5))]);

        // New: id > 20 AND region = 5 — narrower on id, same region.
        let candidates = idx.candidates(&[gt("id", int(20)), eq("region", int(5))]);
        assert!(candidates.contains(&fp(1)));
    }

    #[test]
    fn range_parent_in_subset_class_returned() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Parent constrains only category — class {category}, Complex.
        idx.insert(fp(1), &[gt("category", int(5))]);

        // New constrains category AND status — class {category, status}.
        // Subset enumeration must reach {category} and return the parent.
        let new = vec![
            gt("category", int(10)),
            eq("status", LiteralValue::String("x".into())),
        ];
        let candidates = idx.candidates(&new);
        assert!(candidates.contains(&fp(1)));
    }

    // PGC-129 V1 precision: the per-column index returns a *tight* candidate
    // set. These assert non-subsumers are *excluded* — V0's whole-bucket
    // scan could not satisfy them, so they land with the V1 index.

    #[test]
    fn range_parent_excludes_out_of_range_equality() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(100))]);

        // New: WHERE id = 50 — below the parent's (100, +inf) range.
        let candidates = idx.candidates(&[eq("id", int(50))]);
        assert!(!candidates.contains(&fp(1)));
    }

    #[test]
    fn range_parent_excludes_broader_range() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(50))]);

        // New: WHERE id > 10 — broader than the parent's id > 50.
        let candidates = idx.candidates(&[gt("id", int(10))]);
        assert!(!candidates.contains(&fp(1)));
    }

    #[test]
    fn upper_range_parent_bounds_both_ways() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[lt("id", int(100))]);

        assert!(idx.candidates(&[eq("id", int(50))]).contains(&fp(1)));
        assert!(!idx.candidates(&[eq("id", int(200))]).contains(&fp(1)));
    }

    #[test]
    fn inset_parent_excludes_non_member() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[any_of("status", vec![text("a"), text("b")])]);

        assert!(idx.candidates(&[eq("status", text("b"))]).contains(&fp(1)));
        assert!(!idx.candidates(&[eq("status", text("c"))]).contains(&fp(1)));
    }

    #[test]
    fn inset_parent_subsumes_subset_inset_only() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(
            fp(1),
            &[any_of("status", vec![text("a"), text("b"), text("c")])],
        );

        // Subset IN — subsumed.
        assert!(
            idx.candidates(&[any_of("status", vec![text("a"), text("b")])])
                .contains(&fp(1))
        );
        // IN with a non-member — not subsumed.
        assert!(
            !idx.candidates(&[any_of("status", vec![text("a"), text("d")])])
                .contains(&fp(1))
        );
    }

    #[test]
    fn multi_column_excludes_when_one_column_misses() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Parent: id > 10 AND region = 5.
        idx.insert(fp(1), &[gt("id", int(10)), eq("region", int(5))]);

        // id is covered (20 > 10) but region mismatches — must be excluded.
        let candidates = idx.candidates(&[gt("id", int(20)), eq("region", int(9))]);
        assert!(!candidates.contains(&fp(1)));
    }

    #[test]
    fn two_sided_range_avoids_opaque_fallback() {
        // PGC-189: two-sided range parents go into `range_both`, not the
        // linear fallback. `complex_fallback_total` stays at zero.
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);

        assert_eq!(idx.complex_total(), 1);
        assert_eq!(idx.complex_fallback_total(), 0);
        assert!(idx.candidates(&[eq("id", int(50))]).contains(&fp(1)));
    }

    #[test]
    fn single_sided_range_avoids_fallback() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0))]);

        assert_eq!(idx.complex_total(), 1);
        assert_eq!(idx.complex_fallback_total(), 0);
    }

    #[test]
    fn remove_clears_range_parent() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(100))]);
        idx.remove(fp(1));

        assert!(idx.candidates(&[eq("id", int(200))]).is_empty());
        assert_eq!(idx.complex_total(), 0);
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn remove_one_range_parent_keeps_sibling() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(10))]);
        idx.insert(fp(2), &[gt("id", int(20))]);
        idx.remove(fp(1));

        let candidates = idx.candidates(&[eq("id", int(30))]);
        assert!(!candidates.contains(&fp(1)));
        assert!(candidates.contains(&fp(2)));
        assert_eq!(idx.complex_total(), 1);
    }

    #[test]
    fn two_sided_column_does_not_mask_sibling() {
        // Parent: two-sided range on `id` (range_both), clean equality on
        // `region`. Region's precise filter must still apply across columns.
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(
            fp(1),
            &[gt("id", int(0)), lt("id", int(100)), eq("region", int(5))],
        );

        let hit = idx.candidates(&[eq("id", int(50)), eq("region", int(5))]);
        assert!(hit.contains(&fp(1)));
        let miss = idx.candidates(&[eq("id", int(50)), eq("region", int(9))]);
        assert!(!miss.contains(&fp(1)));
    }

    // PGC-189: two-sided range parents have their own sub-index. Precision
    // tests for the `range_both` code paths.

    #[test]
    fn two_sided_parent_subsumes_interior_equality() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);

        // Inside the interval — covered.
        assert!(idx.candidates(&[eq("id", int(50))]).contains(&fp(1)));
    }

    #[test]
    fn two_sided_parent_excludes_outside_equality() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);

        // Outside on either side — not covered.
        assert!(!idx.candidates(&[eq("id", int(200))]).contains(&fp(1)));
        assert!(!idx.candidates(&[eq("id", int(-10))]).contains(&fp(1)));
    }

    #[test]
    fn two_sided_parent_subsumes_narrower_two_sided_query() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);

        // Narrower interval — covered.
        let narrower = vec![gt("id", int(10)), lt("id", int(90))];
        assert!(idx.candidates(&narrower).contains(&fp(1)));
    }

    #[test]
    fn two_sided_parent_excludes_broader_two_sided_query() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(10)), lt("id", int(90))]);

        // Broader interval (parent narrower than query) — not covered.
        let broader = vec![gt("id", int(0)), lt("id", int(100))];
        assert!(!idx.candidates(&broader).contains(&fp(1)));
    }

    #[test]
    fn two_sided_parent_excludes_partial_overlap() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);

        // Overlaps on the right but extends past the parent — not covered.
        let shifted_right = vec![gt("id", int(50)), lt("id", int(200))];
        assert!(!idx.candidates(&shifted_right).contains(&fp(1)));

        // Overlaps on the left but extends past the parent — not covered.
        let shifted_left = vec![gt("id", int(-50)), lt("id", int(50))];
        assert!(!idx.candidates(&shifted_left).contains(&fp(1)));
    }

    #[test]
    fn two_sided_parent_does_not_cover_single_sided_query() {
        // A finite-bound parent cannot cover a half-infinite query interval.
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);

        // Query (50, +inf): parent's upper=100 < +inf — not covered.
        assert!(!idx.candidates(&[gt("id", int(50))]).contains(&fp(1)));
        // Query (-inf, 50): parent's lower=0 > -inf — not covered.
        assert!(!idx.candidates(&[lt("id", int(50))]).contains(&fp(1)));
    }

    #[test]
    fn two_sided_remove_clears_parent() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]);
        idx.remove(fp(1));

        assert!(idx.candidates(&[eq("id", int(50))]).is_empty());
        assert_eq!(idx.complex_total(), 0);
        assert_eq!(idx.classes_len(), 0);
    }

    #[test]
    fn two_sided_remove_one_keeps_sibling() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(50))]);
        idx.insert(fp(2), &[gt("id", int(0)), lt("id", int(100))]);
        idx.remove(fp(1));

        // Query at id=75 — only parent 2 (which has upper=100) covers it.
        let candidates = idx.candidates(&[eq("id", int(75))]);
        assert!(!candidates.contains(&fp(1)));
        assert!(candidates.contains(&fp(2)));
        assert_eq!(idx.complex_total(), 1);
    }

    #[test]
    fn two_sided_mixed_with_single_sided_class() {
        // Two-sided and single-sided parents coexisting on the same column.
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", int(0)), lt("id", int(100))]); // (0, 100)
        idx.insert(fp(2), &[gt("id", int(20))]); // (20, +inf)

        // id=50 covered by both.
        let mid = idx.candidates(&[eq("id", int(50))]);
        assert!(mid.contains(&fp(1)));
        assert!(mid.contains(&fp(2)));

        // id=200 covered only by the single-sided parent.
        let high = idx.candidates(&[eq("id", int(200))]);
        assert!(!high.contains(&fp(1)));
        assert!(high.contains(&fp(2)));
    }

    // Option A: `Integer(n)` and `Float(n)` collapse to one canonical key, so
    // the index returns cross-variant numeric candidates (no under-return).

    #[test]
    fn numeric_unification_equality_cross_variant() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(200))]);
        assert!(
            idx.candidates(&[eq("id", float_lit(200.0))]).contains(&fp(1)),
            "Float(200.0) probe must find an Integer(200) entry"
        );

        let mut idx2 = ConstraintIndex::<Fingerprint>::new();
        idx2.insert(fp(2), &[eq("id", float_lit(200.0))]);
        assert!(
            idx2.candidates(&[eq("id", int(200))]).contains(&fp(2)),
            "Integer(200) probe must find a Float(200.0) entry"
        );
    }

    #[test]
    fn numeric_unification_range_cross_variant() {
        // Integer lower-bound range, Float point probe.
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("price", int(10))]);
        assert!(idx.candidates(&[eq("price", float_lit(50.0))]).contains(&fp(1)));
        assert!(!idx.candidates(&[eq("price", float_lit(5.0))]).contains(&fp(1)));

        // Float upper-bound range, Integer point probe.
        let mut idx2 = ConstraintIndex::<Fingerprint>::new();
        idx2.insert(fp(2), &[lt("price", float_lit(100.0))]);
        assert!(idx2.candidates(&[eq("price", int(50))]).contains(&fp(2)));
        assert!(!idx2.candidates(&[eq("price", int(200))]).contains(&fp(2)));
    }

    // Point probe: the row is an `Equal`-on-every-column degenerate query.

    #[test]
    fn point_probe_basic() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(200))]);
        idx.insert(fp(2), &[eq("id", int(999))]);
        idx.insert(fp(3), &[]); // unconstrained — matches every row

        let got = idx.candidates_point(|c| match c {
            "id" => vec![ColumnRange::Equal(int(200))],
            _ => vec![ColumnRange::Unknown],
        });
        assert!(got.contains(&fp(1)));
        assert!(got.contains(&fp(3)));
        assert!(!got.contains(&fp(2)), "id=999 must be excluded for a id=200 row");
    }

    #[test]
    fn point_probe_unknown_is_conservative() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(200))]); // equality-pure bucket
        idx.insert(fp(2), &[gt("id", int(100))]); // complex bucket

        // An `Unknown` column (NULL / unchanged-TOAST) must return every entry
        // constraining it — both buckets — never drop one.
        let got = idx.candidates_point(|_| vec![ColumnRange::Unknown]);
        assert!(got.contains(&fp(1)), "equality-pure entry must not be dropped under Unknown");
        assert!(got.contains(&fp(2)), "complex entry must not be dropped under Unknown");
    }

    #[test]
    fn point_probe_partial_unknown_filters_known_columns() {
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        // Two-column equality-pure entries on {id, region}.
        idx.insert(fp(1), &[eq("id", int(1)), eq("region", int(5))]);
        idx.insert(fp(2), &[eq("id", int(2)), eq("region", int(5))]);

        // region pinned to 5, id unknown: both match on the pinned column.
        let got = idx.candidates_point(|c| match c {
            "region" => vec![ColumnRange::Equal(int(5))],
            _ => vec![ColumnRange::Unknown],
        });
        assert!(got.contains(&fp(1)));
        assert!(got.contains(&fp(2)));

        // region pinned to a non-matching value excludes both.
        let none = idx.candidates_point(|c| match c {
            "region" => vec![ColumnRange::Equal(int(9))],
            _ => vec![ColumnRange::Unknown],
        });
        assert!(none.is_empty());
    }

    // `row_value_forms`: every keyable interpretation of the wire text.

    fn has_str_form(forms: &[ColumnRange], s: &str) -> bool {
        forms.iter().any(|r| matches!(r, ColumnRange::Equal(LiteralValue::String(v)) if v == s))
    }
    fn has_num_form(forms: &[ColumnRange], x: f64) -> bool {
        forms.iter().any(
            |r| matches!(r, ColumnRange::Equal(LiteralValue::Float(n)) if *n == NotNan::new(x).unwrap()),
        )
    }

    #[test]
    fn row_value_forms_coercion() {
        let t = point_table();
        let row = [bs("200"), bs("alice"), bs("t")];

        // numeric column "200" → BOTH the String and Float forms (an entry may
        // be keyed under either, e.g. `id = 200` vs `id::text = '200'`).
        let id = row_value_forms(&t, &row, "id");
        assert!(has_str_form(&id, "200"));
        assert!(has_num_form(&id, 200.0));

        // text column "alice" → String form only (not numerically parseable).
        let name = row_value_forms(&t, &row, "name");
        assert!(has_str_form(&name, "alice"));
        assert!(!name.iter().any(|r| matches!(r, ColumnRange::Equal(LiteralValue::Float(_)))));

        // bool column "t" → String("t") plus Boolean(true).
        let active = row_value_forms(&t, &row, "active");
        assert!(has_str_form(&active, "t"));
        assert!(
            active
                .iter()
                .any(|r| matches!(r, ColumnRange::Equal(LiteralValue::Boolean(true))))
        );

        // SQL NULL / absent column → [Unknown] (wildcard).
        let null_row = [None, bs("bob"), bs("f")];
        assert!(matches!(row_value_forms(&t, &null_row, "id").as_slice(), [ColumnRange::Unknown]));
        assert!(matches!(row_value_forms(&t, &row, "nope").as_slice(), [ColumnRange::Unknown]));

        // numeric-looking-but-textual: a non-numeric text yields only String.
        let bad_row = [bs("abc"), bs("bob"), bs("f")];
        let bad = row_value_forms(&t, &bad_row, "id");
        assert!(has_str_form(&bad, "abc"));
        assert!(!bad.iter().any(|r| matches!(r, ColumnRange::Equal(LiteralValue::Float(_)))));
    }

    #[test]
    fn row_value_forms_drives_point_probe() {
        let t = point_table();
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", int(200))]);
        idx.insert(fp(2), &[eq("id", int(7))]);

        let row = [bs("200"), bs("alice"), bs("t")];
        let got = idx.candidates_point(|c| row_value_forms(&t, &row, c));
        assert!(got.contains(&fp(1)));
        assert!(!got.contains(&fp(2)));
    }

    // Regression: a numeric column can hold a String-literal constraint via an
    // identity `::text` cast (`val::text = '42'` → `Comparison(val, Eq,
    // String("42"))`). The point probe must find it through the String form,
    // while still finding ordinary `Num`-keyed entries through the Float form.

    #[test]
    fn point_probe_numeric_column_string_literal_equality() {
        let t = point_table();
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[eq("id", text("200"))]); // id::text = '200' → String
        idx.insert(fp(2), &[eq("id", int(200))]); // id = 200 → Num
        idx.insert(fp(3), &[eq("id", text("7"))]); // non-matching String

        let row = [bs("200"), bs("alice"), bs("t")];
        let got = idx.candidates_point(|c| row_value_forms(&t, &row, c));
        assert!(got.contains(&fp(1)), "String('200') entry found via the String form");
        assert!(got.contains(&fp(2)), "Integer(200) entry found via the Float form");
        assert!(!got.contains(&fp(3)), "String('7') entry must not match a '200' row");
    }

    #[test]
    fn point_probe_numeric_column_string_literal_range() {
        // A String-keyed range walks the lexicographic `Str` region; a '42'
        // row must satisfy `> '10'` lexicographically and not be under-returned.
        let t = point_table();
        let mut idx = ConstraintIndex::<Fingerprint>::new();
        idx.insert(fp(1), &[gt("id", text("10"))]); // id::text > '10'

        let row = [bs("42"), bs("alice"), bs("t")];
        let got = idx.candidates_point(|c| row_value_forms(&t, &row, c));
        assert!(got.contains(&fp(1)), "'42' > '10' lexicographically — must be a candidate");

        // A row whose text is lexicographically below '10' must be excluded.
        let row_lo = [bs("09"), bs("alice"), bs("t")];
        let got_lo = idx.candidates_point(|c| row_value_forms(&t, &row_lo, c));
        assert!(!got_lo.contains(&fp(1)), "'09' < '10' lexicographically");
    }
}
