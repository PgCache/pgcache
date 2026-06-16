//! Baseline benchmarks for `ConstraintIndex` complex-bucket lookup.
//!
//! Two workload families:
//!
//! * **single-sided ranges** (PGC-129 V1) — N parents `WHERE id > k`, all in
//!   class `{id}`. V1 indexes these in `range_lower`; lookup is sub-linear.
//! * **two-sided ranges** (PGC-189 V2) — N parents `WHERE id > k AND id < k+W`
//!   with fixed window W. V1 dumps these in the `opaque` linear fallback and
//!   returns the whole bucket on every query; V2's job is to make the
//!   selective case sub-linear.

#![allow(clippy::unwrap_used)]
#![allow(clippy::cast_possible_wrap)]
#![allow(clippy::cast_sign_loss)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ecow::EcoString;

use pgcache_lib::query::Fingerprint;
use pgcache_lib::query::constraint_index::ConstraintIndex;
use pgcache_lib::query::ast::{BinaryOp, LiteralValue};
use pgcache_lib::query::constraints::TableConstraint;

const SIZES: [usize; 4] = [64, 256, 1024, 4096];

fn col(s: &str) -> EcoString {
    EcoString::from(s)
}

fn int(n: i64) -> LiteralValue {
    LiteralValue::Integer(n)
}

/// `WHERE <c> > v` — single-sided lower-bound range, lands in the complex bucket.
fn gt(c: &str, v: i64) -> TableConstraint {
    TableConstraint::Comparison(col(c), BinaryOp::GreaterThan, int(v))
}

/// `WHERE <c> < v` — single-sided upper-bound range.
fn lt(c: &str, v: i64) -> TableConstraint {
    TableConstraint::Comparison(col(c), BinaryOp::LessThan, int(v))
}

/// Build an index of `n` single-sided range parents, all in class `{id}`.
fn index_single_class(n: usize) -> ConstraintIndex<Fingerprint> {
    let mut idx = ConstraintIndex::<Fingerprint>::new();
    for k in 0..n {
        idx.insert(Fingerprint::from_raw(k as u64), &[gt("id", k as i64)]);
    }
    idx
}

/// Two-sided window width. Each parent covers a `WINDOW`-wide interval, so a
/// point lookup is subsumed by ~`WINDOW` parents regardless of N — the true
/// candidate set is bounded, and V2's sub-linear lookup should ride that.
const TWO_SIDED_WINDOW: i64 = 100;

/// Build an index of `n` two-sided range parents `WHERE id > k AND id < k+W`,
/// all in class `{id}`. Under V1 these all collapse to the per-column `opaque`
/// linear-fallback bucket.
fn index_two_sided_class(n: usize) -> ConstraintIndex<Fingerprint> {
    let mut idx = ConstraintIndex::<Fingerprint>::new();
    for k in 0..n {
        let k = k as i64;
        idx.insert(
            Fingerprint::from_raw(k as u64),
            &[gt("id", k), lt("id", k + TWO_SIDED_WINDOW)],
        );
    }
    idx
}

/// `candidates()` with a *selective* lookup — an equality probe near the
/// bottom of the range, subsumed by only a handful of parents regardless of
/// N. This isolates the lookup cost from the result-set size, so the
/// sub-linear win shows directly.
fn bench_candidates_selective(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsumption/candidates_selective");
    for n in SIZES {
        let idx = index_single_class(n);
        // id = 3 — subsumed only by parents `id > 0/1/2`, i.e. 3 of them.
        let query = [TableConstraint::Comparison(
            col("id"),
            BinaryOp::Equal,
            int(3),
        )];
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(idx.candidates(black_box(&query))));
        });
    }
    group.finish();
}

/// `candidates()` with a midpoint probe — subsumed by ~N/2 parents, so the
/// result set itself grows with N. The lookup is sub-linear but the returned
/// `HashSet` is not; this variant stays output-bound by design.
fn bench_candidates_midpoint(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsumption/candidates_midpoint");
    for n in SIZES {
        let idx = index_single_class(n);
        let query = [TableConstraint::Comparison(
            col("id"),
            BinaryOp::Equal,
            int((n / 2) as i64),
        )];
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(idx.candidates(black_box(&query))));
        });
    }
    group.finish();
}

/// Index build cost — `n` sequential `insert()` calls. Captures the
/// maintenance side that V1 must keep cheaper than the per-Register saving.
fn bench_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsumption/build");
    for n in SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(index_single_class(black_box(n))));
        });
    }
    group.finish();
}

/// Point lookup against a two-sided-range class. True candidate set is bounded
/// by `TWO_SIDED_WINDOW` regardless of N; V1's `opaque` fallback returns every
/// parent (~N) on every query — the linear baseline V2 must beat.
fn bench_two_sided_point(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsumption/two_sided_point");
    for n in SIZES {
        let idx = index_two_sided_class(n);
        // Probe the middle of the parent-center distribution, so the result
        // set isn't artificially clipped by being near an endpoint.
        let query = [TableConstraint::Comparison(
            col("id"),
            BinaryOp::Equal,
            int((n / 2) as i64),
        )];
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(idx.candidates(black_box(&query))));
        });
    }
    group.finish();
}

/// Range lookup against a two-sided-range class. The query itself is two-sided
/// and tight, subsumed by ~`TWO_SIDED_WINDOW` parents. Exercises the
/// `ColumnRange::Range { lower: Some, upper: Some }` arm of `containing` — the
/// query path V2 also has to lift out of the opaque fallback.
fn bench_two_sided_range(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsumption/two_sided_range");
    for n in SIZES {
        let idx = index_two_sided_class(n);
        let mid = (n / 2) as i64;
        let query = [gt("id", mid - 5), lt("id", mid + 5)];
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(idx.candidates(black_box(&query))));
        });
    }
    group.finish();
}

/// Two-sided-range index build — `n` parents each with both bounds. V1's
/// `opaque` push is O(1); V2 will be 2x BTreeMap insert. Maintenance budget
/// check.
fn bench_two_sided_build(c: &mut Criterion) {
    let mut group = c.benchmark_group("subsumption/two_sided_build");
    for n in SIZES {
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, _| {
            b.iter(|| black_box(index_two_sided_class(black_box(n))));
        });
    }
    group.finish();
}

criterion_group!(
    benches,
    bench_candidates_selective,
    bench_candidates_midpoint,
    bench_build,
    bench_two_sided_point,
    bench_two_sided_range,
    bench_two_sided_build,
);
criterion_main!(benches);
