//! Baseline benchmarks for `SubsumptionIndex` complex-bucket lookup (PGC-129).
//!
//! V0 stores complex (non-equality) parents in a per-class `Vec<u64>` that is
//! scanned linearly by `candidates()`. These benches exercise a complex-heavy
//! single column-set class so the V1 within-class index can be measured
//! against a recorded baseline.

#![allow(clippy::unwrap_used)]
#![allow(clippy::cast_possible_wrap)]

use std::hint::black_box;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use ecow::EcoString;

use pgcache_lib::cache::subsumption_index::SubsumptionIndex;
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

/// Build an index of `n` single-sided range parents, all in class `{id}`.
fn index_single_class(n: usize) -> SubsumptionIndex {
    let mut idx = SubsumptionIndex::new();
    for k in 0..n {
        idx.insert(k as u64, &[gt("id", k as i64)]);
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

criterion_group!(
    benches,
    bench_candidates_selective,
    bench_candidates_midpoint,
    bench_build
);
criterion_main!(benches);
