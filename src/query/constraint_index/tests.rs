use super::classify::{Classification, classify, column_set_powerset};
use super::*;
use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::cast::CastTarget;
use crate::query::constraints::TableConstraint;
use crate::query::{Fingerprint, FingerprintSet};
use ecow::EcoString;
use ordered_float::NotNan;
use tokio_postgres::types::Type;

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
        replica_identity_full: false,
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
        idx.candidates(&[eq("id", float_lit(200.0))])
            .contains(&fp(1)),
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
    assert!(
        idx.candidates(&[eq("price", float_lit(50.0))])
            .contains(&fp(1))
    );
    assert!(
        !idx.candidates(&[eq("price", float_lit(5.0))])
            .contains(&fp(1))
    );

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
        "id" => [Some(ColumnRange::Equal(int(200))), None, None],
        _ => [Some(ColumnRange::Unknown), None, None],
    });
    assert!(got.contains(&fp(1)));
    assert!(got.contains(&fp(3)));
    assert!(
        !got.contains(&fp(2)),
        "id=999 must be excluded for a id=200 row"
    );
}

#[test]
fn point_probe_unknown_is_conservative() {
    let mut idx = ConstraintIndex::<Fingerprint>::new();
    idx.insert(fp(1), &[eq("id", int(200))]); // equality-pure bucket
    idx.insert(fp(2), &[gt("id", int(100))]); // complex bucket

    // An `Unknown` column (NULL / unchanged-TOAST) must return every entry
    // constraining it — both buckets — never drop one.
    let got = idx.candidates_point(|_| [Some(ColumnRange::Unknown), None, None]);
    assert!(
        got.contains(&fp(1)),
        "equality-pure entry must not be dropped under Unknown"
    );
    assert!(
        got.contains(&fp(2)),
        "complex entry must not be dropped under Unknown"
    );
}

#[test]
fn point_probe_partial_unknown_filters_known_columns() {
    let mut idx = ConstraintIndex::<Fingerprint>::new();
    // Two-column equality-pure entries on {id, region}.
    idx.insert(fp(1), &[eq("id", int(1)), eq("region", int(5))]);
    idx.insert(fp(2), &[eq("id", int(2)), eq("region", int(5))]);

    // region pinned to 5, id unknown: both match on the pinned column.
    let got = idx.candidates_point(|c| match c {
        "region" => [Some(ColumnRange::Equal(int(5))), None, None],
        _ => [Some(ColumnRange::Unknown), None, None],
    });
    assert!(got.contains(&fp(1)));
    assert!(got.contains(&fp(2)));

    // region pinned to a non-matching value excludes both.
    let none = idx.candidates_point(|c| match c {
        "region" => [Some(ColumnRange::Equal(int(9))), None, None],
        _ => [Some(ColumnRange::Unknown), None, None],
    });
    assert!(none.is_empty());
}

// `row_value_forms`: every keyable interpretation of the wire text.

fn has_str_form(forms: &ColumnForms, s: &str) -> bool {
    forms
        .iter()
        .flatten()
        .any(|r| matches!(r, ColumnRange::Equal(LiteralValue::String(v)) if v == s))
}
fn has_num_form(forms: &ColumnForms, x: f64) -> bool {
    forms.iter().flatten().any(
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
    assert!(
        !name
            .iter()
            .flatten()
            .any(|r| matches!(r, ColumnRange::Equal(LiteralValue::Float(_))))
    );

    // bool column "t" → String("t") plus Boolean(true).
    let active = row_value_forms(&t, &row, "active");
    assert!(has_str_form(&active, "t"));
    assert!(
        active
            .iter()
            .flatten()
            .any(|r| matches!(r, ColumnRange::Equal(LiteralValue::Boolean(true))))
    );

    // SQL NULL / absent column → [Unknown] (wildcard).
    let null_row = [None, bs("bob"), bs("f")];
    assert!(matches!(
        row_value_forms(&t, &null_row, "id"),
        [Some(ColumnRange::Unknown), None, None]
    ));
    assert!(matches!(
        row_value_forms(&t, &row, "nope"),
        [Some(ColumnRange::Unknown), None, None]
    ));

    // numeric-looking-but-textual: a non-numeric text yields only String.
    let bad_row = [bs("abc"), bs("bob"), bs("f")];
    let bad = row_value_forms(&t, &bad_row, "id");
    assert!(has_str_form(&bad, "abc"));
    assert!(
        !bad.iter()
            .flatten()
            .any(|r| matches!(r, ColumnRange::Equal(LiteralValue::Float(_))))
    );
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
    assert!(
        got.contains(&fp(1)),
        "String('200') entry found via the String form"
    );
    assert!(
        got.contains(&fp(2)),
        "Integer(200) entry found via the Float form"
    );
    assert!(
        !got.contains(&fp(3)),
        "String('7') entry must not match a '200' row"
    );
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
    assert!(
        got.contains(&fp(1)),
        "'42' > '10' lexicographically — must be a candidate"
    );

    // A row whose text is lexicographically below '10' must be excluded.
    let row_lo = [bs("09"), bs("alice"), bs("t")];
    let got_lo = idx.candidates_point(|c| row_value_forms(&t, &row_lo, c));
    assert!(!got_lo.contains(&fp(1)), "'09' < '10' lexicographically");
}
