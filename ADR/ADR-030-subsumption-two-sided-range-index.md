# ADR-030: Subsumption two-sided range index

## Status
Accepted

## Context
V1 (ADR-029) routes parents with a two-sided range constraint (`col > a AND
col < b`) into the per-column `opaque` linear-fallback `Vec`, and returns
the whole bucket on every lookup. A class dominated by `BETWEEN`-style
parents therefore re-exposes the linear cost V1 fixed for single-sided
ranges, and is the explicit V2 trigger PGC-189 was filed for.

The query at hand is **2-D dominance**: each parent is a point `(l, u)`;
given a query interval `[qlo, qhi]`, find parents with `l ≤ qlo ∧ u ≥ qhi`.
This includes point and `IN`-set queries collapsed onto a single interval.
The lossy-safe contract still applies — over- or under-returning is at
worst a performance loss.

## Decision
Add a new sub-index `range_both` to `ColumnIndex`:

```
range_both: BTreeMap<LitKey, Vec<(LitKey, u64)>>
```

Two-sided range parents are keyed by their lower bound, with the upper bound
stored inline alongside the fingerprint. Lookup walks the `l ≤ qlo` prefix
(`range(..=qlo)`) and filters each entry by `upper ≥ qhi` during the walk —
single-pass, no intersection materialization, no second map.

The `Placement` enum gains a `RangeBoth { lower, upper }` variant; insert /
remove route by `placement(&ColumnRange)` exactly as the V1 sub-indexes do.
A `range_both.is_empty()` guard in `extend_two_sided` keeps V1 single-sided
workloads from paying the new code path.

## Rationale
- **Inline-filter walk over true 2-D intersection.** Per parent the lower
  posting list carries `(upper, fp)`, so the upper-side check happens during
  the lower walk. One pass, one structure, no intersection-set
  materialization or second-map join.
- **Stdlib only.** A `BTreeMap` mirrors V1's single-sided pattern exactly,
  no new dependency, no custom tree. R-tree / priority-search-tree were
  ranked behind this — their better worst case isn't earned until a
  workload shows the inline-walk cost dominating.
- **Worst case is output-bounded.** When both `qlo` and `qhi` are
  non-selective, the prefix walk is linear — but so is the true candidate
  set. No structure can do better in that regime.
- **Additive to V1.** `range_lower` and `range_upper` are untouched; the V1
  selective lookup measurement (flat ~315 ns across N) is preserved.

## Consequences

### Positive
- Two-sided point and range lookups drop from O(N) to O(log N + window) on
  workloads with bounded result-set windows — measured ~6.4× at N=4096 with
  a window of 100 (38 µs → 6 µs).
- One structure for V2; no new dependency or custom tree.
- `range_both` removes the largest pre-V2 contributor to
  `complex_fallback_total`, sharpening it as a V3 signal.

### Negative
- `Placement` enum grew (`RangeBoth` carries two `LitKey`s, ~64 bytes), and
  `ColumnIndex` gained another `BTreeMap` field. The V1 single-sided
  midpoint bench regressed ~38% at N=4096 (17 µs → 23 µs) — a structural
  layout / cache effect, not function-call overhead (`#[inline]` and the
  empty-`range_both` early-out did not move it). The V1 selective workload
  (the V1 win) is unaffected. The regression sits on the output-bound
  stress case rather than the realistic selective one.
- Build for two-sided parents is ~1.4× V1 (`BTreeMap` insert vs `Vec`
  push). Repaid trivially by a single selective lookup.
- Inline-walk is single-direction (by lower bound). Asymmetric workloads
  where the upper bound would be more selective don't get the picking; a
  dual-map upgrade is the natural escape hatch if benched.

## Implementation Notes
The bound-arity split (single-sided → `range_lower`/`range_upper`;
two-sided → `range_both`; everything else → `opaque`) is now the live V2
boundary. The remaining items left for the linear `opaque` fallback are
casts, multi-constraint columns producing `Range` shapes the structured
buckets don't capture, and `Unknown`/`Empty` cases — exposed by
`complex_fallback_total()` as the V3 signal.
