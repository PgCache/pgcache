# Subsumption index walkthrough

Traces two cached queries through `ConstraintIndex` and then runs four new queries through the region probe and the precise check. Mirrored by `test_walkthrough_two_parents_four_queries` in `src/query/constraint_index/tests.rs`.

Decisions behind the structure: ADR-024 (subsumption rule), ADR-029 (per-column complex index), ADR-030 (two-sided ranges), ADR-037 (generalized index shared with CDC).

## Vocabulary

- **Parent**: an already-cached query whose data might cover a new one.
- **Subsumed**: the new query's result set is guaranteed to lie inside a parent's loaded rows. The parent's predicate region contains the new query's region, so the new query is at least as restrictive.
- **Class**: entries in the index are partitioned per table by `ColumnSet`, the sorted set of columns their constraints mention. Within a class, equality-pure entries sit in a joint-value-tuple hash; everything else goes to a `ComplexIndex` with one `ColumnIndex` per class column.
- **Region probe**: `ConstraintIndex::candidates`. Enumerates the powerset of the new query's constrained columns and probes each existing class. Lossy-safe: it may over-return, never wrongly claims coverage, and a missed candidate only costs an origin populate.
- **Precise check**: `table_constraints_subsumed` in `src/query/constraints/subsume.rs`. Reduces each side to per-column `ColumnRange`s and requires every column the parent constrains to contain the new query's range on that column.

## Setup

Table `orders`. Two parents already registered, both single-table, no LIMIT, Ready.

- Parent A: `WHERE tenant_id = 7`
- Parent B: `WHERE tenant_id = 7 AND created_at > 100`

**Insert A.** `classify` sees one plain equality: `EqualityPure { columns: {tenant_id}, values: [Num(7)] }`. Class `{tenant_id}` is created and A goes into its equality bucket.

**Insert B.** The `>` makes it `Complex { columns: {created_at, tenant_id} }` (sorted). `column_ranges` reduces per column: `created_at` to `Range { lower: 100 exclusive }`, `tenant_id` to `Equal(7)`. Class `{created_at, tenant_id}` is created; B is inserted into both of its `ColumnIndex`es, one bucket each.

Index state after both inserts:

```
classes:
  {tenant_id}
    equality: { [Num(7)] -> [A] }
    complex:  (empty)
  {created_at, tenant_id}
    equality: (empty)
    complex:
      per_column[0] created_at:  range_lower { Num(100) -> [B] }
      per_column[1] tenant_id:   eq          { Num(7)   -> [B] }
```

## Query 1: `WHERE tenant_id = 7 AND status = 'open'`

1. Classify: `EqualityPure`, columns `{status, tenant_id}`, values `[Str("open"), Num(7)]`.
2. Powerset: `{}`, `{status}`, `{tenant_id}`, `{status, tenant_id}`. Only `{tenant_id}` exists as a class.
3. Probe `{tenant_id}`: `column_ranges` on the subset gives `[Equal(7)]`, all equality, so the key is `[Num(7)]`. Equality hash hit: A. The class's complex index is empty.
4. Candidates: `{A}`.
5. Precise check: A constrains only `tenant_id`; cached `Equal(7)` against new `Equal(7)` passes. Gates (no LIMIT, Ready, single relation) pass.

**Subsumed by A.** Query 1 is generation-stamped in the cache DB and served with no origin round-trip.

B was never consulted, correctly: its class constrains `created_at`, which Query 1 leaves open, so B cannot contain it. The class partition rejects it before any value is compared.

## Query 2: `WHERE tenant_id = 7 AND created_at > 150`

1. Classify: `Complex`, columns `{created_at, tenant_id}`. Only the column set is used from here; the equality probe is decided per subset below.
2. Powerset: `{}`, `{created_at}`, `{tenant_id}`, `{created_at, tenant_id}`. Two classes exist.
3. Probe `{tenant_id}`: `column_ranges` on the subset gives `[Equal(7)]`, so the equality key is `[Num(7)]`. Hash hit: A. The class's complex index is empty.
4. Probe `{created_at, tenant_id}`: ranges `[Range { lower: 150 }, Equal(7)]`. Not all equality, so no equality key; the complex probe runs.
   - `created_at` column: `range_lower.range(..=150)` finds key 100, giving `[B]`.
   - `tenant_id` column: `eq[Num(7)]` gives `[B]`.
   - Intersect smallest-first: `{B}`.
5. Candidates: `{A, B}`.
6. Precise check on A: A constrains only `tenant_id`; cached `Equal(7)` against new `Equal(7)` passes. A covers every `created_at`, so the new range is inside it.
7. Precise check on B: `created_at` cached `Range { lower: 100 }` against new `Range { lower: 150 }`; the new bound is at least as tight, pass. `tenant_id` equal, pass.

**Subsumed** (by A or B; the first passing candidate wins). The equality probe at step 3 keys on the new query's ranges over the subset's columns, not on whether the new query is equality-pure overall, so a broad equality parent is reachable from any narrower query that keeps the same equality (PGC-412).

## Query 3: `WHERE tenant_id = 7 AND created_at > 50`

Same walk as Query 2 until the `created_at` column probe. `range_lower.range(..=50)` finds nothing, because B's bound of 100 is above 50. That column's match set is empty, so the intersection is empty. A is still found via `{tenant_id}`.

**Candidates `{A}`, subsumed by A.** Correct: B could not cover it, since rows with `created_at` between 50 and 100 may be absent from B's load, and the index excluded B on the column alone. A loaded every row for tenant 7, so it does cover the query.

## Query 4: `WHERE tenant_id = 7` with only B registered

Powerset: `{}` and `{tenant_id}`. Neither exists as a class, since B lives in `{created_at, tenant_id}`. No candidates.

**Not subsumed.** Correct: B is narrower than the new query. The subset-class rule rejects it purely on column sets, before any value is compared.

## What to take from this

- The class partition does the coarse "could this parent be broader" cut on column sets alone. A parent constraining a column the new query leaves open can never cover it, and that shows up as its class not being a subset.
- The value buckets only refine within a class. Hash for equality and IN, ordered prefix or suffix walks for one-sided bounds, lower-bound walk with an inline upper filter for two-sided bounds, and an always-returned `opaque` list for anything unkeyable.
- The index is a pre-filter. The precise check is the decision, and the gates on the parent (no LIMIT, Ready, single relation) are checked per candidate at lookup time, not at insert.
