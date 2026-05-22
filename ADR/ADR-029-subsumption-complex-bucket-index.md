# ADR-029: Subsumption complex-bucket per-column index

## Status
Accepted

## Context
Predicate subsumption (ADR-024) checks, at query registration, whether a new
query's result set is already covered by cached data. The candidate lookup
that feeds this check is the `SubsumptionIndex`. Its V0 (PGC-119) partitions
queries per table by constraint-column set; within a class, equality-pure
queries hit an O(1) joint-tuple hash, but queries with any non-equality
constraint (Range/IN/cast) fall into a per-class `Vec` that is scanned
linearly at lookup. A workload with many overlapping non-equality
constraints on the same column set therefore re-exposes the O(N) writer
register cliff that V0 fixed for equality-heavy traffic.

The lookup is lossy-safe: returning extra candidates is rejected by the
caller's precise check, and missing a true candidate only costs an origin
populate. Neither direction is a correctness bug, so the index is free to be
a coarse filter. The task (PGC-129) named an Aguilera discrimination tree,
but that paper assumes point-vs-predicate matching, whereas subsumption is
range-vs-range containment — a poorer fit than the name suggests.

## Decision
Replace the per-class complex `Vec` with a `ComplexIndex`: one `ColumnIndex`
per class column, with `candidates()` intersecting their per-column matches.

- Each `ColumnIndex` partitions parents by the `ColumnRange` of their
  constraint on that column: `eq` and `inset` (inverted) hash maps,
  `range_lower` / `range_upper` `BTreeMap`s keyed by the bound, and an
  `opaque` linear-fallback `Vec`.
- The per-column `ColumnRange` is built by `column_range_build` — the same
  reduction the precise `table_constraints_subsumed` check runs — so the
  index and the check share one constraint vocabulary.
- A column's containment lookup is a hash probe or a `BTreeMap` range scan;
  cross-column results are intersected smallest-set-first. Single-column
  classes return the per-column set directly with no intersection pass.
- Only single-sided ranges go into the `BTreeMap`s. Two-sided ranges, casts,
  multi-constraint columns, and incomparable values route to `opaque`, which
  is always returned.
- The V0 equality joint-tuple hash is kept as a separate fast path, untouched.

## Rationale
- **Per-column intersection fits the problem.** A complex parent constrains
  exactly its class columns, so every parent is present in every column's
  index and the intersection is well-defined. No wildcards needed inside a
  class — the column-set powerset already handles them.
- **No discriminator to tune.** A `BTreeMap` is the ordered structure; the
  open question of static vs adaptive discriminator selection dissolves.
- **Maintenance stays cheap.** `BTreeMap` insert/remove is O(log N), repaid
  many times over by the per-lookup saving. Sorted `Vec`s were rejected for
  their O(N) insert.
- **Single-sided ranges are the realistic hot case.** Filters like `ts > $1`
  with many literal values are what re-expose the cliff; they become exact
  1-D dominance queries. Two-sided ranges are deferred to the `opaque`
  fallback rather than carrying a 2-D structure prematurely.
- **Keeping the equality bucket separate** preserves V0's O(1) win for the
  dominant equality-heavy workload with zero regression risk.
- **Sharing `column_range_build`** means the index and the precise check
  reduce constraints identically. A new constraint kind cannot leave the two
  silently inconsistent — both move together, enforced by the type system.

## Consequences

### Positive
- Selective complex lookups become O(1) in N (flat ~300 ns vs V0's linear
  scan); the writer-register cliff for complex-heavy traffic is closed.
- Output-bound lookups (large candidate sets) are no slower than V0.
- One constraint-reduction implementation shared with the precise check —
  no parallel vocabulary to keep consistent.
- `complex_fallback_total()` surfaces `opaque` pressure as a V2 trigger
  signal without adding a metric counter.

### Negative
- Index build is ~2x slower per insert (`BTreeMap` vs `Vec` push, plus the
  full `column_range_build` reduction).
- Two-sided ranges degrade to a linear `opaque` scan; a class dominated by
  them gets no speedup.
- More structure per class (five sub-indexes per column) than the V0 `Vec`.
- The index now depends on `constraints.rs` internals (`ColumnRange`,
  `column_range_build`) — a deliberate coupling, since the index and
  `table_constraints_subsumed` are two halves of one subsumption mechanism.

## Implementation Notes
Two-sided-range support (a real 2-D containment structure) and set-trie
indexing for `InSet` are left to a future V2, triggered when
`complex_fallback_total` grows relative to `complex_total`.
