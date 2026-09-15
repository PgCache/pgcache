# ADR-049: Predicate-disjointness for UPDATE/DELETE read-after-write

## Status
Accepted

## Context
ADR-048 gave per-connection read-after-write row-level precision for INSERT: an inserted row is enumerable, so a read provably disjoint from every inserted row is served from cache. UPDATE and DELETE stayed table-level opaque — any read of a table with a pending UPDATE/DELETE forwarded to origin. That is a large precision loss for the two most common write shapes in OLTP.

UPDATE and DELETE don't enumerate rows; they name an affected set by predicate (`WHERE …`), and the proxy has no catalog and no data. So the question shifts from INSERT's "does any inserted value match the read?" to **"can the write's predicate and the read's predicate co-occur?"** — predicate-vs-predicate reasoning. The grow/shrink asymmetry sharpens it: a DELETE only shrinks the result set; an UPDATE can move a row *into* a read's result (grow), the direction the cache can't cover.

## Decision
Unify all three write kinds under one question — **is the read's predicate disjoint from the write's affected predicate(s)?** — built on one primitive, per-column range disjointness `column_ranges_disjoint(a, b)`: two predicates are disjoint when some column constrained by both has non-overlapping ranges. It is sound in one direction only: it claims disjoint only when provable, so every uncertainty forwards.

- **DELETE** — served if disjoint from `where_pred` (shrink-only; one check).
- **UPDATE** — served if disjoint from *both* `where_pred` (no matched row leaves or changes value) *and* the post-update `image_pred` = `where_pred` with each `SET col = literal` overriding that column to `= value` (no row grows into the read). A non-literal `SET` leaves the column unconstrained in the image.
- **INSERT** — each inserted row is a point predicate `{col = value}`; folded onto the same primitive (served if disjoint from every row).

Extraction stays **resolution-free**: UPDATE/DELETE WHERE predicates and SET assignments come from the raw parse tree, single-column bare comparisons, AND-only. Anything else — OR, functions, casts, cross-column comparisons, subqueries, joins/`USING`/`FROM`, a whole-table statement (no WHERE), or predicate-cap overflow — degrades to table-level opaque. Every fallback widens scope, never misses a write.

## Rationale
- **One primitive defines the semantics; hot shapes specialize its storage.** Predicate disjointness (`column_ranges_disjoint`) generalizes INSERT's point check and remains the reference semantics for all three kinds. The dominant OLTP shapes — all-equality predicates and literal insert rows — are stored as shape-grouped flat tuples (column-major) rather than per-statement range maps, raising the precision cap ~16× for row-at-a-time workloads; their scan checks are hand-specialized equivalents of the primitive, held equivalent by tests. Non-mergeable shapes keep the map-based path.
- **Two checks capture UPDATE soundness.** Disjoint-from-WHERE rules out shrink and value-change of read-set rows; disjoint-from-image rules out grow. Together the read's result is provably unchanged, so serving is safe.
- **Resolution-free keeps the write side catalog-independent**, consistent with ADR-048's constraints; the cost is precision on non-trivial predicates, taken as opaque.
- **Provable-only disjointness** preserves ADR-048's conservative-in-the-unsafe-direction discipline — the gate never serves unless it can prove the read unaffected.

## Consequences

### Positive
- UPDATE and DELETE gain row-level precision; a read provably unaffected serves from cache instead of forwarding.
- The three write kinds share one disjointness path; per-kind metrics (`raw.serve_disjoint_{insert,delete,update}`) show which precision path earns its keep.
- No new soundness surface: disjointness is provable-only and extraction degrades to opaque.

### Negative
- Precision is single-column equality/range, AND-only, literal values (bind parameters are substituted to literals before the gate, so parameterized statements are covered); multi-column predicates and `IN`/`BETWEEN` are follow-ups.
- The UPDATE image approximates: a non-literal `SET` makes that column unconstrained, and check #1 forwards any read overlapping the WHERE even when the updated column is irrelevant to the read.
- Per-table caps bound the row-precise state, and overflow degrades the table to opaque: merged all-equality delete/update tuples (1024, counted together), non-mergeable predicate maps (64 statements), and inserted rows (1024 rows, additionally budgeted by total cells for wide column lists). Metrics record how often each cap bites, so they can be tuned against real workloads.
- The equality-shape checks duplicate the exclusion test instead of calling the shared primitive — the price of flat-tuple storage; equivalence rests on tests, not on a single code path.

## Implementation Notes
`column_ranges_disjoint` lives in the constraint range algebra and reuses the numeric-aware comparison from ADR-048's stale-read fix. UPDATE/DELETE classify on the raw tree in the write classifier (reusing the SELECT WHERE converter). The write log routes each write by shape: all-equality predicates become shape-grouped tuples (deletes keyed by sorted column list; updates by sorted WHERE columns plus sorted SET columns), everything else a per-statement range map checked through the shared primitive. This extends ADR-048; UPDATE/DELETE precision beyond single-column equality/range, `IN`/`BETWEEN`, and in-transaction serving remain out of scope.
