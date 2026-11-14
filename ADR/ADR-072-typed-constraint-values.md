# ADR-072: Constraints hold their column's values, not literals

## Status
Accepted

## Context
Range reasoning (subsumption, the constraint index, the read-after-write check) compared a predicate's literals as they were written. Two strings were different values when their bytes differed, and only bare integers and floats ordered. Both rules are wrong for most column types: `'10.50'` and `'10.5'` are one `numeric`, `'a '` and `'a'` one `char(n)`, uuid hex is case-insensitive, and a date or timestamp bound is always a string.

The false inequality was a correctness defect: the read-after-write check proved a pending write disjoint from a cached read of the very row it changed, and served the stale row. The missing order was a hit-rate defect: date, timestamp and string-spelled numeric ranges never subsumed. ADR-071 closed the first by tagging such literals with their column's type so they compared with nothing, at the cost of forwarding; it also added an ordered-text literal form for byte-ordered columns.

A typed comparison already existed for CDC row evaluation, parsing both sides per comparison. The alternative to a value type was to canonicalise the literal's text and re-parse inside every range comparison, which puts timestamp and decimal parsing on the admission path, per candidate.

## Decision
- **A constraint holds a `ConstraintValue`.** Where a read's constraint is formed, each literal is parsed once into its column's value in one canonical form per value (exact integer or decimal, date, timestamp, instant, boolean, normalised uuid, trimmed `bpchar`, text). Ranges, sets and bounds are built over these.
- **Three cases, kept apart.** A *typed* value compares by value and orders where its column's values do (ADR-071's text order included). An *opaque* value, a literal that does not parse as its column's type or whose type the comparator does not read, decides nothing, and no range is built over it. A *raw* value is a literal kept as written: a write's, whose statement is classified without a catalog, and a value in a cast's domain.
- **A typed value reads a raw one as a value of its own column.** That is how a read meets a write: the write's bare literal is parsed under the read's column type at the comparison.
- **The constraint index keys from the same value.** The separate index-key copy of every range is removed; a typed value yields its key directly.
- **A constraint carries to an equal column only when both compare alike** (same kind, same text order), since its value is its own column's.

## Rationale
- **Spelling comparison becomes unrepresentable.** Structural equality of typed values is value equality, so the existing set and equality logic is correct without a parallel "semantic" path.
- **Parse once.** The cost sits at constraint formation, not in each of the many range comparisons a registration or a gated read performs.
- **One vocabulary.** ADR-071's ordered text and type tags were two special literal forms for the same idea; a typed value subsumes both and takes them out of the AST's literal type.
- **Undecided is explicit.** Opaque values replace the previous habit of comparing whatever bytes were there.

## Consequences

### Positive
- A read after a write to the same value in another spelling is forwarded; after a write to a different value it is served from cache, by proof on values.
- Date, timestamp, string-spelled numeric and mixed integer/decimal ranges subsume, merge and detect contradictions.
- One set of ranges per reference instead of two; the index and the precise check cannot drift.

### Negative
- A read's typed value meeting a write's literal parses that literal at the comparison; a gated read does this per pending write on a constrained column.
- A constraint no longer carries across an equality between columns of different kinds (an integer and a numeric column), which loses some propagation.
- Values in a cast's domain are still compared as written.

## Implementation Notes
`query/typed_compare.rs` holds `ColumnValue` (the canonical parsed value); `query/constraints/value.rs` holds `ConstraintValue` and the comparison rules; `query/predicates/conjunct.rs` is the one place a read's literal becomes a value. `tests/read_after_write_test.rs` and `tests/range_subsumption_test.rs` cover both defects.
