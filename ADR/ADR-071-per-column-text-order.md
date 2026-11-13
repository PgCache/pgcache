# ADR-071: Per-column text order for local comparison

## Status
Accepted

## Context
ADR-068 made the cache PostgreSQL order text as origin does. The Rust side has no collator, yet three places decided text order from bytes: the classifier kept `column < 'literal'` on a text column local, the row comparator answered it with a byte comparison, and the constraint index placed a text bound in a byte-ordered map, so the row probe never offered the query. Under a linguistic collation (`'a' < 'B'`, the reverse of bytes) a CDC row inside a cached range was dropped and the query served without it. Subsumption was safe only because it refused string ordering wholesale (PGC-446), which also denied it to columns where bytes are the order.

Whether bytes decide is a property of the column's collation, not of its type or of the database: a `COLLATE "C"` column can sit in an `en_US` database and the reverse. The catalog carried no collation, and cache tables were created under the database default whatever the column declared, so a served `ORDER BY` on such a column was wrong too.

Alternatives considered: an in-process collator (`strcoll_l` or ICU from Rust), which reintroduces the library-fidelity problem ADR-068 exists to avoid and cannot be proven by a sample; and having the cache PostgreSQL rank literal bounds at registration, which is correct but adds a round trip and a maintained structure and does nothing for row evaluation.

## Decision
- **The catalog classifies each column's text order.** `Bytes` (libc `C`/`POSIX`, the builtin provider), `Linguistic` (any other deterministic collation) or `Nondeterministic`, read from `pg_collation` with the table's columns; a column that declares none takes the database default's. The rule mirrors PostgreSQL's own collate-is-C test, so a libc `C.UTF-8` stays linguistic.
- **The comparator reads it.** Byte-ordered text compares locally under every operator; linguistic text only tells equal from unequal, so an ordering predicate is evaluated by the cache PostgreSQL; nondeterministic text decides nothing locally.
- **Orderability travels in the literal.** Predicate derivation rewrites a string bound on a byte-ordered column to an ordered-text literal form; only that form orders in range reduction, subsumption and the read-after-write check, and only that form gets an ordered index bucket. A plain string bound is equality-only and its range is opaque to the index. A value comparison on a nondeterministic column is no constraint at all.
- **A string compared to a non-text column carries the column's type.** `'10.50'` and `'10.5'` are one `numeric`, `'a '` and `'a'` one `char(n)`; such a literal compares only with another of its type, never by bytes with the bare string a write carries, so the read-after-write check forwards instead of proving the two different.
- **Cache columns are created under the declared collation.** A collation the cache database lacks, or one that sorts the ADR-068 sample differently from origin, makes the table uncacheable and its queries forward.

## Rationale
- **A wrong order is a stale read, so undecided is the default.** Everything unknown (an expression's collation, an analysis build with no origin) is treated as linguistic: equality by bytes, order left to the cache PostgreSQL.
- **One place applies the rule.** Every read-side constraint is formed at one point in predicate derivation; ranges, index keys and row matches all descend from it, so they cannot disagree on what bytes decide.
- **The value form makes misuse unrepresentable.** A side flag on ranges would have to be threaded through every comparison and could be paired with the wrong range silently; a distinct literal form cannot order unless it was derived for a byte-ordered column.
- **The cache PostgreSQL already has the right order.** Routing linguistic ordering to it reuses the batched evaluation path rather than adding a collator.

## Consequences

### Positive
- A text range or `BETWEEN` on a linguistically collated column is maintained correctly by CDC.
- Byte-ordered columns (including `name`, and any `COLLATE "C"` column) gain text range subsumption, range merging and read-after-write disjointness.
- A column collated differently from its database is ordered correctly when served.
- Nondeterministic collations stop being a documented exclusion: equality on them is no longer assumed from bytes.

### Negative
- On `en_US` or ICU origins, ordering predicates on text now cost a cache-PostgreSQL evaluation per CDC batch, and still never subsume.
- A table with a user-created collation is not cached; collations are not copied to the cache database.
- Each Relation message costs one more origin catalog read, since pgoutput carries no collation.
- `bpchar` bounds stay equality-only even when byte-ordered, because trailing spaces are not significant.

## Implementation Notes
`catalog.rs` holds `TextOrder` and `ColumnCollation`; `query/typed_compare.rs` the column-aware comparator; `query/predicates/conjunct.rs` the single rewrite point; `query/constraint_index/value_key.rs` the bound placement rule. `tests/database_locale_test.rs` covers the linguistic range, a byte-ordered column inside an ICU database, and an unavailable collation.
