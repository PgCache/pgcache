# ADR-064: Opaque scalar operators

## Status
Accepted

## Context
The converter knew three operator families: the six comparisons and `LIKE` as `Binary` predicates, and five arithmetic symbols as a two-child `Arithmetic` scalar. Every other operator failed conversion with `UnsupportedOperator` and the statement forwarded, so a predicate over a `json`/`jsonb` column (`data ->> 'kind' = 'invoice'`, `data @> '{"tags": ["red"]}'`, `data ? 'k'`) was never cached, while `data - 'k'` passed only because the subtraction symbol happened to be in the arithmetic list (PGC-577). Supabase's JWT-claims policies and the session-term keying that follows them (PGC-580, PGC-113) need `->>` in the tree.

The `Arithmetic` node already had the shape these operators need: two scalar children, opaque to the predicate model (its conjunct is `Opaque`, so the reference is `unrecognized` and CDC evaluates every change to the table on the cache PG, ADR-059/ADR-027), passed through resolution, cacheability and deparse without interpretation. Only the literal folds read the operator as numeric. Two of the json family's symbols are not immutable in every overload: `text @@ text` reads `default_text_search_config` and `text || anynonarray` renders its operand under DateStyle/TimeZone, so a result computed on the cache PG could differ from origin's.

## Decision
- **One scalar operator kind, several families.** `NodeKind::Operator { op: ScalarOp }` replaces `Arithmetic`; `ScalarOp` is `Arithmetic(ArithmeticOp)` or `Json(JsonOp)`, the latter the thirteen symbols `->`, `->>`, `#>`, `#>>`, `@>`, `<@`, `?`, `?|`, `?&`, `@?`, `@@`, `||`, `#-`. A boolean-valued operator stands in WHERE as a scalar predicate, as a boolean function call does; nothing joins the `Binary` comparison family.
- **Opaque, by construction.** An operator conjunct is never a constraint: no filter, no exactness, no subsumption key. Correctness rests on the cache PG evaluating the same expression over the same rows; the cost is CDC work proportional to the queries over the table.
- **Volatility per operator, under the function rule.** An operator whose every `pg_catalog` overload is immutable is cacheable wherever it stands. `@@` and `||` are treated as STABLE: allowed in the SELECT list, refused elsewhere with `NonImmutableOperator`, which pgcache-fit reports by symbol.
- **Literal canonicalisation knows the operand types** (ADR-056 part C). A json-only operator fixes both operands to types whose text input no session setting changes, so a digit-bearing key such as `->> '2024'` is text, never a date-time literal. `@>`, `<@` and `||` also take arrays, ranges and (`||`) any scalar, so their literal operand is typed as json only beside a column of a settings-insensitive type (json, jsonb, bytea, ...), which reads as text there as a cast to it already did; beside any other column it is blind, since it is an element, a range or, for `timestamp || 'x'`, plain text rather than a value of the column's kind.
- **Unknown symbols still forward.** An operator outside every family keeps the `UnsupportedOperator` refusal; a family is added as a `ScalarOp` variant with its converter, cacheability and canonicalisation rules.

## Rationale
- **Generalising the existing kind over adding one** touched every exhaustive match once; the next family (PGC-581's candidates) is an enum variant. The alternative, a separate json node, would have repeated two dozen "recurse into both children" arms per family.
- **Scalar rather than `Binary`** keeps the json operators out of the comparison recognizer, which would otherwise read `col @> lit` as a column constraint unless guarded, and out of the comparison branch of literal canonicalisation.
- **The function rule for `@@` and `||`** is the rule stable functions already live under, so no new serving behaviour is introduced. Typing `@@` per operand (a jsonb side makes it immutable) needs catalog facts after resolution and belongs with function resolution (PGC-578).

## Consequences

### Positive
- Predicates over json columns cache and are maintained in place; two sessions on different date-time settings share the entry.
- `ARRAY[...]` converts in any scalar position (it was accepted only under `= ANY`), as `?|` and `?&` take a text array.
- A GIN index with a jsonb opclass passes through the cache-side index rewrite unchanged.

### Negative
- Every change to a table with such a query is relevant to it and costs a cache-PG membership evaluation; the CDC fast path does not apply.
- Such a query is never a subsumption parent and never covers a narrower one; `col ->> 'k' = 'v'` as a derived-column constraint is a follow-up.
- `@@` and `||` in WHERE forward even in their immutable overloads.

## Implementation Notes
- Types: `query/ast/types.rs` (`JsonOp`, `ScalarOp`); node and view: `query/ast/rope/` (`NodeKind::Operator`, `Operator<'t>`, `ScalarKind::Operator`).
- Converter: `convert_tree/scalar.rs` (`operator_emit`, `array_emit`), routed from `convert_tree/where_clause.rs` for WHERE positions. The arithmetic literal fold stays arithmetic's alone.
- Cacheability: `cache/query/cacheability.rs` (`operator_volatility_check`); canonicalisation: `query/transform/literal_canon.rs`; fit: `fit/src/classify.rs` (`NonImmutableOperator`).
- Tests: `tests/json_operator_test.rs`.
