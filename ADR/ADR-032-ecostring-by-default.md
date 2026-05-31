# ADR-032: EcoString by Default for Owned String Data

## Status
Accepted

Amends ADR-013. Migration completed under the EcoString-standardization epic (PGC-221).

## Context
[ADR-013](ADR-013-ecostring-identifier-types.md) converted the resolved-AST and catalog *identifier* fields to `ecow::EcoString` and demonstrated large wins on the resolve/pushdown paths. It deliberately scoped the conversion narrowly — to the fields it could justify on allocation grounds — and left an explicit "Not converted" list (`LiteralValue` string variants, `LiteralValue::Parameter`, error fields).

The result, two cycles later, is a codebase split roughly two-thirds `EcoString` / one-third `String` with no stated rule for which to reach for. New code picks inconsistently, `String`↔`EcoString` boundary conversions (`.as_str()`, `.into()`) accrete at the seams, and reviewers have no principle to apply. The cost is maintenance friction, not allocation — `mimalloc` is already the global allocator (`main.rs:22`), so for most of the remaining fields the allocation delta is negligible (see PGC-197/PGC-210).

The decision this ADR records is therefore a *convention* chosen for consistency, with allocation as a secondary benefit where it applies.

## Decision
**`EcoString` is the default type for owned, immutable string data. Use `String` only with a concrete, documented justification.**

Justified reasons to keep `String` (the exhaustive exception list):

1. **Mutable / incrementally-built buffers** — anything written into via `push_str`, `write!`, `format!`-accumulation, or held as a reusable scratch buffer. `EcoString`'s CoW mutation is the wrong tool. (e.g. `Deparse` `&mut String` accumulators, `cache_connection::sql_buf`.)
2. **SQL / payload text of unbounded length** — query bodies and generated SQL fragments, which are large and not clone-hot. (e.g. `PreparedStatement::sql`, `InsertStatement::{prefix,suffix}`.)
3. **Error / log message fields** — arbitrary human-readable text, off every hot path. (e.g. `result::Report` message, `*Error` variant fields.)
4. **serde DTO boundaries** — fields serialized/deserialized at an external boundary where `EcoString` adds an `ecow` serde-feature dependency for no benefit. (e.g. the `/status` API DTOs.)
5. **Foreign boundaries** — values that arrive as `String` from `tokio_postgres`, `pg_query`, or `format!` and are consumed immediately without being stored or cloned; convert with `.into()` only when retained.

Any `String` field that does not fall under one of these gets converted to `EcoString`. When keeping a `String`, the reason should be evident from context or stated in a brief comment.

### Amendment to ADR-013
ADR-013's "Not converted" list is superseded for the following, now converted under reason-by-default:

- **`LiteralValue::String` / `StringWithCast` value field / `Parameter`** — converted (PGC-111). ADR-013 declined these on hot-path allocation grounds; the consistency rule overrides that, and the param-replace AST clone makes the refcount-clone behaviour a real (if secondary) win.

ADR-013's identifier conversions and its remaining carve-outs (mutable buffers, library boundaries) stand unchanged and are subsumed by reasons 1 and 5 above.

## Consequences

### Positive
- One rule a reviewer can apply: `String` must justify itself, `EcoString` needs no defense.
- Fewer `.as_str()` / `.into()` boundary conversions as adjacent fields converge on one type.
- Incidental allocation/clone wins on any field that turns out to be clone-hot, at no extra design cost.

### Negative
- `HashMap<EcoString, _>` lookups still take `.as_str()` keys — `EcoString` does not implement `Borrow<String>` (carried over from ADR-013).
- A bounded migration across ~40 field sites plus the `func_volatility` map's threading; sequenced as per-subsystem PRs to stay reviewable.
- The convention is a default, not a law — the exception list above is load-bearing and must be kept honest, or `String` creeps back in unjustified.
