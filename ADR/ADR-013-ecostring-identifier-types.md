# ADR-013: EcoString for Resolved AST Identifier Types

## Status
Accepted

> **Amended by [ADR-032](ADR-032-ecostring-by-default.md).** ADR-032 adopts EcoString-by-default codebase-wide and reverses the "Not converted" decision below for `LiteralValue` string variants and `Parameter` (now converted on consistency grounds). The identifier conversions and the buffer/boundary carve-outs in this ADR still stand.

## Context
`ResolvedColumnNode` is the most frequently cloned type in the query resolution path. A 65-column UNION query across 7 tables produces 65+ column nodes, each cloned multiple times through scope building, tree transformations, and the two full-query clones in the transform pass. Every clone allocates heap memory and copies string content for schema, table, column, and type name fields — ~7 strings per column node across `ResolvedColumnNode` and its embedded `ColumnMetadata`.

Identifier strings in practice are short:

| Category | Examples | Bytes |
|---|---|---|
| Schema names | `public` | 6 |
| Table names | `users`, `orders` | 5–10 |
| Column names | `id`, `email`, `created_at` | 2–15 |
| Table aliases | `u`, `t1` | 1–3 |
| Short type names | `text`, `integer`, `boolean` | 4–7 |
| Long type names | `character varying`, `timestamp with time zone` | 18–24 |

The vast majority fit in 15 bytes. The outliers are PostgreSQL's verbose type names, which appear only in `ColumnMetadata.type_name` and `cache_type_name`.

Four SSO crate candidates were considered and eliminated: `smol_str` (archived Nov 2025), `smartstring` (last release 2022, MPL license), `flexstr` (immutable, low adoption), `kstring` (MSRV 1.93, designed for map keys). The two viable options were `compact_str` and `ecow::EcoString`:

| | `compact_str` | `ecow` |
|---|---|---|
| Stack size | 24 bytes | 16 bytes |
| Inline capacity | 24 bytes | 15 bytes |
| Strings ≤15 bytes | inline memcpy | inline memcpy |
| Strings 16–24 bytes | inline memcpy | heap, O(1) refcount clone |
| Struct size vs `String` | unchanged | −8 bytes per field |
| `Option<T>` size | 24 bytes | 16 bytes (niche) |
| Clone (heap) | deep copy O(n) | atomic refcount O(1) |
| Mutation | full, drop-in `String` | CoW — deep copy if shared |

`compact_str` inlines everything including `"timestamp with time zone"` (24 bytes), but has the same 24-byte stack footprint as `String`, providing no struct size reduction. `ecow` is 16 bytes, covering all identifier strings inline and handling long type names via atomic reference-counted sharing.

## Decision
Use `ecow::EcoString` for all identifier fields in the resolved AST and catalog:

- `ColumnMetadata`: `name`, `type_name`, `cache_type_name`
- `ResolvedColumnNode`: `schema`, `table`, `table_alias`, `column`
- `ResolvedTableNode`: `schema`, `name`, `alias`
- `ResolvedSelectColumn`: `alias`
- `ResolvedColumnExpr::Function` and `ResolvedWhereExpr::Function`: `name`
- `ResolvedColumnExpr::Identifier`: payload
- `TableMetadata`: `name`, `schema`

**Not converted:**

- AST types (`ColumnNode`, `TableNode`, `FunctionCall`, etc.) — built once during parsing and discarded at resolution. The pg_query C library call dominates parse time (~955 µs); saving allocations at AST construction saves ~1–2 µs, a rounding error.
- `LiteralValue` string variants — hold arbitrary user-provided SQL string literals of unbounded length with no clone pressure on the hot path.
- Error fields (`AstError`, `ResolveError`) — not on any hot path.
- `LiteralValue::Parameter` — `$1`/`$2` are short but created via `format!` which always returns `String`; minimal clone pressure.

## Rationale

**Struct size reduction**: Each converted field shrinks from 24 to 16 bytes. `ResolvedColumnNode` drops from ~224 to ~160 bytes; `ColumnMetadata` from ~96 to ~72 bytes. In a query with 50 column nodes, this is ~3.2 KB less live data, improving CPU cache utilisation across the full resolved tree.

**Clone cost for inline strings is a fixed 16-byte memcpy**: All schema, table, column, alias, and function name strings fit within 15 bytes and are stored inline. Clone never touches the heap for these values.

**O(1) refcounted sharing for long type names**: `"timestamp with time zone"` (24 bytes) and `"character varying"` (18 bytes) exceed the inline capacity but are allocated once at catalog load time. Every `ResolvedColumnNode` cloned from that catalog entry shares the same heap buffer — clone is an atomic refcount increment rather than a per-clone memcpy.

**`TableMetadata` as `EcoString` eliminates conversions at resolution time**: With the catalog using `EcoString` natively, constructing `ResolvedColumnNode` and `ResolvedTableNode` from catalog data is a direct `.clone()` rather than `.as_str().into()`.

## Benchmarks

Measured on the 65-column UNION query across 7 tables used as the benchmark fixture. Parse is dominated by the pg_query C library call and sees minimal benefit. Resolve and pushdown are the heavy clone paths.

**Phase 1** — `ColumnMetadata` + `ResolvedColumnNode`:

| Benchmark | `String` baseline | ecow | Change |
|---|---|---|---|
| parse | ~955 µs | ~889 µs | −5.4% |
| resolve | ~92 µs | ~48.5 µs | **−48.2%** |
| pushdown | ~74 µs | ~25.9 µs | **−65.5%** |

**Phase 2** — all remaining identifier types (including `ResolvedTableNode`, `ResolvedSelectColumn::alias`, `ResolvedColumnExpr::Function/Identifier`, `ResolvedWhereExpr::Function`, `TableMetadata`):

| Benchmark | Phase 1 | Phase 2 (full) | Change | vs `String` baseline |
|---|---|---|---|---|
| parse | ~889 µs | ~869 µs | within noise | −9.0% |
| resolve | ~48.5 µs | ~40.5 µs | **−16.1%** | **−56.0%** |
| pushdown | ~25.9 µs | ~18.0 µs | **−26.7%** | **−75.7%** |

`ResolvedTableNode` alone (7 nodes × 3 fields = 21 strings) showed no measurable gain in isolation — dominated by the 65+ column nodes already converted. The additional Phase 2 gains come primarily from `ResolvedSelectColumn::alias` (cloned once per output column, 65+ times per query clone) and eliminating the `.as_str().into()` intermediate conversion at `ResolvedColumnNode` construction sites.

## Consequences

### Positive
- Resolve and pushdown latency reduced by 56% and 76% vs `String` baseline
- Smaller resolved tree improves CPU cache utilisation
- Long type names share a single heap buffer across all column nodes — no per-clone memcpy
- `Option<EcoString>` is 16 bytes due to niche optimisation

### Negative
- `HashMap<String, _>` lookups require `.as_str()` rather than passing `&EcoString` directly — `EcoString` does not satisfy `Borrow<String>`
- Mutation uses CoW semantics — unsuitable for output buffers or incremental string building; those sites remain `String`
- At library boundaries (`tokio_postgres` row results, `format!`), values arrive as `String` and require `.into()` at the construction site
- `compact_str` would have inlined everything with no atomics, but provides no struct size benefit

## Implementation Notes
The `Deparse` trait was extended with `impl Deparse for EcoString` delegating to `self.as_str().deparse(buf)`. At `String`-consuming boundaries (`HashSet<String>`, error variant fields), call `.to_string()`. At construction from `&str` or `String`, use `.into()`. No changes are needed for consumers that read via `.as_str()` or `Deref<Target=str>`.
