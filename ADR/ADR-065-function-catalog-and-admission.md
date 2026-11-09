# ADR-065: Function catalog, resolution and admission

## Status
Accepted

## Context
A query's function calls were judged by one map from bare `proname` to the worst volatility over every overload in every schema, loaded once at startup by the proxy and once by the writer and never refreshed. Three gaps followed (PGC-578). A qualified call outside `pg_catalog` was refused outright, a user overload of a builtin's name dragged the builtin down for every session, and a function created after startup was unknown for the life of the process. Nothing told the cache that `CREATE OR REPLACE FUNCTION` had changed a body an entry was computed with: the schema-change signal (ADR-058) traced `function` objects as untracked. And the admission rule allowed any function in the SELECT list although the cache database holds only PostgreSQL's own functions, so a user function registered and then failed on every hit, poisoning a serve connection each time (PGC-474); in WHERE it would also have failed every CDC membership evaluation (ADR-027).

Function body inlining (PGC-579) and session-term keying (PGC-580), which the JWT work needs, both resolve calls to `pg_proc` rows and need a change signal to evict what they build.

## Decision
- **One shared function catalog.** `FunctionCatalog` holds every `pg_proc` row by schema, name and arity (defaults and variadics included), with volatility, kind, language, return type, an identity (`schema.name(argtypes)`) and a digest of the body. The proxy loads it at startup; the writer reloads it at its start and on every function signal and publishes it through an `ArcSwap` both read. The parser's own forms (`coalesce`, `nullif`, `greatest`, `least`) have no row and count as immutable builtins.
- **Two stages of verdict.** The proxy, which does not know the session's search_path, judges a call by its builtin candidates (the worst of them), or by the best case over its other candidates when no builtin fits, and memoises the verdict per statement text; a catalog change drops every memoised verdict. The writer resolves each call at registration on the session's search_path, `pg_catalog` implicitly first, applies the volatility rule to the worst case over what the path could pick, and records the resolved oids on the entry.
- **Admission is what the cache database can run.** A builtin (oid below 16384) is always admissible. Any other function is admissible only when the cache database holds the same identity with the same body digest, checked once per oid on the writer's cache connection and remembered until the next function signal; an absence is asked again every time, since an operator may install the function at any moment. A call nothing fits, one a function the cache lacks could answer, or one the path resolves to a non-immutable overload outside the SELECT list refuses registration and forwards the read, counted by `pgcache.queries.function_forwards{reason}`.
- **The signal evicts and reloads.** A `function`, `procedure` or `aggregate` object in a `ddl_command_end` or `sql_drop` payload names its oid; every entry whose recorded oids include it is evicted, the catalog is reloaded and published, the presence cache and the session-key aliases are cleared. `CREATE OR REPLACE` keeps the oid and `DROP` plus `CREATE` changes it; both evict. A function created beside a cached call has an oid no entry recorded; the alias clear covers it: every session resolves afresh, and one whose path now reaches the new function is refused at resolution while the others keep the entry.
- **Candidates, not a pick.** Without argument types the overload PostgreSQL would choose is unknown, so every arity-compatible candidate on the path counts: all must be admissible and the worst volatility decides. A path that lists `pg_catalog` after another schema already forwards every read (`BuiltinsShadowed`), so the writer only ever sees paths where a user overload could win on argument types alone.

## Rationale
- **Builtins first at the proxy** keeps the common refusals (`now()` in WHERE) memoised at the proxy rather than paid as a writer round trip per execution; only a call that a user function alone could answer travels to the writer, which is the case that needs the cache database consulted.
- **Presence by identity and body digest** rather than mirroring function definitions onto the cache database: a mirrored body that reads a table would see the cache's partial tables and return wrong rows silently, C functions cannot be mirrored, and `IMMUTABLE` is a promise PostgreSQL does not check. An operator who installs a function on both databases has made that call deliberately.
- **Evict and reload on the signal** is the existing correctness path of ADR-058 applied to one more object kind; the whole catalog is small (a few thousand rows) and the signal is rare.
- **The catalog shared through `ArcSwap`** is the `ActiveRelations` and `RlsRelations` pattern; a pointer comparison in the proxy's memo tells it when a verdict may be wrong rather than merely stale.

## Consequences

### Positive
- `pg_catalog.lower(x)` and the parser's own forms are cacheable in WHERE; a user function in a SELECT list forwards cleanly instead of failing every hit.
- A function installed on both databases is cached, and its redefinition on origin evicts the entries within the stream's order.
- PGC-579 inlines from the same rows and records the same oids; PGC-580 judges session-term expressions by the same volatility.

### Negative
- A call that only a user function could answer costs a writer round trip and a cache-database lookup per execution while the function is absent there; the proxy cannot memoise that refusal.
- Every arity-compatible overload counts, so a user overload of a builtin's name on the path forwards the session's reads even when PostgreSQL would pick the builtin.
- A function installed on the cache database after a refusal is admitted only when asked again; a presence is forgotten on any function signal and re-established on the next registration.

## Implementation Notes
- Catalog: `catalog/function.rs` (`FunctionCatalog`, `FunctionRow`, `FunctionFacts`, `PathFunctions`, `SharedFunctionCatalog`); the parser's forms in `convert_tree/scalar.rs` (`SQL_FORM_FUNCTIONS`); `FunctionOid` in `oid.rs`.
- Proxy stage: `cache/query/cacheability.rs` over `FunctionFacts`; the verdict drop on a catalog change in `proxy/cacheability_store.rs` (`catalog_reconcile`).
- Writer stage: `writer/registration/resolve.rs` (`functions_resolve`, `function_on_cache`); the oids on `CachedQuery::function_oids`; eviction in `writer/eviction.rs` (`cache_function_invalidate`); the signal in `cdc/schema_change.rs` and `writer/cdc/dispatch.rs`; the reload in `writer/core.rs` (`function_catalog_reload`).
- pgcache-fit judges by the same catalog from an embedded snapshot (`fit/data/functions.tsv`, regenerated by `fit/data/functions_regenerate.sh`).
- Tests: `tests/function_resolution_test.rs`.
