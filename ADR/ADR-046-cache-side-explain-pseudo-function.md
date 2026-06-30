# ADR-046: Cache-Side EXPLAIN via an Intercepted Pseudo-Function

## Status
Accepted

## Context
`EXPLAIN <query>` sent to the proxy is classified as a non-`SELECT` statement and forwarded to origin, so it returns the *origin's* plan. There was no way to see how a cached query is actually executed against the cache database — which matters because the cache-side statement is a rewrite of the client's query (a parameterized shape scanning the replicated source tables, or a `SELECT` from a `pgcache_mv.q_<fingerprint>` materialized table), not the SQL the client wrote. Operators need this to diagnose cache plans (index usage, MV vs source-row, join order) from a normal psql session.

The rewrite knowledge — fingerprint → serve shape, and the runtime MV-vs-source decision — lives in the Rust proxy, not in the cache PostgreSQL. So a real SQL function in the `pgcache_pgrx` extension cannot produce it without duplicating resolution/deparse/MV logic in the extension. Any solution must run inside the proxy. A further constraint: `EXPLAIN ANALYZE` executes the statement, and the source-row serve path stamps scanned rows into the generation garbage-collection tracker (ADR-044) — an introspection command must not pollute that tracker.

## Decision
Introduce a **proxy-intercepted pseudo-function**, `pgcache_explain(<target>[, <options>])`, recognized by name at parse time and never executed as a real function:

- **Interface.** The client runs `SELECT pgcache_explain('<sql>')` or `SELECT pgcache_explain('<fingerprint>')` (fingerprint as printed by `/status`), with an optional second string argument carrying verbatim `EXPLAIN` options (e.g. `'ANALYZE, FORMAT JSON'`). The result is a single-column `QUERY PLAN` text set — identical in shape to native `EXPLAIN` — so psql renders it normally. The chosen backend and the rewritten cache-side SQL are reported as a `NoticeResponse`, keeping them out of the result set.
- **Detection reuses the existing SQL→AST converter.** The interceptor converts the statement with the normal converter and inspects the safe AST for a bare `SELECT pgcache_explain(<string literal>[, <string literal>])`, rather than walking the raw parse tree. No new `unsafe` code is added.
- **Both arguments are string literals.** A first argument that parses as `u64` is a fingerprint; otherwise it is inline SQL. Fingerprints must be quoted.
- **Execution mirrors the serve path.** The statement is resolved to a cached query by fingerprint, the backend is chosen by the same runtime MV-vs-source decision used for serving, and the cache-side statement is run through the extended/parameterized protocol on a pooled cache connection — but **without** the generation-stamping prefix the normal serve issues.
- **A dedicated serve path.** Explain runs as a distinct job on the serve pool, separate from the hot cache-hit state machine, and synthesizes its own response.
- Always enabled; scoped to queries currently cached and Ready. A query that is absent or not Ready returns a single-row result stating its status rather than falling back to origin.

## Rationale
- **Unambiguous and non-disruptive.** An explicit pseudo-function makes clear the plan comes from the cache, and leaves the existing `EXPLAIN`→origin behavior untouched — unlike transparently intercepting `EXPLAIN`, whose target would silently depend on cache state.
- **The rewrite lives where the knowledge is.** Running in the proxy reuses the real serve-shape and MV-decision code; a `pgcache_pgrx` UDF could not, and would drift.
- **No stamping.** The explain connection resets `mem.query_generation` to 0 before running, which disables the row-stamping custom scan (ADR-044) — so even `EXPLAIN ANALYZE` cannot pollute the GC tracker. (The reset is explicit: the serve path sets that GUC with session scope, so a pooled connection would otherwise carry the last serve's generation.)
- **Fingerprint matching already pins the literals.** The query fingerprint hashes the query body including its literal values, so a cached entry's stored constants are exactly those of the matching query; the plan is faithful to the client's constants (the only plan-caching caveat is custom vs. generic plan).
- **Hot path stays pristine.** Keeping explain on a separate serve job avoids adding an introspection mode to the invariant-heavy cache-hit response machine.

## Consequences

### Positive
- Operators can inspect the real cache-side plan (MV table vs. parameterized source-row scan) from plain psql, including `EXPLAIN ANALYZE`.
- No new `unsafe`; detection rides the existing, tested AST converter.
- Existing `EXPLAIN`→origin semantics and the hot cache-hit serve path are unchanged.

### Negative
- Fingerprints must be quoted. An unquoted large integer would be parsed as a float and rounded, so it is deliberately not accepted; supporting it would require preserving big integers through the shared literal type, a change with wide blast radius across shape/deparse/subsumption for a diagnostic-only gain.
- Only queries already cached and Ready can be explained; arbitrary SQL is out of scope.
- The plan omits the per-request `LIMIT`/`OFFSET` (not part of the fingerprint), so it reflects the cached relation scan / full MV read rather than a specific client limit.
- The pseudo-function is invisible to catalog introspection and tab-completion, and does not exist if run directly against the cache database.

## Implementation Notes
The interceptor lives in the proxy query path and routes a recognized call to the cache subsystem over the existing leased-socket mechanism; a dedicated serve-pool job builds the `EXPLAIN`-wrapped cache-side SQL, runs it on a pooled connection, and synthesizes the `QUERY PLAN` response plus NOTICE. For an MV-backed query the plan shown is deliberately the source-table fallback (the query the MV materializes), not the trivial `pgcache_mv.q_<fp>` scan — the steady-state MV serve is already conveyed by the backend NOTICE. See PGC-345.
