# ADR-040: Shape-Keyed Prepared-Statement Serving

## Status
Accepted

## Context
pgcache registers a separate cached query per distinct post-bind query — each literal variation (`WHERE id = 1`, `WHERE id = 2`, …) is its own registered query keyed by its own `Fingerprint`. This per-literal model is deliberate (it is how the cache keys results), but it drove the serve path to generate fresh ad-hoc SQL on every cache hit and to prepare a separate cache-DB statement per fingerprint. Two costs followed.

First, each per-literal statement is a distinct prepared plan on the cache-DB connection. A high-cardinality predicate column produces a near-unbounded stream of one-shot plans, and the cache-DB connection's per-fingerprint statement registry had to actively reconcile itself against cache eviction to keep that working set bounded. The plan churn also provoked relcache-invalidation storms on the cache DB — observed under high-cardinality serving, not merely predicted.

Second, even though queries differing only in their literals share an identical plan, the serve path could not reuse a prepared statement across them, because the statement was keyed on the per-literal fingerprint.

The forcing observation: the plan-explosion driver is *literal cardinality in predicate positions*, but those literals always sit beside a typed column or operator, so they can be lifted into bind parameters without losing type information. If literals are parameterized, all queries that differ only in their literals collapse to one prepared statement — bounding the cache-DB statement working set by query-shape diversity (tens to low hundreds) instead of by literal cardinality.

## Decision
Serve source-row cache hits through prepared statements keyed by query **shape** — the parameterized form of a resolved query — rather than generating ad-hoc SQL per serve.

- **Shape abstraction.** A `QueryShape` (`src/query/shape.rs`) is a resolved query's parameterized form: the prepared-statement SQL with `$1..$k` placeholders, a `ShapeKey` (a hash of that SQL), and the literals replaced by the placeholders in bind order. Two queries that differ only in their literal values produce identical shape SQL and the same `ShapeKey`. The shape is derived from the schema-qualified `ResolvedQueryExpr` — the same form the serve SQL is deparsed from.
- **Additive, not a re-key.** `ShapeKey` is a separate type from `Fingerprint` so the two cannot be confused: the per-literal `Fingerprint` still keys the cache model (results, metrics, MV state), and the `ShapeKey` keys only the serve-side prepared statement. The per-literal registration model is unchanged.
- **Predicate-only parameterization.** The `resolved_query_expr_parameterize` transform (`src/query/transform/parameters/resolved_parameterize.rs`) lifts literals to `$N` only in predicate positions — `WHERE` / `HAVING` / `JOIN-ON`, and the predicates of nested subqueries. SELECT target list, `ORDER BY`, and `VALUES` rows are left inline. Only the four scalar literal forms whose bind type PG can infer from context (string, integer, float, boolean) are parameterized; cast/array/null literals stay inline.
- **Shape-keyed statement cache on the cache connection.** Each cache-DB connection keeps a per-connection registry of named prepared statements (`pgc_<shapekey>`) keyed by `ShapeKey` (`src/pg/cache_connection.rs`). A serve sends a `Parse` for the shape only the first time that shape is seen on the connection; subsequent serves of any literal variation of that shape send only `Bind`/`Execute`. The registry is FIFO-capped as a backstop; the cap does not trip in normal operation.
- **LIMIT/OFFSET bound separately.** The top-level `LIMIT`/`OFFSET` is excluded from the shape and appended by the serve path as its own trailing `$`-params after the shape body, so two queries that differ only in their limit still share one shape.
- **Precompute on the hit path, derive on the cold path.** Most hits carry a precomputed `QueryShape` threaded through the serve request; subsumed serves that lack one derive it from the resolved query at serve time.

## Rationale
- **Shape key over per-literal fingerprint as the statement key** is what lets one prepared plan serve every literal variation. Keying on the fingerprint kept each literal on its own plan — the explosion this ADR removes.
- **Lifting predicate literals, leaving the rest inline** is a soundness boundary, not a convenience. A bound `$N` needs PG type context: a projected literal (`SELECT $1`) fails Parse with "could not determine data type"; `ORDER BY $1` would silently sort by a constant instead of a positional column reference — a wrong-results change; a `VALUES ($1)` row has no inferable type. Predicate literals always sit beside a typed column/operator, so their bind type is inferable, and they are exactly where literal cardinality lives.
- **Restricting to string/integer/float/boolean** avoids binding a value at the wrong type. Cast and array literals carry an explicit `::cast` that disambiguates their type; binding them as text would drop the cast and rely on context inference, which is not always sound. Nulls have no unambiguous bind type. Keeping these inline means two queries differing only in such a literal get distinct shapes — accepted minor proliferation for rarer forms in exchange for never mis-binding.
- **Statement lifecycle decoupled from cache eviction.** A shape statement queries the shared per-relation cache tables and stays valid while the relation is cached. Query eviction does not invalidate it, and a schema change evicts every query on the relation, so the statement is simply never executed again and ages out via the FIFO cap. This removes the per-serve reconciliation the per-fingerprint registry needed.
- **Complementary to the zero-copy hot path (ADR-025).** ADR-025 removed per-row decode/re-encode overhead on the serve path; this ADR removes per-literal plan generation and enables statement reuse across shapes. They address different costs on the same path.

## Consequences

### Positive
- The cache-DB prepared-statement working set is bounded by query-shape diversity rather than literal cardinality, regardless of how many distinct literals are served.
- Queries that differ only in their literals share one prepared plan; after the first use of a shape on a connection, serves send only `Bind`/`Execute`, not `Parse`.
- The per-connection statement registry no longer needs per-serve reconciliation against cache eviction — its lifecycle is decoupled from cache eviction.
- Reduced cache-DB planning work and the relcache-invalidation pressure the per-literal plan churn produced.

### Negative
- Literals that stay inline (cast/array/null, and all SELECT/`ORDER BY`/`VALUES` positions) still proliferate shapes; two queries differing only in such a literal get distinct shapes.
- An added transform and a `QueryShape`/`ShapeKey` abstraction sit on the serve path; subsumed serves without a precomputed shape pay a cold-path derivation.
- A non-integer `LIMIT`/`OFFSET` binds text that fails int8 coercion on the cache DB, erroring that hit so it forwards to origin (chosen over silently dropping the limit and over-returning rows).
- The FIFO statement cap (512 per connection) is an arbitrary backstop set well above the expected shape diversity (tens to low hundreds); the right value awaits real-world workload data.

## Implementation Notes
- `src/query/shape.rs` — `QueryShape` (`sql`, `key`, `literals`), `ShapeKey` (newtype over `u64`, separate from `Fingerprint`), and `query_shape_derive`, which parameterizes, deparses the shaped form, and hashes the SQL into the `ShapeKey`. The top-level LIMIT is excluded from the derived shape.
- `src/query/transform/parameters/resolved_parameterize.rs` — `resolved_query_expr_parameterize` walks predicate positions (`WHERE`/`HAVING`/`JOIN-ON` and nested-subquery predicates, including aggregate internals reached through them) replacing parameterizable literals with `$N` in walk order, returning the shaped query and the literals in placeholder order (`literals[N-1]` binds to `$N`). A round-trip test (re-inject literals at their placeholders, deparse, compare to the original) locks the ordering invariant.
- `src/pg/cache_connection.rs` — `PreparedStatements` (insertion-ordered `VecDeque<ShapeKey>` + `HashSet<ShapeKey>` membership) keyed by `ShapeKey`, FIFO-capped at `PREPARED_STATEMENT_CAP` (512); `pipelined_named_query_send` emits a `Parse` only when the shape is absent and a `Close` for any FIFO-evicted shape ahead of the SELECT, returning a `PrepareOutcome` describing what was sent so the response state machine knows which completions to expect. Statement names are `pgc_<16-hex-of-shapekey>`.
- `src/cache/serve.rs` — `serve_query_send` assembles the shape SQL plus the trailing `LIMIT $(k+1) OFFSET $(k+2)` into the connection's recycled SQL buffer and binds the shape literals plus the separately-rendered limit/offset text; the MV fast path keeps its unnamed extended query and does not use shapes.
- `src/cache/query_cache/serve.rs` — threads the precomputed `Option<QueryShape>` (`serve_shape`) from the hit path through `pool_serve`/`pool_serve_coalesced` into the `ServeRequest`; subsumed serves with no precomputed shape derive it at serve time.
