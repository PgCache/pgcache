# ADR-024: Predicate Subsumption

## Status
Accepted

## Context
When a new query is registered, pgcache populates the cache by executing it against the origin database — a network round-trip, row transfer, and insertion into the cache DB. In many cases, the data needed by the new query is already present in the cache, loaded by a previously registered query. Detecting this and skipping population eliminates the origin round-trip entirely, serving the query from cache immediately.

This is especially valuable for multi-tenant workloads where queries share a common predicate (e.g. `tenant_id = $1`) with additional filters, and for reference tables where a full-table cache covers all filtered queries.

## Decision
At registration time, check whether a new query's result set is already covered by existing cached data using predicate subsumption. If every row that satisfies the new query's predicates is guaranteed to exist in the cache (loaded by a previous query), skip origin population and serve immediately.

### Subsumption Rule

A new query Q_new is subsumed by an existing cached query Q_cached when Q_cached's predicate constraints are a subset of Q_new's constraints — meaning Q_new is at least as restrictive, so its result set fits within Q_cached's already-loaded data.

For the current implementation (v1), this is **equality-only**: every `(column, Equal, value)` triple in the cached query's constraints must appear in the new query's constraints. Non-equality constraints in the cached query cause subsumption to be rejected, since implication checking for inequality ranges is not yet implemented.

Examples:
- Cached `SELECT * FROM orders WHERE tenant_id = 1`, new `SELECT * FROM orders WHERE tenant_id = 1 AND status = 'active'` — subsumed (new is narrower)
- Cached `SELECT * FROM settings` (no WHERE), new `SELECT * FROM settings WHERE key = 'foo'` — subsumed (full table scan covers everything)
- Cached `SELECT * FROM orders WHERE tenant_id = 1`, new `SELECT * FROM orders WHERE tenant_id = 2` — not subsumed (different values)

### What Cannot Be Subsumed

- **Cached queries with LIMIT**: May not have loaded all matching rows
- **Tables a cached query didn't load in full**: a cached query is a candidate for a table only when the table is complete in its branch — every join between the branch's FROM root and the table is a LEFT join with it on the left or a RIGHT join with it on the right, and the branch's WHERE reads no other table. Population loads exactly the rows its constraints describe then. Inner and cross joins, the nullable side of an outer join, and a WHERE on another table disqualify it; ON clauses never do. A table in a derived table, CTE or subquery is judged in its own branch
- **Set operations** (UNION/INTERSECT/EXCEPT): Per-branch constraint analysis not yet implemented
- **OR predicates**: Constraint extraction treats these as opaque
- **Inequality constraints in cached query**: Implication checking (e.g. `id > 10` implies `id > 5`) not yet implemented

### Per-Table Checking

The subsumption check is per-table, not across the full constraint set. For each relation OID in the new query, at least one existing `UpdateQuery` entry must cover that table. This prevents false positives where one table is covered but another isn't (e.g. a join of a covered and uncovered table).

### Flow

**Coordinator** (`query_cache.rs`): On cache miss (or pending/invalidated state), the coordinator sends a `QueryCommand::Register` with a `subsumption_tx` oneshot channel and awaits the writer's response via `subsumption_await()`. The `AdmitAction` tells the writer what to do if subsumption fails:
- `Admit` — proceed with normal registration and population
- `CheckOnly` — only check subsumption, don't register (for pending queries below the admission threshold)

**Writer** (`writer/query.rs`): During `query_register()`, after resolving the query and extracting constraints:
1. `subsumption_check()` iterates each relation OID, looking up existing `UpdateQuery` entries that are Ready, non-limited, and complete for that table
2. For each, calls `table_constraints_subsumed()` to check if the cached equality constraints are a subset of the new query's constraints
3. If all tables are covered: `query_subsume()` assigns a generation, stamps rows in the cache DB (same `SET mem.query_generation` pattern as eviction bumps), registers metadata, marks `Ready`, and responds `SubsumptionResult::Subsumed`
4. If not subsumed: responds `SubsumptionResult::NotSubsumed` immediately, then proceeds with population (if `AdmitAction::Admit`) or does nothing (if `CheckOnly`)

**Back at Coordinator**: On `Subsumed`, the held request is routed to the cache worker — served from cache with no origin round-trip. On `NotSubsumed`, the request is forwarded to origin.

### Generation Stamping

Subsumption reuses the generation stamping pattern from CLOCK eviction bumps: set `mem.query_generation` to the new query's generation, execute the resolved query against the cache DB (which stamps accessed rows via the pgcache_pgrx extension's CustomScan tracker), then reset the generation. This keeps the rows safe from generation-based GC without any row copying.

If the cache DB execution fails (e.g. table dropped by concurrent eviction), subsumption falls back gracefully — responds `NotSubsumed` and proceeds with normal population.

## Rationale
- **Per-register oneshot** for the subsumption result gives the coordinator a synchronous answer for each request without complex event-loop coordination. The coordinator awaits inline, which is acceptable because the writer responds promptly (before dispatching population)
- **Per-table checking** prevents false subsumption in multi-table queries where only some tables have coverage
- **Per-table completeness** rather than single-table candidates: population loads a table's part of every FROM row that passes WHERE, so a table no join can drop rows from, and that WHERE filters only through its own constraints, holds exactly what its constraints describe. Join selectivity is never reasoned about
- **Equality-only v1** covers the highest-value cases (multi-tenant filtering, reference tables) while keeping the logic simple and provably correct. Inequality subsumption can be added later with pairwise implication checks
- **Reusing generation stamping** avoids duplicating data — the rows are already in the cache table, they just need their generation updated

## Consequences

### Positive
- Multi-tenant queries with shared predicates become instant cache registrations after the first tenant-scoped query populates
- Pinned full-table scans effectively cover all filtered queries against that table (synergy with ADR-023)
- First execution of a subsumed query is a cache hit — no origin round-trip at all
- Graceful fallback — subsumption failure is invisible to the client, just takes the normal population path

### Negative
- Every first-miss now waits for the writer to process registration and check subsumption before being forwarded to origin (if not subsumed). This adds a few milliseconds to the non-subsumed path, small relative to a typical origin round-trip
- Equality-only checking misses range-narrowing opportunities (e.g. `id > 10` implying `id > 5`)
- Inner-joined tables are never candidates, even when a foreign key guarantees every row a partner

## Implementation Notes
- Subsumption check: `subsumption_check()` and `query_subsume()` in `src/cache/writer/query.rs`
- Constraint comparison: `table_constraints_subsumed()` in `src/query/constraints.rs`
- Coordinator flow: `subsumption_await()` in `src/cache/query_cache.rs`
- Message types: `SubsumptionResult`, `AdmitAction` in `src/cache/messages.rs`
- Metrics: `pgcache.cache.subsumptions` counter, `pgcache.cache.subsumption_latency_seconds` histogram in `src/metrics.rs`
- Per-query metric: `subsumption_count` on `QueryMetrics` in `src/cache/types.rs`
- Worked example: [docs/walkthroughs/subsumption-index.md](../docs/walkthroughs/subsumption-index.md)
