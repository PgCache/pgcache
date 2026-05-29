# ADR-027: CDC Local Eval Fast Path

## Status
Accepted

## Context
Every CDC row event (INSERT / UPDATE) dispatches an `INSERT … WHERE EXISTS (<per-query predicate>) ON CONFLICT DO UPDATE` statement against the cache DB for each registered update query on that table. Queries are fired in concurrent batches of `pool_size` and processing short-circuits on the first match. For tables with M registered queries:

- Match case: up to `pool_size` DB round-trips (the whole winning batch is awaited before we know which query matched).
- No-match case: M DB round-trips — every query evaluated on the PG side just to learn the row is irrelevant.

For simple single-table queries the WHERE predicate is already decidable against a single CDC row in Rust, via `where_expr_evaluate` (ADR-002's resolved AST). Doing that evaluation in-process trades M PG round-trips for zero in the no-match case, and at most one upsert round-trip in the match case.

## Decision
Classify each update query once at registration as `LocalEval` or `PgEval`, stored on `UpdateQuery.eval_strategy`. At CDC dispatch time, evaluate LocalEval queries in Rust first; if any matches, issue one unconditional UPSERT and skip remaining queries. Only fall through to the existing PG-side `WHERE EXISTS` batch path for PgEval queries.

### Classification (`update_eval_strategy_classify`)
A query is LocalEval when all hold:
- Source is `FromClause` (Subquery / OuterJoin* sources have cross-row semantics that single-row eval can't capture)
- Resolved body is a single-table `SELECT`
- No `GROUP BY` / `HAVING` (row-level match semantics differ from post-aggregation filtering)
- WHERE is absent, or `resolved_where_expr_supported` holds — i.e. the Rust evaluator can decide it (AND / OR / comparisons / IS NULL / IS TRUE etc.)

Anything else (IN lists, LIKE, subqueries, functions, joins) stays PgEval. The support predicate stays in lockstep with `where_expr_evaluate`; extending one requires extending the other.

### Dispatch (`update_queries_execute_concurrent`)
1. **Phase 1 — LocalEval**: iterate complexity-sorted queries; for each LocalEval query, evaluate WHERE in Rust. On first match, emit one `INSERT … ON CONFLICT DO UPDATE` with no predicate and return.
2. **Phase 2 — PgEval**: if Phase 1 finds no match, fall through to the existing concurrent batched `WHERE EXISTS` path, restricted to PgEval queries.

## Rationale
- **Classify once, look up at hit time**: the WHERE shape doesn't change over a query's lifetime; paying classification cost on every CDC event is wasteful.
- **LocalEval first, not interleaved**: Rust evaluation is free (no DB I/O); checking every LocalEval query up front costs nothing and can skip the entire PG batch.
- **Unconditional upsert on local match**: the row is stored per-table, not per-query. Any matching query justifies the same upsert payload, so one write is correct for the whole match set.
- **Conservative classifier**: anything the Rust evaluator might mis-decide falls to PgEval, preserving PG as the correctness oracle. Expanding LocalEval coverage is a deliberate, testable change.
- **Existing PG path untouched for unsupported shapes**: no behavior change for multi-table, aggregated, or complex-predicate queries.

## Consequences

### Positive
- **No-match case** (common — unrelated writes to a busy table) drops from M DB round-trips to zero.
- **Match case** for all-LocalEval query sets drops from up to `pool_size` round-trips to exactly one.
- **Latency**: no waiting on a batch of PG statements to know that none matched.
- **Rust evaluator is now live**: `where_expr_evaluate` was dormant since ADR-002; CDC exercises it against real traffic, building confidence for future uses.

### Negative
- **Semantic drift risk**: the Rust evaluator must agree with PG for the supported shapes. Text-protocol quirks (numeric formatting, boolean encoding) are handled by `where_value_compare_string` today, but exotic types could diverge. Mitigated by narrow initial support surface and existing test coverage of the comparison helper.
- **Two evaluation paths** to maintain. Adding a new SQL feature means updating both `where_expr_evaluate` and `resolved_where_expr_supported`; a mismatch would either silently fall to PgEval (safe) or silently mis-decide (bug). Lockstep lints would help but aren't yet in place.
- **Stale classification** across registrations: existing cached queries keep their original strategy if the evaluator grows new capability. Acceptable — classification is upgraded on eviction and readmission.

## Implementation Notes
- Enum and field: `UpdateEvalStrategy`, `UpdateQuery.eval_strategy` in `src/cache/types.rs`
- Support predicate: `resolved_where_expr_supported` in `src/query/evaluate.rs`
- Classifier: `update_eval_strategy_classify` in `src/cache/writer/query.rs`
- Dispatch split: `update_queries_execute_concurrent` and `cache_upsert_unconditional_sql` in `src/cache/writer/cdc.rs`
- Metrics: `pgcache.cache.cdc_local_eval_hits`, `pgcache.cache.cdc_pg_eval_hits` in `src/metrics.rs`
