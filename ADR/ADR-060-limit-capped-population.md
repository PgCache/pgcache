# ADR-060: LIMIT-capped population

## Status
Accepted

## Context
A `LIMIT n` query needs only its top n rows, while the rows matching its WHERE can be orders of magnitude more, as with `ORDER BY created_at DESC LIMIT 10` over a large table. The cache serves by re-running the query over the rows it holds, so it can return a top n only if it holds those rows. CDC moves rows in and out of the top n as they're inserted, updated and deleted.

The rules for caching a LIMIT were spread across code and several ADRs:
* ADR-026 checks whether a cached LIMIT covers a request.
* ADR-040 binds LIMIT and OFFSET apart from the shape.
* ADR-024 keeps limited queries out of subsumption.
* ADR-045 invalidates them on any DELETE.

Nothing said what a capped population must load. Two wrong-result bugs followed:
* **PGC-523:** the capped read kept the LIMIT but dropped the ORDER BY, so it loaded arbitrary rows.
* **PGC-524:** the cap also applied to subquery reads, so serving evaluated a subquery over part of its rows.

## Decision
**Invariant.** For a capped query, the cache holds a superset of the query's current top N rows, where N is the cap. Serving reapplies the query's WHERE, ORDER BY and LIMIT, so an extra row is harmless and a missing one is a wrong result.

**Which queries are capped.**
* The cap is the root's LIMIT plus OFFSET.
* Only a plain-projection SELECT gets one, judged on the population form (decorrelated when it has one). Its FROM is base tables, alone or joined inner (amended 2026-10-30, PGC-570). These populate in full:
  * shapes that compute their result: aggregates, GROUP BY, HAVING, DISTINCT and window functions. This holds even when their ORDER BY can't be served, which demotes their MV gate to `Skip` (PGC-527);
  * outer joins, and a FROM with a derived table or CTE reference;
  * set operations;
  * an OFFSET with no LIMIT count.
* The LIMIT is not part of the query's identity or serve shape (ADR-040), so one entry serves every LIMIT its cap covers.

**What a capped read loads.**
* Only the root SELECT's own tables are capped. Subquery, derived-table and CTE reads load whole.
* The capped read is the root's FROM and WHERE, sorted by the query's ORDER BY, with `LIMIT N` and no DISTINCT.
* A join's table is read as `SELECT DISTINCT * FROM (<that table's columns over the root's FROM and WHERE, sorted, LIMIT N>)`: the rows of the table that take part in the join's top N result rows. The sort may name another table's column, so it sits inside the subquery, and DISTINCT folds the join's fan-out outside it.
  * Each ORDER BY item is rendered as an expression over the table: an output name or a position becomes its select-list expression, with stars expanded. CDC's window columns and sort keys resolve items the same way, through one resolver.
  * If an item can't be resolved, the read is uncapped.

**More rows.** A request that needs more than the cap forwards to origin. It then re-populates the query under the larger cap, in a new generation. ADR-026 applies the same check when draining coalesced requests.

**CDC keeps the superset.** These rules apply to the capped query's update queries:
* An INSERT, or an UPDATE that makes a row match, upserts the row with no invalidation. Serving re-sorts.
* An UPDATE to a cached row that changes a column defining the window (an ORDER BY, WHERE or HAVING column) invalidates when it could open a gap at the boundary:
  * a WHERE column changed (PGC-94);
  * or the sort key moved in a way that isn't provably a promotion. The check knows sort direction (PGC-334), and an expression sort key stays conservative.
* A row that fails the query's WHERE can't enter its window, so it never invalidates (PGC-336).
* A DELETE invalidates every capped query on the relation (ADR-045).
* A relevant change to a capped query's subquery table invalidates it, including a shrink. An `IN` set losing a row, or a `NOT IN` set gaining one, can vacate a top-N row whose replacement was never loaded (PGC-527).

**Subsumption.** A capped query is never a subsumption parent (ADR-024): it doesn't hold every row its filters describe.

## Rationale
* **The superset invariant lets CDC add rows freely.** Only a change that could lose a top-N row needs reasoning: a demotion, a predicate change, or a delete.
* **The fix follows from the invariant.**
  * A read without the ORDER BY loads arbitrary rows.
  * A capped subquery read loses rows its predicate is evaluated over.
  * One table means no join fan-out to fold, and dropping DISTINCT avoids Postgres's rule that ORDER BY expressions must appear in a DISTINCT select list. A join folds its fan-out with DISTINCT over a sorted, limited subquery, where that rule does not reach.
* **A join's top N is bounded too.** Projecting a table from the join's top N rows gives every row of that table those result rows need, which is the superset the invariant asks for; serving re-runs the join with its ORDER BY and LIMIT over the shared cache tables, and an extra cached row either fails the WHERE or is a true result row that sorts below the window. Joins were first excluded with the computed shapes. On the bench stack a `LIMIT 50` tag page (`post_tags JOIN posts … WHERE tag = ?`) then loaded every post of the tag, up to 34k rows with bodies, on each of ~2,800 invalidations in 16 minutes: 37M rows staged against 300k cached, the merge queue 14 minutes behind, CDC apply 3 s late and a third of cacheable reads forwarded by the read-after-write gate. Outer joins stay excluded: the NULL-filled side's widened read (PGC-556) does not compose with a window.
* **ORDER BY items are resolved to expressions** rather than embedding the query's select list and renaming every column of a wrapper, which keeps the read a plain one-table SELECT.
* **Falling back to an uncapped read is always correct.** It costs rows, never correctness.
* **Computed shapes populate in full** because their result's top N maps to no bounded set of input rows.

## Consequences

### Positive
* A top-N query over a large table populates N rows, not every match.
* Inserts and promotions are maintained in place.
* A request for more rows re-populates one query under a larger cap.
* One cached entry serves every LIMIT and OFFSET within its cap.

### Negative
* Demotions, predicate-column changes and deletes invalidate the query.
* Ties at the window boundary may resolve to different rows in origin and cache, which SQL permits.
* Subquery reads, a root whose FROM is a derived table or CTE, and outer joins load whole even under a cap.
* A capped join issues the join once per table it reads.
* A capped join invalidates on any sort-key change to a cached row, promotions included: the window can cut a row's fan-out, so a promoted row may need partners the population never loaded. The promotion check (PGC-334) stays single-table.
* A test can't tell a correct capped read from an unordered one unless its rows sit in a different physical order from the sort. `assert_limit_order_decides` checks this.

## Implementation Notes
* **The cap:** `base_query_prepare` and `population_max_limit` (`cache/admission/analyze.rs`). The writer and pgcache-fit both use it.
* **The reads:** `population_reads` and `population_select_build` (`cache/writer/population/stream.rs`), with `ResolvedQuery::order_by_source_into`.
* **ORDER BY resolution:** one resolver in `query/resolve/render.rs`, behind `order_by_source` (the capped read, `limit_window_columns_collect`, `limit_order_keys_collect`) and `order_by_position` (MV ORDER BY serving).
* **The window:** `limit_window_move_invalidates`, the DELETE rule in `from_clause_uncached_invalidates`, and the subquery rule in `row_uncached_invalidates` (`cache/writer/cdc/invalidation.rs`).
* **The bump:** `limit_bump_handle` (`cache/writer/registration/lifecycle.rs`).
