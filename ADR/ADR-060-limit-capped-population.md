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
* Only a plain-projection SELECT gets one. These populate in full:
  * shapes that compute their result: aggregates, GROUP BY, HAVING, DISTINCT, window functions and joins;
  * set operations;
  * an OFFSET with no LIMIT count.
* The LIMIT is not part of the query's identity or serve shape (ADR-040), so one entry serves every LIMIT its cap covers.

**What a capped read loads.**
* Only the root SELECT's own table is capped. Subquery, derived-table and CTE reads load whole.
* The capped read is the root's FROM and WHERE, sorted by the query's ORDER BY, with `LIMIT N` and no DISTINCT.
  * Each ORDER BY item is rendered as an expression over the table: an output name or a position becomes its select-list expression.
  * If an item can't be resolved, the read is uncapped.

**More rows.** A request that needs more than the cap forwards to origin. It then re-populates the query under the larger cap, in a new generation. ADR-026 applies the same check when draining coalesced requests.

**CDC keeps the superset.** These rules apply to the capped query's update queries:
* An INSERT, or an UPDATE that makes a row match, upserts the row with no invalidation. Serving re-sorts.
* An UPDATE to a cached row that changes a column defining the window (an ORDER BY, WHERE or HAVING column) invalidates when it could open a gap at the boundary:
  * a WHERE column changed (PGC-94);
  * or the sort key moved in a way that isn't provably a promotion. The check knows sort direction (PGC-334), and an expression sort key stays conservative.
* A row that fails the query's WHERE can't enter its window, so it never invalidates (PGC-336).
* A DELETE invalidates every capped query on the relation (ADR-045).

**Subsumption.** A capped query is never a subsumption parent (ADR-024): it doesn't hold every row its filters describe.

## Rationale
* **The superset invariant lets CDC add rows freely.** Only a change that could lose a top-N row needs reasoning: a demotion, a predicate change, or a delete.
* **The fix follows from the invariant.**
  * A read without the ORDER BY loads arbitrary rows.
  * A capped subquery read loses rows its predicate is evaluated over.
  * One table means no join fan-out to fold, and dropping DISTINCT avoids Postgres's rule that ORDER BY expressions must appear in a DISTINCT select list.
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
* Subquery reads, and a root whose FROM is a derived table or CTE, load whole even under a cap.
* A test can't tell a correct capped read from an unordered one unless its rows sit in a different physical order from the sort. `assert_limit_order_decides` checks this.

## Implementation Notes
* **The cap:** `base_query_prepare` (`cache/admission/analyze.rs`) and `resolution_limits` (`cache/writer/registration/resolve.rs`).
* **The reads:** `population_reads` and `population_select_build` (`cache/writer/population/stream.rs`), with `ResolvedQuery::order_by_source_into`.
* **The window:** `limit_window_move_invalidates`, plus the DELETE rule in `from_clause_uncached_invalidates` (`cache/writer/cdc/invalidation.rs`).
* **The bump:** `limit_bump_handle` (`cache/writer/registration/lifecycle.rs`).
