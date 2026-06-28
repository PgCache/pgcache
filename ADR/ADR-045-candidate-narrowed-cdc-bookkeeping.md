# ADR-045: Candidate-narrowed CDC invalidation and memo eviction

## Status
Accepted

## Context
Every CDC row event drives several per-query bookkeeping passes over the affected relation on the single writer thread. ADR-037 introduced a shared point-probe (`eval_candidates` over the per-relation constraint-containment index) and narrowed the in-place matcher and MV dirty-marking to that candidate set. Two consistency-critical passes were left scanning the **full** per-relation update-query set on every row:

- `update_queries_check_invalidate` — iterates all `update_queries.queries` and runs the precise invalidation check per query.
- memo eviction (`memo_frame_accumulate`) — iterates all memoized results on the relation.

Profiling a write workload (dataset B, ~20k registered queries on one relation, 2-core box) shows the writer CPU-bound with these full-set scans dominant (memo eviction ~7% of total process CPU; the invalidation scan next). The work is in-place-apply bookkeeping, not re-population (re-pops ≈ 0/s); it caps write-workload throughput because the writer is single-threaded while serving scales across the other cores.

The matcher already establishes that the candidate set is a sound narrowing for "does this row belong in this query." The two passes this ADR narrows have **different** correctness requirements, and conflating them is itself a stale-read trap:

- **Invalidation is asymmetric.** Per the consistency model only *growing* changes invalidate; shrinking/in-place changes are applied to the cache table in place and never invalidate. So a missed invalidation is only possible on a grow — the narrowed set need only cover "could this row enter the query (or unconditionally affect it)."
- **Memo eviction is symmetric.** A memoized result snapshot is stale on *any* change to the query's rows — grow, shrink, or in-place value change. So the memo narrowed set must also cover queries the row *left*.

MV dirty-marking is **already** candidate-narrowed and symmetric (PGC-292): `mv_dirty_mark_removed_row` probes the candidate set on the old/removed-row image (PK-only with the `Unknown` wildcard under REPLICA IDENTITY DEFAULT) for shrink, and grow is covered because the growing change invalidates the query and `cache_query_cdc_invalidate` folds the MV-dirty transition. **MV is therefore out of scope here** — this ADR changes only the invalidation check and memo eviction.

A missed invalidation *or* a missed memo eviction is a stale read, which the consistency model forbids. This ADR records the analysis that makes each narrowing safe.

## Decision
Replace the full-set scan in `update_queries_check_invalidate` and memo eviction with per-operation **narrowed sets** — one shape for invalidation (asymmetric), a different shape for memo (symmetric):

**Invalidation** (`update_queries_check_invalidate`):
- **INSERT** — `candidates(new) ∪ always_check`
- **UPDATE** — `candidates(new) ∪ always_check ∪ ( changed_cols ∩ limit_predicate_columns ≠ ∅ ? has_limit_fromclause : ∅ )`
- **DELETE** — `has_limit_fromclause ∪ always_check`

**Memo eviction** (`memo_frame_accumulate`) — symmetric, mirroring MV's removed-row probe:
- **INSERT** — `candidates(new)`
- **UPDATE** — `candidates(new) ∪ candidates(old-image)`
- **DELETE** — `candidates(old-image)` (PK-only under REPLICA IDENTITY DEFAULT → conservative via the `Unknown` wildcard, matching today's full eviction)

backed by three small per-relation sets maintained on `UpdateQueries` alongside the existing indexes, used by the invalidation set:

- `always_check` — fingerprints whose source is `Subquery` or `OuterJoinOptional` (they invalidate unconditionally).
- `has_limit_fromclause` — `has_limit` FromClause fingerprints.
- `limit_predicate_columns` — the union of `predicate_columns` over `has_limit_fromclause`.

The precise per-query check (`row_cached`/`row_uncached_invalidation_check`) and the per-memo precise filter still run on every member of the respective narrowed set; narrowing changes only **which queries are examined**, never the verdict for an examined query.

## Rationale
**Invalidation** fires only via one of the branches below; each lies in the invalidation set, so it never under-returns:

- **Row now matches** (insert grow; join-column change with `row_constraints_match(new)`; window change where the new row matches): the row satisfies the query's constraints, so `candidates(new)` returns it — ADR-037's point probe never under-returns and routes queries with no/partial extractable constraints into the unconstrained class returned for every row.
- **Window/predicate change that makes a row *leave* a query** (the PGC-336 escape hatch — only fires when a predicate column changed): affects only `has_limit` FromClause queries. Rather than probe the pre-image, the UPDATE set includes `has_limit_fromclause` whenever the changed columns intersect `limit_predicate_columns` — a cheap per-relation column-set test, empty for the common case (a `score`/`lasteditdate` update never touches the `owneruserid` predicate), so the expansion is rare.
- **DELETE on a `has_limit` FromClause query** (unconditional — a delete can drop a row from any LIMIT window whose replacement is uncached): covered by `has_limit_fromclause`. Non-limit FromClause deletes provably never invalidate (the INNER-JOIN delete only shrinks), so DELETE invalidation needs no row probe at all.
- **Subquery / OuterJoinOptional unconditional invalidation**: covered by `always_check`.
- **Single-table non-limit FromClause queries** provably never invalidate (uncached → `false`; cached path has no join columns), so they are correctly excluded — and they are the bulk of the skipped scan.

**Memo** must evict whenever the query's rows change in either direction. `candidates(new)` covers grow and in-place-now-matches; `candidates(old-image)` covers shrink (the row left) and in-place value changes (the row's PK still matches the query). Their union is *more precise* than today's "any predicate column changed" proxy (which over-evicts queries the row never belonged to) while remaining complete. The PgEval-memo conservative fallback (evict when the query isn't locally evaluable) is retained — the probe cannot reason about non-locally-evaluable predicates. Under REPLICA IDENTITY DEFAULT the old image is PK-only, so `candidates(old-image)` over-returns via the `Unknown` wildcard — exactly the conservative behaviour today's full DELETE eviction already has.

The shared old-image probe is the same one MV already uses, so memo and MV converge on one narrowing for removed rows; invalidation keeps its own asymmetric set. Maintaining the carve-out sets as membership lists (not recomputed per row) keeps per-row cost O(candidates + carve-outs).

## Consequences

### Positive
- Per-row writer bookkeeping drops from O(queries-on-relation) to O(candidates + carve-out sets); raises the single-writer write-throughput ceiling, which is the binding constraint under writes on small boxes.
- Invalidation and memo eviction join the matcher and MV dirty-marking in driving off the `eval_index` point probe — all per-row consumers now narrow through one index, each with the set shape its correctness requires (asymmetric for invalidation, symmetric for memo/MV).

### Negative
- Three more per-relation sets to maintain on query insert/remove, plus a second (old-image) probe per UPDATE for the memo pass.
- The never-under-return guarantee is now load-bearing for **invalidation and memo**, not just optimization: any change to the probe forms, the carve-out rules, or the source/limit classification that drops a true candidate becomes a stale-read bug. The asymmetric (invalidation) vs symmetric (memo) split is itself part of the invariant — narrowing memo with the invalidation set would miss shrinks.
- Conservative full-set fallbacks remain where the probe cannot reason precisely (PgEval memos; PK-only old images under REPLICA IDENTITY DEFAULT), so those do not benefit.

## Implementation Notes
- Carve-out sets live on `UpdateQueries` (`src/cache/update_query.rs`), updated wherever queries are inserted/removed (mirroring `subsumption`/`eval_index`).
- `update_queries_check_invalidate` takes the asymmetric invalidation set; `memo_frame_accumulate` takes the symmetric `candidates(new) ∪ candidates(old-image)` set (both in `src/cache/writer/cdc/invalidation.rs`). The old-image probe reuses the same `eval_index.candidates_point` call MV already makes in `mv_dirty_mark_removed_row`. The per-row dispatch (`src/cache/writer/cdc/dispatch.rs`) computes the sets once per row. MV dirty-marking is unchanged (already narrowed, PGC-292).
- Tests cover each branch (see `tests/cdc_invalidation_narrowing_test.rs`): subquery unconditional invalidation, DELETE on a limit query, predicate-change-leaves-window, join-grow, and the single-table in-place skip. Add a memo-shrink case: populate a memo for a non-limit single-table query, shrink it via UPDATE/DELETE, and assert the re-served result is fresh (not the stale memo).
