# ADR-059: Per-reference predicate model

## Status
Accepted

## Context
CDC matching, the constraint indexes, subsumption, read-after-write and outer-join terminality all read one name-keyed analysis (`QueryConstraints`, ADR-003) and a separate name-keyed terminality walk. Four root causes kept producing bugs that were patched one at a time:
* **Lossy extraction.** A conjunct the extractor couldn't read was dropped, leaving one global "complete" bit. Its columns were dropped too, so an expression ON equality or a cross-table WHERE comparison contributed no join columns, and a cached UPDATE to them went stale.
* **Facts keyed by table name.** Self-joins and repeated references merged or clobbered each other's constraints, so each consumer grew its own guard.
* **Join semantics computed in three places:** the collector, the terminality walk and the subsumption completeness test.
* **One completeness bit** where each reference needed its own answer.

The consumers need three different contracts, which one structure served poorly:

| Contract | Meaning | Safe direction | Consumers |
|---|---|---|---|
| Necessary condition | every row of the reference in the result satisfies it | weaker is sound | CDC row matching, eval index, subsumption's new query, read-after-write ranges |
| Exact description | population loads every row satisfying it | refuse when unsure | subsumption parents |
| Dependency footprint | columns whose change can alter the result | must not depend on recognition | link columns, change dependence |

## Decision
Every SELECT branch is described by a `QueryBranchPredicates` (`query/predicates/`). It is keyed by table reference (`SlotId`) and column binding, never by name.

**What the model records:**
* **References and joins.** References carry their join path from the FROM root. Joins carry their kind and whether they are reduced. A comma list is a chain of cross joins.
* **Conjuncts.** Each AND-ed part of WHERE or of an ON is one conjunct, with its scope (WHERE or `On(join)`), every column it mentions, and whether it reaches an enclosing SELECT.
  * Its form is a single-column `Constraint`, a column `Equality`, or `Opaque`.
  * BETWEEN and NOT IN split into one conjunct per bound or element.
  * A `USING`/`NATURAL` merged column is an `Equality` conjunct of its join.
  * Unrecognized conjuncts are kept as `Opaque` for their columns.

**Reach.** A conjunct filters reference R when:
* R is within its reach. A WHERE conjunct reaches every reference. An ON reaches both sides of an inner or reduced join, but only the NULL-filled side of an unreduced outer join.
* No unreduced outer join inside that reach NULL-fills R. Below such a join a conjunct can hold on the padding alone, as an anti-join's `IS NULL` does.

**Reduction.** It is computed top-down. A recognized (NULL-rejecting) conjunct whose reach covers an outer join's NULL-filled side, from WHERE or an enclosing ON, makes that join inner.

**Per-reference views:**
* `filters`: the recognized constraints that filter R, closed under equalities that filter the target. A cast comparison doesn't cross an equality.
* `exact`: R is a base table, every join on its path preserves it, every WHERE conjunct is a constraint on R alone, and nothing from an enclosing SELECT restricts it.
* `link_columns`: R's columns in any conjunct that mentions another reference (a nested subquery's tables count), or that reaches an enclosing SELECT. Also R's columns in the ON of an unreduced outer join that preserves R.
* `outer_join_role`:
  * `Kept` when no unreduced outer join NULL-fills R;
  * otherwise `Terminal` or `Optional`, depending on whether a conjunct above that join mentions R.

**One update query per table reference.** Each carries its reference's projection to column names (`RelationPredicates`).
* Consumers OR over a fingerprint's entries, so a row is kept or invalidated if it can stand in for any reference.
* Membership substitutes only the entry's own occurrence.

## Rationale
* **Recognition affects precision, never soundness.** `filters` only weakens when a conjunct is unrecognized, `exact` refuses, and `link_columns` ignores recognition entirely. A new recognizer, such as OR or `IS NULL`, adds precision without new soundness obligations.
* **Slots instead of names** dissolve the self-join and repeated-reference special cases rather than guarding each consumer against them.
* **One derivation of join semantics.** Reach and reduction drive filters, exactness, links and terminality alike, so the four can't disagree.
* **Per-reference update queries** let a self-join's references keep distinct filters, links and roles, and let each use the prepared single-occurrence membership template.

## Consequences

### Positive
* **Fixed staleness:**
  * expression and cast ON equalities, cross-table WHERE comparisons, and their uncached-row form;
  * a preserved reference that shared a table name with a terminal reference.
* **Better precision:**
  * a self-join's references narrow the eval index on their own filters;
  * a reduced outer join is maintained as an inner join;
  * a self-join's preserved reference can be a subsumption parent;
  * a subquery or CTE table can be covered on its own branch's filters.
* **Removed code.** The name-keyed collector, `ConstraintColumn`, `repeated_tables`, the `self_joined` guards and the terminality walk are gone.

### Negative
* **Extra invalidations through subqueries.** A conjunct with an uncorrelated subquery makes the compared column a link column, so an UPDATE to it invalidates even though the subquery's tables are cached in full. Linking only through incompletely cached partners is PGC-520.
* **More entries for self-joins.** A self-join registers one update query per reference, which costs more index entries and membership evaluations per row. Batched evaluation still skips them (PGC-521).
* **The model is built more than once.** It is built per branch at registration and again for subsumption and read-after-write lookups.

## Implementation Notes
* Model: `query/predicates/` (`conjunct.rs` recognizes, `branch.rs` builds, `derive.rs` derives).
* Consumers:
  * `cache/admission/analyze.rs` (update queries, subsumer regions);
  * `cache/admission/subsume.rs` (new-query references);
  * `query/update.rs` (sources from roles);
  * `proxy/connection/relay.rs` (read-after-write ranges).
* `constraints/` keeps only `TableConstraint` and the range algebra.

Supersedes ADR-003; amends ADR-024 and ADR-037.
