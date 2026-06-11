# ADR-035: Population/CDC merge consistency

## Status
Accepted

## Context
Query population and the CDC stream are two independent writers to the same shared cache tables, observing the origin at different points on the replication timeline. Population reads a snapshot of origin (slightly ahead of where CDC has applied); CDC applies committed changes as they stream. Without coordination this produced three defects: **ghost rows** — a row population read at its snapshot is removed at origin during population (DELETE, UPDATE out of the predicate, PK change, TRUNCATE), CDC applies the removal to a cache table that doesn't hold the row yet (a no-op), and population then inserts it, permanently (no future CDC event references it again); **stale serves during catch-up** — a query marked servable the instant population finished could expose a transiently-inconsistent cache while CDC was still behind the population's snapshot; and **bystander torn reads** — a merge that lands snapshot-state rows in the shared table while the apply watermark is still behind that snapshot makes rows *created* in the gap (INSERTs, PK-update successors) visible to other already-Ready queries over the relation, whose serves then mix two origin points in time in one result. Gating only the populating query's Ready cannot fix the third defect: serving is per-query, but the table is shared.

The constraint that shapes the solution: pgcache must never serve data superseded by a committed origin write. Two heavier designs were considered and rejected — tagging every cache row with an LSN for last-writer-wins (a schema change cutting across the shared-table and generation models), and a single transaction snapshot across all of a query's relations (requires a dedicated origin connection per population, conflicting with the connection-reduction effort, and made unnecessary by the merge gate below).

## Decision
- **Stage, then merge in the writer.** Population streams its origin snapshot into per-relation staging tables rather than writing the shared cache table directly. The single-threaded writer performs the staging→cache merge, and only when no CDC frame is open — so the merge never races the CDC frame transaction on the shared table.
- **Merge-time watermark gate.** The merge itself is additionally withheld until the CDC apply watermark reaches the population's snapshot LSN, restoring the invariant that *the shared table never contains state newer than the watermark* — no bystander query can observe a torn mix. Queued merges form a min-heap on `(snapshot_lsn, generation)` — a deadline queue released in order as the watermark advances; `generation` (globally monotonic) breaks snapshot-LSN ties, deliberately not `fingerprint`, since two generations of one query can be in flight at once. While entries are gated, the writer signals the CDC thread to request an immediate keepalive, advancing the watermark within a round-trip instead of waiting for the periodic one.
- **Deleted-key set.** While a population over a relation is in flight, the writer records the primary keys CDC removes (stamped with the removal's commit LSN); the merge filters those keys out, so it can't resurrect a concurrently-removed row.
- **LSN-anchored prune + cap.** Each in-flight population contributes an *anchor floor* (a lower bound on its snapshot LSN); a recorded key is pruned once every floor reaches it, bounding the set to the oldest in-flight population's window. A per-relation size cap is a backstop.
- **Abort watermark for bulk invalidation.** A TRUNCATE or a 40P01 frame recovery raises a per-relation `aborted_below` LSN; a merge whose snapshot predates it aborts and repopulates. This self-clears for post-event snapshots, unlike a sticky flag.
- **Ready follows the merge directly.** With the merge gated on the watermark, the formerly separate deferred-Ready gate is redundant — the watermark already satisfies the Ready condition the moment the merge releases, so the query is finalized Ready inline after a successful merge. (The gate originally sat at Ready-time; it was moved to merge-time when the bystander torn read was found.)
- **Generation-scoped bookkeeping.** Deleted-key tracking is keyed by `(fingerprint, generation)`, and a queued merge is dropped if its query was evicted, invalidated, or superseded by a readmit while it waited — checked before the deadline, so a dead entry at the top of the heap releases its tracking and staging immediately rather than holding them until a deadline that no longer matters.

## Rationale
- **The merge gate removes the need for snapshot/stream exactness.** Because snapshot-state rows only enter the shared table once the watermark has reached the snapshot, the tables have converged by the time the data is visible — regardless of per-relation read skew — so a single cross-relation transaction snapshot isn't required. Gating at merge rather than Ready is what extends the protection from the populating query to every query sharing the relation: at merge time, every staged row is either unchanged since the snapshot (insert is exact), already rewritten by CDC (never-overwrite skips it), or removed (the deleted-key filter excludes it).
- **CDC already maintains a registered query's rows live**, so there is no insert/update gap to replay; only row *removals* and serve timing need handling. This is what collapses the problem to a deleted-key filter plus a gate, rather than full buffer-and-replay.
- **Population never overwrites CDC values** (its merge is a data no-op on conflict), so the only residual hazard is resurrecting a removed row — exactly what the deleted-key set and abort watermark address.
- **An LSN abort watermark, not a sticky flag**, keeps a bulk invalidation from needlessly aborting populations whose snapshot already postdates the event.
- **The keepalive nudge** keeps the gate's correctness from costing first-serve latency on a quiescent origin.

## Consequences

### Positive
- No ghost rows, no stale serves, and no bystander torn reads across DELETE/UPDATE-out/PK-change/TRUNCATE/40P01-recovery and replication lag during population.
- No per-row LSN schema change and no full event buffering; the deleted-key set is bounded by the oldest in-flight population's window.
- Generation scoping makes concurrent populations of the same query (parked + readmitted) independent.

### Negative
- First-serve latency gains a round-trip in the common case and is watermark-bound under CDC lag (mitigated by the nudge); moving the gate to merge-time did not add to it — the same wait simply happens one step earlier.
- The writer-serialized merge can stall the writer for large baselines (acceptable starting point; revisit with chunking if it shows up).
- Staging tables add CREATE/DROP churn per population, and under sustained CDC lag gated merges hold their staging tables and deleted-key recording open longer — likelier overflow→abort→repopulate (degraded but never wrong).
- More writer bookkeeping (deleted-key set, pending-merge heap, frame-deferred stamping) than a direct write.

## Implementation Notes
Staging tables live in a dedicated `pgcache_stage` schema (swept by the cache-database reset). Deleted keys and bulk-invalidation watermarks are buffered per CDC frame and stamped with the commit LSN at `CommitMark`, since the commit LSN isn't known mid-frame. The pending-merge heap is a `std::collections::BinaryHeap<Reverse<…>>` with `Ord` hand-written over the `(snapshot_lsn, generation)` key only; crate heaps were surveyed and rejected (radix-heap's monotone-push invariant is fragile to watermark rewind; keyed queues buy removal we don't need) — revisit only if a profile shows the drain hot.
