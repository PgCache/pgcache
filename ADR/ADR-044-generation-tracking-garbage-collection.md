# ADR-044: Generation tracking as garbage collection

## Status
Accepted

## Context
pgcache caches source rows in shared per-relation tables: one cached row can belong to the result sets of many registered queries at once. As queries are admitted, re-populated, evicted, and re-admitted, rows accumulate, and a row stops being needed once no live cached query references it any more. Reclaiming those rows is a *liveness* problem — "is this row still in active use by any cached query?" — and answering it by reference-counting every row against every query, or by scanning all queries on every reclamation, is too expensive on the hot path.

The critical constraint is what this mechanism must **not** be. Cache consistency — the no-stale-reads guarantee — is already provided entirely by the CDC apply model (see the Cache Consistency Model and ADR-027/035): a cached query is either up to date with origin or invalidated and forwarded. Row *visibility* within the cache Postgres is provided by ordinary MVCC. The generation mechanism layered on top must not be conflated with either; it only decides *when a row may be dropped*, never *whether a read may see a row*. This distinction is subtle and easy to misread (the generation counter looks like it could be a snapshot/version), so it is stated here explicitly.

## Decision
Track row liveness with a monotonic **generation** counter and a shared-memory index in the `pgcache_pgrx` extension, used purely for garbage collection — to identify which cached rows are in active use by cached queries and which can be safely reclaimed.

- **A generation per cached query, assigned at admission.** Generation is a monotonically increasing counter; a cached query is stamped with the current generation when it is added to the cache, so a query's generation also encodes its admission order.
- **Rows carry the highest accessing generation (the core invariant).** As a cached query accesses rows (via the custom scan) or modifies them, each touched row's `(table_oid, pk_hash)` is recorded against that query's generation. The invariant the code maintains is: **a row is marked with the highest generation of any cached query that has accessed it** (max-wins).
- **Eviction proceeds by lowest active generation first.** Only the cached query holding the *minimum active generation* may be evicted; eviction walks upward from there. The `cache_policy` (ADR-038) decides whether a referenced query earns a second chance: under CLOCK it does, implemented by reassigning it a new (higher) generation — effectively re-admitting it so it is no longer the minimum — while FIFO grants none. The order is lowest-generation-first either way; the second chance changes a query's generation, not the eviction order.
- **Reclamation threshold = the minimum active generation.** Once the lowest-generation query is evicted, every row stamped below the new minimum active generation was, by the invariant above, last touched only by queries that are already gone — so it is provably referenced by no active query and can be purged (`pgcache_purge_rows` / `pgcache_generation_purge*`), with no per-row refcounting and no scan of live queries.
- **CDC-applied rows are written at generation 0, which is never reclaimable.** At CDC apply time it is unknown which cached queries will use a row, so it cannot be stamped with a meaningful generation and is written at 0, explicitly excluded from reclamation. Periodically, generation-0 rows are promoted to the current maximum generation (≈ max + 1); once the queries that subsequently access them age out, they fall below the minimum active generation and become reclaimable like any other row. Without this promotion they would never be collected.
- **Two shared-memory indices (dshash + DSA).** A *reverse log* (`generation → list of (table_oid, pk_hash)`) is append-only on the hot path and takes only its partition lock. A *forward hash* (`(table_oid, pk_hash) → max_generation`) is a lazily materialized view of the reverse log, written only by a fold step (a background worker, plus inline from purge) and read by purge and lookup. Writers never touch the forward hash or the maintenance lock.
- **Lifecycle hooks.** An `object_access_hook` clears both indices when a database is dropped, so a cache-database reset (ADR-034 / startup) wipes generation state as a side effect.

## Rationale
- **Generation is GC, not consistency.** This is the load-bearing point. Consistency and visibility come from the CDC apply model and the cache PG's MVCC; the generation counter only governs reclamation timing and eviction order. It is never a snapshot, a read fence, or a version check, and must not be described as one.
- **Max-wins stamping + lowest-first eviction is what makes threshold purge sound.** Because a row carries the highest generation of any query that touched it, and queries are evicted in increasing generation order, the minimum active generation is a clean watermark: everything below it is dead. This replaces exact cross-query refcounting with a single comparison.
- **Generation-0 plus periodic promotion handles the CDC case.** CDC-applied rows have no known consumer at apply time, so they can't be stamped; parking them at 0 keeps them safe from premature reclamation, and promoting them into the active range later ensures they don't leak once genuinely unreferenced.
- **The generation-0 rule is itself proof of the GC-only role.** Rows routinely exist in the cache marked 0 (or unpromoted), which would be impossible to treat as a visibility/version gate — a visibility scheme could not leave live, readable rows "unversioned." Generation is a reclamation hint, not a completeness invariant over readable rows.
- **Append-only reverse log + off-hot-path fold.** The hot path (row access) only appends under a partition lock; the expensive consolidation into the forward hash happens in a background worker. This keeps stamping cheap under load and tolerates the forward hash being *eventually* consistent — acceptable precisely because it drives GC, where lagging the true liveness set only delays reclamation, never causes a wrong read.

## Consequences

### Positive
- Reclamation of no-longer-referenced cached rows by a single minimum-active-generation watermark — no per-row cross-query refcounting and no full-query scans.
- Eviction order and row reclamation share one monotonic counter, so the eviction policy and the GC watermark stay consistent by construction.
- The hot path stays nearly lock-free (append-only reverse log, partition-scoped lock); consolidation cost is deferred to a background worker.
- Generation state is shared-memory and self-clearing on database drop, so it needs no separate teardown.

### Negative
- The forward hash is eventually consistent (fold lags the reverse log), so a row may remain reclaimable-but-not-yet-reclaimed for a window — fine for GC, but it means the structure must never be read as an authoritative liveness or visibility oracle.
- Generation-0 rows depend on the periodic promotion to ever be collected; if promotion stalls, CDC-written rows accumulate (a reclamation-latency issue, never a correctness one).
- The GC framing is subtle and has repeatedly been misread as a consistency/visibility mechanism; this ADR and a callout in CLAUDE.md exist specifically to prevent that.

## Implementation Notes
Shared-memory indices, dshash/DSA bindings, fold, and purge live in `pgcache_pgrx/src/generation.rs`; the background fold worker in `pgcache_pgrx/src/bgworker.rs`; row-access stamping via the custom scan and the modify trigger in `pgcache_pgrx/src/custom_scan.rs` (gated on the `QUERY_GENERATION` GUC `> 0`). SQL entry points (`pgcache_enable_tracking`, `pgcache_generation_dump`, `pgcache_generation_purge`/`_all`, `pgcache_purge_rows`, `pgcache_total_size`) are described in CLAUDE.md's pgcache_pgrx section. pgcache assigns query generations at admission, drives lowest-generation-first eviction, and triggers generation-0 promotion and purge from the cache writer.
