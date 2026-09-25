# ADR-048: Per-connection read-after-write consistency

## Status
Accepted

## Context
pgcache keeps the cache consistent through CDC apply, but CDC lags origin commit. In that window a connection that writes a row and immediately reads it back can receive a cache hit that omits its own just-committed write — a read-after-write violation, visible to the exact single-session flow most likely to notice it. Cross-connection staleness remains CDC's responsibility and is out of scope; the gap is the "read your own writes" guarantee applications assume within one session.

The proxy operates under hard constraints: it has no catalog knowledge, it must never block a connection waiting on the single-threaded writer, and it can only observe the statements it forwards and the wire protocol. The fix therefore has to be conservative in the unsafe direction — any uncertainty must forward to origin rather than risk serving stale.

Alternatives considered and rejected: blocking the read until the watermark advances (no notification primitive exists and it can wedge a connection); sampling the commit LSN by rewriting the write itself (reads a pre-commit LSN); and a global cross-connection barrier (needless serialization for a per-session guarantee).

## Decision
Each connection maintains a per-connection **write log** of the writes it forwarded to origin. A subsequent cacheable read on that connection is served from cache only if provably disjoint from every still-pending write; otherwise it forwards to origin. A pending write clears once the **settled watermark** reaches an upper bound of the write's commit LSN.

- **Commit-LSN bound sampled post-commit.** After a forwarded write's commit-confirming `ReadyForQuery('I')`, inject a standalone `pg_current_wal_insert_lsn()` probe on the origin socket, swallowed by an intercept. Its LSN upper-bounds the write's commit.
- **Forward, never wait.** The gate reads the published settled watermark wait-free and forwards on any doubt; it never blocks or coordinates with the writer.
- **Settled watermark.** Every origin transaction committing at or below it is either applied to the cache or produced no decodable output. Advanced from two sources: the commit-only apply advance, and a keepalive's LSN once the writer is drained — sound because a logical walsender's keepalive carries its *sent* position, past which every decodable commit was already emitted in stream order. The keepalive source is what lets entries clear across non-decodable WAL (DDL, writes to unpublished tables, idle origin), where no commit will ever arrive.
- **Conservative write classification.** `INSERT` with an explicit column list and literal/parameter `VALUES` is row-enumerable; `UPDATE`/`DELETE`/`MERGE`/`TRUNCATE` are table-level; anything unclassifiable (DDL, `CALL`, multi-statement, volatile-function `SELECT`) is connection-level. `PREPARE TRANSACTION` is unstampable and clears only at connection close. Every fallback widens scope, never misses a write.
- **Row-level disjointness reuses subsumption.** A single-table read's per-column ranges (from the constraint analyzer) exclude an inserted row when a known cell falls outside the read's range; comparisons are numeric-aware so integer/float spellings match by value.
- **Bounded state.** Writes aggregate per table; each table carries its own active/waiting tier pair with a per-table commit-LSN bound, so a fresh write never gates an already-applied one and one table's clearance is never held back by another table's later bound. A probe stamps each table independently (a table with a racing write is skipped and re-probed alone). All row-precise stores are capped and overflow degrades that table to opaque.

## Rationale
- **Post-commit standalone sampling is the only safe LSN source.** `pg_current_wal_insert_lsn()` sampled after the commit-confirming RFQ is provably ≥ the commit LSN under `synchronous_commit=off` (the same argument as the population snapshot). Sampling earlier — piggybacked on the write, at forward time, or at a `'T'` RFQ — reads a pre-commit LSN and is the one way the design could serve stale.
- **Log-on-forward is safe by construction.** The proxy never parses per-statement completion, and a mid-batch error makes "which Executes ran" unknowable. Logging at forward time can only create false positives (an errored or rolled-back write lingers until the watermark passes), never a missed write.
- **Forward-not-wait cannot wedge a connection** and needs no notification from the writer — only a wait-free atomic read.
- **Per-connection scope matches the guarantee.** Cross-connection consistency stays with CDC, avoiding any global serialization.
- **Consistency derives from the settled watermark, not the generation counter** (which is garbage collection only — ADR-044).

## Consequences

### Positive
- Read-your-own-writes holds for single-connection flows even while CDC lags.
- Never serves stale: every uncertainty forwards.
- No added latency on the read hot path (wait-free watermark read); the writing connection carries the cost.
- Memory is bounded regardless of write volume.
- Always on: the former `read_your_writes` kill switch was removed with ADR-054
  (the log is the correctness mechanism for in-transaction serving); the
  integration harness disables it through a fault-injection-only hook.

### Negative
- Extra origin forwards during the commit→apply window on the writing connection — the freshness tradeoff; observable via metrics.
- Row-level precision covers single-table INSERT reads only; joins and `UPDATE`/`DELETE` stay table/connection-conservative (later stages: ADR-049 for UPDATE/DELETE, ADR-054 for in-transaction serving).
- Clearance liveness floor: when the WAL tail is non-decodable, entries clear only at the next keepalive, so the forward window can extend to the keepalive cadence (extra forwards, not a wedge).
- The commit-LSN probe adds one origin round-trip per write-then-idle boundary.
- **Known exclusion (PGC-447)**: the gate matches pending writes to reads by
  relation *name*, so a write routed through an auto-updatable view, or
  directly into a partition child while the parent is cached, is not matched
  to reads of the underlying/parent table — such writes sit outside the
  per-connection read-your-writes guarantee (CDC still corrects the cache;
  the exposure is the same-connection commit→settle window). A point-in-time
  name mapping goes stale on view creation / partition attach, so the fix is
  deferred to origin schema-change observation (PGC-458).

## Implementation Notes
The settled watermark is published as an atomic on the per-generation `CacheStateView` and read through the live dispatch, never a cached handle from a dead generation (which would read frozen-high). The gate lives in the `OriginDrain` arm of the connection loop; write classification runs at cacheability-analysis time. This is Stage 1 scope; UPDATE/DELETE predicate intersection and in-transaction serving are follow-ups.
