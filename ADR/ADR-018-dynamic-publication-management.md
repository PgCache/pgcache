# ADR-018: Dynamic Publication Management

## Status
Accepted

## Context
pgcache's CDC pipeline uses PostgreSQL logical replication to detect origin table changes and invalidate cached queries. The publication was created with `FOR ALL TABLES`, meaning the replication stream delivered change events for every table in the origin database regardless of whether any cached query referenced it. The writer filtered events client-side using the shared `active_relations` set — discarding events for unreferenced tables. This wasted network bandwidth and parsing effort, particularly in databases with many tables where only a small subset are cached.

## Decision
Dynamically manage the CDC publication's table list so PostgreSQL filters events at the source. The publication starts empty on startup and is updated via `ALTER PUBLICATION` as queries are registered and evicted.

- **Empty publication at startup**: `replication_provision()` creates the publication without `FOR ALL TABLES`. Tables are added dynamically as queries register. The replication slot is preserved across restarts.
- **Eager update on registration**: `publication_update()` executes immediately after `active_relations_rebuild()` detects a change during `query_register()` — before population dispatch. This ensures CDC captures events for newly cached tables from the moment population begins. Same for `query_readmit()` under CLOCK policy.
- **Deferred update on removal**: Eviction, failed population cleanup, and stale entry cleanup set a `relations_dirty` flag instead of immediately rebuilding. The flag is drained by `publication_dirty_drain()` at the end of each command handler cycle, batching multiple removals into a single `ALTER PUBLICATION`.
- **SET TABLE for idempotent updates**: Non-empty sets use `ALTER PUBLICATION ... SET TABLE` (replaces the entire list atomically). Empty sets use `DROP TABLE` to remove the last tables.
- **Client-side filter retained**: `is_relation_active()` in the CDC processor remains as a safety net for the race window between `ALTER PUBLICATION` execution and the replication stream reflecting the change.

### CLOCK policy interaction
Under CLOCK eviction, CDC invalidation keeps cached query entries with `invalidated = true` for fast readmission. Relation OIDs stay in `active_relations`, so the publication does not shrink on invalidation — only on full eviction or stale cleanup. This is intentionally conservative: the table stays in the publication so CDC events continue flowing for potential readmission.

## Rationale
**Server-side filtering eliminates waste**: Moving the filter from client to server avoids unnecessary network transfer, protocol parsing, and processing for irrelevant tables. The benefit scales with the ratio of total tables to cached tables.

**Dirty flag batches removals**: Eviction and stale cleanup can remove multiple queries per command cycle. Computing active relations and executing ALTER PUBLICATION once per cycle avoids redundant DDL.

**Eager registration update preserves correctness**: Population inserts rows into cache tables and relies on CDC to track subsequent changes. If the table isn't in the publication when population starts, CDC events during that window are lost, leading to stale cache entries.

**OID-to-name resolution is always valid**: Table metadata is never removed from the cache, so resolving OIDs to qualified `schema.table` names for the ALTER PUBLICATION SQL always succeeds. *(Amended by ADR-058: a `DROP TABLE` signalled through the event-trigger message removes the relation's metadata and publication entry, since origin has already dropped it from the publication and naming it again would fail.)*

## Consequences

### Positive
- Reduced network bandwidth — only events for tables with cached queries are streamed
- Reduced CPU — no parsing or client-side filtering of irrelevant CDC events
- Benefit scales with database size — larger databases with more tables see greater improvement
- Transparent to the caching layer — no behavioral change for cached queries

### Negative
- ALTER PUBLICATION executes on the origin database — adds a brief DDL operation on registration and eviction
- Race window between ALTER PUBLICATION and replication stream requires retaining client-side filtering as a safety net
- Writer must track `publication_oids` to avoid unnecessary ALTER PUBLICATION calls

## Implementation Notes
Writer state adds `publication_name`, `publication_oids`, and `relations_dirty` to `CacheWriter`. `active_relations_rebuild()` returns `bool` to signal whether the set changed. `publication_dirty_drain()` is called at the end of both `query_command_handle()` and `cdc_command_handle()` to batch deferred removals.
