# ADR-058: Origin schema-change signal via event triggers and logical messages

## Status
Accepted

## Context
pgcache learns a table's shape once, at registration, and afterwards relies on the replication stream to tell it about changes. pgoutput's only schema signal is the `Relation` message, which it resends lazily, just before the relation's next row change. So an `ALTER TABLE` with no following write is invisible, `DROP TABLE` is invisible forever (PGC-71), and attaching a non-empty partition under `publish_via_partition_root` never streams the partition's existing rows (PGC-70). The same blind spot covers policies, grants and views, which the RLS and privilege work (PGC-113, PGC-463) will need.

An earlier design (PGC-233) proposed a control table in the publication that origin-side triggers would write to, so the change rode the stream as an ordinary row. It needs an origin table, and a row-level trigger per change.

## Decision
Two event triggers on origin, on `ddl_command_end` and `sql_drop`, emit each command's affected objects as a **transactional** `pg_logical_emit_message` with the `pgcache` prefix. pgcache starts replication with `messages 'true'`, decodes the message at its position inside the source transaction, and hands the writer a `SchemaChange` command listing the altered and dropped table OIDs.

- The writer's reaction is **evict and forget**: every query over a named relation is evicted, in-flight populations whose snapshot predates the commit are aborted at CommitMark (the TRUNCATE rule of ADR-035), and the cache table is dropped once nothing references it. The next registration reloads the relation's metadata from the origin catalog. Nothing is reshaped in place.
- A mid-frame signal reuses the intra-transaction DDL path of the `Relation` handler: buffered events of that relation are discarded, or the frame enters recovery if some were already written.
- A dropped relation is also removed from the writer's publication bookkeeping, since origin has already taken it out of the publication and the next `ALTER PUBLICATION` must not name it.
- Only `ALTER TABLE` rows count as altering a table. `CREATE TABLE` has no pre-state to evict, and its message routinely arrives after a query has registered on the new table.
- The trigger filters its own noise at the source: pgcache's `ALTER PUBLICATION` (which reports `publication relation` objects on every registration and eviction) and temp-schema DDL.
- pgcache installs the triggers at provisioning when its role may, otherwise `pgcache --check` prints the SQL for an administrator. They are never dropped at shutdown: they are shared by every pgcache on the database. Without them, behaviour is the pre-existing one.

## Rationale
- **Stream order for free.** A transactional message is decoded with the data changes of its transaction, in commit order. The signal cannot race the rows it affects, and it inherits reconnect and slot semantics. No second channel, no origin table.
- **Fits the frame model.** The writer already handles a schema change arriving mid-frame (ADR-035, PGC-264); the signal is one more producer of that event, with a richer trigger set.
- **Evict-and-forget is the existing correctness path.** Registration already loads metadata from the catalog and population already rebuilds from origin, so no new consistency argument is needed. The cost is one repopulation per affected query, which is what a `Relation`-driven recreate costs today.
- **Payload over polling.** Event triggers see `pg_event_trigger_ddl_commands()` and `pg_event_trigger_dropped_objects()`, which name the objects directly. Polling catalogs would add a staleness window and load on origin.

## Consequences

### Positive
- `DROP TABLE`, a quiet `ALTER TABLE`, and partition attach/detach now invalidate at the right point in the stream.
- The same channel carries policy, grant and view objects for later consumers; the parser already sees and ignores them.
- Degrades gracefully: no triggers, no change in behaviour, and preflight says so.

### Negative
- Every DDL in the database, cached or not, now produces a row-less frame in the writer. DDL is rare and the frame is cheap, but a migration storm is visible as frames.
- A DDL whose message arrives after a query has already registered on the post-DDL catalog is evicted needlessly. Correct, but one wasted repopulation per such race.
- Creating event triggers needs a superuser or the managed-PG master role, so for most deployments the install is an admin step.
- Role-level changes (`pg_authid`, `pg_auth_members`) are global objects that fire no event triggers and are not decoded; they remain unsignalled.
- Dropping a partition directly (`DROP TABLE <partition>`) names only the child, whose parent cannot be resolved in `sql_drop` (the catalog entry is gone); a partition `TRUNCATE` produces nothing under `publish_via_partition_root`. Both remain open under PGC-70.

## Implementation Notes
Trigger SQL and install/uninstall live in `pg/event_trigger.rs`; the payload parser in `cache/cdc/schema_change.rs`; the writer reaction in `writer/cdc/dispatch.rs` next to the `Relation` handler whose mid-frame logic it shares. The writer's own WAL-flush marker (PGC-290) is an empty `pgcache` message and now also arrives on the stream, inside a row-less frame; the processor drops it. Tests: `tests/schema_change_test.rs`, `tests/alter_table_test.rs`.
