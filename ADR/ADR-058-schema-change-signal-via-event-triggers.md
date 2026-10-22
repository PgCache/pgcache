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
- A forgotten relation leaves the writer's catalog at the signal, not at the publication drain: a registration can run between a mid-transaction signal and its commit, and must reload the metadata rather than reuse the pre-DDL entry. The cache table itself is dropped at the drain, when the frame holds no locks. The drain names relations leaving the publication from the origin catalog by OID, since the name pgcache registered them under may be stale by then (`RENAME`, `SET SCHEMA`).
- **Partitions resolve through pgcache's own catalog.** Under `publish_via_partition_root` a signal on a partition concerns its cached root, and `sql_drop` cannot name the parent (the catalog row is gone by then). So table metadata carries the partition tree, loaded at registration and extended by the `CREATE TABLE … PARTITION OF` signal (which reports the parent, still visible at `ddl_command_end`); the writer resolves any signalled OID to the cached tables whose tree holds it. It also carries the ancestors, so an `ALTER TABLE` on a root (which propagates to its partitions) reaches a partition cached on its own. Attach and detach evict the root, so the next registration reloads the tree.
- **`TRUNCATE` of a partition** is not DDL and pgoutput skips it under partition root, so every partition carries a statement-level `AFTER TRUNCATE` trigger emitting the same kind of message with its own OID. The `ddl_command_end` trigger installs it on each partition created or attached later; the install sweeps existing ones. A setting mutes the DDL trigger while pgcache's own install DDL runs.
- Only `ALTER TABLE` rows count as altering a table. `CREATE TABLE` has no pre-state to evict, and its message routinely arrives after a query has registered on the new table.
- The trigger filters its own noise at the source: pgcache's `ALTER PUBLICATION` (which reports `publication relation` objects on every registration and eviction) and temp-schema DDL.
- pgcache installs the triggers at provisioning when its role may, otherwise `pgcache --check` prints the SQL for an administrator. They are never dropped at shutdown: they are shared by every pgcache on the database. Without them, behaviour is the pre-existing one.
- **The `Relation` message is the fallback signal, and it can't be read as a catalog.** Its column key flags mark the replica identity. Under DEFAULT that is the primary key. Under USING INDEX it is the identity index, and the cache keys on that index, because a CDC key tuple carries only its columns (PGC-531). Under FULL or NOTHING the flags say nothing about the key, so it is read from the origin catalog, falling back to the registered key if the catalog can't be read. A message that differs only in its replica identity updates the table's identity in place rather than reshaping it (PGC-530).

## Rationale
- **Stream order for free.** A transactional message is decoded with the data changes of its transaction, in commit order. The signal cannot race the rows it affects, and it inherits reconnect and slot semantics. No second channel, no origin table.
- **Fits the frame model.** The writer already handles a schema change arriving mid-frame (ADR-035, PGC-264); the signal is one more producer of that event, with a richer trigger set.
- **Evict-and-forget is the existing correctness path.** Registration already loads metadata from the catalog and population already rebuilds from origin, so no new consistency argument is needed. The cost is one repopulation per affected query, which is what a `Relation`-driven recreate costs today.
- **Payload over polling.** Event triggers see `pg_event_trigger_ddl_commands()` and `pg_event_trigger_dropped_objects()`, which name the objects directly. Polling catalogs would add a staleness window and load on origin.
- **Partition tree in pgcache, not snapshotted at origin.** The alternative, a `ddl_command_start` trigger serialising `pg_inherits` per `DROP` for `sql_drop` to read back, works but costs a third trigger and a whole-database catalog copy per drop. Holding the tree in table metadata is one more catalog fact pgcache already captures at registration (indexes, replica identity), kept exact by signals it already receives, and every race between the registration's catalog read and the stream falls on the safe side: a create before the read is both listed and re-announced, a drop before the read is absent and its signal ignored against post-drop data.

## Consequences

### Positive
- `DROP TABLE`, a quiet `ALTER TABLE`, partition attach/detach, and a partition's truncate or drop now invalidate at the right point in the stream.
- The same channel carries policy, grant and view objects for later consumers. View objects already have one: any change to a view reloads the write-alias lists the read-after-write gate matches on (ADR-048). Policy and grant objects are parsed and ignored.
- Degrades gracefully: no triggers, no change in behaviour, and preflight says so.

### Negative
- Every DDL in the database, cached or not, now produces a row-less frame in the writer. DDL is rare and the frame is cheap, but a migration storm is visible as frames.
- A DDL whose message arrives after a query has already registered on the post-DDL catalog is evicted needlessly. Correct, but one wasted repopulation per such race.
- Creating event triggers needs a superuser or the managed-PG master role, so for most deployments the install is an admin step.
- Role-level changes (`pg_authid`, `pg_auth_members`) are global objects that fire no event triggers and are not decoded; they remain unsignalled.
- Every partition in the database carries a `pgcache_truncate` trigger, visible in `\d`, whether or not its root is cached. Installing it inside the `ddl_command_end` trigger runs DDL as the user issuing the create or attach; a failure there is downgraded to a warning so user DDL never fails on pgcache's account.
- `TRUNCATE` of a partitioned root yields pgoutput's truncate plus one message per partition; all but the first resolve to a table already forgotten.

## Implementation Notes
Trigger SQL and install/uninstall live in `pg/event_trigger.rs`; the payload parser in `cache/cdc/schema_change.rs`; the writer reaction in `writer/cdc/dispatch.rs` next to the `Relation` handler whose mid-frame logic it shares, with the partition-tree lookup and the forgotten-table drop in `writer/table.rs`. The partition tree is `TableMetadata::partitions` and `::ancestors`, catalog-only fields refilled on the `Relation` path like indexes are. The writer's own WAL-flush marker (PGC-290) is an empty `pgcache` message and now also arrives on the stream, inside a row-less frame; the processor drops it. Tests: `tests/schema_change_test.rs`, `tests/alter_table_test.rs`.
