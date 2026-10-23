# ADR-061: Hit-path privilege verdicts

## Status
Accepted

## Context
A forwarded query runs on origin as the client's own role, so origin enforces table, column, schema and function privileges. A cache hit runs on the cache database as pgcache's user, against rows populated as pgcache's origin user, and until this decision it was decided on the fingerprint and session settings alone: any role that could connect was served any cached result whose key matched (PGC-463). The effective role was already tracked per connection (ADR-056) but reached nothing.

Three shapes were considered. Keying entries on the role multiplies entries per role and still populates as pgcache's user. Probing the client's own origin connection for `has_table_privilege` answers under the exact session but needs a round trip from dispatch back to the connection, since only the writer knows which relations a query touches. Asking origin on pgcache's own connection with the role's name as the argument gives the same answer, because the `has_*_privilege(role, object, privilege)` family evaluates inherited membership exactly as origin does at execution.

## Decision
- **The effective role travels with the read.** `role` unless `none`, else `session_authorization`, else the startup user, at the read's scope (a `SET LOCAL ROLE` counts inside its block). An unknown value already forwards.
- **Every Ready entry publishes what a read of it needs**: the columns it references per base relation, keyed by OID, and the names of the functions it calls. A subsumed query has its own entry and its own needs, so the check is against the request's columns, not the broader entry's.
- **Verdicts are cached per (role, object) in the shared state view**: for a relation, `USAGE` on its schema plus `SELECT` on the table (`Full`), or the set of granted columns, or denied; for a function name, `EXECUTE` on every overload, as the volatility map takes the worst case over overloads. A read referencing no column of a relation needs `SELECT` on the table or on any one column, which is what origin requires.
- **Two serve points gate on the verdict**: a direct Ready hit, and the coalesce drain for each waiter's role. The memo and the MV path sit behind the hit gate. A denied read forwards, and origin answers with its own error. A read with no verdict forwards and asks the writer to look the verdicts up; the registering role's verdicts are fetched before its query is published Ready, so its first hit is not forwarded.
- **The writer owns the lookup**, as it owns `Resolve`: one catalog read per lookup on its origin connection, recorded in the store. A failed read clears the pending marks so the next read asks again. A role that does not exist is denied.
- **Every DDL prunes the store, by visibility.** The schema-change signal (ADR-058) now reaches the writer for any `ddl_command_end` or `sql_drop` message, not only those naming a table. Tags are not enumerated: an owner change, a `SET SCHEMA` or a `REASSIGN OWNED` changes who may read as surely as a `GRANT`. Truncate signals cannot change privileges and are ignored. Each lookup reads `pg_current_snapshot()` before its catalog reads and stores it with the verdicts; the writer knows each source transaction's xid from the stream's `BEGIN`, so applying a DDL drops exactly the verdicts whose snapshot could not see that transaction, and keeps the ones whose read already reflected it. A result that arrives after a DDL is validated the same way against the DDL applied since its check, through a bounded log; a lookup older than the log is dropped.

## Rationale
- **Origin's own computation.** The privilege functions with a role-name argument fold in role membership, inheritance and superuser status; pgcache never models any of it, and never needs membership in client roles.
- **Natural cost profile.** One catalog read per (role, relation) first sight, then a map lookup per hit. Grants are rare, so a whole flush per DDL costs a handful of forwards, far less than the repopulation an `ALTER TABLE` already triggers.
- **Name resolution stays origin's.** Origin drops schemas a role lacks `USAGE` on from its effective search path, while the writer resolves names as pgcache's user. Requiring `USAGE` on the resolved table's schema closes the gap: with it, origin resolves the same table; without it, the read forwards.
- **Partitioned tables need no special case.** Origin checks privileges on the table a query names, not on its partitions, and that is the OID pgcache registers under.

## Consequences

### Positive
- A role is served from cache only what origin would let it read, including column-level and schema-level grants, under `SET ROLE` and `SET SESSION AUTHORIZATION`.
- A `REVOKE` takes effect at the next read once its signal is applied; a `GRANT` likewise.
- No role in the memo key, no per-role entries, no change to population or CDC.

### Negative
- A role's first hit on each relation, and every role's first hit after a DDL its verdict predates, is forwarded while the verdict is looked up. `pgcache.queries.privilege_forwards` counts these by reason. A pinned query has no registering role, so its first hit per role forwards once.
- Role-level changes are not signalled: membership grants, `ALTER ROLE … SUPERUSER`, or a role dropped and recreated under the same name leave stale verdicts until a later DDL the verdict's read did not see. This is the open decision carried from ADR-058.
- Without the schema-change triggers, a `GRANT` or `REVOKE` is not observed at all until a DDL that is.
- A function's `EXECUTE` is checked by name across every overload, so a role denied one overload is denied the name. Operators carry no privilege of their own and are not checked.
- The store is keyed by role name, so it cannot tell a recreated role from the original.

## Implementation Notes
`cache/privileges.rs` is the store and its check; `writer/privileges.rs` the lookup and the registration-time prefetch; `query/resolve/analysis.rs` derives the needs beside the read-after-write gate's column map, which stays name-keyed. The gate is `privilege_gate` in `cache/query_cache/dispatch.rs`, applied in `ready_hit` and in the coalesce drain. The flush is in the writer's schema-change handler. Tests: `tests/privilege_test.rs`.
