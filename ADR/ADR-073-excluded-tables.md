# ADR-073: Excluded Tables

## Status
Accepted

## Context
The table allowlist (ADR-020) restricts caching to listed tables. A database with many tables, or tables added by migrations, wants the opposite: cache everything except a few write-heavy or sensitive tables (`audit_logs`, `sessions`), without listing every other table.

The allowlist is checked at dispatch against the table names a query writes, before resolution. For an allowlist every miss of that check fails closed: a reference it does not recognise forwards. The same check for a blocklist fails open, and the excluded table would be copied into the cache database and joined to the publication, by three routes:

- a bare name the session's `search_path` resolves to the schema a schema-qualified entry names;
- a row-level security policy's own tables, which the writer adds when it inlines the policy (ADR-062) and the client query never names;
- a pinned query, registered with the writer without passing dispatch.

Alternatives considered: the dispatch check alone (leaves the routes open); a writer-only check on resolved names (exact, but a refused resolution is not remembered, so every read of an excluded table would round-trip the writer, and these are the high-traffic tables).

## Decision
- **`excluded_tables`** is a list with the entry forms of `allowed_tables` (`table`, `schema.table`, case-insensitive), settable by TOML, `--excluded_tables` and `PUT /config`. A query is cacheable when every table it references is allowed and none is excluded: exclusion wins, and both lists may be set.
- **One value, `TableFilter`**, holds both parsed lists and owns the matching. Dispatch, the writer and `--check` all ask it.
- **Dispatch checks the written names, erring toward forwarding.** A schema-qualified entry also matches a reference that names no schema, since that reference may resolve to the entry's schema.
- **The writer checks the resolved names** where a query's cache tables are ensured, the one path to a query's canonical form, and refuses the registration before the table is created. A binding kept from an earlier resolve is rechecked before it is reused.
- **A filter change at runtime evicts.** On its tick the writer evicts the cached queries on the tables the filter has come to refuse, a bounded number per tick as the periodic eviction does; a table's last release takes it out of the publication and drops its cache table as for any unreferenced table.
- **The allowlist takes the same two checks and the same eviction** (amending ADR-020, which checked at dispatch only and gated admission without evicting): the writer applies the whole filter, not the blocklist alone.
- **An `allowed_tables` entry that an exclusion covers whole is warned of**, at startup and when the filter changes, since it never takes effect.
- **A config reload resolves the file under the startup flags**, as startup did, so a list given by flag is not dropped by a file that does not name it.

## Rationale
- **Two checks, each where its input is cheap.** Dispatch has the written names on every request and forwards without the writer; the writer has the resolved names and sees what dispatch cannot. Together an excluded table never gets a cache table.
- **Over-forwarding a bare name** costs cache hits on a same-named table in another schema; the alternative is resolving at dispatch, or the writer round trip per read that the dispatch check exists to avoid.
- **Eviction rather than invalidation** at runtime: an invalidated entry is readmitted from its kept metadata on the next read without resolving again, and would keep the table in the publication. Eviction uses the existing path and ends CDC for the table.
- **Exclusion wins without a startup error**: either list can change alone through `PUT /config`, so a conflict check at startup would not hold at runtime.

## Consequences

### Positive
- Operators exclude a few tables instead of maintaining an allowlist of the rest.
- An excluded table costs origin no publication membership and no CDC, and its rows never reach the cache database, including through a policy or a pinned query.
- A runtime exclusion takes effect without a restart and reclaims the table.
- The allowlist no longer caches a table reached only through a policy or a pinned query, and narrowing it at runtime ends CDC for the tables that left it.

### Negative
- With `analytics.events` excluded, a bare `events` that resolves to another schema forwards; the query must qualify the name to be cached.
- A read that reaches an excluded table only through a policy asks the writer on every request, as other policy refusals do.
- A runtime filter change reaches such policy-only entries on the writer's next tick (up to a second), not at once, and a table with more queries than a tick's bound takes several.
- A pinned query evicted by a runtime filter change is not pinned again when the filter lets its table back in; until a restart it is an ordinary query.
- A pinned query on a table outside the allowlist, formerly populated though never served, is now skipped with a warning.
- Naming one partition does not exclude its rows from queries on the partitioned table.

## Implementation Notes
`TableFilter` lives in `settings/table_filter.rs` and replaces the parsed allowlist in `DynamicConfig` and `PreflightSettings`. Refusals count in `pgcache.queries.excluded_skipped` or `pgcache.queries.allowlist_skipped` at both checks; the writer's are `CacheError::TableExcluded` and `CacheError::TableNotAllowed`, logged as routing decisions.
