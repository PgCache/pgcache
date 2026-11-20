# ADR-020: Table Allowlist

## Status
Accepted

## Context
pgcache caches any query that passes cacheability analysis (`CacheableQuery::try_new`). In some deployments, the user may only want to cache a specific subset of tables that are logically related.

## Decision
Add an optional table allowlist (`allowed_tables`) that restricts caching to queries whose tables are all present in the list. When configured, only queries where **every** referenced table appears in the allowlist are admitted to the cache. Queries referencing any non-allowlisted table are forwarded directly to origin.

### Configuration

```toml
# Only cache queries referencing these tables.
# Supports both unqualified ("orders") and schema-qualified ("audit.orders") names.
# If omitted or empty, all tables are cacheable (current behavior).
allowed_tables = ["settings", "products", "inventory.categories"]
```

Also available as `--allowed_tables TABLE1,TABLE2,...` on the CLI and through `PUT /config` (ADR-028). When omitted or empty, all tables are cacheable (backward compatible default).

### Schema Qualification

Each allowlist entry is parsed into `(Option<schema>, table)`:
- `"orders"` → `(None, "orders")` — matches `orders` in **any** schema
- `"audit.orders"` → `(Some("audit"), "orders")` — matches only `audit.orders`

Matching is case-insensitive. Both the allowlist entries and query table references are lowercased before comparison.

### Enforcement Points

The allowlist is one half of the table filter (`TableFilter`, ADR-073), checked in two places:

- **At dispatch**, on every request, before the query's identity is resolved: against the table names the query writes, anywhere in its tree (FROM, joins, CTE bodies, subqueries). A query like `SELECT * FROM allowlisted WHERE EXISTS (SELECT 1 FROM non_allowlisted)` is forwarded. A reference that names no schema is admitted only by an entry that names none, since where it resolves is not known there.
- **At the writer**, where a query's cache tables are ensured: against the tables by the schema they resolved to. This refuses what dispatch cannot see, a row-level security policy's own tables (ADR-062) and a pinned query, before the table gets a cache table.

### Retention

A table that leaves the allowlist at runtime is forwarded at once by the dispatch check, and on its next tick the writer evicts the queries cached on it, which takes the table out of the publication and drops its cache table. Originally the allowlist gated admission only and left such entries to natural eviction; they were never served again but stayed maintained by CDC, and an entry reaching the table only through a policy stayed served.

## Rationale
- **Dispatch-level check** forwards a refused query without a writer round trip; the writer check covers the tables a query reaches without naming them
- **All-tables-must-match** semantics prevent partial caching of join queries where one side shouldn't be cached
- **Case-insensitive matching** with optional schema qualification handles the most common PostgreSQL naming patterns without requiring users to know the exact schema for every table
- **Backward compatible default** (no allowlist = all tables cacheable) means existing deployments are unaffected

## Consequences

### Positive
- Operators can precisely control which tables consume cache resources
- Reduces cache churn and population overhead for tables that don't benefit from caching
- Simple mental model: list the tables you want cached, everything else goes to origin

### Negative
- Users must manually maintain the allowlist as their caching needs evolve
- No wildcard or pattern matching — each table must be listed explicitly
- A schema-qualified entry does not admit an unqualified reference; the query must qualify the name to be cached

## Implementation Notes
- Config: `allowed_tables` in `SettingsToml` and `DynamicConfig`; parsing and matching in `TableFilter` (`src/settings/table_filter.rs`), shared with `excluded_tables` (ADR-073)
- Metric: `pgcache.queries.allowlist_skipped` counts each refused read, at dispatch and at the writer (`CacheError::TableNotAllowed`)
- Startup log at `info` level reports the configured allowlist or "all tables cacheable"; an entry `excluded_tables` cancels is warned of
