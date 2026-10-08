# ADR-006: Search Path Support for Schema Resolution

## Status
Accepted

## Context

pgcache needs to correctly resolve unqualified table names (e.g., `SELECT * FROM users` without a schema prefix) to their fully-qualified forms for cache key generation and query matching. PostgreSQL uses the `search_path` session variable to determine which schemas to search when resolving unqualified identifiers.

Previously, pgcache hardcoded `"public"` as the default schema, which is incorrect when clients use custom search paths like `SET search_path TO myapp, public` or when applications rely on the default `"$user", public` configuration.

### PostgreSQL Search Path Behavior

PostgreSQL's search path has specific semantics:
- Comma-separated list of schema names
- `$user` (unquoted) is a special token that expands to the current session user's username
- `"$user"` (quoted) is a literal schema name, not the special token
- Unquoted identifiers are case-folded to lowercase
- Quoted identifiers preserve case
- PostgreSQL 18+ sends `search_path` via `ParameterStatus` messages during startup and on changes
- Pre-18 versions do not automatically report `search_path`

## Decision

Implement comprehensive search path tracking per connection with automatic fallback for older PostgreSQL versions.

### Data Model

Introduced `SearchPathEntry` enum and `SearchPath` struct in `src/proxy/search_path.rs`:

```rust
pub enum SearchPathEntry {
    Schema(String),      // Literal schema name
    SessionUser,         // Unquoted $user - resolves to session_user at query time
}

pub struct SearchPath(pub Vec<SearchPathEntry>);
```

The `SearchPath::parse()` method handles PostgreSQL's identifier quoting rules:
- Unquoted `$user` → `SessionUser` variant
- Quoted `"$user"` → `Schema("$user")` (literal)
- Unquoted identifiers → lowercased schema name
- Quoted identifiers → preserved case, unescaped `""` to `"`

The `SearchPath::resolve()` method expands `SessionUser` entries using the connection's session user.

### Connection State Tracking

`ConnectionState` tracks per-connection search path state:
- `search_path: Option<SearchPath>` - Parsed search path (if known)
- `search_path_query_pending: bool` - Waiting for SHOW query response
- `first_ready_for_query: bool` - Triggers fallback query if needed
- `session_user: Option<String>` - For resolving `$user`

### Acquisition Strategy

Two-phase approach for maximum compatibility:

**Phase 1: ParameterStatus (PostgreSQL 18+)**
Monitor `ParameterStatus` messages from the origin database during startup and throughout the session. When a `search_path` parameter is received, parse and store it.

**Phase 2: Fallback Query (Pre-18)**
On first `ReadyForQuery` after authentication, if `search_path` is still unknown:
1. Inject `SHOW search_path;` query to origin
2. Set `search_path_query_pending = true`
3. Swallow the response at the probe's ordered egress slot (ADR-055; originally an intercept state)
4. Parse value from DataRow, store parsed search path
5. Resume normal operation (messages are not forwarded to client)

### Query Resolution Integration

The resolved search path is:
1. Passed through `ProxyMessage` to the cache runtime
2. Used by `query_register()` for table resolution
3. Used by `select_statement_resolve()` to find tables in the correct schemas

When caching a query, the search path determines which schema each unqualified table reference belongs to. This resolved schema is then used for cache key generation, ensuring queries against the same table in different schemas are correctly differentiated.

If the search path is unknown at cache time, the cache returns an error rather than guessing, forcing the query to be forwarded to the origin database. This ensures correctness over availability.

### Cache Worker Execution

The cache worker executes queries against the cache database using a `WorkerRequest` that contains the **resolved AST** (with schema-qualified table names) rather than the original query. This design means:

- The cache worker's database connection doesn't need the client's search_path
- Queries are executed with fully-qualified table names (e.g., `myapp.users` instead of `users`)
- Schema resolution happens once during query registration, not on every cache hit

```rust
pub struct WorkerRequest {
    pub query_type: QueryType,
    pub data: BytesMut,
    pub resolved: ResolvedSelectStatement,  // Schema-qualified AST
    pub result_formats: Vec<i16>,
    pub client_socket: TcpStream,
    pub reply_tx: Sender<CacheReply>,
}
```

## Consequences

### Positive
- Correct schema resolution for all client search path configurations
- Transparent operation - no client code changes required
- Compatible with all PostgreSQL versions (18+ native, pre-18 via fallback)
- Per-connection isolation ensures multi-tenant scenarios work correctly
- Clean separation between parsing (SearchPath) and resolution (resolved refs)

### Negative
- Additional latency for pre-18 PostgreSQL during first query (one round-trip for SHOW)
- Memory overhead for storing search path per connection
- Complexity in tracking query interception state

### Not Implemented (Future Work)
- Tracking `SET search_path` commands mid-session for PostgreSQL < 18 (PG 18+ tracks via ParameterStatus)
- `pg_catalog` and `pg_temp` implicit schema handling
- Function-scoped search_path changes
- `SET LOCAL` + transaction rollback handling

## References
- [PostgreSQL search_path documentation](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH)
- [PostgreSQL Protocol: ParameterStatus](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC)
