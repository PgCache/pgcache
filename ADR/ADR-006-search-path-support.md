# ADR-006: Search Path Support for Schema Resolution

## Status
Accepted. The acquisition strategy (ParameterStatus, fallback `SHOW`, piggyback) is superseded by ADR-056; the parsing rules and the Cache Key section stand.

## Context

pgcache needs to correctly resolve unqualified table names (e.g., `SELECT * FROM users` without a schema prefix) to their fully-qualified forms for cache key generation and query matching. PostgreSQL uses the `search_path` session variable to determine which schemas to search when resolving unqualified identifiers.

Previously, pgcache hardcoded `"public"` as the default schema, which is incorrect when clients use custom search paths like `SET search_path TO myapp, public` or when applications rely on the default `"$user", public` configuration.

### PostgreSQL Search Path Behavior

PostgreSQL's search path has specific semantics:
- Comma-separated list of schema names
- `$user`, quoted or not, is a special token that expands to the current session user's username (PostgreSQL strips the quotes before matching it, which is how the default `"$user", public` works)
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
- `$user`, quoted or unquoted → `SessionUser` variant
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

If the search path is unknown at cache time, the query is forwarded to the origin database rather than guessed at. This ensures correctness over availability.

### Cache Key

A cached query's identity (`Fingerprint`) is the hash of its *resolved* form: schema-qualified tables and, since the PGC-287 class 2 slice, canonical literals (`resolved_query_fingerprint`). Only the writer can compute that, since resolution needs the catalog. The proxy therefore keys dispatch on a `SessionKey`: the AST hash folded with the session's resolved search_path, computed without the catalog. Every query goes through the alias map; a bare and a qualified spelling of one table share the entry.

A shared alias map (`SessionKey → Fingerprint`) on the cache state view joins the two. On an alias miss the dispatch sends `QueryCommand::Resolve`; the writer resolves the query against the catalog (table schemas memoized per path and name in `schema_resolutions`), records the alias, and replies with the fingerprint. Registration resolves the same way and asserts it reached the same identity. This happens once per (query, path) pair process-wide; later requests, from any connection, read the alias. The writer is the only party that writes the map; aliases whose fingerprint has left both query maps are swept with the stale-entry cleanup, and dispatch treats an alias to a missing entry as an ordinary cold miss under that fingerprint.

Unqualified function, type and operator names are not part of the key. The cache database holds no user objects, so a query can only be served with builtin semantics; the one way origin can differ for the same text is a path that lists `pg_catalog` after another schema, where a user object with a builtin's exact signature shadows it. Such a session forwards (`SearchPathState::BuiltinsShadowed`). A schema-qualified function call is cacheable only when the schema is `pg_catalog`; the qualifier stays on the AST and in the fingerprint.

Keying on the resolved schema rather than on the path itself means two paths that resolve a query the same way share one entry, and pinned queries (resolved under `public` at startup) are reached by sessions on any path. Keying on the path alone was rejected: it duplicates every entry per distinct resolved path (the default `"$user", public` resolves differently per role) and leaves pinned entries unreachable from the default path. Resolution consults the origin catalog in path order; it must not short-circuit on an already-cached table in a later schema, because an earlier schema may shadow it.

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
- `pg_catalog` and `pg_temp` implicit schema handling
- Function-scoped search_path changes

## References
- [PostgreSQL search_path documentation](https://www.postgresql.org/docs/current/ddl-schemas.html#DDL-SCHEMAS-PATH)
- [PostgreSQL Protocol: ParameterStatus](https://www.postgresql.org/docs/current/protocol-flow.html#PROTOCOL-ASYNC)
