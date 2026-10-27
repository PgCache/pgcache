# ADR-062: Row-level security by policy inlining

## Status
Accepted

## Context
A table with row-level security filters each role's reads through its policies. pgcache populates as its own role and serves rows to any role, so until this decision a secured table was never cached: registration refused it and every read forwarded (PGC-112). The policies origin applies depend on the effective role (its attributes, ownership of the table, `FORCE`, membership in each policy's role list), on `row_security`, and on whatever the policy expressions read, which in the deployments that matter is `current_user` or a custom setting such as `app.tenant_id` carrying the tenant (PGC-113).

Two ways to cache under policies were considered. Role-keyed entries populated as the client role make every insert into a secured table an invalidation, since pgcache cannot evaluate a policy it has not modelled against a CDC row, and need pgcache to be a member of every client role. Inlining the policies as an ordinary predicate keeps CDC maintenance and subsumption as they are. Where to inline was decided late: the proxy sees the session but resolves names only against what the writer publishes and has no catalog, while the writer is already on the path of every first read, resolving the session key to its canonical fingerprint.

## Decision
- **The writer inlines, at resolution.** Once a read's base query is bound, every reference of a secured relation (`relrowsecurity`, carried on the table metadata) takes the policies that apply to the read's role, ANDed onto the WHERE of the SELECT that holds the reference. The inlined tree is the canonical form: it is resolved again, fingerprinted, populated and maintained like any other query, and the alias from the read's key leads to it.
- **The proxy widens the key.** The writer publishes the names of the secured relations with the custom settings their policies read. A read naming one is keyed on the effective role, `row_security`, and the session's values of those settings, carried on the request as the context the writer inlines under. Two sessions with equal values share the inlined entry; a tenant or role with different values has its own.
- **Applicability is origin's answer.** One catalog read per (role, relation) at resolution: exempt when the role is a superuser, has `BYPASSRLS`, or owns the table without `FORCE`; otherwise the policies whose role list the role has the privileges of (`pg_has_role(…, 'USAGE')`). Permissive policies are ORed, restrictive ones ANDed on, as origin combines them; no permissive policy is default deny.
- **Session terms are placeholders.** `current_user`, `current_role`, `user`, `session_user` and `current_setting(<literal>[, missing_ok])` convert to a literal variant like a bind parameter and are substituted from the context before the predicate is checked for cacheability. An unsubstituted term in a client's own query keeps the query uncacheable.
- **Custom settings are tracked per connection** with the same session-and-block state as the fixed settings: a literal `SET`, `SET LOCAL` or `set_config` sets the value at once; `RESET`, `TO DEFAULT`, a computed value or `RESET ALL` leaves it to the settings probe, which gains one `current_setting(name, true)` column per tracked name, NULL meaning never set. A read over a secured relation makes the connection track the settings its policies read.
- **Refusals forward, by reason.** A term the session cannot supply (a role or setting unknown, a setting unset where `missing_ok` is false), no applicable permissive policy, `row_security` off under an applicable policy, a policy outside the cacheable subset, and a secured reference on the nullable side of an outer join each refuse the registration and count under `pgcache.queries.rls_forwards` (`rls_skipped` for an uninlinable policy). Population needs to see every row, so the writer reads at start whether its role is a superuser or has `BYPASSRLS` and, when it is neither, inlines nothing: every read over a secured relation forwards under the uninlinable counter, with a warning in the log, and preflight names the `ALTER ROLE` to run.
- **Changes reach the inlined form through the existing signals.** `CREATE`, `ALTER` and `DROP POLICY` arrive as policy objects on the schema-change stream (ADR-058), resolved to their table by identity: the table's queries are evicted and its policies reloaded. `ALTER TABLE` reloads a table's row-security state; a row-security flip seen through the `Relation` message evicts likewise. A role or membership change seen by the role poll (ADR-061) clears every RLS-keyed alias, so each next read resolves again onto the inlined form that now applies, without repopulating entries whose form did not change.

## Rationale
- **The writer has what inlining needs.** Exact name resolution through the catalog (an uncached table shadowing a secured one on the search path cannot mislead it), the role's applicability from origin in the same step, and the first read of every key already passing through it. The proxy's part, widening the key, needs no verdict and no policy, so a role's first read registers instead of forwarding.
- **Natural keying at the entry level.** The alias is per role and settings, the entry per inlined tree: tenant 7's roles share one entry, and an exempt role shares the plain query's.
- **CDC and subsumption are untouched.** The inlined predicate is a conjunct on the relation's reference, so a single-table secured query is maintained in place and the constraint index keys it like any filter.
- **A policy subquery costs what it would cost origin.** Origin evaluates policy expressions with the querying role's privileges, so the tables a policy reads join the query's privilege needs through the inlined tree and need nothing more.

## Consequences

### Positive
- Secured tables are cached for the common tenant-in-a-setting and `current_user` policies, with results equal to origin's for each role and tenant, kept in place by CDC.
- Every refusal is a forward with origin's own answer, counted by reason.

### Negative
- Policies reading a JWT claims setting (`current_setting('request.jwt.claims')::jsonb ->> 'sub'`, the Supabase pattern) are not inlined yet: the whole claims literal differs per token, so the extraction must fold to the claim first, and the `auth.uid()` wrapper is a SQL function body to inline. Both are the next slice.
- A secured relation on the nullable side of an outer join forwards; the predicate belongs in the ON clause there, which this decision leaves out. So does a policy whose subquery reads a relation with policies of its own, or the policy table again, or a built-in setting: each is kept as uninlinable rather than modelled.
- A read of a secured relation keyed before the writer published its name carries no context and forwards once; the writer publishes on discovering it.
- A role change re-resolves every RLS-keyed read once; a policy change repopulates the table's entries.
- One more catalog read per (role, secured relation) at resolution, and the settings probe grows by a column per tracked custom setting.

## Implementation Notes
`cache/writer/rls.rs` loads policies (`pg_get_expr` under the writer connection's search_path, table names qualified, bound to find the policy table's own columns), decides applicability and rewrites the bound tree; `registration/resolve.rs` calls it between the first and the second resolution. `query/ast/types.rs` has the `SessionTerm` literal, `query/transform/session_terms.rs` its substitution and context types. The proxy side is `proxy/connection/rls.rs` (the key), `proxy/session_settings.rs` (custom settings and probe columns) and `query/keying_settings.rs` (`RlsKeying`); the published names are `RlsRelations` on the state view. Policy objects are parsed in `cache/cdc/schema_change.rs`. Tests: `tests/rls_test.rs`, and the rewrite's unit tests in `cache/writer/rls.rs`.
