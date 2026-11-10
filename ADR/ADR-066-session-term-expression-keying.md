# ADR-066: Session terms in client queries by expression keying

## Status
Accepted

## Context
A client query that reads the session, `WHERE tenant = current_setting('app.tenant')::int` or `WHERE owner = current_user`, converts to a tree whose leaf is a `SessionTerm` literal (ADR-062). Until this decision only the writer's RLS inliner substituted such a term, inside policy text, so a client query carrying one forwarded on every execution (PGC-580). The JWT pattern Supabase and PostgREST use puts the term inside an expression: a policy or a client predicate reads `(current_setting('request.jwt.claims', true)::jsonb ->> 'sub')::uuid`, in the shipped helpers wrapped in `nullif`, `coalesce` and a FROM-less `(select auth.uid())`.

Three facts shape where the substitution can happen. The cacheability verdict is interned per statement text and shared by every connection, so anything session-specific must happen on the relay path, as the RLS key already does. The raw setting is the wrong key for the JWT pattern: every token differs in `exp` and `iat`, so keying on `request.jwt.claims` would make each token its own alias and writer round trip although all tokens of one user fold to the same `sub`. And the writer owns the canonical form, so the literals it substitutes must be exactly the ones the proxy keyed on, or two equal sessions could register two fingerprints.

## Decision
- **Fragments, not terms.** At verdict time a cacheable query records its term fragments: the maximal subtrees over a session term built only of kinds the proxy evaluates itself — a cast, `coalesce`, `nullif`, the json access operators `->`, `->>`, `#>`, `#>>` on a json literal with literal keys, and a FROM-less single-item scalar subquery, in scalar or predicate position. A term inside anything else is a fragment on its own, and a fragment never holds a bind parameter. Only a customized setting (a dotted name) is a term the proxy folds: a built-in setting changes under `SET` with no custom shadow to fold from, so `current_setting('timezone')` stays a bare term and the query forwards, as ADR-062 decided for policies.
- **The proxy folds, the key carries the literals.** On the relay path the connection tracks the settings the fragments read, as it does for a secured relation's policies, and folds every fragment to a literal under its session values with origin's semantics: NULL through `coalesce`, `nullif` and the access operators, a cast of a plain string to a cast literal, a cast to `json`/`jsonb` parsing the text, `->` and `#>` yielding a json value (a json null included), a text extraction rendering only what json and jsonb render alike. The folded literals, in fragment order, are part of the read's key beside the search_path, the literal settings and the RLS context. A fragment the session cannot supply a value for (a setting unknown or unset without `missing_ok`, the role unknown) or that the fold cannot evaluate (a decimal rendered as text, an object as text, a document that is not json, a cast over a cast literal, `nullif` over a typed operand, a negative array index) forwards the read with origin's own answer.
- **The writer substitutes the recorded fragments.** The fragment roots travel with the cacheable query and follow their nodes through parameter substitution, so the writer replaces exactly the subtrees the proxy folded, by the literals in the key's order, evaluating nothing; the canonical form, literal canonicalisation, fingerprinting and CDC maintenance then see a plain literal. A count mismatch refuses the read.
- **A tracked `set_config` is not a write.** A SELECT whose built-in `set_config` calls all name their setting by a literal changes no table: its effect is applied by name, it does not enter the read-after-write log, and it leaves the isolation default alone unless it names it. A value that arrives as a bind parameter is resolved from the portal at Execute, so a request that sets its claims by parameter needs no settings probe before its first read; an `is_local` that is not a literal leaves the value unknown and lasting until the probe. A `set_config` of another schema is a user's function and keeps the volatile-call treatment.

## Rationale
- **Folding at the proxy is what makes the key the claim.** Two tokens of one user produce the same literal list, so one alias and one fingerprint; the alias map grows with distinct values, not with tokens. Context keying with a writer-side fold would have kept the RLS mechanism as it is at the cost of a writer round trip per token, the cost this exists to remove.
- **The fragment set is decided once.** Re-deriving fragments on the bound tree would pair the wrong subtrees: `coalesce(current_setting('app.tenant', true), $1)` has the bare term as its fragment under the proxy, where the parameter breaks the shape, and the whole `coalesce` once `$1` is a literal. Carrying the roots keeps one fragment set on both sides; a parameter substitutes to one leaf, so each root lands in one place.
- **The fold is fail-closed.** Everything it declines, origin renders; a declined fragment costs a forward, never a wrong literal.
- **One fold for policies and client queries.** The RLS inliner's trivial substitution is the degenerate fragment; slice 2 of PGC-113 moves policies onto the same fold.

## Consequences

### Positive
- `current_setting('app.tenant')::int`, `current_user` and a claim extracted from `request.jwt.claims` in a client's own predicate or SELECT list are cached per value and shared by sessions with equal values, with results equal to origin's for each.
- The PostgREST request shape, a block that sets the claims by `set_config` and reads, is served from the second request on.

### Negative
- A fragment's literal is part of every read's key hash and of the writer's canonical form, so a query over a high-cardinality term (a per-user claim) has an entry per value, like a query over a per-user literal.
- The fold covers the forms the Supabase helpers use; a term under another function (`lower(current_setting(…))`) is substituted as a bare term and the function runs on the cache database, which is what origin does too.
- `auth.uid()` itself stays forwarded until its SQL body is inlined (PGC-579); the fold is ready for what the inlining produces.

## Implementation Notes
- Shape: `query/transform/term_fold.rs` (`scalar_is_term_fragment`, `where_subquery_is_term_fragment`, `query_tree_term_fragments`); cacheability accepts a fragment where a literal is accepted (`cache/query/cacheability.rs`), and `CacheableQuery` records the fragments.
- Fold and substitution: `query/transform/term_eval.rs` (`query_tree_term_fragments_fold`, `query_tree_term_fragments_replace`), proxy-only.
- Keying: `KeyingSettings::term_values` (`query/keying_settings.rs`), built by `session_keying_apply` in `proxy/connection/rls.rs`; the writer substitutes in `registration/resolve.rs` (`query_bind_canonical`).
- `set_config` effects: `proxy/query.rs` (`set_config_calls_effect`, the write-class exemption), `CustomSettingEffect::parameter` resolved in `proxy/connection/extended.rs`.
- Tests: `tests/session_term_test.rs`; the fold's unit tests in `term_eval.rs`.
