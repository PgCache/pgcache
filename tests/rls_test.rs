//! Row-level security by policy inlining (PGC-113). The policies that apply
//! to the effective role are folded into the query as an ordinary predicate,
//! with the session terms they read (`current_setting`, `current_user`)
//! replaced by the session's values, before the query is keyed; a tenant is
//! an entry of its own, two sessions of one tenant share it, and CDC keeps it
//! in place. A role the policies exempt (owner, `BYPASSRLS`, superuser) reads
//! the whole table from cache. What cannot be inlined forwards: an unset
//! setting, no applicable permissive policy, `row_security = off`, or a
//! policy outside the cacheable subset.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::{Client, SimpleQueryMessage, error::SqlState};

use crate::util::{
    MetricsSnapshot, TestContext, assert_cache_hit, assert_cache_miss, connect_cache_db,
    metrics_delta, query_warm, result_rows,
};

mod util;

type Rows = Vec<Vec<Option<String>>>;

const TENANT_READ: &str = "SELECT id, body FROM tenant_docs ORDER BY id";
const OWNED_READ: &str = "SELECT id, body FROM owned_docs ORDER BY id";
const JUDGED_READ: &str = "SELECT id, body FROM judged_docs ORDER BY id";
const CLAIMED_READ: &str = "SELECT id, body FROM claimed_docs ORDER BY id";
const NULLED_READ: &str = "SELECT id, body FROM nulled_docs ORDER BY id";

const ANN_TOKEN: &str = r#"{"sub":"ann","role":"authenticated","exp":1700000000}"#;
const ANN_TOKEN_LATER: &str = r#"{"sub":"ann","role":"authenticated","exp":1700000999}"#;
const BOB_TOKEN: &str = r#"{"sub":"bob","role":"authenticated","exp":1700000000}"#;

/// Supabase's `auth.uid()`, body as shipped: a STABLE sql function over the
/// claims setting, inlined into the policies that call it (PGC-579).
const AUTH_UID: &str = "CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$ \
       select coalesce( \
         nullif(current_setting('request.jwt.claim.sub', true), ''), \
         (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub') \
       )::uuid \
     $$";
const ANN_UUID_TOKEN: &str =
    r#"{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated","exp":1700000000}"#;
const ANN_UUID_TOKEN_LATER: &str =
    r#"{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated","exp":1700000999}"#;
const BOB_UUID_TOKEN: &str =
    r#"{"sub":"22222222-2222-2222-2222-222222222222","role":"authenticated","exp":1700000000}"#;
/// A valid token of a user who owns nothing.
const STRANGER_UUID_TOKEN: &str =
    r#"{"sub":"33333333-3333-3333-3333-333333333333","role":"authenticated","exp":1700000000}"#;
/// PostgREST's claims for an unauthenticated request.
const ANON_TOKEN: &str = r#"{"role":"anon"}"#;
const UID_READ: &str = "SELECT id, body FROM uid_docs ORDER BY id";

/// `public.tenant_of`: on the writer connection's search_path, so a policy
/// calling it deparses the bare name.
const TENANT_OF: &str = "CREATE FUNCTION public.tenant_of(id int) RETURNS int \
     LANGUAGE sql IMMUTABLE AS $$ SELECT id % 2 $$";

/// `tenant_docs`, readable by `tenant_user` and `tenant_peer`, under a
/// permissive policy selecting the tenant named by `app.tenant_id`.
async fn tenant_docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE tenant_docs (id int primary key, tenant int, body text)",
        "INSERT INTO tenant_docs VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'c')",
        "CREATE ROLE tenant_user LOGIN",
        "CREATE ROLE tenant_peer LOGIN",
        "GRANT SELECT ON tenant_docs TO tenant_user, tenant_peer",
        "ALTER TABLE tenant_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY tenant_policy ON tenant_docs FOR SELECT \
         USING (tenant = current_setting('app.tenant_id')::int)",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await
}

/// `owned_docs`, readable by `alice` and `bob`, each seeing the rows they
/// own.
async fn owned_docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE owned_docs (id int primary key, owner text, body text)",
        "INSERT INTO owned_docs VALUES (1, 'alice', 'a1'), (2, 'bob', 'b1'), (3, 'alice', 'a2')",
        "CREATE ROLE alice LOGIN",
        "CREATE ROLE bob LOGIN",
        "GRANT SELECT ON owned_docs TO alice, bob",
        "ALTER TABLE owned_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY owner_policy ON owned_docs FOR SELECT USING (owner = current_user)",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await
}

/// `claimed_docs`, readable by `claimant`, under the Supabase-style policy
/// selecting the owner named by the `sub` claim of `request.jwt.claims`.
async fn claimed_docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE claimed_docs (id int primary key, owner text, body text)",
        "INSERT INTO claimed_docs VALUES (1, 'ann', 'a'), (2, 'bob', 'b'), (3, 'ann', 'c')",
        "CREATE ROLE claimant LOGIN",
        "GRANT SELECT ON claimed_docs TO claimant",
        "ALTER TABLE claimed_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY claim_policy ON claimed_docs FOR SELECT \
         USING (owner = (current_setting('request.jwt.claims', true)::jsonb ->> 'sub'))",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await
}

/// `nulled_docs`, readable by `nulled_user`, under a policy whose term sits
/// in a typed `nullif`: a shape the fold refuses, so the term is substituted
/// on its own and the rest is left to the database, as before PGC-580.
async fn nulled_docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE nulled_docs (id int primary key, tenant int, body text)",
        "INSERT INTO nulled_docs VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'c')",
        "CREATE ROLE nulled_user LOGIN",
        "GRANT SELECT ON nulled_docs TO nulled_user",
        "ALTER TABLE nulled_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY nulled_policy ON nulled_docs FOR SELECT \
         USING (tenant = nullif(current_setting('app.tenant_id')::int, 0))",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await
}

/// pgcache started over `public.tenant_of`, so its catalog knows the
/// function from the start, then `app.judged_docs`, readable by
/// `judged_user`, under a policy that calls it; the cache database holds the
/// function too.
async fn judged_docs_setup() -> Result<TestContext, Error> {
    let mut ctx = TestContext::setup_before_start(|origin| async move {
        for sql in ["CREATE SCHEMA app", TENANT_OF] {
            origin.simple_query(sql).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await?;
    for sql in [
        "CREATE TABLE app.judged_docs (id int primary key, body text)",
        "INSERT INTO app.judged_docs VALUES (1, 'a'), (2, 'b'), (3, 'c')",
        "CREATE ROLE judged_user LOGIN",
        "GRANT USAGE ON SCHEMA app TO judged_user",
        "GRANT SELECT ON app.judged_docs TO judged_user",
        "ALTER TABLE app.judged_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY odd_docs ON app.judged_docs FOR SELECT USING (tenant_of(id) = 1)",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    connect_cache_db(&ctx.dbs)
        .await?
        .simple_query(TENANT_OF)
        .await
        .map_err(Error::other)?;
    ctx.cdc_apply_settle().await?;
    Ok(ctx)
}

/// pgcache started over the `auth` schema and `auth.uid()`, then `uid_docs`,
/// readable by `uid_user`, under the Supabase policy `owner = auth.uid()`.
async fn uid_docs_setup() -> Result<TestContext, Error> {
    let ctx = TestContext::setup_before_start(auth_schema_create).await?;
    uid_docs_create(ctx).await
}

/// The `auth` schema and `auth.uid()` on origin before pgcache starts, so
/// its function catalog knows the function from the start.
async fn auth_schema_create(origin: Client) -> Result<Client, Error> {
    for sql in ["CREATE SCHEMA auth", AUTH_UID] {
        origin.simple_query(sql).await.map_err(Error::other)?;
    }
    Ok(origin)
}

/// `uid_docs`, readable by `uid_user`, under the Supabase policy
/// `owner = auth.uid()`, on a context started over the `auth` schema.
async fn uid_docs_create(mut ctx: TestContext) -> Result<TestContext, Error> {
    for sql in [
        "CREATE TABLE uid_docs (id int primary key, owner uuid, body text)",
        "INSERT INTO uid_docs VALUES \
           (1, '11111111-1111-1111-1111-111111111111', 'a'), \
           (2, '22222222-2222-2222-2222-222222222222', 'b'), \
           (3, '11111111-1111-1111-1111-111111111111', 'c')",
        "CREATE ROLE uid_user LOGIN",
        "GRANT USAGE ON SCHEMA auth TO uid_user",
        "GRANT SELECT ON uid_docs TO uid_user",
        "ALTER TABLE uid_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY uid_policy ON uid_docs FOR SELECT USING (owner = auth.uid())",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    Ok(ctx)
}

/// Through the proxy as `user`, and directly on origin as `user`, both with
/// `app.tenant_id` set to `tenant` when given.
async fn sessions_as(
    ctx: &TestContext,
    user: &str,
    tenant: Option<&str>,
) -> Result<(Client, Client), Error> {
    let set = tenant.map(|tenant| format!("SET app.tenant_id = '{tenant}'"));
    sessions_with(ctx, user, set.as_deref().as_slice()).await
}

/// Through the proxy as `user`, and directly on origin as `user`, each with
/// `statements` run first.
async fn sessions_with(
    ctx: &TestContext,
    user: &str,
    statements: &[&str],
) -> Result<(Client, Client), Error> {
    let proxied = ctx.proxy_client_connect_as(user).await?;
    let origin = ctx.origin_connect_as(user).await?;
    for sql in statements {
        proxied.simple_query(sql).await.map_err(Error::other)?;
        origin.simple_query(sql).await.map_err(Error::other)?;
    }
    Ok((proxied, origin))
}

fn token_set(token: &str) -> String {
    format!("SELECT set_config('request.jwt.claims', '{token}', false)")
}

async fn rows(client: &Client, sql: &str) -> Result<Rows, Error> {
    Ok(result_rows(
        &client.simple_query(sql).await.map_err(Error::other)?,
    ))
}

/// Register `sql` through `client` into a steady cache hit (miss, settle,
/// hit) and return the rows the hit served.
async fn register(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<Rows, Error> {
    let m = ctx.metrics().await?;
    client.simple_query(sql).await.map_err(Error::other)?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    let served = rows(client, sql).await?;
    assert_cache_hit(ctx, m).await?;
    Ok(served)
}

/// Read `sql` through `client` until one read's metrics delta satisfies
/// `done`, returning that read's rows.
async fn read_until(
    ctx: &mut TestContext,
    client: &Client,
    sql: &str,
    done: impl Fn(&MetricsSnapshot) -> bool,
) -> Result<Rows, Error> {
    for _ in 0..40 {
        let before = ctx.metrics().await?;
        let res = client.simple_query(sql).await.map_err(Error::other)?;
        let after = ctx.metrics_after_outcomes(&before, 1).await?;
        if done(&metrics_delta(&before, &after)) {
            return Ok(result_rows(&res));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::other(format!(
        "no read of `{sql}` reached the expected outcome"
    )))
}

/// `sql` through `client` becomes a cache hit without a registration of its
/// own: a role's first read may forward while its verdicts are looked up
/// (counted as a miss, like every forward), but nothing else misses.
async fn hit_wait(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<Rows, Error> {
    let before = ctx.metrics().await?;
    let served = read_until(ctx, client, sql, |delta| delta.queries_cache_hit == 1).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_miss, delta.queries_privilege_forward_pending,
        "served from an entry that already existed"
    );
    Ok(served)
}

/// One read of `sql` through `client` forwards for the RLS `reason`, and the
/// result (rows or error) is origin's own.
async fn rls_forward_assert(
    ctx: &mut TestContext,
    client: &Client,
    sql: &str,
    reason: impl Fn(&MetricsSnapshot) -> u64,
) -> Result<Result<Vec<SimpleQueryMessage>, tokio_postgres::Error>, Error> {
    let before = ctx.metrics().await?;
    let result = client.simple_query(sql).await;
    let mut delta = MetricsSnapshot::default();
    for _ in 0..40 {
        delta = metrics_delta(&before, &ctx.metrics().await?);
        if reason(&delta) >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(reason(&delta), 1, "forwarded for the RLS reason: {sql}");
    assert_eq!(delta.queries_cache_hit, 0, "never served from cache: {sql}");
    Ok(result)
}

fn assert_sqlstate(
    result: Result<Vec<SimpleQueryMessage>, tokio_postgres::Error>,
    state: &SqlState,
    what: &str,
) {
    let got = result.as_ref().ok().map(|messages| result_rows(messages));
    let err = result.expect_err(&format!("{what}: expected an error, got rows {got:?}"));
    assert_eq!(err.code(), Some(state), "{what}: {err}");
}

/// The writer's role poll has seen a role catalog change since `before`.
async fn role_change_wait(ctx: &mut TestContext, before: &MetricsSnapshot) -> Result<(), Error> {
    for _ in 0..100 {
        if ctx.metrics().await?.cache_role_changes > before.cache_role_changes {
            return Ok(());
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err(Error::other("the role poll never saw the role change"))
}

fn row(id: &str, body: &str) -> Vec<Option<String>> {
    vec![Some(id.to_owned()), Some(body.to_owned())]
}

/// The tenant named by `app.tenant_id` is inlined into the read: each tenant
/// is an entry of its own serving origin's rows for that tenant, and
/// switching back to a tenant already cached is an immediate hit.
#[tokio::test]
async fn test_tenant_setting_policy_is_inlined_and_cached() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;

    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, TENANT_READ).await?);

    for client in [&user, &origin] {
        client
            .simple_query("SET app.tenant_id = '2'")
            .await
            .map_err(Error::other)?;
    }
    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin, TENANT_READ).await?);

    user.simple_query("SET app.tenant_id = '1'")
        .await
        .map_err(Error::other)?;
    let m = ctx.metrics().await?;
    assert_eq!(
        rows(&user, TENANT_READ).await?,
        vec![row("1", "a"), row("3", "c")]
    );
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A tenant setting the policy's cast rejects (`abc` as an integer) is a
/// value origin rejects for every role the policy applies to: the read
/// forwards with origin's error and registers nothing, where a blind fold
/// registered a form whose population failed on every read (PGC-596). A
/// role the policy exempts never evaluates it and is served from cache.
#[tokio::test]
async fn test_value_a_policy_cast_rejects_forwards_where_the_policy_applies() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    for sql in [
        "CREATE ROLE docs_owner LOGIN",
        "ALTER TABLE tenant_docs OWNER TO docs_owner",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;

    let (user, origin_user) = sessions_as(&ctx, "tenant_user", Some("abc")).await?;
    let before = ctx.metrics().await?;
    let result = rls_forward_assert(&mut ctx, &user, TENANT_READ, |d| {
        d.queries_rls_forward_unsupported
    })
    .await?;
    assert_sqlstate(
        result,
        &SqlState::INVALID_TEXT_REPRESENTATION,
        "abc is not an integer",
    );
    assert_sqlstate(
        origin_user.simple_query(TENANT_READ).await,
        &SqlState::INVALID_TEXT_REPRESENTATION,
        "origin's own answer",
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.cache_writer_register_binds, 0,
        "nothing registered\n{delta:#?}"
    );

    let (owner, _) = sessions_as(&ctx, "docs_owner", Some("abc")).await?;
    assert_eq!(
        register(&mut ctx, &owner, TENANT_READ).await?,
        vec![row("1", "a"), row("2", "b"), row("3", "c")]
    );
    Ok(())
}

/// A token whose `sub` is not a uuid under `owner = auth.uid()`: the body's
/// `::uuid` rejects it on origin for every read the policy applies to, so
/// the read forwards with origin's error and registers nothing (PGC-596).
/// The session's first reads may forward while the claim setting beside the
/// token is probed; every one of them is origin's error all the same.
#[tokio::test]
async fn test_claim_a_policy_cast_rejects_forwards_with_origins_error() -> Result<(), Error> {
    let mut ctx = uid_docs_setup().await?;
    let (user, origin) = sessions_with(&ctx, "uid_user", &[&token_set(ANN_TOKEN)]).await?;
    assert_sqlstate(
        origin.simple_query(UID_READ).await,
        &SqlState::INVALID_TEXT_REPRESENTATION,
        "ann is not a uuid",
    );
    let before = ctx.metrics().await?;
    let mut delta = MetricsSnapshot::default();
    for _ in 0..40 {
        assert_sqlstate(
            user.simple_query(UID_READ).await,
            &SqlState::INVALID_TEXT_REPRESENTATION,
            "origin's own answer through the proxy",
        );
        delta = metrics_delta(&before, &ctx.metrics_after_outcomes(&before, 1).await?);
        if delta.queries_rls_forward_unsupported >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        delta.queries_rls_forward_unsupported, 1,
        "forwarded for the rejected claim\n{delta:#?}"
    );
    assert_eq!(delta.queries_cache_hit, 0, "{delta:#?}");
    assert_eq!(
        delta.cache_writer_register_binds, 0,
        "nothing registered\n{delta:#?}"
    );
    Ok(())
}

/// Two roles reading as the same tenant produce the same inlined query and
/// share one entry: the second never registers.
#[tokio::test]
async fn test_sessions_of_one_tenant_share_an_entry() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, _) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    register(&mut ctx, &user, TENANT_READ).await?;

    let (peer, origin_peer) = sessions_as(&ctx, "tenant_peer", Some("1")).await?;
    let served = hit_wait(&mut ctx, &peer, TENANT_READ).await?;
    assert_eq!(served, rows(&origin_peer, TENANT_READ).await?);
    Ok(())
}

/// `SET LOCAL app.tenant_id` inside a block scopes the inlined tenant to the
/// block: the read in the block serves that tenant, the read after `COMMIT`
/// serves the session's.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_set_local_tenant_in_a_block_is_served_for_the_block() -> Result<(), Error> {
    // In-block serving needs the read-after-write gate the harness turns off.
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0")]).await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, _) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    register(&mut ctx, &user, TENANT_READ).await?;

    for sql in ["BEGIN", "SET LOCAL app.tenant_id = '2'"] {
        user.simple_query(sql).await.map_err(Error::other)?;
    }
    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("2", "b")], "the block's tenant");
    user.simple_query("COMMIT").await.map_err(Error::other)?;

    let m = ctx.metrics().await?;
    assert_eq!(
        rows(&user, TENANT_READ).await?,
        vec![row("1", "a"), row("3", "c")],
        "the session's tenant again"
    );
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A session that never set `app.tenant_id` cannot have the policy inlined:
/// the read forwards and origin's error (an unrecognized parameter) comes
/// back. Setting it afterwards serves.
#[tokio::test]
async fn test_unset_tenant_setting_forwards_to_origin_error() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", None).await?;

    let result = rls_forward_assert(&mut ctx, &user, TENANT_READ, |d| {
        d.queries_rls_forward_setting_unknown
    })
    .await?;
    assert_sqlstate(
        result,
        &SqlState::UNDEFINED_OBJECT,
        "tenant_user with no tenant",
    );

    for client in [&user, &origin] {
        client
            .simple_query("SET app.tenant_id = '2'")
            .await
            .map_err(Error::other)?;
    }
    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, rows(&origin, TENANT_READ).await?);
    Ok(())
}

/// `current_user` in a policy is inlined as the effective role's name: each
/// role is its own entry and sees only its rows.
#[tokio::test]
async fn test_current_user_policy_is_inlined_per_role() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    owned_docs_create(&mut ctx).await?;

    let (alice, origin_alice) = sessions_as(&ctx, "alice", None).await?;
    let served = register(&mut ctx, &alice, OWNED_READ).await?;
    assert_eq!(served, vec![row("1", "a1"), row("3", "a2")]);
    assert_eq!(served, rows(&origin_alice, OWNED_READ).await?);

    let (bob, origin_bob) = sessions_as(&ctx, "bob", None).await?;
    let served = register(&mut ctx, &bob, OWNED_READ).await?;
    assert_eq!(served, vec![row("2", "b1")]);
    assert_eq!(served, rows(&origin_bob, OWNED_READ).await?);
    Ok(())
}

/// Roles the policies do not apply to read the whole table from cache: the
/// owner (until `FORCE`), a `BYPASSRLS` role and the superuser. Under
/// `FORCE ROW LEVEL SECURITY` the owner is subject to the policy like
/// anyone else.
#[tokio::test]
async fn test_exempt_roles_read_every_row_from_cache() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    for sql in [
        "CREATE ROLE docs_owner LOGIN",
        "ALTER TABLE tenant_docs OWNER TO docs_owner",
        "CREATE ROLE bypasser LOGIN BYPASSRLS",
        "GRANT SELECT ON tenant_docs TO bypasser",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let all = vec![row("1", "a"), row("2", "b"), row("3", "c")];

    let (owner, _) = sessions_as(&ctx, "docs_owner", None).await?;
    assert_eq!(register(&mut ctx, &owner, TENANT_READ).await?, all);
    let (bypasser, _) = sessions_as(&ctx, "bypasser", None).await?;
    assert_eq!(hit_wait(&mut ctx, &bypasser, TENANT_READ).await?, all);
    let superuser = ctx.proxy_client_connect().await?;
    assert_eq!(hit_wait(&mut ctx, &superuser, TENANT_READ).await?, all);

    ctx.origin_query("ALTER TABLE tenant_docs FORCE ROW LEVEL SECURITY", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let result = rls_forward_assert(&mut ctx, &owner, TENANT_READ, |d| {
        d.queries_rls_forward_setting_unknown
    })
    .await?;
    assert_sqlstate(result, &SqlState::UNDEFINED_OBJECT, "owner under FORCE");
    owner
        .simple_query("SET app.tenant_id = '2'")
        .await
        .map_err(Error::other)?;
    assert_eq!(
        register(&mut ctx, &owner, TENANT_READ).await?,
        vec![row("2", "b")]
    );
    Ok(())
}

/// With `row_security = off` origin refuses a read a policy would filter;
/// the read forwards and that refusal comes back. Turned on again, it serves.
#[tokio::test]
async fn test_row_security_off_with_an_applicable_policy_forwards() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    register(&mut ctx, &user, TENANT_READ).await?;

    user.simple_query("SET row_security = off")
        .await
        .map_err(Error::other)?;
    let result = rls_forward_assert(&mut ctx, &user, TENANT_READ, |d| {
        d.queries_rls_forward_row_security_off
    })
    .await?;
    assert_sqlstate(
        result,
        &SqlState::INSUFFICIENT_PRIVILEGE,
        "tenant_user with row_security off",
    );

    user.simple_query("SET row_security = on")
        .await
        .map_err(Error::other)?;
    assert_eq!(
        hit_wait(&mut ctx, &user, TENANT_READ).await?,
        rows(&origin, TENANT_READ).await?
    );
    Ok(())
}

/// The inlined predicate is an ordinary single-table conjunct, so CDC keeps
/// the tenant's entry in place: rows entering, leaving and moving between
/// tenants are applied without invalidation.
#[tokio::test]
async fn test_cdc_changes_on_an_rls_table_apply_in_place() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    register(&mut ctx, &user, TENANT_READ).await?;

    let m = ctx.metrics().await?;
    for sql in [
        "INSERT INTO tenant_docs VALUES (4, 1, 'd'), (5, 2, 'e')",
        "UPDATE tenant_docs SET tenant = 2 WHERE id = 3",
        "UPDATE tenant_docs SET tenant = 1 WHERE id = 2",
        "DELETE FROM tenant_docs WHERE id = 1",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let served = rows(&user, TENANT_READ).await?;
    assert_eq!(served, vec![row("2", "b"), row("4", "d")]);
    assert_eq!(served, rows(&origin, TENANT_READ).await?);
    let delta = metrics_delta(&m, &assert_cache_hit(&mut ctx, m.clone()).await?);
    assert_eq!(delta.cache_invalidations, 0, "applied in place");
    Ok(())
}

/// `pattern_docs`, readable by `patterned`, with no policy yet: each test
/// adds the policy whose operator it exercises.
async fn pattern_docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE pattern_docs (id int primary key, body text, tag text)",
        "INSERT INTO pattern_docs VALUES (1, 'alpha', 'hidden'), (2, 'Beta', NULL), \
         (3, 'alpine', 'shown'), (4, 'gamma', 'shown')",
        "CREATE ROLE patterned LOGIN",
        "GRANT SELECT ON pattern_docs TO patterned",
        "ALTER TABLE pattern_docs ENABLE ROW LEVEL SECURITY",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    Ok(())
}

const PATTERN_READ: &str = "SELECT id, body FROM pattern_docs ORDER BY id";

/// A policy under `qual` is inlined and cached for `patterned`, serving
/// origin's rows, and each origin `change` reaches the entry in place.
async fn pattern_policy_check(
    qual: &str,
    expected: Vec<Vec<Option<String>>>,
    changes: &[(&str, Vec<Vec<Option<String>>>)],
) -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    pattern_docs_create(&mut ctx).await?;
    let policy = format!("CREATE POLICY pattern_policy ON pattern_docs FOR SELECT USING ({qual})");
    ctx.origin_query(policy.as_str(), &[]).await?;
    ctx.cdc_apply_settle().await?;
    let (user, origin) = sessions_as(&ctx, "patterned", None).await?;
    let served = register(&mut ctx, &user, PATTERN_READ).await?;
    assert_eq!(served, expected, "{qual}");
    assert_eq!(served, rows(&origin, PATTERN_READ).await?, "{qual}");
    let m = ctx.metrics().await?;
    for (change, expected) in changes {
        ctx.origin_query(*change, &[]).await?;
        ctx.cdc_apply_settle().await?;
        let served = rows(&user, PATTERN_READ).await?;
        assert_eq!(&served, expected, "{qual} after {change}");
        assert_eq!(
            served,
            rows(&origin, PATTERN_READ).await?,
            "{qual} after {change}"
        );
    }
    let delta = metrics_delta(&m, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_hit,
        u64::try_from(changes.len()).unwrap_or(u64::MAX),
        "each read after a change is a hit: {qual}\n{delta:#?}"
    );
    assert_eq!(
        delta.queries_cache_miss, 0,
        "no re-registration: {qual}\n{delta:#?}"
    );
    assert_eq!(delta.cache_invalidations, 0, "applied in place: {qual}");
    Ok(())
}

/// A `LIKE` policy: `pg_get_expr` spells it with the `~~` operator, which
/// the converter reads as the keyword form (PGC-606). `ILIKE` likewise.
#[tokio::test]
async fn test_like_policy_is_inlined_and_maintained() -> Result<(), Error> {
    pattern_policy_check(
        "body LIKE 'al%'",
        vec![row("1", "alpha"), row("3", "alpine")],
        &[
            (
                "UPDATE pattern_docs SET body = 'alto' WHERE id = 4",
                vec![row("1", "alpha"), row("3", "alpine"), row("4", "alto")],
            ),
            (
                "UPDATE pattern_docs SET body = 'omega' WHERE id = 1",
                vec![row("3", "alpine"), row("4", "alto")],
            ),
        ],
    )
    .await?;
    pattern_policy_check(
        "body NOT ILIKE 'b%'",
        vec![row("1", "alpha"), row("3", "alpine"), row("4", "gamma")],
        &[(
            "UPDATE pattern_docs SET body = 'bravo' WHERE id = 4",
            vec![row("1", "alpha"), row("3", "alpine")],
        )],
    )
    .await
}

/// A regex policy and a `SIMILAR TO` policy (deparsed as `~` over
/// `similar_to_escape`) are inlined and maintained like `LIKE`.
#[tokio::test]
async fn test_regex_and_similar_to_policies_are_inlined() -> Result<(), Error> {
    pattern_policy_check(
        "body ~* '^a'",
        vec![row("1", "alpha"), row("3", "alpine")],
        &[(
            "INSERT INTO pattern_docs VALUES (5, 'Avalon', NULL)",
            vec![row("1", "alpha"), row("3", "alpine"), row("5", "Avalon")],
        )],
    )
    .await?;
    pattern_policy_check(
        "body SIMILAR TO 'g%'",
        vec![row("4", "gamma")],
        &[("DELETE FROM pattern_docs WHERE id = 4", Vec::new())],
    )
    .await
}

/// `IS DISTINCT FROM` in a policy keeps NULL as a value: a NULL tag is
/// distinct from 'hidden' and visible.
#[tokio::test]
async fn test_is_distinct_from_policy_is_inlined() -> Result<(), Error> {
    pattern_policy_check(
        "tag IS DISTINCT FROM 'hidden'",
        vec![row("2", "Beta"), row("3", "alpine"), row("4", "gamma")],
        &[(
            "UPDATE pattern_docs SET tag = NULL WHERE id = 1",
            vec![
                row("1", "alpha"),
                row("2", "Beta"),
                row("3", "alpine"),
                row("4", "gamma"),
            ],
        )],
    )
    .await
}

/// A policy created, altered or dropped reaches the cache through the
/// schema-change signal: the table's entries are evicted and the next read
/// inlines the new policy set. With no permissive policy left the read
/// forwards and origin returns nothing.
#[tokio::test]
async fn test_policy_change_evicts_and_reinlines() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    register(&mut ctx, &user, TENANT_READ).await?;

    ctx.origin_query(
        "CREATE POLICY shared_docs ON tenant_docs FOR SELECT USING (body = 'b')",
        &[],
    )
    .await?;
    ctx.cdc_apply_settle().await?;
    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("2", "b"), row("3", "c")]);
    assert_eq!(served, rows(&origin, TENANT_READ).await?);

    ctx.origin_query(
        "ALTER POLICY shared_docs ON tenant_docs USING (body = 'zzz')",
        &[],
    )
    .await?;
    ctx.cdc_apply_settle().await?;
    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);

    for sql in [
        "DROP POLICY shared_docs ON tenant_docs",
        "DROP POLICY tenant_policy ON tenant_docs",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let result = rls_forward_assert(&mut ctx, &user, TENANT_READ, |d| {
        d.queries_rls_forward_default_deny
    })
    .await?;
    assert_eq!(
        result_rows(&result.map_err(Error::other)?),
        Vec::<Vec<Option<String>>>::new(),
        "default deny"
    );
    Ok(())
}

/// A policy `TO role_list` applies to the roles that inherit a listed role:
/// a member reads from cache, a non-member has no permissive policy and
/// forwards to origin's empty answer, and a membership granted later is
/// seen by the role poll.
#[tokio::test]
async fn test_role_list_policy_applies_through_membership() -> Result<(), Error> {
    let mut ctx = TestContext::setup_with_args(&["--role_poll_interval", "1"]).await?;
    tenant_docs_create(&mut ctx).await?;
    for sql in [
        "DROP POLICY tenant_policy ON tenant_docs",
        "CREATE ROLE tenant_readers",
        "CREATE ROLE member LOGIN IN ROLE tenant_readers",
        "CREATE ROLE outsider LOGIN",
        "GRANT SELECT ON tenant_docs TO member, outsider",
        "CREATE POLICY readers_policy ON tenant_docs FOR SELECT TO tenant_readers \
         USING (tenant = current_setting('app.tenant_id')::int)",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;

    let (member, origin_member) = sessions_as(&ctx, "member", Some("1")).await?;
    let served = register(&mut ctx, &member, TENANT_READ).await?;
    assert_eq!(served, rows(&origin_member, TENANT_READ).await?);

    let (outsider, origin_outsider) = sessions_as(&ctx, "outsider", Some("1")).await?;
    let result = rls_forward_assert(&mut ctx, &outsider, TENANT_READ, |d| {
        d.queries_rls_forward_default_deny
    })
    .await?;
    assert_eq!(
        result_rows(&result.map_err(Error::other)?),
        Vec::<Vec<Option<String>>>::new()
    );

    let before = ctx.metrics().await?;
    ctx.origin_query("GRANT tenant_readers TO outsider", &[])
        .await?;
    role_change_wait(&mut ctx, &before).await?;
    let served = hit_wait(&mut ctx, &outsider, TENANT_READ).await?;
    assert_eq!(served, rows(&origin_outsider, TENANT_READ).await?);
    Ok(())
}

/// Restrictive policies are ANDed onto the OR of the permissive ones, as
/// origin combines them.
#[tokio::test]
async fn test_restrictive_policy_is_anded_with_permissive() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    for sql in [
        "INSERT INTO tenant_docs VALUES (4, 1, 'hidden')",
        "CREATE POLICY not_hidden ON tenant_docs AS RESTRICTIVE FOR SELECT \
         USING (body <> 'hidden')",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;

    let served = register(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, TENANT_READ).await?);
    Ok(())
}

/// A policy outside the cacheable subset (a volatile function) cannot be
/// inlined: reads over the table forward, counted in `rls_skipped`, and are
/// still correct.
#[tokio::test]
async fn test_uninlinable_policy_forwards() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for sql in [
        "CREATE TABLE lucky_docs (id int primary key, body text)",
        "INSERT INTO lucky_docs VALUES (1, 'a')",
        "CREATE ROLE gambler LOGIN",
        "GRANT SELECT ON lucky_docs TO gambler",
        "ALTER TABLE lucky_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY lucky ON lucky_docs FOR SELECT USING (random() < 2)",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let (gambler, _) = sessions_as(&ctx, "gambler", None).await?;
    let sql = "SELECT id, body FROM lucky_docs ORDER BY id";

    for _ in 0..2 {
        let result = rls_forward_assert(&mut ctx, &gambler, sql, |d| d.queries_rls_skipped).await?;
        assert_eq!(
            result_rows(&result.map_err(Error::other)?),
            vec![row("1", "a")]
        );
    }
    Ok(())
}

/// Enabling row-level security on a cached table evicts its queries (the
/// `ALTER TABLE` signal). The superuser is exempt from the policies, so its
/// read registers again over the whole table.
#[tokio::test]
async fn test_enabling_rls_evicts_cached_queries() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for sql in [
        "CREATE TABLE plain_docs (id int primary key, body text)",
        "INSERT INTO plain_docs VALUES (1, 'a')",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await?;
    let sql = "SELECT id, body FROM plain_docs WHERE id = 1";
    query_warm(&mut ctx, sql).await?;

    ctx.origin_query("ALTER TABLE plain_docs ENABLE ROW LEVEL SECURITY", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let superuser = ctx.proxy_client_connect().await?;
    let served = register(&mut ctx, &superuser, sql).await?;
    assert_eq!(served, vec![row("1", "a")]);
    Ok(())
}

/// A policy's subquery may alias its own table like the client aliases the
/// secured one. The inlined predicate must still correlate to the client's
/// table, not to the policy's: with the alias captured, the membership
/// check degenerates to `m.tenant = m.tenant` and every member sees every
/// tenant's rows from cache.
#[tokio::test]
async fn test_policy_alias_shadowing_the_query_alias_keeps_the_correlation() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for sql in [
        "CREATE TABLE tenant_docs (id int primary key, tenant int, body text)",
        "INSERT INTO tenant_docs VALUES (1, 1, 'a'), (2, 2, 'b'), (3, 1, 'c')",
        "CREATE TABLE memberships (user_name text primary key, tenant int)",
        "INSERT INTO memberships VALUES ('alice', 1), ('bob', 2)",
        "CREATE ROLE alice LOGIN",
        "CREATE ROLE bob LOGIN",
        "GRANT SELECT ON tenant_docs, memberships TO alice, bob",
        "ALTER TABLE tenant_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY by_membership ON tenant_docs FOR SELECT USING (EXISTS ( \
             SELECT 1 FROM memberships m \
             WHERE m.user_name = current_user AND m.tenant = tenant_docs.tenant))",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let sql = "SELECT id, body FROM tenant_docs m ORDER BY id";

    let (alice, origin_alice) = sessions_as(&ctx, "alice", None).await?;
    let served = register(&mut ctx, &alice, sql).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_alice, sql).await?);

    let (bob, origin_bob) = sessions_as(&ctx, "bob", None).await?;
    let served = register(&mut ctx, &bob, sql).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_bob, sql).await?);
    Ok(())
}

/// A policy's function call is deparsed bare when the writer connection's
/// search_path reaches it; the inlined call means that function for a
/// session whose own path does not reach it, so the read is cached rather
/// than refused as unknown.
#[tokio::test]
async fn test_policy_function_resolves_on_the_writers_path() -> Result<(), Error> {
    let mut ctx = judged_docs_setup().await?;
    let (proxied, origin) = sessions_as(&ctx, "judged_user", None).await?;
    for client in [&proxied, &origin] {
        client
            .simple_query("SET search_path = app")
            .await
            .map_err(Error::other)?;
    }
    let served = register(&mut ctx, &proxied, JUDGED_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, JUDGED_READ).await?);
    Ok(())
}

/// A same-named function on the session's search_path is not the one the
/// policy calls: origin enforces the policy's own function, and so does the
/// cached entry, although the session's one is absent from the cache
/// database.
#[tokio::test]
async fn test_policy_function_is_not_taken_from_the_sessions_path() -> Result<(), Error> {
    let mut ctx = judged_docs_setup().await?;
    ctx.origin_query(
        "CREATE FUNCTION app.tenant_of(id int) RETURNS int LANGUAGE sql IMMUTABLE AS $$ SELECT 1 $$",
        &[],
    )
    .await?;
    ctx.cache_settle().await?;
    let (proxied, origin) = sessions_as(&ctx, "judged_user", None).await?;
    for client in [&proxied, &origin] {
        client
            .simple_query("SET search_path = app, public")
            .await
            .map_err(Error::other)?;
    }
    let served = register(&mut ctx, &proxied, JUDGED_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, JUDGED_READ).await?);
    Ok(())
}

/// A bare call that functions in two schemas of the writer's search_path
/// answer cannot be told apart by arity, so the policy is not inlined and
/// the read forwards.
#[tokio::test]
async fn test_policy_calling_an_overloaded_builtin_is_not_inlined() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for sql in [
        "CREATE FUNCTION public.lower(t text) RETURNS text LANGUAGE sql IMMUTABLE AS $$ SELECT 'x' $$",
        "CREATE TABLE vague_docs (id int primary key, body text)",
        "INSERT INTO vague_docs VALUES (1, 'a'), (2, 'B')",
        "CREATE ROLE vague_user LOGIN",
        "GRANT SELECT ON vague_docs TO vague_user",
        "ALTER TABLE vague_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY lowered ON vague_docs FOR SELECT USING (lower(body) = 'a')",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let (proxied, _) = sessions_as(&ctx, "vague_user", None).await?;
    let sql = "SELECT id, body FROM vague_docs ORDER BY id";
    let result = rls_forward_assert(&mut ctx, &proxied, sql, |d| d.queries_rls_skipped).await?;
    assert_eq!(
        result_rows(&result.map_err(Error::other)?),
        vec![row("1", "a")]
    );
    Ok(())
}

/// A policy extracting a claim from the JWT claims setting is keyed on the
/// claim, as a client query's own extraction is (ADR-066): a later token of
/// the same user, differing in `exp`, is a hit on the same entry with no
/// registration of its own, and another user's token has its own entry.
#[tokio::test]
async fn test_jwt_claim_policy_is_keyed_on_the_claim() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    claimed_docs_create(&mut ctx).await?;
    let (ann, origin_ann) = sessions_with(&ctx, "claimant", &[&token_set(ANN_TOKEN)]).await?;
    let served = register(&mut ctx, &ann, CLAIMED_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_ann, CLAIMED_READ).await?);

    let (ann_later, _) = sessions_with(&ctx, "claimant", &[&token_set(ANN_TOKEN_LATER)]).await?;
    assert_eq!(
        hit_wait(&mut ctx, &ann_later, CLAIMED_READ).await?,
        served,
        "a later token of the same user shares the entry"
    );

    let (bob, origin_bob) = sessions_with(&ctx, "claimant", &[&token_set(BOB_TOKEN)]).await?;
    let served = register(&mut ctx, &bob, CLAIMED_READ).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_bob, CLAIMED_READ).await?);
    Ok(())
}

/// A policy calling `auth.uid()` is inlined with the function's body and
/// keyed on the claim the body extracts (PGC-579): a later token of the same
/// user hits the entry, another user's token has its own, and each serves
/// origin's rows. The body reads a claim setting beside the token that no
/// session sets; the connection tracks it from its start, so the startup
/// probe samples it before the first read (PGC-599).
#[tokio::test]
async fn test_auth_uid_policy_is_inlined_and_keyed_on_the_claim() -> Result<(), Error> {
    let mut ctx = uid_docs_setup().await?;
    let (ann, origin_ann) = sessions_with(&ctx, "uid_user", &[&token_set(ANN_UUID_TOKEN)]).await?;
    let served = read_until(&mut ctx, &ann, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_ann, UID_READ).await?);

    let (ann_later, _) =
        sessions_with(&ctx, "uid_user", &[&token_set(ANN_UUID_TOKEN_LATER)]).await?;
    let before = ctx.metrics().await?;
    let later = read_until(&mut ctx, &ann_later, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(
        later, served,
        "a later token of the same user shares the entry"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_miss,
        delta.queries_privilege_forward_pending + delta.queries_rls_forward_setting_unknown,
        "served from the entry that already existed\n{delta:#?}"
    );

    let (bob, origin_bob) = sessions_with(&ctx, "uid_user", &[&token_set(BOB_UUID_TOKEN)]).await?;
    let before = ctx.metrics().await?;
    let served = read_until(&mut ctx, &bob, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_bob, UID_READ).await?);
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.queries_cache_miss
            > delta.queries_privilege_forward_pending + delta.queries_rls_forward_setting_unknown,
        "another user registers an entry of its own\n{delta:#?}"
    );
    Ok(())
}

/// The PostgREST request shape over a secured table: a block that sets the
/// claims for the block and reads. A second request with a later token of
/// the same user is a hit; outside a request the claims setting is an
/// empty string, which is not json, and the read is origin's error through
/// the proxy as directly.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_postgrest_style_block_over_a_claim_policy_is_served_per_claim() -> Result<(), Error> {
    // In-block serving needs the read-after-write gate the harness turns off.
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0")]).await?;
    claimed_docs_create(&mut ctx).await?;
    let (proxied, origin) = sessions_with(&ctx, "claimant", &[]).await?;
    let request_begin = |token: &str| {
        [
            "BEGIN".to_owned(),
            format!("SELECT set_config('request.jwt.claims', '{token}', true)"),
        ]
    };
    for client in [&proxied, &origin] {
        for sql in request_begin(ANN_TOKEN) {
            client.simple_query(&sql).await.map_err(Error::other)?;
        }
    }
    let served = register(&mut ctx, &proxied, CLAIMED_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, CLAIMED_READ).await?);
    for client in [&proxied, &origin] {
        client.simple_query("COMMIT").await.map_err(Error::other)?;
    }

    for client in [&proxied, &origin] {
        for sql in request_begin(ANN_TOKEN_LATER) {
            client.simple_query(&sql).await.map_err(Error::other)?;
        }
    }
    assert_eq!(hit_wait(&mut ctx, &proxied, CLAIMED_READ).await?, served);
    for client in [&proxied, &origin] {
        client.simple_query("COMMIT").await.map_err(Error::other)?;
    }

    for client in [&proxied, &origin] {
        let result = client.simple_query(CLAIMED_READ).await;
        assert_sqlstate(
            result,
            &SqlState::INVALID_TEXT_REPRESENTATION,
            "an empty claims setting is not json",
        );
    }
    Ok(())
}

/// The Supabase request as PostgREST sends it: as `authenticator`, a block
/// that sets the role and the claims for the block in one statement, reads
/// under the `owner = auth.uid()` policy for `authenticated`, and commits.
/// The first request registers; a later token of the same user is a hit;
/// another user has an entry of its own; a user who owns nothing is served
/// nothing; an `anon` request on the same connection, which no policy
/// covers, is default-denied rather than served the `authenticated` entry;
/// and `authenticator` itself, outside a request, has no SELECT on the
/// table, through the proxy as directly.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_supabase_request_as_postgrest_sends_it() -> Result<(), Error> {
    // In-block serving needs the read-after-write gate the harness turns off.
    let ctx = TestContext::setup_fault_before_start(
        &[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0")],
        auth_schema_create,
    )
    .await?;
    let mut ctx = uid_docs_create(ctx).await?;
    for sql in [
        "CREATE ROLE authenticator LOGIN NOINHERIT",
        "CREATE ROLE authenticated NOLOGIN",
        "CREATE ROLE anon NOLOGIN",
        "GRANT authenticated, anon TO authenticator",
        "GRANT USAGE ON SCHEMA auth TO authenticated, anon",
        "GRANT SELECT ON uid_docs TO authenticated, anon",
        "DROP POLICY uid_policy ON uid_docs",
        "CREATE POLICY uid_policy ON uid_docs FOR SELECT TO authenticated \
         USING (owner = auth.uid())",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    let (proxied, origin) = sessions_with(&ctx, "authenticator", &[]).await?;
    let request_begin_as = |role: &str, token: &str| {
        [
            "BEGIN".to_owned(),
            format!(
                "SELECT set_config('role', '{role}', true), \
                 set_config('request.jwt.claims', '{token}', true)"
            ),
        ]
    };
    let request_begin = |token: &str| request_begin_as("authenticated", token);
    async fn both(clients: [&Client; 2], statements: &[String]) -> Result<(), Error> {
        for client in clients {
            for sql in statements {
                client.simple_query(sql).await.map_err(Error::other)?;
            }
        }
        Ok(())
    }
    let commit = ["COMMIT".to_owned()];

    both([&proxied, &origin], &request_begin(ANN_UUID_TOKEN)).await?;
    let served = read_until(&mut ctx, &proxied, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, UID_READ).await?);
    both([&proxied, &origin], &commit).await?;

    both([&proxied, &origin], &request_begin(ANN_UUID_TOKEN_LATER)).await?;
    let before = ctx.metrics().await?;
    let later = read_until(&mut ctx, &proxied, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(later, served, "a later token of the same user is a hit");
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_miss,
        delta.queries_privilege_forward_pending + delta.queries_rls_forward_setting_unknown,
        "served from the entry that already existed\n{delta:#?}"
    );
    both([&proxied, &origin], &commit).await?;

    both([&proxied, &origin], &request_begin(BOB_UUID_TOKEN)).await?;
    let before = ctx.metrics().await?;
    let served = read_until(&mut ctx, &proxied, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin, UID_READ).await?);
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.queries_cache_miss
            > delta.queries_privilege_forward_pending + delta.queries_rls_forward_setting_unknown,
        "another user registers an entry of its own\n{delta:#?}"
    );
    both([&proxied, &origin], &commit).await?;

    both([&proxied, &origin], &request_begin(STRANGER_UUID_TOKEN)).await?;
    let served = read_until(&mut ctx, &proxied, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(
        served,
        Rows::new(),
        "a user owning nothing is served nothing"
    );
    assert_eq!(served, rows(&origin, UID_READ).await?);
    both([&proxied, &origin], &commit).await?;

    both([&proxied, &origin], &request_begin_as("anon", ANON_TOKEN)).await?;
    let result = rls_forward_assert(&mut ctx, &proxied, UID_READ, |delta| {
        delta.queries_rls_forward_default_deny
    })
    .await?;
    assert_eq!(
        result_rows(&result.map_err(Error::other)?),
        Rows::new(),
        "no policy covers anon: default deny, not the authenticated entry"
    );
    assert_eq!(rows(&origin, UID_READ).await?, Rows::new());
    both([&proxied, &origin], &commit).await?;

    for client in [&proxied, &origin] {
        assert_sqlstate(
            client.simple_query(UID_READ).await,
            &SqlState::INSUFFICIENT_PRIVILEGE,
            "authenticator has no SELECT of its own",
        );
    }
    Ok(())
}

/// A policy term in a shape the fold refuses (a typed `nullif`) is still
/// inlined, the term substituted on its own: what slice 1 cached stays
/// cached, keyed on the setting's value.
#[tokio::test]
async fn test_unfoldable_policy_fragment_substitutes_its_term() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    nulled_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "nulled_user", Some("1")).await?;
    let served = register(&mut ctx, &user, NULLED_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, NULLED_READ).await?);

    for client in [&user, &origin] {
        client
            .simple_query("SET app.tenant_id = '0'")
            .await
            .map_err(Error::other)?;
    }
    let served = register(&mut ctx, &user, NULLED_READ).await?;
    assert_eq!(
        served,
        Vec::<Vec<Option<String>>>::new(),
        "nullif gives NULL"
    );
    assert_eq!(served, rows(&origin, NULLED_READ).await?);
    Ok(())
}

const DEFAULTED_READ: &str = "SELECT id, body FROM defaulted_docs ORDER BY id";

/// `defaulted_docs`, readable by `defaulted_user`, under a policy that falls
/// back to the `public` tenant while `app.tenant_name` is absent: the shape
/// that tells an absent setting from an empty one.
async fn defaulted_docs_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE defaulted_docs (id int primary key, tenant text, body text)",
        "INSERT INTO defaulted_docs VALUES (1, 'public', 'p'), (2, 'acme', 'a')",
        "CREATE ROLE defaulted_user LOGIN",
        "GRANT SELECT ON defaulted_docs TO defaulted_user",
        "ALTER TABLE defaulted_docs ENABLE ROW LEVEL SECURITY",
        "CREATE POLICY defaulted_policy ON defaulted_docs FOR SELECT \
         USING (tenant = coalesce(current_setting('app.tenant_name', true), 'public'))",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await
}

/// A function whose body switches the tenant with `set_config`: the client
/// statement names no setting.
const APP_LOGIN: &str = "CREATE FUNCTION app_login(t int) RETURNS text LANGUAGE sql VOLATILE \
     AS $$ SELECT set_config('app.tenant_id', $1::text, false) $$";

/// Read `sql` through `client` until a read is a cache hit, however many
/// forwards (a probe, a verdict) and the registration come first, and return
/// what the hit served.
async fn hit_eventually(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<Rows, Error> {
    read_until(ctx, client, sql, |delta| delta.queries_cache_hit == 1).await
}

/// Run `sql` on both sessions.
async fn both_run(proxied: &Client, origin: &Client, sql: &str) -> Result<(), Error> {
    for client in [proxied, origin] {
        client.simple_query(sql).await.map_err(Error::other)?;
    }
    Ok(())
}

/// The tenant switched by a server-side function calling `set_config`: the
/// next read is keyed on the new tenant, as origin reads it.
#[tokio::test]
async fn test_tenant_set_by_a_function_is_tracked() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    ctx.origin_query(APP_LOGIN, &[]).await?;
    ctx.cdc_apply_settle().await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    let served = hit_eventually(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);

    both_run(&user, &origin, "SELECT app_login(2)").await?;
    let expected = rows(&origin, TENANT_READ).await?;
    assert_eq!(expected, vec![row("2", "b")], "origin reads as tenant 2");
    let served = rows(&user, TENANT_READ).await?;
    assert_eq!(served, expected, "pgcache reads as tenant 2 like origin");
    Ok(())
}

/// As above with the read-after-write gate on, as in production: the first
/// read after the call may forward for the write; the cache hit that
/// follows is the new tenant's.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_tenant_set_by_a_function_is_tracked_under_the_write_gate() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0")]).await?;
    tenant_docs_create(&mut ctx).await?;
    ctx.origin_query(APP_LOGIN, &[]).await?;
    ctx.cdc_apply_settle().await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    let served = hit_eventually(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);

    both_run(&user, &origin, "SELECT app_login(2)").await?;
    let expected = rows(&origin, TENANT_READ).await?;
    let served = hit_eventually(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(
        served, expected,
        "the first cache hit after the call is tenant 2's"
    );
    Ok(())
}

/// The role switched by a plpgsql function (`EXECUTE 'SET ROLE …'`): `role`
/// is not a reported setting, and the statement names none, yet the next
/// read is keyed on the new role.
#[tokio::test]
async fn test_role_set_by_a_function_is_tracked() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    owned_docs_create(&mut ctx).await?;
    let before = ctx.metrics().await?;
    for sql in [
        "GRANT bob TO alice",
        "CREATE FUNCTION become(r text) RETURNS void LANGUAGE plpgsql \
         AS $$ BEGIN EXECUTE 'SET ROLE ' || quote_ident(r); END $$",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    role_change_wait(&mut ctx, &before).await?;
    let (user, origin) = sessions_as(&ctx, "alice", None).await?;
    let served = hit_eventually(&mut ctx, &user, OWNED_READ).await?;
    assert_eq!(served, vec![row("1", "a1"), row("3", "a2")]);

    both_run(&user, &origin, "SELECT become('bob')").await?;
    let expected = rows(&origin, OWNED_READ).await?;
    assert_eq!(expected, vec![row("2", "b1")], "origin reads as bob");
    let served = rows(&user, OWNED_READ).await?;
    assert_eq!(served, expected, "pgcache reads as bob like origin");
    Ok(())
}

/// `SET LOCAL` of a setting the session never had defines it at origin: after
/// the block `current_setting(name, true)` is the empty string, not NULL, and
/// a policy defaulting the absent setting no longer applies its default.
#[tokio::test]
async fn test_set_local_of_an_absent_setting_leaves_origins_placeholder() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    defaulted_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "defaulted_user", None).await?;
    let served = hit_eventually(&mut ctx, &user, DEFAULTED_READ).await?;
    assert_eq!(served, rows(&origin, DEFAULTED_READ).await?);
    assert_eq!(
        served,
        vec![row("1", "p")],
        "the default tenant while absent"
    );

    for sql in ["BEGIN", "SET LOCAL app.tenant_name = 'acme'", "COMMIT"] {
        both_run(&user, &origin, sql).await?;
    }
    let expected = rows(&origin, DEFAULTED_READ).await?;
    assert_eq!(
        expected,
        Rows::new(),
        "origin compares against the empty placeholder"
    );
    let served = rows(&user, DEFAULTED_READ).await?;
    assert_eq!(served, expected, "pgcache matches origin after the block");
    Ok(())
}

/// `SET LOCAL` outside a block (a warning at origin, no effect on the value)
/// defines the placeholder all the same.
#[tokio::test]
async fn test_set_local_outside_a_block_leaves_origins_placeholder() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    defaulted_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "defaulted_user", None).await?;
    let served = hit_eventually(&mut ctx, &user, DEFAULTED_READ).await?;
    assert_eq!(served, vec![row("1", "p")]);

    both_run(&user, &origin, "SET LOCAL app.tenant_name = 'acme'").await?;
    let expected = rows(&origin, DEFAULTED_READ).await?;
    assert_eq!(
        expected,
        Rows::new(),
        "origin compares against the empty placeholder"
    );
    let served = rows(&user, DEFAULTED_READ).await?;
    assert_eq!(
        served, expected,
        "pgcache matches origin after the statement"
    );
    Ok(())
}

/// Ownership is checked through role membership at origin: a member of the
/// owning role is exempt from the table's policies and reads every row.
#[tokio::test]
async fn test_owner_through_membership_is_exempt() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    for sql in [
        "CREATE ROLE docs_owners",
        "CREATE ROLE owner_member LOGIN IN ROLE docs_owners",
        "ALTER TABLE tenant_docs OWNER TO docs_owners",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let (user, origin) = sessions_as(&ctx, "owner_member", Some("1")).await?;
    let expected = rows(&origin, TENANT_READ).await?;
    assert_eq!(expected, vec![row("1", "a"), row("2", "b"), row("3", "c")]);
    let served = hit_eventually(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, expected, "pgcache exempts the member like origin");
    Ok(())
}

/// A policy created while the schema-change signal is gone is unreported;
/// the writer's poll finds the signal missing and tears the cache down, and
/// the rebuilt cache (its triggers reinstalled) reads under the new policy.
#[tokio::test]
async fn test_policy_change_while_the_signal_is_lost_is_honoured_after_the_rebuild()
-> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    let served = hit_eventually(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);

    for sql in [
        "DROP EVENT TRIGGER pgcache_ddl_command_end",
        "DROP EVENT TRIGGER pgcache_sql_drop",
        "CREATE POLICY deny_all ON tenant_docs AS RESTRICTIVE FOR SELECT USING (false)",
        "UPDATE tenant_docs SET body = body WHERE id = 1",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    let expected = rows(&origin, TENANT_READ).await?;
    assert_eq!(expected, Rows::new(), "origin denies everything");

    let mut restarted = false;
    for _ in 0..120 {
        if ctx.metrics().await?.cache_restarts_total > 0 {
            restarted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    assert!(restarted, "the poll tears the cache down");
    let served = rows(&user, TENANT_READ).await?;
    assert_eq!(served, expected, "pgcache denies everything like origin");
    let served = hit_eventually(&mut ctx, &user, TENANT_READ).await?;
    assert_eq!(served, expected, "and so does the rebuilt cache's entry");
    Ok(())
}

/// pgcache connected to origin as `caretaker`, a superuser of the test's
/// making, so the test can take its attributes away. The bootstrap superuser
/// cannot lose `SUPERUSER`.
async fn pgcache_as_caretaker(extra_args: &[&str]) -> Result<TestContext, Error> {
    let mut args = vec!["--origin_user", "caretaker"];
    args.extend_from_slice(extra_args);
    TestContext::setup_before_start_with_args(
        |origin| async move {
            origin
                .simple_query("CREATE ROLE caretaker SUPERUSER LOGIN")
                .await
                .map_err(Error::other)?;
            Ok(origin)
        },
        &args,
    )
    .await
}

/// `ALTER ROLE` on pgcache's own role takes effect without a restart: the
/// role poll sees the attribute change and reads the bypass again. Without
/// it every read over a secured relation forwards under `rls_skipped`; with
/// it back, the reads serve again.
#[tokio::test]
async fn test_bypass_revoked_and_granted_at_runtime_is_seen_by_the_role_poll() -> Result<(), Error>
{
    let mut ctx = pgcache_as_caretaker(&[]).await?;
    tenant_docs_create(&mut ctx).await?;
    let (user, origin) = sessions_as(&ctx, "tenant_user", Some("1")).await?;
    let expected = rows(&origin, TENANT_READ).await?;
    assert_eq!(register(&mut ctx, &user, TENANT_READ).await?, expected);

    let m = ctx.metrics().await?;
    ctx.origin_query("ALTER ROLE caretaker NOSUPERUSER NOBYPASSRLS", &[])
        .await?;
    role_change_wait(&mut ctx, &m).await?;
    let mut skipped = false;
    for _ in 0..40 {
        let before = ctx.metrics().await?;
        assert_eq!(rows(&user, TENANT_READ).await?, expected);
        let delta = metrics_delta(&before, &ctx.metrics_after_outcomes(&before, 1).await?);
        assert_eq!(
            delta.queries_cache_hit, 0,
            "nothing served without the bypass"
        );
        if delta.queries_rls_skipped == 1 {
            skipped = true;
            break;
        }
    }
    assert!(skipped, "the read forwards under rls_skipped");

    let m = ctx.metrics().await?;
    ctx.origin_query("ALTER ROLE caretaker BYPASSRLS", &[])
        .await?;
    role_change_wait(&mut ctx, &m).await?;
    assert_eq!(
        hit_eventually(&mut ctx, &user, TENANT_READ).await?,
        expected
    );
    Ok(())
}

/// A population by a role that cannot bypass row-level security fails
/// instead of loading the rows origin leaves that role: the population
/// sessions run with `row_security` off. With the role poll disabled the
/// writer keeps believing it bypasses after the revocation and inlines the
/// policy; the owner under `FORCE` would otherwise load an empty table for
/// `alice` and serve it.
#[tokio::test]
async fn test_population_without_the_bypass_fails_closed() -> Result<(), Error> {
    let mut ctx = pgcache_as_caretaker(&["--role_poll_interval", "0"]).await?;
    owned_docs_create(&mut ctx).await?;
    // The caretaker must own the table to publish it, and `FORCE` puts the
    // owner under the policies like any other role.
    for sql in [
        "ALTER TABLE owned_docs OWNER TO caretaker",
        "ALTER TABLE owned_docs FORCE ROW LEVEL SECURITY",
        "ALTER ROLE caretaker NOSUPERUSER NOBYPASSRLS",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    let (alice, origin) = sessions_as(&ctx, "alice", None).await?;
    let expected = rows(&origin, OWNED_READ).await?;
    assert_eq!(expected, vec![row("1", "a1"), row("3", "a2")]);

    let start = ctx.metrics().await?;
    for _ in 0..20 {
        let before = ctx.metrics().await?;
        assert_eq!(rows(&alice, OWNED_READ).await?, expected);
        let delta = metrics_delta(&before, &ctx.metrics_after_outcomes(&before, 1).await?);
        assert_eq!(
            delta.queries_cache_hit, 0,
            "a filtered population is never served"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let delta = metrics_delta(&start, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_rls_skipped, 0,
        "the writer inlined, believing it bypasses"
    );
    assert!(
        delta.queries_cache_miss >= 2,
        "each failed population registers again"
    );
    Ok(())
}

/// A connection tracks the settings the published policies read from its
/// start, so its startup probe samples them (PGC-599). Once a user's entry
/// exists, a new connection's first request as PostgREST sends it is a hit:
/// no writer refusal while the never-set `request.jwt.claim.sub` is probed,
/// and no forward. A user without an entry registers on the first read
/// instead of forwarding first.
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_a_new_connections_first_request_over_a_claim_policy_is_served() -> Result<(), Error> {
    let ctx = TestContext::setup_fault_before_start(
        &[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0")],
        auth_schema_create,
    )
    .await?;
    let mut ctx = uid_docs_create(ctx).await?;
    for sql in [
        "CREATE ROLE authenticator LOGIN NOINHERIT",
        "CREATE ROLE authenticated NOLOGIN",
        "GRANT authenticated TO authenticator",
        "GRANT USAGE ON SCHEMA auth TO authenticated",
        "GRANT SELECT ON uid_docs TO authenticated",
        "DROP POLICY uid_policy ON uid_docs",
        "CREATE POLICY uid_policy ON uid_docs FOR SELECT TO authenticated \
         USING (owner = auth.uid())",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    // Two statements, as PostgREST sends them: a batch would be a connection
    // write the read-after-write gate forwards behind.
    let request_begin = |token: &str| {
        [
            "BEGIN".to_owned(),
            format!(
                "SELECT set_config('role', 'authenticated', true), \
                 set_config('request.jwt.claims', '{token}', true)"
            ),
        ]
    };
    async fn request_open(client: &Client, statements: [String; 2]) -> Result<(), Error> {
        for sql in statements {
            client.simple_query(&sql).await.map_err(Error::other)?;
        }
        Ok(())
    }

    let (warm, origin) = sessions_with(&ctx, "authenticator", &[]).await?;
    for client in [&warm, &origin] {
        request_open(client, request_begin(ANN_UUID_TOKEN)).await?;
    }
    let served = read_until(&mut ctx, &warm, UID_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, UID_READ).await?);

    let fresh = ctx.proxy_client_connect_as("authenticator").await?;
    request_open(&fresh, request_begin(ANN_UUID_TOKEN_LATER)).await?;
    let before = ctx.metrics().await?;
    assert_eq!(rows(&fresh, UID_READ).await?, served);
    let delta = metrics_delta(&before, &ctx.metrics_after_outcomes(&before, 1).await?);
    assert_eq!(
        delta.queries_cache_hit, 1,
        "a new connection's first request is a hit\n{delta:#?}"
    );
    assert_eq!(
        delta.queries_rls_forward_setting_unknown, 0,
        "nothing was unknown\n{delta:#?}"
    );
    assert_eq!(
        delta.conn_settings_probes, 0,
        "sampled at startup, not at the read\n{delta:#?}"
    );

    let fresh_bob = ctx.proxy_client_connect_as("authenticator").await?;
    request_open(&fresh_bob, request_begin(BOB_UUID_TOKEN)).await?;
    let before = ctx.metrics().await?;
    assert_eq!(rows(&fresh_bob, UID_READ).await?, vec![row("2", "b")]);
    let delta = metrics_delta(&before, &ctx.metrics_after_outcomes(&before, 1).await?);
    assert_eq!(
        delta.queries_cache_miss, 1,
        "a user without an entry registers at once\n{delta:#?}"
    );
    assert_eq!(
        delta.queries_rls_forward_setting_unknown, 0,
        "nothing was unknown\n{delta:#?}"
    );
    assert_eq!(
        delta.conn_settings_probes, 0,
        "sampled at startup, not at the read\n{delta:#?}"
    );
    Ok(())
}
