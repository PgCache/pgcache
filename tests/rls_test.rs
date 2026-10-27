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
    MetricsSnapshot, TestContext, assert_cache_hit, assert_cache_miss, metrics_delta, query_warm,
    result_rows,
};

mod util;

type Rows = Vec<Vec<Option<String>>>;

const TENANT_READ: &str = "SELECT id, body FROM tenant_docs ORDER BY id";
const OWNED_READ: &str = "SELECT id, body FROM owned_docs ORDER BY id";

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
    ctx.cache_settle().await
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
    ctx.cache_settle().await
}

/// Through the proxy as `user`, and directly on origin as `user`, both with
/// `app.tenant_id` set to `tenant` when given.
async fn sessions_as(
    ctx: &TestContext,
    user: &str,
    tenant: Option<&str>,
) -> Result<(Client, Client), Error> {
    let proxied = ctx.proxy_client_connect_as(user).await?;
    let origin = ctx.origin_connect_as(user).await?;
    if let Some(tenant) = tenant {
        let set = format!("SET app.tenant_id = '{tenant}'");
        proxied.simple_query(&set).await.map_err(Error::other)?;
        origin.simple_query(&set).await.map_err(Error::other)?;
    }
    Ok((proxied, origin))
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
    ctx.cache_settle().await?;
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
    ctx.cache_settle().await?;
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

/// Without the schema-change signal, enabling row-level security reaches
/// pgcache through the re-sent Relation message of the next write, which
/// evicts the table's queries.
#[tokio::test]
async fn test_enabling_rls_without_signal_evicts_on_next_write() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    for trigger in ["pgcache_ddl_command_end", "pgcache_sql_drop"] {
        ctx.origin_query(&format!("DROP EVENT TRIGGER {trigger}"), &[])
            .await?;
    }
    for sql in [
        "CREATE TABLE plain_docs (id int primary key, body text)",
        "INSERT INTO plain_docs VALUES (1, 'a')",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await?;
    let sql = "SELECT id, body FROM plain_docs WHERE id = 1";
    query_warm(&mut ctx, sql).await?;

    for change in [
        "ALTER TABLE plain_docs ENABLE ROW LEVEL SECURITY",
        "UPDATE plain_docs SET body = 'a' WHERE id = 1",
    ] {
        ctx.origin_query(change, &[]).await?;
    }
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
    ctx.cache_settle().await?;
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
