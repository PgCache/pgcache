//! Function body inlining (PGC-579): a `LANGUAGE sql` function that is
//! STABLE or IMMUTABLE has its body inlined into the query at the proxy's
//! verdict, so a call the volatility rule refused (`auth.uid()` in WHERE)
//! is cached as what its body computes, the session terms the body reads
//! are keyed on their folded values (ADR-066), and a redefinition on origin
//! evicts the entries computed with the old body. What the body rules leave
//! out (plpgsql, STRICT with arguments, a name two schemas answer, a
//! `BEGIN ATOMIC` body) keeps today's refusal.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::{Client, error::SqlState};

use crate::util::{MetricsSnapshot, TestContext, metrics_delta, result_rows};

mod util;

type Rows = Vec<Vec<Option<String>>>;

const ANN: &str = "11111111-1111-1111-1111-111111111111";
const BOB: &str = "22222222-2222-2222-2222-222222222222";
const ANN_TOKEN: &str =
    r#"{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated","exp":1700000000}"#;
const ANN_TOKEN_LATER: &str =
    r#"{"sub":"11111111-1111-1111-1111-111111111111","role":"authenticated","exp":1700000999}"#;
const BOB_TOKEN: &str =
    r#"{"sub":"22222222-2222-2222-2222-222222222222","role":"anon","exp":1700000000}"#;

/// The helpers Supabase ships in its `auth` schema, bodies as shipped.
const AUTH_SCHEMA: &[&str] = &[
    "CREATE SCHEMA auth",
    "CREATE FUNCTION auth.uid() RETURNS uuid LANGUAGE sql STABLE AS $$ \
       select coalesce( \
         nullif(current_setting('request.jwt.claim.sub', true), ''), \
         (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'sub') \
       )::uuid \
     $$",
    "CREATE FUNCTION auth.role() RETURNS text LANGUAGE sql STABLE AS $$ \
       select coalesce( \
         nullif(current_setting('request.jwt.claim.role', true), ''), \
         (nullif(current_setting('request.jwt.claims', true), '')::jsonb ->> 'role') \
       )::text \
     $$",
    "CREATE FUNCTION auth.jwt() RETURNS jsonb LANGUAGE sql STABLE AS $$ \
       select coalesce( \
         nullif(current_setting('request.jwt.claim', true), ''), \
         nullif(current_setting('request.jwt.claims', true), '') \
       )::jsonb \
     $$",
];

const DOCS: &[&str] = &[
    "CREATE TABLE docs (id int primary key, owner uuid, body text)",
    "INSERT INTO docs VALUES \
       (1, '11111111-1111-1111-1111-111111111111', 'a'), \
       (2, '22222222-2222-2222-2222-222222222222', 'b'), \
       (3, '11111111-1111-1111-1111-111111111111', 'c')",
];

const UID_READ: &str = "SELECT id, body FROM docs WHERE owner = auth.uid() ORDER BY id";

/// `public.current_tenant`, bare in the client's queries: unique in the
/// catalog, so the proxy inlines it and the writer checks the session's
/// path resolves the name to it.
const CURRENT_TENANT: &str = "CREATE FUNCTION current_tenant() RETURNS int LANGUAGE sql STABLE \
     AS $$ select (current_setting('app.claims', true)::jsonb ->> 'tenant')::int $$";

/// A body with a FROM: a scalar subquery once inlined, uncorrelated after
/// the claim folds, so the table it reads joins the entry's maintenance.
const TENANT_NAME: &str = "CREATE FUNCTION tenant_name() RETURNS text LANGUAGE sql STABLE \
     AS $$ select name from tenants where id = current_setting('app.tenant')::int $$";

const TENANTS: &[&str] = &[
    "CREATE TABLE tenants (id int primary key, name text)",
    "INSERT INTO tenants VALUES (1, 'acme'), (2, 'globex')",
    "CREATE TABLE tenant_rows (id int primary key, tenant int, label text)",
    "INSERT INTO tenant_rows VALUES (1, 1, 'acme'), (2, 2, 'globex'), (3, 1, 'acme'), (4, 2, 'acme')",
];

const TENANT_READ: &str = "SELECT id FROM tenant_rows WHERE tenant = current_tenant() ORDER BY id";
const TENANT_NAME_READ: &str = "SELECT id FROM tenant_rows WHERE label = tenant_name() ORDER BY id";

const ITEMS: &[&str] = &[
    "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, n INTEGER)",
    "INSERT INTO items VALUES (1, 'Ann', 10), (2, 'Bob', 20), (3, NULL, 30)",
];

const DOUBLE_IT: &str = "CREATE FUNCTION double_it(x INTEGER) RETURNS INTEGER \
     LANGUAGE sql IMMUTABLE AS $$ SELECT x * 2 $$";
const TRIPLE_IT: &str = "CREATE OR REPLACE FUNCTION double_it(x INTEGER) RETURNS INTEGER \
     LANGUAGE sql IMMUTABLE AS $$ SELECT x * 3 $$";
const DOUBLE_READ: &str = "SELECT id FROM items WHERE double_it(n) = 20 ORDER BY id";

/// Origin with `statements` in place before pgcache starts, so its function
/// catalog knows the functions from the start.
async fn setup_with(statements: &[&'static [&'static str]]) -> Result<TestContext, Error> {
    let statements: Vec<&'static str> = statements.iter().flat_map(|s| s.iter().copied()).collect();
    TestContext::setup_before_start(|origin| async move {
        for sql in statements {
            origin.simple_query(sql).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await
}

/// Through the proxy and directly on origin, both as `postgres`, each with
/// `statements` run first.
async fn sessions_with(ctx: &TestContext, statements: &[&str]) -> Result<(Client, Client), Error> {
    let proxied = ctx.proxy_client_connect().await?;
    let origin = ctx.origin_connect_as("postgres").await?;
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

fn row(id: &str, body: &str) -> Vec<Option<String>> {
    vec![Some(id.to_owned()), Some(body.to_owned())]
}

fn id_row(id: &str) -> Vec<Option<String>> {
    vec![Some(id.to_owned())]
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

/// Warm `sql` through `client` into a cache hit: a setting the body reads
/// that the session never set is unknown until probed, so the first read may
/// forward; the entry registers once and the read after serves. Returns the
/// rows the hit served.
async fn warm(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<Rows, Error> {
    let before = ctx.metrics().await?;
    let served = read_until(ctx, client, sql, |delta| delta.queries_cache_hit == 1).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert!(
        delta.queries_cache_miss >= 1,
        "registered an entry of its own: {sql}\n{delta:#?}"
    );
    assert_eq!(
        delta.queries_cache_error, 0,
        "no error on the cache PG: {sql}"
    );
    Ok(served)
}

/// `sql` through `client` becomes a cache hit without a registration of its
/// own: the session's first read may forward while a setting is probed or a
/// verdict looked up, but nothing misses except the privilege lookup.
async fn hit_wait(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<Rows, Error> {
    let before = ctx.metrics().await?;
    let served = read_until(ctx, client, sql, |delta| delta.queries_cache_hit == 1).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.queries_cache_miss, delta.queries_privilege_forward_pending,
        "served from an entry that already existed: {sql}\n{delta:#?}"
    );
    Ok(served)
}

/// One read of `sql` through `client` is refused at the proxy as uncacheable
/// and answered with origin's rows.
async fn uncacheable_assert(
    ctx: &mut TestContext,
    client: &Client,
    origin: &Client,
    sql: &str,
) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    let proxied = rows(client, sql).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_uncacheable, 1, "refused at the proxy: {sql}");
    assert_eq!(delta.queries_cache_miss, 0, "never registered: {sql}");
    assert_eq!(delta.queries_cache_error, 0);
    assert_eq!(proxied, rows(origin, sql).await?, "{sql}");
    Ok(())
}

/// `auth.uid()` in a client's WHERE is cached as the claim it extracts: a
/// later token of the same user is a hit on the same entry, another user's
/// token has an entry of its own, and each serves origin's rows.
#[tokio::test]
async fn test_auth_uid_in_where_is_keyed_on_the_claim() -> Result<(), Error> {
    let mut ctx = setup_with(&[AUTH_SCHEMA, DOCS]).await?;
    let (ann, origin_ann) = sessions_with(&ctx, &[&token_set(ANN_TOKEN)]).await?;
    let served = warm(&mut ctx, &ann, UID_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_ann, UID_READ).await?);

    let (ann_later, _) = sessions_with(&ctx, &[&token_set(ANN_TOKEN_LATER)]).await?;
    assert_eq!(
        hit_wait(&mut ctx, &ann_later, UID_READ).await?,
        served,
        "a later token of the same user shares the entry"
    );

    let (bob, origin_bob) = sessions_with(&ctx, &[&token_set(BOB_TOKEN)]).await?;
    let served = warm(&mut ctx, &bob, UID_READ).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_bob, UID_READ).await?);
    Ok(())
}

/// Without a token the claims setting is unset: `auth.uid()` is NULL, the
/// read matches nothing, and is served so; a session that then sets a token
/// is keyed afresh.
#[tokio::test]
async fn test_auth_uid_without_a_token_is_null() -> Result<(), Error> {
    let mut ctx = setup_with(&[AUTH_SCHEMA, DOCS]).await?;
    let (anon, origin_anon) = sessions_with(&ctx, &[]).await?;
    let served = warm(&mut ctx, &anon, UID_READ).await?;
    assert_eq!(served, Vec::<Vec<Option<String>>>::new());
    assert_eq!(served, rows(&origin_anon, UID_READ).await?);

    for client in [&anon, &origin_anon] {
        client
            .simple_query(&token_set(ANN_TOKEN))
            .await
            .map_err(Error::other)?;
    }
    let served = warm(&mut ctx, &anon, UID_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_anon, UID_READ).await?);
    Ok(())
}

/// `auth.role()` and a field of `auth.jwt()` in the SELECT list are inlined
/// and folded like a claim the query extracts itself: the row carries the
/// session's values and two tokens of one user share the entry. Another
/// user's read differs only in its literals, so it is served from the same
/// source rows by subsumption, its own literals in the result.
#[tokio::test]
async fn test_auth_role_and_jwt_in_the_select_list_are_inlined() -> Result<(), Error> {
    let mut ctx = setup_with(&[AUTH_SCHEMA, DOCS]).await?;
    let sql = "SELECT id, auth.role() AS role, auth.jwt() ->> 'sub' AS sub \
               FROM docs WHERE id = 1";
    let (ann, origin_ann) = sessions_with(&ctx, &[&token_set(ANN_TOKEN)]).await?;
    let served = warm(&mut ctx, &ann, sql).await?;
    assert_eq!(
        served,
        vec![vec![
            Some("1".to_owned()),
            Some("authenticated".to_owned()),
            Some(ANN.to_owned()),
        ]]
    );
    assert_eq!(served, rows(&origin_ann, sql).await?);

    let (ann_later, _) = sessions_with(&ctx, &[&token_set(ANN_TOKEN_LATER)]).await?;
    assert_eq!(hit_wait(&mut ctx, &ann_later, sql).await?, served);

    let (bob, origin_bob) = sessions_with(&ctx, &[&token_set(BOB_TOKEN)]).await?;
    let served = read_until(&mut ctx, &bob, sql, |delta| delta.queries_cache_hit == 1).await?;
    assert_eq!(
        served,
        vec![vec![
            Some("1".to_owned()),
            Some("anon".to_owned()),
            Some(BOB.to_owned()),
        ]]
    );
    assert_eq!(served, rows(&origin_bob, sql).await?);
    Ok(())
}

/// A bare call that only one function in the catalog answers is inlined;
/// sessions with equal claims share the entry.
#[tokio::test]
async fn test_bare_helper_unique_in_the_catalog_is_inlined() -> Result<(), Error> {
    let mut ctx = setup_with(&[TENANTS, &[CURRENT_TENANT]]).await?;
    let claims = |tenant: u32| format!("SET app.claims = '{{\"tenant\": {tenant}}}'");
    let (one, origin_one) = sessions_with(&ctx, &[&claims(1)]).await?;
    let served = warm(&mut ctx, &one, TENANT_READ).await?;
    assert_eq!(served, vec![id_row("1"), id_row("3")]);
    assert_eq!(served, rows(&origin_one, TENANT_READ).await?);

    let (peer, _) = sessions_with(&ctx, &[&claims(1)]).await?;
    assert_eq!(hit_wait(&mut ctx, &peer, TENANT_READ).await?, served);

    let (two, origin_two) = sessions_with(&ctx, &[&claims(2)]).await?;
    let served = warm(&mut ctx, &two, TENANT_READ).await?;
    assert_eq!(served, vec![id_row("2"), id_row("4")]);
    assert_eq!(served, rows(&origin_two, TENANT_READ).await?);
    Ok(())
}

/// The proxy inlined a bare name the catalog holds once, but the session's
/// search_path does not reach it: origin's own error is the answer, the
/// registration is refused on the path, and nothing is served.
#[tokio::test]
async fn test_bare_helper_off_the_search_path_forwards() -> Result<(), Error> {
    let mut ctx = setup_with(&[TENANTS, &[CURRENT_TENANT, "CREATE SCHEMA app"]]).await?;
    let sql = "SELECT id FROM public.tenant_rows WHERE tenant = current_tenant() ORDER BY id";
    let (one, _) = sessions_with(&ctx, &["SET app.claims = '{\"tenant\": 1}'"]).await?;
    warm(&mut ctx, &one, sql).await?;

    let (off_path, origin_off_path) = sessions_with(
        &ctx,
        &[
            "SET app.claims = '{\"tenant\": 1}'",
            "SET search_path = app",
        ],
    )
    .await?;
    let origin_error = origin_off_path
        .simple_query(sql)
        .await
        .expect_err("origin does not resolve the bare name on this path");
    assert_eq!(origin_error.code(), Some(&SqlState::UNDEFINED_FUNCTION));
    let before = ctx.metrics().await?;
    let error = off_path
        .simple_query(sql)
        .await
        .expect_err("the proxy forwards and relays origin's error");
    assert_eq!(error.code(), Some(&SqlState::UNDEFINED_FUNCTION), "{error}");
    let mut delta = MetricsSnapshot::default();
    for _ in 0..40 {
        delta = metrics_delta(&before, &ctx.metrics().await?);
        if delta.queries_function_forward_inlined_elsewhere >= 1 {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        delta.queries_function_forward_inlined_elsewhere, 1,
        "refused on the session's path\n{delta:#?}"
    );
    assert_eq!(delta.queries_cache_hit, 0, "never served");
    assert_eq!(delta.queries_cache_error, 0);
    Ok(())
}

/// A body with a FROM is inlined as a scalar subquery: the read is cached,
/// and a change to the table the body reads reaches the served rows.
#[tokio::test]
async fn test_helper_reading_a_table_is_maintained_by_cdc() -> Result<(), Error> {
    let mut ctx = setup_with(&[TENANTS, &[TENANT_NAME]]).await?;
    let (one, origin_one) = sessions_with(&ctx, &["SET app.tenant = '1'"]).await?;
    let served = warm(&mut ctx, &one, TENANT_NAME_READ).await?;
    assert_eq!(served, vec![id_row("1"), id_row("3"), id_row("4")]);
    assert_eq!(served, rows(&origin_one, TENANT_NAME_READ).await?);

    ctx.origin_query("UPDATE tenants SET name = 'globex' WHERE id = 1", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert_eq!(
        rows(&origin_one, TENANT_NAME_READ).await?,
        vec![id_row("2")],
        "origin follows the renamed tenant"
    );
    let served = read_until(&mut ctx, &one, TENANT_NAME_READ, |delta| {
        delta.queries_cache_hit == 1
    })
    .await?;
    assert_eq!(served, vec![id_row("2")]);
    Ok(())
}

/// An IMMUTABLE sql function with an argument is inlined with the argument
/// substituted, so the read is cached although the cache database lacks the
/// function; `CREATE OR REPLACE` evicts the entry and the read follows the
/// new body.
#[tokio::test]
async fn test_immutable_sql_helper_with_an_argument_is_inlined_and_redefinition_evicts()
-> Result<(), Error> {
    let mut ctx = setup_with(&[ITEMS, &[DOUBLE_IT]]).await?;
    let (client, origin) = sessions_with(&ctx, &[]).await?;
    let served = warm(&mut ctx, &client, DOUBLE_READ).await?;
    assert_eq!(served, vec![id_row("1")]);
    assert_eq!(served, rows(&origin, DOUBLE_READ).await?);
    let before = ctx.metrics().await?;
    assert_eq!(rows(&client, DOUBLE_READ).await?, served);
    let delta = metrics_delta(&before, &ctx.metrics_after_outcomes(&before, 1).await?);
    assert_eq!(delta.queries_cache_hit, 1, "{delta:#?}");

    ctx.origin_query(TRIPLE_IT, &[]).await?;
    ctx.cache_settle().await?;
    assert!(
        rows(&origin, DOUBLE_READ).await?.is_empty(),
        "origin now triples"
    );
    let served = warm(&mut ctx, &client, DOUBLE_READ).await?;
    assert_eq!(served, Vec::<Vec<Option<String>>>::new());
    assert_eq!(served, rows(&origin, DOUBLE_READ).await?);
    Ok(())
}

/// What the body rules leave out keeps today's refusal: a plpgsql body, a
/// STRICT function with arguments, a bare name two schemas answer, and a
/// `BEGIN ATOMIC` body; the schema-qualified call of the ambiguous name is
/// inlined.
#[tokio::test]
async fn test_bodies_outside_the_rules_are_not_inlined() -> Result<(), Error> {
    let mut ctx = setup_with(&[
        ITEMS,
        &[
            "CREATE FUNCTION plpg_n(x INTEGER) RETURNS INTEGER LANGUAGE plpgsql STABLE \
             AS $$ begin return x; end $$",
            "CREATE FUNCTION strict_n(x INTEGER) RETURNS INTEGER LANGUAGE sql STABLE STRICT \
             AS $$ select x $$",
            "CREATE SCHEMA a",
            "CREATE SCHEMA b",
            "CREATE FUNCTION a.pick() RETURNS INTEGER LANGUAGE sql STABLE AS $$ select 20 $$",
            "CREATE FUNCTION b.pick() RETURNS INTEGER LANGUAGE sql STABLE AS $$ select 30 $$",
            "CREATE FUNCTION atomic_n(x INTEGER) RETURNS INTEGER STABLE \
             BEGIN ATOMIC SELECT x; END",
        ],
    ])
    .await?;
    let (client, origin) = sessions_with(&ctx, &["SET search_path = a, public"]).await?;
    for sql in [
        "SELECT id FROM items WHERE plpg_n(n) = 20",
        "SELECT id FROM items WHERE strict_n(n) = 20",
        "SELECT id FROM items WHERE pick() = n",
        "SELECT id FROM items WHERE atomic_n(n) = 20",
    ] {
        uncacheable_assert(&mut ctx, &client, &origin, sql).await?;
    }
    let sql = "SELECT id FROM items WHERE n = a.pick() ORDER BY id";
    let served = warm(&mut ctx, &client, sql).await?;
    assert_eq!(served, vec![id_row("2")]);
    assert_eq!(served, rows(&origin, sql).await?);
    Ok(())
}
