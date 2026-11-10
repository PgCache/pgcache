//! Session terms in a client's own query (PGC-580): a read of
//! `current_setting(…)` or `current_user` is keyed on the term's value and
//! cached, two sessions with equal values sharing one entry, as a policy's
//! terms are since PGC-113. A value the session cannot supply forwards with
//! origin's answer. A JWT claims setting keys on the extracted claim, not on
//! the token, so two tokens of one user share the entry.

use std::io::Error;
use std::time::Duration;

use tokio_postgres::{Client, error::SqlState};

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, metrics_delta, result_rows};

mod util;

type Rows = Vec<Vec<Option<String>>>;

const DOCS: &[&str] = &[
    "CREATE TABLE docs (id int primary key, tenant int, owner text, body text)",
    "INSERT INTO docs VALUES (1, 1, 'ann', 'a'), (2, 2, 'bob', 'b'), (3, 1, 'ann', 'c')",
    "CREATE ROLE ann LOGIN",
    "CREATE ROLE bob LOGIN",
    "GRANT SELECT ON docs TO ann, bob",
];

const TENANT_READ: &str =
    "SELECT id, body FROM docs WHERE tenant = current_setting('app.tenant')::int ORDER BY id";
const OWNER_READ: &str = "SELECT id, body FROM docs WHERE owner = current_user ORDER BY id";
const CLAIM_READ: &str = "SELECT id, body FROM docs \
     WHERE owner = (current_setting('request.jwt.claims', true)::jsonb ->> 'sub') ORDER BY id";
const FAST_FORM_READ: &str =
    "SELECT id, body FROM docs WHERE owner = (SELECT current_setting('app.who')) ORDER BY id";

const ANN_TOKEN: &str = r#"{"sub":"ann","role":"authenticated","exp":1700000000}"#;
const ANN_TOKEN_LATER: &str = r#"{"sub":"ann","role":"authenticated","exp":1700000999}"#;
const BOB_TOKEN: &str = r#"{"sub":"bob","role":"authenticated","exp":1700000000}"#;

async fn setup() -> Result<TestContext, Error> {
    TestContext::setup_before_start(|origin| async move {
        for sql in DOCS {
            origin.simple_query(sql).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await
}

fn row(id: &str, body: &str) -> Vec<Option<String>> {
    vec![Some(id.to_owned()), Some(body.to_owned())]
}

async fn rows(client: &Client, sql: &str) -> Result<Rows, Error> {
    Ok(result_rows(
        &client.simple_query(sql).await.map_err(Error::other)?,
    ))
}

/// A proxied session and its origin twin as `user`, each with `statements`
/// run first.
async fn sessions(
    ctx: &TestContext,
    user: Option<&str>,
    statements: &[&str],
) -> Result<(Client, Client), Error> {
    let (proxied, origin) = match user {
        Some(user) => (
            ctx.proxy_client_connect_as(user).await?,
            ctx.origin_connect_as(user).await?,
        ),
        None => (
            ctx.proxy_client_connect().await?,
            ctx.origin_connect().await?,
        ),
    };
    for sql in statements {
        proxied.simple_query(sql).await.map_err(Error::other)?;
        origin.simple_query(sql).await.map_err(Error::other)?;
    }
    Ok((proxied, origin))
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

/// One read of `sql` through `client`, served from cache at once.
async fn hit(ctx: &mut TestContext, client: &Client, sql: &str) -> Result<Rows, Error> {
    let m = ctx.metrics().await?;
    let served = rows(client, sql).await?;
    assert_cache_hit(ctx, m).await?;
    Ok(served)
}

/// A custom setting in a predicate keys the read on its value: each tenant
/// has its own entry, and a second session of the same tenant shares it.
#[tokio::test]
async fn test_custom_setting_term_is_cached_per_tenant() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (tenant_1, origin_1) = sessions(&ctx, None, &["SET app.tenant = '1'"]).await?;
    let served = register(&mut ctx, &tenant_1, TENANT_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_1, TENANT_READ).await?);

    let (tenant_1_again, _) = sessions(&ctx, None, &["SET app.tenant = '1'"]).await?;
    assert_eq!(
        hit(&mut ctx, &tenant_1_again, TENANT_READ).await?,
        served,
        "a second session of the tenant shares the entry"
    );

    let (tenant_2, origin_2) = sessions(&ctx, None, &["SET app.tenant = '2'"]).await?;
    let served = register(&mut ctx, &tenant_2, TENANT_READ).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_2, TENANT_READ).await?);
    Ok(())
}

/// `SET LOCAL` changes the term for the block alone: the read inside sees
/// the block's tenant, the one after sees the session's again.
#[tokio::test]
async fn test_set_local_scopes_the_term_to_the_block() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (proxied, origin) = sessions(&ctx, None, &["SET app.tenant = '1'"]).await?;
    let session_rows = register(&mut ctx, &proxied, TENANT_READ).await?;

    for client in [&proxied, &origin] {
        client
            .batch_execute("BEGIN; SET LOCAL app.tenant = '2'")
            .await
            .map_err(Error::other)?;
    }
    assert_eq!(rows(&proxied, TENANT_READ).await?, vec![row("2", "b")]);
    assert_eq!(
        rows(&proxied, TENANT_READ).await?,
        rows(&origin, TENANT_READ).await?
    );
    for client in [&proxied, &origin] {
        client.batch_execute("COMMIT").await.map_err(Error::other)?;
    }
    assert_eq!(hit(&mut ctx, &proxied, TENANT_READ).await?, session_rows);
    Ok(())
}

/// A setting the session never set is origin's error (`42704`), not a
/// cached answer and not a cache error.
#[tokio::test]
async fn test_unset_setting_forwards_with_origins_error() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (proxied, _) = sessions(&ctx, None, &[]).await?;
    let before = ctx.metrics().await?;
    let error = proxied
        .simple_query(TENANT_READ)
        .await
        .expect_err("an unset setting is an error on origin");
    assert_eq!(
        error.as_db_error().map(|e| e.code().clone()),
        Some(SqlState::UNDEFINED_OBJECT),
        "{error}"
    );
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(delta.queries_cache_hit, 0);
    assert_eq!(delta.queries_cache_error, 0);
    Ok(())
}

/// `current_user` keys the read on the role: each role has its own entry
/// with its own rows, and a second session of a role shares it.
#[tokio::test]
async fn test_current_user_term_is_cached_per_role() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (ann, origin_ann) = sessions(&ctx, Some("ann"), &[]).await?;
    let served = register(&mut ctx, &ann, OWNER_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_ann, OWNER_READ).await?);

    let (bob, origin_bob) = sessions(&ctx, Some("bob"), &[]).await?;
    let served = register(&mut ctx, &bob, OWNER_READ).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_bob, OWNER_READ).await?);

    let (ann_again, _) = sessions(&ctx, Some("ann"), &[]).await?;
    assert_eq!(
        hit(&mut ctx, &ann_again, OWNER_READ).await?,
        vec![row("1", "a"), row("3", "c")]
    );
    Ok(())
}

/// A claim extracted from the JWT claims setting keys the read on the
/// claim: two tokens of one user, differing in `exp`, share the entry, and
/// another user's token has its own.
#[tokio::test]
async fn test_jwt_claim_extraction_is_keyed_on_the_claim() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let token_set =
        |token: &str| format!("SELECT set_config('request.jwt.claims', '{token}', false)");
    let (ann, origin_ann) = sessions(&ctx, None, &[&token_set(ANN_TOKEN)]).await?;
    let served = register(&mut ctx, &ann, CLAIM_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin_ann, CLAIM_READ).await?);

    let (ann_later, _) = sessions(&ctx, None, &[&token_set(ANN_TOKEN_LATER)]).await?;
    assert_eq!(
        hit(&mut ctx, &ann_later, CLAIM_READ).await?,
        served,
        "a later token of the same user shares the entry"
    );

    let (bob, origin_bob) = sessions(&ctx, None, &[&token_set(BOB_TOKEN)]).await?;
    let served = register(&mut ctx, &bob, CLAIM_READ).await?;
    assert_eq!(served, vec![row("2", "b")]);
    assert_eq!(served, rows(&origin_bob, CLAIM_READ).await?);
    Ok(())
}

/// The PostgREST request shape: a block that sets the claims for the block
/// and reads. A second request with a later token of the same user is a
/// hit; a request without a token sees no rows, as on origin.
#[tokio::test]
async fn test_postgrest_style_block_is_served_per_claim() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (proxied, origin) = sessions(&ctx, None, &[]).await?;
    let request =
        |token: &str| format!("BEGIN; SELECT set_config('request.jwt.claims', '{token}', true)");
    for client in [&proxied, &origin] {
        client
            .batch_execute(&request(ANN_TOKEN))
            .await
            .map_err(Error::other)?;
    }
    let served = register(&mut ctx, &proxied, CLAIM_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, CLAIM_READ).await?);
    for client in [&proxied, &origin] {
        client.batch_execute("COMMIT").await.map_err(Error::other)?;
    }

    for client in [&proxied, &origin] {
        client
            .batch_execute(&request(ANN_TOKEN_LATER))
            .await
            .map_err(Error::other)?;
    }
    assert_eq!(hit(&mut ctx, &proxied, CLAIM_READ).await?, served);
    for client in [&proxied, &origin] {
        client.batch_execute("COMMIT").await.map_err(Error::other)?;
    }

    // Outside a request no claims are set: `missing_ok` makes the claim
    // NULL, which matches no owner.
    assert_eq!(rows(&proxied, CLAIM_READ).await?, Rows::new());
    assert_eq!(rows(&origin, CLAIM_READ).await?, Rows::new());
    Ok(())
}

/// The documented fast form wraps the term in a FROM-less scalar subquery;
/// it folds to the term's value like the bare call.
#[tokio::test]
async fn test_fast_form_scalar_subquery_is_folded() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (proxied, origin) = sessions(&ctx, None, &["SET app.who = 'ann'"]).await?;
    let served = register(&mut ctx, &proxied, FAST_FORM_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, FAST_FORM_READ).await?);
    Ok(())
}

/// A `set_config` whose value arrives as a bind parameter is known to the
/// proxy at Bind: the read that follows needs no settings probe.
#[tokio::test]
async fn test_bound_set_config_value_is_known_without_a_probe() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (proxied, _) = sessions(&ctx, None, &["SELECT 1"]).await?;
    // The connection's own startup probe settles before the count starts.
    tokio::time::sleep(Duration::from_millis(200)).await;
    let before = ctx.metrics().await?;

    proxied.batch_execute("BEGIN").await.map_err(Error::other)?;
    proxied
        .query("SELECT set_config('app.tenant', $1, true)", &[&"1"])
        .await
        .map_err(Error::other)?;
    let m = ctx.metrics().await?;
    assert_eq!(
        rows(&proxied, TENANT_READ).await?,
        vec![row("1", "a"), row("3", "c")]
    );
    let m = assert_cache_miss(&mut ctx, m).await?;
    proxied
        .batch_execute("COMMIT")
        .await
        .map_err(Error::other)?;
    ctx.cache_settle().await?;

    proxied.batch_execute("BEGIN").await.map_err(Error::other)?;
    proxied
        .query("SELECT set_config('app.tenant', $1, true)", &[&"1"])
        .await
        .map_err(Error::other)?;
    assert_eq!(
        rows(&proxied, TENANT_READ).await?,
        vec![row("1", "a"), row("3", "c")]
    );
    assert_cache_hit(&mut ctx, m).await?;
    proxied
        .batch_execute("COMMIT")
        .await
        .map_err(Error::other)?;

    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.conn_settings_probes, 0,
        "a bound set_config value is known at Bind, so no probe was needed"
    );
    Ok(())
}
