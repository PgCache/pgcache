//! Session terms in a client's own query (PGC-580): a read of
//! `current_setting(…)` or `current_user` is keyed on the term's value and
//! cached, two sessions with equal values sharing one entry, as a policy's
//! terms are since PGC-113. A value the session cannot supply forwards with
//! origin's answer. A JWT claims setting keys on the extracted claim, not on
//! the token, so two tokens of one user share the entry.

use std::io::Error;

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
const FALLBACK_READ: &str = "SELECT id, body FROM docs \
     WHERE tenant = coalesce(current_setting('app.tenant', true), $1)::int ORDER BY id";

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

/// A context whose in-block reads are served: the read-after-write gate the
/// harness turns off is what admits a read inside a transaction block. The
/// table and roles are created after start, so the stream is settled before
/// the first read.
#[cfg(feature = "fault-injection")]
async fn setup_in_block() -> Result<TestContext, Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_READ_YOUR_WRITES_OFF", "0")]).await?;
    for sql in DOCS {
        ctx.origin_query(*sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    Ok(ctx)
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

/// The rows of an extended-protocol read of `sql` with one text parameter.
async fn bound_rows(client: &Client, sql: &str, parameter: &str) -> Result<Rows, Error> {
    let rows = client
        .query(sql, &[&parameter])
        .await
        .map_err(Error::other)?;
    Ok(rows
        .iter()
        .map(|r| {
            vec![
                Some(r.get::<_, i32>(0).to_string()),
                r.get::<_, Option<String>>(1),
            ]
        })
        .collect())
}

/// A term beside a bind parameter, `coalesce(current_setting(…), $1)`: the
/// proxy folds the term alone, since the parameter is no part of what it
/// can evaluate, and the writer substitutes that same term in the bound
/// query, so the parameter's fallback applies when the setting is unset
/// and the setting wins when it is set.
#[tokio::test]
async fn test_term_beside_a_parameter_keeps_the_parameter() -> Result<(), Error> {
    let mut ctx = setup().await?;
    let (unset, origin) = sessions(&ctx, None, &[]).await?;
    // The first read forwards: the setting is unknown until the probe it
    // schedules reports it unset.
    let fallback = bound_rows(&unset, FALLBACK_READ, "2").await?;
    assert_eq!(fallback, vec![row("2", "b")]);
    assert_eq!(fallback, bound_rows(&origin, FALLBACK_READ, "2").await?);
    let m = ctx.metrics().await?;
    assert_eq!(bound_rows(&unset, FALLBACK_READ, "2").await?, fallback);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    assert_eq!(bound_rows(&unset, FALLBACK_READ, "2").await?, fallback);
    assert_cache_hit(&mut ctx, m).await?;

    let (tenant_1, origin_1) = sessions(&ctx, None, &["SET app.tenant = '1'"]).await?;
    let m = ctx.metrics().await?;
    let set = bound_rows(&tenant_1, FALLBACK_READ, "2").await?;
    assert_eq!(set, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(set, bound_rows(&origin_1, FALLBACK_READ, "2").await?);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    assert_eq!(bound_rows(&tenant_1, FALLBACK_READ, "2").await?, set);
    assert_cache_hit(&mut ctx, m).await?;
    Ok(())
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
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_set_local_scopes_the_term_to_the_block() -> Result<(), Error> {
    let mut ctx = setup_in_block().await?;
    let (proxied, origin) = sessions(&ctx, None, &["SET app.tenant = '1'"]).await?;
    let session_rows = register(&mut ctx, &proxied, TENANT_READ).await?;

    for client in [&proxied, &origin] {
        for sql in ["BEGIN", "SET LOCAL app.tenant = '2'"] {
            client.simple_query(sql).await.map_err(Error::other)?;
        }
    }
    let served = register(&mut ctx, &proxied, TENANT_READ).await?;
    assert_eq!(served, vec![row("2", "b")], "the block's tenant");
    assert_eq!(served, rows(&origin, TENANT_READ).await?);
    for client in [&proxied, &origin] {
        client.simple_query("COMMIT").await.map_err(Error::other)?;
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
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_postgrest_style_block_is_served_per_claim() -> Result<(), Error> {
    let mut ctx = setup_in_block().await?;
    let (proxied, origin) = sessions(&ctx, None, &[]).await?;
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
    let served = register(&mut ctx, &proxied, CLAIM_READ).await?;
    assert_eq!(served, vec![row("1", "a"), row("3", "c")]);
    assert_eq!(served, rows(&origin, CLAIM_READ).await?);
    for client in [&proxied, &origin] {
        client.simple_query("COMMIT").await.map_err(Error::other)?;
    }

    for client in [&proxied, &origin] {
        for sql in request_begin(ANN_TOKEN_LATER) {
            client.simple_query(&sql).await.map_err(Error::other)?;
        }
    }
    assert_eq!(hit(&mut ctx, &proxied, CLAIM_READ).await?, served);
    for client in [&proxied, &origin] {
        client.simple_query("COMMIT").await.map_err(Error::other)?;
    }

    // Outside a request the local `set_config` has reverted to an empty
    // string, which is not json: the read is origin's error through the
    // proxy as directly, never a cached answer.
    for client in [&proxied, &origin] {
        let error = client
            .simple_query(CLAIM_READ)
            .await
            .expect_err("an empty claims setting is not json");
        assert_eq!(
            error.as_db_error().map(|e| e.code().clone()),
            Some(SqlState::INVALID_TEXT_REPRESENTATION),
            "{error}"
        );
    }
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
/// proxy at Execute: no settings probe runs between it and the read that
/// follows, in the first request or the next. (A block's end does probe: a
/// local `set_config` of a setting never set at session level leaves it as
/// an empty string, which only origin can report.)
#[cfg(feature = "fault-injection")]
#[tokio::test]
async fn test_bound_set_config_value_is_known_without_a_probe() -> Result<(), Error> {
    let mut ctx = setup_in_block().await?;
    let (proxied, _) = sessions(&ctx, None, &[]).await?;
    let expected = vec![row("1", "a"), row("3", "c")];

    proxied.batch_execute("BEGIN").await.map_err(Error::other)?;
    let before = ctx.metrics().await?;
    proxied
        .query("SELECT set_config('app.tenant', $1, true)", &[&"1"])
        .await
        .map_err(Error::other)?;
    let m = ctx.metrics().await?;
    assert_eq!(rows(&proxied, TENANT_READ).await?, expected);
    let m = assert_cache_miss(&mut ctx, m).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.conn_settings_probes, 0,
        "the bound value needed no probe"
    );
    proxied
        .batch_execute("COMMIT")
        .await
        .map_err(Error::other)?;
    ctx.cache_settle().await?;

    proxied.batch_execute("BEGIN").await.map_err(Error::other)?;
    let before = ctx.metrics().await?;
    proxied
        .query("SELECT set_config('app.tenant', $1, true)", &[&"1"])
        .await
        .map_err(Error::other)?;
    assert_eq!(rows(&proxied, TENANT_READ).await?, expected);
    assert_cache_hit(&mut ctx, m).await?;
    let delta = metrics_delta(&before, &ctx.metrics().await?);
    assert_eq!(
        delta.conn_settings_probes, 0,
        "the bound value needed no probe"
    );
    proxied
        .batch_execute("COMMIT")
        .await
        .map_err(Error::other)?;
    Ok(())
}
