//! The cache database is created under origin's locale, and startup is
//! refused when the cache host orders text differently from origin (PGC-587).

use std::io::Error;
use std::time::Duration;

use tokio_postgres::{Config, NoTls, SimpleQueryMessage};

use crate::util::{
    TestContext, assert_cache_hit, assert_cache_miss, connect_cache_db, pgcache_run_to_exit,
    start_databases,
};

mod util;

fn column(res: &[SimpleQueryMessage]) -> Vec<String> {
    res.iter()
        .filter_map(|message| {
            let SimpleQueryMessage::Row(row) = message else {
                return None;
            };
            Some(row.get(0).unwrap_or_default().to_owned())
        })
        .collect()
}

/// A libc locale the origin cluster has installed that is not its template
/// default, so the mirror is observable. `pg_collation` lists what `initdb`
/// imported from the host, so the pick does not assume any one locale exists.
const OTHER_LOCALE_SQL: &str = "SELECT collcollate FROM pg_collation \
     WHERE collprovider = 'c' AND collcollate = collctype \
       AND collencoding IN (-1, pg_char_to_encoding('UTF8')) \
       AND lower(replace(collcollate, '-', '')) <> \
           lower(replace((SELECT datcollate FROM pg_database \
                          WHERE datname = current_database()), '-', '')) \
       AND collcollate ~ '^[a-z]{2}_[A-Z]{2}\\.(utf8|UTF-8)$' \
     ORDER BY collcollate LIMIT 1";

/// An origin database under a locale the cache cluster's template does not
/// use: the cache database pgcache creates carries origin's collation and
/// ctype, and a text `ORDER BY` served from cache returns the forwarded order.
#[tokio::test]
async fn test_cache_database_mirrors_origin_locale() -> Result<(), Error> {
    let mut ctx = TestContext::setup_before_start_with_args(
        |origin| async move {
            let locale: String = origin
                .query_opt(OTHER_LOCALE_SQL, &[])
                .await
                .map_err(Error::other)?
                .ok_or_else(|| {
                    Error::other("origin cluster has no installed UTF-8 locale besides its default")
                })?
                .get(0);
            origin
                .batch_execute(&format!(
                    "CREATE DATABASE origin_other TEMPLATE template0 \
                     LC_COLLATE '{locale}' LC_CTYPE '{locale}'"
                ))
                .await
                .map_err(Error::other)?;
            Ok(origin)
        },
        &["--origin_database", "origin_other"],
    )
    .await?;

    let expected = ctx
        .origin
        .query_one(
            "SELECT datcollate, datctype FROM pg_database WHERE datname = 'origin_other'",
            &[],
        )
        .await
        .map_err(Error::other)?;
    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let mirrored = cache_db
        .query_one(
            "SELECT datcollate, datctype FROM pg_database WHERE datname = current_database()",
            &[],
        )
        .await
        .map_err(Error::other)?;
    assert_eq!(
        mirrored.get::<_, String>(0),
        expected.get::<_, String>(0),
        "LC_COLLATE mirrored"
    );
    assert_eq!(
        mirrored.get::<_, String>(1),
        expected.get::<_, String>(1),
        "LC_CTYPE mirrored"
    );

    // The harness client names the default origin database; a session on
    // origin_other is the one pgcache caches for.
    let (proxy, connection) = Config::new()
        .host("127.0.0.1")
        .port(ctx.cache_port)
        .user("postgres")
        .dbname("origin_other")
        .connect(NoTls)
        .await
        .map_err(Error::other)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("proxy connection error: {e}");
        }
    });
    for sql in [
        "CREATE TABLE names (id int primary key, name text)",
        "INSERT INTO names VALUES (1, 'b'), (2, 'A'), (3, 'a'), (4, 'B'), (5, 'ä'), (6, 'a-b'), (7, 'ab')",
    ] {
        proxy.simple_query(sql).await.map_err(Error::other)?;
    }
    let sql = "select name from names order by name, id";
    let m = ctx.metrics().await?;
    let forwarded = column(&proxy.simple_query(sql).await.map_err(Error::other)?);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let served = column(&proxy.simple_query(sql).await.map_err(Error::other)?);
    let _ = assert_cache_hit(&mut ctx, m).await?;
    assert_eq!(served, forwarded, "cache orders text as origin does");
    Ok(())
}

/// With the fault hook reversing the cache side's sample order, startup is
/// refused and the message names the first disagreeing pair.
#[tokio::test]
async fn test_startup_refused_when_collation_order_differs() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let output = pgcache_run_to_exit(
        &dbs,
        &["--cache_policy", "fifo"],
        &[("PGCACHE_FAULT_COLLATION_MISMATCH", "1")],
        Duration::from_secs(60),
    )?;
    assert!(!output.status.success(), "startup refused");
    let text = format!(
        "{}{}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        text.contains("cache host orders text differently from origin"),
        "names the mismatch: {text}"
    );
    assert!(
        text.contains("at sample position 0"),
        "names the first disagreeing position: {text}"
    );
    Ok(())
}
