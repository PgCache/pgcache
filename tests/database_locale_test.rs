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

/// `--origin_locale` prints origin's locale line and exits 0; the image's
/// entrypoint chooses its cache PostgreSQL flavour from it.
#[tokio::test]
async fn test_origin_locale_mode_prints_origin_locale() -> Result<(), Error> {
    let (dbs, _origin) = start_databases().await?;
    let output = pgcache_run_to_exit(&dbs, &["--origin_locale"], &[], Duration::from_secs(30))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(output.status.success(), "exit 0: {stdout}");
    let line = stdout
        .lines()
        .find(|l| l.starts_with("provider="))
        .unwrap_or_default();
    assert!(
        line.contains("provider=libc"),
        "names the provider: {stdout}"
    );
    assert!(
        line.contains("lc_collate="),
        "names the collation: {stdout}"
    );
    Ok(())
}

/// A cache over an origin database under ICU `en-US`, where collation order
/// and byte order disagree (`'a' < 'B'` there, `'a' > 'B'` in bytes), and a
/// proxy session on that database with `ddl` applied through it.
async fn icu_origin_setup(ddl: &[&str]) -> Result<(TestContext, tokio_postgres::Client), Error> {
    let ctx = TestContext::setup_before_start_with_args(
        |origin| async move {
            origin
                .batch_execute(
                    "CREATE DATABASE origin_icu TEMPLATE template0 ENCODING 'UTF8' \
                     LOCALE_PROVIDER icu ICU_LOCALE 'en-US' LC_COLLATE 'C' LC_CTYPE 'C'",
                )
                .await
                .map_err(Error::other)?;
            Ok(origin)
        },
        &["--origin_database", "origin_icu"],
    )
    .await?;
    let (proxy, connection) = Config::new()
        .host("127.0.0.1")
        .port(ctx.cache_port)
        .user("postgres")
        .dbname("origin_icu")
        .connect(NoTls)
        .await
        .map_err(Error::other)?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("proxy connection error: {e}");
        }
    });
    for sql in ddl {
        proxy.simple_query(sql).await.map_err(Error::other)?;
    }
    Ok((ctx, proxy))
}

/// `sql` through the proxy, as its first column.
async fn rows(proxy: &tokio_postgres::Client, sql: &str) -> Result<Vec<String>, Error> {
    Ok(column(
        &proxy.simple_query(sql).await.map_err(Error::other)?,
    ))
}

/// What origin answers for `predicate` over `names`: a volatile call keeps
/// the read off the cache.
async fn origin_ids(proxy: &tokio_postgres::Client, predicate: &str) -> Result<Vec<String>, Error> {
    rows(
        proxy,
        &format!("select id from names where {predicate} and random() >= 0 order by id"),
    )
    .await
}

/// A text range over a linguistically collated column is maintained by the
/// cache database, not by comparing bytes: a row that enters the range only
/// under the collation's order (PGC-446) is served after its insert.
#[tokio::test]
async fn test_text_range_follows_linguistic_collation() -> Result<(), Error> {
    let (mut ctx, proxy) = icu_origin_setup(&[
        "CREATE TABLE names (id int primary key, name text)",
        "INSERT INTO names VALUES (1, 'A'), (2, 'C')",
    ])
    .await?;

    let sql = "select id from names where name < 'B' order by id";
    let m = ctx.metrics().await?;
    assert_eq!(rows(&proxy, sql).await?, ["1"]);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    assert_eq!(rows(&proxy, sql).await?, ["1"]);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    // 'a' is inside the range only under the collation, '0' under both
    // orders, 'c' under neither.
    proxy
        .simple_query("INSERT INTO names VALUES (3, 'a'), (4, '0'), (5, 'c')")
        .await
        .map_err(Error::other)?;
    ctx.cdc_apply_settle().await?;
    let expected = origin_ids(&proxy, "name < 'B'").await?;
    assert_eq!(expected, ["1", "3", "4"]);
    let m = ctx.metrics().await?;
    assert_eq!(rows(&proxy, sql).await?, expected);
    let _ = assert_cache_hit(&mut ctx, m).await?;

    // A narrower text range is not provably inside the cached one.
    let expected = origin_ids(&proxy, "name < 'A'").await?;
    let m = ctx.metrics().await?;
    assert_eq!(
        rows(&proxy, "select id from names where name < 'A' order by id").await?,
        expected
    );
    let _ = assert_cache_miss(&mut ctx, m).await?;
    Ok(())
}

/// A column declared under a byte-ordered collation inside a linguistic
/// database: the cache table's column carries the collation, so a served
/// `ORDER BY` and range read origin's order, and its ranges subsume.
#[tokio::test]
async fn test_byte_ordered_column_in_linguistic_database() -> Result<(), Error> {
    let (mut ctx, proxy) = icu_origin_setup(&[
        "CREATE TABLE names (id int primary key, name text COLLATE \"C\")",
        "INSERT INTO names VALUES (1, 'A'), (2, 'C'), (3, 'a'), (4, 'b')",
    ])
    .await?;

    let cache_db = connect_cache_db(&ctx.dbs).await?;
    let ordered = "select id from names order by name, id";
    let m = ctx.metrics().await?;
    let forwarded = rows(&proxy, ordered).await?;
    assert_eq!(
        forwarded,
        ["1", "2", "3", "4"],
        "origin sorts the column by bytes"
    );
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    assert_eq!(rows(&proxy, ordered).await?, forwarded);
    let m = assert_cache_hit(&mut ctx, m).await?;
    let collation: String = cache_db
        .query_one(
            "SELECT co.collname::text FROM pg_attribute a \
             JOIN pg_collation co ON co.oid = a.attcollation \
             WHERE a.attrelid = 'names'::regclass AND a.attname = 'name'",
            &[],
        )
        .await
        .map_err(Error::other)?
        .get(0);
    assert_eq!(
        collation, "C",
        "the cache column is created under origin's collation"
    );

    // In bytes 'a' is above 'M': the range holds 1 and 2 only.
    let wide = "select id from names where name < 'M' order by id";
    assert_eq!(rows(&proxy, wide).await?, ["1", "2"]);
    ctx.cache_settle().await?;
    let m = ctx.metrics_after_outcomes(&m, 1).await?;
    let narrow = "select id from names where name < 'B' order by id";
    assert_eq!(rows(&proxy, narrow).await?, ["1"]);
    let after = ctx.metrics_after_outcomes(&m, 1).await?;
    assert_eq!(
        after.cache_subsumptions,
        m.cache_subsumptions + 1,
        "a byte-ordered text range is subsumed by a wider one"
    );

    proxy
        .simple_query("INSERT INTO names VALUES (5, 'B'), (6, 'c')")
        .await
        .map_err(Error::other)?;
    ctx.cdc_apply_settle().await?;
    let expected = origin_ids(&proxy, "name < 'M'").await?;
    assert_eq!(expected, ["1", "2", "5"]);
    let m = ctx.metrics().await?;
    assert_eq!(rows(&proxy, wide).await?, expected);
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}

/// A column under a collation the cache database does not have (here a
/// nondeterministic one created on origin) keeps its table off the cache:
/// reads are forwarded and stay correct.
#[tokio::test]
async fn test_table_with_unavailable_collation_is_forwarded() -> Result<(), Error> {
    let (mut ctx, proxy) = icu_origin_setup(&[
        "CREATE COLLATION case_insensitive \
         (provider = icu, locale = 'und-u-ks-level2', deterministic = false)",
        "CREATE TABLE names (id int primary key, name text COLLATE case_insensitive)",
        "INSERT INTO names VALUES (1, 'a'), (2, 'A'), (3, 'b')",
    ])
    .await?;

    let sql = "select id from names where name = 'a' order by id";
    let m = ctx.metrics().await?;
    assert_eq!(rows(&proxy, sql).await?, ["1", "2"]);
    ctx.cache_settle().await?;
    assert_eq!(rows(&proxy, sql).await?, ["1", "2"]);
    let after = ctx.metrics().await?;
    assert_eq!(
        after.queries_cache_hit, m.queries_cache_hit,
        "never served from cache"
    );
    Ok(())
}
