//! pgcache's own sessions run under origin's default locales (ADR-056,
//! class 2): the text a locale-rendered value takes in population and in
//! pgoutput is parsed back under the same `lc_monetary`.

use std::io::Error;

use tokio_postgres::SimpleQueryMessage;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss};

mod util;

fn rows(res: &[SimpleQueryMessage]) -> Vec<Vec<String>> {
    res.iter()
        .filter_map(|message| {
            let SimpleQueryMessage::Row(row) = message else {
                return None;
            };
            Some(
                (0..row.len())
                    .map(|i| row.get(i).unwrap_or_default().to_owned())
                    .collect(),
            )
        })
        .collect()
}

/// Origin's role default `lc_monetary` differs from the cache cluster's; a
/// `money` column still round-trips through population and CDC and renders
/// as origin renders it.
#[tokio::test]
async fn test_money_round_trips_under_origin_locale() -> Result<(), Error> {
    let mut ctx = TestContext::setup_before_start(|origin| async move {
        for sql in [
            "ALTER ROLE postgres SET lc_monetary = 'de_DE.UTF-8'",
            "CREATE TABLE prices (id int primary key, amount money)",
            "INSERT INTO prices VALUES (1, 1234.5::numeric::money)",
        ] {
            origin.batch_execute(sql).await.map_err(Error::other)?;
        }
        Ok(origin)
    })
    .await?;
    let sql = "select id, amount from prices order by id";
    let direct = ctx.origin_connect().await?;
    let expected = rows(&direct.simple_query(sql).await.map_err(Error::other)?);
    assert_eq!(expected.len(), 1);
    assert!(
        expected[0][1].contains(','),
        "de_DE renders a decimal comma: {expected:?}"
    );

    let m = ctx.metrics().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(rows(&res), expected);
    let m = assert_cache_miss(&mut ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(sql).await?;
    assert_eq!(rows(&res), expected, "populated under origin's locale");
    let m = assert_cache_hit(&mut ctx, m).await?;

    ctx.origin_query("INSERT INTO prices VALUES (2, 99.99::numeric::money)", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    let expected = rows(&direct.simple_query(sql).await.map_err(Error::other)?);
    assert_eq!(expected.len(), 2);
    let res = ctx.simple_query(sql).await?;
    assert_eq!(
        rows(&res),
        expected,
        "applied from pgoutput under origin's locale"
    );
    let _ = assert_cache_hit(&mut ctx, m).await?;
    Ok(())
}
