//! Regression test for a publication-registration race: if a relation is
//! already a member of pgcache's publication by the time pgcache tries to
//! add it itself (e.g. a prior attempt that failed partway, or any other
//! out-of-band `ALTER PUBLICATION`), registering a cacheable query against
//! that relation must still succeed instead of repeating the identical
//! failing `ALTER PUBLICATION ... ADD TABLE` forever.

use std::io::Error;

use crate::util::{TestContext, query_warm};

mod util;

#[tokio::test]
async fn test_registration_recovers_when_relation_already_published() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;

    ctx.query(
        "create table race_t (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into race_t (id, val) values (1, 'one')", &[])
        .await?;
    ctx.cdc_decode_settle().await?;

    // Simulate the relation already being a publication member through some
    // path other than pgcache's own registration bookkeeping. "pub_test" is
    // this test harness's configured publication name
    // (`tests/data/default_config.toml`).
    ctx.origin
        .execute("alter publication pub_test add table race_t", &[])
        .await
        .map_err(Error::other)?;

    // pgcache's own `publication_oids` bookkeeping has no record of this
    // relation yet, so registering a cacheable query against it repeats the
    // identical `ALTER PUBLICATION ... ADD TABLE` Postgres already rejects as
    // "already a member" — and must recover and finish registering rather
    // than retrying that failure indefinitely.
    query_warm(&mut ctx, "select id, val from race_t where id = 1").await?;
    Ok(())
}
