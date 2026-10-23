//! Tables with row-level security are never cached (PGC-112): pgcache
//! populates as its own role and cannot apply the policies, so a query over
//! such a table forwards, and enabling RLS on a cached table evicts it.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss, assert_row_at, metrics_delta};

mod util;

const READ: &str = "SELECT id, tenant FROM tenant_rows WHERE id = 1";

async fn tenant_rows_create(ctx: &mut TestContext) -> Result<(), Error> {
    for sql in [
        "CREATE TABLE tenant_rows (id int primary key, tenant text)",
        "INSERT INTO tenant_rows VALUES (1, 'acme')",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cache_settle().await
}

/// One read of [`READ`] that must be forwarded for row-level security:
/// counted in `rls_skipped`, never a hit, and still correct.
async fn assert_rls_forward(ctx: &mut TestContext) -> Result<(), Error> {
    let before = ctx.metrics().await?;
    let res = ctx.simple_query(READ).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("tenant", "acme")])?;
    let after = ctx.metrics_after_outcomes(&before, 1).await?;
    let delta = metrics_delta(&before, &after);
    assert_eq!(
        delta.queries_rls_skipped, 1,
        "forwarded for row-level security"
    );
    assert_eq!(delta.queries_cache_hit, 0, "never served from cache");
    Ok(())
}

/// Register [`READ`] into a steady cache hit.
async fn assert_registers(ctx: &mut TestContext) -> Result<(), Error> {
    let m = ctx.metrics().await?;
    let res = ctx.simple_query(READ).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("tenant", "acme")])?;
    let m = assert_cache_miss(ctx, m).await?;
    ctx.cache_settle().await?;
    let res = ctx.simple_query(READ).await?;
    assert_row_at(&res, 1, &[("id", "1"), ("tenant", "acme")])?;
    assert_cache_hit(ctx, m).await?;
    Ok(())
}

/// A table with row-level security enabled before its first query is never
/// registered: every read forwards and is counted.
#[tokio::test]
async fn test_rls_table_is_forwarded_not_cached() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_rows_create(&mut ctx).await?;
    ctx.origin_query("ALTER TABLE tenant_rows ENABLE ROW LEVEL SECURITY", &[])
        .await?;

    assert_rls_forward(&mut ctx).await?;
    assert_rls_forward(&mut ctx).await?;
    Ok(())
}

/// Enabling row-level security on a cached table evicts its queries (the
/// `ALTER TABLE` signal), and disabling it lets them register again.
#[tokio::test]
async fn test_enabling_rls_evicts_and_disabling_recaches() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    tenant_rows_create(&mut ctx).await?;
    assert_registers(&mut ctx).await?;

    ctx.origin_query("ALTER TABLE tenant_rows ENABLE ROW LEVEL SECURITY", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert_rls_forward(&mut ctx).await?;

    ctx.origin_query("ALTER TABLE tenant_rows DISABLE ROW LEVEL SECURITY", &[])
        .await?;
    ctx.cdc_apply_settle().await?;
    assert_registers(&mut ctx).await?;
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
    tenant_rows_create(&mut ctx).await?;
    assert_registers(&mut ctx).await?;

    for sql in [
        "ALTER TABLE tenant_rows ENABLE ROW LEVEL SECURITY",
        "UPDATE tenant_rows SET tenant = 'acme' WHERE id = 1",
    ] {
        ctx.origin_query(sql, &[]).await?;
    }
    ctx.cdc_apply_settle().await?;
    assert_rls_forward(&mut ctx).await?;
    Ok(())
}
