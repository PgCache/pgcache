//! Binary serve-path coverage for named prepared statements (PGC-229).
//!
//! `pg_prepared_statements` is session-local, so a test client can't observe
//! the worker pool's prepared statements directly — these verify the behavior
//! instead: correct results across the statement-reuse path, the parameterized
//! `LIMIT $1 OFFSET $2` (including the NULL = no-limit case), and that the
//! eviction-driven close sweep doesn't corrupt pooled connections.
//!
//! `ctx.query` goes through tokio-postgres' extended protocol with binary
//! result format, which is the serve path these tests target.

use std::io::Error;

use crate::util::{TestContext, assert_cache_hit, assert_cache_miss};

mod util;

/// Creates an `items` table with 10 rows; category 'z' holds ids 7..=10.
async fn setup_items_table(ctx: &mut TestContext) -> Result<(), Error> {
    ctx.query(
        "CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT, category TEXT)",
        &[],
    )
    .await?;
    ctx.query(
        "INSERT INTO items (id, name, category) VALUES \
         (1, 'a', 'x'), (2, 'b', 'x'), (3, 'c', 'x'), (4, 'd', 'y'), (5, 'e', 'y'), \
         (6, 'f', 'y'), (7, 'g', 'z'), (8, 'h', 'z'), (9, 'i', 'z'), (10, 'j', 'z')",
        &[],
    )
    .await?;
    Ok(())
}

/// A cached binary query served repeatedly must stay correct across the
/// statement-reuse path: the first hit on a pool connection prepares the named
/// statement, later hits skip Parse and only Bind/Execute (no ParseComplete in
/// the response). Repeating well past the pool size exercises reuse on every
/// connection.
#[tokio::test]
async fn test_binary_hit_statement_reuse() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let query = "SELECT id, name FROM items WHERE category = 'z' ORDER BY id";

    let m = ctx.metrics().await?;
    let rows = ctx.query(query, &[]).await?;
    assert_eq!(rows.len(), 4);
    let mut m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    for _ in 0..10 {
        let rows = ctx.query(query, &[]).await?;
        let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("id")).collect();
        assert_eq!(ids, vec![7, 8, 9, 10]);
        assert_eq!(rows[0].get::<_, String>("name"), "g");
        m = assert_cache_hit(&mut ctx, m).await?;
    }

    Ok(())
}

/// The cached query is populated once with no limit (full result), then served
/// with assorted LIMIT/OFFSET combinations off that one prepared statement via
/// the `$1`/`$2` params — including a final no-limit hit that binds NULL for
/// both (PG reads LIMIT NULL as no limit, OFFSET NULL as 0).
#[tokio::test]
async fn test_binary_limit_offset_params() -> Result<(), Error> {
    let mut ctx = TestContext::setup().await?;
    setup_items_table(&mut ctx).await?;

    let base = "SELECT id, name FROM items WHERE category = 'z' ORDER BY id";

    // Miss with no limit → full result (ids 7,8,9,10) populated.
    let m = ctx.metrics().await?;
    let rows = ctx.query(base, &[]).await?;
    assert_eq!(rows.len(), 4);
    let m = assert_cache_miss(&mut ctx, m).await?;

    ctx.cache_settle().await?;

    let ids = |rows: &[tokio_postgres::Row]| -> Vec<i32> {
        rows.iter().map(|r| r.get::<_, i32>("id")).collect()
    };

    // LIMIT 2 → 7,8
    let rows = ctx.query(&format!("{base} LIMIT 2"), &[]).await?;
    assert_eq!(ids(&rows), vec![7, 8]);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // LIMIT 2 OFFSET 1 → 8,9
    let rows = ctx.query(&format!("{base} LIMIT 2 OFFSET 1"), &[]).await?;
    assert_eq!(ids(&rows), vec![8, 9]);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // OFFSET 2, no limit → 9,10
    let rows = ctx.query(&format!("{base} OFFSET 2"), &[]).await?;
    assert_eq!(ids(&rows), vec![9, 10]);
    let m = assert_cache_hit(&mut ctx, m).await?;

    // No limit again → all rows (both params bind NULL).
    let rows = ctx.query(base, &[]).await?;
    assert_eq!(ids(&rows), vec![7, 8, 9, 10]);
    let _m = assert_cache_hit(&mut ctx, m).await?;

    Ok(())
}

/// Binary serving stays correct while reconciliation closes evicted statements.
/// Prepared statements are per-connection and decoupled from eviction: each
/// serve runs one round-robin reconciliation step that closes a statement whose
/// query was evicted (pipelined ahead of the serve). After ps_a is prepared then
/// evicted, continuing to serve a live query drives that close path on the pool
/// connections — if the CloseComplete handling desynced a connection, these
/// serves would return wrong data, error, or hang.
#[tokio::test]
async fn test_binary_reconciliation_closes_evicted_statement() -> Result<(), Error> {
    // 200KB cache — fits ~2 populated tables, forcing FIFO eviction of the
    // oldest (ps_a) when later tables are registered.
    let mut ctx = TestContext::setup_small_cache(200 * 1024).await?;

    for table in &["ps_a", "ps_b", "ps_c"] {
        ctx.query(
            &format!("CREATE TABLE {table} (id INTEGER PRIMARY KEY, data TEXT)"),
            &[],
        )
        .await?;
        for i in 0..500 {
            ctx.query(
                &format!("INSERT INTO {table} (id, data) VALUES ($1, $2)"),
                &[&i, &format!("payload-{table}-{i:060}")],
            )
            .await?;
        }
    }

    // Populate + hit ps_a on the binary path so its named statement is prepared.
    let q_a = "SELECT id, data FROM ps_a WHERE id < 5 ORDER BY id";
    ctx.query(q_a, &[]).await?;
    ctx.cache_settle().await?;
    assert_eq!(ctx.query(q_a, &[]).await?.len(), 5);

    // Register big queries on ps_b/ps_c to push the cache over the limit and
    // evict ps_a — its prepared statements are now dead weight on the pool.
    ctx.query("SELECT id, data FROM ps_b", &[]).await?;
    ctx.query("SELECT id, data FROM ps_c", &[]).await?;
    ctx.cache_settle().await?;

    // Keep serving a live query: each serve reconciles one statement, so the
    // connection(s) holding ps_a's evicted statement close it (CloseComplete
    // pipelined ahead of the serve). Results must stay correct throughout.
    let q_c = "SELECT id, data FROM ps_c WHERE id < 3 ORDER BY id";
    for _ in 0..20 {
        let rows = ctx.query(q_c, &[]).await?;
        let ids: Vec<i32> = rows.iter().map(|r| r.get::<_, i32>("id")).collect();
        assert_eq!(ids, vec![0, 1, 2]);
    }

    Ok(())
}
