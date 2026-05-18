//! Loader for the upstream PostgreSQL regression fixtures.
//!
//! Phase 1 ports `aggregates.sql`/`select.sql`/etc., which reference the
//! canonical `onek` pseudo-random table. Its data file is vendored under
//! `conformance/data/` (see `data/ATTRIBUTION.md`) and embedded at
//! compile time so the binary is self-contained.

use anyhow::{Context, Result};
use bytes::Bytes;
use futures_util::SinkExt;
use futures_util::pin_mut;
use tokio_postgres::Client;

/// `onek` schema from `src/test/regress/sql/test_setup.sql`, plus a
/// synthetic `PRIMARY KEY (unique1)` not present upstream. pgcache only
/// caches tables with a primary key (PGC-135: `table.rs` metadata SQL
/// inner-joins `pg_constraint` on `contype='p'`); `unique1` is a unique,
/// non-null 0..999 permutation so it is a valid key over the vendored
/// data.
const ONEK_DDL: &str = "CREATE TABLE onek (
    unique1     int4 PRIMARY KEY,
    unique2     int4,
    two         int4,
    four        int4,
    ten         int4,
    twenty      int4,
    hundred     int4,
    thousand    int4,
    twothousand int4,
    fivethous   int4,
    tenthous    int4,
    odd         int4,
    even        int4,
    stringu1    name,
    stringu2    name,
    string4     name
)";

/// 1000 rows, tab-delimited, in PostgreSQL COPY text format.
const ONEK_DATA: &str = include_str!("../data/onek.data");

/// `J1_TBL` / `J2_TBL` from `src/test/regress/sql/join.sql`. Upstream
/// these have no key and contain NULLs and a duplicate `(5, -5)` row, so
/// no natural primary key exists. A surrogate `*_pk` column is appended
/// last (pgcache only caches tables with a PK — PGC-135). The name is
/// distinct per table so `NATURAL JOIN`, which couples on common column
/// names, still couples only on `i` exactly as upstream. `SELECT *`
/// includes the surrogate, but the harness compares pgcache against
/// origin (the oracle), not against upstream golden output, so the extra
/// column is consistent on both sides.
const J1_TBL_DDL: &str = "CREATE TABLE j1_tbl (
    i     integer,
    j     integer,
    t     text,
    j1_pk integer PRIMARY KEY
)";

const J1_TBL_DATA: &str = "INSERT INTO j1_tbl (i, j, t, j1_pk) VALUES
    (1, 4, 'one', 1),
    (2, 3, 'two', 2),
    (3, 2, 'three', 3),
    (4, 1, 'four', 4),
    (5, 0, 'five', 5),
    (6, 6, 'six', 6),
    (7, 7, 'seven', 7),
    (8, 8, 'eight', 8),
    (0, NULL, 'zero', 9),
    (NULL, NULL, 'null', 10),
    (NULL, 0, 'zero', 11)";

const J2_TBL_DDL: &str = "CREATE TABLE j2_tbl (
    i     integer,
    k     integer,
    j2_pk integer PRIMARY KEY
)";

const J2_TBL_DATA: &str = "INSERT INTO j2_tbl (i, k, j2_pk) VALUES
    (1, -1, 1),
    (2, 2, 2),
    (3, -3, 3),
    (2, 4, 4),
    (5, -5, 5),
    (5, -5, 6),
    (0, NULL, 7),
    (NULL, NULL, 8),
    (NULL, 0, 9)";

/// `foo` from `src/test/regress/sql/select.sql` (the ORDER BY / NULLS
/// section). Upstream `foo (f1 int)` with rows `(42),(3),(10),(7),
/// (null),(null),(1)`. A surrogate `foo_pk` is appended (pgcache caches
/// only tables with a PK — PGC-135) so the NULL-ordering queries are
/// actually served from cache, not just forwarded; suites add it as a
/// deterministic ORDER BY tiebreaker since the two NULL rows' relative
/// order is otherwise unspecified.
const FOO_DDL: &str = "CREATE TABLE foo (
    f1     integer,
    foo_pk integer PRIMARY KEY
)";

const FOO_DATA: &str = "INSERT INTO foo (f1, foo_pk) VALUES
    (42, 1),
    (3, 2),
    (10, 3),
    (7, 4),
    (NULL, 5),
    (NULL, 6),
    (1, 7)";

/// Quote a PostgreSQL identifier for safe interpolation.
fn ident_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
}

/// Add `table` to pgcache's CDC publication so its rows replicate to the
/// cache. Idempotent. A no-op when no publication is configured (an
/// external pgcache that hasn't provisioned one yet); the table simply
/// won't be cacheable, which the suite annotations tolerate.
async fn publication_table_ensure(
    client: &Client,
    publication: Option<&str>,
    table: &str,
) -> Result<()> {
    let Some(pubname) = publication else {
        tracing::warn!(
            table,
            "no --publication given; table will not be added to pgcache's CDC \
             publication and may not replicate"
        );
        return Ok(());
    };
    let already: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = 'public' AND tablename = $2)",
            &[&pubname, &table],
        )
        .await
        .context("checking publication membership")?
        .get(0);
    if already {
        tracing::info!(publication = pubname, table, "already in publication");
        return Ok(());
    }
    client
        .batch_execute(&format!(
            "ALTER PUBLICATION {} ADD TABLE {}",
            ident_quote(pubname),
            ident_quote(table)
        ))
        .await
        .with_context(|| format!("adding {table} to publication {pubname} (does it exist?)"))?;
    tracing::info!(publication = pubname, table, "added to publication");
    Ok(())
}

/// Create and populate `j1_tbl` + `j2_tbl` on origin (idempotent), and
/// add them to pgcache's publication so the join suite can assert
/// `cached` routing. Mirrors [`onek_load`]; data is small enough to
/// inline rather than vendor a file.
pub async fn join_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    for (table, ddl, data) in [
        ("j1_tbl", J1_TBL_DDL, J1_TBL_DATA),
        ("j2_tbl", J2_TBL_DDL, J2_TBL_DATA),
    ] {
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {table}"))
            .await
            .with_context(|| format!("dropping any existing {table}"))?;
        client
            .batch_execute(ddl)
            .await
            .with_context(|| format!("creating {table}"))?;
        publication_table_ensure(client, publication, table).await?;
        client
            .batch_execute(data)
            .await
            .with_context(|| format!("loading {table} data"))?;
        tracing::info!(table, "loaded join fixture");
    }
    Ok(())
}

/// Create and populate `foo` on origin (idempotent) and add it to
/// pgcache's publication, so the select suite's NULL-ordering queries
/// are cached. Mirrors [`join_tables_load`].
pub async fn select_tables_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS foo")
        .await
        .context("dropping any existing foo")?;
    client
        .batch_execute(FOO_DDL)
        .await
        .context("creating foo")?;
    publication_table_ensure(client, publication, "foo").await?;
    client
        .batch_execute(FOO_DATA)
        .await
        .context("loading foo data")?;
    tracing::info!(table = "foo", "loaded select fixture");
    Ok(())
}

/// Create and populate `onek` on origin (idempotent).
///
/// pgcache adds the table to its own publication and registers it when a
/// query first references it (requires the synthetic PK — see
/// `ONEK_DDL`). The manual `ALTER PUBLICATION` is a best-effort nudge for
/// external pgcache instances that may not have provisioned it yet.
pub async fn onek_load(client: &Client, publication: Option<&str>) -> Result<()> {
    client
        .batch_execute("DROP TABLE IF EXISTS onek")
        .await
        .context("dropping any existing onek")?;
    client
        .batch_execute(ONEK_DDL)
        .await
        .context("creating onek")?;

    publication_table_ensure(client, publication, "onek").await?;

    let sink = client
        .copy_in::<_, Bytes>("COPY onek FROM STDIN")
        .await
        .context("starting COPY onek")?;
    pin_mut!(sink);
    sink.send(Bytes::from_static(ONEK_DATA.as_bytes()))
        .await
        .context("streaming onek data")?;
    let rows = sink.finish().await.context("finishing COPY onek")?;

    tracing::info!(rows, "loaded onek fixture");
    Ok(())
}
