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

/// Quote a PostgreSQL identifier for safe interpolation.
fn ident_quote(s: &str) -> String {
    format!("\"{}\"", s.replace('"', "\"\""))
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

    if let Some(pubname) = publication {
        let already: bool = client
            .query_one(
                "SELECT EXISTS (SELECT 1 FROM pg_publication_tables \
                 WHERE pubname = $1 AND schemaname = 'public' AND tablename = 'onek')",
                &[&pubname],
            )
            .await
            .context("checking publication membership")?
            .get(0);
        if already {
            tracing::info!(publication = pubname, "onek already in publication");
        } else {
            client
                .batch_execute(&format!(
                    "ALTER PUBLICATION {} ADD TABLE onek",
                    ident_quote(pubname)
                ))
                .await
                .with_context(|| {
                    format!("adding onek to publication {pubname} (does it exist?)")
                })?;
            tracing::info!(publication = pubname, "added onek to publication");
        }
    } else {
        tracing::warn!(
            "no --publication given; onek will not be added to pgcache's CDC \
             publication and may not replicate"
        );
    }

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
