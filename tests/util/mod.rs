#![allow(dead_code)]
#![allow(unused_imports)]

mod assertions;
mod context;
mod http;
mod metrics;
mod pgproto;
mod process;

use std::io::Error;

use postgres_types::ToSql;
use tokio_postgres::{Client, Row, SimpleQueryMessage, ToStatement};

// --- Re-exports: keep the `crate::util::Foo` import paths stable ---

pub use assertions::{assert_row_at, assert_row_fields, extract_row};
pub use context::{TestContext, cache_settle_at, lsn_parse};
pub use http::{http_get, http_post, http_put};
pub use metrics::{
    MetricsSnapshot, assert_cache_hit, assert_cache_miss, assert_not_subsumed, assert_subsume_hit,
    metrics_delta, metrics_http_get,
};
pub use pgproto::pgproto_run;
pub use process::{
    PgCacheProcess, TempDBs, connect_cache_db, connect_pgcache, connect_pgcache_allowlist,
    connect_pgcache_clock, connect_pgcache_pinned, connect_pgcache_pinned_small_cache,
    connect_pgcache_small_cache, connect_pgcache_tls, pgcache_client_connect, proxy_wait_for_ready,
    start_databases,
};

// --- Standalone helpers that don't belong to a specific submodule ---

pub async fn query<T>(
    _pgcache: &mut PgCacheProcess,
    client: &Client,
    statement: &T,
    params: &[&(dyn ToSql + Sync)],
) -> Result<Vec<Row>, Error>
where
    T: ?Sized + ToStatement,
{
    client.query(statement, params).await.map_err(Error::other)
}

pub async fn simple_query(
    _pgcache: &mut PgCacheProcess,
    client: &Client,
    query: &str,
) -> Result<Vec<SimpleQueryMessage>, Error> {
    client.simple_query(query).await.map_err(Error::other)
}
