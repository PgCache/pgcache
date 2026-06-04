use std::io::Error;
use std::time::{Duration, Instant};

use postgres_types::ToSql;
use tokio_postgres::{Client, Config, NoTls, Row, SimpleQueryMessage, Statement, ToStatement};

use super::http::http_get;

use super::metrics::MetricsSnapshot;
use super::process::{
    PgCacheProcess, TempDBs, connect_pgcache, connect_pgcache_allowlist, connect_pgcache_clock,
    connect_pgcache_fault, connect_pgcache_pinned, connect_pgcache_pinned_small_cache,
    connect_pgcache_small_cache, start_databases,
};

/// Test context combining all resources needed for integration tests.
/// Fields are ordered for correct drop sequence: drop clients first
/// (closing connections and allowing spawned tasks to exit), then kill
/// pgcache, and finally tear down temp databases.
pub struct TestContext {
    pub cache: Client,     // connected through pgcache proxy
    pub origin: Client,    // direct connection to origin database
    pub cache_port: u16,   // port pgcache proxy is listening on
    pub metrics_port: u16, // port for HTTP metrics endpoint
    pub pgcache: PgCacheProcess,
    pub dbs: TempDBs,
}

impl TestContext {
    pub async fn setup() -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) = connect_pgcache(&dbs).await?;
        Ok(Self {
            dbs,
            pgcache,
            cache_port,
            metrics_port,
            cache,
            origin,
        })
    }

    /// Set up a test context with fault-injection environment variables set on
    /// the pgcache process (requires the binary built with `fault-injection`).
    pub async fn setup_fault(env: &[(&str, &str)]) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) = connect_pgcache_fault(&dbs, env).await?;
        Ok(Self {
            dbs,
            pgcache,
            cache_port,
            metrics_port,
            cache,
            origin,
        })
    }

    /// Set up a test context with a small cache to force eviction.
    pub async fn setup_small_cache(cache_size: usize) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_small_cache(&dbs, cache_size).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Set up a test context with clock eviction policy.
    pub async fn setup_clock(admission_threshold: u32) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_clock(&dbs, admission_threshold).await?;
        Ok(Self {
            dbs,
            pgcache,
            cache_port,
            metrics_port,
            cache,
            origin,
        })
    }

    /// Set up a test context with a table allowlist.
    pub async fn setup_allowlist(allowed_tables: &str) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_allowlist(&dbs, allowed_tables).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Set up a test context with pinned queries.
    /// The `before_start` closure runs against the origin database after tables
    /// are created but before pgcache spawns, so pinned queries can reference
    /// existing tables.
    pub async fn setup_pinned<F, Fut>(pinned_queries: &str, before_start: F) -> Result<Self, Error>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<Client, Error>>,
    {
        let (dbs, origin) = start_databases().await?;
        // Run setup closure to create tables/data before pgcache starts
        let origin = before_start(origin).await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_pinned(&dbs, pinned_queries).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Set up a test context with pinned queries and a small cache to force eviction.
    pub async fn setup_pinned_small_cache<F, Fut>(
        pinned_queries: &str,
        cache_size: usize,
        before_start: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<Client, Error>>,
    {
        let (dbs, origin) = start_databases().await?;
        let origin = before_start(origin).await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_pinned_small_cache(&dbs, pinned_queries, cache_size).await?;
        Ok(Self {
            cache,
            origin,
            cache_port,
            metrics_port,
            pgcache,
            dbs,
        })
    }

    /// Create an additional client connection through the pgcache proxy.
    pub async fn proxy_client_connect(&self) -> Result<Client, Error> {
        let (client, connection) = Config::new()
            .host("localhost")
            .port(self.cache_port)
            .user("postgres")
            .dbname("origin_test")
            .connect(NoTls)
            .await
            .map_err(Error::other)?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("proxy connection error: {e}");
            }
        });

        Ok(client)
    }

    /// Execute query through pgcache proxy
    pub async fn query<T>(
        &mut self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.cache
            .query(statement, params)
            .await
            .map_err(Error::other)
    }

    /// Execute query directly on origin (bypassing pgcache)
    pub async fn origin_query<T>(
        &mut self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.origin
            .query(statement, params)
            .await
            .map_err(Error::other)
    }

    /// Execute simple query through pgcache proxy
    pub async fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.cache.simple_query(query).await.map_err(Error::other)
    }

    /// Get metrics from pgcache HTTP endpoint
    pub async fn metrics(&mut self) -> Result<MetricsSnapshot, Error> {
        super::metrics::metrics_http_get(self.metrics_port).await
    }

    /// Prepare a statement through pgcache proxy
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.cache.prepare(query).await.map_err(Error::other)
    }

    /// Execute query_one through pgcache proxy
    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.cache
            .query_one(statement, params)
            .await
            .map_err(Error::other)
    }

    /// Wait for pgcache to apply all CDC events committed up to the current
    /// origin WAL position. Captures `pg_current_wal_lsn()` from origin and
    /// polls `/status` until `cdc.last_applied_lsn` reaches that LSN.
    ///
    /// Use this after a write to origin (directly or via the cache proxy)
    /// before reading data that should reflect the write.
    ///
    /// Times out after 5 seconds. The error includes both LSNs to make
    /// stalls diagnosable.
    pub async fn cdc_settle(&self) -> Result<(), Error> {
        self.cdc_settle_with_timeout(Duration::from_secs(5)).await
    }

    /// Same as `cdc_settle` with an explicit timeout. Useful for tests that
    /// stress slow paths or need a tighter bound.
    pub async fn cdc_settle_with_timeout(&self, timeout: Duration) -> Result<(), Error> {
        // Use `pg_current_wal_insert_lsn()` (not `pg_current_wal_lsn()`):
        // the latter returns the WAL *write* position, which can lag behind
        // the just-committed record under synchronous_commit=off (the
        // walwriter flushes asynchronously). The insert position always
        // includes the committed record. CDC will eventually catch up to
        // it via flush + decode, advancing `last_applied_lsn`.
        let captured_lsn_str: String = self
            .origin
            .query_one("SELECT pg_current_wal_insert_lsn()::text", &[])
            .await
            .map_err(Error::other)?
            .get(0);
        let captured_lsn = lsn_parse(&captured_lsn_str)?;

        let deadline = Instant::now() + timeout;
        loop {
            let (status, body) = http_get(self.metrics_port, "/status").await?;
            if status != 200 {
                return Err(Error::other(format!("/status returned {status}: {body}")));
            }
            let json: serde_json::Value = serde_json::from_str(&body)
                .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;
            let applied = json
                .get("cdc")
                .and_then(|c| c.get("last_applied_lsn"))
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| Error::other("cdc.last_applied_lsn missing or not a u64"))?;
            if applied >= captured_lsn {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::other(format!(
                    "cdc_settle timed out: applied={applied} captured={captured_lsn} ({captured_lsn_str})"
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Wait for all currently-registered queries to reach a terminal state
    /// (i.e. no entry remains in `Loading` or `Pending(_)`). Use after a
    /// cache miss when the next read should be served from cache.
    ///
    /// A small grace period precedes the first poll because there's a brief
    /// window after a SELECT returns to the client during which the
    /// proxy → coordinator → writer registration message hasn't yet been
    /// processed. The grace lets registration land so we observe the
    /// `Loading` state we then wait to leave.
    ///
    /// Times out after 5 seconds. The error lists the offending entries.
    pub async fn cache_settle(&self) -> Result<(), Error> {
        self.cache_settle_with_timeout(Duration::from_secs(5)).await
    }

    /// Same as `cache_settle` with an explicit timeout.
    pub async fn cache_settle_with_timeout(&self, timeout: Duration) -> Result<(), Error> {
        cache_settle_at(self.metrics_port, timeout).await
    }
}

/// Free-function variant of `TestContext::cache_settle_with_timeout` for
/// tests that don't use `TestContext` (e.g. those that drive the proxy
/// through a custom client like `connect_pgcache_tls`).
pub async fn cache_settle_at(metrics_port: u16, timeout: Duration) -> Result<(), Error> {
    // Grace window for the registration message to reach the writer.
    // Typical hop latency is sub-millisecond; 20 ms is comfortably above.
    // If registration takes longer, the subsequent poll will observe
    // the resulting `Loading` state and continue to wait.
    tokio::time::sleep(Duration::from_millis(20)).await;

    let deadline = Instant::now() + timeout;
    loop {
        let (status, body) = http_get(metrics_port, "/status").await?;
        if status != 200 {
            return Err(Error::other(format!("/status returned {status}: {body}")));
        }
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;
        let queries = json
            .get("queries")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| Error::other("queries missing or not an array"))?;
        let in_flight: Vec<String> = queries
            .iter()
            .filter_map(|q| {
                let state = q.get("state").and_then(serde_json::Value::as_str)?;
                if state == "Loading" || state.starts_with("Pending") {
                    let fp = q
                        .get("fingerprint")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    Some(format!("{fp}={state}"))
                } else {
                    None
                }
            })
            .collect();
        if in_flight.is_empty() {
            return Ok(());
        }
        if Instant::now() >= deadline {
            return Err(Error::other(format!(
                "cache_settle timed out: {in_flight:?}"
            )));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Parse a PostgreSQL LSN in `"X/Y"` hex form (as returned by
/// `pg_current_wal_lsn()::text`) into a `u64` matching the wire-protocol
/// encoding used in `cdc.last_applied_lsn`.
pub fn lsn_parse(s: &str) -> Result<u64, Error> {
    let (hi, lo) = s
        .split_once('/')
        .ok_or_else(|| Error::other(format!("invalid LSN format: {s}")))?;
    let hi = u64::from_str_radix(hi, 16)
        .map_err(|e| Error::other(format!("invalid LSN high: {s}: {e}")))?;
    let lo = u64::from_str_radix(lo, 16)
        .map_err(|e| Error::other(format!("invalid LSN low: {s}: {e}")))?;
    Ok((hi << 32) | lo)
}
