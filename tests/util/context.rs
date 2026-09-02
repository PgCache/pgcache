use std::io::Error;
use std::time::{Duration, Instant};

use postgres_types::ToSql;
use tokio_postgres::{Client, Config, NoTls, Row, SimpleQueryMessage, Statement, ToStatement};

use super::http::http_get;

use super::metrics::MetricsSnapshot;
use super::process::{
    PgCacheProcess, TempDBs, connect_pgcache, connect_pgcache_allowlist, connect_pgcache_args,
    connect_pgcache_clock, connect_pgcache_fault, connect_pgcache_pinned,
    connect_pgcache_pinned_small_cache, connect_pgcache_small_cache, start_databases,
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

/// The CDC watermarks read from `/status`.
struct CdcStatusSnapshot {
    /// `last_received_lsn`: the receive/liveness watermark (keepalive-advanced).
    received: u64,
    /// `last_applied_lsn`: the commit-only apply watermark.
    applied: u64,
    /// Whether the CDC apply pipeline has no work in flight.
    apply_idle: bool,
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

    /// Set up a test context with extra pgcache CLI args (e.g.
    /// `--mv_compute_min_rows 0` to force MV materialization of `Gated` shapes
    /// regardless of size).
    pub async fn setup_with_args(extra_args: &[&str]) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_args(&dbs, extra_args).await?;
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

    /// Set up a test context that force-evicts down to `max_cached_queries` via
    /// the fault-injection count cap (requires `--features fault-injection`).
    pub async fn setup_small_cache(max_cached_queries: usize) -> Result<Self, Error> {
        let (dbs, origin) = start_databases().await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_small_cache(&dbs, max_cached_queries).await?;
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

    /// Set up a test context with pinned queries that force-evicts down to
    /// `max_cached_queries` via the fault-injection count cap (requires
    /// `--features fault-injection`).
    pub async fn setup_pinned_small_cache<F, Fut>(
        pinned_queries: &str,
        max_cached_queries: usize,
        before_start: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Client) -> Fut,
        Fut: std::future::Future<Output = Result<Client, Error>>,
    {
        let (dbs, origin) = start_databases().await?;
        let origin = before_start(origin).await?;
        let (pgcache, cache_port, metrics_port, cache) =
            connect_pgcache_pinned_small_cache(&dbs, pinned_queries, max_cached_queries).await?;
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

    /// Wait until CDC has *consumed the replication stream* up to the origin's
    /// current WAL position — the decode stage. Polls until `last_received_lsn`
    /// (the decode/liveness watermark, advanced by keepalives as well as
    /// commits) reaches the captured position.
    ///
    /// Use this as a drain barrier where there may be nothing to apply — e.g.
    /// after setup writes to tables no cached query references yet — since the
    /// decode watermark advances via keepalives regardless of application.
    /// When a test then asserts that a committed change is *reflected in the
    /// cache*, use [`cdc_apply_settle`](Self::cdc_apply_settle) instead: this
    /// one can return while a delivered commit's effects are still pending.
    ///
    /// Times out after 5 seconds.
    pub async fn cdc_decode_settle(&self) -> Result<(), Error> {
        self.cdc_decode_settle_with_timeout(Duration::from_secs(5))
            .await
    }

    /// Same as [`cdc_decode_settle`](Self::cdc_decode_settle) with an explicit
    /// timeout.
    pub async fn cdc_decode_settle_with_timeout(&self, timeout: Duration) -> Result<(), Error> {
        let captured_lsn_str = self.flush_lsn_capture().await?;
        let captured_lsn = lsn_parse(&captured_lsn_str)?;
        let deadline = Instant::now() + timeout;
        loop {
            let cdc = self.cdc_status().await?;
            if cdc.received >= captured_lsn {
                return Ok(());
            }
            if Instant::now() >= deadline {
                return Err(Error::other(format!(
                    "cdc decode settle timed out: received={} captured={captured_lsn} ({captured_lsn_str})",
                    cdc.received
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// Wait until pgcache has *applied to the cache* every committed change up
    /// to the origin's current WAL position — the apply stage.
    ///
    /// Use this after a write to a table a cached query references, before
    /// reading data that must reflect the write. Unlike
    /// [`cdc_decode_settle`](Self::cdc_decode_settle) it never returns early on
    /// the keepalive cursor, so an invalidation or in-place update is visible
    /// once it returns.
    ///
    /// Times out after 5 seconds.
    pub async fn cdc_apply_settle(&self) -> Result<(), Error> {
        self.cdc_apply_settle_with_timeout(Duration::from_secs(5))
            .await
    }

    /// Same as [`cdc_apply_settle`](Self::cdc_apply_settle) with an explicit
    /// timeout.
    ///
    /// Settles when the commit-only watermark reaches the flush target. The
    /// target can occasionally land past the last real commit — on a *flushed*
    /// background record (e.g. a running-xacts snapshot) that produces no cache
    /// mutation, so the commit watermark can never reach it. The fallback path
    /// handles that: if the decode cursor has passed the target and the writer
    /// reports no in-flight apply work (`apply_idle`) continuously for a fixed
    /// window, the residual gap is non-applyable WAL and we settle. Under
    /// `synchronous_commit=on` (the harness default) a real committed write is
    /// flushed and delivered promptly, so `apply_idle` cannot stay set across
    /// the window while a genuine change is still pending — the fallback only
    /// fires for a true background-record gap.
    pub async fn cdc_apply_settle_with_timeout(&self, timeout: Duration) -> Result<(), Error> {
        // Fallback window: the target can land past the last real commit on a
        // *flushed* background record (e.g. a running-xacts snapshot emitted by
        // concurrent activity) that produces no cache mutation, so the commit
        // watermark can never reach it. When the decode cursor has passed the
        // target and the writer reports no in-flight apply work (`apply_idle`)
        // continuously for this long, the residual gap is non-applyable WAL and
        // we settle. Must exceed the worst-case decode->deliver->apply latency
        // so a not-yet-delivered frame (transient `apply_idle`) never settles us
        // early, and held frames (an `apply_idle == false` batch) never do.
        const APPLY_IDLE_STABLE: Duration = Duration::from_millis(500);
        let captured_lsn_str = self.flush_lsn_capture().await?;
        let captured_lsn = lsn_parse(&captured_lsn_str)?;
        let deadline = Instant::now() + timeout;
        let mut apply_idle_since: Option<Instant> = None;
        loop {
            let cdc = self.cdc_status().await?;
            if cdc.applied >= captured_lsn {
                return Ok(());
            }
            if cdc.received >= captured_lsn && cdc.apply_idle {
                let since = *apply_idle_since.get_or_insert_with(Instant::now);
                if since.elapsed() >= APPLY_IDLE_STABLE {
                    return Ok(());
                }
            } else {
                apply_idle_since = None;
            }
            if Instant::now() >= deadline {
                return Err(Error::other(format!(
                    "cdc apply settle timed out: applied={} received={} apply_idle={} captured={captured_lsn} ({captured_lsn_str})",
                    cdc.applied, cdc.received, cdc.apply_idle
                )));
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }

    /// The origin's current flush position, as the `0/HEX` text form.
    ///
    /// The target is `pg_current_wal_flush_lsn()`, not the *insert* position:
    /// the insert position can include a background record (e.g. a
    /// running-xacts snapshot) left unflushed in the WAL buffer, which the
    /// logical walsender never decodes or sends. The flush position is the
    /// ceiling CDC can reach, and under `synchronous_commit=on` (the harness
    /// default) it already includes every committed write.
    async fn flush_lsn_capture(&self) -> Result<String, Error> {
        Ok(self
            .origin
            .query_one("SELECT pg_current_wal_flush_lsn()::text", &[])
            .await
            .map_err(Error::other)?
            .get(0))
    }

    /// Read the CDC watermarks from `/status`.
    async fn cdc_status(&self) -> Result<CdcStatusSnapshot, Error> {
        let (status, body) = http_get(self.metrics_port, "/status").await?;
        if status != 200 {
            return Err(Error::other(format!("/status returned {status}: {body}")));
        }
        let json: serde_json::Value = serde_json::from_str(&body)
            .map_err(|e| Error::other(format!("invalid JSON: {e}\nbody: {body}")))?;
        let cdc = json
            .get("cdc")
            .ok_or_else(|| Error::other("status body missing cdc block"))?;
        let lsn = |field: &str| {
            cdc.get(field)
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| Error::other(format!("cdc.{field} missing or not a u64")))
        };
        Ok(CdcStatusSnapshot {
            received: lsn("last_received_lsn")?,
            applied: lsn("last_applied_lsn")?,
            apply_idle: cdc
                .get("apply_idle")
                .and_then(serde_json::Value::as_bool)
                .unwrap_or(false),
        })
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
                // MV builds run off the writer thread, so a /status response
                // no longer implies a dispatched build has finished — treat
                // scheduled/in-flight builds as unsettled work too.
                let mv_state = q
                    .get("mv_state")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let mv_in_flight =
                    mv_state.starts_with("Scheduled") || mv_state.starts_with("Building");
                if state == "Loading" || state.starts_with("Pending") || mv_in_flight {
                    let fp = q
                        .get("fingerprint")
                        .and_then(serde_json::Value::as_u64)
                        .unwrap_or(0);
                    Some(format!("{fp}={state}/mv:{mv_state}"))
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
