//! Ephemeral mode: provision origin + cache Postgres via `pgtemp` and
//! spawn a pgcache built from this workspace, so a conformance run needs
//! no pre-existing infrastructure.
//!
//! Mirrors the integration-test recipe in `tests/util/process.rs`:
//! origin gets `wal_level=logical`; cache preloads `pgcache_pgrx` and
//! has the extension created; pgcache itself provisions the publication
//! and replication slot. pgcache's stdout+stderr are captured to a file
//! so the PGC-102 swallowed-error check runs by default.

use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant, SystemTime};

use anyhow::{Context, Result, bail};
use pgtemp::{PgTempDB, PgTempDBBuilder};
use tokio::process::{Child, Command};
use tokio_postgres::NoTls;

/// Options for launching an ephemeral pgcache.
pub struct SpawnOptions {
    pub pgcache_bin: Option<PathBuf>,
    pub workers: usize,
    pub log_level: String,
    pub ready_timeout: Duration,
    /// Extra environment for the spawned pgcache (e.g. `PGCACHE_FAULT_*`
    /// hooks — those require a binary built with `--features
    /// fault-injection`; check `/status` `fault_injection` to fail loudly
    /// instead of silently exercising nothing).
    pub env: Vec<(String, String)>,
}

/// A running ephemeral stack: pgcache child + the two temp Postgres
/// instances it depends on, plus the endpoints the harness needs.
///
/// Field order matters: `child` is declared first so the pgcache process
/// is killed before the temp databases it holds connections to are torn
/// down.
pub struct SpawnedPgcache {
    child: Child,
    _origin_db: PgTempDB,
    _cache_db: PgTempDB,
    pub origin_url: String,
    pub cache_url: String,
    /// The raw cache PostgreSQL (not the proxy) — for failure diagnostics
    /// (`pg_stat_activity` of the backends pgcache itself holds).
    pub cache_db_url: String,
    pub status_url: String,
    pub metrics_url: String,
    pub logs_file: PathBuf,
    pub publication: String,
}

/// `postgres://<user>@127.0.0.1:<port>/<db>` — no TLS, trust auth, as
/// used by both pgtemp instances and the transparent proxy.
fn conn_url(user: &str, port: u16, db: &str) -> String {
    format!("postgres://{user}@127.0.0.1:{port}/{db}")
}

/// An OS-assigned free TCP port (bound then released — small race window
/// acceptable for a test harness).
fn port_free() -> Result<u16> {
    let l = TcpListener::bind("127.0.0.1:0").context("binding ephemeral port")?;
    let port = l.local_addr().context("reading ephemeral port")?.port();
    Ok(port)
}

/// The pgcache binary alongside the conformance binary (same target
/// profile dir). Built from the workspace if absent.
fn pgcache_bin_resolve(override_path: Option<PathBuf>) -> Result<PathBuf> {
    if let Some(p) = override_path {
        if !p.exists() {
            bail!("--pgcache-bin {} does not exist", p.display());
        }
        return Ok(p);
    }
    let exe = std::env::current_exe().context("locating current executable")?;
    let dir = exe
        .parent()
        .context("current executable has no parent dir")?;
    let candidate = dir.join("pgcache");
    if candidate.exists() {
        return Ok(candidate);
    }

    let workspace_root = Path::new(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .context("conformance crate has no parent (workspace root)")?;
    tracing::info!("pgcache binary not found; building (cargo build --bin pgcache)");
    let status = std::process::Command::new(env!("CARGO"))
        .args(["build", "--bin", "pgcache"])
        .current_dir(workspace_root)
        .status()
        .context("running cargo build --bin pgcache")?;
    if !status.success() {
        bail!("cargo build --bin pgcache failed");
    }
    if !candidate.exists() {
        bail!(
            "built pgcache but binary not found at {}",
            candidate.display()
        );
    }
    Ok(candidate)
}

/// pgcache CLI-only argument vector for the given coordinates.
#[allow(clippy::too_many_arguments)]
fn pgcache_args(
    origin_user: &str,
    origin_port: u16,
    origin_db: &str,
    cache_user: &str,
    cache_port: u16,
    cache_db: &str,
    listen_port: u16,
    metrics_port: u16,
    publication: &str,
    slot: &str,
    workers: usize,
    log_level: &str,
) -> Vec<String> {
    let s = str::to_string;
    vec![
        s("--origin_host"),
        s("127.0.0.1"),
        s("--origin_port"),
        origin_port.to_string(),
        s("--origin_user"),
        s(origin_user),
        s("--origin_database"),
        s(origin_db),
        s("--cache_host"),
        s("127.0.0.1"),
        s("--cache_port"),
        cache_port.to_string(),
        s("--cache_user"),
        s(cache_user),
        s("--cache_database"),
        s(cache_db),
        s("--cdc_publication_name"),
        s(publication),
        s("--cdc_slot_name"),
        s(slot),
        s("--listen_socket"),
        format!("127.0.0.1:{listen_port}"),
        s("--metrics_socket"),
        format!("127.0.0.1:{metrics_port}"),
        s("--num_workers"),
        workers.to_string(),
        s("--log_level"),
        s(log_level),
    ]
}

async fn wait_ready(readyz: &str, timeout: Duration, log_path: &Path) -> Result<()> {
    let http = reqwest::Client::new();
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(r) = http.get(readyz).send().await
            && r.status().is_success()
        {
            return Ok(());
        }
        if Instant::now() >= deadline {
            let body = std::fs::read_to_string(log_path).unwrap_or_default();
            let tail: Vec<&str> = body.lines().rev().take(30).collect();
            let tail: String = tail.into_iter().rev().collect::<Vec<_>>().join("\n");
            bail!("pgcache did not become ready within {timeout:?}; log tail:\n{tail}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

impl SpawnedPgcache {
    pub async fn launch(opts: SpawnOptions) -> Result<Self> {
        let bin = pgcache_bin_resolve(opts.pgcache_bin)?;

        let origin_db = PgTempDBBuilder::new()
            .with_dbname("conformance_origin")
            .with_config_param("wal_level", "logical")
            .start_async()
            .await;
        let cache_db = PgTempDBBuilder::new()
            .with_dbname("conformance_cache")
            .with_config_param("log_destination", "stderr")
            .with_config_param("logging_collector", "on")
            .with_config_param("shared_preload_libraries", "pgcache_pgrx")
            .start_async()
            .await;

        // Coordinates captured before the DBs are moved into the struct.
        let origin_user = origin_db.db_user().to_string();
        let origin_port = origin_db.db_port();
        let origin_name = origin_db.db_name().to_string();
        let cache_user = cache_db.db_user().to_string();
        let cache_port = cache_db.db_port();
        let cache_name = cache_db.db_name().to_string();

        // pgcache requires its tracking extension in the cache database.
        let cache_admin_url = conn_url(&cache_user, cache_port, &cache_name);
        let (client, connection) = tokio_postgres::connect(&cache_admin_url, NoTls)
            .await
            .context("connecting to ephemeral cache db")?;
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                tracing::error!(error = %e, "cache db setup connection ended");
            }
        });
        client
            .execute("CREATE EXTENSION IF NOT EXISTS pgcache_pgrx", &[])
            .await
            .context("creating pgcache_pgrx extension on cache db")?;
        drop(client);

        let listen_port = port_free()?;
        let metrics_port = port_free()?;
        let suffix = format!(
            "{}_{}",
            std::process::id(),
            SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        );
        let publication = format!("pgc_conf_pub_{suffix}");
        let slot = format!("pgc_conf_slot_{suffix}");

        let log_path = std::env::temp_dir().join(format!("pgcache_conformance_{suffix}.log"));
        let log_file = std::fs::File::create(&log_path)
            .with_context(|| format!("creating pgcache log file {}", log_path.display()))?;
        let log_err = log_file
            .try_clone()
            .context("cloning log file handle for stderr")?;

        let args = pgcache_args(
            &origin_user,
            origin_port,
            &origin_name,
            &cache_user,
            cache_port,
            &cache_name,
            listen_port,
            metrics_port,
            &publication,
            &slot,
            opts.workers,
            &opts.log_level,
        );

        tracing::info!(bin = %bin.display(), listen_port, metrics_port, "spawning pgcache");
        let child = Command::new(&bin)
            .args(&args)
            .envs(opts.env.iter().map(|(k, v)| (k.as_str(), v.as_str())))
            .stdin(Stdio::null())
            .stdout(Stdio::from(log_file))
            .stderr(Stdio::from(log_err))
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("spawning {}", bin.display()))?;

        let status_url = format!("http://127.0.0.1:{metrics_port}/status");
        let metrics_url = format!("http://127.0.0.1:{metrics_port}/metrics");
        let readyz = format!("http://127.0.0.1:{metrics_port}/readyz");

        wait_ready(&readyz, opts.ready_timeout, &log_path)
            .await
            .context("waiting for spawned pgcache /readyz")?;

        Ok(Self {
            child,
            _origin_db: origin_db,
            _cache_db: cache_db,
            origin_url: conn_url(&origin_user, origin_port, &origin_name),
            // Transparent proxy: clients connect to the listen port using
            // the origin database name and origin user (trust auth).
            cache_url: conn_url(&origin_user, listen_port, &origin_name),
            cache_db_url: cache_admin_url.clone(),
            status_url,
            metrics_url,
            logs_file: log_path,
            publication,
        })
    }

    /// Stop pgcache and reap it. SIGKILL (not graceful) — the ephemeral
    /// origin is torn down right after, so the orphaned replication slot
    /// dies with it; no clean slot-drop is needed.
    pub async fn shutdown(mut self) {
        if let Some(id) = self.child.id() {
            tracing::info!(pid = id, "stopping spawned pgcache");
        }
        let _ = self.child.start_kill();
        let _ = self.child.wait().await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn conn_url_shape() {
        assert_eq!(
            conn_url("postgres", 5599, "origin"),
            "postgres://postgres@127.0.0.1:5599/origin"
        );
    }

    #[test]
    fn port_free_returns_bindable_port() {
        let p = port_free().unwrap();
        assert!(p > 0);
        TcpListener::bind(("127.0.0.1", p)).unwrap();
    }

    #[test]
    fn args_contain_required_cli_only_fields() {
        let a = pgcache_args(
            "pg", 5432, "origin", "pgc", 7654, "cache", 6432, 9090, "pub_x", "slot_x", 2, "info",
        );
        for needle in [
            "--origin_host",
            "--origin_port",
            "--cache_database",
            "--cdc_publication_name",
            "--listen_socket",
            "--metrics_socket",
            "--num_workers",
        ] {
            assert!(a.iter().any(|x| x == needle), "missing {needle}");
        }
        let joined = a.join(" ");
        assert!(joined.contains("127.0.0.1:6432"));
        assert!(joined.contains("127.0.0.1:9090"));
    }
}
