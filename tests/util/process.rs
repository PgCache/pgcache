#![allow(clippy::indexing_slicing)]
#![allow(clippy::unwrap_used)]

use std::{
    io::{Error, Read, Write, stdout},
    net::TcpListener,
    ops::{Deref, DerefMut},
    path::Path,
    process::{Child, Command, Stdio},
    sync::Once,
};

use pgcache_lib::tls::{MakeRustlsConnect, tls_config_with_cert};
use pgtemp::{PgTempDB, PgTempDBBuilder};
use tokio_postgres::{Client, Config, NoTls};
use tokio_util::bytes::{Buf, BytesMut};

static CRYPTO_INIT: Once = Once::new();

/// Initialize the rustls crypto provider (required before using TLS)
fn crypto_provider_init() {
    CRYPTO_INIT.call_once(|| {
        rustls::crypto::aws_lc_rs::default_provider()
            .install_default()
            .expect("install crypto provider");
    });
}

pub fn find_available_port() -> Result<u16, Error> {
    // Bind to port 0 to let the OS assign an available port
    let listener = TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    // Drop the listener to free the port
    drop(listener);
    Ok(port)
}

/// Guard structure that automatically kills and waits for the pgcache process on drop.
/// This ensures proper cleanup even if tests panic.
pub struct PgCacheProcess {
    child: Child,
}

impl PgCacheProcess {
    fn new(child: Child) -> Self {
        Self { child }
    }
}

impl Drop for PgCacheProcess {
    fn drop(&mut self) {
        // Kill the process and wait for it to exit
        // We ignore errors here because the process might already be dead
        let _ = self.child.kill();
        let _ = self.child.wait();

        // Drain stdout if available
        if let Some(ref mut child_stdout) = self.child.stdout {
            let _ = std::io::copy(child_stdout, &mut stdout());
        }
    }
}

impl Deref for PgCacheProcess {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.child
    }
}

impl DerefMut for PgCacheProcess {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.child
    }
}

pub fn proxy_wait_for_ready(pgcache: &mut PgCacheProcess) -> Result<(), Error> {
    const NEEDLE: &str = "Listening to";
    //wait for listening message from proxy before proceeding
    let mut buf = BytesMut::new();
    let mut read_buf = [0u8; 1024];

    let mut stdout = pgcache.stdout.take().unwrap();
    while !String::from_utf8_lossy(&buf).contains(NEEDLE) {
        if buf.len() > NEEDLE.len() {
            buf.advance(buf.len() - NEEDLE.len());
        }
        let cnt = stdout.read(&mut read_buf).unwrap_or_default();
        if cnt == 0 {
            return Err(Error::other("Unexpected end of stdout"));
        }
        std::io::stdout().write_all(&read_buf[0..cnt])?;
        buf.extend_from_slice(&read_buf[0..cnt]);
    }
    pgcache.stdout = Some(stdout);

    Ok(())
}

pub struct TempDBs {
    pub origin: PgTempDB,
    pub cache: PgTempDB,
}

pub async fn start_databases() -> Result<(TempDBs, Client), Error> {
    let db = PgTempDBBuilder::new()
        .with_dbname("origin_test")
        .with_config_param("wal_level", "logical")
        .start_async()
        .await;

    let db_cache = PgTempDBBuilder::new()
        .with_dbname("cache_test")
        .with_config_param("log_destination", "stderr")
        .with_config_param("log_directory", "/tmp/")
        .with_config_param("logging_collector", "on")
        .with_config_param("shared_preload_libraries", "pgcache_pgrx,pg_stat_statements")
        .start_async()
        .await;

    // Set up pgcache_pgrx extension on cache database
    let (cache_client, cache_connection) = Config::new()
        .host("localhost")
        .port(db_cache.db_port())
        .user(db_cache.db_user())
        .dbname(db_cache.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = cache_connection.await {
            eprintln!("cache connection error: {e}");
        }
    });

    cache_client
        .execute("CREATE EXTENSION pgcache_pgrx", &[])
        .await
        .map_err(Error::other)?;

    // Set up logical replication on origin
    let (origin_client, origin_connection) = Config::new()
        .host("localhost")
        .port(db.db_port())
        .user(db.db_user())
        .dbname(db.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = origin_connection.await {
            eprintln!("connection error: {e}");
        }
    });

    // Publication and replication slot are created by pgcache's replication_provision().
    // The test only needs wal_level=logical on the origin (set in PgTempDBBuilder above).

    Ok((
        TempDBs {
            origin: db,
            cache: db_cache,
        },
        origin_client,
    ))
}

/// Spawn a pgcache process with the given extra CLI arguments.
/// Copies the default config to a temp file so each process gets its own copy
/// (prevents concurrent tests from corrupting the shared config via PUT /config).
fn pgcache_spawn(
    dbs: &TempDBs,
    listen_port: u16,
    metrics_port: u16,
    extra_args: &[&str],
) -> PgCacheProcess {
    let listen_socket = format!("127.0.0.1:{}", listen_port);
    let metrics_socket = format!("127.0.0.1:{}", metrics_port);

    let config_path = std::env::temp_dir().join(format!("pgcache_test_{listen_port}.toml"));
    std::fs::copy("tests/data/default_config.toml", &config_path)
        .expect("copy default config to temp file");

    let mut cmd = Command::new(env!("CARGO_BIN_EXE_pgcache"));
    cmd.arg("--config")
        .arg(&config_path)
        .arg("--origin_host")
        .arg("127.0.0.1")
        .arg("--origin_port")
        .arg(dbs.origin.db_port().to_string())
        .arg("--origin_user")
        .arg(dbs.origin.db_user())
        .arg("--origin_database")
        .arg(dbs.origin.db_name())
        .arg("--cache_host")
        .arg("127.0.0.1")
        .arg("--cache_port")
        .arg(dbs.cache.db_port().to_string())
        .arg("--cache_user")
        .arg(dbs.cache.db_user())
        .arg("--cache_database")
        .arg(dbs.cache.db_name())
        .arg("--listen_socket")
        .arg(&listen_socket)
        .arg("--metrics_socket")
        .arg(&metrics_socket)
        .arg("--log_level")
        .arg("info");

    for arg in extra_args {
        cmd.arg(arg);
    }

    let child = cmd
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("run pgcache");

    PgCacheProcess::new(child)
}

/// Connect a plain TCP client to pgcache.
async fn pgcache_client_connect(listen_port: u16) -> Result<Client, Error> {
    let (client, connection) = Config::new()
        .host("localhost")
        .port(listen_port)
        .user("postgres")
        .dbname("origin_test")
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok(client)
}

pub async fn connect_pgcache(dbs: &TempDBs) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let mut pgcache = pgcache_spawn(dbs, listen_port, metrics_port, &["--cache_policy", "fifo"]);
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with clock eviction policy.
pub async fn connect_pgcache_clock(
    dbs: &TempDBs,
    admission_threshold: u32,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let threshold_str = admission_threshold.to_string();
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &[
            "--cache_policy",
            "clock",
            "--admission_threshold",
            &threshold_str,
        ],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with a small cache size to force eviction.
pub async fn connect_pgcache_small_cache(
    dbs: &TempDBs,
    cache_size: usize,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let cache_size_str = cache_size.to_string();
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &["--cache_policy", "fifo", "--cache_size", &cache_size_str],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with a table allowlist.
pub async fn connect_pgcache_allowlist(
    dbs: &TempDBs,
    allowed_tables: &str,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &["--cache_policy", "fifo", "--allowed_tables", allowed_tables],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with pinned queries (FIFO policy).
pub async fn connect_pgcache_pinned(
    dbs: &TempDBs,
    pinned_queries: &str,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &["--cache_policy", "fifo", "--pinned_queries", pinned_queries],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with pinned queries and a small cache size (FIFO policy).
pub async fn connect_pgcache_pinned_small_cache(
    dbs: &TempDBs,
    pinned_queries: &str,
    cache_size: usize,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let cache_size_str = cache_size.to_string();
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &[
            "--cache_policy",
            "fifo",
            "--pinned_queries",
            pinned_queries,
            "--cache_size",
            &cache_size_str,
        ],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;
    let client = pgcache_client_connect(listen_port).await?;
    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect to pgcache with TLS enabled on the proxy.
pub async fn connect_pgcache_tls(
    dbs: &TempDBs,
) -> Result<(PgCacheProcess, u16, u16, Client), Error> {
    crypto_provider_init();

    let listen_port = find_available_port()?;
    let metrics_port = find_available_port()?;
    let mut pgcache = pgcache_spawn(
        dbs,
        listen_port,
        metrics_port,
        &[
            "--tls_cert",
            "tests/data/certs/server.crt",
            "--tls_key",
            "tests/data/certs/server.key",
            "--cache_policy",
            "fifo",
        ],
    );
    proxy_wait_for_ready(&mut pgcache).map_err(Error::other)?;

    let cert_path = Path::new("tests/data/certs/server.crt");
    let tls_config = tls_config_with_cert(cert_path)?;
    let tls = MakeRustlsConnect::new(tls_config);

    let (client, connection) = Config::new()
        .host("localhost")
        .port(listen_port)
        .user("postgres")
        .dbname("origin_test")
        .connect(tls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {e}");
        }
    });

    Ok((pgcache, listen_port, metrics_port, client))
}

/// Connect directly to the cache database (bypassing pgcache proxy).
/// Useful for verifying internal cache state like indexes.
pub async fn connect_cache_db(dbs: &TempDBs) -> Result<Client, Error> {
    let (client, connection) = Config::new()
        .host("localhost")
        .port(dbs.cache.db_port())
        .user(dbs.cache.db_user())
        .dbname(dbs.cache.db_name())
        .connect(NoTls)
        .await
        .map_err(Error::other)?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("cache db connection error: {e}");
        }
    });

    Ok(client)
}
