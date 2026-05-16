use std::path::PathBuf;

use clap::Parser;

/// SQL conformance harness: result-diff pgcache vs origin.
///
/// Origin is the oracle. Every statement runs against origin and pgcache;
/// any divergence in result set, routing, or a swallowed pgcache error
/// fails the suite.
///
/// With `--cache` the harness targets an already-running pgcache
/// (external mode). With no `--cache` it provisions everything itself:
/// ephemeral origin + cache Postgres via pgtemp and a pgcache built from
/// this workspace (ephemeral mode) — no other flags required.
#[derive(Debug, Clone, Parser)]
#[command(name = "pgcache-conformance", version)]
pub struct Cli {
    /// Connection string for the origin PostgreSQL (the oracle). Required
    /// in external mode; ignored in ephemeral mode (origin is spawned).
    #[arg(long)]
    pub origin: Option<String>,

    /// Connection string for an already-running pgcache. Omit to run in
    /// ephemeral mode (origin + cache + pgcache all provisioned here).
    #[arg(long)]
    pub cache: Option<String>,

    /// pgcache `/status` endpoint. Required when `--cache` is given;
    /// derived automatically in ephemeral mode.
    #[arg(long)]
    pub status_url: Option<String>,

    /// pgcache `/metrics` endpoint. Optional; used only for diagnosing
    /// CDC settling timeouts, never for assertions.
    #[arg(long)]
    pub metrics_url: Option<String>,

    /// Path to the pgcache log file. Tailed over each statement's time
    /// window to catch swallowed errors (the PGC-102 check). Optional:
    /// without it that check is skipped. In ephemeral mode the harness
    /// captures the spawned child's stdout/stderr instead, so this is
    /// only relevant for external runs.
    #[arg(long)]
    pub logs_file: Option<PathBuf>,

    /// pgcache's CDC publication name (`[cdc].publication_name`). When
    /// set, fixtures are added to this publication so they replicate to
    /// pgcache. The publication is assumed to exist (it may be empty).
    /// Set automatically in ephemeral mode.
    #[arg(long)]
    pub publication: Option<String>,

    /// A `.slt` file or a directory of `.slt` files to run.
    #[arg(long)]
    pub tests: PathBuf,

    /// Budget for CDC settling between DML and the next SELECT.
    #[arg(long, default_value_t = 60)]
    pub cdc_timeout_secs: u64,

    /// Write a JUnit XML report to this path.
    #[arg(long)]
    pub junit: Option<PathBuf>,

    /// (Ephemeral mode) Path to a prebuilt pgcache binary. Default:
    /// the `pgcache` binary alongside this one, building it if absent.
    #[arg(long)]
    pub pgcache_bin: Option<PathBuf>,

    /// (Ephemeral mode) pgcache worker count.
    #[arg(long, default_value_t = 2)]
    pub pgcache_workers: usize,

    /// (Ephemeral mode) pgcache log level.
    #[arg(long, default_value = "info")]
    pub pgcache_log_level: String,

    /// (Ephemeral mode) Readiness budget for the spawned pgcache.
    #[arg(long, default_value_t = 60)]
    pub spawn_timeout_secs: u64,
}

/// How the harness obtains origin + pgcache for the run.
#[derive(Debug, Clone)]
pub enum CacheSource {
    /// Connect to an already-running pgcache and origin.
    External {
        origin_url: String,
        cache_url: String,
        status_url: String,
        metrics_url: Option<String>,
        logs_file: Option<PathBuf>,
    },
    /// Provision ephemeral origin + cache Postgres and spawn pgcache
    /// from this workspace.
    Ephemeral,
}

impl Cli {
    /// Resolve how origin + pgcache will be obtained.
    ///
    /// `--cache` selects external mode, which then requires `--origin`
    /// (the oracle) and `--status-url` (routing assertions can't run
    /// without it). No `--cache` selects ephemeral mode, which needs no
    /// other connection flags.
    pub fn cache_source(&self) -> anyhow::Result<CacheSource> {
        match &self.cache {
            Some(cache_url) => {
                let origin_url = self
                    .origin
                    .clone()
                    .ok_or_else(|| anyhow::anyhow!("--origin is required when --cache is given"))?;
                let status_url = self.status_url.clone().ok_or_else(|| {
                    anyhow::anyhow!("--status-url is required when --cache is given")
                })?;
                Ok(CacheSource::External {
                    origin_url,
                    cache_url: cache_url.clone(),
                    status_url,
                    metrics_url: self.metrics_url.clone(),
                    logs_file: self.logs_file.clone(),
                })
            }
            None => Ok(CacheSource::Ephemeral),
        }
    }
}
