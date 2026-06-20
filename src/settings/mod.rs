use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;

use error_set::error_set;
use rootcause::Report;
use serde::{Deserialize, Serialize};

mod cli;
mod dynamic;
mod toml_file;

#[cfg(test)]
mod tests;

pub use dynamic::{
    DynamicConfig, DynamicConfigHandle, DynamicConfigPatch, LogReloadHandle, StaticConfigSnapshot,
};
pub use toml_file::{config_file_dynamic_extract, config_file_dynamic_update};

error_set! {
    ConfigError := {
        ArgumentError(Box<dyn Error + Send + Sync + 'static>),
        TomlError(Box<dyn Error + Send + Sync + 'static>),

        #[display("Missing argument: {name}")]
        ArgumentMissing{ name: &'static str},
        IoError(io::Error),
    }
}

/// Result type with location-tracking error reports for configuration operations.
pub type ConfigResult<T> = Result<T, Report<ConfigError>>;

impl From<lexopt::Error> for ConfigError {
    fn from(error: lexopt::Error) -> Self {
        Self::ArgumentError(Box::new(error))
    }
}

impl From<toml::de::Error> for ConfigError {
    fn from(error: toml::de::Error) -> Self {
        Self::TomlError(Box::new(error))
    }
}

/// SSL/TLS connection mode for PostgreSQL connections.
///
/// Matches PostgreSQL semantics:
/// - `disable`: no encryption
/// - `require`: encrypt but don't verify the server certificate
/// - `verify-full`: encrypt and verify the server certificate against trusted CAs
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum SslMode {
    /// No TLS encryption (default for backwards compatibility)
    #[default]
    Disable,
    /// Require TLS encryption, but don't verify the server certificate.
    /// Matches PostgreSQL's `sslmode=require`.
    Require,
    /// Require TLS encryption and verify the server certificate against trusted CAs.
    /// Matches PostgreSQL's `sslmode=verify-full`.
    VerifyFull,
}

/// Error returned when parsing an invalid SSL mode string
#[derive(Debug, Clone)]
pub struct ParseSslModeError(String);

impl fmt::Display for ParseSslModeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid SSL mode: '{}', expected 'disable', 'require', or 'verify-full'",
            self.0
        )
    }
}

impl Error for ParseSslModeError {}

/// Cache eviction policy
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum CachePolicy {
    /// FIFO eviction: oldest-registered query evicted first, no admission gating
    Fifo,
    /// CLOCK eviction: second-chance algorithm with frequency-based admission
    #[default]
    Clock,
}

/// Error returned when parsing an invalid cache policy string
#[derive(Debug, Clone)]
pub struct ParseCachePolicyError(String);

impl fmt::Display for ParseCachePolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "invalid cache policy: '{}', expected 'fifo' or 'clock'",
            self.0
        )
    }
}

impl Error for ParseCachePolicyError {}

impl FromStr for CachePolicy {
    type Err = ParseCachePolicyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "fifo" => Ok(CachePolicy::Fifo),
            "clock" => Ok(CachePolicy::Clock),
            _ => Err(ParseCachePolicyError(s.to_owned())),
        }
    }
}

/// Parsed allowlist entry: (optional schema, table name), both lowercased.
pub type AllowlistEntry = (Option<String>, String);

/// Parsed and ready-to-match allowlist. None = all tables cacheable.
pub type Allowlist = Option<Vec<AllowlistEntry>>;

impl FromStr for SslMode {
    type Err = ParseSslModeError;

    /// Parse SSL mode from string (case-insensitive)
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "disable" => Ok(SslMode::Disable),
            "require" => Ok(SslMode::Require),
            "verify-full" | "verify_full" => Ok(SslMode::VerifyFull),
            _ => Err(ParseSslModeError(s.to_owned())),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct PgSettings {
    pub host: String,
    pub port: u16,
    pub user: String,
    #[serde(default)]
    pub password: Option<String>,
    pub database: String,
    #[serde(default)]
    pub ssl_mode: SslMode,
}

/// Partial PostgreSQL settings where all fields are optional.
/// Used for replication settings that cascade defaults from origin.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct PgSettingsPartial {
    pub host: Option<String>,
    pub port: Option<u16>,
    pub user: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    pub database: Option<String>,
    #[serde(default)]
    pub ssl_mode: Option<SslMode>,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct CdcSettings {
    pub publication_name: String,
    pub slot_name: String,
}

#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct ListenSettings {
    pub socket: SocketAddr,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsSettings {
    pub socket: SocketAddr,
}

/// Internal struct for TOML deserialization with optional replication settings.
#[derive(Debug, Clone, Deserialize)]
struct SettingsToml {
    origin: PgSettings,
    #[serde(default)]
    replication: Option<PgSettingsPartial>,
    cache: PgSettings,
    cdc: CdcSettings,
    listen: ListenSettings,
    num_workers: usize,
    cache_size: Option<usize>,
    #[serde(default)]
    tls_cert: Option<PathBuf>,
    #[serde(default)]
    tls_key: Option<PathBuf>,
    #[serde(default)]
    metrics: Option<MetricsSettings>,
    #[serde(default)]
    log_level: Option<String>,
    #[serde(default)]
    cache_policy: Option<CachePolicy>,
    #[serde(default)]
    admission_threshold: Option<u32>,
    /// Materialized-view size gate: a Measure query is materialized iff
    /// `result_rows × mv_size_ratio ≤ source_rows` at first population.
    /// Defaults to 10.
    #[serde(default)]
    mv_size_ratio: Option<u32>,
    /// Materialized-view compute-avoidance gate: a `Gated` query is materialized
    /// iff its origin-population source-row count is `>= mv_compute_min_rows`.
    /// Defaults to 1000.
    #[serde(default)]
    mv_compute_min_rows: Option<u64>,
    /// Total-bytes budget for the in-process hot-result cache (PGC-236).
    /// 0 disables in-memory result memoization. Defaults to 64 MiB.
    #[serde(default)]
    memo_cache_size: Option<usize>,
    /// Optional absolute RSS ceiling (bytes) for registration throttling.
    /// Omitted → 80% of detected RAM. Can only lower the effective ceiling.
    #[serde(default)]
    memory_limit: Option<usize>,
    /// Optional cap (bytes) on cache-volume space used; over it, pgcache
    /// throttles registration and drops tables. Omitted → auto from free space.
    #[serde(default)]
    disk_limit: Option<usize>,
    /// Only cache queries referencing these tables.
    /// Supports both unqualified ("orders") and schema-qualified ("audit.orders") names.
    /// If omitted or empty, all tables are cacheable.
    #[serde(default)]
    allowed_tables: Option<Vec<String>>,
    /// Queries to pin in cache at startup. Pinned queries are pre-registered,
    /// protected from eviction, and auto-readmitted after CDC invalidation.
    #[serde(default)]
    pinned_queries: Option<Vec<String>>,
    /// Tables to pin in cache at startup. Syntactic sugar — each table name is
    /// expanded to `SELECT * FROM {table}` and merged with `pinned_queries`.
    #[serde(default)]
    pinned_tables: Option<Vec<String>>,
    /// Enable anonymous telemetry (default: true). Set to false to disable.
    #[serde(default)]
    telemetry: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct Settings {
    pub origin: PgSettings,
    /// Replication connection settings. Defaults to origin if not specified.
    /// Each field cascades from origin if not explicitly set.
    pub replication: PgSettings,
    pub cache: PgSettings,
    pub cdc: CdcSettings,
    pub listen: ListenSettings,
    pub num_workers: usize,
    /// TLS certificate file path (PEM format) for client connections
    pub tls_cert: Option<PathBuf>,
    /// TLS private key file path (PEM format) for client connections
    pub tls_key: Option<PathBuf>,
    /// Prometheus metrics endpoint configuration
    pub metrics: Option<MetricsSettings>,
    /// Runtime-adjustable configuration (cache_size, cache_policy, etc.)
    /// Backed by ArcSwap for lock-free reads on the hot path.
    pub dynamic: DynamicConfigHandle,
    /// Queries to pin in cache at startup. Pinned queries are pre-registered,
    /// protected from eviction, and auto-readmitted after CDC invalidation.
    pub pinned_queries: Option<Vec<String>>,
    /// Enable anonymous telemetry (default: true).
    /// Disable via CLI --telemetry_off, TOML telemetry = false, or env PGCACHE_TELEMETRY=off.
    pub telemetry: bool,
}
