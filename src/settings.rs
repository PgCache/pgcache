use std::{
    error::Error,
    fmt, fs,
    fs::read_to_string,
    io,
    net::SocketAddr,
    path::Path,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Mutex},
};

use arc_swap::ArcSwap;
use error_set::error_set;
use lexopt::prelude::*;
use rootcause::Report;
use serde::{Deserialize, Serialize};

use crate::result::MapIntoReport;

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

/// Parse an allowlist entry string into (optional schema, table name).
/// Supports "table" and "schema.table" forms.
pub fn allowlist_entry_parse(entry: &str) -> AllowlistEntry {
    let entry = entry.trim();
    match entry.rsplit_once('.') {
        Some((schema, table)) => (Some(schema.to_lowercase()), table.to_lowercase()),
        None => (None, entry.to_lowercase()),
    }
}

/// Parse config strings into a ready-to-match allowlist.
pub fn allowlist_parse(tables: &Option<Vec<String>>) -> Allowlist {
    tables
        .as_ref()
        .filter(|v| !v.is_empty())
        .map(|entries| entries.iter().map(|e| allowlist_entry_parse(e)).collect())
}

/// Runtime-adjustable configuration fields.
/// Stored behind ArcSwap for lock-free reads on the hot path.
#[derive(Debug, Clone, Serialize)]
pub struct DynamicConfig {
    /// Explicit disk-size budget (bytes) for cached data. When `None`, the limit
    /// is auto-derived from the cache volume's live free space, keeping a reserve
    /// free (PGC-251 Slice 2); a configured value is a hard override. Deprecated
    /// in favour of the auto limit.
    pub cache_size: Option<usize>,
    pub cache_policy: CachePolicy,
    pub admission_threshold: u32,
    pub allowed_tables: Option<Vec<String>>,
    #[serde(skip)]
    pub allowed_tables_parsed: Allowlist,
    pub log_level: Option<String>,
    /// Materialized-view size gate: a Measure-classified query is materialized
    /// iff `result_rows × mv_size_ratio ≤ source_rows` at first population.
    /// Sticky: retuning affects future first-population decisions only.
    pub mv_size_ratio: u32,
    /// Total-bytes budget for the in-process hot-result cache (PGC-236).
    /// 0 disables in-memory result memoization.
    pub memo_cache_size: usize,
    /// Optional absolute ceiling (bytes) on pgcache's resident memory, used to
    /// throttle new-query registration before the box runs out of RAM. When
    /// `None`, the ceiling is derived dynamically as 80% of detected RAM. A
    /// configured value can only lower the effective ceiling, never raise it.
    pub memory_limit: Option<usize>,
}

const DEFAULT_ADMISSION_THRESHOLD: u32 = 1;
const DEFAULT_MV_SIZE_RATIO: u32 = 10;
/// Default total-bytes budget for the in-process hot-result cache: 64 MiB.
const DEFAULT_MEMO_CACHE_SIZE: usize = 64 * 1024 * 1024;

impl DynamicConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache_size: Option<usize>,
        cache_policy: Option<CachePolicy>,
        admission_threshold: Option<u32>,
        allowed_tables: Option<Vec<String>>,
        log_level: Option<String>,
        mv_size_ratio: Option<u32>,
        memo_cache_size: Option<usize>,
        memory_limit: Option<usize>,
    ) -> Self {
        Self {
            cache_size,
            cache_policy: cache_policy.unwrap_or_default(),
            admission_threshold: admission_threshold.unwrap_or(DEFAULT_ADMISSION_THRESHOLD),
            allowed_tables_parsed: allowlist_parse(&allowed_tables),
            allowed_tables,
            log_level,
            mv_size_ratio: mv_size_ratio.unwrap_or(DEFAULT_MV_SIZE_RATIO),
            memo_cache_size: memo_cache_size.unwrap_or(DEFAULT_MEMO_CACHE_SIZE),
            memory_limit,
        }
    }
}

/// Type-erased handle for reloading the tracing log filter at runtime.
type LogReloadFn = Box<dyn Fn(&str) -> Result<(), String> + Send + Sync>;
type LogCurrentFn = Box<dyn Fn() -> Option<String> + Send + Sync>;

pub struct LogReloadHandle {
    pub reload: LogReloadFn,
    pub current: LogCurrentFn,
}

/// Snapshot of static config fields captured at startup.
/// Used to detect when the TOML file has been edited and a restart is needed.
#[derive(Debug, Clone, PartialEq)]
pub struct StaticConfigSnapshot {
    pub origin: PgSettings,
    pub cache: PgSettings,
    pub listen: ListenSettings,
    pub num_workers: usize,
    pub cdc: CdcSettings,
}

impl StaticConfigSnapshot {
    pub fn from_settings(settings: &Settings) -> Self {
        Self {
            origin: settings.origin.clone(),
            cache: settings.cache.clone(),
            listen: settings.listen.clone(),
            num_workers: settings.num_workers,
            cdc: settings.cdc.clone(),
        }
    }

    fn from_toml(config: &SettingsToml) -> Self {
        Self {
            origin: config.origin.clone(),
            cache: config.cache.clone(),
            listen: config.listen.clone(),
            num_workers: config.num_workers,
            cdc: config.cdc.clone(),
        }
    }
}

/// Shared handle for reading/updating dynamic config. Cloneable, lock-free reads.
pub struct DynamicConfigHandle {
    inner: Arc<ArcSwap<DynamicConfig>>,
    // Arc-wrapped so cloning the handle (done per query when a connection
    // clones `CacheDispatch`) is a refcount bump, not a deep copy of
    // the static config. Both are set once at startup and never mutated.
    config_path: Option<Arc<PathBuf>>,
    log_reload: Arc<Mutex<Option<LogReloadHandle>>>,
    static_snapshot: Option<Arc<StaticConfigSnapshot>>,
}

impl Clone for DynamicConfigHandle {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
            config_path: self.config_path.clone(),
            log_reload: Arc::clone(&self.log_reload),
            static_snapshot: self.static_snapshot.clone(),
        }
    }
}

impl fmt::Debug for DynamicConfigHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DynamicConfigHandle")
            .field("config", &*self.inner.load())
            .field("config_path", &self.config_path)
            .finish()
    }
}

impl DynamicConfigHandle {
    fn new(
        config: DynamicConfig,
        config_path: Option<PathBuf>,
        static_snapshot: Option<StaticConfigSnapshot>,
    ) -> Self {
        Self {
            inner: Arc::new(ArcSwap::from_pointee(config)),
            config_path: config_path.map(Arc::new),
            log_reload: Arc::new(Mutex::new(None)),
            static_snapshot: static_snapshot.map(Arc::new),
        }
    }

    /// Lock-free read of current dynamic config. Hot path.
    pub fn load(&self) -> arc_swap::Guard<Arc<DynamicConfig>> {
        self.inner.load()
    }

    /// Path to the TOML config file, if one was provided at startup.
    pub fn config_path(&self) -> Option<&Path> {
        self.config_path.as_deref().map(PathBuf::as_path)
    }

    /// Check if the TOML config file has static fields that differ from the running config.
    /// Returns true if a restart is needed to apply the changes.
    pub fn restart_required(&self) -> bool {
        let (Some(path), Some(snapshot)) = (self.config_path.as_deref(), &self.static_snapshot)
        else {
            return false;
        };
        let Ok(content) = read_to_string(path) else {
            return false;
        };
        let Ok(config) = toml::from_str::<SettingsToml>(&content) else {
            return false;
        };
        let file_snapshot = StaticConfigSnapshot::from_toml(&config);
        **snapshot != file_snapshot
    }

    /// Query the effective log level from the tracing subscriber.
    /// Returns None if no reload handle is set (e.g., console-subscriber mode).
    pub fn effective_log_level(&self) -> Option<String> {
        if let Ok(guard) = self.log_reload.lock()
            && let Some(ref handle) = *guard
        {
            (handle.current)()
        } else {
            None
        }
    }

    /// Set the log reload handle. Called once at startup after tracing is initialized.
    pub fn log_reload_handle_set(&self, handle: LogReloadHandle) {
        if let Ok(mut guard) = self.log_reload.lock() {
            *guard = Some(handle);
        }
    }

    /// If log_level changed, reload the tracing filter.
    fn log_level_reload(&self, new_level: Option<&str>) {
        if let Ok(guard) = self.log_reload.lock()
            && let Some(ref handle) = *guard
            && let Err(e) = (handle.reload)(new_level.unwrap_or("info"))
        {
            tracing::error!("log level reload failed: {e}");
        }
    }

    /// Swap in a new dynamic config. Called on config update via admin API.
    /// Reloads log level if it changed.
    pub fn update(&self, new: DynamicConfig) {
        let old = self.inner.load();
        let log_changed = old.log_level != new.log_level;
        let new_log_level = new.log_level.clone();
        self.inner.store(Arc::new(new));
        if log_changed {
            self.log_level_reload(new_log_level.as_deref());
        }
    }

    /// Create a handle with default dynamic config and no config file. For tests.
    #[cfg(test)]
    pub fn test_default() -> Self {
        Self::new(
            DynamicConfig::new(None, None, None, None, None, None, None, None),
            None,
            None,
        )
    }
}

/// Partial update for dynamic config fields via PUT /config.
/// None = don't change, Some(None) = unset to default, Some(Some(v)) = set to v.
///
/// For nullable fields (cache_size, allowed_tables, log_level), JSON `null`
/// means "unset" (Some(None)), absent means "don't change" (None).
#[derive(Debug, Clone, Deserialize)]
pub struct DynamicConfigPatch {
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub cache_size: Option<Option<usize>>,
    #[serde(default)]
    pub cache_policy: Option<CachePolicy>,
    #[serde(default)]
    pub admission_threshold: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub allowed_tables: Option<Option<Vec<String>>>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub log_level: Option<Option<String>>,
    #[serde(default)]
    pub mv_size_ratio: Option<u32>,
    #[serde(default)]
    pub memo_cache_size: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub memory_limit: Option<Option<usize>>,
}

/// Deserialize a double-Option: absent → None, null → Some(None), value → Some(Some(v)).
fn deserialize_double_option<'de, T, D>(deserializer: D) -> Result<Option<Option<T>>, D::Error>
where
    T: Deserialize<'de>,
    D: serde::Deserializer<'de>,
{
    Ok(Some(Option::<T>::deserialize(deserializer)?))
}

impl DynamicConfigPatch {
    /// Merge this patch into an existing config, producing a new DynamicConfig.
    pub fn apply(&self, current: &DynamicConfig) -> DynamicConfig {
        let allowlist_changed = self.allowed_tables.is_some();
        let allowed_tables = match &self.allowed_tables {
            Some(v) => v.clone(),
            None => current.allowed_tables.clone(),
        };
        DynamicConfig {
            cache_size: match self.cache_size {
                Some(v) => v,
                None => current.cache_size,
            },
            cache_policy: self.cache_policy.unwrap_or(current.cache_policy),
            admission_threshold: self
                .admission_threshold
                .unwrap_or(current.admission_threshold),
            allowed_tables_parsed: if allowlist_changed {
                allowlist_parse(&allowed_tables)
            } else {
                current.allowed_tables_parsed.clone()
            },
            allowed_tables,
            log_level: match &self.log_level {
                Some(v) => v.clone(),
                None => current.log_level.clone(),
            },
            mv_size_ratio: self.mv_size_ratio.unwrap_or(current.mv_size_ratio),
            memo_cache_size: self.memo_cache_size.unwrap_or(current.memo_cache_size),
            memory_limit: match self.memory_limit {
                Some(v) => v,
                None => current.memory_limit,
            },
        }
    }
}

/// Extract dynamic config fields from a parsed TOML config file.
fn dynamic_config_from_toml(config: &SettingsToml) -> DynamicConfig {
    DynamicConfig::new(
        config.cache_size,
        config.cache_policy,
        config.admission_threshold,
        config.allowed_tables.clone(),
        config.log_level.clone(),
        config.mv_size_ratio,
        config.memo_cache_size,
        config.memory_limit,
    )
}

/// Read a TOML config file and extract the dynamic config fields.
pub fn config_file_dynamic_extract(path: &Path) -> ConfigResult<DynamicConfig> {
    let content = read_to_string(path).map_into_report::<ConfigError>()?;
    let config: SettingsToml = toml::from_str(&content).map_into_report::<ConfigError>()?;
    Ok(dynamic_config_from_toml(&config))
}

/// Apply a patch to the TOML config file, preserving formatting and comments.
/// Returns the new effective dynamic config after the update.
#[allow(clippy::indexing_slicing)] // toml_edit doc[key] creates keys, does not panic
pub fn config_file_dynamic_update(path: &Path, patch: &DynamicConfigPatch) -> ConfigResult<()> {
    let content = read_to_string(path).map_into_report::<ConfigError>()?;
    let mut doc: toml_edit::DocumentMut = content
        .parse()
        .map_err(|e: toml_edit::TomlError| ConfigError::TomlError(Box::new(e)))
        .map_into_report::<ConfigError>()?;

    if let Some(v) = &patch.cache_size {
        match v {
            Some(size) => {
                let size_i64 = i64::try_from(*size).expect("cache size fits in i64");
                doc["cache_size"] = toml_edit::value(size_i64);
            }
            None => {
                doc.remove("cache_size");
            }
        }
    }

    if let Some(policy) = &patch.cache_policy {
        let s = match policy {
            CachePolicy::Fifo => "fifo",
            CachePolicy::Clock => "clock",
        };
        doc["cache_policy"] = toml_edit::value(s);
    }

    if let Some(threshold) = &patch.admission_threshold {
        doc["admission_threshold"] = toml_edit::value(*threshold as i64);
    }

    if let Some(ratio) = &patch.mv_size_ratio {
        doc["mv_size_ratio"] = toml_edit::value(*ratio as i64);
    }

    if let Some(v) = &patch.memo_cache_size {
        let v_i64 = i64::try_from(*v).expect("memo cache size fits in i64");
        doc["memo_cache_size"] = toml_edit::value(v_i64);
    }

    if let Some(v) = &patch.memory_limit {
        match v {
            Some(limit) => {
                let limit_i64 = i64::try_from(*limit).expect("memory limit fits in i64");
                doc["memory_limit"] = toml_edit::value(limit_i64);
            }
            None => {
                doc.remove("memory_limit");
            }
        }
    }

    if let Some(v) = &patch.allowed_tables {
        match v {
            Some(tables) => {
                let mut arr = toml_edit::Array::new();
                for t in tables {
                    arr.push(t.as_str());
                }
                doc["allowed_tables"] = toml_edit::value(arr);
            }
            None => {
                doc.remove("allowed_tables");
            }
        }
    }

    if let Some(v) = &patch.log_level {
        match v {
            Some(level) => doc["log_level"] = toml_edit::value(level.as_str()),
            None => {
                doc.remove("log_level");
            }
        }
    }

    fs::write(path, doc.to_string()).map_into_report::<ConfigError>()?;
    Ok(())
}

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

impl PgSettingsPartial {
    /// Merge with a base PgSettings, using base values for any unspecified fields.
    pub fn merge_with(&self, base: &PgSettings) -> PgSettings {
        PgSettings {
            host: self.host.clone().unwrap_or_else(|| base.host.clone()),
            port: self.port.unwrap_or(base.port),
            user: self.user.clone().unwrap_or_else(|| base.user.clone()),
            password: self.password.clone().or_else(|| base.password.clone()),
            database: self
                .database
                .clone()
                .unwrap_or_else(|| base.database.clone()),
            ssl_mode: self.ssl_mode.unwrap_or(base.ssl_mode),
        }
    }
}

/// Resolve replication settings from the three-tier cascade:
/// 1. Origin defaults (base)
/// 2. TOML `[replication]` partial (if present)
/// 3. CLI `--replication_*` overrides (if present)
pub fn replication_settings_resolve(
    origin: &PgSettings,
    toml_replication: Option<PgSettingsPartial>,
    cli_overrides: PgSettingsPartial,
) -> PgSettings {
    let base = match toml_replication {
        Some(partial) => partial.merge_with(origin),
        None => origin.clone(),
    };
    cli_overrides.merge_with(&base)
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
    /// Total-bytes budget for the in-process hot-result cache (PGC-236).
    /// 0 disables in-memory result memoization. Defaults to 64 MiB.
    #[serde(default)]
    memo_cache_size: Option<usize>,
    /// Optional absolute RSS ceiling (bytes) for registration throttling.
    /// Omitted → 80% of detected RAM. Can only lower the effective ceiling.
    #[serde(default)]
    memory_limit: Option<usize>,
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

/// Parse the next CLI argument as a string.
fn arg_string(parser: &mut lexopt::Parser) -> ConfigResult<String> {
    parser
        .value()
        .map_into_report::<ConfigError>()?
        .string()
        .map_into_report::<ConfigError>()
}

/// Parse the next CLI argument via `FromStr`.
fn arg_parse<T: FromStr>(parser: &mut lexopt::Parser) -> ConfigResult<T>
where
    T::Err: Error + Send + Sync + 'static,
{
    parser
        .value()
        .map_into_report::<ConfigError>()?
        .parse()
        .map_into_report::<ConfigError>()
}

/// Parse the next CLI argument as a custom enum type, mapping parse errors to `ArgumentError`.
fn arg_enum<T: FromStr>(parser: &mut lexopt::Parser) -> ConfigResult<T>
where
    T::Err: fmt::Display,
{
    let s = arg_string(parser)?;
    s.parse()
        .map_err(|e: T::Err| Report::from(ConfigError::ArgumentError(e.to_string().into())))
}

/// Require an `Option<T>` to be `Some`, or return `ArgumentMissing`.
fn require<T>(value: Option<T>, name: &'static str) -> ConfigResult<T> {
    value.ok_or_else(|| Report::from(ConfigError::ArgumentMissing { name }))
}

/// Parse a comma-separated string into `Option<Vec<String>>`.
/// Returns `None` if the input is `None` or results in an empty list.
fn csv_parse(csv: Option<String>) -> Option<Vec<String>> {
    csv.map(|s| {
        s.split(',')
            .map(|t| t.trim().to_owned())
            .filter(|t| !t.is_empty())
            .collect::<Vec<_>>()
    })
    .filter(|v| !v.is_empty())
}

/// Parse a semicolon-separated string into `Option<Vec<String>>`.
/// Semicolons are used instead of commas because SQL queries contain commas.
/// Returns `None` if the input is `None` or results in an empty list.
fn pinned_queries_parse(input: Option<String>) -> Option<Vec<String>> {
    input
        .map(|s| {
            s.split(';')
                .map(|t| t.trim().to_owned())
                .filter(|t| !t.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
}

/// Expand table names into pinned queries (`SELECT * FROM {table}`)
/// and merge with any explicit pinned queries.
fn pinned_tables_expand_and_merge(
    pinned_queries: Option<Vec<String>>,
    pinned_tables: Option<Vec<String>>,
) -> Option<Vec<String>> {
    let expanded = pinned_tables.map(|tables| {
        tables
            .into_iter()
            .map(|t| format!("SELECT * FROM {t}"))
            .collect::<Vec<_>>()
    });

    match (pinned_queries, expanded) {
        (Some(mut queries), Some(tables)) => {
            queries.extend(tables);
            Some(queries)
        }
        (Some(queries), None) => Some(queries),
        (None, Some(tables)) => Some(tables),
        (None, None) => None,
    }
}

/// Raw CLI argument values before merging with config file.
#[derive(Default)]
struct CliArgs {
    origin_host: Option<String>,
    origin_port: Option<u16>,
    origin_user: Option<String>,
    origin_database: Option<String>,
    origin_ssl_mode: Option<SslMode>,
    origin_password: Option<String>,
    replication_host: Option<String>,
    replication_port: Option<u16>,
    replication_user: Option<String>,
    replication_database: Option<String>,
    replication_ssl_mode: Option<SslMode>,
    replication_password: Option<String>,
    cache_host: Option<String>,
    cache_port: Option<u16>,
    cache_user: Option<String>,
    cache_database: Option<String>,
    cdc_publication_name: Option<String>,
    cdc_slot_name: Option<String>,
    listen_socket: Option<SocketAddr>,
    num_workers: Option<usize>,
    cache_size: Option<usize>,
    tls_cert: Option<PathBuf>,
    tls_key: Option<PathBuf>,
    metrics_socket: Option<SocketAddr>,
    log_level: Option<String>,
    cache_policy: Option<CachePolicy>,
    admission_threshold: Option<u32>,
    mv_size_ratio: Option<u32>,
    memo_cache_size: Option<usize>,
    memory_limit: Option<usize>,
    allowed_tables: Option<String>,
    pinned_queries: Option<String>,
    pinned_tables: Option<String>,
    telemetry_off: bool,
}

fn cli_args_parse() -> ConfigResult<(CliArgs, Option<SettingsToml>, Option<PathBuf>)> {
    let mut args = CliArgs::default();
    let mut config = None;
    let mut config_path = None;
    let mut parser = lexopt::Parser::from_env();

    while let Some(arg) = parser.next().map_into_report::<ConfigError>()? {
        match arg {
            Short('c') | Long("config") => {
                let path = arg_string(&mut parser)?;
                let file = read_to_string(&path).map_into_report::<ConfigError>()?;
                config = Some(toml::from_str(&file).map_into_report::<ConfigError>()?);
                config_path = Some(PathBuf::from(path));
            }
            Long("origin_host") => args.origin_host = Some(arg_string(&mut parser)?),
            Long("origin_port") => args.origin_port = Some(arg_parse(&mut parser)?),
            Long("origin_user") => args.origin_user = Some(arg_string(&mut parser)?),
            Long("origin_database") => args.origin_database = Some(arg_string(&mut parser)?),
            Long("origin_ssl_mode") => args.origin_ssl_mode = Some(arg_enum(&mut parser)?),
            Long("origin_password") => args.origin_password = Some(arg_string(&mut parser)?),
            Long("replication_host") => args.replication_host = Some(arg_string(&mut parser)?),
            Long("replication_port") => args.replication_port = Some(arg_parse(&mut parser)?),
            Long("replication_user") => args.replication_user = Some(arg_string(&mut parser)?),
            Long("replication_database") => {
                args.replication_database = Some(arg_string(&mut parser)?)
            }
            Long("replication_ssl_mode") => {
                args.replication_ssl_mode = Some(arg_enum(&mut parser)?)
            }
            Long("replication_password") => {
                args.replication_password = Some(arg_string(&mut parser)?)
            }
            Long("cache_host") => args.cache_host = Some(arg_string(&mut parser)?),
            Long("cache_port") => args.cache_port = Some(arg_parse(&mut parser)?),
            Long("cache_user") => args.cache_user = Some(arg_string(&mut parser)?),
            Long("cache_database") => args.cache_database = Some(arg_string(&mut parser)?),
            Long("cdc_publication_name") => {
                args.cdc_publication_name = Some(arg_string(&mut parser)?)
            }
            Long("cdc_slot_name") => args.cdc_slot_name = Some(arg_string(&mut parser)?),
            Long("listen_socket") => args.listen_socket = Some(arg_parse(&mut parser)?),
            Long("num_workers") => args.num_workers = Some(arg_parse(&mut parser)?),
            Long("cache_size") => args.cache_size = Some(arg_parse(&mut parser)?),
            Long("tls_cert") => args.tls_cert = Some(PathBuf::from(arg_string(&mut parser)?)),
            Long("tls_key") => args.tls_key = Some(PathBuf::from(arg_string(&mut parser)?)),
            Long("metrics_socket") => args.metrics_socket = Some(arg_parse(&mut parser)?),
            Long("log_level") => args.log_level = Some(arg_string(&mut parser)?),
            Long("cache_policy") => args.cache_policy = Some(arg_enum(&mut parser)?),
            Long("admission_threshold") => args.admission_threshold = Some(arg_parse(&mut parser)?),
            Long("mv_size_ratio") => args.mv_size_ratio = Some(arg_parse(&mut parser)?),
            Long("memo_cache_size") => args.memo_cache_size = Some(arg_parse(&mut parser)?),
            Long("memory_limit") => args.memory_limit = Some(arg_parse(&mut parser)?),
            Long("allowed_tables") => args.allowed_tables = Some(arg_string(&mut parser)?),
            Long("pinned_queries") => args.pinned_queries = Some(arg_string(&mut parser)?),
            Long("pinned_tables") => args.pinned_tables = Some(arg_string(&mut parser)?),
            Long("telemetry_off") => args.telemetry_off = true,
            Long("help") => {
                Settings::print_usage_and_exit(parser.bin_name().unwrap_or_default());
            }
            Short(_) | Long(_) | Value(_) => {
                return Err(ConfigError::ArgumentError(Box::new(arg.unexpected())).into());
            }
        }
    }

    Ok((args, config, config_path))
}

/// Resolve telemetry enabled state from CLI > TOML > env var > default (true).
fn telemetry_resolve(cli_off: bool, toml_value: Option<bool>) -> bool {
    if cli_off {
        return false;
    }
    if let Some(v) = toml_value {
        return v;
    }
    if let Ok(v) = std::env::var("PGCACHE_TELEMETRY") {
        return !matches!(v.to_lowercase().as_str(), "off" | "false" | "0");
    }
    true
}

/// Resolve mv_size_ratio from CLI > TOML > env var.
/// Returns None to fall through to `DEFAULT_MV_SIZE_RATIO` in `DynamicConfig::new`.
fn mv_size_ratio_resolve(cli: Option<u32>, toml_value: Option<u32>) -> Option<u32> {
    if let Some(v) = cli {
        return Some(v);
    }
    if let Some(v) = toml_value {
        return Some(v);
    }
    if let Ok(v) = std::env::var("PGCACHE_MV_SIZE_RATIO")
        && let Ok(parsed) = v.parse::<u32>()
    {
        return Some(parsed);
    }
    None
}

/// Resolve memo_cache_size from CLI > TOML > env var.
/// Returns None to fall through to `DEFAULT_MEMO_CACHE_SIZE` in `DynamicConfig::new`.
fn memo_cache_size_resolve(cli: Option<usize>, toml_value: Option<usize>) -> Option<usize> {
    if let Some(v) = cli {
        return Some(v);
    }
    if let Some(v) = toml_value {
        return Some(v);
    }
    if let Ok(v) = std::env::var("PGCACHE_MEMO_CACHE_SIZE")
        && let Ok(parsed) = v.parse::<usize>()
    {
        return Some(parsed);
    }
    None
}

/// Resolve memory_limit from CLI > TOML > env var.
/// Returns None to leave the ceiling at the dynamic 80%-of-RAM default.
fn memory_limit_resolve(cli: Option<usize>, toml_value: Option<usize>) -> Option<usize> {
    if let Some(v) = cli {
        return Some(v);
    }
    if let Some(v) = toml_value {
        return Some(v);
    }
    if let Ok(v) = std::env::var("PGCACHE_MEMORY_LIMIT")
        && let Ok(parsed) = v.parse::<usize>()
    {
        return Some(parsed);
    }
    None
}

fn settings_build(
    args: CliArgs,
    config: Option<SettingsToml>,
    config_path: Option<PathBuf>,
) -> ConfigResult<Settings> {
    let mut settings = if let Some(mut config) = config {
        settings_build_with_config(args, &mut config, config_path)?
    } else {
        settings_build_cli_only(args)?
    };

    // Lowercase CDC names to avoid quoting in postgres
    settings.cdc.publication_name = settings.cdc.publication_name.to_ascii_lowercase();
    settings.cdc.slot_name = settings.cdc.slot_name.to_ascii_lowercase();

    // Capture static config snapshot for restart-required detection
    settings.dynamic.static_snapshot =
        Some(Arc::new(StaticConfigSnapshot::from_settings(&settings)));

    Ok(settings)
}

/// Build settings by merging CLI args over a TOML config file.
fn settings_build_with_config(
    args: CliArgs,
    config: &mut SettingsToml,
    config_path: Option<PathBuf>,
) -> ConfigResult<Settings> {
    let origin_overrides = PgSettingsPartial {
        host: args.origin_host,
        port: args.origin_port,
        user: args.origin_user,
        password: args.origin_password,
        database: args.origin_database,
        ssl_mode: args.origin_ssl_mode,
    };
    let origin = origin_overrides.merge_with(&config.origin);

    let replication = replication_settings_resolve(
        &origin,
        config.replication.take(),
        PgSettingsPartial {
            host: args.replication_host,
            port: args.replication_port,
            user: args.replication_user,
            password: args.replication_password,
            database: args.replication_database,
            ssl_mode: args.replication_ssl_mode,
        },
    );

    let cache_overrides = PgSettingsPartial {
        host: args.cache_host,
        port: args.cache_port,
        user: args.cache_user,
        password: None,
        database: args.cache_database,
        ssl_mode: None,
    };
    let cache = cache_overrides.merge_with(&config.cache);

    let dynamic = DynamicConfig::new(
        args.cache_size.or(config.cache_size),
        args.cache_policy.or(config.cache_policy),
        args.admission_threshold.or(config.admission_threshold),
        csv_parse(args.allowed_tables).or(config.allowed_tables.take()),
        args.log_level.or_else(|| config.log_level.clone()),
        mv_size_ratio_resolve(args.mv_size_ratio, config.mv_size_ratio),
        memo_cache_size_resolve(args.memo_cache_size, config.memo_cache_size),
        memory_limit_resolve(args.memory_limit, config.memory_limit),
    );

    Ok(Settings {
        origin,
        replication,
        cache,
        cdc: CdcSettings {
            publication_name: args
                .cdc_publication_name
                .unwrap_or_else(|| config.cdc.publication_name.clone()),
            slot_name: args
                .cdc_slot_name
                .unwrap_or_else(|| config.cdc.slot_name.clone()),
        },
        listen: ListenSettings {
            socket: args.listen_socket.unwrap_or(config.listen.socket),
        },
        num_workers: args.num_workers.unwrap_or(config.num_workers),
        tls_cert: args.tls_cert.or_else(|| config.tls_cert.clone()),
        tls_key: args.tls_key.or_else(|| config.tls_key.clone()),
        metrics: args
            .metrics_socket
            .map(|socket| MetricsSettings { socket })
            .or_else(|| config.metrics.clone()),
        dynamic: DynamicConfigHandle::new(dynamic, config_path, None),
        pinned_queries: pinned_tables_expand_and_merge(
            pinned_queries_parse(args.pinned_queries).or(config.pinned_queries.take()),
            csv_parse(args.pinned_tables).or(config.pinned_tables.take()),
        ),
        telemetry: telemetry_resolve(args.telemetry_off, config.telemetry),
    })
}

/// Build settings from CLI args alone (no config file). Required fields must be present.
fn settings_build_cli_only(args: CliArgs) -> ConfigResult<Settings> {
    let origin = PgSettings {
        host: require(args.origin_host, "origin_host")?,
        port: require(args.origin_port, "origin_port")?,
        user: require(args.origin_user, "origin_user")?,
        password: args.origin_password,
        database: require(args.origin_database, "origin_database")?,
        ssl_mode: args.origin_ssl_mode.unwrap_or_default(),
    };

    // CLI-only mode: replication defaults to origin, with CLI overrides
    let replication = replication_settings_resolve(
        &origin,
        None,
        PgSettingsPartial {
            host: args.replication_host,
            port: args.replication_port,
            user: args.replication_user,
            password: args.replication_password,
            database: args.replication_database,
            ssl_mode: args.replication_ssl_mode,
        },
    );

    Ok(Settings {
        origin,
        replication,
        cache: PgSettings {
            host: require(args.cache_host, "cache_host")?,
            port: require(args.cache_port, "cache_port")?,
            user: require(args.cache_user, "cache_user")?,
            password: None, // Cache is localhost, uses trust auth
            database: require(args.cache_database, "cache_database")?,
            ssl_mode: SslMode::Disable, // Cache is always localhost, no TLS needed
        },
        cdc: CdcSettings {
            publication_name: require(args.cdc_publication_name, "cdc_publication_name")?,
            slot_name: require(args.cdc_slot_name, "cdc_slot_name")?,
        },
        listen: ListenSettings {
            socket: require(args.listen_socket, "listen_socket")?,
        },
        num_workers: require(args.num_workers, "num_workers")?,
        tls_cert: args.tls_cert,
        tls_key: args.tls_key,
        metrics: args.metrics_socket.map(|socket| MetricsSettings { socket }),
        dynamic: DynamicConfigHandle::new(
            DynamicConfig::new(
                args.cache_size,
                args.cache_policy,
                args.admission_threshold,
                csv_parse(args.allowed_tables),
                args.log_level,
                mv_size_ratio_resolve(args.mv_size_ratio, None),
                memo_cache_size_resolve(args.memo_cache_size, None),
                memory_limit_resolve(args.memory_limit, None),
            ),
            None, // no config file in CLI-only mode
            None, // snapshot set in settings_build
        ),
        pinned_queries: pinned_tables_expand_and_merge(
            pinned_queries_parse(args.pinned_queries),
            csv_parse(args.pinned_tables),
        ),
        telemetry: telemetry_resolve(args.telemetry_off, None),
    })
}

impl Settings {
    pub fn from_args() -> ConfigResult<Settings> {
        let (args, config, config_path) = cli_args_parse()?;
        settings_build(args, config, config_path)
    }

    fn print_usage_and_exit(name: &str) -> ! {
        println!(
            "Usage: {name} -c|--config TOML_FILE --origin_host HOST --origin_port PORT --origin_user USER --origin_database DB \n \
            [--origin_password PASSWORD] [--origin_ssl_mode disable|require|verify-full] \n \
            [--replication_host HOST] [--replication_port PORT] [--replication_user USER] [--replication_database DB] \n \
            [--replication_password PASSWORD] [--replication_ssl_mode disable|require|verify-full] \n \
            --cache_host HOST --cache_port PORT --cache_user USER --cache_database DB \n \
            --cdc_publication_name NAME --cdc_slot_name SLOT_NAME \n \
            --listen_socket IP_AND_PORT \n \
            --num_workers NUMBER \n \
            [--cache_size BYTES] (deprecated: hard disk-size override; default auto-derives from free disk) \n \
            [--cache_policy fifo|clock] (default: clock) \n \
            [--admission_threshold N] (default: 1, clock policy only) \n \
            [--mv_size_ratio N] (default: 10, materialized view size gate) \n \
            [--memo_cache_size BYTES] (default: 64 MiB, in-process hot-result cache budget; 0 disables) \n \
            [--memory_limit BYTES] (default: 80% of detected RAM; absolute ceiling for registration throttling, can only lower) \n \
            [--tls_cert CERT_FILE --tls_key KEY_FILE] \n \
            [--metrics_socket IP_AND_PORT] \n \
            [--allowed_tables TABLE1,TABLE2,...] (restrict caching to these tables) \n \
            [--pinned_queries QUERY1;QUERY2;...] (pin queries in cache at startup, semicolon-separated) \n \
            [--pinned_tables TABLE1,TABLE2,...] (pin SELECT * FROM table for each table) \n \
            [--log_level LEVEL] (e.g., debug, info, pgcache_lib::cache=debug) \n \
            [--telemetry_off] (disable anonymous telemetry)"
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    fn base_settings() -> PgSettings {
        PgSettings {
            host: "base.example.com".to_owned(),
            port: 5432,
            user: "base_user".to_owned(),
            password: Some("base_password".to_owned()),
            database: "base_db".to_owned(),
            ssl_mode: SslMode::Disable,
        }
    }

    #[test]
    fn partial_merge_empty_uses_all_base_values() {
        let base = base_settings();
        let partial = PgSettingsPartial::default();

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "base.example.com");
        assert_eq!(result.port, 5432);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn partial_merge_host_only_override() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "override.example.com");
        assert_eq!(result.port, 5432);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn partial_merge_multiple_fields_override() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            port: Some(6432),
            ssl_mode: Some(SslMode::Require),
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "override.example.com");
        assert_eq!(result.port, 6432);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn partial_merge_all_fields_override() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            port: Some(6432),
            user: Some("override_user".to_owned()),
            password: Some("override_password".to_owned()),
            database: Some("override_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.host, "override.example.com");
        assert_eq!(result.port, 6432);
        assert_eq!(result.user, "override_user");
        assert_eq!(result.password, Some("override_password".to_owned()));
        assert_eq!(result.database, "override_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn partial_merge_password_override_when_base_has_none() {
        let mut base = base_settings();
        base.password = None;

        let partial = PgSettingsPartial {
            password: Some("new_password".to_owned()),
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.password, Some("new_password".to_owned()));
    }

    #[test]
    fn partial_merge_password_inherited_when_partial_has_none() {
        let base = base_settings();
        let partial = PgSettingsPartial {
            host: Some("override.example.com".to_owned()),
            password: None, // Not specified, should inherit from base
            ..Default::default()
        };

        let result = partial.merge_with(&base);

        assert_eq!(result.password, Some("base_password".to_owned()));
    }

    #[test]
    fn partial_merge_both_passwords_none() {
        let mut base = base_settings();
        base.password = None;

        let partial = PgSettingsPartial::default();

        let result = partial.merge_with(&base);

        assert_eq!(result.password, None);
    }

    #[test]
    fn toml_parse_no_replication_section() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
password = "origin_password"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

        assert!(settings.replication.is_none());

        // When replication is None, it should default to origin
        let replication = match settings.replication {
            Some(partial) => partial.merge_with(&settings.origin),
            None => settings.origin,
        };

        assert_eq!(replication.host, "origin.example.com");
        assert_eq!(replication.port, 5432);
        assert_eq!(replication.user, "origin_user");
        assert_eq!(replication.password, Some("origin_password".to_owned()));
        assert_eq!(replication.database, "origin_db");
    }

    #[test]
    fn toml_parse_partial_replication_section() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
password = "secret"
database = "mydb"
ssl_mode = "require"

[replication]
host = "postgres.example.com"
port = 5432

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

        assert!(settings.replication.is_some());
        let partial = settings.replication.as_ref().expect("replication section");

        assert_eq!(partial.host, Some("postgres.example.com".to_owned()));
        assert_eq!(partial.port, Some(5432));
        assert_eq!(partial.user, None);
        assert_eq!(partial.password, None);
        assert_eq!(partial.database, None);
        assert_eq!(partial.ssl_mode, None);

        // After merging, unspecified fields should come from origin
        let replication = partial.merge_with(&settings.origin);

        assert_eq!(replication.host, "postgres.example.com");
        assert_eq!(replication.port, 5432);
        assert_eq!(replication.user, "app_user");
        assert_eq!(replication.password, Some("secret".to_owned()));
        assert_eq!(replication.database, "mydb");
        assert_eq!(replication.ssl_mode, SslMode::Require);
    }

    #[test]
    fn toml_parse_full_replication_section() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
password = "secret"
database = "mydb"

[replication]
host = "postgres.example.com"
port = 5432
user = "replication_user"
password = "replication_secret"
database = "mydb"
ssl_mode = "require"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");
        let partial = settings.replication.as_ref().expect("replication section");

        let replication = partial.merge_with(&settings.origin);

        assert_eq!(replication.host, "postgres.example.com");
        assert_eq!(replication.port, 5432);
        assert_eq!(replication.user, "replication_user");
        assert_eq!(replication.password, Some("replication_secret".to_owned()));
        assert_eq!(replication.database, "mydb");
        assert_eq!(replication.ssl_mode, SslMode::Require);
    }

    #[test]
    fn toml_parse_replication_host_only() {
        let toml_str = r#"
num_workers = 4

[origin]
host = "pgbouncer.example.com"
port = 6432
user = "app_user"
database = "mydb"

[replication]
host = "postgres.example.com"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");
        let partial = settings.replication.as_ref().expect("replication section");

        let replication = partial.merge_with(&settings.origin);

        // Only host is overridden
        assert_eq!(replication.host, "postgres.example.com");
        // All other fields come from origin
        assert_eq!(replication.port, 6432);
        assert_eq!(replication.user, "app_user");
        assert_eq!(replication.password, None);
        assert_eq!(replication.database, "mydb");
        assert_eq!(replication.ssl_mode, SslMode::Disable);
    }

    // ==================== replication_settings_resolve Tests ====================

    #[test]
    fn replication_resolve_no_toml_no_cli_defaults_to_origin() {
        let origin = base_settings();

        let result = replication_settings_resolve(&origin, None, PgSettingsPartial::default());

        assert_eq!(result.host, origin.host);
        assert_eq!(result.port, origin.port);
        assert_eq!(result.user, origin.user);
        assert_eq!(result.password, origin.password);
        assert_eq!(result.database, origin.database);
        assert_eq!(result.ssl_mode, origin.ssl_mode);
    }

    #[test]
    fn replication_resolve_toml_partial_merges_with_origin() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            port: Some(5433),
            ..Default::default()
        };

        let result =
            replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

        assert_eq!(result.host, "replica.example.com");
        assert_eq!(result.port, 5433);
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
        assert_eq!(result.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn replication_resolve_cli_overrides_origin_when_no_toml() {
        let origin = base_settings();
        let cli = PgSettingsPartial {
            host: Some("cli-host.example.com".to_owned()),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, None, cli);

        assert_eq!(result.host, "cli-host.example.com");
        assert_eq!(result.port, origin.port);
        assert_eq!(result.user, origin.user);
        assert_eq!(result.password, origin.password);
        assert_eq!(result.database, origin.database);
    }

    #[test]
    fn replication_resolve_cli_overrides_toml_partial() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("toml-replica.example.com".to_owned()),
            port: Some(5433),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            host: Some("cli-replica.example.com".to_owned()),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        // CLI host wins over TOML host
        assert_eq!(result.host, "cli-replica.example.com");
        // TOML port wins over origin port (CLI didn't specify)
        assert_eq!(result.port, 5433);
        // Remaining fields from origin
        assert_eq!(result.user, "base_user");
        assert_eq!(result.password, Some("base_password".to_owned()));
        assert_eq!(result.database, "base_db");
    }

    #[test]
    fn replication_resolve_cli_overrides_all_fields() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("toml-replica.example.com".to_owned()),
            user: Some("toml_user".to_owned()),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            host: Some("cli-host.example.com".to_owned()),
            port: Some(6432),
            user: Some("cli_user".to_owned()),
            password: Some("cli_password".to_owned()),
            database: Some("cli_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        assert_eq!(result.host, "cli-host.example.com");
        assert_eq!(result.port, 6432);
        assert_eq!(result.user, "cli_user");
        assert_eq!(result.password, Some("cli_password".to_owned()));
        assert_eq!(result.database, "cli_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn replication_resolve_cli_password_overrides_toml_password() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            password: Some("toml_password".to_owned()),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            password: Some("cli_password".to_owned()),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        assert_eq!(result.password, Some("cli_password".to_owned()));
    }

    #[test]
    fn replication_resolve_cli_password_not_set_preserves_toml_password() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            password: Some("toml_password".to_owned()),
            ..Default::default()
        };

        let result =
            replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

        assert_eq!(result.password, Some("toml_password".to_owned()));
    }

    #[test]
    fn replication_resolve_full_toml_with_no_cli_uses_toml() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            port: Some(5433),
            user: Some("repl_user".to_owned()),
            password: Some("repl_pass".to_owned()),
            database: Some("repl_db".to_owned()),
            ssl_mode: Some(SslMode::Require),
        };

        let result =
            replication_settings_resolve(&origin, Some(toml_partial), PgSettingsPartial::default());

        assert_eq!(result.host, "replica.example.com");
        assert_eq!(result.port, 5433);
        assert_eq!(result.user, "repl_user");
        assert_eq!(result.password, Some("repl_pass".to_owned()));
        assert_eq!(result.database, "repl_db");
        assert_eq!(result.ssl_mode, SslMode::Require);
    }

    #[test]
    fn replication_resolve_cli_port_only_with_toml_host() {
        let origin = base_settings();
        let toml_partial = PgSettingsPartial {
            host: Some("toml-host.example.com".to_owned()),
            ..Default::default()
        };
        let cli = PgSettingsPartial {
            port: Some(6432),
            ..Default::default()
        };

        let result = replication_settings_resolve(&origin, Some(toml_partial), cli);

        // TOML host preserved
        assert_eq!(result.host, "toml-host.example.com");
        // CLI port applied
        assert_eq!(result.port, 6432);
        // Origin fills the rest
        assert_eq!(result.user, "base_user");
        assert_eq!(result.database, "base_db");
    }

    // ==================== settings_build Tests ====================

    fn base_toml_config() -> SettingsToml {
        SettingsToml {
            origin: PgSettings {
                host: "origin.example.com".to_owned(),
                port: 5432,
                user: "origin_user".to_owned(),
                password: Some("origin_pass".to_owned()),
                database: "origin_db".to_owned(),
                ssl_mode: SslMode::Disable,
            },
            replication: None,
            cache: PgSettings {
                host: "localhost".to_owned(),
                port: 5433,
                user: "cache_user".to_owned(),
                password: None,
                database: "cache_db".to_owned(),
                ssl_mode: SslMode::Disable,
            },
            cdc: CdcSettings {
                publication_name: "pub".to_owned(),
                slot_name: "slot".to_owned(),
            },
            listen: ListenSettings {
                socket: "127.0.0.1:6432".parse().expect("valid socket addr"),
            },
            num_workers: 4,
            cache_size: None,
            tls_cert: None,
            tls_key: None,
            metrics: None,
            log_level: None,
            cache_policy: None,
            admission_threshold: None,
            mv_size_ratio: None,
            memo_cache_size: None,
            memory_limit: None,
            allowed_tables: None,
            pinned_queries: None,
            pinned_tables: None,
            telemetry: None,
        }
    }

    #[test]
    fn settings_build_no_replication_defaults_to_origin() {
        let config = base_toml_config();
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        assert_eq!(settings.replication.host, "origin.example.com");
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
    }

    #[test]
    fn settings_build_toml_replication_partial_merges_with_origin() {
        let mut config = base_toml_config();
        config.replication = Some(PgSettingsPartial {
            host: Some("replica.example.com".to_owned()),
            ..Default::default()
        });
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        assert_eq!(settings.replication.host, "replica.example.com");
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
    }

    #[test]
    fn settings_build_cli_replication_overrides_no_toml_section() {
        let config = base_toml_config();
        let args = CliArgs {
            replication_host: Some("cli-replica.example.com".to_owned()),
            replication_port: Some(6432),
            ..Default::default()
        };

        let settings = settings_build(args, Some(config), None).expect("build settings");

        assert_eq!(settings.replication.host, "cli-replica.example.com");
        assert_eq!(settings.replication.port, 6432);
        // Remaining fields cascade from origin
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
    }

    #[test]
    fn settings_build_cli_replication_overrides_toml_replication() {
        let mut config = base_toml_config();
        config.replication = Some(PgSettingsPartial {
            host: Some("toml-replica.example.com".to_owned()),
            port: Some(5433),
            ..Default::default()
        });
        let args = CliArgs {
            replication_host: Some("cli-replica.example.com".to_owned()),
            ..Default::default()
        };

        let settings = settings_build(args, Some(config), None).expect("build settings");

        // CLI host wins over TOML host
        assert_eq!(settings.replication.host, "cli-replica.example.com");
        // TOML port preserved (CLI didn't specify)
        assert_eq!(settings.replication.port, 5433);
        // Origin fills unspecified fields
        assert_eq!(settings.replication.user, "origin_user");
    }

    #[test]
    fn settings_build_cli_origin_override_cascades_to_replication() {
        let config = base_toml_config();
        let args = CliArgs {
            origin_host: Some("cli-origin.example.com".to_owned()),
            ..Default::default()
        };

        let settings = settings_build(args, Some(config), None).expect("build settings");

        // Origin was overridden by CLI
        assert_eq!(settings.origin.host, "cli-origin.example.com");
        // Replication inherits the CLI-overridden origin (no TOML replication section)
        assert_eq!(settings.replication.host, "cli-origin.example.com");
    }

    #[test]
    fn settings_build_cdc_names_lowercased() {
        let mut config = base_toml_config();
        config.cdc.publication_name = "MY_PUB".to_owned();
        config.cdc.slot_name = "MY_SLOT".to_owned();
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        assert_eq!(settings.cdc.publication_name, "my_pub");
        assert_eq!(settings.cdc.slot_name, "my_slot");
    }

    // ==================== settings_build CLI-only Tests ====================

    /// All required CLI fields populated, no config file.
    fn base_cli_args() -> CliArgs {
        CliArgs {
            origin_host: Some("origin.example.com".to_owned()),
            origin_port: Some(5432),
            origin_user: Some("origin_user".to_owned()),
            origin_password: Some("origin_pass".to_owned()),
            origin_database: Some("origin_db".to_owned()),
            cache_host: Some("localhost".to_owned()),
            cache_port: Some(5433),
            cache_user: Some("cache_user".to_owned()),
            cache_database: Some("cache_db".to_owned()),
            cdc_publication_name: Some("pub".to_owned()),
            cdc_slot_name: Some("slot".to_owned()),
            listen_socket: Some("127.0.0.1:6432".parse().expect("valid socket addr")),
            num_workers: Some(4),
            ..Default::default()
        }
    }

    #[test]
    fn settings_build_cli_only_replication_defaults_to_origin() {
        let args = base_cli_args();

        let settings = settings_build(args, None, None).expect("build settings");

        assert_eq!(settings.replication.host, "origin.example.com");
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
        assert_eq!(settings.replication.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn settings_build_cli_only_replication_host_override() {
        let args = CliArgs {
            replication_host: Some("replica.example.com".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None, None).expect("build settings");

        assert_eq!(settings.replication.host, "replica.example.com");
        // Remaining fields inherited from origin
        assert_eq!(settings.replication.port, 5432);
        assert_eq!(settings.replication.user, "origin_user");
        assert_eq!(
            settings.replication.password,
            Some("origin_pass".to_owned())
        );
        assert_eq!(settings.replication.database, "origin_db");
    }

    #[test]
    fn settings_build_cli_only_replication_all_fields_override() {
        let args = CliArgs {
            replication_host: Some("replica.example.com".to_owned()),
            replication_port: Some(6432),
            replication_user: Some("repl_user".to_owned()),
            replication_password: Some("repl_pass".to_owned()),
            replication_database: Some("repl_db".to_owned()),
            replication_ssl_mode: Some(SslMode::Require),
            ..base_cli_args()
        };

        let settings = settings_build(args, None, None).expect("build settings");

        assert_eq!(settings.replication.host, "replica.example.com");
        assert_eq!(settings.replication.port, 6432);
        assert_eq!(settings.replication.user, "repl_user");
        assert_eq!(settings.replication.password, Some("repl_pass".to_owned()));
        assert_eq!(settings.replication.database, "repl_db");
        assert_eq!(settings.replication.ssl_mode, SslMode::Require);
        // Origin unchanged
        assert_eq!(settings.origin.host, "origin.example.com");
        assert_eq!(settings.origin.ssl_mode, SslMode::Disable);
    }

    #[test]
    fn settings_build_cli_only_missing_origin_host_errors() {
        let mut args = base_cli_args();
        args.origin_host = None;

        let err = settings_build(args, None, None).expect_err("missing origin_host");
        assert!(err.to_string().contains("origin_host"));
    }

    #[test]
    fn settings_build_cli_only_defaults() {
        let args = base_cli_args();

        let settings = settings_build(args, None, None).expect("build settings");

        assert_eq!(settings.origin.ssl_mode, SslMode::Disable);
        let dynamic = settings.dynamic.load();
        assert_eq!(dynamic.cache_policy, CachePolicy::Clock);
        assert_eq!(dynamic.admission_threshold, 1);
        assert_eq!(settings.cache.ssl_mode, SslMode::Disable);
        assert_eq!(settings.cache.password, None);
    }

    #[test]
    fn settings_build_cli_only_cdc_names_lowercased() {
        let args = CliArgs {
            cdc_publication_name: Some("MY_PUB".to_owned()),
            cdc_slot_name: Some("MY_SLOT".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None, None).expect("build settings");

        assert_eq!(settings.cdc.publication_name, "my_pub");
        assert_eq!(settings.cdc.slot_name, "my_slot");
    }

    // ==================== pinned_queries Tests ====================

    #[test]
    fn settings_build_pinned_queries_default_none() {
        let args = base_cli_args();
        let settings = settings_build(args, None, None).expect("build settings");
        assert!(settings.pinned_queries.is_none());
    }

    #[test]
    fn settings_build_pinned_queries_from_toml() {
        let mut config = base_toml_config();
        config.pinned_queries = Some(vec![
            "SELECT * FROM users".to_owned(),
            "SELECT * FROM orders".to_owned(),
        ]);
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT * FROM users");
        assert_eq!(pinned[1], "SELECT * FROM orders");
    }

    #[test]
    fn settings_build_pinned_queries_cli_semicolon() {
        let args = CliArgs {
            pinned_queries: Some("SELECT id, name FROM a;SELECT id, name FROM b".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None, None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT id, name FROM a");
        assert_eq!(pinned[1], "SELECT id, name FROM b");
    }

    #[test]
    fn settings_build_pinned_queries_cli_overrides_toml() {
        let mut config = base_toml_config();
        config.pinned_queries = Some(vec!["SELECT * FROM toml_table".to_owned()]);
        let args = CliArgs {
            pinned_queries: Some("SELECT * FROM cli_table".to_owned()),
            ..CliArgs::default()
        };

        let settings = settings_build(args, Some(config), None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 1);
        assert_eq!(pinned[0], "SELECT * FROM cli_table");
    }

    #[test]
    fn toml_parse_pinned_queries() {
        let toml_str = r#"
num_workers = 4

pinned_queries = [
    "SELECT * FROM settings",
    "SELECT id, name FROM lookup",
]

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

        let pinned = settings.pinned_queries.expect("pinned_queries present");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT * FROM settings");
        assert_eq!(pinned[1], "SELECT id, name FROM lookup");
    }

    // ==================== pinned_tables Tests ====================

    #[test]
    fn settings_build_pinned_tables_expands_to_queries() {
        let mut config = base_toml_config();
        config.pinned_tables = Some(vec!["settings".to_owned(), "products".to_owned()]);
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT * FROM settings");
        assert_eq!(pinned[1], "SELECT * FROM products");
    }

    #[test]
    fn settings_build_pinned_tables_merged_with_pinned_queries() {
        let mut config = base_toml_config();
        config.pinned_queries = Some(vec!["SELECT id, name FROM users".to_owned()]);
        config.pinned_tables = Some(vec!["settings".to_owned()]);
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT id, name FROM users");
        assert_eq!(pinned[1], "SELECT * FROM settings");
    }

    #[test]
    fn settings_build_pinned_tables_cli_csv() {
        let args = CliArgs {
            pinned_tables: Some("settings,products".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None, None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT * FROM settings");
        assert_eq!(pinned[1], "SELECT * FROM products");
    }

    #[test]
    fn settings_build_pinned_tables_schema_qualified() {
        let mut config = base_toml_config();
        config.pinned_tables = Some(vec!["analytics.events".to_owned()]);
        let args = CliArgs::default();

        let settings = settings_build(args, Some(config), None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 1);
        assert_eq!(pinned[0], "SELECT * FROM analytics.events");
    }

    #[test]
    fn settings_build_pinned_tables_cli_merges_with_pinned_queries_cli() {
        let args = CliArgs {
            pinned_queries: Some("SELECT id FROM users".to_owned()),
            pinned_tables: Some("settings".to_owned()),
            ..base_cli_args()
        };

        let settings = settings_build(args, None, None).expect("build settings");

        let pinned = settings.pinned_queries.expect("pinned queries set");
        assert_eq!(pinned.len(), 2);
        assert_eq!(pinned[0], "SELECT id FROM users");
        assert_eq!(pinned[1], "SELECT * FROM settings");
    }

    #[test]
    fn toml_parse_pinned_tables() {
        let toml_str = r#"
num_workers = 4

pinned_tables = ["settings", "products"]

[origin]
host = "origin.example.com"
port = 5432
user = "origin_user"
database = "origin_db"

[cache]
host = "localhost"
port = 5433
user = "cache_user"
database = "cache_db"

[cdc]
publication_name = "test_pub"
slot_name = "test_slot"

[listen]
socket = "127.0.0.1:5434"
"#;

        let settings: SettingsToml = toml::from_str(toml_str).expect("parse TOML");

        let tables = settings.pinned_tables.expect("pinned_tables present");
        assert_eq!(tables.len(), 2);
        assert_eq!(tables[0], "settings");
        assert_eq!(tables[1], "products");
    }

    // ==================== DynamicConfigPatch Tests ====================

    fn base_dynamic_config() -> DynamicConfig {
        DynamicConfig::new(
            Some(1_000_000),
            Some(CachePolicy::Clock),
            Some(2),
            Some(vec!["public.users".to_owned()]),
            Some("info".to_owned()),
            None,
            None,
            None,
        )
    }

    #[test]
    fn config_patch_apply_empty_preserves_current() {
        let current = base_dynamic_config();
        let patch = DynamicConfigPatch {
            cache_size: None,
            cache_policy: None,
            admission_threshold: None,
            allowed_tables: None,
            log_level: None,
            mv_size_ratio: None,
            memo_cache_size: None,
            memory_limit: None,
        };
        let result = patch.apply(&current);
        assert_eq!(result.cache_size, Some(1_000_000));
        assert_eq!(result.cache_policy, CachePolicy::Clock);
        assert_eq!(result.admission_threshold, 2);
        assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));
        assert_eq!(result.log_level, Some("info".to_owned()));
        assert_eq!(result.mv_size_ratio, DEFAULT_MV_SIZE_RATIO);
    }

    #[test]
    fn config_patch_apply_set_values() {
        let current = base_dynamic_config();
        let patch = DynamicConfigPatch {
            cache_size: Some(Some(2_000_000)),
            cache_policy: Some(CachePolicy::Fifo),
            admission_threshold: Some(5),
            allowed_tables: Some(Some(vec!["orders".to_owned()])),
            log_level: Some(Some("debug".to_owned())),
            mv_size_ratio: Some(25),
            memo_cache_size: Some(8_000_000),
            memory_limit: None,
        };
        let result = patch.apply(&current);
        assert_eq!(result.cache_size, Some(2_000_000));
        assert_eq!(result.cache_policy, CachePolicy::Fifo);
        assert_eq!(result.admission_threshold, 5);
        assert_eq!(result.memo_cache_size, 8_000_000);
        assert_eq!(result.allowed_tables, Some(vec!["orders".to_owned()]));
        assert_eq!(result.log_level, Some("debug".to_owned()));
        assert_eq!(result.mv_size_ratio, 25);
    }

    #[test]
    fn config_patch_apply_unset_optional_fields() {
        let current = base_dynamic_config();
        let patch = DynamicConfigPatch {
            cache_size: Some(None),
            cache_policy: None,
            admission_threshold: None,
            allowed_tables: Some(None),
            log_level: Some(None),
            mv_size_ratio: None,
            memo_cache_size: None,
            memory_limit: None,
        };
        let result = patch.apply(&current);
        assert_eq!(result.cache_size, None);
        assert_eq!(result.allowed_tables, None);
        assert!(result.allowed_tables_parsed.is_none());
        assert_eq!(result.log_level, None);
    }

    #[test]
    fn config_patch_json_deserialize() {
        let json = r#"{"cache_size": 500, "admission_threshold": 3}"#;
        let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
        assert_eq!(patch.cache_size, Some(Some(500)));
        assert_eq!(patch.admission_threshold, Some(3));
        assert!(patch.cache_policy.is_none());
        assert!(patch.allowed_tables.is_none());
        assert!(patch.log_level.is_none());
    }

    #[test]
    fn config_patch_json_null_unsets() {
        let json = r#"{"cache_size": null, "log_level": null}"#;
        let patch: DynamicConfigPatch = serde_json::from_str(json).expect("parse JSON");
        assert_eq!(patch.cache_size, Some(None));
        assert_eq!(patch.log_level, Some(None));
    }

    #[test]
    fn config_file_toml_round_trip() {
        let toml_content = r#"# Main config
num_workers = 4
cache_size = 1000000
cache_policy = "clock"
admission_threshold = 2
log_level = "info"
allowed_tables = ["public.users"]

[origin]
host = "localhost"
port = 5432
user = "test"
database = "testdb"

[cache]
host = "localhost"
port = 5433
user = "test"
database = "cachedb"

[cdc]
publication_name = "pub"
slot_name = "slot"

[listen]
socket = "127.0.0.1:6432"
"#;

        let dir = std::env::temp_dir().join("pgcache_test_config");
        let _ = fs::create_dir_all(&dir);
        let path = dir.join("test_round_trip.toml");
        fs::write(&path, toml_content).expect("write test TOML");

        // Apply a patch
        let patch = DynamicConfigPatch {
            cache_size: Some(Some(2_000_000)),
            cache_policy: Some(CachePolicy::Fifo),
            admission_threshold: None,
            allowed_tables: None,
            log_level: Some(None),
            mv_size_ratio: None,
            memo_cache_size: None,
            memory_limit: None,
        };
        config_file_dynamic_update(&path, &patch).expect("update TOML");

        // Re-read to verify the changes
        let result = config_file_dynamic_extract(&path).expect("extract after update");
        assert_eq!(result.cache_size, Some(2_000_000));
        assert_eq!(result.cache_policy, CachePolicy::Fifo);
        assert_eq!(result.admission_threshold, 2); // unchanged
        assert!(result.log_level.is_none()); // unset
        assert_eq!(result.allowed_tables, Some(vec!["public.users".to_owned()]));

        let updated = fs::read_to_string(&path).expect("read updated TOML");
        assert!(updated.contains("# Main config"));
        assert!(updated.contains("cache_size = 2000000"));
        assert!(updated.contains(r#"cache_policy = "fifo""#));
        assert!(!updated.contains("log_level")); // removed

        let _ = fs::remove_file(&path);
    }
}
