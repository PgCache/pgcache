use std::error::Error;
use std::fmt;
use std::fs::read_to_string;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use lexopt::prelude::*;
use rootcause::Report;

use super::dynamic::{DynamicConfig, DynamicConfigHandle, StaticBaseline};
use super::toml_file::config_file_create;
use super::{
    BoxedError, CachePolicy, CdcSettings, ConfigError, ConfigResult, DEFAULT_PUBLICATION_NAME,
    DEFAULT_SLOT_NAME, ListenSettings, MetricsSettings, PgSettings, PgSettingsPartial,
    PreflightSettings, RunMode, Settings, SettingsToml, SslMode, StaticConfigSnapshot, TableFilter,
    toml_parse_error,
};
use crate::result::MapIntoReport;

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
pub(super) fn replication_settings_resolve(
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
    s.parse().map_err(|e: T::Err| {
        Report::from(ConfigError::ArgumentError(BoxedError::new(e.to_string())))
    })
}

/// Require an `Option<T>` to be `Some`, or return `ArgumentMissing`.
fn require<T>(value: Option<T>, name: &'static str) -> ConfigResult<T> {
    value.ok_or_else(|| Report::from(ConfigError::ArgumentMissing { name }))
}

/// Split `input` on `separator` into trimmed, non-empty entries; `None` if the
/// input is `None` or leaves nothing.
fn separated_parse(input: Option<String>, separator: char) -> Option<Vec<String>> {
    input
        .map(|s| {
            s.split(separator)
                .map(|t| t.trim().to_owned())
                .filter(|t| !t.is_empty())
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
}

/// Parse a comma-separated list.
pub(super) fn csv_parse(csv: Option<String>) -> Option<Vec<String>> {
    separated_parse(csv, ',')
}

/// Parse a semicolon-separated query list. Semicolons are used instead of
/// commas because SQL queries contain commas.
pub(super) fn pinned_queries_parse(input: Option<String>) -> Option<Vec<String>> {
    separated_parse(input, ';')
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
pub(super) struct CliArgs {
    pub(super) origin: PgSettingsPartial,
    pub(super) replication: PgSettingsPartial,
    /// Only host, port, user and database have flags; the cache is local,
    /// with trust auth and no TLS.
    pub(super) cache: PgSettingsPartial,
    pub(super) cdc_publication_name: Option<String>,
    pub(super) cdc_slot_name: Option<String>,
    pub(super) listen_socket: Option<SocketAddr>,
    pub(super) num_workers: Option<usize>,
    pub(super) population_workers_min: Option<usize>,
    pub(super) population_workers_max: Option<usize>,
    pub(super) role_poll_interval: Option<u64>,
    pub(super) tls_cert: Option<PathBuf>,
    pub(super) tls_key: Option<PathBuf>,
    pub(super) metrics_socket: Option<SocketAddr>,
    pub(super) dynamic: DynamicArgs,
    pub(super) pinned_queries: Option<String>,
    pub(super) pinned_tables: Option<String>,
    pub(super) telemetry_off: bool,
    pub(super) check: bool,
    pub(super) cleanup: bool,
    pub(super) purge: bool,
    pub(super) healthcheck: bool,
    pub(super) origin_locale: bool,
}

/// CLI values for the runtime-adjustable settings ([`DynamicConfig`]). Kept
/// after startup so a config reload resolves the file the way startup did.
#[derive(Clone, Default)]
pub(super) struct DynamicArgs {
    pub(super) cache_size: Option<usize>,
    pub(super) cache_policy: Option<CachePolicy>,
    pub(super) admission_threshold: Option<u32>,
    pub(super) allowed_tables: Option<String>,
    pub(super) excluded_tables: Option<String>,
    pub(super) log_level: Option<String>,
    pub(super) mv_size_ratio: Option<u32>,
    pub(super) mv_compute_min_rows: Option<u64>,
    pub(super) memo_cache_size: Option<usize>,
    pub(super) memory_limit: Option<usize>,
    pub(super) disk_limit: Option<usize>,
}

/// TOML values for the runtime-adjustable settings; all `None` without a
/// config file.
#[derive(Default)]
struct DynamicToml {
    cache_size: Option<usize>,
    cache_policy: Option<CachePolicy>,
    admission_threshold: Option<u32>,
    allowed_tables: Option<Vec<String>>,
    excluded_tables: Option<Vec<String>>,
    log_level: Option<String>,
    mv_size_ratio: Option<u32>,
    mv_compute_min_rows: Option<u64>,
    memo_cache_size: Option<usize>,
    memory_limit: Option<usize>,
    disk_limit: Option<usize>,
}

impl DynamicToml {
    fn from_config(config: &mut SettingsToml) -> Self {
        Self {
            cache_size: config.cache_size,
            cache_policy: config.cache_policy,
            admission_threshold: config.admission_threshold,
            allowed_tables: config.allowed_tables.take(),
            excluded_tables: config.excluded_tables.take(),
            log_level: config.log_level.clone(),
            mv_size_ratio: config.mv_size_ratio,
            mv_compute_min_rows: config.mv_compute_min_rows,
            memo_cache_size: config.memo_cache_size,
            memory_limit: config.memory_limit,
            disk_limit: config.disk_limit,
        }
    }
}

/// Which connection a `--<target>_<field>` flag configures.
#[derive(Clone, Copy, PartialEq, Eq)]
enum PgTarget {
    Origin,
    Replication,
    Cache,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PgField {
    Host,
    Port,
    User,
    Database,
    SslMode,
    Password,
}

/// The connection setting a flag such as `origin_host` names, or `None`. The
/// cache takes no password or TLS mode flag.
fn pg_flag(name: &str) -> Option<(PgTarget, PgField)> {
    let (target, field) = if let Some(field) = name.strip_prefix("origin_") {
        (PgTarget::Origin, field)
    } else if let Some(field) = name.strip_prefix("replication_") {
        (PgTarget::Replication, field)
    } else {
        (PgTarget::Cache, name.strip_prefix("cache_")?)
    };
    let field = match field {
        "host" => PgField::Host,
        "port" => PgField::Port,
        "user" => PgField::User,
        "database" => PgField::Database,
        "ssl_mode" => PgField::SslMode,
        "password" => PgField::Password,
        _ => return None,
    };
    let cache_only_local = matches!(field, PgField::SslMode | PgField::Password);
    if target == PgTarget::Cache && cache_only_local {
        return None;
    }
    Some((target, field))
}

#[derive(Clone, Copy)]
enum DynamicField {
    CacheSize,
    CachePolicy,
    AdmissionThreshold,
    MvSizeRatio,
    MvComputeMinRows,
    MemoCacheSize,
    MemoryLimit,
    DiskLimit,
    AllowedTables,
    ExcludedTables,
    LogLevel,
}

/// A flag handled by a grouped setter rather than its own match arm. Resolved
/// from the flag name before the value is read, so no borrow of the parser
/// outlives the lookup.
enum FlagRoute {
    Pg(PgTarget, PgField),
    Dynamic(DynamicField),
}

fn flag_route(name: &str) -> Option<FlagRoute> {
    if let Some((target, field)) = pg_flag(name) {
        return Some(FlagRoute::Pg(target, field));
    }
    let field = match name {
        "cache_size" => DynamicField::CacheSize,
        "cache_policy" => DynamicField::CachePolicy,
        "admission_threshold" => DynamicField::AdmissionThreshold,
        "mv_size_ratio" => DynamicField::MvSizeRatio,
        "mv_compute_min_rows" => DynamicField::MvComputeMinRows,
        "memo_cache_size" => DynamicField::MemoCacheSize,
        "memory_limit" => DynamicField::MemoryLimit,
        "disk_limit" => DynamicField::DiskLimit,
        "allowed_tables" => DynamicField::AllowedTables,
        "excluded_tables" => DynamicField::ExcludedTables,
        "log_level" => DynamicField::LogLevel,
        _ => return None,
    };
    Some(FlagRoute::Dynamic(field))
}

/// Flag names for the required fields of a connection built from CLI args
/// alone.
struct PgRequiredFlags {
    host: &'static str,
    port: &'static str,
    user: &'static str,
    database: &'static str,
}

const ORIGIN_REQUIRED_FLAGS: PgRequiredFlags = PgRequiredFlags {
    host: "origin_host",
    port: "origin_port",
    user: "origin_user",
    database: "origin_database",
};

const CACHE_REQUIRED_FLAGS: PgRequiredFlags = PgRequiredFlags {
    host: "cache_host",
    port: "cache_port",
    user: "cache_user",
    database: "cache_database",
};

const POPULATION_WORKERS_MIN_ENV: &str = "PGCACHE_POPULATION_WORKERS_MIN";
const POPULATION_WORKERS_MAX_ENV: &str = "PGCACHE_POPULATION_WORKERS_MAX";
const ROLE_POLL_INTERVAL_ENV: &str = "PGCACHE_ROLE_POLL_INTERVAL";

/// Seconds between role catalog polls when nothing sets them.
const ROLE_POLL_INTERVAL_DEFAULT_SECS: u64 = 5;

/// The role poll interval, CLI over TOML over env var, as a duration.
fn role_poll_interval_resolve(cli: Option<u64>, toml_value: Option<u64>) -> Duration {
    Duration::from_secs(
        env_override_resolve(cli, toml_value, ROLE_POLL_INTERVAL_ENV)
            .unwrap_or(ROLE_POLL_INTERVAL_DEFAULT_SECS),
    )
}

impl PgSettingsPartial {
    /// Set one field from the flag's value.
    fn flag_set(&mut self, field: PgField, parser: &mut lexopt::Parser) -> ConfigResult<()> {
        match field {
            PgField::Host => self.host = Some(arg_string(parser)?),
            PgField::Port => self.port = Some(arg_parse(parser)?),
            PgField::User => self.user = Some(arg_string(parser)?),
            PgField::Database => self.database = Some(arg_string(parser)?),
            PgField::SslMode => self.ssl_mode = Some(arg_enum(parser)?),
            PgField::Password => self.password = Some(arg_string(parser)?),
        }
        Ok(())
    }

    /// Settings from CLI args alone; host, port, user and database are required.
    fn require(self, flags: &PgRequiredFlags) -> ConfigResult<PgSettings> {
        Ok(PgSettings {
            host: require(self.host, flags.host)?,
            port: require(self.port, flags.port)?,
            user: require(self.user, flags.user)?,
            password: self.password,
            database: require(self.database, flags.database)?,
            ssl_mode: self.ssl_mode.unwrap_or_default(),
        })
    }
}

/// The flags that take part in resolving the static settings. Kept after
/// startup so `restart_required` resolves an edited config file the way
/// startup did.
#[derive(Clone)]
pub(super) struct StaticFlags {
    origin: PgSettingsPartial,
    cache: PgSettingsPartial,
    listen_socket: Option<SocketAddr>,
    num_workers: Option<usize>,
    cdc_publication_name: Option<String>,
    cdc_slot_name: Option<String>,
}

impl CliArgs {
    pub(super) fn static_flags(&self) -> StaticFlags {
        StaticFlags {
            origin: self.origin.clone(),
            cache: self.cache.clone(),
            listen_socket: self.listen_socket,
            num_workers: self.num_workers,
            cdc_publication_name: self.cdc_publication_name.clone(),
            cdc_slot_name: self.cdc_slot_name.clone(),
        }
    }

    fn pg_mut(&mut self, target: PgTarget) -> &mut PgSettingsPartial {
        match target {
            PgTarget::Origin => &mut self.origin,
            PgTarget::Replication => &mut self.replication,
            PgTarget::Cache => &mut self.cache,
        }
    }
}

/// Where the config file stands once the flags are read.
enum ConfigFile {
    /// No `--config`.
    Unset,
    Loaded {
        path: PathBuf,
        config: Box<SettingsToml>,
    },
    /// `--config_create` and nothing at the path yet.
    ToCreate { path: PathBuf },
}

impl ConfigFile {
    fn loaded(&self) -> Option<&SettingsToml> {
        match self {
            ConfigFile::Loaded { config, .. } => Some(config),
            ConfigFile::Unset | ConfigFile::ToCreate { .. } => None,
        }
    }

    fn into_loaded(self) -> Option<SettingsToml> {
        match self {
            ConfigFile::Loaded { config, .. } => Some(*config),
            ConfigFile::Unset | ConfigFile::ToCreate { .. } => None,
        }
    }
}

fn cli_args_parse() -> ConfigResult<(CliArgs, ConfigFile)> {
    let mut args = CliArgs::default();
    let mut config_path = None;
    let mut config_create = false;
    let mut parser = lexopt::Parser::from_env();

    while let Some(arg) = parser.next().map_into_report::<ConfigError>()? {
        let routed = match &arg {
            Long(name) => flag_route(name),
            Short(_) | Value(_) => None,
        };
        match routed {
            Some(FlagRoute::Pg(target, field)) => {
                args.pg_mut(target).flag_set(field, &mut parser)?;
                continue;
            }
            Some(FlagRoute::Dynamic(field)) => {
                args.dynamic.flag_set(field, &mut parser)?;
                continue;
            }
            None => {}
        }
        match arg {
            Short('c') | Long("config") => {
                config_path = Some(PathBuf::from(arg_string(&mut parser)?));
            }
            Long("config_create") => config_create = true,
            Long("cdc_publication_name") => {
                args.cdc_publication_name = Some(arg_string(&mut parser)?)
            }
            Long("cdc_slot_name") => args.cdc_slot_name = Some(arg_string(&mut parser)?),
            Long("listen_socket") => args.listen_socket = Some(arg_parse(&mut parser)?),
            Long("num_workers") => args.num_workers = Some(arg_parse(&mut parser)?),
            Long("population_workers_min") => {
                args.population_workers_min = Some(arg_parse(&mut parser)?);
            }
            Long("population_workers_max") => {
                args.population_workers_max = Some(arg_parse(&mut parser)?);
            }
            Long("role_poll_interval") => {
                args.role_poll_interval = Some(arg_parse(&mut parser)?);
            }
            Long("tls_cert") => args.tls_cert = Some(PathBuf::from(arg_string(&mut parser)?)),
            Long("tls_key") => args.tls_key = Some(PathBuf::from(arg_string(&mut parser)?)),
            Long("metrics_socket") => args.metrics_socket = Some(arg_parse(&mut parser)?),
            Long("pinned_queries") => args.pinned_queries = Some(arg_string(&mut parser)?),
            Long("pinned_tables") => args.pinned_tables = Some(arg_string(&mut parser)?),
            Long("telemetry_off") => args.telemetry_off = true,
            Long("check") => args.check = true,
            Long("cleanup") => args.cleanup = true,
            Long("purge") => args.purge = true,
            Long("healthcheck") => args.healthcheck = true,
            Long("origin_locale") => args.origin_locale = true,
            Long("help") => {
                Settings::print_usage_and_exit(parser.bin_name().unwrap_or_default());
            }
            Long("version") => {
                println!("pgcache {}", env!("CARGO_PKG_VERSION"));
                std::process::exit(0);
            }
            Short(_) | Long(_) | Value(_) => {
                return Err(ConfigError::ArgumentError(BoxedError::new(arg.unexpected())).into());
            }
        }
    }

    let config_file = config_read(config_path, config_create)?;
    Ok((args, config_file))
}

impl DynamicArgs {
    /// Set one runtime-adjustable setting from the flag's value.
    fn flag_set(&mut self, field: DynamicField, parser: &mut lexopt::Parser) -> ConfigResult<()> {
        match field {
            DynamicField::CacheSize => self.cache_size = Some(arg_parse(parser)?),
            DynamicField::CachePolicy => self.cache_policy = Some(arg_enum(parser)?),
            DynamicField::AdmissionThreshold => {
                self.admission_threshold = Some(arg_parse(parser)?);
            }
            DynamicField::MvSizeRatio => self.mv_size_ratio = Some(arg_parse(parser)?),
            DynamicField::MvComputeMinRows => self.mv_compute_min_rows = Some(arg_parse(parser)?),
            DynamicField::MemoCacheSize => self.memo_cache_size = Some(arg_parse(parser)?),
            DynamicField::MemoryLimit => self.memory_limit = Some(arg_parse(parser)?),
            DynamicField::DiskLimit => self.disk_limit = Some(arg_parse(parser)?),
            DynamicField::AllowedTables => self.allowed_tables = Some(arg_string(parser)?),
            DynamicField::ExcludedTables => self.excluded_tables = Some(arg_string(parser)?),
            DynamicField::LogLevel => self.log_level = Some(arg_string(parser)?),
        }
        Ok(())
    }
}

/// Read the config file. Read after the flag loop so flag order is irrelevant.
/// In create mode a missing file is expected: a serving start writes it from
/// the flags, and the one-shot modes run from the flags alone. Other IO
/// errors (permissions, etc.) still fail even in create mode, since those
/// aren't typos.
fn config_read(config_path: Option<PathBuf>, config_create: bool) -> ConfigResult<ConfigFile> {
    let Some(path) = config_path else {
        return Ok(ConfigFile::Unset);
    };
    match read_to_string(&path) {
        Ok(file) => {
            let config = toml::from_str(&file)
                .map_err(|e| toml_parse_error(e.message(), e.span(), &file))
                .map_into_report::<ConfigError>()?;
            Ok(ConfigFile::Loaded {
                path,
                config: Box::new(config),
            })
        }
        Err(e) if config_create && e.kind() == std::io::ErrorKind::NotFound => {
            Ok(ConfigFile::ToCreate { path })
        }
        Err(e) => Err(e).map_into_report::<ConfigError>(),
    }
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

/// Resolve a setting from CLI > TOML > `env_var` (when it parses). `None`
/// falls through to the setting's own default.
fn env_override_resolve<T: FromStr>(
    cli: Option<T>,
    toml_value: Option<T>,
    env_var: &str,
) -> Option<T> {
    cli.or(toml_value)
        .or_else(|| std::env::var(env_var).ok()?.parse().ok())
}

/// The runtime-adjustable settings, CLI over TOML over env var. A `None`
/// falls through to the default in `DynamicConfig::new`: the MV gates and
/// memo budget to their constants, `memory_limit` to 80% of RAM, and
/// `disk_limit` to the cache volume's free space.
/// The runtime-adjustable settings of a config file under `flags`. Startup
/// and a config reload both resolve through here, so a reload cannot drop
/// what a flag set.
pub(super) fn dynamic_config_from_file(
    flags: &DynamicArgs,
    config: &mut SettingsToml,
) -> DynamicConfig {
    dynamic_config_resolve(flags.clone(), DynamicToml::from_config(config))
}

fn dynamic_config_resolve(args: DynamicArgs, toml: DynamicToml) -> DynamicConfig {
    let cache_size = args.cache_size.or(toml.cache_size);
    cache_size_deprecation_warn(cache_size.is_some());
    DynamicConfig::new(
        cache_size,
        args.cache_policy.or(toml.cache_policy),
        args.admission_threshold.or(toml.admission_threshold),
        csv_parse(args.allowed_tables).or(toml.allowed_tables),
        csv_parse(args.excluded_tables).or(toml.excluded_tables),
        args.log_level.or(toml.log_level),
        env_override_resolve(
            args.mv_size_ratio,
            toml.mv_size_ratio,
            "PGCACHE_MV_SIZE_RATIO",
        ),
        env_override_resolve(
            args.mv_compute_min_rows,
            toml.mv_compute_min_rows,
            "PGCACHE_MV_COMPUTE_MIN_ROWS",
        ),
        env_override_resolve(
            args.memo_cache_size,
            toml.memo_cache_size,
            "PGCACHE_MEMO_CACHE_SIZE",
        ),
        env_override_resolve(args.memory_limit, toml.memory_limit, "PGCACHE_MEMORY_LIMIT"),
        env_override_resolve(args.disk_limit, toml.disk_limit, "PGCACHE_DISK_LIMIT"),
    )
}

/// Effective (min, max) population worker bounds: apply the num_workers-derived
/// defaults and force `max >= min >= 1`.
pub(super) fn population_workers_bounds(
    num_workers: usize,
    min: Option<usize>,
    max: Option<usize>,
) -> (usize, usize) {
    // A *derived* floor defers to an explicitly configured lower ceiling —
    // raising the user's max to a default they never chose discards their
    // setting (PGC-457). An explicit min still wins over an explicit max
    // (tested-intentional: the floor is the liveness guarantee).
    let derived_min = num_workers.max(2);
    let min = min
        .unwrap_or_else(|| max.map_or(derived_min, |m| derived_min.min(m)))
        .max(1);
    let max = max.unwrap_or(num_workers * 8).max(min);
    (min, max)
}

fn cache_size_deprecation_warn(set: bool) {
    if set {
        tracing::warn!(
            "`cache_size` is deprecated and ignored; use `disk_limit` (the disk analogue of memory_limit)"
        );
    }
}

/// Settings for `--check`: origin, replication and CDC names only. CDC names
/// fall back to the defaults so a bare `--check --origin_*` invocation works.
fn preflight_settings_build(
    args: CliArgs,
    config: Option<SettingsToml>,
) -> ConfigResult<PreflightSettings> {
    let (origin, replication, mut cdc, allowed_tables, excluded_tables) = match config {
        Some(mut config) => {
            let origin = args.origin.merge_with(&config.origin);
            let replication =
                replication_settings_resolve(&origin, config.replication.take(), args.replication);
            let cdc = CdcSettings {
                publication_name: args
                    .cdc_publication_name
                    .unwrap_or_else(|| config.cdc.publication_name.clone()),
                slot_name: args
                    .cdc_slot_name
                    .unwrap_or_else(|| config.cdc.slot_name.clone()),
            };
            let allowed = csv_parse(args.dynamic.allowed_tables).or(config.allowed_tables.take());
            let excluded =
                csv_parse(args.dynamic.excluded_tables).or(config.excluded_tables.take());
            (origin, replication, cdc, allowed, excluded)
        }
        None => {
            let origin = args.origin.require(&ORIGIN_REQUIRED_FLAGS)?;
            let replication = replication_settings_resolve(&origin, None, args.replication);
            let cdc = CdcSettings {
                publication_name: args
                    .cdc_publication_name
                    .unwrap_or_else(|| DEFAULT_PUBLICATION_NAME.to_owned()),
                slot_name: args
                    .cdc_slot_name
                    .unwrap_or_else(|| DEFAULT_SLOT_NAME.to_owned()),
            };
            (
                origin,
                replication,
                cdc,
                csv_parse(args.dynamic.allowed_tables),
                csv_parse(args.dynamic.excluded_tables),
            )
        }
    };
    cdc.publication_name = cdc.publication_name.to_ascii_lowercase();
    cdc.slot_name = cdc.slot_name.to_ascii_lowercase();
    Ok(PreflightSettings {
        origin,
        replication,
        cdc,
        table_filter: TableFilter::new(allowed_tables.as_deref(), excluded_tables.as_deref()),
    })
}

pub(super) fn settings_build(
    args: CliArgs,
    config: Option<SettingsToml>,
    config_path: Option<PathBuf>,
) -> ConfigResult<Settings> {
    let flags = args.static_flags();
    let running = static_settings_resolve(&flags, config.as_ref())?;
    let mut settings = if let Some(mut config) = config {
        settings_build_with_config(args, running.clone(), &mut config, config_path)?
    } else {
        settings_build_cli_only(args, running.clone())?
    };

    settings.dynamic.static_baseline = Some(Arc::new(StaticBaseline { running, flags }));

    Ok(settings)
}

/// Pinned queries from explicit queries plus `SELECT *` for each pinned table,
/// CLI over TOML for each list.
fn pinned_queries_resolve(
    cli_queries: Option<String>,
    cli_tables: Option<String>,
    toml_queries: Option<Vec<String>>,
    toml_tables: Option<Vec<String>>,
) -> Option<Vec<String>> {
    pinned_tables_expand_and_merge(
        pinned_queries_parse(cli_queries).or(toml_queries),
        csv_parse(cli_tables).or(toml_tables),
    )
}

/// The cache connection: flags over the file's `[cache]` table, or the flags
/// alone when the file has none, and then all four are required.
fn cache_settings_resolve(
    flags: &PgSettingsPartial,
    toml_cache: Option<&PgSettings>,
) -> ConfigResult<PgSettings> {
    match toml_cache {
        Some(base) => Ok(flags.merge_with(base)),
        // Cache is localhost: trust auth (no password flag) and no TLS.
        None => Ok(PgSettings {
            ssl_mode: SslMode::Disable,
            ..flags.clone().require(&CACHE_REQUIRED_FLAGS)?
        }),
    }
}

/// The static settings: flags over the config file, or the flags alone
/// without one, and then each is required. Startup and `restart_required`
/// both resolve through here, so the two cannot disagree.
pub(super) fn static_settings_resolve(
    flags: &StaticFlags,
    config: Option<&SettingsToml>,
) -> ConfigResult<StaticConfigSnapshot> {
    let listen_socket = flags.listen_socket;
    let num_workers = flags.num_workers;
    let publication_name = flags.cdc_publication_name.clone();
    let slot_name = flags.cdc_slot_name.clone();
    let (origin, cache, socket, num_workers, publication_name, slot_name) = match config {
        Some(config) => (
            flags.origin.merge_with(&config.origin),
            cache_settings_resolve(&flags.cache, config.cache.as_ref())?,
            listen_socket.unwrap_or(config.listen.socket),
            num_workers.unwrap_or(config.num_workers),
            publication_name.unwrap_or_else(|| config.cdc.publication_name.clone()),
            slot_name.unwrap_or_else(|| config.cdc.slot_name.clone()),
        ),
        None => (
            flags.origin.clone().require(&ORIGIN_REQUIRED_FLAGS)?,
            cache_settings_resolve(&flags.cache, None)?,
            require(listen_socket, "listen_socket")?,
            require(num_workers, "num_workers")?,
            require(publication_name, "cdc_publication_name")?,
            require(slot_name, "cdc_slot_name")?,
        ),
    };
    Ok(StaticConfigSnapshot {
        origin,
        cache,
        listen: ListenSettings { socket },
        num_workers,
        // Lowercase CDC names to avoid quoting in postgres
        cdc: CdcSettings {
            publication_name: publication_name.to_ascii_lowercase(),
            slot_name: slot_name.to_ascii_lowercase(),
        },
    })
}

/// Build settings by merging CLI args over a TOML config file.
fn settings_build_with_config(
    args: CliArgs,
    statics: StaticConfigSnapshot,
    config: &mut SettingsToml,
    config_path: Option<PathBuf>,
) -> ConfigResult<Settings> {
    let StaticConfigSnapshot {
        origin,
        cache,
        listen,
        num_workers,
        cdc,
    } = statics;
    let replication =
        replication_settings_resolve(&origin, config.replication.take(), args.replication);

    let (population_workers_min, population_workers_max) = population_workers_bounds(
        num_workers,
        env_override_resolve(
            args.population_workers_min,
            config.population_workers_min,
            POPULATION_WORKERS_MIN_ENV,
        ),
        env_override_resolve(
            args.population_workers_max,
            config.population_workers_max,
            POPULATION_WORKERS_MAX_ENV,
        ),
    );
    let dynamic = dynamic_config_from_file(&args.dynamic, config);

    Ok(Settings {
        origin,
        replication,
        cache,
        cdc,
        listen,
        num_workers,
        population_workers_min,
        population_workers_max,
        role_poll_interval: role_poll_interval_resolve(
            args.role_poll_interval,
            config.role_poll_interval,
        ),
        tls_cert: args.tls_cert.or_else(|| config.tls_cert.clone()),
        tls_key: args.tls_key.or_else(|| config.tls_key.clone()),
        metrics: args
            .metrics_socket
            .map(|socket| MetricsSettings { socket })
            .or_else(|| config.metrics.clone()),
        dynamic: DynamicConfigHandle::new(dynamic, config_path, args.dynamic),
        pinned_queries: pinned_queries_resolve(
            args.pinned_queries,
            args.pinned_tables,
            config.pinned_queries.take(),
            config.pinned_tables.take(),
        ),
        telemetry: telemetry_resolve(args.telemetry_off, config.telemetry),
    })
}

/// Build settings from CLI args alone (no config file). Required fields must be present.
fn settings_build_cli_only(args: CliArgs, statics: StaticConfigSnapshot) -> ConfigResult<Settings> {
    let StaticConfigSnapshot {
        origin,
        cache,
        listen,
        num_workers,
        cdc,
    } = statics;

    // CLI-only mode: replication defaults to origin, with CLI overrides
    let replication = replication_settings_resolve(&origin, None, args.replication);
    let dynamic = dynamic_config_resolve(args.dynamic.clone(), DynamicToml::default());

    let (population_workers_min, population_workers_max) = population_workers_bounds(
        num_workers,
        env_override_resolve(
            args.population_workers_min,
            None,
            POPULATION_WORKERS_MIN_ENV,
        ),
        env_override_resolve(
            args.population_workers_max,
            None,
            POPULATION_WORKERS_MAX_ENV,
        ),
    );
    Ok(Settings {
        origin,
        replication,
        cache,
        cdc,
        listen,
        num_workers,
        population_workers_min,
        population_workers_max,
        role_poll_interval: role_poll_interval_resolve(args.role_poll_interval, None),
        tls_cert: args.tls_cert,
        tls_key: args.tls_key,
        metrics: args.metrics_socket.map(|socket| MetricsSettings { socket }),
        // No config file in CLI-only mode.
        dynamic: DynamicConfigHandle::new(dynamic, None, args.dynamic),
        pinned_queries: pinned_queries_resolve(args.pinned_queries, args.pinned_tables, None, None),
        telemetry: telemetry_resolve(args.telemetry_off, None),
    })
}

/// The admin socket `--healthcheck` probes: the flag, else the config file's
/// `[metrics]` socket. Without either there is nothing to probe.
fn healthcheck_socket_resolve(
    flag: Option<SocketAddr>,
    config: Option<&SettingsToml>,
) -> ConfigResult<SocketAddr> {
    flag.or_else(|| config.and_then(|c| c.metrics.as_ref()).map(|m| m.socket))
        .ok_or_else(|| {
            ConfigError::ArgumentMissing {
                name: "metrics_socket",
            }
            .into()
        })
}

impl RunMode {
    pub fn from_args() -> ConfigResult<RunMode> {
        let (args, config_file) = cli_args_parse()?;
        if args.healthcheck {
            return Ok(RunMode::Healthcheck(healthcheck_socket_resolve(
                args.metrics_socket,
                config_file.loaded(),
            )?));
        }
        if args.check {
            return Ok(RunMode::Check(Box::new(preflight_settings_build(
                args,
                config_file.into_loaded(),
            )?)));
        }
        if args.purge && !args.cleanup {
            return Err(ConfigError::ArgumentMissing { name: "cleanup" }.into());
        }
        if args.cleanup {
            let purge = args.purge;
            return Ok(RunMode::Cleanup {
                settings: Box::new(preflight_settings_build(args, config_file.into_loaded())?),
                purge,
            });
        }
        if args.origin_locale {
            return Ok(RunMode::OriginLocale(Box::new(preflight_settings_build(
                args,
                config_file.into_loaded(),
            )?)));
        }
        // A created file is read back and merged like any other, so the
        // first start and every later one resolve settings the same way.
        let (config, config_path) = match config_file {
            ConfigFile::Unset => (None, None),
            ConfigFile::Loaded { path, config } => (Some(*config), Some(path)),
            ConfigFile::ToCreate { path } => (Some(config_file_create(&path, &args)?), Some(path)),
        };
        Ok(RunMode::Serve(Box::new(settings_build(
            args,
            config,
            config_path,
        )?)))
    }
}

impl Settings {
    fn print_usage_and_exit(name: &str) -> ! {
        println!(
            "Usage: {name} -c|--config TOML_FILE --origin_host HOST --origin_port PORT --origin_user USER --origin_database DB \n \
            [--config_create] (when the config file is missing, write it from the flags given, without passwords or the cache connection, and start from it) \n \
            [--origin_password PASSWORD] [--origin_ssl_mode disable|require|verify-full] \n \
            [--replication_host HOST] [--replication_port PORT] [--replication_user USER] [--replication_database DB] \n \
            [--replication_password PASSWORD] [--replication_ssl_mode disable|require|verify-full] \n \
            --cache_host HOST --cache_port PORT --cache_user USER --cache_database DB \n \
            --cdc_publication_name NAME --cdc_slot_name SLOT_NAME \n \
            --listen_socket IP_AND_PORT \n \
            --num_workers NUMBER \n \
            [--population_workers_min NUMBER] (population worker floor; default max(num_workers, 2)) \n \
            [--population_workers_max NUMBER] (population worker ceiling; default num_workers * 8) \n \
            [--role_poll_interval SECONDS] (default: 5; origin role catalog poll for membership and attribute changes; 0 disables) \n \
            [--cache_size BYTES] (deprecated and ignored; use --disk_limit) \n \
            [--cache_policy fifo|clock] (default: clock) \n \
            [--admission_threshold N] (default: 1, clock policy only) \n \
            [--mv_size_ratio N] (default: 10, materialized view size gate) \n \
            [--mv_compute_min_rows N] (default: 1000, ComputeAvoid MV gate threshold in source rows) \n \
            [--memo_cache_size BYTES] (default: a quarter of the memory budget, at least 64 MiB; in-process hot-result cache budget; 0 disables) \n \
            [--memory_limit BYTES] (default: 80% of detected RAM; absolute ceiling for registration throttling, can only lower) \n \
            [--disk_limit BYTES] (default: auto from free disk; cap on cache-volume space used before throttling + table drops) \n \
            [--tls_cert CERT_FILE --tls_key KEY_FILE] \n \
            [--metrics_socket IP_AND_PORT] \n \
            [--allowed_tables TABLE1,TABLE2,...] (restrict caching to these tables) \n \
            [--excluded_tables TABLE1,TABLE2,...] (never cache queries on these tables; wins over --allowed_tables) \n \
            [--pinned_queries QUERY1;QUERY2;...] (pin queries in cache at startup, semicolon-separated) \n \
            [--pinned_tables TABLE1,TABLE2,...] (pin SELECT * FROM table for each table) \n \
            [--log_level LEVEL] (e.g., debug, info, pgcache_lib::cache=debug) \n \
            [--telemetry_off] (disable anonymous telemetry) \n \
            [--check] (check the origin is ready for pgcache, print a report and exit; cache settings not needed) \n \
            [--cleanup [--purge]] (drop this instance's replication slot and publication on the origin and exit, for an instance that was killed or is being retired; --purge also drops the event triggers and schema every pgcache on the database shares) \n \
            [--healthcheck] (probe a running pgcache's /healthz on --metrics_socket and exit 0 when it answers; for container health checks) \n \
            [--origin_locale] (print origin's database locale, provider and collation version on one line and exit; the image picks its cache PostgreSQL from it) \n \
            [--version] (print the pgcache version and exit) \n \
            A flag overrides the config file's value. Without a config file the unbracketed flags are required; with one, only the --cache_* flags, and only when the file has no [cache] table."
        );
        std::process::exit(1);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pg_route(name: &str) -> Option<(PgTarget, PgField)> {
        match flag_route(name)? {
            FlagRoute::Pg(target, field) => Some((target, field)),
            FlagRoute::Dynamic(_) => None,
        }
    }

    #[test]
    fn test_flag_route_connection_flags() {
        assert!(pg_route("origin_host") == Some((PgTarget::Origin, PgField::Host)));
        assert!(pg_route("origin_ssl_mode") == Some((PgTarget::Origin, PgField::SslMode)));
        assert!(
            pg_route("replication_password") == Some((PgTarget::Replication, PgField::Password))
        );
        assert!(pg_route("cache_database") == Some((PgTarget::Cache, PgField::Database)));
    }

    #[test]
    fn test_flag_route_cache_has_no_password_or_ssl_mode_flag() {
        assert!(flag_route("cache_password").is_none());
        assert!(flag_route("cache_ssl_mode").is_none());
    }

    #[test]
    fn test_flag_route_cache_prefixed_dynamic_settings() {
        assert!(matches!(
            flag_route("cache_size"),
            Some(FlagRoute::Dynamic(DynamicField::CacheSize))
        ));
        assert!(matches!(
            flag_route("cache_policy"),
            Some(FlagRoute::Dynamic(DynamicField::CachePolicy))
        ));
    }

    #[test]
    fn test_flag_route_unknown_flags() {
        assert!(flag_route("origin_hostname").is_none());
        assert!(flag_route("num_workers").is_none());
        assert!(flag_route("bogus").is_none());
    }
}
