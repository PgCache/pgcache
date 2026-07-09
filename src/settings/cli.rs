use std::fs::read_to_string;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use lexopt::prelude::*;
use rootcause::Report;

use crate::result::MapIntoReport;

use super::dynamic::{DynamicConfig, DynamicConfigHandle};
use super::*;

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
        .map_err(|e: T::Err| Report::from(ConfigError::ArgumentError(BoxedError::new(e.to_string()))))
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
pub(super) struct CliArgs {
    pub(super) origin_host: Option<String>,
    pub(super) origin_port: Option<u16>,
    pub(super) origin_user: Option<String>,
    pub(super) origin_database: Option<String>,
    pub(super) origin_ssl_mode: Option<SslMode>,
    pub(super) origin_password: Option<String>,
    pub(super) replication_host: Option<String>,
    pub(super) replication_port: Option<u16>,
    pub(super) replication_user: Option<String>,
    pub(super) replication_database: Option<String>,
    pub(super) replication_ssl_mode: Option<SslMode>,
    pub(super) replication_password: Option<String>,
    pub(super) cache_host: Option<String>,
    pub(super) cache_port: Option<u16>,
    pub(super) cache_user: Option<String>,
    pub(super) cache_database: Option<String>,
    pub(super) cdc_publication_name: Option<String>,
    pub(super) cdc_slot_name: Option<String>,
    pub(super) listen_socket: Option<SocketAddr>,
    pub(super) num_workers: Option<usize>,
    pub(super) cache_size: Option<usize>,
    pub(super) tls_cert: Option<PathBuf>,
    pub(super) tls_key: Option<PathBuf>,
    pub(super) metrics_socket: Option<SocketAddr>,
    pub(super) log_level: Option<String>,
    pub(super) cache_policy: Option<CachePolicy>,
    pub(super) admission_threshold: Option<u32>,
    pub(super) mv_size_ratio: Option<u32>,
    pub(super) mv_compute_min_rows: Option<u64>,
    pub(super) memo_cache_size: Option<usize>,
    pub(super) memory_limit: Option<usize>,
    pub(super) disk_limit: Option<usize>,
    pub(super) allowed_tables: Option<String>,
    pub(super) pinned_queries: Option<String>,
    pub(super) pinned_tables: Option<String>,
    pub(super) telemetry_off: bool,
}

fn cli_args_parse() -> ConfigResult<(CliArgs, Option<SettingsToml>, Option<PathBuf>)> {
    let mut args = CliArgs::default();
    let mut config = None;
    let mut config_path = None;
    let mut config_create = false;
    let mut parser = lexopt::Parser::from_env();

    while let Some(arg) = parser.next().map_into_report::<ConfigError>()? {
        match arg {
            Short('c') | Long("config") => {
                config_path = Some(PathBuf::from(arg_string(&mut parser)?));
            }
            Long("config_create") => config_create = true,
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
            Long("mv_compute_min_rows") => args.mv_compute_min_rows = Some(arg_parse(&mut parser)?),
            Long("memo_cache_size") => args.memo_cache_size = Some(arg_parse(&mut parser)?),
            Long("memory_limit") => args.memory_limit = Some(arg_parse(&mut parser)?),
            Long("disk_limit") => args.disk_limit = Some(arg_parse(&mut parser)?),
            Long("allowed_tables") => args.allowed_tables = Some(arg_string(&mut parser)?),
            Long("pinned_queries") => args.pinned_queries = Some(arg_string(&mut parser)?),
            Long("pinned_tables") => args.pinned_tables = Some(arg_string(&mut parser)?),
            Long("telemetry_off") => args.telemetry_off = true,
            Long("help") => {
                Settings::print_usage_and_exit(parser.bin_name().unwrap_or_default());
            }
            Short(_) | Long(_) | Value(_) => {
                return Err(ConfigError::ArgumentError(BoxedError::new(arg.unexpected())).into());
            }
        }
    }

    // Read the config file after the loop so flag order is irrelevant. In create
    // mode a missing file is expected — start from defaults and let the dynamic
    // config write-back create it on the first `PUT /config`. Other IO errors
    // (permissions, etc.) still fail even in create mode, since those aren't typos.
    if let Some(path) = &config_path {
        match read_to_string(path) {
            Ok(file) => config = Some(toml::from_str(&file).map_into_report::<ConfigError>()?),
            Err(e) if config_create && e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e).map_into_report::<ConfigError>(),
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

/// Resolve mv_compute_min_rows from CLI > TOML > env var.
/// Returns None to fall through to `DEFAULT_MV_COMPUTE_MIN_ROWS` in
/// `DynamicConfig::new`.
fn mv_compute_min_rows_resolve(cli: Option<u64>, toml_value: Option<u64>) -> Option<u64> {
    if let Some(v) = cli {
        return Some(v);
    }
    if let Some(v) = toml_value {
        return Some(v);
    }
    if let Ok(v) = std::env::var("PGCACHE_MV_COMPUTE_MIN_ROWS")
        && let Ok(parsed) = v.parse::<u64>()
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

/// Resolve disk_limit from CLI > TOML > env var.
/// Returns None to auto-derive the limit from the cache volume's free space.
fn disk_limit_resolve(cli: Option<usize>, toml_value: Option<usize>) -> Option<usize> {
    if let Some(v) = cli {
        return Some(v);
    }
    if let Some(v) = toml_value {
        return Some(v);
    }
    if let Ok(v) = std::env::var("PGCACHE_DISK_LIMIT")
        && let Ok(parsed) = v.parse::<usize>()
    {
        return Some(parsed);
    }
    None
}

fn cache_size_deprecation_warn(set: bool) {
    if set {
        tracing::warn!(
            "`cache_size` is deprecated and ignored; use `disk_limit` (the disk analogue of memory_limit)"
        );
    }
}

pub(super) fn settings_build(
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
pub(super) fn settings_build_with_config(
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

    cache_size_deprecation_warn(args.cache_size.or(config.cache_size).is_some());
    let dynamic = DynamicConfig::new(
        args.cache_size.or(config.cache_size),
        args.cache_policy.or(config.cache_policy),
        args.admission_threshold.or(config.admission_threshold),
        csv_parse(args.allowed_tables).or(config.allowed_tables.take()),
        args.log_level.or_else(|| config.log_level.clone()),
        mv_size_ratio_resolve(args.mv_size_ratio, config.mv_size_ratio),
        mv_compute_min_rows_resolve(args.mv_compute_min_rows, config.mv_compute_min_rows),
        memo_cache_size_resolve(args.memo_cache_size, config.memo_cache_size),
        memory_limit_resolve(args.memory_limit, config.memory_limit),
        disk_limit_resolve(args.disk_limit, config.disk_limit),
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
pub(super) fn settings_build_cli_only(args: CliArgs) -> ConfigResult<Settings> {
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

    cache_size_deprecation_warn(args.cache_size.is_some());
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
                mv_compute_min_rows_resolve(args.mv_compute_min_rows, None),
                memo_cache_size_resolve(args.memo_cache_size, None),
                memory_limit_resolve(args.memory_limit, None),
                disk_limit_resolve(args.disk_limit, None),
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
            [--config_create] (create the config file if missing; persist dynamic /config changes to it) \n \
            [--origin_password PASSWORD] [--origin_ssl_mode disable|require|verify-full] \n \
            [--replication_host HOST] [--replication_port PORT] [--replication_user USER] [--replication_database DB] \n \
            [--replication_password PASSWORD] [--replication_ssl_mode disable|require|verify-full] \n \
            --cache_host HOST --cache_port PORT --cache_user USER --cache_database DB \n \
            --cdc_publication_name NAME --cdc_slot_name SLOT_NAME \n \
            --listen_socket IP_AND_PORT \n \
            --num_workers NUMBER \n \
            [--cache_size BYTES] (deprecated and ignored; use --disk_limit) \n \
            [--cache_policy fifo|clock] (default: clock) \n \
            [--admission_threshold N] (default: 1, clock policy only) \n \
            [--mv_size_ratio N] (default: 10, materialized view size gate) \n \
            [--mv_compute_min_rows N] (default: 1000, ComputeAvoid MV gate threshold in source rows) \n \
            [--memo_cache_size BYTES] (default: 64 MiB, in-process hot-result cache budget; 0 disables) \n \
            [--memory_limit BYTES] (default: 80% of detected RAM; absolute ceiling for registration throttling, can only lower) \n \
            [--disk_limit BYTES] (default: auto from free disk; cap on cache-volume space used before throttling + table drops) \n \
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
