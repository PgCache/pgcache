use std::fs;
use std::fs::{OpenOptions, read_to_string};
use std::io::Write;
#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;

use rootcause::Report;

use super::cli::{
    CliArgs, DynamicArgs, csv_parse, dynamic_config_from_file, pinned_queries_parse,
    static_settings_resolve,
};
use super::dynamic::DynamicConfig;
use super::{
    BoxedError, CachePolicy, ConfigError, ConfigResult, DynamicConfigPatch, PgSettingsPartial,
    SettingsToml, SslMode, toml_parse_error,
};
use crate::result::{MapIntoReport, ReportExt};

/// Read a TOML config file and resolve its dynamic config fields under
/// `flags`, the way startup did.
pub(super) fn config_file_dynamic_extract(
    path: &Path,
    flags: &DynamicArgs,
) -> ConfigResult<DynamicConfig> {
    let content = read_to_string(path).map_into_report::<ConfigError>()?;
    let mut config: SettingsToml = toml::from_str(&content)
        .map_err(|e| toml_parse_error(e.message(), e.span(), &content))
        .map_into_report::<ConfigError>()?;
    Ok(dynamic_config_from_file(flags, &mut config))
}

/// Apply a patch to the TOML config file, preserving formatting and comments.
pub fn config_file_dynamic_update(path: &Path, patch: &DynamicConfigPatch) -> ConfigResult<()> {
    let content = read_to_string(path).map_into_report::<ConfigError>()?;
    let mut doc: toml_edit::DocumentMut = content
        .parse()
        .map_err(|e: toml_edit::TomlError| toml_parse_error(e.message(), e.span(), &content))
        .map_into_report::<ConfigError>()?;

    let edits = [
        ("cache_size", nullable_int_edit(patch.cache_size)?),
        (
            "cache_policy",
            patch
                .cache_policy
                .map(|policy| Some(toml_edit::value(cache_policy_name(policy)))),
        ),
        ("admission_threshold", int_edit(patch.admission_threshold)?),
        ("mv_size_ratio", int_edit(patch.mv_size_ratio)?),
        ("mv_compute_min_rows", int_edit(patch.mv_compute_min_rows)?),
        ("memo_cache_size", int_edit(patch.memo_cache_size)?),
        ("memory_limit", nullable_int_edit(patch.memory_limit)?),
        ("disk_limit", nullable_int_edit(patch.disk_limit)?),
        (
            "allowed_tables",
            patch
                .allowed_tables
                .as_ref()
                .map(|tables| tables.as_deref().map(string_array_item)),
        ),
        (
            "excluded_tables",
            patch
                .excluded_tables
                .as_ref()
                .map(|tables| tables.as_deref().map(string_array_item)),
        ),
        (
            "log_level",
            patch
                .log_level
                .as_ref()
                .map(|level| level.as_deref().map(toml_edit::value)),
        ),
    ];
    for (key, edit) in edits {
        key_edit_apply(&mut doc, key, edit);
    }

    fs::write(path, doc.to_string()).map_into_report::<ConfigError>()?;
    Ok(())
}

/// Apply one key's patch: `None` leaves it, `Some(None)` removes it, and
/// `Some(Some(item))` sets it. An existing key is overwritten in place so its
/// own formatting (e.g. a comment line above it) survives.
fn key_edit_apply(
    doc: &mut toml_edit::DocumentMut,
    key: &str,
    edit: Option<Option<toml_edit::Item>>,
) {
    match edit {
        None => {}
        Some(None) => {
            doc.remove(key);
        }
        Some(Some(item)) => match doc.get_mut(key) {
            Some(slot) => *slot = item,
            None => {
                doc.insert(key, item);
            }
        },
    }
}

/// A TOML integer item. TOML integers are `i64`; a value beyond that is
/// rejected rather than truncated.
fn int_item<T: TryInto<i64> + Copy + std::fmt::Display>(value: T) -> ConfigResult<toml_edit::Item> {
    let int: i64 = value.try_into().map_err(|_| {
        Report::from(ConfigError::ArgumentError(BoxedError::new(format!(
            "{value} exceeds the TOML integer range"
        ))))
    })?;
    Ok(toml_edit::value(int))
}

/// The edit for a setting the patch sets or leaves alone.
fn int_edit<T: TryInto<i64> + Copy + std::fmt::Display>(
    value: Option<T>,
) -> ConfigResult<Option<Option<toml_edit::Item>>> {
    value.map(|v| int_item(v).map(Some)).transpose()
}

/// The edit for a setting the patch sets, clears or leaves alone.
fn nullable_int_edit<T: TryInto<i64> + Copy + std::fmt::Display>(
    value: Option<Option<T>>,
) -> ConfigResult<Option<Option<toml_edit::Item>>> {
    value.map(|v| v.map(int_item).transpose()).transpose()
}

/// Opens a created config file.
const CREATED_HEADER: &str = "\
# Written by pgcache --config_create from the settings it first started with.
# Passwords and the cache database connection are not stored here: keep
# giving those as flags or environment variables.
";

/// Write a config file at `path` from the flags of a first start and return
/// it parsed, so that start runs from the file as every later one will.
pub(super) fn config_file_create(path: &Path, args: &CliArgs) -> ConfigResult<SettingsToml> {
    let content = config_file_render(args)?;
    let location = || format!("creating config file {}", path.display());
    if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
        fs::create_dir_all(parent)
            .map_into_report::<ConfigError>()
            .attach_loc(location())?;
    }
    let mut options = OpenOptions::new();
    options.write(true).create_new(true);
    // Owner-only: the file names the origin and is the user's to widen.
    #[cfg(unix)]
    options.mode(0o600);
    options
        .open(path)
        .and_then(|mut file| file.write_all(content.as_bytes()))
        .map_into_report::<ConfigError>()
        .attach_loc(location())?;
    toml::from_str(&content)
        .map_err(|e| toml_parse_error(e.message(), e.span(), &content))
        .map_into_report::<ConfigError>()
}

/// The text of a created config file. Required settings are always written
/// and optional ones only when a flag set them, so a default derived at
/// startup is not frozen into the file. Passwords and the cache connection
/// are never written.
fn config_file_render(args: &CliArgs) -> ConfigResult<String> {
    let statics = static_settings_resolve(&args.static_flags(), None)?;
    let dynamic = &args.dynamic;
    let string_item = |text: &str| toml_edit::value(text);
    let path_item = |path: &Path| toml_edit::value(path.display().to_string());
    let list_item = |values: Option<Vec<String>>| values.as_deref().map(string_array_item);

    let mut doc = toml_edit::DocumentMut::new();
    doc.insert("num_workers", int_item(statics.num_workers)?);
    // `cache_size` is left out: it is deprecated and ignored.
    let optional = [
        (
            "population_workers_min",
            args.population_workers_min.map(int_item).transpose()?,
        ),
        (
            "population_workers_max",
            args.population_workers_max.map(int_item).transpose()?,
        ),
        (
            "role_poll_interval",
            args.role_poll_interval.map(int_item).transpose()?,
        ),
        ("tls_cert", args.tls_cert.as_deref().map(path_item)),
        ("tls_key", args.tls_key.as_deref().map(path_item)),
        ("log_level", dynamic.log_level.as_deref().map(string_item)),
        (
            "cache_policy",
            dynamic
                .cache_policy
                .map(|policy| string_item(cache_policy_name(policy))),
        ),
        (
            "admission_threshold",
            dynamic.admission_threshold.map(int_item).transpose()?,
        ),
        (
            "mv_size_ratio",
            dynamic.mv_size_ratio.map(int_item).transpose()?,
        ),
        (
            "mv_compute_min_rows",
            dynamic.mv_compute_min_rows.map(int_item).transpose()?,
        ),
        (
            "memo_cache_size",
            dynamic.memo_cache_size.map(int_item).transpose()?,
        ),
        (
            "memory_limit",
            dynamic.memory_limit.map(int_item).transpose()?,
        ),
        ("disk_limit", dynamic.disk_limit.map(int_item).transpose()?),
        (
            "allowed_tables",
            list_item(csv_parse(dynamic.allowed_tables.clone())),
        ),
        (
            "excluded_tables",
            list_item(csv_parse(dynamic.excluded_tables.clone())),
        ),
        (
            "pinned_queries",
            list_item(pinned_queries_parse(args.pinned_queries.clone())),
        ),
        (
            "pinned_tables",
            list_item(csv_parse(args.pinned_tables.clone())),
        ),
        (
            "telemetry",
            args.telemetry_off.then(|| toml_edit::value(false)),
        ),
    ];
    for (key, item) in optional {
        if let Some(item) = item {
            doc.insert(key, item);
        }
    }

    let mut origin = toml_edit::Table::new();
    origin.insert("host", string_item(&statics.origin.host));
    origin.insert("port", int_item(statics.origin.port)?);
    origin.insert("user", string_item(&statics.origin.user));
    origin.insert("database", string_item(&statics.origin.database));
    if let Some(mode) = args.origin.ssl_mode {
        origin.insert("ssl_mode", string_item(ssl_mode_name(mode)));
    }
    doc.insert("origin", toml_edit::Item::Table(origin));

    let replication = replication_table(&args.replication)?;
    if !replication.is_empty() {
        doc.insert("replication", toml_edit::Item::Table(replication));
    }

    let mut cdc = toml_edit::Table::new();
    cdc.insert(
        "publication_name",
        string_item(&statics.cdc.publication_name),
    );
    cdc.insert("slot_name", string_item(&statics.cdc.slot_name));
    doc.insert("cdc", toml_edit::Item::Table(cdc));

    let mut listen = toml_edit::Table::new();
    listen.insert("socket", string_item(&statics.listen.socket.to_string()));
    doc.insert("listen", toml_edit::Item::Table(listen));

    if let Some(socket) = args.metrics_socket {
        let mut metrics = toml_edit::Table::new();
        metrics.insert("socket", string_item(&socket.to_string()));
        doc.insert("metrics", toml_edit::Item::Table(metrics));
    }

    Ok(format!("{CREATED_HEADER}{doc}"))
}

/// The `[replication]` overrides given by flag, without the password.
fn replication_table(flags: &PgSettingsPartial) -> ConfigResult<toml_edit::Table> {
    let mut table = toml_edit::Table::new();
    if let Some(host) = &flags.host {
        table.insert("host", toml_edit::value(host.as_str()));
    }
    if let Some(port) = flags.port {
        table.insert("port", int_item(port)?);
    }
    if let Some(user) = &flags.user {
        table.insert("user", toml_edit::value(user.as_str()));
    }
    if let Some(database) = &flags.database {
        table.insert("database", toml_edit::value(database.as_str()));
    }
    if let Some(mode) = flags.ssl_mode {
        table.insert("ssl_mode", toml_edit::value(ssl_mode_name(mode)));
    }
    Ok(table)
}

fn ssl_mode_name(mode: SslMode) -> &'static str {
    match mode {
        SslMode::Disable => "disable",
        SslMode::Require => "require",
        SslMode::VerifyFull => "verify-full",
    }
}

fn cache_policy_name(policy: CachePolicy) -> &'static str {
    match policy {
        CachePolicy::Fifo => "fifo",
        CachePolicy::Clock => "clock",
    }
}

fn string_array_item(values: &[String]) -> toml_edit::Item {
    let mut arr = toml_edit::Array::new();
    for v in values {
        arr.push(v.as_str());
    }
    toml_edit::value(arr)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc(text: &str) -> toml_edit::DocumentMut {
        text.parse().expect("parse test TOML")
    }

    #[test]
    fn test_key_edit_apply_overwrites_in_place_keeping_the_comment_above() {
        let mut d = doc("# the limit\nmemory_limit = 1\nlog_level = \"info\"\n");
        key_edit_apply(&mut d, "memory_limit", Some(Some(toml_edit::value(2))));
        assert_eq!(
            d.to_string(),
            "# the limit\nmemory_limit = 2\nlog_level = \"info\"\n"
        );
    }

    #[test]
    fn test_key_edit_apply_inserts_removes_and_leaves() {
        let mut d = doc("log_level = \"info\"\n");
        key_edit_apply(&mut d, "disk_limit", Some(Some(toml_edit::value(5))));
        key_edit_apply(&mut d, "log_level", Some(None));
        key_edit_apply(&mut d, "cache_size", None);
        assert_eq!(d.to_string(), "disk_limit = 5\n");
    }

    #[test]
    fn test_int_item_rejects_values_beyond_i64() {
        assert!(int_item(u64::MAX).is_err());
        assert!(int_item(usize::MAX).is_err());
        assert!(int_item(7u32).is_ok());
    }
}
