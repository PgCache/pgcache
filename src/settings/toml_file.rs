use std::fs;
use std::fs::read_to_string;
use std::path::Path;

use rootcause::Report;

use super::dynamic::DynamicConfig;
use super::{BoxedError, CachePolicy, ConfigError, ConfigResult, DynamicConfigPatch, SettingsToml};
use crate::result::MapIntoReport;

/// Extract dynamic config fields from a parsed TOML config file.
fn dynamic_config_from_toml(config: &SettingsToml) -> DynamicConfig {
    DynamicConfig::new(
        config.cache_size,
        config.cache_policy,
        config.admission_threshold,
        config.allowed_tables.clone(),
        config.log_level.clone(),
        config.mv_size_ratio,
        config.mv_compute_min_rows,
        config.memo_cache_size,
        config.memory_limit,
        config.disk_limit,
    )
}

/// Read a TOML config file and extract the dynamic config fields.
pub fn config_file_dynamic_extract(path: &Path) -> ConfigResult<DynamicConfig> {
    let content = read_to_string(path).map_into_report::<ConfigError>()?;
    let config: SettingsToml = toml::from_str(&content).map_into_report::<ConfigError>()?;
    Ok(dynamic_config_from_toml(&config))
}

/// Apply a patch to the TOML config file, preserving formatting and comments.
pub fn config_file_dynamic_update(path: &Path, patch: &DynamicConfigPatch) -> ConfigResult<()> {
    let content = read_to_string(path).map_into_report::<ConfigError>()?;
    let mut doc: toml_edit::DocumentMut = content
        .parse()
        .map_err(|e: toml_edit::TomlError| ConfigError::TomlError(BoxedError::new(e)))
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
