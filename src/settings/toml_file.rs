use std::fs;
use std::fs::read_to_string;
use std::path::Path;

use crate::result::MapIntoReport;

use super::dynamic::DynamicConfig;
use super::*;

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

    if let Some(v) = &patch.disk_limit {
        match v {
            Some(limit) => {
                let limit_i64 = i64::try_from(*limit).expect("disk limit fits in i64");
                doc["disk_limit"] = toml_edit::value(limit_i64);
            }
            None => {
                doc.remove("disk_limit");
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
