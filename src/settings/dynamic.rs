use std::fmt;
use std::fs::read_to_string;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use arc_swap::ArcSwap;
use serde::Deserialize;

use super::cli::allowlist_parse;
use super::*;

/// Runtime-adjustable configuration fields.
/// Stored behind ArcSwap for lock-free reads on the hot path.
#[derive(Debug, Clone, Serialize)]
pub struct DynamicConfig {
    /// Deprecated, superseded by `disk_limit` (PGC-276). Ignored if set; a
    /// deprecation warning is logged. Retained only so existing configs parse.
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
    /// Materialized-view compute-avoidance gate: a `Gated` query is materialized
    /// iff its origin-population source-row count is `>= mv_compute_min_rows`.
    /// Sticky: retuning affects future first-population decisions only.
    pub mv_compute_min_rows: u64,
    /// Total-bytes budget for the in-process hot-result cache (PGC-236).
    /// 0 disables in-memory result memoization.
    pub memo_cache_size: usize,
    /// Optional absolute ceiling (bytes) on pgcache's resident memory, used to
    /// throttle new-query registration before the box runs out of RAM. When
    /// `None`, the ceiling is derived dynamically as 80% of detected RAM. A
    /// configured value can only lower the effective ceiling, never raise it.
    pub memory_limit: Option<usize>,
    /// Optional cap (bytes) on space used on the cache volume. When the volume's
    /// used bytes exceed it, pgcache throttles new-query registration and drops
    /// cache tables to reclaim space (PGC-276). When `None`, derived from live
    /// free space, keeping a reserve free (PGC-251 Slice 2). The disk analogue of
    /// `memory_limit`.
    pub disk_limit: Option<usize>,
}

const DEFAULT_ADMISSION_THRESHOLD: u32 = 1;

pub(super) const DEFAULT_MV_SIZE_RATIO: u32 = 10;

pub(super) const DEFAULT_MV_COMPUTE_MIN_ROWS: u64 = 1000;

/// Floor (and RAM-undetectable fallback) for the in-process hot-result cache
/// budget: 64 MiB.
const DEFAULT_MEMO_CACHE_SIZE: usize = 64 * 1024 * 1024;

/// Fraction of the detected memory budget used as the memo default (PROTOTYPE,
/// PGC-277). The memo competes for the same throttle ceiling as registration by
/// its actual footprint, so this is a soft ceiling on the hot-result cache.
const MEMO_RAM_FRACTION_DIVISOR: u64 = 4;

/// RAM-relative default memo budget: 1/[`MEMO_RAM_FRACTION_DIVISOR`] of the
/// detected memory budget, floored at [`DEFAULT_MEMO_CACHE_SIZE`]. Falls back to
/// the floor when RAM is undetectable (non-Linux/non-macOS).
fn memo_default() -> usize {
    crate::memory::total_budget_bytes()
        .and_then(|b| usize::try_from(b / MEMO_RAM_FRACTION_DIVISOR).ok())
        .map_or(DEFAULT_MEMO_CACHE_SIZE, |v| v.max(DEFAULT_MEMO_CACHE_SIZE))
}

impl DynamicConfig {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        cache_size: Option<usize>,
        cache_policy: Option<CachePolicy>,
        admission_threshold: Option<u32>,
        allowed_tables: Option<Vec<String>>,
        log_level: Option<String>,
        mv_size_ratio: Option<u32>,
        mv_compute_min_rows: Option<u64>,
        memo_cache_size: Option<usize>,
        memory_limit: Option<usize>,
        disk_limit: Option<usize>,
    ) -> Self {
        Self {
            cache_size,
            cache_policy: cache_policy.unwrap_or_default(),
            admission_threshold: admission_threshold.unwrap_or(DEFAULT_ADMISSION_THRESHOLD),
            allowed_tables_parsed: allowlist_parse(&allowed_tables),
            allowed_tables,
            log_level,
            mv_size_ratio: mv_size_ratio.unwrap_or(DEFAULT_MV_SIZE_RATIO),
            mv_compute_min_rows: mv_compute_min_rows.unwrap_or(DEFAULT_MV_COMPUTE_MIN_ROWS),
            memo_cache_size: memo_cache_size.unwrap_or_else(memo_default),
            memory_limit,
            disk_limit,
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
    pub(super) static_snapshot: Option<Arc<StaticConfigSnapshot>>,
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
    pub(super) fn new(
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
            DynamicConfig::new(None, None, None, None, None, None, None, None, None, None),
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
    pub mv_compute_min_rows: Option<u64>,
    #[serde(default)]
    pub memo_cache_size: Option<usize>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub memory_limit: Option<Option<usize>>,
    #[serde(default, deserialize_with = "deserialize_double_option")]
    pub disk_limit: Option<Option<usize>>,
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
            mv_compute_min_rows: self
                .mv_compute_min_rows
                .unwrap_or(current.mv_compute_min_rows),
            memo_cache_size: self.memo_cache_size.unwrap_or(current.memo_cache_size),
            memory_limit: match self.memory_limit {
                Some(v) => v,
                None => current.memory_limit,
            },
            disk_limit: match self.disk_limit {
                Some(v) => v,
                None => current.disk_limit,
            },
        }
    }
}
