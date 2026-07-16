//! cgroup memory resolution (PGC-354).
//!
//! Resolves the process's own cgroup from `/proc/self/cgroup` and walks its
//! ancestors for the tightest memory limit, so a limit set above the leaf —
//! e.g. systemd `MemoryMax=` on the service, or nested cgroups without a
//! cgroup namespace — is honored instead of silently budgeting against host
//! RAM. Usage is reported as the Kubernetes-style *working set* (`usage −
//! inactive_file`): raw `memory.current` counts the cache PG's clean,
//! freely-reclaimable page cache, which would latch the registration throttle
//! once the cached dataset outgrows the limit.
//!
//! Parsing helpers are platform-independent so they unit-test on the
//! (non-Linux) dev machine; filesystem access is Linux-only.

/// Memory figures of the binding (tightest-limit) cgroup level. `None` from
/// [`cgroup_memory_snapshot`] means "not memory-limited" (or not detectable),
/// and callers fall back to host-wide accounting.
#[cfg(any(target_os = "linux", test))]
// Only `working_set()` is exercised by the platform-independent tests; the
// fields are read by the Linux-only callers in `sys.rs`.
#[cfg_attr(not(target_os = "linux"), allow(dead_code))]
pub(super) struct CgroupMemory {
    /// The tightest memory limit found on the self→root ancestor chain.
    pub limit: u64,
    /// Raw usage of the binding level (`memory.current` / `usage_in_bytes`).
    pub current: u64,
    /// Reclaimable file-backed pages of the binding level; 0 if `memory.stat`
    /// is unreadable, degrading [`Self::working_set`] to raw usage.
    pub inactive_file: u64,
    /// Anonymous (private) memory of the binding level (v2 `anon`, v1
    /// `total_rss`); `None` if unreadable.
    pub anon: Option<u64>,
}

#[cfg(any(target_os = "linux", test))]
impl CgroupMemory {
    /// Usage minus reclaimable page cache — the figure the throttle should
    /// compare against the limit (Kubernetes "working set" convention).
    pub fn working_set(&self) -> u64 {
        self.current.saturating_sub(self.inactive_file)
    }
}

/// cgroup v1 reports a near-`u64::MAX` sentinel when unlimited; treat any
/// implausibly large limit (≥ 1 PiB) as "no limit".
#[cfg(any(target_os = "linux", test))]
pub(super) fn sane_limit(v: u64) -> Option<u64> {
    const MAX_SANE: u64 = 1 << 50; // 1 PiB
    (v < MAX_SANE).then_some(v)
}

/// The unified-hierarchy (v2) entry of `/proc/self/cgroup`: the `0::<path>` line.
#[cfg(any(target_os = "linux", test))]
fn self_path_v2(proc_self_cgroup: &str) -> Option<&str> {
    proc_self_cgroup.lines().find_map(|l| l.strip_prefix("0::"))
}

/// The v1 memory-controller entry of `/proc/self/cgroup`: the
/// `<id>:<controllers>:<path>` line whose controller list contains `memory`.
#[cfg(any(target_os = "linux", test))]
fn self_path_v1_memory(proc_self_cgroup: &str) -> Option<&str> {
    proc_self_cgroup.lines().find_map(|l| {
        let mut parts = l.splitn(3, ':');
        let _hierarchy_id = parts.next()?;
        let controllers = parts.next()?;
        let path = parts.next()?;
        controllers
            .split(',')
            .any(|c| c == "memory")
            .then_some(path)
    })
}

/// The self→root chain of cgroup paths relative to the hierarchy mount:
/// `/a/b` → `["a/b", "a", ""]` (self first, root last). The root's limit file
/// doesn't exist, so it drops out of the walk naturally.
#[cfg(any(target_os = "linux", test))]
fn ancestor_paths(rel: &str) -> Vec<String> {
    let trimmed = rel.trim_matches('/');
    let mut paths = vec![String::new()];
    if trimmed.is_empty() {
        return paths;
    }
    let mut prefix = String::new();
    for segment in trimmed.split('/') {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(segment);
        paths.push(prefix.clone());
    }
    paths.reverse();
    paths
}

/// A `<key> <value>` line of `memory.stat`. Requires the space after the key
/// so `anon` doesn't match `anon_thp` (nor `total_rss` match `total_rss_huge`).
#[cfg(any(target_os = "linux", test))]
fn stat_value(stat: &str, key: &str) -> Option<u64> {
    stat.lines().find_map(|l| {
        let rest = l.strip_prefix(key)?.strip_prefix(' ')?;
        rest.trim().parse().ok()
    })
}

/// A v2 `memory.max` value: `"max"` means unlimited.
#[cfg(any(target_os = "linux", test))]
fn limit_v2_parse(s: &str) -> Option<u64> {
    let s = s.trim();
    if s == "max" {
        return None;
    }
    s.parse::<u64>().ok().and_then(sane_limit)
}

/// Index of the binding level among per-ancestor limits ordered self→root:
/// the smallest real limit. On a tie the shallower (closer-to-root) level
/// wins — its usage includes siblings, the conservative direction.
#[cfg(any(target_os = "linux", test))]
fn binding_level_select(limits: &[Option<u64>]) -> Option<usize> {
    let mut best: Option<(usize, u64)> = None;
    for (index, limit) in limits.iter().enumerate() {
        if let Some(limit) = *limit
            && best.is_none_or(|(_, b)| limit <= b)
        {
            best = Some((index, limit));
        }
    }
    best.map(|(index, _)| index)
}

/// Snapshot the binding memory-limited cgroup, or `None` when no ancestor has
/// a real memory limit (callers then use host-wide accounting).
#[cfg(target_os = "linux")]
pub(super) fn cgroup_memory_snapshot() -> Option<CgroupMemory> {
    use std::fs;
    use std::path::{Path, PathBuf};

    fn cgroup_file(base: &str, rel: &str, file: &str) -> PathBuf {
        let mut path = PathBuf::from(base);
        if !rel.is_empty() {
            path.push(rel);
        }
        path.push(file);
        path
    }

    fn read_u64(path: &Path) -> Option<u64> {
        fs::read_to_string(path).ok()?.trim().parse().ok()
    }

    let proc_cgroup = fs::read_to_string("/proc/self/cgroup").ok()?;
    let v2 = Path::new("/sys/fs/cgroup/cgroup.controllers").exists();
    let (base, rel, limit_file) = if v2 {
        ("/sys/fs/cgroup", self_path_v2(&proc_cgroup)?, "memory.max")
    } else {
        (
            "/sys/fs/cgroup/memory",
            self_path_v1_memory(&proc_cgroup)?,
            "memory.limit_in_bytes",
        )
    };

    let dirs = ancestor_paths(rel);
    let limits: Vec<Option<u64>> = dirs
        .iter()
        .map(|dir| {
            let contents = fs::read_to_string(cgroup_file(base, dir, limit_file)).ok()?;
            if v2 {
                limit_v2_parse(&contents)
            } else {
                contents.trim().parse().ok().and_then(sane_limit)
            }
        })
        .collect();
    let binding = binding_level_select(&limits)?;
    let dir = &dirs[binding];
    let limit = limits[binding]?;

    let usage_file = if v2 {
        "memory.current"
    } else {
        "memory.usage_in_bytes"
    };
    let current = read_u64(&cgroup_file(base, dir, usage_file))?;
    // v2 memory.stat is hierarchical (includes descendants); v1 needs total_*.
    let stat = fs::read_to_string(cgroup_file(base, dir, "memory.stat")).unwrap_or_default();
    let (inactive_file_key, anon_key) = if v2 {
        ("inactive_file", "anon")
    } else {
        ("total_inactive_file", "total_rss")
    };
    Some(CgroupMemory {
        limit,
        current,
        inactive_file: stat_value(&stat, inactive_file_key).unwrap_or(0),
        anon: stat_value(&stat, anon_key),
    })
}

#[cfg(test)]
mod tests {
    use super::{
        CgroupMemory, ancestor_paths, binding_level_select, limit_v2_parse, sane_limit,
        self_path_v1_memory, self_path_v2, stat_value,
    };

    #[test]
    fn test_self_path_v2_plain() {
        assert_eq!(
            self_path_v2("0::/system.slice/pgcache.service\n"),
            Some("/system.slice/pgcache.service")
        );
    }

    #[test]
    fn test_self_path_v2_container_root() {
        // Docker with a private cgroup namespace: the namespace root.
        assert_eq!(self_path_v2("0::/\n"), Some("/"));
    }

    #[test]
    fn test_self_path_v2_hybrid_picks_unified_line() {
        let contents = "12:memory:/docker/abc\n1:name=systemd:/docker/abc\n0::/docker/abc\n";
        assert_eq!(self_path_v2(contents), Some("/docker/abc"));
    }

    #[test]
    fn test_self_path_v1_memory_multi_line() {
        let contents = "5:cpuacct,cpu:/docker/abc\n4:memory:/docker/abc\n1:name=systemd:/init\n";
        assert_eq!(self_path_v1_memory(contents), Some("/docker/abc"));
    }

    #[test]
    fn test_self_path_v1_memory_combined_controllers() {
        assert_eq!(self_path_v1_memory("2:cpu,memory:/x\n"), Some("/x"));
    }

    #[test]
    fn test_self_path_v1_memory_ignores_v2_line() {
        assert_eq!(self_path_v1_memory("0::/docker/abc\n"), None);
    }

    #[test]
    fn test_ancestor_paths_nested() {
        assert_eq!(
            ancestor_paths("/system.slice/pgcache.service"),
            vec!["system.slice/pgcache.service", "system.slice", ""]
        );
    }

    #[test]
    fn test_ancestor_paths_single_segment() {
        assert_eq!(ancestor_paths("/a"), vec!["a", ""]);
    }

    #[test]
    fn test_ancestor_paths_root() {
        assert_eq!(ancestor_paths("/"), vec![""]);
    }

    #[test]
    fn test_stat_value_requires_exact_key() {
        // `anon` must not match the `anon_thp` line.
        let stat = "anon_thp 4096\nanon 123456\ninactive_file 789\n";
        assert_eq!(stat_value(stat, "anon"), Some(123_456));
        assert_eq!(stat_value(stat, "inactive_file"), Some(789));
    }

    #[test]
    fn test_stat_value_absent_key() {
        assert_eq!(stat_value("anon 1\n", "inactive_file"), None);
    }

    #[test]
    fn test_limit_v2_parse_max_is_unlimited() {
        assert_eq!(limit_v2_parse("max\n"), None);
        assert_eq!(limit_v2_parse("2147483648\n"), Some(2_147_483_648));
    }

    #[test]
    fn test_limit_v2_parse_rejects_insane() {
        assert_eq!(limit_v2_parse(&format!("{}\n", 1_u64 << 55)), None);
    }

    #[test]
    fn test_sane_limit_rejects_unlimited_sentinel() {
        // cgroup v1 "unlimited" sentinel.
        assert_eq!(sane_limit(0x7FFF_FFFF_FFFF_F000), None);
        assert_eq!(
            sane_limit(2 * 1024 * 1024 * 1024),
            Some(2 * 1024 * 1024 * 1024)
        );
    }

    #[test]
    fn test_binding_level_ancestor_tighter_than_self() {
        // self unlimited, parent 4 GiB, grandparent 8 GiB → parent binds.
        let limits = [None, Some(4 << 30), Some(8 << 30)];
        assert_eq!(binding_level_select(&limits), Some(1));
    }

    #[test]
    fn test_binding_level_self_tightest() {
        let limits = [Some(1 << 30), Some(4 << 30), None];
        assert_eq!(binding_level_select(&limits), Some(0));
    }

    #[test]
    fn test_binding_level_tie_prefers_shallower() {
        // Equal limits: the ancestor's usage includes siblings — conservative.
        let limits = [Some(4 << 30), Some(4 << 30)];
        assert_eq!(binding_level_select(&limits), Some(1));
    }

    #[test]
    fn test_binding_level_none_without_limits() {
        assert_eq!(binding_level_select(&[None, None]), None);
    }

    #[test]
    fn test_working_set_subtracts_page_cache() {
        let snapshot = CgroupMemory {
            limit: 4 << 30,
            current: 3 << 30,
            inactive_file: 1 << 30,
            anon: None,
        };
        assert_eq!(snapshot.working_set(), 2 << 30);
    }

    #[test]
    fn test_working_set_saturates() {
        // inactive_file can momentarily exceed a racing `current` read.
        let snapshot = CgroupMemory {
            limit: 4 << 30,
            current: 1 << 20,
            inactive_file: 2 << 20,
            anon: None,
        };
        assert_eq!(snapshot.working_set(), 0);
    }
}
