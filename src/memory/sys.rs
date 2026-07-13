//! Platform memory/disk introspection.
//!
//! Linux is the production/Docker target: RSS from `/proc/self/status`, the
//! budget from `min(host MemTotal, cgroup memory limit)` so a constrained
//! container budgets against its limit rather than host RAM. On non-Linux
//! (dev/test) detection returns `None` and the caller leaves registration
//! unthrottled.
//!
//! The pure decision math that consumes these readings lives in the parent
//! [`memory`](super) module.

/// Current resident set size (RSS) of this process in bytes, or `None` if it
/// can't be read (non-Linux, or `/proc` unavailable).
pub fn process_rss_bytes() -> Option<u64> {
    imp::process_rss_bytes()
}

/// Total memory available to this process in bytes: the smaller of host RAM and
/// any cgroup memory limit. `None` if undetectable.
pub fn total_budget_bytes() -> Option<u64> {
    imp::total_budget_bytes()
}

/// Total memory *currently used* on this machine, in bytes. In a memory-limited
/// cgroup this is the cgroup's usage (`memory.current`); otherwise it is host
/// `MemTotal - MemAvailable`. Either way it counts pgcache **and** the cache
/// Postgres it manages — which share the same box/container — so the registration
/// budget covers the whole footprint, not just pgcache's own RSS. `None` if
/// undetectable (non-Linux).
pub fn system_used_bytes() -> Option<u64> {
    imp::system_used_bytes()
}

/// Whole-system *private* (anonymous, non-shared) used memory in bytes — used
/// memory minus shared memory, where the cache Postgres `shared_buffers` lives.
/// This is the *growing* pool the count cap targets (pgcache in-process state +
/// the cache-PG plan cache), measured without any privileged per-backend access.
/// `None` if undetectable (non-Linux). Note: if the cache PG uses huge pages,
/// `shared_buffers` won't be in `Shmem`, so this over-counts (cap stays
/// conservative — the safe direction).
pub fn system_private_bytes() -> Option<u64> {
    imp::system_private_bytes()
}

/// Total and available bytes of the filesystem hosting `path` (the cache PG data
/// directory), via `statvfs`. `None` if the path can't be stat'd — not visible to
/// this process, or non-unix — in which case the caller takes no auto disk limit.
/// pgcache always controls the cache PG's location, so the path is local. PGC-251
/// Slice 2.
#[cfg(unix)]
pub fn disk_stats_bytes(path: &std::path::Path) -> Option<(u64, u64)> {
    let vfs = nix::sys::statvfs::statvfs(path).ok()?;
    let frag = vfs.fragment_size() as u64;
    Some((
        vfs.blocks() as u64 * frag,
        vfs.blocks_available() as u64 * frag,
    ))
}

#[cfg(not(unix))]
pub fn disk_stats_bytes(_path: &std::path::Path) -> Option<(u64, u64)> {
    None
}

// Pure parsing helpers, kept platform-independent so they are unit-testable on
// the (non-Linux) dev machine.
#[cfg(any(target_os = "linux", test))]
fn field_kb(table: &str, key: &str) -> Option<u64> {
    let value = table.lines().find_map(|l| l.strip_prefix(key))?;
    let kb: u64 = value.split_whitespace().next()?.parse().ok()?;
    Some(kb * 1024)
}

#[cfg(any(target_os = "linux", test))]
fn budget_from(host: Option<u64>, cgroup: Option<u64>) -> Option<u64> {
    match (host, cgroup) {
        (Some(h), Some(c)) => Some(h.min(c)),
        (h, c) => h.or(c),
    }
}

/// cgroup v1 reports a near-`u64::MAX` sentinel when unlimited; treat any
/// implausibly large limit (≥ 1 PiB) as "no limit".
#[cfg(any(target_os = "linux", test))]
fn sane_limit(v: u64) -> Option<u64> {
    const MAX_SANE: u64 = 1 << 50; // 1 PiB
    (v < MAX_SANE).then_some(v)
}

#[cfg(target_os = "linux")]
mod imp {
    use std::fs;

    use super::{budget_from, field_kb, sane_limit};

    pub fn process_rss_bytes() -> Option<u64> {
        // VmRSS in /proc/self/status is the resident size in kB (what `top`
        // shows as RES). Cheaper than walking smaps and exact enough here.
        field_kb(&fs::read_to_string("/proc/self/status").ok()?, "VmRSS:")
    }

    pub fn total_budget_bytes() -> Option<u64> {
        budget_from(host_mem_total(), cgroup_mem_limit())
    }

    pub fn system_used_bytes() -> Option<u64> {
        // In a memory-limited cgroup, its own usage is the binding figure (and
        // includes the co-located cache Postgres). Otherwise use host usage.
        if let Some(used) = cgroup_used() {
            return Some(used);
        }
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        let total = field_kb(&meminfo, "MemTotal:")?;
        let available = field_kb(&meminfo, "MemAvailable:")?;
        Some(total.saturating_sub(available))
    }

    pub fn system_private_bytes() -> Option<u64> {
        // cgroup: anonymous (private) memory from memory.stat; shared_buffers is
        // counted under `shmem`, not `anon`, so `anon` is already the private pool.
        if cgroup_used().is_some()
            && let Ok(stat) = fs::read_to_string("/sys/fs/cgroup/memory.stat")
            && let Some(anon) = stat.lines().find_map(|l| l.strip_prefix("anon "))
            && let Ok(v) = anon.trim().parse::<u64>()
        {
            return Some(v);
        }
        // host: used − Shmem (shared_buffers lives in Shmem).
        let meminfo = fs::read_to_string("/proc/meminfo").ok()?;
        let total = field_kb(&meminfo, "MemTotal:")?;
        let available = field_kb(&meminfo, "MemAvailable:")?;
        let shmem = field_kb(&meminfo, "Shmem:").unwrap_or(0);
        Some(total.saturating_sub(available).saturating_sub(shmem))
    }

    fn host_mem_total() -> Option<u64> {
        field_kb(&fs::read_to_string("/proc/meminfo").ok()?, "MemTotal:")
    }

    /// cgroup memory limit: v2 `memory.max`, then v1 `memory.limit_in_bytes`.
    fn cgroup_mem_limit() -> Option<u64> {
        if let Ok(s) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
            let s = s.trim();
            if s == "max" {
                return None;
            }
            return s.parse::<u64>().ok().and_then(sane_limit);
        }
        if let Ok(s) = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes") {
            return s.trim().parse::<u64>().ok().and_then(sane_limit);
        }
        None
    }

    /// Current cgroup memory usage, but only when memory is actually limited
    /// (i.e. containerized). `None` on an unlimited/root cgroup so the caller
    /// falls back to host-wide usage.
    fn cgroup_used() -> Option<u64> {
        // cgroup v2: usage is meaningful as a budget only when memory.max is set.
        if let Ok(maxs) = fs::read_to_string("/sys/fs/cgroup/memory.max") {
            if maxs.trim() == "max" {
                return None;
            }
            return fs::read_to_string("/sys/fs/cgroup/memory.current")
                .ok()?
                .trim()
                .parse()
                .ok();
        }
        // cgroup v1: only if the limit is a real (sane) value.
        let limit = fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes").ok()?;
        sane_limit(limit.trim().parse().ok()?)?;
        fs::read_to_string("/sys/fs/cgroup/memory/memory.usage_in_bytes")
            .ok()?
            .trim()
            .parse()
            .ok()
    }
}

#[cfg(all(not(target_os = "linux"), target_os = "macos"))]
mod imp {
    pub fn process_rss_bytes() -> Option<u64> {
        None
    }

    /// Host RAM via `sysctl -n hw.memsize`. macOS has no cgroup limit, so this
    /// is the whole budget. Lets the RAM-relative memo default exercise locally;
    /// the other introspection points stay `None`, so the registration monitor
    /// (which needs `system_used_bytes`) remains disabled on the dev box. Shells
    /// out rather than add a `libc` FFI dep — this prototype path is queried once
    /// at startup.
    pub fn total_budget_bytes() -> Option<u64> {
        let out = std::process::Command::new("sysctl")
            .args(["-n", "hw.memsize"])
            .output()
            .ok()?;
        let size: u64 = String::from_utf8_lossy(&out.stdout).trim().parse().ok()?;
        (size > 0).then_some(size)
    }

    pub fn system_used_bytes() -> Option<u64> {
        None
    }
    pub fn system_private_bytes() -> Option<u64> {
        None
    }
}

#[cfg(all(not(target_os = "linux"), not(target_os = "macos")))]
mod imp {
    pub fn process_rss_bytes() -> Option<u64> {
        None
    }
    pub fn total_budget_bytes() -> Option<u64> {
        None
    }
    pub fn system_used_bytes() -> Option<u64> {
        None
    }
    pub fn system_private_bytes() -> Option<u64> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::{budget_from, field_kb, sane_limit};

    #[test]
    fn test_field_kb_parses_proc_line() {
        let table = "Name:\tpgcache\nVmRSS:\t  123456 kB\nVmHWM:\t 200000 kB\n";
        assert_eq!(field_kb(table, "VmRSS:"), Some(123456 * 1024));
    }

    #[test]
    fn test_field_kb_meminfo() {
        let table = "MemTotal:       16384000 kB\nMemFree:         1000000 kB\n";
        assert_eq!(field_kb(table, "MemTotal:"), Some(16_384_000 * 1024));
    }

    #[test]
    fn test_field_kb_absent_key() {
        assert_eq!(field_kb("MemFree: 100 kB\n", "VmRSS:"), None);
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
    fn test_budget_takes_min_of_host_and_cgroup() {
        assert_eq!(budget_from(Some(64), Some(2)), Some(2));
        assert_eq!(budget_from(Some(64), None), Some(64));
        assert_eq!(budget_from(None, Some(2)), Some(2));
        assert_eq!(budget_from(None, None), None);
    }
}
