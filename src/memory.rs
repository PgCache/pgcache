//! System memory introspection for the registration memory budget (Layer 2).
//!
//! Linux is the production/Docker target: RSS from `/proc/self/status`, the
//! budget from `min(host MemTotal, cgroup memory limit)` so a constrained
//! container budgets against its limit rather than host RAM. On non-Linux
//! (dev/test) detection returns `None` and the caller leaves registration
//! unthrottled.

/// Current resident set size (RSS) of this process in bytes, or `None` if it
/// can't be read (non-Linux, or `/proc` unavailable).
pub fn process_rss_bytes() -> Option<u64> {
    sys::process_rss_bytes()
}

/// Total memory available to this process in bytes: the smaller of host RAM and
/// any cgroup memory limit. `None` if undetectable.
pub fn total_budget_bytes() -> Option<u64> {
    sys::total_budget_bytes()
}

/// Total memory *currently used* on this machine, in bytes. In a memory-limited
/// cgroup this is the cgroup's usage (`memory.current`); otherwise it is host
/// `MemTotal - MemAvailable`. Either way it counts pgcache **and** the cache
/// Postgres it manages — which share the same box/container — so the registration
/// budget covers the whole footprint, not just pgcache's own RSS. `None` if
/// undetectable (non-Linux).
pub fn system_used_bytes() -> Option<u64> {
    sys::system_used_bytes()
}

/// Outcome of one memory-monitor sample.
pub struct ThrottleDecision {
    /// Whether new-query registration should be throttled (forwarded to origin).
    pub throttled: bool,
    /// The used-memory high-water mark above which registration throttles.
    pub high_mark: u64,
}

/// Pure throttle evaluation, driving `cache::runtime::memory_monitor`. `used` is
/// whole-system used memory (pgcache + the cache Postgres). The ceiling is 80%
/// of `total_ram`, only ever *lowered* by `memory_limit`; the memo's unfilled
/// budget is reserved on top; and a 2%-of-ceiling hysteresis band (only relevant
/// when already throttled) keeps the flag from flapping.
pub fn throttle_evaluate(
    used: u64,
    total_ram: u64,
    memory_limit: Option<u64>,
    memo_budget: u64,
    memo_used: u64,
    currently_throttled: bool,
) -> ThrottleDecision {
    let default_ceiling = total_ram / 5 * 4; // 80%, exact-division form
    let ceiling = memory_limit.map_or(default_ceiling, |l| l.min(default_ceiling));
    let memo_reserve = memo_budget.saturating_sub(memo_used);
    let high_mark = ceiling.saturating_sub(memo_reserve);
    let low_mark = high_mark.saturating_sub(ceiling / 100 * 2);
    let throttled = if currently_throttled {
        used > low_mark
    } else {
        used > high_mark
    };
    ThrottleDecision {
        throttled,
        high_mark,
    }
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
mod sys {
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

#[cfg(not(target_os = "linux"))]
mod sys {
    pub fn process_rss_bytes() -> Option<u64> {
        None
    }
    pub fn total_budget_bytes() -> Option<u64> {
        None
    }
    pub fn system_used_bytes() -> Option<u64> {
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

    const GB: u64 = 1 << 30;

    #[test]
    fn test_throttle_below_budget_not_throttled() {
        let d = super::throttle_evaluate(GB, 10 * GB, None, 0, 0, false);
        assert_eq!(d.high_mark, 8 * GB); // 80% of 10 GB
        assert!(!d.throttled);
    }

    #[test]
    fn test_throttle_above_eighty_percent() {
        let d = super::throttle_evaluate(9 * GB, 10 * GB, None, 0, 0, false);
        assert!(d.throttled);
    }

    #[test]
    fn test_memory_limit_lowers_ceiling() {
        // limit 2 GB pulls the ceiling well below 80% of 10 GB.
        let d = super::throttle_evaluate(3 * GB, 10 * GB, Some(2 * GB), 0, 0, false);
        assert_eq!(d.high_mark, 2 * GB);
        assert!(d.throttled);
    }

    #[test]
    fn test_memory_limit_cannot_raise_ceiling() {
        // A limit above 80% of RAM is ignored; ceiling stays at 8 GB.
        let d = super::throttle_evaluate(7 * GB, 10 * GB, Some(100 * GB), 0, 0, false);
        assert_eq!(d.high_mark, 8 * GB);
        assert!(!d.throttled);
    }

    #[test]
    fn test_memo_budget_reserved() {
        // Reserving a 4 GB memo budget pulls the high mark from 8 GB to 4 GB.
        let d = super::throttle_evaluate(5 * GB, 10 * GB, None, 4 * GB, 0, false);
        assert_eq!(d.high_mark, 4 * GB);
        assert!(d.throttled);
        // Once the memo has filled its budget, nothing is reserved.
        let d = super::throttle_evaluate(5 * GB, 10 * GB, None, 4 * GB, 4 * GB, false);
        assert_eq!(d.high_mark, 8 * GB);
        assert!(!d.throttled);
    }

    #[test]
    fn test_hysteresis_band() {
        // high_mark = 8 GB, low_mark = 8 GB - 2% of 8 GB = 7.84 GB.
        let rss = 8 * GB - GB / 10; // 7.9 GB, between low and high marks
        // Not yet throttled: below the high mark, stays off.
        assert!(!super::throttle_evaluate(rss, 10 * GB, None, 0, 0, false).throttled);
        // Already throttled: above the low mark, stays on.
        assert!(super::throttle_evaluate(rss, 10 * GB, None, 0, 0, true).throttled);
    }
}
