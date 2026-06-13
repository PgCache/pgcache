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

/// Whole-system *private* (anonymous, non-shared) used memory in bytes — used
/// memory minus shared memory, where the cache Postgres `shared_buffers` lives.
/// This is the *growing* pool the count cap targets (pgcache in-process state +
/// the cache-PG plan cache), measured without any privileged per-backend access.
/// `None` if undetectable (non-Linux). Note: if the cache PG uses huge pages,
/// `shared_buffers` won't be in `Shmem`, so this over-counts (cap stays
/// conservative — the safe direction).
pub fn system_private_bytes() -> Option<u64> {
    sys::system_private_bytes()
}

/// Outcome of one memory-monitor sample.
pub struct ThrottleDecision {
    /// Whether new-query registration should be throttled (forwarded to origin).
    pub throttled: bool,
    /// The used-memory high-water mark above which registration throttles.
    pub high_mark: u64,
    /// The effective memory ceiling (80% RAM, lowered by `memory_limit`), before
    /// the memo reserve. The count cap budgets against this.
    pub ceiling: u64,
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
        ceiling,
    }
}

/// EWMA smoothing factor for the per-query derivative samples. The peak tracks
/// the *EWMA*, not raw samples, so a single noisy chunk (a coincident page-cache
/// blip read over one [`COUNT_CAP_MIN_GROWTH`] window) can't reach the peak and
/// collapse the cap — only a sustained rise walks the EWMA up.
pub const COUNT_CAP_EWMA_ALPHA: f64 = 0.3;
/// Per-tick decay of the peak per-query cost — a backstop for gradual workload
/// drift, *not* the primary recovery path (connection recycling resets the peak
/// when it actually reclaims memory, PGC-251 Slice 1d). At ~0.9994 per 500ms tick
/// the half-life is ~10 minutes. Smoothing the EWMA input is what kills the limit
/// cycle, so this no longer needs to be slow.
pub const COUNT_CAP_PEAK_DECAY: f64 = 0.9994;
/// The cap floor — never evict below this many queries (also a sanity floor on a
/// huge measured marginal).
pub const COUNT_CAP_MIN_QUERIES: usize = 1000;
/// Minimum count growth since the last sample before a new derivative is taken.
/// A small Δcount divides into private-memory measurement noise and yields a
/// wild per-query figure, so accumulate at least this many new queries first.
pub const COUNT_CAP_MIN_GROWTH: usize = 2000;
/// Max fractional increase of the published count cap per monitor tick. Limiting
/// only *increases* (tightening is always safe) means one bad sample — e.g. a
/// churn-contaminated derivative — can't spike the cap before the next clean
/// sample corrects it; the cap ramps toward the corrected target instead.
pub const CAP_MAX_INCREASE_FRACTION: f64 = 0.5;

/// Result of one count-cap evaluation.
pub struct CountCapDecision {
    /// Max registered queries that fit the memory budget. `usize::MAX` means
    /// "no cap" (insufficient signal, or memory not detectable).
    pub cap: usize,
    /// Updated EWMA of the per-query derivative, carried to the next sample.
    pub marginal_ewma: f64,
    /// Updated decaying peak of the EWMA, carried to the next tick. The caller
    /// may reset it to `marginal_ewma` after a full pool recycle.
    pub peak_marginal: f64,
    /// Whether a growth sample was taken (Δcount ≥ [`COUNT_CAP_MIN_GROWTH`]); the
    /// caller advances its high-water reference to the current point when set.
    pub sampled: bool,
}

/// Derive the cached-query count cap from the measured **private** per-query
/// footprint (see `cache::runtime::memory_monitor` and PGC-251).
///
/// `private_used` excludes `shared_buffers` (it lives in shared memory / `Shmem`),
/// so it tracks only the *growing* pool — pgcache's in-process state plus the
/// cache Postgres plan cache — measured for the running workload, no constants or
/// `pgcache_pgrx`.
///
/// The per-query cost is the **incremental derivative** `Δprivate / Δcount`,
/// sampled only while the count is *growing*. This is deliberately not the ratio
/// `private / count`: private RSS is sticky (eviction doesn't return it to the
/// OS), so dividing sticky private by a *shrinking* post-eviction count inflates
/// the estimate, collapsing the cap toward the floor — the same closed-loop
/// death-spiral a controller on `used` would hit. The derivative is immune: when
/// the count is held at the cap (`Δcount ≈ 0`) there's simply no sample, and it
/// also excludes one-time fixed costs (they don't recur per added query).
///
/// The derivative samples are smoothed into an **EWMA**, and a **decaying peak of
/// the EWMA** is tracked: the per-query cost rises as a plan propagates across the
/// serve pool, so the peak is the measured fully-propagated worst case, while the
/// EWMA stops a single noisy chunk from reaching it. `shared_buffers` is reserved
/// separately (it fills toward its fixed config size).
///
/// The cap is anchored on the **live** private reading rather than a fixed
/// cold-start baseline: `cap = count + (ceiling − shared_buffers − private) / peak`
/// (floored at [`COUNT_CAP_MIN_QUERIES`]). When private grows as modeled this is
/// algebraically identical to the fixed-baseline form `(budget − baseline)/peak`,
/// but it self-corrects when the fixed floor drifts up (a prior heavy workload
/// warms catcache/arenas) or RSS is sticky after eviction: as private approaches
/// the budget the cap converges to `count` (no room to grow), and past the budget
/// the cap drops below `count`, forcing eviction until recycling reclaims memory.
/// It also cleanly separates recovery paths — recycling reclaiming memory lifts
/// the cap automatically (the anchor moved), while the caller's probe re-learns
/// only the *slope* after a workload-cost change. Returns `usize::MAX` (no cap)
/// until a growth sample exists. PGC-251 refinement 2.
#[allow(clippy::too_many_arguments)] // current + high-water + ewma/peak state
pub fn count_cap_evaluate(
    private_used: u64,
    hw_private: u64,
    count: usize,
    hw_count: usize,
    ceiling: u64,
    shared_buffers: u64,
    prev_marginal_ewma: f64,
    prev_peak_marginal: f64,
) -> CountCapDecision {
    // Sample only on a meaningful growth chunk past the last high-water: requires
    // Δcount ≥ MIN_GROWTH (else noise dominates) and that private actually grew.
    // Plateau/eviction churn never advances past the high-water, so no spurious
    // sample from the eviction-shrunk count.
    let mut ewma = prev_marginal_ewma;
    let mut sampled = false;
    if count >= hw_count + COUNT_CAP_MIN_GROWTH && private_used > hw_private {
        #[allow(clippy::cast_precision_loss)] // bytes / count, well under 2^52
        let sample = (private_used - hw_private) as f64 / (count - hw_count) as f64;
        ewma = if prev_marginal_ewma > 0.0 {
            COUNT_CAP_EWMA_ALPHA * sample + (1.0 - COUNT_CAP_EWMA_ALPHA) * prev_marginal_ewma
        } else {
            sample
        };
        sampled = true;
    }
    // Peak of the EWMA, decayed every tick (backstop for gradual drift).
    let peak = (prev_peak_marginal * COUNT_CAP_PEAK_DECAY).max(ewma);
    let budget = ceiling.saturating_sub(shared_buffers);
    let cap = if peak <= 0.0 {
        usize::MAX
    } else {
        // cap = count + (budget − private)/peak, anchored on the live private
        // reading. `remaining` goes negative once private exceeds the budget,
        // pulling the cap below the current count to force eviction.
        #[allow(clippy::cast_precision_loss)]
        let remaining = budget as f64 - private_used as f64;
        #[allow(
            clippy::cast_precision_loss,
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss
        )]
        let c = (count as f64 + remaining / peak).max(0.0) as usize;
        c.max(COUNT_CAP_MIN_QUERIES)
    };
    CountCapDecision {
        cap,
        marginal_ewma: ewma,
        peak_marginal: peak,
        sampled,
    }
}

/// Rate-limit an *increase* of the published cap to [`CAP_MAX_INCREASE_FRACTION`]
/// of the previous value. Decreases (tightening is always safe) and the
/// cold-start uncapped (`usize::MAX`) → finite step pass through unchanged. The
/// caller applies the probe bump separately and exempts it (it must grow by
/// `MIN_GROWTH` to re-sample). PGC-251 Slice 1e.
pub fn cap_rate_limit(target: usize, prev: Option<usize>) -> usize {
    match prev {
        Some(p) if p != usize::MAX && target > p => {
            #[allow(
                clippy::cast_precision_loss,
                clippy::cast_possible_truncation,
                clippy::cast_sign_loss
            )]
            let step = (p as f64 * CAP_MAX_INCREASE_FRACTION) as usize;
            target.min(p.saturating_add(step))
        }
        _ => target,
    }
}

/// Fraction of total cache-disk capacity kept free as a reserve — the cache is
/// auto-evicted to preserve this much headroom. PGC-251 Slice 2.
pub const DISK_RESERVE_FRACTION: f64 = 0.05;
/// Absolute cap on the disk reserve: enough headroom to keep the box usable,
/// without writing off a large slice of big volumes (10% of a ~1 TiB laptop
/// disk left the cache permanently empty). PGC-251 Slice 2.
pub const DISK_RESERVE_CAP: u64 = 10 << 30; // 10 GiB
/// Absolute floor on the disk reserve, regardless of total capacity, so a small
/// volume still keeps a meaningful buffer. PGC-251 Slice 2.
pub const DISK_RESERVE_FLOOR: u64 = 1 << 30; // 1 GiB

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

/// Minimum free space to keep on the cache volume: `clamp(fraction·total, floor,
/// cap)`. Disk eviction engages when live free space drops below this. Read from
/// statvfs each tick; no cache-size measurement needed (PGC-251 Slice 2, PGC-276).
#[allow(
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss
)]
pub fn disk_reserve_auto(total: u64) -> u64 {
    ((total as f64 * DISK_RESERVE_FRACTION) as u64).clamp(DISK_RESERVE_FLOOR, DISK_RESERVE_CAP)
}

/// Resolve the effective cache-volume usage cap: an explicit `disk_limit`
/// config, else auto-derived to keep [`disk_reserve_auto`] free
/// (`total − reserve`). The single place the optional config is defaulted, so
/// the rest of the writer works with a concrete limit (PGC-276).
pub fn disk_limit_resolve(total: u64, configured: Option<usize>) -> u64 {
    configured.map_or_else(|| total.saturating_sub(disk_reserve_auto(total)), |l| l as u64)
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

    const MB: u64 = 1 << 20;

    // Args: (private, hw_private, count, hw_count, ceiling, shared_buffers,
    //        prev_marginal_ewma, prev_peak_marginal).

    #[test]
    fn test_count_cap_no_sample_without_growth() {
        // Δcount = 0 → no derivative sample; with no prior peak, cap is uncapped.
        let d = super::count_cap_evaluate(2 * GB, 2 * GB, 50_000, 50_000, 8 * GB, 0, 0.0, 0.0);
        assert_eq!(d.cap, usize::MAX);
        assert!(!d.sampled);
        assert_eq!(d.peak_marginal, 0.0);
    }

    #[test]
    fn test_count_cap_no_sample_below_min_growth() {
        // Δcount = 100 (< MIN_GROWTH) → noise-prone, discarded: no sample.
        let d = super::count_cap_evaluate(2 * GB, GB, 100, 0, 8 * GB, 0, 0.0, 0.0);
        assert!(!d.sampled);
        assert_eq!(d.cap, usize::MAX);
    }

    #[test]
    fn test_count_cap_derivative_seeds_ewma() {
        // Δprivate = 1 GB over Δcount = 2^20 → 1024 B/query, seeds EWMA + peak.
        // private = 1 GB = peak·count exactly (baseline 0), so the live anchor
        // gives cap = count + (8 GiB − 1 GiB)/1024 = 2^20 + 7·2^20 = 2^23 —
        // identical to the fixed-baseline form when private grows as modeled.
        let d = super::count_cap_evaluate(GB, 0, 1 << 20, 0, 8 * GB, 0, 0.0, 0.0);
        assert!(d.sampled);
        assert!((d.marginal_ewma - 1024.0).abs() < 1e-6);
        assert!((d.peak_marginal - 1024.0).abs() < 1e-6);
        assert_eq!(d.cap, 1 << 23);
    }

    #[test]
    fn test_count_cap_ewma_smooths_spike() {
        // A single 4× spike (sample 4096 vs prev EWMA 1024) only moves the EWMA
        // to 0.3·4096 + 0.7·1024 = 1945.6 — not to 4096 — so the cap can't
        // collapse on one noisy chunk. peak follows the (smoothed) EWMA up.
        let d = super::count_cap_evaluate(4 * GB, 0, 1 << 20, 0, 8 * GB, 0, 1024.0, 1024.0);
        assert!((d.marginal_ewma - 1945.6).abs() < 0.1);
        assert!((d.peak_marginal - 1945.6).abs() < 0.1);
    }

    #[test]
    fn test_count_cap_peak_holds_above_lower_ewma() {
        // A lower sample drags the EWMA down, but the peak holds (only decaying),
        // so the cap stays conservative. sample 512, prev EWMA 1024 →
        // EWMA = 0.3·512 + 0.7·1024 = 870.4; peak = max(1024·decay, 870.4).
        let d = super::count_cap_evaluate(GB / 2, 0, 1 << 20, 0, 8 * GB, 0, 1024.0, 1024.0);
        assert!((d.marginal_ewma - 870.4).abs() < 0.1);
        let expected_peak = 1024.0 * super::COUNT_CAP_PEAK_DECAY;
        assert!((d.peak_marginal - expected_peak).abs() < 1e-6);
    }

    #[test]
    fn test_count_cap_stable_under_eviction() {
        // The death-spiral guard: sticky private while the count is driven *down*
        // by eviction stays below the high-water → NO sample; EWMA unchanged, peak
        // only decays. (count 1000 << hw_count 20000.)
        let d = super::count_cap_evaluate(2 * GB, 2 * GB, 1_000, 20_000, 8 * GB, 0, 1024.0, 1024.0);
        assert!(!d.sampled);
        assert!((d.marginal_ewma - 1024.0).abs() < 1e-6);
        // peak = max(prev_peak·decay, ewma) = max(1023.4, 1024) = 1024: the
        // unchanged EWMA holds it up — no collapse from the eviction-shrunk count.
        assert!((d.peak_marginal - 1024.0).abs() < 1e-6);
    }

    #[test]
    fn test_count_cap_reserves_shared_buffers() {
        // shared_buffers 4 GB halves the 8 GB ceiling's headroom → cap halves.
        // cap = 2^20 + (8 GiB − 4 GiB − 1 GiB)/1024 = 2^20 + 3·2^20 = 2^22.
        let d = super::count_cap_evaluate(GB, 0, 1 << 20, 0, 8 * GB, 4 * GB, 0.0, 0.0);
        assert_eq!(d.cap, 1 << 22);
    }

    #[test]
    fn test_count_cap_floored_at_min_queries() {
        // 4 GB / 2000 = 2 MB/query; live private 4 GB already exceeds the 256 MB
        // budget → remaining negative → cap pulled below count, clamped to floor.
        let d = super::count_cap_evaluate(4 * GB, 0, 2000, 0, 256 * MB, 0, 0.0, 0.0);
        assert_eq!(d.cap, super::COUNT_CAP_MIN_QUERIES);
    }

    #[test]
    fn test_count_cap_live_anchor_holds_at_budget() {
        // Refinement 2: live private exactly at the private budget (8 GiB, no
        // shared_buffers) → zero remaining headroom → cap == current count, where
        // a stale cold-start baseline would have kept admitting. No new sample.
        let d =
            super::count_cap_evaluate(8 * GB, 8 * GB, 10_000, 10_000, 8 * GB, 0, 1024.0, 1024.0);
        assert!(!d.sampled);
        assert_eq!(d.cap, 10_000);
    }

    #[test]
    fn test_count_cap_over_budget_forces_eviction() {
        // Refinement 2: live private (9 GiB) exceeds the 8 GiB budget → remaining
        // negative → cap drops below the current count, forcing eviction until
        // recycling reclaims memory. 10_000 − 1 GiB/1024 < 0 → clamped to floor.
        let d =
            super::count_cap_evaluate(9 * GB, 9 * GB, 10_000, 10_000, 8 * GB, 0, 1024.0, 1024.0);
        assert_eq!(d.cap, super::COUNT_CAP_MIN_QUERIES);
    }

    #[test]
    fn test_count_cap_tracks_drifted_floor() {
        // Refinement 2: the fixed floor drifted up — live private is 7.5 GiB with
        // only 10k queries cached (a stale baseline would still see headroom for
        // far more). The live anchor caps near the current count: remaining =
        // (8 − 7.5) GiB / 1024 = 524_288 → cap = 10_000 + 524_288.
        let d = super::count_cap_evaluate(
            7680 * MB,
            7680 * MB,
            10_000,
            10_000,
            8 * GB,
            0,
            1024.0,
            1024.0,
        );
        assert_eq!(d.cap, 534_288);
    }

    #[test]
    fn test_cap_rate_limit_increase_clamped() {
        // A spike (target 141797) is clamped to prev + 50% = 5973 + 2986 = 8959.
        assert_eq!(super::cap_rate_limit(141_797, Some(5973)), 8959);
    }

    #[test]
    fn test_cap_rate_limit_increase_under_step_passes() {
        // Within the +50% step → unchanged.
        assert_eq!(super::cap_rate_limit(5000, Some(4000)), 5000);
    }

    #[test]
    fn test_cap_rate_limit_decrease_immediate() {
        // Tightening is always safe — passes through.
        assert_eq!(super::cap_rate_limit(3000, Some(20000)), 3000);
    }

    #[test]
    fn test_cap_rate_limit_first_cap_exempt() {
        // Cold start (no prior) establishes the cap fully.
        assert_eq!(super::cap_rate_limit(13000, None), 13000);
    }

    #[test]
    fn test_cap_rate_limit_uncapped_prev_exempt() {
        // MAX → finite is a loosening-to-capped *decrease*; exempt.
        assert_eq!(super::cap_rate_limit(13000, Some(usize::MAX)), 13000);
    }

    #[test]
    fn test_disk_reserve_fraction() {
        // 100 GiB total → reserve = clamp(5%, 1 GiB, 10 GiB) = 5 GiB.
        assert_eq!(super::disk_reserve_auto(100 * GB), 5 * GB);
    }

    #[test]
    fn test_disk_reserve_cap_applies() {
        // 400 GiB total → 5% = 20 GiB > 10 GiB cap → reserve = 10 GiB. Without
        // the cap a mostly-full large volume would never stop evicting.
        assert_eq!(super::disk_reserve_auto(400 * GB), 10 * GB);
    }

    #[test]
    fn test_disk_reserve_floor_applies() {
        // 4 GiB total → 5% = 0.2 GiB < 1 GiB floor → reserve = 1 GiB.
        assert_eq!(super::disk_reserve_auto(4 * GB), GB);
    }
}
