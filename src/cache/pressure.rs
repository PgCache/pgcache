//! Box CPU pressure (PGC-554): the reading, the parsers for the kernel files
//! behind it, and the cell the cpu monitor publishes it through for the pool
//! controllers and the registration gate to read wait-free.

use std::sync::atomic::{AtomicU64, Ordering};

/// Box CPU pressure. `avg10` / `avg60` come from the kernel's PSI
/// (`/proc/pressure/cpu`, or the process's own cgroup `cpu.pressure`): the
/// fraction of wall time over the last 10 s and 60 s in which at least one
/// runnable task waited for a CPU (the `some` line; CPU has no system-level
/// `full`). They say *whether* the box is short of CPU and saturate near 1
/// once it is. `waiting` is the magnitude PSI lacks: the average number of
/// tasks waiting for a CPU, box-wide, from the scheduler's run-queue wait
/// time (`/proc/schedstat`), smoothed over a few seconds. It keeps moving at
/// saturation (a 2-vCPU box read 0.95 on PSI at load 9 and at load 34), so
/// the controllers judge their experiments on it. All zero where the files
/// are unavailable, so every consumer behaves as it did before the signal
/// existed.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct CpuPressure {
    pub avg10: f64,
    pub avg60: f64,
    pub waiting: f64,
}

impl CpuPressure {
    /// The higher-pressure reading of two sources, window by window; `None`
    /// only when both are. Used by the Linux reader, which has two sources.
    #[cfg(any(target_os = "linux", test))]
    pub(crate) fn max_of(a: Option<Self>, b: Option<Self>) -> Option<Self> {
        match (a, b) {
            (Some(a), Some(b)) => Some(Self {
                avg10: a.avg10.max(b.avg10),
                avg60: a.avg60.max(b.avg60),
                waiting: a.waiting.max(b.waiting),
            }),
            (a, b) => a.or(b),
        }
    }
}

/// Parse a PSI file's `some` line — `some avg10=1.23 avg60=0.45 avg300=…
/// total=…`, percentages — into fractions. `None` without a complete `some`
/// line.
pub(crate) fn psi_cpu_parse(text: &str) -> Option<CpuPressure> {
    let line = text.lines().find_map(|l| l.strip_prefix("some "))?;
    let field = |key: &str| -> Option<f64> {
        line.split_whitespace()
            .find_map(|kv| kv.strip_prefix(key))
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .map(|percent| (percent / 100.0).min(1.0))
    };
    Some(CpuPressure {
        avg10: field("avg10=")?,
        avg60: field("avg60=")?,
        waiting: 0.0,
    })
}

/// Sum of the per-CPU run-queue wait time in `/proc/schedstat`, nanoseconds:
/// the eighth number on every `cpuN` line, "time spent waiting to run by
/// tasks on this processor". Maintained by the scheduler's delay accounting
/// whether or not `kernel.sched_schedstats` is on. `None` without a `cpu`
/// line. Used by the Linux reader.
#[cfg(any(target_os = "linux", test))]
pub(crate) fn schedstat_wait_ns_parse(text: &str) -> Option<u64> {
    let mut total: Option<u64> = None;
    for line in text.lines() {
        let mut fields = line.split_whitespace();
        let Some(name) = fields.next() else {
            continue;
        };
        if !name.starts_with("cpu") {
            continue;
        }
        let wait: u64 = fields.nth(7)?.parse().ok()?;
        total = Some(total.unwrap_or(0).saturating_add(wait));
    }
    total
}

/// Tasks waiting for a CPU, on average over an interval: the run-queue wait
/// time accrued across all CPUs divided by the interval (one task waiting the
/// whole interval accrues exactly one interval of wait).
pub(crate) fn waiting_tasks(wait_ns_delta: u64, interval_ns: u64) -> f64 {
    if interval_ns == 0 {
        return 0.0;
    }
    // Both are nanosecond totals well under 2^53 over any one interval.
    #[allow(clippy::cast_precision_loss)]
    let ratio = wait_ns_delta as f64 / interval_ns as f64;
    ratio
}

/// PSI `some` shares for the last 10 s and 60 s, as f64 bits in atomics.
#[derive(Default)]
pub struct CpuPressureCell {
    avg10_bits: AtomicU64,
    avg60_bits: AtomicU64,
    waiting_bits: AtomicU64,
}

impl CpuPressureCell {
    pub fn get(&self) -> CpuPressure {
        CpuPressure {
            avg10: f64::from_bits(self.avg10_bits.load(Ordering::Relaxed)),
            avg60: f64::from_bits(self.avg60_bits.load(Ordering::Relaxed)),
            waiting: f64::from_bits(self.waiting_bits.load(Ordering::Relaxed)),
        }
    }

    pub fn set(&self, pressure: CpuPressure) {
        self.avg10_bits
            .store(pressure.avg10.to_bits(), Ordering::Relaxed);
        self.avg60_bits
            .store(pressure.avg60.to_bits(), Ordering::Relaxed);
        self.waiting_bits
            .store(pressure.waiting.to_bits(), Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cell_round_trips_a_reading() {
        let cell = CpuPressureCell::default();
        assert_eq!(cell.get(), CpuPressure::default());
        let reading = CpuPressure {
            avg10: 0.42,
            avg60: 0.17,
            waiting: 2.5,
        };
        cell.set(reading);
        assert_eq!(cell.get(), reading);
    }

    #[test]
    fn test_psi_cpu_parse_reads_some_as_fractions() {
        let text = "some avg10=12.50 avg60=3.25 avg300=0.80 total=21182001832\n\
                    full avg10=0.00 avg60=0.00 avg300=0.00 total=0\n";
        let p = psi_cpu_parse(text).expect("some line parsed");
        assert!((p.avg10 - 0.125).abs() < 1e-9);
        assert!((p.avg60 - 0.0325).abs() < 1e-9);
    }

    #[test]
    fn test_psi_cpu_parse_rejects_incomplete_line() {
        assert_eq!(psi_cpu_parse("full avg10=0.00 avg60=0.00\n"), None);
        assert_eq!(psi_cpu_parse("some avg10=1.00 total=5\n"), None);
        assert_eq!(psi_cpu_parse(""), None);
    }

    #[test]
    fn test_max_of_takes_the_higher_window_from_either_source() {
        let system = CpuPressure {
            avg10: 0.9,
            avg60: 0.2,
            waiting: 3.0,
        };
        let own = CpuPressure {
            avg10: 0.1,
            avg60: 0.6,
            waiting: 1.0,
        };
        assert_eq!(
            CpuPressure::max_of(Some(system), Some(own)),
            Some(CpuPressure {
                avg10: 0.9,
                avg60: 0.6,
                waiting: 3.0,
            })
        );
        assert_eq!(CpuPressure::max_of(None, Some(own)), Some(own));
        assert_eq!(CpuPressure::max_of(Some(system), None), Some(system));
        assert_eq!(CpuPressure::max_of(None, None), None);
    }

    #[test]
    fn test_psi_cpu_parse_clamps_to_one() {
        let p = psi_cpu_parse("some avg10=100.00 avg60=250.00 avg300=0 total=0\n").expect("parsed");
        assert_eq!(p.avg10, 1.0);
        assert_eq!(p.avg60, 1.0);
    }

    #[test]
    fn test_schedstat_wait_sums_the_eighth_number_of_every_cpu_line() {
        let text = "version 17\ntimestamp 4305596104\n\
                    cpu0 0 0 0 0 0 0 34447768830264 296080687436802 255564104\n\
                    domain0 MC 3 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0\n\
                    cpu1 0 0 0 0 0 0 34000000000000 300000000000000 255000000\n";
        assert_eq!(
            schedstat_wait_ns_parse(text),
            Some(296_080_687_436_802 + 300_000_000_000_000)
        );
        assert_eq!(schedstat_wait_ns_parse("version 17\n"), None);
    }

    #[test]
    fn test_waiting_tasks_is_wait_time_per_interval() {
        assert_eq!(waiting_tasks(2_000_000_000, 1_000_000_000), 2.0);
        assert_eq!(waiting_tasks(500_000_000, 1_000_000_000), 0.5);
        assert_eq!(waiting_tasks(5, 0), 0.0);
    }
}
