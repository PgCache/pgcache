//! Box-level CPU pressure monitor (PGC-554): samples the kernel's PSI and
//! run-queue wait once a second and publishes them for the pool controllers
//! and the registration gate. Each of those judges itself by its own queue,
//! so on a shared box all of them ride to their ceilings while the writer,
//! the fold bgworker and the cache PG starve; this is the one input that sees
//! the box.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio_util::sync::CancellationToken;
use tracing::debug;

use crate::cache::pressure::{CpuPressure, waiting_tasks};
use crate::cache::types::CacheStateView;

/// Smoothing of the waiting-tasks reading: a per-second sample weighs half,
/// so the value settles within the controllers' five-tick verify windows yet
/// a single noisy second cannot move it by itself.
const WAITING_EWMA_ALPHA: f64 = 0.5;

pub(super) async fn cpu_monitor(state_view: Arc<CacheStateView>, cancel: CancellationToken) {
    const TICK: Duration = Duration::from_secs(1);
    let mut unavailable_logged = false;
    let mut waiting = WaitingTasks::default();
    loop {
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(TICK) => {}
        }
        let mut pressure = match psi_read() {
            Some(pressure) => pressure,
            None => {
                if !unavailable_logged {
                    debug!("cpu pressure: PSI unavailable; controllers see no box pressure");
                    unavailable_logged = true;
                }
                CpuPressure::default()
            }
        };
        pressure.waiting = fault_waiting().unwrap_or_else(|| waiting.sample());
        state_view.cpu_pressure.set(pressure);
        let handles = &crate::metrics::handles().cache;
        handles.cpu_pressure_avg10.set(pressure.avg10);
        handles.cpu_pressure_avg60.set(pressure.avg60);
        handles.cpu_waiting_tasks.set(pressure.waiting);
    }
}

/// The smoothed waiting-tasks reading from successive run-queue wait totals.
#[derive(Default)]
struct WaitingTasks {
    previous: Option<(Instant, u64)>,
    ewma: f64,
}

impl WaitingTasks {
    fn sample(&mut self) -> f64 {
        let Some(total) = crate::memory::cpu_runqueue_wait_ns() else {
            return 0.0;
        };
        let now = Instant::now();
        if let Some((at, before)) = self.previous {
            let interval = u64::try_from(now.duration_since(at).as_nanos()).unwrap_or(u64::MAX);
            let sample = waiting_tasks(total.saturating_sub(before), interval);
            self.ewma += WAITING_EWMA_ALPHA * (sample - self.ewma);
        }
        self.previous = Some((now, total));
        self.ewma
    }
}

fn psi_read() -> Option<CpuPressure> {
    fault_psi().or_else(crate::memory::cpu_pressure)
}

/// Test-only (fault-injection feature): a file named by
/// `PGCACHE_FAULT_CPU_PRESSURE_FILE` stands in for the kernel — a PSI `some`
/// line, and optionally a `waiting <tasks>` line — so a test can raise and
/// lower box pressure at will.
#[cfg(feature = "fault-injection")]
fn fault_file() -> Option<String> {
    let path = std::env::var("PGCACHE_FAULT_CPU_PRESSURE_FILE").ok()?;
    std::fs::read_to_string(path).ok()
}
#[cfg(feature = "fault-injection")]
fn fault_psi() -> Option<CpuPressure> {
    crate::cache::pressure::psi_cpu_parse(&fault_file()?)
}
#[cfg(feature = "fault-injection")]
fn fault_waiting() -> Option<f64> {
    fault_file()?
        .lines()
        .find_map(|l| l.strip_prefix("waiting "))
        .and_then(|v| v.trim().parse::<f64>().ok())
}
#[cfg(not(feature = "fault-injection"))]
fn fault_psi() -> Option<CpuPressure> {
    None
}
#[cfg(not(feature = "fault-injection"))]
fn fault_waiting() -> Option<f64> {
    None
}
