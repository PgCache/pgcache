//! Bounded condition-polling: the shared `deadline → loop → check-done →
//! check-deadline → sleep` skeleton behind `cdc_settling::settle` and
//! `status_client::cache_settle`.
//!
//! Centralizing it keeps the easy-to-get-wrong ordering — *done is checked
//! before the deadline*, so a poll that becomes satisfied exactly at the
//! timeout still succeeds — in one place.
//!
//! Deliberately NOT used by `status_client::fetch` (a retry-returning-value
//! idiom: it has no done-predicate, it returns as soon as the response
//! isn't a transient 5xx) or the `runner.rs` PGC-148 retry loop (its body
//! does breaker-checked snapshots and non-local control flow —
//! `continue 'records` / early `ControlFlow::Break` — that a closure can't
//! express without a worse re-dispatch enum). Forcing those in would be a
//! leaky abstraction.

use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Result, bail};
use tokio::time::{Instant, sleep};

/// Call `step` every `interval` until it returns `Break` (the awaited
/// condition holds), or fail once `timeout` elapses.
///
/// `step` owns the per-poll fallible work and the decision: `Break(())`
/// = satisfied; `Continue(msg)` = still pending, where `msg` is the
/// caller-formatted error to fail with if the deadline passes (the caller
/// owns the full wording, so per-site message contracts are preserved).
/// `step`'s own `Err` propagates immediately.
pub async fn poll_until<F, Fut>(timeout: Duration, interval: Duration, mut step: F) -> Result<()>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<ControlFlow<(), String>>>,
{
    let deadline = Instant::now() + timeout;
    loop {
        // Done is checked before the deadline: a poll satisfied exactly at
        // the timeout still succeeds.
        match step().await? {
            ControlFlow::Break(()) => return Ok(()),
            ControlFlow::Continue(pending_msg) => {
                if Instant::now() >= deadline {
                    bail!("{pending_msg}");
                }
                sleep(interval).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::cell::Cell;

    #[tokio::test]
    async fn returns_ok_when_step_breaks() {
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || async {
            Ok(ControlFlow::Break(()))
        })
        .await
        .unwrap();
    }

    #[tokio::test]
    async fn breaks_on_a_later_poll() {
        let n = Cell::new(0u32);
        poll_until(Duration::from_secs(1), Duration::from_millis(1), || async {
            n.set(n.get() + 1);
            if n.get() >= 3 {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue("pending".to_owned()))
            }
        })
        .await
        .unwrap();
        assert_eq!(n.get(), 3);
    }

    #[tokio::test]
    async fn times_out_with_the_callers_message() {
        let err = poll_until(
            Duration::from_millis(40),
            Duration::from_millis(5),
            || async { Ok(ControlFlow::Continue("custom detail".to_owned())) },
        )
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "custom detail");
    }

    #[tokio::test]
    async fn propagates_step_error() {
        let err = poll_until(Duration::from_secs(1), Duration::from_millis(1), || async {
            anyhow::bail!("step blew up")
        })
        .await
        .unwrap_err();
        assert_eq!(err.to_string(), "step blew up");
    }
}
