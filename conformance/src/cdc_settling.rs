//! CDC settling: block until pgcache has applied every effect up to a
//! given origin LSN.
//!
//! Between any DML and the next SELECT, the runner captures
//! `pg_current_wal_lsn()` on origin and polls pgcache
//! `/status::cdc.last_applied_lsn` until it reaches that point.
//! `last_applied_lsn` is the only transaction-aligned watermark; the
//! wire-side `received`/`flushed` gauges do not imply effects are
//! visible in the cache.

use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio_postgres::types::PgLsn;

/// A source of pgcache's current `last_applied_lsn` (the `/status`
/// client implements this).
pub trait LsnSource {
    fn last_applied_lsn(&self) -> impl Future<Output = Result<u64>> + Send;
}

/// Parse a PostgreSQL LSN (`"0/3A1B2C00"`) into a `u64`, via the
/// `postgres-types` `PgLsn` parser (same `(hi << 32) | lo` encoding).
pub fn lsn_parse(s: &str) -> Result<u64> {
    s.trim()
        .parse::<PgLsn>()
        .map(u64::from)
        .map_err(|_| anyhow!("invalid PostgreSQL LSN: {s:?}"))
}

/// Poll `source` until its `last_applied_lsn` reaches `target`, tolerating
/// arbitrarily long drains as long as the watermark keeps advancing: the
/// deadline is a STALL bound, reset on every observed advance. Detects a
/// wedged writer without bounding healthy drain time — deep backlogs are
/// legitimate under cross-frame batching (PGC-242), where drain duration
/// scales with how far the load outran the writer.
pub async fn settle_while_progressing(
    source: &impl LsnSource,
    target: u64,
    stall_timeout: Duration,
) -> Result<()> {
    let mut last_seen = None;
    let mut stall_start = std::time::Instant::now();
    loop {
        let applied = source
            .last_applied_lsn()
            .await
            .context("polling /status::cdc.last_applied_lsn")?;
        if applied >= target {
            return Ok(());
        }
        if last_seen != Some(applied) {
            last_seen = Some(applied);
            stall_start = std::time::Instant::now();
        } else if stall_start.elapsed() >= stall_timeout {
            return Err(anyhow!(
                "CDC apply stalled for {stall_timeout:?}: applied LSN {applied}                  has not reached target {target}"
            ));
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// Poll `source` until its `last_applied_lsn` reaches `target`, or fail
/// once `timeout` elapses.
pub async fn settle(source: &impl LsnSource, target: u64, timeout: Duration) -> Result<()> {
    crate::poll::poll_until(timeout, Duration::from_millis(25), || async {
        let applied = source
            .last_applied_lsn()
            .await
            .context("polling /status::cdc.last_applied_lsn")?;
        if applied >= target {
            Ok(ControlFlow::Break(()))
        } else {
            Ok(ControlFlow::Continue(format!(
                "CDC settling timed out after {timeout:?}: applied LSN {applied} \
                 has not reached target {target}"
            )))
        }
    })
    .await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_canonical_lsn() {
        assert_eq!(lsn_parse("0/0").unwrap(), 0);
        assert_eq!(lsn_parse("0/3A1B2C00").unwrap(), 0x3A1B2C00);
        assert_eq!(lsn_parse("1/0").unwrap(), 1 << 32);
        assert_eq!(
            lsn_parse("16/B374D848").unwrap(),
            (0x16u64 << 32) | 0xB374D848
        );
    }

    #[test]
    fn rejects_malformed_lsn() {
        assert!(lsn_parse("deadbeef").is_err());
        assert!(lsn_parse("zz/00").is_err());
        assert!(lsn_parse("0/").is_err());
    }

    struct FixedSource(u64);
    impl LsnSource for FixedSource {
        async fn last_applied_lsn(&self) -> Result<u64> {
            Ok(self.0)
        }
    }

    /// Advances by one on every poll until `cap`, then freezes.
    struct SteppingSource {
        v: std::sync::atomic::AtomicU64,
        cap: u64,
    }
    impl LsnSource for SteppingSource {
        async fn last_applied_lsn(&self) -> Result<u64> {
            use std::sync::atomic::Ordering;
            let v = self.v.load(Ordering::Relaxed);
            if v < self.cap {
                self.v.store(v + 1, Ordering::Relaxed);
            }
            Ok(v)
        }
    }

    #[tokio::test]
    async fn progressing_settle_outlasts_a_fixed_deadline() {
        // 30 polls × ~50ms ≈ 1.5s of healthy drain with a 200ms stall bound:
        // a fixed 200ms deadline would fail; progress resets the stall clock.
        let src = SteppingSource {
            v: std::sync::atomic::AtomicU64::new(0),
            cap: 30,
        };
        settle_while_progressing(&src, 30, Duration::from_millis(200))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn progressing_settle_fails_on_stall() {
        let src = SteppingSource {
            v: std::sync::atomic::AtomicU64::new(0),
            cap: 5,
        };
        let err = settle_while_progressing(&src, 999, Duration::from_millis(200))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("stalled"), "{err}");
    }

    #[tokio::test]
    async fn settle_returns_when_already_caught_up() {
        let src = FixedSource(100);
        settle(&src, 50, Duration::from_secs(1)).await.unwrap();
    }

    #[tokio::test]
    async fn settle_times_out_when_behind() {
        let src = FixedSource(10);
        let err = settle(&src, 999, Duration::from_millis(80))
            .await
            .unwrap_err();
        assert!(err.to_string().contains("CDC settling timed out"));
    }
}
