//! CDC settling: block until pgcache has applied every effect up to a
//! given origin LSN.
//!
//! Between any DML and the next SELECT, the runner captures
//! `pg_current_wal_lsn()` on origin and polls pgcache
//! `/status::cdc.last_applied_lsn` until it reaches that point.
//! `last_applied_lsn` is the only transaction-aligned watermark; the
//! wire-side `received`/`flushed` gauges do not imply effects are
//! visible in the cache.

use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use tokio::time::{Instant, sleep};
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

/// Poll `source` until its `last_applied_lsn` reaches `target`, or fail
/// once `timeout` elapses.
pub async fn settle(source: &impl LsnSource, target: u64, timeout: Duration) -> Result<()> {
    let deadline = Instant::now() + timeout;
    let poll_interval = Duration::from_millis(25);
    loop {
        let applied = source
            .last_applied_lsn()
            .await
            .context("polling /status::cdc.last_applied_lsn")?;
        if applied >= target {
            return Ok(());
        }
        if Instant::now() >= deadline {
            bail!(
                "CDC settling timed out after {timeout:?}: applied LSN {applied} \
                 has not reached target {target}"
            );
        }
        sleep(poll_interval).await;
    }
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
