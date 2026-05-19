//! HTTP client for pgcache's `/status` endpoint.
//!
//! Routing assertions are driven entirely by per-fingerprint
//! `hit_count`/`miss_count` deltas, and CDC settling by
//! `cdc.last_applied_lsn`, both read from `/status`.

use std::collections::HashMap;
use std::ops::ControlFlow;
use std::time::Duration;

use anyhow::{Context, Result};
use pgcache_lib::cache::StatusResponse;
use tokio::time::{Instant, sleep};

use crate::cdc_settling::LsnSource;

// Mirror of pgcache's serialized `QueryStatusData.state` values that mean
// the query is still mid-population — a cross-crate string contract.
const STATE_LOADING: &str = "Loading";
const STATE_PENDING_PREFIX: &str = "Pending";

/// A point-in-time view of `/status` reduced to what the harness asserts on.
#[derive(Debug, Clone, Default)]
pub struct StatusSnapshot {
    pub last_applied_lsn: u64,
    /// Per-fingerprint counters, keyed by `QueryStatusData.fingerprint`.
    pub queries: HashMap<u64, QueryCounters>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct QueryCounters {
    pub hit_count: u64,
    pub miss_count: u64,
}

pub struct StatusClient {
    status_url: String,
    http: reqwest::Client,
}

impl StatusClient {
    pub fn new(status_url: impl Into<String>) -> Self {
        Self {
            status_url: status_url.into(),
            http: reqwest::Client::new(),
        }
    }

    pub fn status_url(&self) -> &str {
        &self.status_url
    }

    async fn fetch(&self) -> Result<StatusResponse> {
        // pgcache's single-threaded cache writer serves `/status`; when it's
        // briefly busy (CDC apply / MV build) it can miss the handler's 2s
        // budget and return 503. That's transient — retry within a bounded
        // window before giving up.
        const RETRY_BUDGET: Duration = Duration::from_secs(5);
        const RETRY_BACKOFF: Duration = Duration::from_millis(100);
        let deadline = Instant::now() + RETRY_BUDGET;

        loop {
            let resp = self
                .http
                .get(&self.status_url)
                .send()
                .await
                .with_context(|| format!("GET {}", self.status_url))?;

            if resp.status().is_server_error() && Instant::now() < deadline {
                sleep(RETRY_BACKOFF).await;
                continue;
            }

            return resp
                .error_for_status()
                .context("/status returned a non-success status")?
                .json()
                .await
                .context("decoding /status JSON");
        }
    }

    /// Fetch `/status` and reduce it to a [`StatusSnapshot`].
    pub async fn snapshot(&self) -> Result<StatusSnapshot> {
        let resp = self.fetch().await?;

        let queries = resp
            .queries
            .iter()
            .map(|q| {
                (
                    q.fingerprint,
                    QueryCounters {
                        hit_count: q.hit_count,
                        miss_count: q.miss_count,
                    },
                )
            })
            .collect();

        Ok(StatusSnapshot {
            last_applied_lsn: resp.cdc.last_applied_lsn,
            queries,
        })
    }

    /// Block until no query is mid-population (state `Loading` or
    /// `Pending*`), or `timeout` elapses.
    ///
    /// Population is asynchronous: a query's first execution only
    /// registers it; the cached result lands later. Without this wait the
    /// cache-hit execution races a cold-start population and a `cached`
    /// query spuriously looks like a routing miss (notably the first
    /// query in a suite). Mirrors the integration tests' `cache_settle`.
    pub async fn cache_settle(&self, timeout: Duration) -> Result<()> {
        // Grace for the registration message to reach the writer, so we
        // don't observe "settled" before the query even enters Loading.
        sleep(Duration::from_millis(20)).await;

        crate::poll::poll_until(timeout, Duration::from_millis(25), || async move {
            let resp = self.fetch().await?;
            let in_flight: Vec<String> = resp
                .queries
                .iter()
                .filter(|q| q.state == STATE_LOADING || q.state.starts_with(STATE_PENDING_PREFIX))
                .map(|q| format!("{}={}", q.fingerprint, q.state))
                .collect();
            if in_flight.is_empty() {
                Ok(ControlFlow::Break(()))
            } else {
                Ok(ControlFlow::Continue(format!(
                    "cache did not settle within {timeout:?}; in-flight: {}",
                    in_flight.join(", ")
                )))
            }
        })
        .await
    }
}

impl LsnSource for StatusClient {
    async fn last_applied_lsn(&self) -> Result<u64> {
        Ok(self.snapshot().await?.last_applied_lsn)
    }
}
