use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use crate::pg::protocol::extended::ResultFormats;
use crate::query::ast::LimitClause;
use crate::query::{Fingerprint, FingerprintMap};

use crate::cache::messages::PipelineDescribe;
use crate::cache::query_cache::{QueryRequest, QueryType};
use crate::cache::types::{CacheStateView, CachedQueryState};

/// Coalesce-forward deadline policy (PGC-335 fix A). A request parked on a
/// `Loading` query is forwarded to origin once it has waited this long, so serve
/// latency is decoupled from population latency while coalescing still absorbs
/// fast populations.
///
/// A cold (first) population has no per-query estimate, so a short fixed bound
/// is used: the cost is unknown and possibly large, the herd is one-time, and we
/// don't make readers wait on an unknown build. A re-population scales the bound
/// by the query's observed fetch+stage time: below the crossover
/// (`floor / (1 - factor)` = 500 ms) the bound exceeds the estimate, so cheap
/// re-pops are waited out and served from cache; above it the bound dips under
/// the estimate, so expensive re-pops forward early while the population still
/// completes in the background for later requests. The ceiling caps a runaway
/// estimate; a query that is both slow and frequently re-populated is a
/// discard-backoff candidate (separate work), not something to block on.
const COALESCE_COLD_DEADLINE_MS: f64 = 200.0;
const COALESCE_REPOP_FACTOR: f64 = 0.8;
const COALESCE_REPOP_FLOOR_MS: f64 = 100.0;
const COALESCE_REPOP_CEILING_MS: f64 = 10_000.0;
const COALESCE_EWMA_ALPHA: f64 = 0.3;

/// Fold a new fetch+stage sample (ms) into the per-query EWMA estimate, seeding
/// from the first sample. Smooths so a single slow outlier doesn't swing the
/// re-population deadline (PGC-335).
pub(crate) fn fetch_stage_ewma_update(prev: Option<f64>, sample_ms: f64) -> f64 {
    match prev {
        None => sample_ms,
        Some(p) => COALESCE_EWMA_ALPHA * sample_ms + (1.0 - COALESCE_EWMA_ALPHA) * p,
    }
}

/// Time a `Loading` waiter should wait before forwarding to origin, given the
/// query's observed fetch+stage estimate (`None` for a cold first population).
pub(crate) fn coalesce_deadline(estimate_ms: Option<f64>) -> Duration {
    let ms = match estimate_ms {
        None => COALESCE_COLD_DEADLINE_MS,
        Some(p) => (p * COALESCE_REPOP_FACTOR + COALESCE_REPOP_FLOOR_MS)
            .clamp(COALESCE_REPOP_FLOOR_MS, COALESCE_REPOP_CEILING_MS),
    };
    Duration::from_secs_f64(ms / 1000.0)
}

/// Key for grouping coalesced requests. Requests in the same group
/// produce identical wire protocol bytes and can share a single serve execution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(super) struct CoalesceKey {
    query_type: QueryType,
    emit_rfq: bool,
    has_parse: bool,
    has_bind: bool,
    pipeline_describe: PipelineDescribe,
    result_formats: ResultFormats,
    limit: Option<LimitClause>,
}

impl CoalesceKey {
    /// Build a CoalesceKey from a QueryRequest's pipeline context.
    pub(super) fn from_request(msg: &QueryRequest) -> CoalesceKey {
        let (emit_rfq, has_parse, has_bind, pipeline_describe) = match &msg.pipeline {
            Some(p) => (p.emit_rfq, p.has_parse, p.has_bind, p.describe),
            None => (false, false, false, PipelineDescribe::None),
        };
        CoalesceKey {
            query_type: msg.query_type,
            emit_rfq,
            has_parse,
            has_bind,
            pipeline_describe,
            result_formats: msg.result_formats.clone(),
            limit: msg.cacheable_query.query.limit.clone(),
        }
    }
}

/// Outer key: fingerprint (O(1) drain on Ready/Failed).
/// Inner key: CoalesceKey grouping requests that share identical response bytes.
type WaitingQueue = FingerprintMap<HashMap<CoalesceKey, Vec<QueryRequest>>>;

/// Coalescing wait queue with the enqueue/drain ordering invariant encapsulated.
///
/// The `Mutex` is private and [`enqueue_if_loading`](Self::enqueue_if_loading) is
/// the only way to add a waiter — it re-checks the entry state *under the lock*
/// and refuses to enqueue if the state has advanced. This makes the
/// orphaned-waiter race unrepresentable: a waiter cannot be added after the
/// `Ready` drain has run, because the writer sets `Ready` before sending the
/// notify that drives [`drain`](Self::drain), and that drain removes under the
/// same lock. Callers therefore cannot skip the re-check or get the
/// waiting→cached_queries lock ordering wrong.
pub(super) struct CoalesceQueue {
    inner: Mutex<WaitingQueue>,
}

impl CoalesceQueue {
    pub(super) fn new() -> Self {
        Self {
            inner: Mutex::new(HashMap::default()),
        }
    }

    /// Enqueue `msg` as a coalesced waiter iff `fingerprint` is still `Loading`
    /// (re-checked under the lock). Returns `Err(msg)` when the state has
    /// advanced so the caller can re-dispatch against the current state.
    // The large `Err` payload is intentional: it returns the message by move for
    // re-dispatch. Boxing would allocate on the (rare) state-advanced path.
    #[allow(clippy::result_large_err)]
    pub(super) fn enqueue_if_loading(
        &self,
        state_view: &CacheStateView,
        fingerprint: Fingerprint,
        key: CoalesceKey,
        msg: QueryRequest,
    ) -> Result<(), QueryRequest> {
        let mut guard = self.inner.lock().expect("lock coalesce queue");
        let still_loading = state_view.cached_queries.get(&fingerprint).map(|e| e.state)
            == Some(CachedQueryState::Loading);
        if !still_loading {
            return Err(msg);
        }
        guard
            .entry(fingerprint)
            .or_default()
            .entry(key)
            .or_default()
            .push(msg);
        Ok(())
    }

    /// Remove and return all waiter groups for a fingerprint (Ready/Failed drain).
    pub(super) fn drain(
        &self,
        fingerprint: Fingerprint,
    ) -> Option<HashMap<CoalesceKey, Vec<QueryRequest>>> {
        self.inner
            .lock()
            .expect("lock coalesce queue")
            .remove(&fingerprint)
    }

    /// Remove and return all waiters whose forward deadline has passed
    /// (`deadline_at <= now`). Drives the deadline sweep that forwards
    /// slow-population waiters to origin (PGC-335). Races safely with `drain`:
    /// both mutate under the same lock, so each waiter is removed exactly once.
    pub(super) fn drain_expired(&self, now: Instant) -> Vec<QueryRequest> {
        let mut guard = self.inner.lock().expect("lock coalesce queue");
        let mut expired = Vec::new();
        guard.retain(|_fingerprint, groups| {
            groups.retain(|_key, waiters| {
                let mut kept = Vec::with_capacity(waiters.len());
                for waiter in std::mem::take(waiters) {
                    if waiter.timing.deadline_at.is_some_and(|d| d <= now) {
                        expired.push(waiter);
                    } else {
                        kept.push(waiter);
                    }
                }
                *waiters = kept;
                !waiters.is_empty()
            });
            !groups.is_empty()
        });
        expired
    }

    /// Total waiters across all groups (gauge).
    pub(super) fn waiter_count(&self) -> usize {
        self.inner
            .lock()
            .expect("lock coalesce queue")
            .values()
            .flat_map(|groups| groups.values())
            .map(Vec::len)
            .sum()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ms(d: Duration) -> f64 {
        d.as_secs_f64() * 1000.0
    }

    #[test]
    fn cold_population_uses_fixed_short_deadline() {
        assert!((ms(coalesce_deadline(None)) - 200.0).abs() < 1e-6);
    }

    #[test]
    fn cheap_repop_deadline_exceeds_estimate() {
        // Below the crossover: wait it out, serve from cache (no forward).
        assert!(ms(coalesce_deadline(Some(50.0))) > 50.0); // 0.8*50+100 = 140
        assert!((ms(coalesce_deadline(Some(50.0))) - 140.0).abs() < 1e-6);
    }

    #[test]
    fn crossover_at_500ms() {
        // floor/(1-factor) = 100/0.2 = 500ms: deadline == estimate at the crossover.
        assert!((ms(coalesce_deadline(Some(500.0))) - 500.0).abs() < 1e-6);
    }

    #[test]
    fn expensive_repop_deadline_below_estimate() {
        // Above the crossover: forward early.
        assert!(ms(coalesce_deadline(Some(1000.0))) < 1000.0); // 900
        assert!(ms(coalesce_deadline(Some(10_000.0))) < 10_000.0); // 8100
    }

    #[test]
    fn ceiling_clamps_runaway_estimate() {
        // 0.8*P+100 would exceed 10s for P > 12_375ms; clamp holds at 10s.
        assert!((ms(coalesce_deadline(Some(50_000.0))) - 10_000.0).abs() < 1e-6);
    }

    #[test]
    fn floor_holds_for_near_zero_estimate() {
        assert!((ms(coalesce_deadline(Some(0.0))) - 100.0).abs() < 1e-6);
    }
}
