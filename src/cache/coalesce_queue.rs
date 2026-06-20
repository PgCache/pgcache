use std::collections::HashMap;
use std::sync::Mutex;

use crate::pg::protocol::extended::ResultFormats;
use crate::query::ast::LimitClause;
use crate::query::{Fingerprint, FingerprintMap};

use crate::cache::messages::PipelineDescribe;
use crate::cache::query_cache::{QueryRequest, QueryType};
use crate::cache::types::{CacheStateView, CachedQueryState};

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
