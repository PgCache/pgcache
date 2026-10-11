use std::sync::Arc;
use std::time::Instant;

use tracing::error;

use super::{CacheDispatch, CoalescedClient, QueryRequest};
use crate::cache::Generation;
use crate::cache::messages::slices_concat;
use crate::cache::query::{limit_is_sufficient, limit_rows_needed};
use crate::cache::reference_settings::{self, ReferenceSettings};
use crate::cache::types::{SharedQueryShape, SharedResolvedQuery};
use crate::query::Fingerprint;
use crate::result::error_chain_format;

/// What a Ready entry serves its drained waiters, read once per drain.
struct DrainServing {
    fingerprint: Fingerprint,
    generation: Generation,
    bound: SharedResolvedQuery,
    serve_shape: SharedQueryShape,
    max_limit: Option<u64>,
    /// `Some` for a reference-only entry: the settings a waiter must be on.
    reference: Option<Arc<ReferenceSettings>>,
}

/// The gate a direct hit applies in `ready_hit`, for a reference-only entry:
/// a waiter off the reference settings would read other rows at origin, so
/// it is forwarded; the rest are returned to be served.
fn waiters_on_reference(
    waiters: Vec<QueryRequest>,
    reference: Option<&ReferenceSettings>,
) -> Vec<QueryRequest> {
    let Some(reference) = reference else {
        return waiters;
    };
    let (on_reference, off_reference): (Vec<_>, Vec<_>) = waiters
        .into_iter()
        .partition(|w| w.keying.literals == reference.literals);
    if !off_reference.is_empty() {
        crate::metrics::handles()
            .cache
            .reference_only_forward_total
            .increment(off_reference.len() as u64);
        for msg in off_reference {
            let _ = msg.forward();
        }
    }
    on_reference
}

impl CacheDispatch {
    /// Total number of requests waiting across all coalescing groups.
    fn waiting_count(&self) -> usize {
        self.waiting.waiter_count()
    }

    /// Drain all coalesced waiters for a fingerprint that became Ready.
    /// Each coalescing group dispatches a single serve request that broadcasts
    /// response bytes to all clients in the group.
    pub fn waiting_drain_ready(
        &self,
        fingerprint: Fingerprint,
        generation: Generation,
        bound: SharedResolvedQuery,
        serve_shape: SharedQueryShape,
        max_limit: Option<u64>,
    ) {
        let Some(groups) = self.waiting.drain(fingerprint) else {
            return;
        };

        let reference_only = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .is_some_and(|v| v.reference_only);
        let serving = DrainServing {
            fingerprint,
            generation,
            bound,
            serve_shape,
            max_limit,
            reference: reference_only.then(reference_settings::current),
        };

        let served: u64 = groups
            .into_values()
            .map(|waiters| self.coalesced_group_serve(waiters, &serving))
            .sum();

        if served > 0 {
            crate::metrics::handles()
                .cache
                .coalesce_served
                .increment(served);
        }
        #[allow(clippy::cast_precision_loss)] // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting_count() as f64);
    }

    /// Serve one coalescing group from `serving`: one serve request whose
    /// response bytes are broadcast to every waiter. Returns the number of
    /// waiters served beyond the primary.
    fn coalesced_group_serve(&self, mut waiters: Vec<QueryRequest>, serving: &DrainServing) -> u64 {
        // Stamp drain_started_at on every waiter (including the one that
        // becomes primary) so `coalesce_wait_seconds` fires per-waiter,
        // not just per-group.
        let drain_started = Instant::now();
        for w in &mut waiters {
            w.timing.drain_started_at = Some(drain_started);
        }
        waiters = waiters_on_reference(waiters, serving.reference.as_deref());
        if waiters.is_empty() {
            return 0;
        }
        let primary = waiters.remove(0);

        // Check whether the cached rows cover this group's LIMIT
        let primary_needed = limit_rows_needed(&primary.cacheable_query.query().limit);
        if !limit_is_sufficient(serving.max_limit, primary_needed) {
            let _ = primary.forward();
            for msg in waiters {
                let _ = msg.forward();
            }
            return 0;
        }

        let served = waiters.len() as u64;
        let coalesced: Vec<CoalescedClient> = waiters
            .into_iter()
            .map(|msg| {
                let fallback = match msg.pipeline {
                    Some(pipeline) => slices_concat(&pipeline.buffered_bytes),
                    None => msg.data,
                };
                CoalescedClient {
                    client_socket: msg.client_socket,
                    reply_tx: msg.reply_tx,
                    timing: msg.timing,
                    data: fallback,
                }
            })
            .collect();

        // Coalesced dispatch: MV decision is made once for the whole group
        // (all waiters share the same fingerprint and dispatch moment).
        // The group already passed `limit_is_sufficient(max_limit, primary_needed)`
        // above; reuse `primary_needed` as the rows-needed witness.
        let mv = self.mv_dispatch_decide(serving.fingerprint, primary_needed);
        if let Err(e) = self.pool_serve_coalesced(
            serving.fingerprint,
            primary,
            Arc::clone(&serving.bound),
            Arc::clone(&serving.serve_shape),
            serving.generation,
            mv,
            coalesced,
        ) {
            error!(
                "coalesce serve failed: {}",
                error_chain_format(e.current_context()),
            );
        }
        served
    }

    /// Drain all coalesced waiters for a fingerprint that failed.
    /// Falls back to forwarding each waiter to origin.
    pub fn waiting_drain_failed(&self, fingerprint: Fingerprint) {
        let Some(groups) = self.waiting.drain(fingerprint) else {
            return;
        };

        for (_key, waiters) in groups {
            let drain_started = Instant::now();
            for mut msg in waiters {
                msg.timing.drain_started_at = Some(drain_started);
                let _ = msg.forward();
            }
        }

        #[allow(clippy::cast_precision_loss)] // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting_count() as f64);
    }

    /// Forward to origin every coalesced waiter whose forward deadline has
    /// passed. Decouples serve latency from population latency: a slow
    /// population (large cold first-population, or a re-pop running past its
    /// estimate) no longer holds its waiters hostage — they degrade to origin
    /// while the population completes in the background (PGC-335).
    pub fn waiting_drain_expired(&self) {
        let expired = self.waiting.drain_expired(Instant::now());
        if expired.is_empty() {
            return;
        }
        let forwarded = expired.len() as u64;
        let drain_started = Instant::now();
        for mut msg in expired {
            msg.timing.drain_started_at = Some(drain_started);
            let _ = msg.forward();
        }
        crate::metrics::handles()
            .cache
            .coalesce_deadline_forward
            .increment(forwarded);
        #[allow(clippy::cast_precision_loss)] // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting_count() as f64);
    }
}
