use std::sync::Arc;
use std::time::Instant;

use tracing::error;

use super::{CacheDispatch, CoalescedClient, QueryRequest};
use crate::cache::messages::slices_concat;
use crate::cache::query::{limit_is_sufficient, limit_rows_needed};
use crate::cache::reference_settings::{self, ReferenceSettings};
use crate::cache::types::{CachedQueryState, QueryServing, ServingAt};
use crate::query::Fingerprint;
use crate::query::resolve::PrivilegeNeeds;
use crate::result::error_chain_format;

/// A Ready entry's drain: the serving the writer published with Ready, at
/// its generation, and for a reference-only entry the settings a waiter
/// must be on.
struct Drain {
    fingerprint: Fingerprint,
    at: ServingAt,
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
    pub(crate) fn waiting_drain_ready(&self, fingerprint: Fingerprint, at: ServingAt) {
        let Some(groups) = self.waiting.drain(fingerprint) else {
            return;
        };

        // The writer may have evicted, invalidated or readmitted the entry
        // since it sent Ready; the rows the serving reads are then no longer
        // its to serve, so the waiters go to origin.
        let current = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .is_some_and(|view| {
                view.state == CachedQueryState::Ready && view.generation == at.generation
            });
        if !current {
            self.groups_forward(groups.into_values());
            return;
        }

        let drain = Drain {
            fingerprint,
            reference: at.serving.reference_only.then(reference_settings::current),
            at,
        };

        let served: u64 = groups
            .into_values()
            .map(|waiters| self.coalesced_group_serve(waiters, &drain))
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

    /// The gate a direct hit applies in `ready_hit`, for the waiters' roles
    /// (PGC-463): a waiter whose role lacks a verdict or is denied is
    /// forwarded; the rest are returned to be served.
    fn waiters_privileged(
        &self,
        waiters: Vec<QueryRequest>,
        needs: &PrivilegeNeeds,
    ) -> Vec<QueryRequest> {
        let (served, forwarded): (Vec<_>, Vec<_>) = waiters
            .into_iter()
            .partition(|w| self.privilege_gate(&w.effective_role, needs));
        for msg in forwarded {
            let _ = msg.forward();
        }
        served
    }

    /// Serve one coalescing group from `drain`: one serve request whose
    /// response bytes are broadcast to every waiter. Returns the number of
    /// waiters served beyond the primary.
    fn coalesced_group_serve(&self, mut waiters: Vec<QueryRequest>, drain: &Drain) -> u64 {
        let serving: &QueryServing = &drain.at.serving;
        // Stamp drain_started_at on every waiter (including the one that
        // becomes primary) so `coalesce_wait_seconds` fires per-waiter,
        // not just per-group.
        let drain_started = Instant::now();
        for w in &mut waiters {
            w.timing.drain_started_at = Some(drain_started);
        }
        waiters = waiters_on_reference(waiters, drain.reference.as_deref());
        waiters = self.waiters_privileged(waiters, &serving.read_facts.privilege_needs);
        if waiters.is_empty() {
            return 0;
        }
        let primary = waiters.remove(0);

        // Check whether the cached rows cover this group's LIMIT
        let primary_needed = limit_rows_needed(&primary.limit);
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
        let mv = self.mv_dispatch_decide(drain.fingerprint, primary_needed);
        if let Err(e) = self.pool_serve_coalesced(
            drain.fingerprint,
            primary,
            Arc::clone(&serving.bound),
            Arc::clone(&serving.serve_shape),
            drain.at.generation,
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
        self.groups_forward(groups.into_values());
    }

    /// Forward every waiter of the drained `groups` to origin.
    fn groups_forward(&self, groups: impl IntoIterator<Item = Vec<QueryRequest>>) {
        for waiters in groups {
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
