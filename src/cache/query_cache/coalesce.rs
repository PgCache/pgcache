use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tracing::error;

use crate::query::Fingerprint;
use crate::result::error_chain_format;

use crate::cache::messages::slices_concat;
use crate::cache::query::{limit_is_sufficient, limit_rows_needed};
use crate::cache::types::SharedResolved;

use super::dispatch::reply_forward;
use super::{CacheDispatch, CoalescedClient};

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
        generation: u64,
        resolved: SharedResolved,
        deparsed_sql: EcoString,
        max_limit: Option<u64>,
    ) {
        let Some(groups) = self.waiting.drain(fingerprint) else {
            return;
        };

        // The entry is Ready at drain time, so its propagated serve shape (if any)
        // is present; read it once for the whole drain rather than threading it
        // through the WriterNotify::Ready message.
        let serve_shape = self
            .state_view
            .cached_queries
            .get(&fingerprint)
            .and_then(|v| v.serve_shape.clone());

        let mut served = 0u64;
        for (_key, mut waiters) in groups {
            // Stamp drain_started_at on every waiter (including the one that
            // becomes primary) so `coalesce_wait_seconds` fires per-waiter,
            // not just per-group.
            let drain_started = Instant::now();
            for w in &mut waiters {
                w.timing.drain_started_at = Some(drain_started);
            }
            let primary = waiters.remove(0);

            // Check whether the cached rows cover this group's LIMIT
            let primary_needed = limit_rows_needed(&primary.cacheable_query.query.limit);
            if !limit_is_sufficient(max_limit, primary_needed) {
                let _ = reply_forward(
                    primary.reply_tx,
                    primary.client_socket,
                    primary.pipeline,
                    primary.data,
                    primary.timing,
                );
                for msg in waiters {
                    let _ = reply_forward(
                        msg.reply_tx,
                        msg.client_socket,
                        msg.pipeline,
                        msg.data,
                        msg.timing,
                    );
                }
                continue;
            }

            served += waiters.len() as u64;

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
            let mv = self.mv_dispatch_decide(fingerprint, primary_needed);
            if let Err(e) = self.pool_serve_coalesced(
                fingerprint,
                primary,
                Arc::clone(&resolved),
                deparsed_sql.clone(),
                serve_shape.clone(),
                generation,
                mv,
                coalesced,
            ) {
                error!(
                    "coalesce serve failed: {}",
                    error_chain_format(e.current_context()),
                );
            }
        }

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
                let _ = reply_forward(
                    msg.reply_tx,
                    msg.client_socket,
                    msg.pipeline,
                    msg.data,
                    msg.timing,
                );
            }
        }

        #[allow(clippy::cast_precision_loss)] // queue depth, never near 2^53
        crate::metrics::handles()
            .cache
            .coalesce_waiting
            .set(self.waiting_count() as f64);
    }
}
