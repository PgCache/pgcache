//! Registering queries with the writer: pinned queries at startup, and a
//! dispatch's register-then-await-subsumption round trip.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use tokio::sync::oneshot;
use tracing::debug;

use super::{CacheDispatch, QueryRequest};
use crate::cache::messages::{AdmitAction, QueryCommand, RegisterRequest, SubsumptionResult};
use crate::cache::mv::MvMeta;
use crate::cache::mv_shape::ShapeGate;
use crate::cache::query::limit_rows_needed;
use crate::cache::types::{CachedQueryState, CachedQueryView, PinnedQuery, QueryMetrics};
use crate::cache::{CacheError, CacheResult};
use crate::query::Fingerprint;
use crate::timing::duration_to_ns_u64;

impl CacheDispatch {
    /// Register pinned queries at startup by sending Register commands with `pinned: true`.
    pub fn pinned_queries_register(&self, pinned: &[PinnedQuery]) -> CacheResult<()> {
        for pq in pinned {
            // Set Loading state in CacheStateView
            self.state_view.cached_queries.insert(
                pq.fingerprint,
                CachedQueryView {
                    state: CachedQueryState::Loading,
                    generation: 0,
                    resolved: None,
                    deparsed_sql: None,
                    serve_shape: None,
                    max_limit: None,
                    referenced: false,
                    // Writer fills this in after resolution/classification.
                    mv: MvMeta::new(ShapeGate::Skip, None),
                },
            );
            let now = NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
            self.state_view
                .metrics
                .entry(pq.fingerprint)
                .or_insert_with(|| QueryMetrics::new(now));

            let (subsumption_tx, _subsumption_rx) = oneshot::channel();
            self.register_send(RegisterRequest {
                fingerprint: pq.fingerprint,
                cacheable_query: Arc::clone(&pq.cacheable_query),
                search_path: vec!["public".into()].into(),
                started_at: Instant::now(),
                subsumption_tx,
                admit_action: AdmitAction::Admit,
                pinned: true,
            })?;
        }
        Ok(())
    }

    /// Send a Register command to the writer thread.
    fn register_send(&self, request: RegisterRequest) -> CacheResult<()> {
        self.query_tx
            .send(QueryCommand::Register(request))
            .map_err(|_| CacheError::WriterSend.into())
    }

    /// Hold a request, send Register with subsumption oneshot, and route
    /// based on the writer's response. Subsumed → serve from cache,
    /// NotSubsumed → forward to origin.
    pub(super) async fn subsumption_await(
        &self,
        msg: QueryRequest,
        fingerprint: Fingerprint,
        admit_action: AdmitAction,
    ) -> CacheResult<()> {
        let (subsumption_tx, subsumption_rx) = oneshot::channel();

        let request = RegisterRequest {
            fingerprint,
            cacheable_query: Arc::clone(&msg.cacheable_query),
            search_path: Arc::clone(&msg.search_path),
            started_at: Instant::now(),
            subsumption_tx,
            admit_action,
            pinned: false,
        };
        if self.register_send(request).is_err() {
            // Writer channel closed (cache subsystem torn down or restarting):
            // degrade by forwarding to origin rather than failing the client.
            debug!("register channel closed; forwarding query to origin");
            self.metrics_miss_record(fingerprint);
            return msg.forward();
        }

        match subsumption_rx.await {
            Ok(SubsumptionResult::Subsumed {
                generation,
                resolved,
                deparsed_sql,
            }) => {
                self.metrics_hit_record(fingerprint);
                // Subsumed queries have mv_state = MeasurePending (see Future Work:
                // "MV first-pop for subsumed queries"); mv_dispatch_decide returns
                // false and the serve goes through the fallthrough path.
                let rows_needed = limit_rows_needed(&msg.cacheable_query.query().limit);
                let mv = self.mv_dispatch_decide(fingerprint, rows_needed);
                self.pool_serve(
                    fingerprint,
                    msg,
                    resolved,
                    deparsed_sql,
                    None,
                    generation,
                    mv,
                )
            }
            Ok(SubsumptionResult::NotSubsumed) | Err(_) => {
                self.metrics_miss_record(fingerprint);
                msg.forward()
            }
        }
    }
}
