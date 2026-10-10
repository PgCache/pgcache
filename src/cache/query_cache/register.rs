//! Registering queries with the writer: pinned queries at startup, and a
//! dispatch's register-then-await-subsumption round trip.

use std::num::NonZeroU64;
use std::sync::Arc;
use std::time::Instant;

use ecow::EcoString;
use tokio::sync::oneshot;
use tracing::{debug, warn};

use super::{CacheDispatch, QueryRequest};
use crate::cache::Generation;
use crate::cache::messages::{AdmitAction, QueryCommand, RegisterRequest, SubsumptionResult};
use crate::cache::mv::MvMeta;
use crate::cache::mv_shape::ShapeGate;
use crate::cache::query::limit_rows_needed;
use crate::cache::reference_settings;
use crate::cache::types::{CachedQueryState, CachedQueryView, PinnedQuery, QueryMetrics};
use crate::cache::{CacheError, CacheResult};
use crate::query::Fingerprint;
use crate::query::keying_settings::KeyingSettings;
use crate::timing::duration_to_ns_u64;

/// Pinned queries have no client session, so their bare table names resolve
/// under this fixed path. A session on any other path reaches the same entry
/// through the alias map once the writer has resolved its pair.
fn pinned_search_path() -> Arc<[EcoString]> {
    Arc::from([EcoString::from("public")])
}

impl CacheDispatch {
    /// Register pinned queries at startup by sending Register commands with `pinned: true`.
    pub async fn pinned_queries_register(&self, pinned: &[PinnedQuery]) -> CacheResult<()> {
        // Pinned queries have no session: their literals mean what the
        // reference makes of them.
        let keying = Arc::new(KeyingSettings {
            search_path: pinned_search_path(),
            literals: reference_settings::current().literals.clone(),
        });
        for pq in pinned {
            let session_key = keying.session_key(pq.cacheable_query.query());
            let fingerprint = match self
                .session_key_resolve(session_key, &pq.cacheable_query, &keying)
                .await
            {
                Some(fingerprint) => fingerprint,
                None => {
                    warn!("pinned query {session_key} does not resolve, skipping");
                    continue;
                }
            };
            // Set Loading state in CacheStateView
            self.state_view.cached_queries.insert(
                fingerprint,
                CachedQueryView {
                    state: CachedQueryState::Loading,
                    generation: Generation::ZERO,
                    resolved: None,
                    read_columns: None,
                    deparsed_sql: None,
                    serve_shape: None,
                    max_limit: None,
                    reference_only: false,
                    referenced: false,
                    // Writer fills this in after resolution/classification.
                    mv: MvMeta::new(ShapeGate::Skip, None),
                },
            );
            let now = NonZeroU64::new(duration_to_ns_u64(self.state_view.started_at.elapsed()));
            self.state_view
                .metrics
                .entry(fingerprint)
                .or_insert_with(|| QueryMetrics::new(now, &self.state_view.latency_template));

            let (subsumption_tx, _subsumption_rx) = oneshot::channel();
            self.register_send(RegisterRequest {
                fingerprint,
                cacheable_query: Arc::clone(&pq.cacheable_query),
                keying: Arc::clone(&keying),
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
            keying: Arc::clone(&msg.keying),
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
