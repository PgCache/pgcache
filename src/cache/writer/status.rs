use crate::pg::Lsn;
use std::sync::atomic::Ordering;

use tokio::task::yield_now;

use crate::cache::status::{
    CacheStatusData, CdcStatusData, LatencyStats, QueryStatusData, StatusRequest, StatusResponse,
};
use crate::query::ast::Deparse;

use super::super::types::CachedQueryState;

use super::core::*;

impl WriterCore {
    /// Update cache state gauges with current values.
    //
    // Counts and byte totals are converted to f64 for Prometheus gauges; gauges
    // accept f64 by API and the precision loss only matters above 2^53.
    #[allow(clippy::cast_precision_loss)]
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) fn state_gauges_update(&self) {
        let registered = self.cache.cached_queries.len();
        crate::metrics::handles()
            .state
            .queries_registered
            .set(registered as f64);
        // Publish for the memory monitor to size the count cap (PGC-251).
        self.state_view
            .registered_count
            .store(registered, Ordering::Relaxed);

        {
            let mut loading_count = 0;
            let mut pending_count = 0;
            let mut invalidated_count = 0;

            for entry in self.state_view.cached_queries.iter() {
                match entry.value().state {
                    CachedQueryState::Loading => loading_count += 1,
                    CachedQueryState::Pending { .. } => pending_count += 1,
                    CachedQueryState::Invalidated => invalidated_count += 1,
                    CachedQueryState::Ready => {}
                }
            }

            crate::metrics::handles()
                .state
                .queries_loading
                .set(loading_count as f64);
            // Feed the adaptive gate its population-stage congestion signal
            // (PGC-277): the authoritative in-flight count, no drift.
            self.state_view.reg_gate.loading_set(loading_count);
            crate::metrics::handles()
                .state
                .queries_pending
                .set(pending_count as f64);
            crate::metrics::handles()
                .state
                .queries_invalidated
                .set(invalidated_count as f64);
        }

        // Cache-volume disk stats and the used-level at which reclaim engages,
        // from statvfs (PGC-276). Only meaningful once a reading exists.
        #[allow(clippy::cast_precision_loss)]
        if self.disk_total != 0 {
            let state = &crate::metrics::handles().state;
            state.disk_total.set(self.disk_total as f64);
            state.disk_available.set(self.disk_available as f64);
            state.disk_used.set(self.disk_used() as f64);
            state.disk_limit.set(self.disk_limit_effective as f64);
        }
        crate::metrics::handles()
            .state
            .generation
            .set(self.cache.generation_counter as f64);
        crate::metrics::handles()
            .state
            .tables_tracked
            .set(self.cache.tables.len() as f64);
    }

    /// Update gauges that correlate Register cost against state size. Suspected
    /// O(N) hot spots (`subsumption_check`, `update_query_register` sort) scale
    /// with these.
    #[allow(clippy::cast_precision_loss)]
    pub(super) fn writer_scale_gauges_update(&self) {
        let (total, max_per_relation) = self
            .cache
            .update_queries
            .iter()
            .map(|entry| entry.queries.len())
            .fold((0usize, 0usize), |(sum, max), n| (sum + n, max.max(n)));
        crate::metrics::handles()
            .state
            .update_queries_total
            .set(total as f64);
        crate::metrics::handles()
            .state
            .update_queries_max_per_relation
            .set(max_per_relation as f64);
    }

    /// Build and send a status response for an admin `/status` request.
    pub(super) async fn status_respond(&self, req: StatusRequest, last_applied_lsn: Lsn) {
        let cache = &self.cache;

        let (mut total_hits, mut total_misses) = (0u64, 0u64);
        for entry in self.state_view.metrics.iter() {
            total_hits += entry.hit_count;
            total_misses += entry.miss_count;
        }

        let dynamic = cache.dynamic.load();
        let cache_status = CacheStatusData {
            size_bytes: usize::try_from(self.disk_used()).unwrap_or(usize::MAX),
            size_limit_bytes: Some(
                usize::try_from(self.disk_limit_effective).unwrap_or(usize::MAX),
            ),
            generation: cache.generation_counter,
            tables_tracked: cache.tables.len(),
            policy: format!("{:?}", dynamic.cache_policy),
            queries_registered: cache.cached_queries.len(),
            uptime_ms: u64::try_from(self.state_view.started_at.elapsed().as_millis())
                .unwrap_or(u64::MAX),
            cache_hits: total_hits,
            cache_misses: total_misses,
        };

        let mut queries: Vec<QueryStatusData> = Vec::with_capacity(cache.cached_queries.len());
        for q in &cache.cached_queries {
            let mut sql_preview = String::with_capacity(128);
            Deparse::deparse(&*q.resolved, &mut sql_preview);
            sql_preview.truncate(200);

            let tables: Vec<String> = q
                .relation_oids
                .iter()
                .filter_map(|oid| {
                    cache
                        .tables
                        .get1(oid)
                        .map(|t| format!("{}.{}", t.schema, t.name))
                })
                .collect();

            let (state, mv_state) = self
                .state_view
                .cached_queries
                .get(&q.fingerprint)
                .map(|entry| {
                    (
                        format!("{:?}", entry.value().state),
                        format!("{:?}", entry.value().mv.state),
                    )
                })
                .unwrap_or_else(|| ("Unknown".to_owned(), "Unknown".to_owned()));

            // Look up per-query metrics (shared read access)
            let metrics = self.state_view.metrics.get(&q.fingerprint);
            let now_ns =
                u64::try_from(self.state_view.started_at.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let (
                hit_count,
                miss_count,
                idle_duration_ms,
                registered_duration_ms,
                cached_duration_ms,
                invalidation_count,
                readmission_count,
                eviction_count,
                subsumption_count,
                population_count,
                last_population_duration_ms,
                total_bytes_served,
                population_row_count,
                cache_hit_latency,
            ) = match &metrics {
                Some(m) => {
                    let latency_stats = if !m.cache_hit_latency.is_empty() {
                        Some(LatencyStats {
                            count: m.cache_hit_latency.len(),
                            mean_us: m.cache_hit_latency.mean(),
                            p50_us: m.cache_hit_latency.value_at_quantile(0.5),
                            p95_us: m.cache_hit_latency.value_at_quantile(0.95),
                            p99_us: m.cache_hit_latency.value_at_quantile(0.99),
                            min_us: m.cache_hit_latency.min(),
                            max_us: m.cache_hit_latency.max(),
                        })
                    } else {
                        None
                    };

                    (
                        m.hit_count,
                        m.miss_count,
                        m.last_hit_at_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.registered_at_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.cached_since_ns
                            .map(|ns| now_ns.saturating_sub(ns.get()) / 1_000_000),
                        m.invalidation_count,
                        m.readmission_count,
                        m.eviction_count,
                        m.subsumption_count,
                        m.population_count,
                        m.last_population_duration_us.map(|us| us.get() / 1_000),
                        m.total_bytes_served,
                        m.population_row_count,
                        latency_stats,
                    )
                }
                None => (0, 0, None, None, None, 0, 0, 0, 0, 0, None, 0, 0, None),
            };

            queries.push(QueryStatusData {
                fingerprint: q.fingerprint,
                sql_preview,
                tables,
                state,
                mv_state,
                cached_bytes: q.cached_bytes,
                max_limit: q.max_limit,
                pinned: q.pinned,
                hit_count,
                miss_count,
                idle_duration_ms,
                registered_duration_ms,
                cached_duration_ms,
                invalidation_count,
                readmission_count,
                eviction_count,
                subsumption_count,
                population_count,
                last_population_duration_ms,
                total_bytes_served,
                population_row_count,
                cache_hit_latency,
            });

            yield_now().await;
        }

        let response = StatusResponse {
            cache: cache_status,
            cdc: CdcStatusData { last_applied_lsn },
            queries,
            fault_injection: cfg!(feature = "fault-injection"),
        };

        let _ = req.reply_tx.send(response);
    }
}
