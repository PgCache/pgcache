//! Shared cache-dispatch helpers.
//!
//! These functions are the building blocks of [`CacheDispatch::query_dispatch`]:
//! the allowlist check, the hit-path mutations (metrics, CLOCK bit), and the MV
//! serve decision. They operate on the `Send` shared state ([`CacheStateView`])
//! and are factored out so the dispatch logic stays readable.

use crate::query::Fingerprint;
use std::num::NonZeroU64;
use std::ops::ControlFlow;
use std::sync::atomic::Ordering;

use tracing::error;

use crate::query::ast::{AstNode, QueryExpr, TableNode};
use crate::settings::{Allowlist, CachePolicy};
use crate::timing::duration_to_ns_u64;

use super::{
    messages::QueryCommand,
    mv::{MvServe, MvState},
    query::limit_is_sufficient,
    types::CacheStateView,
};

/// MV serve decision.
pub(crate) enum MvDecision {
    Serve(MvServe),
    /// MV is `Pending`; needs a `Pending → Scheduled` flip plus an `MvBuild`
    /// send (owned by the dispatcher, which has `query_tx`).
    NeedsSchedule {
        has_table: bool,
    },
}

/// Check whether all tables in the query are in the allowlist. Returns true if
/// no allowlist is configured (all tables allowed).
pub(crate) fn query_allowlist_check(allowlist: &Allowlist, query: &QueryExpr) -> bool {
    let Some(entries) = allowlist else {
        return true;
    };
    // Allowed iff every TableNode matches an allowlist entry: break on the
    // first non-matching table, so a clean walk (Continue) means "all allowed".
    // Entries are lowercased at parse time; match without allocating a
    // lowercased copy of each table/schema name on this serve-path walk.
    query
        .try_for_each_node::<TableNode, ()>(&mut |t| {
            let allowed = entries.iter().any(|(ws, wt)| {
                eq_lowercased(wt, &t.name)
                    && match ws {
                        Some(ws) => t.schema.as_deref().is_some_and(|s| eq_lowercased(ws, s)),
                        None => true,
                    }
            });
            if allowed {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        })
        .is_continue()
}

/// Unicode case-insensitive equality without allocating. `lowered` must already
/// be lowercased (allowlist entries are, at parse time); `raw` is lowercased
/// lazily per character during the comparison. Equivalent to
/// `lowered == raw.to_lowercase()` with no intermediate `String`.
fn eq_lowercased(lowered: &str, raw: &str) -> bool {
    lowered.chars().eq(raw.chars().flat_map(char::to_lowercase))
}

/// Record a cache hit in the shared view: bump the GC hit counter and the
/// per-query metrics. Concurrency-safe (atomic + DashMap shard locks); called
/// inline from connection tasks.
pub(crate) fn metrics_hit_record(state_view: &CacheStateView, fingerprint: Fingerprint) {
    state_view.hits_since_gc.fetch_add(1, Ordering::Relaxed);
    if let Some(mut m) = state_view.metrics.get_mut(&fingerprint) {
        m.hit_count += 1;
        m.last_hit_at_ns = NonZeroU64::new(duration_to_ns_u64(state_view.started_at.elapsed()));
    }
}

/// Set the CLOCK reference bit for eviction tracking.
pub(crate) fn clock_reference_set(
    state_view: &CacheStateView,
    cache_policy: CachePolicy,
    fingerprint: &Fingerprint,
) {
    if cache_policy == CachePolicy::Clock
        && let Some(mut entry) = state_view.cached_queries.get_mut(fingerprint)
    {
        entry.referenced = true;
    }
}

/// Inspect `mv_state` to decide whether to serve from the MV fast path, source
/// rows, or (Pending) defer to the dispatcher for scheduling. The single site
/// for `mv_hits`/`mv_fallthrough` counting — including the `Pending` case, which
/// falls through to source rows while the dispatcher schedules the build.
pub(crate) fn mv_serve_decide(
    state_view: &CacheStateView,
    fingerprint: Fingerprint,
    rows_needed: Option<u64>,
) -> MvDecision {
    let observed = state_view
        .cached_queries
        .get(&fingerprint)
        .map(|e| (e.mv.state, e.mv.output_columns.clone(), e.mv.limit));

    match observed {
        None => MvDecision::Serve(MvServe::SourceRow),
        Some((MvState::Fresh, Some(cols), mv_limit))
            if limit_is_sufficient(mv_limit, rows_needed) =>
        {
            crate::metrics::handles().cache.mv_hits.increment(1);
            MvDecision::Serve(MvServe::Mv(cols))
        }
        Some((MvState::Fresh, Some(_), _)) => {
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::Serve(MvServe::SourceRow)
        }
        Some((MvState::Fresh, None, _)) => {
            error!(
                fingerprint = %fingerprint,
                "MV is Fresh but output columns were never captured; serving from source rows"
            );
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::Serve(MvServe::SourceRow)
        }
        Some((MvState::Pending { has_table }, _, _)) => {
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::NeedsSchedule { has_table }
        }
        Some((
            MvState::Scheduled { .. } | MvState::Building { .. } | MvState::BuildingDirty { .. },
            _,
            _,
        )) => {
            crate::metrics::handles().cache.mv_fallthrough.increment(1);
            MvDecision::Serve(MvServe::SourceRow)
        }
        Some((MvState::Skipped | MvState::Ineligible, _, _)) => {
            MvDecision::Serve(MvServe::SourceRow)
        }
    }
}

/// Check-and-transition under write guard: `Pending { has_table } → Scheduled
/// { has_table }`. Returns the command to send when the transition wins the
/// race; `None` when another dispatch beat us or the entry moved elsewhere.
pub(crate) fn mv_schedule(
    state_view: &CacheStateView,
    fingerprint: Fingerprint,
    has_table: bool,
) -> Option<QueryCommand> {
    let mut entry = state_view.cached_queries.get_mut(&fingerprint)?;
    if entry.mv.state != (MvState::Pending { has_table }) {
        return None;
    }
    entry.mv.state = MvState::Scheduled { has_table };
    Some(QueryCommand::MvBuild { fingerprint })
}
