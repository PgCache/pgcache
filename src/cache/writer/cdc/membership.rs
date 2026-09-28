use crate::oid::Oid;
use crate::query::FingerprintSet;

use tracing::trace;

use crate::pg::protocol::ByteString;

use super::super::super::CacheResult;
use super::super::super::update_query::{UpdateEvalStrategy, UpdateQueries, UpdateQuery};
use super::super::core::WriterCore;
use super::row_match::update_query_matches_locally;

use super::*;

/// One CDC row's membership-evaluation inputs.
pub(super) struct MembershipRow<'a> {
    pub(super) row_data: &'a [Option<ByteString>],
    /// Candidate queries whose extracted constraints the row could satisfy,
    /// computed once by the caller and shared with the invalidation and memo
    /// passes. The eval index holds the full population (LocalEval and PgEval,
    /// unconstrained queries as always-candidates), so narrowing to this set
    /// never drops a true match (PGC-292).
    pub(super) candidates: &'a FingerprintSet,
    /// Precomputed segment matrix (PGC-241), when the batch covers this row.
    pub(super) batch: Option<BatchEvalView<'a>>,
}

/// LocalEval: Rust evaluation over the candidates. Cheap, so always evaluated
/// in full; `mv_dirty_mark` self-gates on `Fresh`.
fn local_eval_match(
    core: &WriterCore,
    update_queries: &UpdateQueries,
    row: &MembershipRow<'_>,
) -> bool {
    let mut matched = false;
    for update_query in maintained_candidates(core, update_queries, row.candidates) {
        if update_query.eval_strategy != UpdateEvalStrategy::LocalEval
            || !update_query_matches_locally(update_query, row.row_data)
        {
            continue;
        }
        trace!(
            "update_queries local-eval matched fingerprint {}",
            update_query.fingerprint
        );
        core.mv_dirty_mark(update_query.fingerprint);
        matched = true;
    }
    if matched {
        crate::metrics::handles().cdc.local_eval_hits.increment(1);
    }
    matched
}

/// Candidates still maintained in place. A query flagged for invalidation this
/// frame forwards to origin and repopulates — matching the pre-deferral
/// ordering, where the inline invalidate ran before the executor.
fn maintained_candidates<'a>(
    core: &'a WriterCore,
    update_queries: &'a UpdateQueries,
    candidates: &'a FingerprintSet,
) -> impl Iterator<Item = &'a UpdateQuery> {
    candidates
        .iter()
        .filter(|fingerprint| !core.frame_invalidations.contains(fingerprint))
        .filter_map(|fingerprint| update_queries.queries.get(fingerprint))
}

/// Batch-covered PgEval queries consult the segment matrix — no round-trip.
/// `frame_invalidations` is already applied (the matrix is built unfiltered);
/// the Fresh-MV dirty-mark self-gates, mirroring the per-row fresh/rest split.
/// Returns whether any batched query hit, and the uncovered queries left for
/// per-row evaluation.
fn pg_eval_batched_match<'a>(
    core: &WriterCore,
    pg_eval: Vec<&'a UpdateQuery>,
    batch: Option<&BatchEvalView<'_>>,
) -> (bool, Vec<&'a UpdateQuery>) {
    let Some(view) = batch else {
        return (false, pg_eval);
    };
    let (batched, fallback): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = pg_eval
        .into_iter()
        .partition(|q| view.covers(q.fingerprint));
    let mut matched = false;
    for update_query in batched.iter().filter(|q| view.hit(q.fingerprint)) {
        trace!(
            "update_queries batched pg-eval matched fingerprint {}",
            update_query.fingerprint
        );
        core.mv_dirty_mark(update_query.fingerprint);
        matched = true;
    }
    (matched, fallback)
}

impl WriterCdc {
    /// Decide whether a CDC row belongs in cache and, if so, upsert it once.
    ///
    /// Phase A (read-only): determine which cached queries the row matches.
    /// LocalEval queries are evaluated in Rust; PgEval queries are batched into
    /// combined `SELECT EXISTS (p1), …` round-trips on one autocommit pool
    /// connection, so they observe the pre-source-transaction snapshot, never
    /// the in-flight frame's uncommitted writes.
    ///
    /// Phase B (in-frame): if any query matched, a single unconditional upsert
    /// into the source table's cache table on `cdc_write_conn`. The shared
    /// cache table holds the row iff some cached query needs it, so one upsert
    /// suffices regardless of how many matched.
    ///
    /// Returns true if the row matched any cached query.
    #[cfg_attr(feature = "hotpath", hotpath::measure)]
    pub(super) async fn update_queries_execute_batch(
        &mut self,
        core: &mut WriterCore,
        relation_oid: Oid,
        row: MembershipRow<'_>,
    ) -> CacheResult<bool> {
        let matched = self
            .row_membership_evaluate(core, relation_oid, &row)
            .await?;

        // Phase B: single in-frame write, buffered for the frame flush (PGC-228).
        if matched {
            self.frame_cache_upsert(core, relation_oid, row.row_data)
                .await?;
        }

        Ok(matched)
    }

    /// Phase A of `update_queries_execute_batch`. Fresh-MV queries must be
    /// fully evaluated so every match is dirty-marked (else a Fresh MV silently
    /// goes stale). Non-Fresh queries only decide whether the row belongs in
    /// the shared cache table — one match triggers the single upsert, so they
    /// short-circuit.
    async fn row_membership_evaluate(
        &mut self,
        core: &WriterCore,
        relation_oid: Oid,
        row: &MembershipRow<'_>,
    ) -> CacheResult<bool> {
        // No cached query references this relation → nothing to upsert.
        // Not an error (the relation simply isn't maintained in place).
        let Some(update_queries) = core.cache.update_queries.get(&relation_oid) else {
            return Ok(false);
        };
        let Some(table_metadata) = core.cache.tables.get1(&relation_oid) else {
            return Ok(false);
        };

        let total_queries = update_queries.queries.len();
        trace!("update_queries_execute_batch start [{total_queries}]");
        if total_queries == 0 {
            return Ok(false);
        }

        let mut matched = local_eval_match(core, update_queries, row);

        let pg_eval: Vec<&UpdateQuery> =
            maintained_candidates(core, update_queries, row.candidates)
                .filter(|q| q.eval_strategy == UpdateEvalStrategy::PgEval)
                .collect();
        if pg_eval.is_empty() {
            return Ok(matched);
        }

        let (batched_hit, fallback) = pg_eval_batched_match(core, pg_eval, row.batch.as_ref());
        matched |= batched_hit;

        // Per-row fallback for non-batchable shapes / uncovered rows.
        let (fresh_pg, rest_pg): (Vec<&UpdateQuery>, Vec<&UpdateQuery>) = fallback
            .into_iter()
            .partition(|q| core.mv_dirty_eval_required(q.fingerprint));

        let fresh_hits = self
            .pg_eval_matches(&fresh_pg, table_metadata, row.row_data)
            .await?;
        for &fingerprint in &fresh_hits {
            core.mv_dirty_mark(fingerprint);
        }
        let mut pg_hit = !fresh_hits.is_empty();
        matched |= pg_hit;

        // Non-Fresh queries only decide the upsert: skip them entirely once
        // anything matched, else stop at the first match.
        if !matched
            && self
                .pg_eval_any(&rest_pg, table_metadata, row.row_data)
                .await?
        {
            matched = true;
            pg_hit = true;
        }

        if pg_hit {
            crate::metrics::handles().cdc.pg_eval_hits.increment(1);
        }

        Ok(matched)
    }
}
