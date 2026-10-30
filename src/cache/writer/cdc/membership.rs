use tracing::trace;

use super::WriterCdc;
use super::row_match::update_query_matches_locally;
use super::segment_eval::BatchEvalView;
use crate::cache::CacheResult;
use crate::cache::update_query::{
    ChangeRelevance, UpdateEvalStrategy, UpdateQueries, UpdateQuery, UpdateQueryKey,
};
use crate::cache::writer::core::WriterCore;
use crate::catalog::TableMetadata;
use crate::oid::Oid;
use crate::pg::protocol::ByteString;
use crate::query::FingerprintSet;

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
    /// Which candidates' MVs a match may dirty: an in-place UPDATE that kept
    /// every column a query reads leaves its MV current (PGC-404).
    pub(super) relevance: ChangeRelevance<'a>,
}

/// The PgEval candidates the segment matrix did not cover, queued for
/// per-row evaluation: the dirtyable Fresh-MV ones apart from the rest.
struct FallbackQueues<'a> {
    fresh: &'a [UpdateQueryKey],
    rest: &'a [UpdateQueryKey],
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
        if row
            .relevance
            .touches(update_queries, update_query.fingerprint)
        {
            core.mv_dirty_mark(update_query.fingerprint);
        }
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
        .flat_map(|fingerprint| update_queries.queries_of(*fingerprint))
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

    /// Phase A of `update_queries_execute_batch`. Fresh-MV queries the change
    /// can touch must be fully evaluated so every match is dirty-marked (else
    /// a Fresh MV silently goes stale). The rest only decide whether the row
    /// belongs in the shared cache table — one match triggers the single
    /// upsert, so they short-circuit.
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

        let total_queries = update_queries.update_query_count();
        trace!("update_queries_execute_batch start [{total_queries}]");
        if total_queries == 0 {
            return Ok(false);
        }

        let mut matched = local_eval_match(core, update_queries, row);

        // One pass over the PgEval candidates. A batch-covered query reads
        // the segment matrix here, no round-trip (`frame_invalidations` is
        // already applied; the matrix is built unfiltered). An uncovered one
        // queues for per-row evaluation: a dirtyable Fresh MV the change can
        // touch apart, since every match of it must be reported, the rest
        // only to decide the upsert. The queues are the writer's scratch, so
        // steady state allocates nothing for them.
        let mut fresh_keys = std::mem::take(&mut self.fresh_keys);
        let mut rest_keys = std::mem::take(&mut self.rest_keys);
        fresh_keys.clear();
        rest_keys.clear();
        for update_query in maintained_candidates(core, update_queries, row.candidates)
            .filter(|q| q.eval_strategy == UpdateEvalStrategy::PgEval)
        {
            let key = update_query.key();
            let fingerprint = update_query.fingerprint;
            if let Some(view) = row.batch.as_ref().filter(|view| view.covers(key)) {
                if view.hit(key) {
                    trace!("update_queries batched pg-eval matched fingerprint {fingerprint}");
                    if row.relevance.touches(update_queries, fingerprint) {
                        core.mv_dirty_mark(fingerprint);
                    }
                    matched = true;
                }
            } else if core.mv_dirty_eval_required(fingerprint)
                && row.relevance.touches(update_queries, fingerprint)
            {
                fresh_keys.push(key);
            } else {
                rest_keys.push(key);
            }
        }
        let queues = FallbackQueues {
            fresh: &fresh_keys,
            rest: &rest_keys,
        };
        let result = self
            .pg_eval_fallback(core, update_queries, table_metadata, row, queues, matched)
            .await;
        self.fresh_keys = fresh_keys;
        self.rest_keys = rest_keys;
        result
    }

    /// Per-row fallback for non-batchable shapes and uncovered rows:
    /// `fresh` in full so each match dirty-marks its MV, `rest` only until
    /// something matches, since one match is enough for the upsert.
    async fn pg_eval_fallback(
        &mut self,
        core: &WriterCore,
        update_queries: &UpdateQueries,
        table_metadata: &TableMetadata,
        row: &MembershipRow<'_>,
        queues: FallbackQueues<'_>,
        mut matched: bool,
    ) -> CacheResult<bool> {
        let mut pg_hit = self
            .pg_eval_matches(
                update_queries,
                queues.fresh,
                table_metadata,
                row.row_data,
                |hit| core.mv_dirty_mark(hit),
            )
            .await?;
        matched |= pg_hit;
        if !matched
            && self
                .pg_eval_any(update_queries, queues.rest, table_metadata, row.row_data)
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
