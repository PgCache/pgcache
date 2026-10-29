//! Pure admission analysis, shared by the cache writer and pgcache-fit
//! (PGC-391): decorrelation → per-table update-query derivation → constraint
//! analysis → subsumer-eligibility gates, plus the subsumption-coverage
//! decision. Everything here is a pure function of the resolved query and the
//! catalog; the writer layers storage and parent-readiness on top, pgcache-fit
//! replays the same functions offline over a synthesized catalog.

use ecow::EcoString;

use super::update_query::UpdateQuery;
use crate::oid::Oid;
use crate::query::constraints::TableRanges;
use crate::query::join_region::JoinRegion;

mod analyze;
mod subsume;
mod update_classify;

#[cfg(feature = "proxy")]
pub(crate) use analyze::shape_gate_classify;
pub use analyze::{
    AdmissionDepth, AdmissionQuery, CanonicalResolution, base_query_prepare, population_max_limit,
    query_admission_analyze, query_decorrelate_checked, query_pushdown, query_resolve_canonical,
};
pub use subsume::{
    SubsumptionCover, SubsumptionReference, subsumption_covered, subsumption_references,
};

/// Everything the writer stores (and pgcache-fit simulates) for one table of
/// an admitted query.
pub struct TableAdmission {
    pub relation_oid: Oid,
    pub table_name: EcoString,
    /// The built update query. With [`AdmissionDepth::DecisionOnly`] the
    /// CDC-eval caches (`compiled_where`, `pg_eval_template`,
    /// `pg_batchable`, `change_dependent`) are left unset.
    pub update_query: UpdateQuery,
    /// Population loads every row the update query's filters describe, so
    /// they are a subsumption region but for a LIMIT, which a limit bump can
    /// change after registration.
    pub subsumer_shape: bool,
    /// Mirrors the writer's `can_subsume`: eligible for the subsumption
    /// index. Parent readiness is checked per lookup in
    /// [`subsumption_covered`].
    pub subsumer_eligible: bool,
}

pub struct AdmissionAnalysis {
    /// Whether decorrelation transformed the query.
    pub transformed: bool,
    pub tables: Vec<TableAdmission>,
    /// The query's join region when it can parent a join of its shape: one
    /// SELECT over several base tables whose WHERE the model read whole, so
    /// population loaded exactly the region (PGC-417). The index gates on
    /// `has_limit` like a table's, though a join's population is never
    /// capped (ADR-060), so today a limited join parents too.
    pub join_region: Option<JoinRegion>,
}

/// One registered subsumption candidate as seen at lookup time.
pub struct SubsumerCandidate<'a> {
    /// The parent's region: it holds every row these ranges describe.
    pub region: &'a TableRanges,
    pub has_limit: bool,
    /// The parent is Ready: populated, not invalidated. Always true for
    /// pgcache-fit's infinite-cache model.
    pub ready: bool,
}

/// Source of subsumption candidates: per relation, and per join shape. The
/// writer implements this over its subsumption `ConstraintIndex`es plus
/// parent state; pgcache-fit over its offline registry.
pub trait SubsumerSource {
    fn candidates(
        &self,
        reference: &SubsumptionReference,
    ) -> impl Iterator<Item = SubsumerCandidate<'_>>;

    /// The parents of `join`'s shape whose region might hold its rows; the
    /// region is over the join's WHERE constraints keyed by slot.
    fn join_candidates(&self, join: &JoinRegion) -> impl Iterator<Item = SubsumerCandidate<'_>>;
}
