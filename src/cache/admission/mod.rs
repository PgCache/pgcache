//! Pure admission analysis, shared by the cache writer and pgcache-fit
//! (PGC-391): decorrelation → per-table update-query derivation → constraint
//! analysis → subsumer-eligibility gates, plus the subsumption-coverage
//! decision. Everything here is a pure function of the resolved query and the
//! catalog; the writer layers storage and parent-readiness on top, pgcache-fit
//! replays the same functions offline over a synthesized catalog.

use ecow::EcoString;

use crate::oid::Oid;
use crate::query::constraints::{QueryConstraints, TableConstraint};

use super::update_query::UpdateQuery;

mod analyze;
mod subsume;
mod update_classify;

pub use analyze::{
    AdmissionDepth, base_query_prepare, query_admission_analyze, shape_gate_classify,
};
pub use subsume::subsumption_covered;

/// Everything the writer stores (and pgcache-fit simulates) for one table of
/// an admitted query.
pub struct TableAdmission {
    pub relation_oid: Oid,
    pub table_name: EcoString,
    /// The built update query. With [`AdmissionDepth::DecisionOnly`] the
    /// CDC-eval caches (`compiled_where`, `pg_eval_template`,
    /// `pg_batchable`, `change_dependent`) are left unset.
    pub update_query: UpdateQuery,
    /// Mirrors the writer's `can_subsume`: eligible for the subsumption
    /// index. The remaining gates (parent readiness, parent single-relation)
    /// are checked per lookup in [`subsumption_covered`].
    pub subsumer_eligible: bool,
    /// Constraints to index under (empty for self-joins — the broadest
    /// class, so no true match is dropped).
    pub index_constraints: Vec<TableConstraint>,
}

pub struct AdmissionAnalysis {
    /// Whether decorrelation transformed the query.
    pub transformed: bool,
    pub tables: Vec<TableAdmission>,
}

/// One registered subsumption candidate as seen at lookup time.
pub struct SubsumerCandidate<'a> {
    /// The candidate update query's constraints (per-table, per-branch).
    pub constraints: &'a QueryConstraints,
    pub has_limit: bool,
    /// The candidate's parent query references exactly one relation.
    /// Multi-table parents have implicit join filtering constraint analysis
    /// doesn't capture, so coverage can't be reasoned about.
    pub single_relation: bool,
    /// The parent is Ready: populated, not invalidated. Always true for
    /// pgcache-fit's infinite-cache model.
    pub ready: bool,
}

/// Source of subsumption candidates for one relation. The writer implements
/// this over the per-relation subsumption `ConstraintIndex` plus parent
/// state; pgcache-fit over its offline registry.
pub trait SubsumerSource {
    fn candidates(
        &self,
        relation_oid: Oid,
        table_constraints: &[TableConstraint],
    ) -> impl Iterator<Item = SubsumerCandidate<'_>>;
}
