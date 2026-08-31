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

pub use analyze::query_admission_analyze;
pub use subsume::subsumption_covered;

/// Everything the writer stores (and pgcache-fit simulates) for one table of
/// an admitted query.
pub struct TableAdmission {
    pub relation_oid: Oid,
    pub table_name: EcoString,
    /// Fully built update query, `change_dependent` included.
    pub update_query: UpdateQuery,
    /// The relation appears more than once in this update query (a
    /// self-join): its name-collapsed constraints only hold for one arm
    /// (PGC-256), so it is indexed unconstrained and never subsumes.
    pub self_joined: bool,
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
