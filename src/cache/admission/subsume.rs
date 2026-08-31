//! The subsumption-coverage decision, extracted from the writer's
//! `subsumption_check` (PGC-391). Pure over a [`SubsumerSource`]: the writer
//! supplies candidates from its per-relation index plus parent state,
//! pgcache-fit from its offline registry.

use crate::oid::Oid;
use crate::query::constraints::{QueryConstraints, TableConstraint, table_constraints_subsumed};

use super::{SubsumerCandidate, SubsumerSource};

/// Whether every relation of the new query is covered by some registered
/// candidate whose constraints are implied by the new query's. Candidate
/// gates: no LIMIT, parent Ready, parent references exactly one relation.
///
/// `relations` are the new query's `(relation_oid, table_name)` pairs; an
/// empty list is never covered. The new query's constraints must come from a
/// plain SELECT — set operations require per-branch analysis and are
/// rejected by the caller.
pub fn subsumption_covered(
    new_constraints: &QueryConstraints,
    relations: &[(Oid, &str)],
    source: &impl SubsumerSource,
) -> bool {
    if relations.is_empty() {
        return false;
    }
    let empty: Vec<TableConstraint> = Vec::new();
    relations.iter().all(|(relation_oid, table_name)| {
        let new_table_constraints = new_constraints
            .table_constraints
            .get(*table_name)
            .unwrap_or(&empty);
        source
            .candidates(*relation_oid, new_table_constraints)
            .any(|candidate| {
                let SubsumerCandidate {
                    constraints,
                    has_limit,
                    single_relation,
                    ready,
                } = candidate;
                !has_limit
                    && ready
                    && single_relation
                    && table_constraints_subsumed(new_constraints, constraints, table_name)
            })
    })
}
