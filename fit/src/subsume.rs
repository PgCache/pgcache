//! Offline subsumption registry. Admission gating and the coverage decision
//! are the proxy's own (`pgcache_lib::cache::admission`); this module only
//! stores admitted candidates per relation, one per exact reference as the
//! writer does, and per join shape (PGC-417) — indexed through the same
//! `ConstraintIndex` (ADR-037) — and answers lookups with every parent
//! Ready (infinite cache, nothing ever invalidates).

use std::collections::HashMap;
use std::hash::Hash;

use pgcache_lib::cache::UpdateQueryKey;
use pgcache_lib::cache::admission::{
    CandidateState, SubsumerCandidate, SubsumerSource, SubsumptionCover, SubsumptionReference,
    subsumption_covered,
};
use pgcache_lib::id_hash::IdHashable;
use pgcache_lib::oid::Oid;
use pgcache_lib::query::Fingerprint;
use pgcache_lib::query::constraint_index::ConstraintIndex;
use pgcache_lib::query::constraints::TableRanges;
use pgcache_lib::query::join_region::{JoinRegion, JoinShape};

use crate::classify::CacheableAnalysis;

#[derive(Default)]
pub struct SubsumerRegistry {
    /// Per relation, keyed by update query: the exact references.
    by_relation: HashMap<Oid, Subsumers<UpdateQueryKey>>,
    /// Per join shape, keyed by fingerprint: the parents' WHERE regions.
    by_shape: HashMap<JoinShape, Subsumers<Fingerprint>>,
}

/// The parents of one relation or shape: their regions, indexed.
struct Subsumers<K> {
    index: ConstraintIndex<K>,
    admitted: HashMap<K, AdmittedSubsumer>,
}

struct AdmittedSubsumer {
    fingerprint: Fingerprint,
    region: TableRanges,
    has_limit: bool,
}

impl<K: IdHashable + Copy> Default for Subsumers<K> {
    fn default() -> Self {
        Subsumers {
            index: ConstraintIndex::new(),
            admitted: HashMap::new(),
        }
    }
}

impl<K: IdHashable + Copy + Hash + Eq> Subsumers<K> {
    fn insert(&mut self, key: K, fingerprint: Fingerprint, region: &TableRanges, has_limit: bool) {
        self.index.insert(key, region);
        self.admitted.insert(
            key,
            AdmittedSubsumer {
                fingerprint,
                region: region.clone(),
                has_limit,
            },
        );
    }

    /// The parents whose region might hold every row `ranges` admits.
    fn candidates(&self, ranges: &TableRanges) -> impl Iterator<Item = SubsumerCandidate<'_>> {
        self.index
            .candidates(ranges)
            .into_iter()
            .filter_map(move |key| {
                self.admitted.get(&key).map(|subsumer| SubsumerCandidate {
                    fingerprint: subsumer.fingerprint,
                    region: &subsumer.region,
                    has_limit: subsumer.has_limit,
                    state: CandidateState::Ready,
                })
            })
    }
}

impl SubsumerRegistry {
    pub fn new() -> Self {
        SubsumerRegistry::default()
    }

    /// Store the query's eligible per-table admissions as subsumption
    /// candidates, keyed like the writer's update queries: by fingerprint and
    /// position among the query's references of the relation, and the query
    /// itself as a parent of its join shape when its WHERE region is exact.
    /// Eligibility was decided by the shared admission analysis; the
    /// remaining gates run per lookup in `subsumption_covered`.
    pub fn subsumer_register(&mut self, analysis: &CacheableAnalysis) {
        if let Some(region) = analysis.join_region.as_ref().filter(|region| region.exact) {
            self.by_shape.entry(region.shape).or_default().insert(
                analysis.fingerprint,
                analysis.fingerprint,
                &region.ranges,
                analysis.has_limit,
            );
        }
        let mut ordinals: HashMap<Oid, usize> = HashMap::new();
        for admission in &analysis.admissions {
            let ordinal = ordinals.entry(admission.relation_oid).or_default();
            let key = UpdateQueryKey {
                fingerprint: analysis.fingerprint,
                ordinal: *ordinal,
            };
            *ordinal += 1;
            let (true, Some(region)) = (admission.subsumer_eligible, &admission.subsumer_region)
            else {
                continue;
            };
            self.by_relation
                .entry(admission.relation_oid)
                .or_default()
                .insert(key, analysis.fingerprint, region, analysis.has_limit);
        }
    }

    /// Whether every relation the query references is covered by registered
    /// subsumers, and how — the writer's `subsumption_check`, offline.
    pub fn query_cover(&self, analysis: &CacheableAnalysis) -> Option<SubsumptionCover> {
        subsumption_covered(
            analysis.references.as_ref()?,
            analysis.join_region.as_ref(),
            self,
        )
    }

    #[cfg(test)]
    fn query_subsumed(&self, analysis: &CacheableAnalysis) -> bool {
        self.query_cover(analysis).is_some()
    }
}

impl SubsumerSource for SubsumerRegistry {
    fn candidates(
        &self,
        reference: &SubsumptionReference,
    ) -> impl Iterator<Item = SubsumerCandidate<'_>> {
        self.by_relation
            .get(&reference.relation_oid)
            .into_iter()
            .flat_map(move |relation| relation.candidates(&reference.ranges))
    }

    fn join_candidates(&self, join: &JoinRegion) -> impl Iterator<Item = SubsumerCandidate<'_>> {
        self.by_shape
            .get(&join.shape)
            .into_iter()
            .flat_map(move |shape| shape.candidates(&join.ranges))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_synth::catalog_synthesize;
    use crate::classify::{ParseOutcome, Verdict, statement_classify, statement_parse};
    use crate::volatility::builtin_functions_load;
    use pgcache_lib::query::ast::rope::QueryTree;

    /// Classify a corpus together (shared synthesized catalog), returning the
    /// cacheable analyses in corpus order.
    fn corpus_analyze(sqls: &[&str]) -> Vec<CacheableAnalysis> {
        let parsed: Vec<_> = sqls.iter().map(|sql| statement_parse(sql, &[])).collect();
        let corpus: Vec<QueryTree> = parsed
            .iter()
            .filter_map(|p| match &p.outcome {
                ParseOutcome::Select(expr) => Some(expr.clone()),
                _ => None,
            })
            .collect();
        let catalog = catalog_synthesize(corpus.iter());
        let builtins = builtin_functions_load();
        parsed
            .iter()
            .map(
                |p| match statement_classify(p, &catalog.tables, &builtins) {
                    Verdict::Cacheable(analysis) => *analysis,
                    _ => panic!("expected cacheable corpus"),
                },
            )
            .collect()
    }

    #[test]
    fn test_unconstrained_parent_subsumes_constrained_child() {
        let analyses = corpus_analyze(&["SELECT * FROM users", "SELECT * FROM users WHERE id = 5"]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_narrow_parent_does_not_subsume_broader_child() {
        let analyses = corpus_analyze(&["SELECT * FROM users WHERE id = 5", "SELECT * FROM users"]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_range_parent_subsumes_tighter_range() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM orders WHERE total > 10",
            "SELECT * FROM orders WHERE total > 100",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(registry.query_subsumed(&analyses[1]));
    }

    /// Each arm of a same-relation set operation is loaded in full, so each
    /// is a parent on its own, as in the writer. A child spanning both arms
    /// is covered value by value (PGC-494); an arm the model can't read
    /// whole is no parent (PGC-511).
    #[test]
    fn test_relation_in_two_branches_each_arm_admitted() {
        let analyses = corpus_analyze(&[
            "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM users WHERE id = 2",
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 3",
            "SELECT * FROM users WHERE id IN (1, 2)",
            "SELECT id FROM users WHERE id = 2 UNION ALL SELECT id FROM users WHERE id = 1",
            "SELECT id FROM users WHERE id = 1 \
             UNION ALL SELECT id FROM users WHERE id = 2 OR name = 'a'",
            "SELECT * FROM users WHERE id = 2",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(registry.query_subsumed(&analyses[1]));
        assert!(!registry.query_subsumed(&analyses[2]));
        assert!(
            registry.query_subsumed(&analyses[3]),
            "each value under an arm"
        );
        assert!(registry.query_subsumed(&analyses[4]), "each arm covered");

        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[5]);
        assert!(registry.query_subsumed(&analyses[1]));
        assert!(
            !registry.query_subsumed(&analyses[6]),
            "the OR arm is no region"
        );
    }

    /// Per-literal point parents cover an IN-list child value by value; a
    /// value no parent holds leaves it uncovered (PGC-494).
    #[test]
    fn test_point_parents_cover_in_set_child() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 2",
            "SELECT * FROM users WHERE id = 3",
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            "SELECT * FROM users WHERE id IN (1, 4)",
            "SELECT * FROM users WHERE id IN (3, 1) AND name = 'a'",
        ]);
        let mut registry = SubsumerRegistry::new();
        for parent in &analyses[..3] {
            registry.subsumer_register(parent);
        }
        assert!(registry.query_subsumed(&analyses[3]));
        assert!(!registry.query_subsumed(&analyses[4]), "4 has no parent");
        assert!(registry.query_subsumed(&analyses[5]));
    }

    /// A set-operation child is covered when every arm is, and not when one
    /// arm reads a relation no parent holds (PGC-495).
    #[test]
    fn test_set_operation_child_covered_per_arm() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM users WHERE id > 0",
            "SELECT * FROM orders WHERE total > 0",
            "SELECT id FROM users WHERE id = 1 UNION SELECT id FROM orders WHERE total = 5",
            "SELECT id FROM users WHERE id = 1 INTERSECT SELECT id FROM orders WHERE total = 5",
            "SELECT id FROM users WHERE id = 1 EXCEPT SELECT id FROM orders WHERE total < 0",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(
            !registry.query_subsumed(&analyses[2]),
            "orders arm uncovered"
        );
        registry.subsumer_register(&analyses[1]);
        assert!(registry.query_subsumed(&analyses[2]));
        assert!(registry.query_subsumed(&analyses[3]));
        assert!(
            !registry.query_subsumed(&analyses[4]),
            "total < 0 is outside total > 0"
        );
    }

    #[test]
    fn test_limit_parent_not_admitted() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM users LIMIT 10",
            "SELECT * FROM users WHERE id = 5",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_inner_join_parent_not_admitted() {
        // The inner join drops users without orders, so the parent didn't
        // load every user.
        let analyses = corpus_analyze(&[
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            "SELECT * FROM users WHERE id = 5",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_left_join_preserved_parent_subsumes() {
        // A LEFT JOIN keeps every user its WHERE admits, whatever the ON
        // clause says; the orders it loaded are only the matched ones.
        let analyses = corpus_analyze(&[
            "SELECT * FROM users u LEFT JOIN orders o \
             ON u.id = o.user_id AND o.rank = u.id + 1 WHERE u.name = 'a'",
            "SELECT * FROM users WHERE name = 'a' AND id = 5",
            "SELECT * FROM orders WHERE user_id = 5",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(registry.query_subsumed(&analyses[1]));
        assert!(!registry.query_subsumed(&analyses[2]));
    }

    #[test]
    fn test_incomplete_where_parent_not_admitted() {
        // An OR is an opaque conjunct, so the reference is not exact.
        let analyses = corpus_analyze(&[
            "SELECT * FROM users WHERE id = 1 OR name = 'a'",
            "SELECT * FROM users WHERE id = 1",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_self_join_parent_not_admitted() {
        // PGC-256: self-joined update queries are never subsumers.
        let analyses = corpus_analyze(&[
            "SELECT * FROM emp e JOIN emp m ON e.manager_id = m.id",
            "SELECT * FROM emp WHERE id = 1",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    /// A cached join parents a join of its shape under a WHERE that implies
    /// its own, whatever the join's references load (PGC-417); another
    /// shape, a wider WHERE or a WHERE the model can't read whole is no
    /// cover. A LIMIT on a join caps no population (ADR-060), so it still
    /// parents.
    #[test]
    fn test_same_shape_join_parent_subsumes_narrower_where() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id > 0",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 5 AND o.total = 1",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE o.total = 1",
            "SELECT * FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.id = 5",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id > 0 OR o.total = 1",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 5",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id > 0 LIMIT 10",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert_eq!(
            registry.query_cover(&analyses[1]),
            Some(SubsumptionCover::Join)
        );
        assert!(!registry.query_subsumed(&analyses[2]), "wider WHERE");
        assert!(!registry.query_subsumed(&analyses[3]), "another shape");

        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[4]);
        assert!(!registry.query_subsumed(&analyses[5]), "an OR is no parent");
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[6]);
        assert!(!analyses[6].has_limit, "a join's LIMIT caps no population");
        assert_eq!(
            registry.query_cover(&analyses[5]),
            Some(SubsumptionCover::Join)
        );
    }

    #[test]
    fn test_join_child_covered_by_two_single_table_parents() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM users",
            "SELECT * FROM orders",
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        registry.subsumer_register(&analyses[1]);
        assert!(registry.query_subsumed(&analyses[2]));
    }

    #[test]
    fn test_derived_table_branch_constraints_prevent_false_subsumption() {
        // The derived table caches only id=5 rows; its per-branch constraints
        // (from the shared admission analysis) must not cover other literals.
        let analyses = corpus_analyze(&[
            "SELECT * FROM (SELECT * FROM users WHERE id = 5) s",
            "SELECT * FROM users WHERE id = 7",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_cross_schema_same_name_not_subsumed() {
        let analyses = corpus_analyze(&[
            "SELECT * FROM tenant_a.users",
            "SELECT * FROM tenant_b.users WHERE id = 5",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }
}
