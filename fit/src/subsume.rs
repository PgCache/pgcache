//! Offline subsumption registry. Admission gating and the coverage decision
//! are the proxy's own (`pgcache_lib::cache::admission`); this module only
//! stores admitted candidates per relation — indexed through the same
//! `ConstraintIndex` the writer uses (ADR-037) — and answers lookups with
//! `ready` always true (infinite cache, nothing ever invalidates).

use std::collections::HashMap;

use pgcache_lib::cache::admission::{SubsumerCandidate, SubsumerSource, subsumption_covered};
use pgcache_lib::oid::Oid;
use pgcache_lib::query::Fingerprint;
use pgcache_lib::query::constraint_index::ConstraintIndex;
use pgcache_lib::query::constraints::{QueryConstraints, TableConstraint};

use crate::classify::CacheableAnalysis;

#[derive(Default)]
pub struct SubsumerRegistry {
    by_relation: HashMap<Oid, RelationSubsumers>,
}

struct RelationSubsumers {
    index: ConstraintIndex<Fingerprint>,
    admitted: HashMap<Fingerprint, AdmittedSubsumer>,
}

struct AdmittedSubsumer {
    constraints: std::rc::Rc<QueryConstraints>,
    has_limit: bool,
    single_relation: bool,
}

impl RelationSubsumers {
    fn new() -> Self {
        RelationSubsumers {
            index: ConstraintIndex::new(),
            admitted: HashMap::new(),
        }
    }
}

impl SubsumerRegistry {
    pub fn new() -> Self {
        SubsumerRegistry::default()
    }

    /// Store the query's eligible per-table admissions as subsumption
    /// candidates. Eligibility was decided by the shared admission analysis;
    /// the remaining gates run per lookup in `subsumption_covered`.
    pub fn subsumer_register(&mut self, analysis: &CacheableAnalysis) {
        // The writer's gate counts relation_oids with duplicates (one per
        // update query), so a same-table UNION parent is multi-relation.
        let single_relation = analysis.admissions.len() == 1;
        for admission in &analysis.admissions {
            if !admission.subsumer_eligible {
                continue;
            }
            let relation = self
                .by_relation
                .entry(admission.relation_oid)
                .or_insert_with(RelationSubsumers::new);
            relation
                .index
                .insert(analysis.fingerprint, &admission.index_constraints);
            relation.admitted.insert(
                analysis.fingerprint,
                AdmittedSubsumer {
                    constraints: std::rc::Rc::clone(&admission.constraints),
                    has_limit: analysis.has_limit,
                    single_relation,
                },
            );
        }
    }

    /// Whether every relation the query references is covered by some
    /// registered subsumer — the writer's `subsumption_check`, offline.
    pub fn query_subsumed(&self, analysis: &CacheableAnalysis) -> bool {
        let Some(new_constraints) = &analysis.constraints else {
            return false;
        };
        let relations: Vec<(Oid, &str)> = analysis
            .relations
            .iter()
            .map(|(oid, name)| (*oid, name.as_str()))
            .collect();
        subsumption_covered(new_constraints, &relations, self)
    }
}

impl SubsumerSource for SubsumerRegistry {
    fn candidates(
        &self,
        relation_oid: Oid,
        table_constraints: &[TableConstraint],
    ) -> impl Iterator<Item = SubsumerCandidate<'_>> {
        self.by_relation
            .get(&relation_oid)
            .into_iter()
            .flat_map(move |relation| {
                relation
                    .index
                    .candidates(table_constraints)
                    .into_iter()
                    .filter_map(move |fingerprint| {
                        relation
                            .admitted
                            .get(&fingerprint)
                            .map(|subsumer| SubsumerCandidate {
                                constraints: &subsumer.constraints,
                                has_limit: subsumer.has_limit,
                                single_relation: subsumer.single_relation,
                                ready: true,
                            })
                    })
            })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_synth::catalog_synthesize;
    use crate::classify::{ParseOutcome, Verdict, statement_classify, statement_parse};
    use crate::volatility::builtin_functions_load;
    use pgcache_lib::query::ast::QueryExpr;

    /// Classify a corpus together (shared synthesized catalog), returning the
    /// cacheable analyses in corpus order.
    fn corpus_analyze(sqls: &[&str]) -> Vec<CacheableAnalysis> {
        let parsed: Vec<_> = sqls.iter().map(|sql| statement_parse(sql, &[])).collect();
        let corpus: Vec<QueryExpr> = parsed
            .iter()
            .filter_map(|p| match &p.outcome {
                ParseOutcome::Select(expr) => Some((**expr).clone()),
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
    fn test_multi_table_parent_rejected_at_lookup() {
        // Multi-table parents are indexed (like the writer) but rejected per
        // lookup by the single_relation gate.
        let analyses = corpus_analyze(&[
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id",
            "SELECT * FROM users WHERE id = 5",
        ]);
        let mut registry = SubsumerRegistry::new();
        registry.subsumer_register(&analyses[0]);
        assert!(!registry.query_subsumed(&analyses[1]));
    }

    #[test]
    fn test_incomplete_where_parent_not_admitted() {
        // OR breaks constraint extraction → where_analysis_complete = false.
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
