//! Offline subsumption: which cacheable queries would be served from data an
//! already-registered broader query holds. Mirrors the writer's
//! `subsumption_check` gates: a subsumer must be a plain SELECT over exactly
//! one relation (not self-joined), without LIMIT, with fully analyzed WHERE
//! constraints; a set-operation query can never be subsumed.

use std::collections::HashMap;

use ecow::EcoString;
use pgcache_lib::query::constraints::{QueryConstraints, table_constraints_subsumed};

use crate::classify::CacheableAnalysis;

#[derive(Default)]
pub struct SubsumerRegistry {
    by_table: HashMap<EcoString, Vec<QueryConstraints>>,
}

impl SubsumerRegistry {
    pub fn new() -> Self {
        SubsumerRegistry::default()
    }

    /// Admit a registered query as a subsumption parent if it passes all
    /// eligibility gates; otherwise a no-op.
    pub fn subsumer_register(&mut self, analysis: &CacheableAnalysis) {
        if analysis.relations.len() != 1 || analysis.self_joined || analysis.has_limit {
            return;
        }
        let Some(constraints) = &analysis.constraints else {
            return;
        };
        if !constraints.where_analysis_complete {
            return;
        }
        let Some(table) = analysis.relations.first() else {
            return;
        };
        self.by_table
            .entry(table.clone())
            .or_default()
            .push(constraints.clone());
    }

    /// Whether every relation the query references is covered by some
    /// registered subsumer whose constraints are implied by the query's.
    pub fn query_subsumed(&self, analysis: &CacheableAnalysis) -> bool {
        if analysis.relations.is_empty() {
            return false;
        }
        let Some(new_constraints) = &analysis.constraints else {
            return false;
        };
        analysis.relations.iter().all(|table| {
            self.by_table.get(table).is_some_and(|candidates| {
                candidates
                    .iter()
                    .any(|cached| table_constraints_subsumed(new_constraints, cached, table))
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_synth::catalog_synthesize;
    use crate::classify::{ParseOutcome, Verdict, statement_classify, statement_parse};
    use crate::input::TraceStatement;
    use crate::volatility::builtin_functions_load;
    use pgcache_lib::query::ast::QueryExpr;

    /// Classify a corpus together (shared synthesized catalog), returning the
    /// cacheable analyses in corpus order.
    fn corpus_analyze(sqls: &[&str]) -> Vec<CacheableAnalysis> {
        let parsed: Vec<_> = sqls
            .iter()
            .map(|sql| {
                statement_parse(TraceStatement {
                    sql: (*sql).to_owned(),
                    parameters: Vec::new(),
                    calls: 1,
                    total_time_ms: None,
                })
            })
            .collect();
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
    fn test_multi_table_parent_not_admitted() {
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
}
