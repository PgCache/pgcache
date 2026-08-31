//! Infinite-cache replay: the theoretical maximum hit rate from repetition
//! plus subsumption. Write-driven invalidation is not simulated (future mode);
//! writes are counted so that extension has its inputs.

use std::collections::HashMap;

use pgcache_lib::cache::query::limit_is_sufficient;
use pgcache_lib::query::Fingerprint;
use pgcache_lib::query::write::TransactionBoundary;

use crate::classify::{AnalyzedStatement, Verdict};
use crate::subsume::SubsumerRegistry;

#[derive(Debug, Default, Clone, serde::Serialize)]
pub struct HitrateStats {
    pub statements: u64,
    pub calls: u64,
    pub write_calls: u64,
    pub utility_calls: u64,
    pub non_cacheable_calls: u64,
    /// Cacheable SELECTs issued inside an explicit transaction — the proxy
    /// forwards these (transaction gate), so they can never be hits.
    pub in_transaction_calls: u64,
    pub cacheable_calls: u64,
    /// Calls answered by an already-registered fingerprint with a sufficient
    /// cached LIMIT window.
    pub hits: u64,
    /// First-seen fingerprints answered from a subsuming registered query.
    pub subsumption_hits: u64,
    /// First-seen fingerprints that had to populate from origin.
    pub cold_misses: u64,
    /// Repeats asking for more rows than the cached window: a miss plus
    /// repopulation with the larger window, mirroring the proxy's limit bump.
    pub limit_bumps: u64,
}

impl HitrateStats {
    pub fn rate_over_cacheable(&self) -> f64 {
        rate(self.hits + self.subsumption_hits, self.cacheable_calls)
    }

    pub fn rate_over_selects(&self) -> f64 {
        rate(
            self.hits + self.subsumption_hits,
            self.cacheable_calls + self.non_cacheable_calls + self.in_transaction_calls,
        )
    }

    pub fn rate_over_all(&self) -> f64 {
        rate(self.hits + self.subsumption_hits, self.calls)
    }
}

fn rate(hits: u64, total: u64) -> f64 {
    if total == 0 {
        0.0
    } else {
        hits as f64 / total as f64
    }
}

/// Replay classified statements against an infinite cache. Admission
/// threshold is 1 (pgcache's default): a cacheable query registers on first
/// sight, so of its `calls`, the first is a cold miss (or subsumption hit)
/// and the rest are hits. Two proxy serve gates are modeled: SELECTs inside
/// an explicit transaction are forwarded, and a repeat needing more rows
/// than the cached LIMIT window is a limit bump, not a hit.
pub fn hitrate_replay(items: &[AnalyzedStatement]) -> HitrateStats {
    let mut stats = HitrateStats::default();
    // Registered fingerprint → cached LIMIT window (`None` = unlimited).
    let mut seen: HashMap<Fingerprint, Option<u64>> = HashMap::new();
    let mut registry = SubsumerRegistry::new();
    // Per-session explicit-transaction state, keyed by the trace's backend
    // identity. Boundaries come from the parsed statement kind.
    let mut in_transaction: HashMap<u64, bool> = HashMap::new();

    for item in items {
        let calls = item.trace.calls.max(1);
        stats.statements += 1;
        stats.calls += calls;
        match &*item.verdict {
            Verdict::Write(_) => stats.write_calls += calls,
            Verdict::Utility(boundary) => {
                stats.utility_calls += calls;
                if let Some(boundary) = boundary {
                    in_transaction
                        .insert(item.trace.session, *boundary == TransactionBoundary::Begin);
                }
            }
            Verdict::Passthrough { .. } => stats.non_cacheable_calls += calls,
            Verdict::Cacheable(analysis) => {
                if in_transaction
                    .get(&item.trace.session)
                    .copied()
                    .unwrap_or(false)
                {
                    stats.in_transaction_calls += calls;
                    continue;
                }
                stats.cacheable_calls += calls;
                match seen.get_mut(&analysis.fingerprint) {
                    Some(cached_max) => {
                        if limit_is_sufficient(*cached_max, analysis.max_limit) {
                            stats.hits += calls;
                        } else {
                            stats.limit_bumps += 1;
                            stats.hits += calls - 1;
                            *cached_max = analysis.max_limit;
                        }
                    }
                    None => {
                        seen.insert(analysis.fingerprint, analysis.max_limit);
                        if registry.query_subsumed(analysis) {
                            // Served from a broader registered query's rows;
                            // no population round-trip.
                            stats.subsumption_hits += calls;
                        } else {
                            stats.cold_misses += 1;
                            stats.hits += calls - 1;
                        }
                        registry.subsumer_register(analysis);
                    }
                }
            }
        }
    }
    stats
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_synth::catalog_synthesize;
    use crate::classify::{ParseOutcome, statement_classify, statement_parse};
    use crate::input::TraceStatement;
    use crate::volatility::builtin_functions_load;
    use pgcache_lib::query::ast::QueryExpr;

    fn replay(sqls: &[&str]) -> HitrateStats {
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
        let items: Vec<AnalyzedStatement> = sqls
            .iter()
            .zip(parsed)
            .map(|(sql, p)| {
                let verdict = statement_classify(&p, &catalog.tables, &builtins);
                AnalyzedStatement {
                    trace: TraceStatement {
                        sql: (*sql).to_owned(),
                        parameters: Vec::new(),
                        calls: 1,
                        total_time_ms: None,
                        session: 0,
                    },
                    parsed: std::rc::Rc::new(p),
                    verdict: std::rc::Rc::new(verdict),
                }
            })
            .collect();
        hitrate_replay(&items)
    }

    #[test]
    fn test_repeated_literal_hits() {
        let stats = replay(&[
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 1",
        ]);
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.subsumption_hits, 0);
    }

    #[test]
    fn test_distinct_literals_rescued_by_subsuming_parent() {
        let stats = replay(&[
            "SELECT * FROM users",
            "SELECT * FROM users WHERE id = 1",
            "SELECT * FROM users WHERE id = 2",
            "SELECT * FROM users WHERE id = 3",
        ]);
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.subsumption_hits, 3);
    }

    #[test]
    fn test_writes_and_utility_counted_not_cached() {
        let stats = replay(&[
            "BEGIN",
            "UPDATE users SET name = 'x' WHERE id = 1",
            "COMMIT",
            "SELECT * FROM users WHERE id = 1",
        ]);
        assert_eq!(stats.write_calls, 1);
        assert_eq!(stats.utility_calls, 2);
        assert_eq!(stats.cacheable_calls, 1);
        assert_eq!(stats.cold_misses, 1);
    }

    #[test]
    fn test_pgss_calls_weighting() {
        let sql = "SELECT * FROM users WHERE id = 7";
        let parsed = statement_parse(sql, &[]);
        let corpus: Vec<QueryExpr> = match &parsed.outcome {
            ParseOutcome::Select(expr) => vec![(**expr).clone()],
            _ => vec![],
        };
        let catalog = catalog_synthesize(corpus.iter());
        let builtins = builtin_functions_load();
        let verdict = statement_classify(&parsed, &catalog.tables, &builtins);
        let stats = hitrate_replay(&[AnalyzedStatement {
            trace: TraceStatement {
                sql: sql.to_owned(),
                parameters: Vec::new(),
                calls: 100,
                total_time_ms: None,
                session: 0,
            },
            parsed: std::rc::Rc::new(parsed),
            verdict: std::rc::Rc::new(verdict),
        }]);
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.hits, 99);
        assert_eq!(stats.cacheable_calls, 100);
    }
}
