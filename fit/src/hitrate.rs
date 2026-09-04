//! Infinite-cache replay: the theoretical maximum hit rate from repetition
//! plus subsumption, driven by the proxy's own serve decision
//! (`cache::serve_decision`). Write-driven invalidation is not simulated
//! (future mode); writes are counted so that extension has its inputs.

use std::collections::HashMap;

use pgcache_lib::cache::serve_decision::{
    AdmitAction, DecisionInput, EntrySnapshot, ServeDecision, serve_decide,
};
use pgcache_lib::query::Fingerprint;
use pgcache_lib::query::write::TransactionBoundary;
use pgcache_lib::settings::{CachePolicy, DEFAULT_ADMISSION_THRESHOLD};

use crate::classify::{AnalyzedStatement, Verdict};
use crate::subsume::SubsumerRegistry;

/// Proxy configuration the replay mirrors.
#[derive(Debug, Clone, Copy)]
pub struct ReplayConfig {
    /// A query registers on its Nth sighting; earlier sightings are forwarded.
    pub admission_threshold: u32,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        ReplayConfig {
            admission_threshold: DEFAULT_ADMISSION_THRESHOLD,
        }
    }
}

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
    /// Fingerprints answered from a subsuming registered query on the sighting
    /// that would otherwise have registered or stayed pending.
    pub subsumption_hits: u64,
    /// Admitted fingerprints that had to populate from origin.
    pub cold_misses: u64,
    /// Repeats asking for more rows than the cached window: a miss plus
    /// repopulation with the larger window, mirroring the proxy's limit bump.
    pub limit_bumps: u64,
    /// Sightings below the admission threshold: forwarded without registering.
    pub pending_forwards: u64,
    /// Forwards the proxy takes only under memory pressure or the registration
    /// rate cap — inputs offline replay never sets, so always zero.
    #[serde(skip_serializing_if = "is_zero")]
    pub pressure_forwards: u64,
}

fn is_zero(value: &u64) -> bool {
    *value == 0
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

/// Replay classified statements against an infinite cache, one call at a
/// time through the proxy's serve decision. Populations complete instantly
/// (no time axis), so `Loading` is never observed and a coalesced waiter —
/// which the proxy serves from cache once the population lands — cannot
/// arise. One proxy gate sits in front of the decision: SELECTs inside an
/// explicit transaction are forwarded (fit-local until PGC-406).
pub fn hitrate_replay(items: &[AnalyzedStatement], config: ReplayConfig) -> HitrateStats {
    let mut stats = HitrateStats::default();
    let mut entries: HashMap<Fingerprint, EntrySnapshot> = HashMap::new();
    let mut registry = SubsumerRegistry::new();
    // Per-session explicit-transaction state, keyed by the trace's backend
    // identity. Boundaries come from the parsed statement kind.
    let mut in_transaction: HashMap<u64, bool> = HashMap::new();

    for item in items {
        let calls = item.trace.calls.unwrap_or(1);
        stats.statements += 1;
        stats.calls += calls;
        let analysis = match &*item.verdict {
            Verdict::Write(_) => {
                stats.write_calls += calls;
                continue;
            }
            Verdict::Utility(boundary) => {
                stats.utility_calls += calls;
                if let Some(boundary) = boundary {
                    in_transaction
                        .insert(item.trace.session, *boundary == TransactionBoundary::Begin);
                }
                continue;
            }
            Verdict::Passthrough { .. } => {
                stats.non_cacheable_calls += calls;
                continue;
            }
            Verdict::Cacheable(analysis) => analysis,
        };
        if in_transaction
            .get(&item.trace.session)
            .copied()
            .unwrap_or(false)
        {
            stats.in_transaction_calls += calls;
            continue;
        }
        stats.cacheable_calls += calls;

        let fingerprint = analysis.fingerprint;
        let input = DecisionInput {
            rows_needed: analysis.rows_needed,
            admission_threshold: config.admission_threshold,
            cache_policy: CachePolicy::default(),
            throttled: false,
            pending_credit: 0,
        };
        for _ in 0..calls {
            let entry = entries.get(&fingerprint).copied();
            match serve_decide(entry.as_ref(), &input, || true) {
                ServeDecision::Hit | ServeDecision::Coalesce => stats.hits += 1,
                ServeDecision::LimitBump { .. } => {
                    stats.limit_bumps += 1;
                    if let Some(entry) = entry {
                        entries.insert(fingerprint, entry.population_complete(input.rows_needed));
                    }
                }
                ServeDecision::Register { transition, action } => {
                    let claimed = EntrySnapshot {
                        state: transition.new,
                        max_limit: None,
                    };
                    // The writer checks subsumption for both actions and serves
                    // a subsumed query from the parent's rows; only a query that
                    // ends up Ready becomes a subsumer itself.
                    if registry.query_subsumed(analysis) {
                        stats.subsumption_hits += 1;
                        entries
                            .insert(fingerprint, claimed.population_complete(analysis.max_limit));
                        registry.subsumer_register(analysis);
                    } else {
                        match action {
                            AdmitAction::Admit => {
                                stats.cold_misses += 1;
                                entries.insert(
                                    fingerprint,
                                    claimed.population_complete(analysis.max_limit),
                                );
                                registry.subsumer_register(analysis);
                            }
                            AdmitAction::CheckOnly => {
                                stats.pending_forwards += 1;
                                entries.insert(fingerprint, claimed);
                            }
                        }
                    }
                }
                ServeDecision::Forward(_) => stats.pressure_forwards += 1,
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
        replay_with(sqls, ReplayConfig::default())
    }

    fn replay_with(sqls: &[&str], config: ReplayConfig) -> HitrateStats {
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
                        sql: (*sql).into(),
                        parameters: Vec::new(),
                        calls: None,
                        total_time_ms: None,
                        session: 0,
                    },
                    parsed: std::rc::Rc::new(p),
                    verdict: std::rc::Rc::new(verdict),
                }
            })
            .collect();
        hitrate_replay(&items, config)
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
        let stats = hitrate_replay(
            &[AnalyzedStatement {
                trace: TraceStatement {
                    sql: sql.into(),
                    parameters: Vec::new(),
                    calls: Some(100),
                    total_time_ms: None,
                    session: 0,
                },
                parsed: std::rc::Rc::new(parsed),
                verdict: std::rc::Rc::new(verdict),
            }],
            ReplayConfig::default(),
        );
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.hits, 99);
        assert_eq!(stats.cacheable_calls, 100);
    }

    #[test]
    fn test_admission_threshold_forwards_until_reached() {
        let config = ReplayConfig {
            admission_threshold: 2,
        };
        let stats = replay_with(
            &[
                "SELECT * FROM users WHERE id = 1",
                "SELECT * FROM users WHERE id = 1",
                "SELECT * FROM users WHERE id = 1",
            ],
            config,
        );
        assert_eq!(stats.pending_forwards, 1);
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.cacheable_calls, 3);
    }

    #[test]
    fn test_pending_query_still_served_by_subsumer() {
        let config = ReplayConfig {
            admission_threshold: 2,
        };
        let stats = replay_with(
            &[
                "SELECT * FROM users",
                "SELECT * FROM users",
                "SELECT * FROM users WHERE id = 1",
            ],
            config,
        );
        assert_eq!(stats.pending_forwards, 1);
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.subsumption_hits, 1);
    }

    #[test]
    fn test_limit_bump_then_hit() {
        let stats = replay(&[
            "SELECT * FROM users ORDER BY id LIMIT 10",
            "SELECT * FROM users ORDER BY id LIMIT 20",
            "SELECT * FROM users ORDER BY id LIMIT 15",
        ]);
        assert_eq!(stats.cold_misses, 1);
        assert_eq!(stats.limit_bumps, 1);
        assert_eq!(stats.hits, 1);
    }
}
