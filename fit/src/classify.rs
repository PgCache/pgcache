//! Per-statement verdict pipeline, mirroring the proxy's two stages offline:
//! Stage A (syntactic cacheability, no catalog) and Stage B (resolution,
//! decorrelation, constraint analysis against the synthesized catalog).

use ecow::EcoString;
use iddqd::BiHashMap;
use pgcache_lib::cache::admission::{
    base_query_prepare, query_admission_analyze, shape_gate_classify,
};
use pgcache_lib::cache::{CacheabilityError, CacheableQuery};
use pgcache_lib::catalog::TableMetadata;
use pgcache_lib::oid::Oid;
use pgcache_lib::query::ast::{
    QueryExpr, RawStatement, query_expr_fingerprint, statement_convert_raw,
};
use pgcache_lib::query::constraints::{
    QueryConstraints, TableConstraint, analyze_query_constraints,
};
use pgcache_lib::query::resolve::query_expr_resolve;
use pgcache_lib::query::transform::{predicate_pushdown_apply, query_expr_parameters_replace};
use pgcache_lib::query::write::WriteClass;
use pgcache_lib::query::{Fingerprint, ShapeKey, query_shape_derive};

use crate::input::{TraceStatement, query_parameters_infer};
use crate::volatility::BuiltinFunctions;

/// Result of parsing one traced statement through the converter (and
/// substituting logged bind parameters when present).
pub enum ParseOutcome {
    Select(Box<QueryExpr>),
    SelectUnconvertible {
        error: String,
        cte_write: Option<WriteClass>,
    },
    Write(WriteClass),
    Utility,
    ParseError(String),
    ParameterError(String),
}

pub struct ParsedStatement {
    pub trace: TraceStatement,
    pub outcome: ParseOutcome,
    /// Parameters whose type OID was inferred from the value text.
    pub inferred_parameters: usize,
}

pub fn statement_parse(trace: TraceStatement) -> ParsedStatement {
    let mut inferred_parameters = 0;
    let raw = pg_query::parse_raw_scoped(&trace.sql, |tree| unsafe { statement_convert_raw(tree) });
    let outcome = match raw {
        Err(parse_error) => ParseOutcome::ParseError(parse_error.to_string()),
        // Structural failure (empty input, multiple statements in one payload).
        Ok(Err(ast_error)) => ParseOutcome::ParseError(ast_error.to_string()),
        Ok(Ok(RawStatement::Select {
            converted,
            cte_write,
        })) => match converted {
            Err(ast_error) => ParseOutcome::SelectUnconvertible {
                error: ast_error.to_string(),
                cte_write,
            },
            Ok(expr) => {
                if trace.parameters.is_empty() {
                    ParseOutcome::Select(expr)
                } else {
                    let (parameters, inferred) = query_parameters_infer(&trace.parameters);
                    inferred_parameters = inferred;
                    match query_expr_parameters_replace(&expr, &parameters) {
                        Ok(substituted) => ParseOutcome::Select(Box::new(substituted)),
                        Err(error) => ParseOutcome::ParameterError(error.to_string()),
                    }
                }
            }
        },
        Ok(Ok(RawStatement::Write(write_class))) => ParseOutcome::Write(write_class),
        Ok(Ok(RawStatement::ReadOnlyUtility)) => ParseOutcome::Utility,
    };
    ParsedStatement {
        trace,
        outcome,
        inferred_parameters,
    }
}

/// Flat fit-owned reason enum mapping the lib's error layers, so the report
/// stays stable if lib variants shift.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, serde::Serialize)]
pub enum PassthroughReason {
    ParseError,
    ConversionUnsupported,
    ParameterSubstitution,
    UnsupportedQueryType,
    UnsupportedFrom,
    UnsupportedSubquery,
    UnsupportedWhereClause,
    NonImmutableFunction,
    HasLimit,
    SystemCatalogReference,
    ResolutionFailed,
    DecorrelationFailed,
}

impl PassthroughReason {
    pub fn label(self) -> &'static str {
        match self {
            PassthroughReason::ParseError => "parse error",
            PassthroughReason::ConversionUnsupported => "unsupported construct (conversion)",
            PassthroughReason::ParameterSubstitution => "parameter substitution failed",
            PassthroughReason::UnsupportedQueryType => "unsupported query type",
            PassthroughReason::UnsupportedFrom => "unsupported FROM clause",
            PassthroughReason::UnsupportedSubquery => "unsupported subquery",
            PassthroughReason::UnsupportedWhereClause => "unsupported WHERE clause",
            PassthroughReason::NonImmutableFunction => "non-immutable function",
            PassthroughReason::HasLimit => "LIMIT not cacheable here",
            PassthroughReason::SystemCatalogReference => "system catalog reference",
            PassthroughReason::ResolutionFailed => "resolution failed",
            PassthroughReason::DecorrelationFailed => "non-decorrelatable subquery",
        }
    }
}

fn cacheability_reason(error: &CacheabilityError) -> PassthroughReason {
    match error {
        CacheabilityError::UnsupportedQueryType => PassthroughReason::UnsupportedQueryType,
        CacheabilityError::UnsupportedFrom => PassthroughReason::UnsupportedFrom,
        CacheabilityError::UnsupportedSubquery => PassthroughReason::UnsupportedSubquery,
        CacheabilityError::UnsupportedWhereClause => PassthroughReason::UnsupportedWhereClause,
        CacheabilityError::NonImmutableFunction => PassthroughReason::NonImmutableFunction,
        CacheabilityError::HasLimit => PassthroughReason::HasLimit,
        CacheabilityError::SystemCatalogReference => PassthroughReason::SystemCatalogReference,
    }
}

/// Stage A + B analysis of a cacheable SELECT, carrying the pieces of the
/// shared admission analysis the offline model needs.
pub struct CacheableAnalysis {
    pub fingerprint: Fingerprint,
    pub shape_key: ShapeKey,
    /// Distinct referenced relations, keyed like the writer: by oid.
    pub relations: Vec<(Oid, EcoString)>,
    pub has_limit: bool,
    /// Rows needed to satisfy the query's LIMIT+OFFSET (`None` = unlimited),
    /// mirroring the writer's `max_limit`; drives the replay's
    /// limit-sufficiency gate.
    pub max_limit: Option<u64>,
    /// Whole-query constraints of the original (pre-decorrelation) resolved
    /// form — the subsumed-side input; `None` for set-operation queries
    /// (rejected by subsumption outright).
    pub constraints: Option<QueryConstraints>,
    /// One entry per admitted update query (the subsumer side).
    pub admissions: Vec<FitAdmission>,
}

/// The slice of a [`TableAdmission`] the offline registry keeps.
pub struct FitAdmission {
    pub relation_oid: Oid,
    /// The update query's per-branch constraints — what subsumption compares.
    pub constraints: QueryConstraints,
    pub index_constraints: Vec<TableConstraint>,
    pub subsumer_eligible: bool,
}

pub enum Verdict {
    Cacheable(Box<CacheableAnalysis>),
    Passthrough {
        reason: PassthroughReason,
        /// Write classification of a data-modifying CTE inside a failed
        /// SELECT — the statement still writes.
        cte_write: Option<WriteClass>,
    },
    Write(WriteClass),
    Utility,
}

pub fn statement_classify(
    parsed: &ParsedStatement,
    catalog: &BiHashMap<TableMetadata>,
    builtins: &BuiltinFunctions,
) -> Verdict {
    let expr = match &parsed.outcome {
        ParseOutcome::ParseError(_) => {
            return Verdict::Passthrough {
                reason: PassthroughReason::ParseError,
                cte_write: None,
            };
        }
        ParseOutcome::ParameterError(_) => {
            return Verdict::Passthrough {
                reason: PassthroughReason::ParameterSubstitution,
                cte_write: None,
            };
        }
        ParseOutcome::SelectUnconvertible { cte_write, .. } => {
            return Verdict::Passthrough {
                reason: PassthroughReason::ConversionUnsupported,
                cte_write: cte_write.clone(),
            };
        }
        ParseOutcome::Write(write_class) => return Verdict::Write(write_class.clone()),
        ParseOutcome::Utility => return Verdict::Utility,
        ParseOutcome::Select(expr) => expr,
    };

    // Stage A: syntactic cacheability against the builtin volatility map.
    let cacheable = match CacheableQuery::try_new((**expr).clone(), &builtins.volatility) {
        Ok(cacheable) => cacheable,
        Err(error) => {
            return Verdict::Passthrough {
                reason: cacheability_reason(&error),
                cte_write: None,
            };
        }
    };

    // Stage B against the (synthesized) catalog — the writer's own query
    // preparation: LIMIT stripped and max_limit derived (base_query_prepare),
    // predicates pushed into derived-table branches, and reducer shapes
    // forcing unbounded population, exactly as in query_resolve.
    let (base_query, user_max_limit) = base_query_prepare(&cacheable.query);

    let Ok(resolved) =
        query_expr_resolve(&base_query, catalog, &["public"]).map(predicate_pushdown_apply)
    else {
        return Verdict::Passthrough {
            reason: PassthroughReason::ResolutionFailed,
            cte_write: None,
        };
    };
    let shape_gate = shape_gate_classify(&resolved, &builtins.aggregates);
    let max_limit = if shape_gate.is_reducer() {
        None
    } else {
        user_max_limit
    };
    let has_limit = max_limit.is_some();
    let fingerprint = query_expr_fingerprint(&cacheable.query);
    let analysis = match query_admission_analyze(
        &resolved,
        fingerprint,
        has_limit,
        &builtins.aggregates,
        catalog,
    ) {
        Ok(analysis) => analysis,
        Err(_) => {
            return Verdict::Passthrough {
                reason: PassthroughReason::DecorrelationFailed,
                cte_write: None,
            };
        }
    };

    let mut relations: Vec<(Oid, EcoString)> = Vec::new();
    for table in &analysis.tables {
        if !relations.iter().any(|(oid, _)| *oid == table.relation_oid) {
            relations.push((table.relation_oid, table.table_name.clone()));
        }
    }
    let admissions = analysis
        .tables
        .into_iter()
        .map(|table| FitAdmission {
            relation_oid: table.relation_oid,
            constraints: table.update_query.constraints,
            index_constraints: table.index_constraints,
            subsumer_eligible: table.subsumer_eligible,
        })
        .collect();
    // The subsumed side analyzes the original (pre-decorrelation) resolved
    // form, like the writer's subsumption_check.
    let constraints = resolved.as_select().map(analyze_query_constraints);

    Verdict::Cacheable(Box::new(CacheableAnalysis {
        fingerprint,
        shape_key: query_shape_derive(&resolved).key,
        relations,
        has_limit,
        max_limit,
        constraints,
        admissions,
    }))
}

/// Test-only: parse SQL straight to a `QueryExpr` through the same raw path
/// the pipeline uses.
#[cfg(test)]
pub(crate) fn query_parse(sql: &str) -> QueryExpr {
    let raw = pg_query::parse_raw_scoped(sql, |tree| unsafe { statement_convert_raw(tree) })
        .expect("parse SQL")
        .expect("classify statement");
    match raw {
        RawStatement::Select { converted, .. } => *converted.expect("convert SELECT"),
        _ => panic!("expected SELECT statement"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog_synth::catalog_synthesize;
    use crate::volatility::builtin_functions_load;

    fn classify(sql: &str) -> Verdict {
        classify_with_parameters(sql, &[])
    }

    fn classify_with_parameters(sql: &str, parameters: &[Option<&str>]) -> Verdict {
        let trace = TraceStatement {
            sql: sql.to_owned(),
            parameters: parameters.iter().map(|p| p.map(str::to_owned)).collect(),
            calls: 1,
            total_time_ms: None,
        };
        let parsed = statement_parse(trace);
        let corpus: Vec<QueryExpr> = match &parsed.outcome {
            ParseOutcome::Select(expr) => vec![(**expr).clone()],
            _ => vec![],
        };
        let catalog = catalog_synthesize(corpus.iter());
        let builtins = builtin_functions_load();
        statement_classify(&parsed, &catalog.tables, &builtins)
    }

    fn passthrough_reason(verdict: &Verdict) -> Option<PassthroughReason> {
        match verdict {
            Verdict::Passthrough { reason, .. } => Some(*reason),
            _ => None,
        }
    }

    #[test]
    fn test_simple_select_cacheable() {
        assert!(matches!(
            classify("SELECT id, name FROM users WHERE id = 1"),
            Verdict::Cacheable(_)
        ));
    }

    #[test]
    fn test_join_cacheable() {
        assert!(matches!(
            classify("SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id = 1"),
            Verdict::Cacheable(_)
        ));
    }

    #[test]
    fn test_volatile_function_passthrough() {
        let verdict = classify("SELECT * FROM users WHERE id = floor(random() * 100)::int");
        assert!(matches!(
            passthrough_reason(&verdict),
            Some(
                PassthroughReason::NonImmutableFunction | PassthroughReason::ConversionUnsupported
            )
        ));
    }

    #[test]
    fn test_stable_function_passthrough() {
        let verdict = classify("SELECT * FROM events WHERE created_at > now()");
        assert_eq!(
            passthrough_reason(&verdict),
            Some(PassthroughReason::NonImmutableFunction)
        );
    }

    #[test]
    fn test_system_catalog_passthrough() {
        let verdict = classify("SELECT * FROM pg_class");
        assert_eq!(
            passthrough_reason(&verdict),
            Some(PassthroughReason::SystemCatalogReference)
        );
    }

    #[test]
    fn test_full_join_passthrough() {
        let verdict = classify("SELECT * FROM a FULL JOIN b ON a.id = b.id");
        assert!(passthrough_reason(&verdict).is_some());
    }

    #[test]
    fn test_comma_join_passthrough() {
        let verdict = classify("SELECT * FROM a, b WHERE a.id = b.id");
        assert!(passthrough_reason(&verdict).is_some());
    }

    #[test]
    fn test_locking_clause_passthrough() {
        let verdict = classify("SELECT * FROM users WHERE id = 1 FOR UPDATE");
        assert!(passthrough_reason(&verdict).is_some());
    }

    #[test]
    fn test_insert_classified_as_write() {
        let verdict = classify("INSERT INTO users (id, name) VALUES (1, 'a')");
        assert!(matches!(verdict, Verdict::Write(_)));
    }

    #[test]
    fn test_update_classified_as_write() {
        let verdict = classify("UPDATE users SET name = 'b' WHERE id = 1");
        assert!(matches!(verdict, Verdict::Write(WriteClass::Table(_))));
    }

    #[test]
    fn test_txn_control_is_utility() {
        assert!(matches!(classify("BEGIN"), Verdict::Utility));
        assert!(matches!(classify("COMMIT"), Verdict::Utility));
    }

    #[test]
    fn test_garbage_is_parse_error() {
        let verdict = classify("SELEC id FRM users");
        assert_eq!(
            passthrough_reason(&verdict),
            Some(PassthroughReason::ParseError)
        );
    }

    #[test]
    fn test_self_join_excluded_from_subsumers() {
        let Verdict::Cacheable(analysis) =
            classify("SELECT * FROM emp e JOIN emp m ON e.manager_id = m.id WHERE e.id = 1")
        else {
            panic!("expected cacheable");
        };
        assert_eq!(analysis.relations.len(), 1);
        assert!(!analysis.admissions.is_empty());
        assert!(analysis.admissions.iter().all(|a| !a.subsumer_eligible));
    }

    /// Spike (a), locked in as a regression test: a logged text parameter
    /// substituted with an inferred OID must fingerprint identically to the
    /// same query written with an inline literal.
    #[test]
    fn test_inferred_parameter_fingerprints_match_inline_literal() {
        let inline = classify("SELECT * FROM users WHERE id = 42");
        let parameterized =
            classify_with_parameters("SELECT * FROM users WHERE id = $1", &[Some("42")]);
        let (Verdict::Cacheable(a), Verdict::Cacheable(b)) = (inline, parameterized) else {
            panic!("expected both cacheable");
        };
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    #[test]
    fn test_string_parameter_fingerprints_match_inline_literal() {
        let inline = classify("SELECT * FROM users WHERE name = 'ada'");
        let parameterized =
            classify_with_parameters("SELECT * FROM users WHERE name = $1", &[Some("ada")]);
        let (Verdict::Cacheable(a), Verdict::Cacheable(b)) = (inline, parameterized) else {
            panic!("expected both cacheable");
        };
        assert_eq!(a.fingerprint, b.fingerprint);
    }

    /// Spike (b), locked in: a pgss-style `$N` statement flows through Stage
    /// A/B without erroring and with complete WHERE analysis (subsumption on
    /// parameter bounds is simply inconclusive).
    #[test]
    fn test_pgss_placeholder_survives_both_stages() {
        let Verdict::Cacheable(analysis) = classify("SELECT * FROM users WHERE id = $1") else {
            panic!("expected cacheable");
        };
        let constraints = analysis.constraints.as_ref().expect("select constraints");
        assert!(constraints.where_analysis_complete);
    }
}
