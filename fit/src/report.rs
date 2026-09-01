//! Report assembly and rendering: doc-style text output and `--json`.

use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use pgcache_lib::query::write::WriteClass;
use pgcache_lib::query::{Fingerprint, ShapeKey};

use crate::catalog_synth::SynthesisStats;
use crate::classify::{AnalyzedStatement, ParseOutcome, PassthroughReason, Verdict};
use crate::hitrate::HitrateStats;
use crate::input::TraceFormat;
use crate::subsume::SubsumerRegistry;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BucketAggregate {
    pub statements: u64,
    pub calls: u64,
    pub time_ms: f64,
}

impl BucketAggregate {
    fn add(&mut self, calls: u64, time_ms: Option<f64>) {
        self.statements += 1;
        self.calls += calls;
        self.time_ms += time_ms.unwrap_or(0.0);
    }
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct ReasonAggregate {
    pub reason: PassthroughReason,
    pub label: &'static str,
    #[serde(flatten)]
    pub bucket: BucketAggregate,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct TableWrites {
    pub table: String,
    pub calls: u64,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct StatementVerdict {
    pub sql: ecow::EcoString,
    pub verdict: &'static str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<&'static str>,
    /// Underlying parse/conversion error message, when there is one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub detail: Option<String>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct CheckReport {
    pub statements: u64,
    pub calls: u64,
    pub time_available: bool,
    pub time_ms: f64,
    pub cacheable: BucketAggregate,
    pub passthrough: Vec<ReasonAggregate>,
    pub write: BucketAggregate,
    pub utility: BucketAggregate,
    pub writes_by_table: Vec<TableWrites>,
    pub distinct_statements: usize,
    pub distinct_fingerprints: usize,
    pub distinct_shapes: usize,
    pub post_subsumption_estimate: usize,
    pub shape_level_fingerprints: bool,
    pub assumptions: Vec<String>,
    pub verdicts: Vec<StatementVerdict>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HitrateReport {
    #[serde(flatten)]
    pub stats: HitrateStats,
    pub hit_rate_cacheable: f64,
    pub hit_rate_selects: f64,
    pub hit_rate_all: f64,
    pub assumptions: Vec<String>,
}

pub fn assumptions_build(
    synth: &SynthesisStats,
    inferred_parameters: usize,
    format: TraceFormat,
    parameter_details_dropped: usize,
) -> Vec<String> {
    let mut out = vec![
        "all tables assumed to have a primary key".to_owned(),
        "all relations assumed to be tables (views not detectable without schema)".to_owned(),
        "all unqualified names assumed in schema public".to_owned(),
        "enum/composite types not detectable".to_owned(),
        "function volatility from a builtin PostgreSQL snapshot; unknown \
         (extension/user-defined) functions treated as non-immutable"
            .to_owned(),
    ];
    if parameter_details_dropped > 0 {
        out.push(format!(
            "unparseable 'Parameters:' details (statements analyzed as \
             unparameterized): {parameter_details_dropped}"
        ));
    }
    if synth.heuristic_attributions > 0 {
        out.push(format!(
            "columns attributed heuristically to the first FROM-order table: {}",
            synth.heuristic_attributions
        ));
    }
    if synth.skipped_unqualified > 0 {
        out.push(format!(
            "unqualified column references skipped (derived sources in scope): {}",
            synth.skipped_unqualified
        ));
    }
    if synth.inferred_columns > 0 {
        out.push(format!(
            "column types inferred from literal comparisons: {}",
            synth.inferred_columns
        ));
    }
    if synth.conflicted_columns > 0 {
        out.push(format!(
            "columns with conflicting literal evidence left as text: {}",
            synth.conflicted_columns
        ));
    }
    if inferred_parameters > 0 {
        out.push(format!(
            "logged parameter values with type OIDs inferred from value shape: {inferred_parameters}"
        ));
    }
    if format == TraceFormat::PgssCsv {
        out.push(
            "pg_stat_statements input is pre-normalized ($N): fingerprints are shape-level"
                .to_owned(),
        );
    }
    out
}

fn write_class_table(write_class: &WriteClass) -> String {
    let relation = match write_class {
        WriteClass::InsertRows(insert) => Some(&insert.relation),
        WriteClass::Table(relation) => Some(relation),
        WriteClass::Connection | WriteClass::ConnectionUnstampable => None,
    };
    match relation {
        Some(r) => match &r.schema {
            Some(schema) => format!("{schema}.{}", r.name),
            None => r.name.to_string(),
        },
        None => "(connection scope)".to_owned(),
    }
}

pub fn check_report_build(
    items: &[AnalyzedStatement],
    synth: &SynthesisStats,
    format: TraceFormat,
    parameter_details_dropped: usize,
    include_verdicts: bool,
) -> CheckReport {
    let mut statements = 0u64;
    let mut calls = 0u64;
    let mut time_ms = 0.0f64;
    let mut time_available = false;
    let mut cacheable = BucketAggregate::default();
    let mut write = BucketAggregate::default();
    let mut utility = BucketAggregate::default();
    let mut passthrough_by_reason: HashMap<PassthroughReason, BucketAggregate> = HashMap::new();
    let mut writes_by_table: HashMap<String, u64> = HashMap::new();
    let mut distinct_statements: HashSet<&str> = HashSet::new();
    let mut fingerprints: HashSet<Fingerprint> = HashSet::new();
    let mut shapes: HashSet<ShapeKey> = HashSet::new();
    let mut inferred_parameters = 0usize;
    let mut verdicts = if include_verdicts {
        Vec::with_capacity(items.len())
    } else {
        Vec::new()
    };
    let mut registry = SubsumerRegistry::new();
    let mut subsumed_fingerprints = 0usize;

    for item in items {
        let statement_calls = item.trace.calls.max(1);
        statements += 1;
        calls += statement_calls;
        if let Some(t) = item.trace.total_time_ms {
            time_available = true;
            time_ms += t;
        }
        inferred_parameters += item.parsed.inferred_parameters;
        distinct_statements.insert(item.trace.sql.as_str());

        let (verdict_label, reason_label) = match &*item.verdict {
            Verdict::Cacheable(analysis) => {
                cacheable.add(statement_calls, item.trace.total_time_ms);
                if fingerprints.insert(analysis.fingerprint) {
                    if registry.query_subsumed(analysis) {
                        subsumed_fingerprints += 1;
                    }
                    registry.subsumer_register(analysis);
                }
                shapes.insert(analysis.shape_key);
                ("cacheable", None)
            }
            Verdict::Passthrough { reason, cte_write } => {
                passthrough_by_reason
                    .entry(*reason)
                    .or_default()
                    .add(statement_calls, item.trace.total_time_ms);
                if let Some(write_class) = cte_write {
                    *writes_by_table
                        .entry(write_class_table(write_class))
                        .or_default() += statement_calls;
                }
                ("passthrough", Some(reason.label()))
            }
            Verdict::Write(write_class) => {
                write.add(statement_calls, item.trace.total_time_ms);
                *writes_by_table
                    .entry(write_class_table(write_class))
                    .or_default() += statement_calls;
                ("write", None)
            }
            Verdict::Utility(_) => {
                utility.add(statement_calls, item.trace.total_time_ms);
                ("utility", None)
            }
        };
        if include_verdicts {
            let detail = match &item.parsed.outcome {
                ParseOutcome::ParseError(error) | ParseOutcome::ParameterError(error) => {
                    Some(error.clone())
                }
                ParseOutcome::SelectUnconvertible { error, .. } => Some(error.clone()),
                _ => None,
            };
            verdicts.push(StatementVerdict {
                sql: item.trace.sql.clone(),
                verdict: verdict_label,
                reason: reason_label,
                detail,
            });
        }
    }

    let mut passthrough: Vec<ReasonAggregate> = passthrough_by_reason
        .into_iter()
        .map(|(reason, bucket)| ReasonAggregate {
            reason,
            label: reason.label(),
            bucket,
        })
        .collect();
    passthrough.sort_by_key(|reason| std::cmp::Reverse(reason.bucket.calls));

    let mut writes_sorted: Vec<TableWrites> = writes_by_table
        .into_iter()
        .map(|(table, calls)| TableWrites { table, calls })
        .collect();
    writes_sorted.sort_by(|a, b| b.calls.cmp(&a.calls).then(a.table.cmp(&b.table)));

    let distinct_fingerprints = fingerprints.len();
    CheckReport {
        statements,
        calls,
        time_available,
        time_ms,
        cacheable,
        passthrough,
        write,
        utility,
        writes_by_table: writes_sorted,
        distinct_statements: distinct_statements.len(),
        distinct_fingerprints,
        distinct_shapes: shapes.len(),
        post_subsumption_estimate: distinct_fingerprints - subsumed_fingerprints,
        shape_level_fingerprints: format == TraceFormat::PgssCsv,
        assumptions: assumptions_build(
            synth,
            inferred_parameters,
            format,
            parameter_details_dropped,
        ),
        verdicts,
    }
}

pub fn hitrate_report_build(
    stats: HitrateStats,
    synth: &SynthesisStats,
    inferred_parameters: usize,
    format: TraceFormat,
    parameter_details_dropped: usize,
) -> HitrateReport {
    let mut assumptions = assumptions_build(
        synth,
        inferred_parameters,
        format,
        parameter_details_dropped,
    );
    if stats.in_transaction_calls > 0 {
        assumptions.push(format!(
            "SELECTs inside explicit transactions are forwarded, never served \
             (proxy transaction gate): {} calls",
            stats.in_transaction_calls
        ));
    }
    if stats.limit_bumps > 0 {
        assumptions.push(format!(
            "repeats needing more rows than the cached LIMIT window repopulate \
             (limit bump), not hit: {}",
            stats.limit_bumps
        ));
    }
    HitrateReport {
        hit_rate_cacheable: stats.rate_over_cacheable(),
        hit_rate_selects: stats.rate_over_selects(),
        hit_rate_all: stats.rate_over_all(),
        assumptions,
        stats,
    }
}

// ---------------------------------------------------------------------------
// text rendering
// ---------------------------------------------------------------------------

fn percent(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 / whole as f64 * 100.0
    }
}

fn percent_time(part: f64, whole: f64) -> f64 {
    if whole <= 0.0 {
        0.0
    } else {
        part / whole * 100.0
    }
}

pub fn check_report_render(report: &CheckReport) -> String {
    let mut out = String::new();
    let _ = writeln!(
        out,
        "pgcache-fit check — {} statements, {} calls",
        report.statements, report.calls
    );
    let _ = writeln!(out);

    let bucket_line = |out: &mut String, name: &str, bucket: &BucketAggregate| {
        let mut line = format!(
            "{name:<13}{:>5.1}% of calls ({} statements)",
            percent(bucket.calls, report.calls),
            bucket.statements
        );
        if report.time_available {
            let _ = write!(
                line,
                " / {:.1}% of time",
                percent_time(bucket.time_ms, report.time_ms)
            );
        }
        let _ = writeln!(out, "{line}");
    };

    bucket_line(&mut out, "Cacheable:", &report.cacheable);
    let passthrough_total: u64 = report.passthrough.iter().map(|r| r.bucket.calls).sum();
    if passthrough_total > 0 {
        let _ = writeln!(
            out,
            "Passthrough: {:>5.1}% of calls",
            percent(passthrough_total, report.calls)
        );
        for reason in &report.passthrough {
            let mut line = format!(
                "  {:<32}{:>5.1}% of calls ({})",
                reason.label,
                percent(reason.bucket.calls, report.calls),
                reason.bucket.statements
            );
            if report.time_available {
                let _ = write!(
                    line,
                    " / {:.1}% of time",
                    percent_time(reason.bucket.time_ms, report.time_ms)
                );
            }
            let _ = writeln!(out, "{line}");
        }
    }
    if report.write.calls > 0 {
        bucket_line(&mut out, "Writes:", &report.write);
    }
    if report.utility.calls > 0 {
        bucket_line(&mut out, "Utility:", &report.utility);
    }

    if !report.writes_by_table.is_empty() {
        let _ = writeln!(out);
        let _ = writeln!(out, "Write mix by table:");
        for table_writes in &report.writes_by_table {
            let _ = writeln!(
                out,
                "  {:<32}{} calls",
                table_writes.table, table_writes.calls
            );
        }
    }

    let _ = writeln!(out);
    let fingerprint_kind = if report.shape_level_fingerprints {
        "fingerprints (shape-level)"
    } else {
        "fingerprints"
    };
    let _ = writeln!(
        out,
        "Shapes: {} distinct statements → {} {} → {} shapes → {} after subsumption",
        report.distinct_statements,
        report.distinct_fingerprints,
        fingerprint_kind,
        report.distinct_shapes,
        report.post_subsumption_estimate
    );

    let _ = writeln!(out);
    let _ = writeln!(out, "Assumptions (schema-less mode):");
    for assumption in &report.assumptions {
        let _ = writeln!(out, "  - {assumption}");
    }
    out
}

pub fn hitrate_report_render(report: &HitrateReport) -> String {
    let stats = &report.stats;
    let mut out = String::new();
    let _ = writeln!(
        out,
        "pgcache-fit hitrate — {} statements, {} calls",
        stats.statements, stats.calls
    );
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Writes:        {} calls (invalidation not simulated — future mode)",
        stats.write_calls
    );
    let _ = writeln!(out, "Utility:       {} calls", stats.utility_calls);
    let _ = writeln!(out, "Non-cacheable: {} calls", stats.non_cacheable_calls);
    if stats.in_transaction_calls > 0 {
        let _ = writeln!(
            out,
            "In-transaction:{} calls (forwarded — proxy transaction gate)",
            stats.in_transaction_calls
        );
    }
    let _ = writeln!(out, "Cacheable:     {} calls", stats.cacheable_calls);
    let _ = writeln!(out, "  hits              {}", stats.hits);
    let _ = writeln!(out, "  subsumption hits  {}", stats.subsumption_hits);
    let _ = writeln!(out, "  cold misses       {}", stats.cold_misses);
    if stats.limit_bumps > 0 {
        let _ = writeln!(out, "  limit bumps       {}", stats.limit_bumps);
    }
    let _ = writeln!(out);
    let _ = writeln!(
        out,
        "Hit rate: {:.1}% of cacheable SELECTs / {:.1}% of all SELECTs / {:.1}% of all statements",
        report.hit_rate_cacheable * 100.0,
        report.hit_rate_selects * 100.0,
        report.hit_rate_all * 100.0
    );
    let _ = writeln!(out);
    let _ = writeln!(out, "Assumptions (schema-less mode):");
    for assumption in &report.assumptions {
        let _ = writeln!(out, "  - {assumption}");
    }
    out
}
