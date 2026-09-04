//! Report assembly and rendering: doc-style text output and `--json`.

use std::collections::{HashMap, HashSet};
use std::fmt::Write;

use pgcache_lib::query::write::WriteClass;
use pgcache_lib::query::{Fingerprint, ShapeKey};

use crate::catalog_synth::SynthesisStats;
use crate::classify::{AnalyzedStatement, ParseOutcome, PassthroughReason, Verdict};
use crate::hitrate::{HitrateStats, ReplayConfig};
use crate::input::TraceFormat;

#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct BucketAggregate {
    pub statements: u64,
    pub calls: u64,
    pub time_ms: f64,
}

impl BucketAggregate {
    fn add(&mut self, calls: Option<u64>, time_ms: Option<f64>) {
        self.statements += 1;
        self.calls += calls.unwrap_or(0);
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
    pub statements: u64,
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
    /// Call counts exist only when the source carries them (pg_stat_statements).
    pub calls_available: bool,
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
    pub shape_level_fingerprints: bool,
    pub assumptions: Vec<String>,
    pub verdicts: Vec<StatementVerdict>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct HitrateReport {
    #[serde(flatten)]
    pub stats: HitrateStats,
    pub admission_threshold: u32,
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
    let mut calls_available = false;
    let mut time_ms = 0.0f64;
    let mut time_available = false;
    let mut cacheable = BucketAggregate::default();
    let mut write = BucketAggregate::default();
    let mut utility = BucketAggregate::default();
    let mut passthrough_by_reason: HashMap<PassthroughReason, BucketAggregate> = HashMap::new();
    let mut writes_by_table: HashMap<String, (u64, u64)> = HashMap::new();
    let mut distinct_statements: HashSet<&str> = HashSet::new();
    let mut fingerprints: HashSet<Fingerprint> = HashSet::new();
    let mut shapes: HashSet<ShapeKey> = HashSet::new();
    let mut inferred_parameters = 0usize;
    let mut verdicts = if include_verdicts {
        Vec::with_capacity(items.len())
    } else {
        Vec::new()
    };

    for item in items {
        let statement_calls = item.trace.calls;
        statements += 1;
        if let Some(c) = statement_calls {
            calls_available = true;
            calls += c;
        }
        if let Some(t) = item.trace.total_time_ms {
            time_available = true;
            time_ms += t;
        }
        inferred_parameters += item.parsed.inferred_parameters;
        distinct_statements.insert(item.trace.sql.as_str());

        let (verdict_label, reason_label) = match &*item.verdict {
            Verdict::Cacheable(analysis) => {
                cacheable.add(statement_calls, item.trace.total_time_ms);
                fingerprints.insert(analysis.fingerprint);
                shapes.insert(analysis.shape_key);
                ("cacheable", None)
            }
            Verdict::Passthrough { reason, cte_write } => {
                passthrough_by_reason
                    .entry(*reason)
                    .or_default()
                    .add(statement_calls, item.trace.total_time_ms);
                if let Some(write_class) = cte_write {
                    let entry = writes_by_table
                        .entry(write_class_table(write_class))
                        .or_default();
                    entry.0 += 1;
                    entry.1 += statement_calls.unwrap_or(0);
                }
                ("passthrough", Some(reason.label()))
            }
            Verdict::Write(write_class) => {
                write.add(statement_calls, item.trace.total_time_ms);
                let entry = writes_by_table
                    .entry(write_class_table(write_class))
                    .or_default();
                entry.0 += 1;
                entry.1 += statement_calls.unwrap_or(0);
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
    passthrough
        .sort_by_key(|reason| std::cmp::Reverse((reason.bucket.statements, reason.bucket.calls)));

    let mut writes_sorted: Vec<TableWrites> = writes_by_table
        .into_iter()
        .map(|(table, (statements, calls))| TableWrites {
            table,
            statements,
            calls,
        })
        .collect();
    writes_sorted.sort_by(|a, b| {
        (b.statements, b.calls)
            .cmp(&(a.statements, a.calls))
            .then(a.table.cmp(&b.table))
    });

    CheckReport {
        statements,
        calls_available,
        calls,
        time_available,
        time_ms,
        cacheable,
        passthrough,
        write,
        utility,
        writes_by_table: writes_sorted,
        distinct_statements: distinct_statements.len(),
        distinct_fingerprints: fingerprints.len(),
        distinct_shapes: shapes.len(),
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
    config: ReplayConfig,
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
    if config.admission_threshold > 1 {
        assumptions.push(format!(
            "admission threshold {}: a query is forwarded until its {}th sighting \
             (proxy Pending gate): {} calls",
            config.admission_threshold, config.admission_threshold, stats.pending_forwards
        ));
    }
    HitrateReport {
        admission_threshold: config.admission_threshold,
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

/// Width of a left-aligned label column: the longest label plus a gap, never
/// narrower than the default so short reports keep a stable layout.
fn label_column_width<'a>(labels: impl Iterator<Item = &'a str>) -> usize {
    labels
        .map(|label| label.chars().count() + 2)
        .max()
        .unwrap_or(0)
        .max(32)
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
    let mut header = format!("pgcache-fit check — {} statements", report.statements);
    if report.calls_available {
        let _ = write!(header, " ({} calls", report.calls);
        if report.time_available {
            let _ = write!(header, ", {:.1} ms", report.time_ms);
        }
        header.push(')');
    }
    let _ = writeln!(out, "{header}");
    let _ = writeln!(out);

    // Call/time weighting is appended after the statement view, only when
    // the source carries it.
    let weighted = |bucket: &BucketAggregate| -> String {
        let mut suffix = String::new();
        if report.calls_available {
            let _ = write!(
                suffix,
                "   calls {:>5.1}%",
                percent(bucket.calls, report.calls)
            );
        }
        if report.time_available {
            let _ = write!(
                suffix,
                "  time {:>5.1}%",
                percent_time(bucket.time_ms, report.time_ms)
            );
        }
        suffix
    };

    let bucket_line = |out: &mut String, name: &str, bucket: &BucketAggregate| {
        let _ = writeln!(
            out,
            "{name:<13}{} statements ({:.1}%){}",
            bucket.statements,
            percent(bucket.statements, report.statements),
            weighted(bucket)
        );
    };

    bucket_line(&mut out, "Cacheable:", &report.cacheable);
    if !report.passthrough.is_empty() {
        let passthrough_total =
            report
                .passthrough
                .iter()
                .fold(BucketAggregate::default(), |mut total, reason| {
                    total.statements += reason.bucket.statements;
                    total.calls += reason.bucket.calls;
                    total.time_ms += reason.bucket.time_ms;
                    total
                });
        bucket_line(&mut out, "Passthrough:", &passthrough_total);
        let label_width = label_column_width(report.passthrough.iter().map(|r| r.label));
        for reason in &report.passthrough {
            let _ = writeln!(
                out,
                "  {:<label_width$}{} ({:.1}%){}",
                reason.label,
                reason.bucket.statements,
                percent(reason.bucket.statements, report.statements),
                weighted(&reason.bucket)
            );
        }
    }
    if report.write.statements > 0 {
        bucket_line(&mut out, "Writes:", &report.write);
    }
    if report.utility.statements > 0 {
        bucket_line(&mut out, "Utility:", &report.utility);
    }

    if !report.writes_by_table.is_empty() {
        let _ = writeln!(out);
        let _ = writeln!(out, "Write mix by table:");
        let label_width =
            label_column_width(report.writes_by_table.iter().map(|t| t.table.as_str()));
        for table_writes in &report.writes_by_table {
            let noun = if table_writes.statements == 1 {
                "statement"
            } else {
                "statements"
            };
            let mut line = format!(
                "  {:<label_width$}{} {noun}",
                table_writes.table, table_writes.statements
            );
            if report.calls_available {
                let _ = write!(line, " ({} calls)", table_writes.calls);
            }
            let _ = writeln!(out, "{line}");
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
        "Shapes: {} distinct statements → {} {} → {} shapes",
        report.distinct_statements,
        report.distinct_fingerprints,
        fingerprint_kind,
        report.distinct_shapes
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
        "pgcache-fit hitrate [experimental] — {} statements, {} calls",
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
    if stats.pending_forwards > 0 {
        let _ = writeln!(out, "  pending forwards  {}", stats.pending_forwards);
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
