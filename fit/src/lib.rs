//! pgcache-fit: offline cacheability analyzer and hit-rate estimator.
//!
//! Answers "would pgcache help my workload?" without deploying anything, by
//! running pgcache's own query-analysis pipeline over a query list or
//! statement trace. This crate is the analysis library; `main.rs` is the CLI
//! over it and the wasm build exposes the same entry points to the browser.

pub mod catalog_synth;
pub mod classify;
pub mod hitrate;
pub mod input;
pub mod report;
pub mod subsume;
pub mod volatility;

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::rc::Rc;

use ecow::EcoString;
use pgcache_lib::query::ast::rope::QueryTree;

use crate::catalog_synth::{SynthCatalog, catalog_synthesize};
use crate::classify::{
    AnalyzedStatement, ParseOutcome, ParsedStatement, statement_classify, statement_parse,
};
use crate::hitrate::ReplayConfig;
use crate::input::{TraceFormat, TraceStatement, statements_read};
use crate::report::{CheckReport, HitrateReport};
use crate::volatility::builtin_functions_load;

/// A parsed and classified trace: every occurrence carries a shared verdict
/// for its distinct `(sql, parameters)` text.
pub struct Analysis {
    pub items: Vec<AnalyzedStatement>,
    pub catalog: SynthCatalog,
    pub format: TraceFormat,
    pub inferred_parameters: usize,
    pub parameter_details_dropped: usize,
}

/// Parse and classify a trace already read into memory. `format` is the
/// caller's decision (see [`input::trace_format_detect`]).
pub fn trace_analyze(content: &str, format: TraceFormat) -> anyhow::Result<Analysis> {
    let trace = statements_read(content, format)?;
    let statements = trace.statements;
    anyhow::ensure!(
        !statements.is_empty(),
        "no statements found (detected format: {format:?})"
    );

    // Raw traces are dominated by byte-identical repeats: parse and classify
    // once per distinct (sql, parameters) pair and share the results per
    // occurrence. `distinct` keeps first-seen order so catalog synthesis and
    // its heuristic counters stay deterministic.
    let mut parse_memo: HashMap<(EcoString, Vec<Option<EcoString>>), Rc<ParsedStatement>> =
        HashMap::new();
    let mut distinct: Vec<Rc<ParsedStatement>> = Vec::new();
    let occurrences: Vec<(TraceStatement, Rc<ParsedStatement>)> = statements
        .into_iter()
        .map(|trace| {
            let key = (trace.sql.clone(), trace.parameters.clone());
            let parsed = match parse_memo.entry(key) {
                Entry::Occupied(entry) => Rc::clone(entry.get()),
                Entry::Vacant(entry) => {
                    let parsed = Rc::new(statement_parse(&trace.sql, &trace.parameters));
                    distinct.push(Rc::clone(&parsed));
                    entry.insert(Rc::clone(&parsed));
                    parsed
                }
            };
            (trace, parsed)
        })
        .collect();

    let corpus: Vec<&QueryTree> = distinct
        .iter()
        .filter_map(|p| match &p.outcome {
            ParseOutcome::Select(expr) => Some(expr),
            _ => None,
        })
        .collect();
    let catalog = catalog_synthesize(corpus);
    let builtins = builtin_functions_load();

    let verdict_memo: HashMap<*const ParsedStatement, Rc<_>> = distinct
        .iter()
        .map(|p| {
            let verdict = Rc::new(statement_classify(p, &catalog.tables, &builtins));
            (Rc::as_ptr(p), verdict)
        })
        .collect();

    let mut inferred_parameters = 0;
    let items: Vec<AnalyzedStatement> = occurrences
        .into_iter()
        .map(|(trace, parsed)| {
            inferred_parameters += parsed.inferred_parameters;
            let verdict = Rc::clone(
                verdict_memo
                    .get(&Rc::as_ptr(&parsed))
                    .expect("verdict memoized for every distinct statement"),
            );
            AnalyzedStatement {
                trace,
                parsed,
                verdict,
            }
        })
        .collect();
    Ok(Analysis {
        items,
        catalog,
        format,
        inferred_parameters,
        parameter_details_dropped: trace.parameter_details_dropped,
    })
}

/// The `check` report: cacheable / passthrough / write / utility verdicts.
pub fn check_run(analysis: &Analysis) -> CheckReport {
    report::check_report_build(
        &analysis.items,
        &analysis.catalog.stats,
        analysis.format,
        analysis.parameter_details_dropped,
    )
}

/// The `hitrate` report: replay the trace under an infinite cache.
///
/// Rejects pg_stat_statements input: its rows are pre-normalized (`$N`), one
/// per shape, so replaying them would count `calls - 1` of every shape as
/// per-literal hits.
pub fn hitrate_run(analysis: &Analysis, config: ReplayConfig) -> anyhow::Result<HitrateReport> {
    anyhow::ensure!(
        analysis.format != TraceFormat::PgssCsv,
        "pg_stat_statements input is pre-normalized ($N): per-literal hit rates \
         cannot be derived from it — use `check` for shape-level analysis"
    );
    let stats = hitrate::hitrate_replay(&analysis.items, config);
    Ok(report::hitrate_report_build(
        stats,
        config,
        &analysis.catalog.stats,
        analysis.inferred_parameters,
        analysis.format,
        analysis.parameter_details_dropped,
    ))
}
