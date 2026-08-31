//! pgcache-fit: offline cacheability analyzer and hit-rate estimator.
//!
//! Answers "would pgcache help my workload?" without deploying anything, by
//! running pgcache's own query-analysis pipeline over a query list or
//! statement trace.

mod catalog_synth;
mod classify;
mod hitrate;
mod input;
mod report;
mod subsume;
mod volatility;

use std::path::PathBuf;

use anyhow::Context;
use clap::{Parser, Subcommand};
use pgcache_lib::query::ast::QueryExpr;

use crate::catalog_synth::{SynthCatalog, catalog_synthesize};
use crate::classify::{
    ParseOutcome, ParsedStatement, Verdict, statement_classify, statement_parse,
};
use crate::input::{TraceFormat, statements_read, trace_format_detect};
use crate::volatility::builtin_functions_load;

const OUT_OF_SCOPE: &str = "\
Runs pgcache's query-analysis pipeline offline, in schema-less mode: the \
catalog is synthesized from the query corpus itself and every report carries \
an explicit assumptions block.

Not simulated in v0 (planned extensions): write-driven invalidation, schema \
dump input (--schema), live-database catalogs, time-windowed hit rates.";

#[derive(Parser)]
#[command(name = "pgcache-fit", version, about, long_about = OUT_OF_SCOPE)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Classify statements as cacheable vs passthrough, with reasons
    Check {
        /// Query list or trace: .sql file, postgres log (csvlog/stderr), or
        /// pg_stat_statements CSV
        input: PathBuf,
        /// Emit the report (and per-statement verdicts) as JSON
        #[arg(long)]
        json: bool,
        /// Override input format auto-detection
        #[arg(long, value_enum)]
        format: Option<TraceFormat>,
    },
    /// Replay a trace and estimate the max hit rate under an infinite cache
    Hitrate {
        /// Statement trace: postgres log (csvlog/stderr) or .sql file
        input: PathBuf,
        /// Emit the report as JSON
        #[arg(long)]
        json: bool,
        /// Override input format auto-detection
        #[arg(long, value_enum)]
        format: Option<TraceFormat>,
    },
}

struct Analysis {
    items: Vec<(ParsedStatement, Verdict)>,
    catalog: SynthCatalog,
    format: TraceFormat,
    inferred_parameters: usize,
    parameter_details_dropped: usize,
}

fn trace_analyze(path: &PathBuf, format_override: Option<TraceFormat>) -> anyhow::Result<Analysis> {
    let content =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let format = format_override.unwrap_or_else(|| trace_format_detect(path, &content));
    let trace = statements_read(&content, format)?;
    let statements = trace.statements;
    anyhow::ensure!(
        !statements.is_empty(),
        "no statements found in {} (detected format: {format:?}; override with --format)",
        path.display()
    );

    let parsed: Vec<ParsedStatement> = statements.into_iter().map(statement_parse).collect();
    let corpus: Vec<&QueryExpr> = parsed
        .iter()
        .filter_map(|p| match &p.outcome {
            ParseOutcome::Select(expr) => Some(&**expr),
            _ => None,
        })
        .collect();
    let catalog = catalog_synthesize(corpus);
    let builtins = builtin_functions_load();
    let inferred_parameters = parsed.iter().map(|p| p.inferred_parameters).sum();
    let items = parsed
        .into_iter()
        .map(|p| {
            let verdict = statement_classify(&p, &catalog.tables, &builtins);
            (p, verdict)
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

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Command::Check {
            input,
            json,
            format,
        } => {
            let analysis = trace_analyze(&input, format)?;
            let report = report::check_report_build(
                &analysis.items,
                &analysis.catalog.stats,
                analysis.format,
                analysis.parameter_details_dropped,
            );
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report::check_report_render(&report));
            }
        }
        Command::Hitrate {
            input,
            json,
            format,
        } => {
            let analysis = trace_analyze(&input, format)?;
            // pgss rows are pre-normalized ($N), one row per shape; replaying
            // them would count calls-1 of every shape as per-literal hits.
            anyhow::ensure!(
                analysis.format != TraceFormat::PgssCsv,
                "pg_stat_statements input is pre-normalized ($N): per-literal hit rates \
                 cannot be derived from it — use `check` for shape-level analysis"
            );
            let stats = hitrate::hitrate_replay(&analysis.items);
            let report = report::hitrate_report_build(
                stats,
                &analysis.catalog.stats,
                analysis.inferred_parameters,
                analysis.format,
                analysis.parameter_details_dropped,
            );
            if json {
                println!("{}", serde_json::to_string_pretty(&report)?);
            } else {
                print!("{}", report::hitrate_report_render(&report));
            }
        }
    }
    Ok(())
}
