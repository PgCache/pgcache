//! Statement sources: plain SQL files, postgres logs (csvlog + best-effort
//! stderr), and pg_stat_statements CSV dumps.

use anyhow::Context;
use bytes::Bytes;
use clap::ValueEnum;
use pgcache_lib::cache::QueryParameters;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum TraceFormat {
    /// Semicolon-separated SQL statements
    Sql,
    /// PostgreSQL csvlog output
    CsvLog,
    /// PostgreSQL stderr log output (best-effort)
    StderrLog,
    /// pg_stat_statements dump as CSV with a header row
    PgssCsv,
}

/// One traced statement: the SQL text, any logged bind-parameter values
/// (text form; `None` = NULL), and pg_stat_statements weights when present.
#[derive(Debug, Clone)]
pub struct TraceStatement {
    pub sql: String,
    pub parameters: Vec<Option<String>>,
    pub calls: u64,
    pub total_time_ms: Option<f64>,
    /// Backend identity from the log line prefix (pid); 0 when the source
    /// carries none (SQL files, pgss). Scopes the replay's transaction gate.
    pub session: u64,
}

impl TraceStatement {
    fn from_sql(sql: impl Into<String>) -> Self {
        TraceStatement {
            sql: sql.into(),
            parameters: Vec::new(),
            calls: 1,
            total_time_ms: None,
            session: 0,
        }
    }
}

pub fn trace_format_detect(path: &std::path::Path, content: &str) -> TraceFormat {
    let extension = path.extension().and_then(|e| e.to_str()).unwrap_or("");
    if extension.eq_ignore_ascii_case("sql") {
        return TraceFormat::Sql;
    }
    let first_line = content.lines().find(|l| !l.trim().is_empty()).unwrap_or("");
    if pgss_header_is(first_line) {
        return TraceFormat::PgssCsv;
    }
    if csvlog_line_is(first_line) {
        return TraceFormat::CsvLog;
    }
    if content.contains("LOG:") {
        return TraceFormat::StderrLog;
    }
    TraceFormat::Sql
}

fn pgss_header_is(line: &str) -> bool {
    let fields: Vec<String> = line
        .split(',')
        .map(|f| f.trim().trim_matches('"').to_ascii_lowercase())
        .collect();
    let has = |name: &str| fields.iter().any(|f| f == name);
    has("query") && (has("calls") || has("total_exec_time") || has("total_time"))
}

fn csvlog_line_is(line: &str) -> bool {
    let dated = line.len() > 10
        && line.as_bytes().first().is_some_and(u8::is_ascii_digit)
        && line.as_bytes().get(4) == Some(&b'-')
        && line.as_bytes().get(7) == Some(&b'-');
    dated && line.matches(',').count() >= 15
}

pub fn statements_read(content: &str, format: TraceFormat) -> anyhow::Result<Vec<TraceStatement>> {
    match format {
        TraceFormat::Sql => sql_read(content),
        TraceFormat::CsvLog => csvlog_read(content),
        TraceFormat::StderrLog => Ok(stderr_log_read(content)),
        TraceFormat::PgssCsv => pgss_read(content),
    }
}

fn sql_read(content: &str) -> anyhow::Result<Vec<TraceStatement>> {
    let statements = pg_query::split_with_scanner(content).context("splitting SQL input")?;
    Ok(statements
        .into_iter()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(TraceStatement::from_sql)
        .collect())
}

// ---------------------------------------------------------------------------
// postgres logs
// ---------------------------------------------------------------------------

/// csvlog fixed field positions (stable across supported versions; later
/// versions only append columns).
const CSVLOG_PID: usize = 3;
const CSVLOG_SEVERITY: usize = 11;
const CSVLOG_MESSAGE: usize = 13;
const CSVLOG_DETAIL: usize = 14;

fn csvlog_read(content: &str) -> anyhow::Result<Vec<TraceStatement>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .flexible(true)
        .from_reader(content.as_bytes());
    let mut out = Vec::new();
    for record in reader.records() {
        let record = record.context("reading csvlog record")?;
        if record.get(CSVLOG_SEVERITY) != Some("LOG") {
            continue;
        }
        let Some(message) = record.get(CSVLOG_MESSAGE) else {
            continue;
        };
        let Some((sql, duration_ms)) = log_message_statement(message) else {
            continue;
        };
        let parameters = record
            .get(CSVLOG_DETAIL)
            .and_then(detail_parameters)
            .unwrap_or_default();
        let session = record
            .get(CSVLOG_PID)
            .and_then(|pid| pid.trim().parse::<u64>().ok())
            .unwrap_or(0);
        out.push(TraceStatement {
            sql: sql.to_owned(),
            parameters,
            calls: 1,
            total_time_ms: duration_ms,
            session,
        });
    }
    Ok(out)
}

/// Extract the SQL payload (and duration, when the message came from
/// `log_min_duration_statement`) out of a log message. Returns `None` for
/// non-statement messages, including bare `duration:` lines.
fn log_message_statement(message: &str) -> Option<(&str, Option<f64>)> {
    let mut duration_ms = None;
    let mut rest = message;
    if let Some(after) = rest.strip_prefix("duration: ") {
        let ms_end = after.find(" ms")?;
        duration_ms = after.get(..ms_end).and_then(|d| d.parse::<f64>().ok());
        rest = after.get(ms_end + 3..)?.trim_start();
    }
    if let Some(sql) = rest.strip_prefix("statement: ") {
        return Some((sql, duration_ms));
    }
    if let Some(after) = rest.strip_prefix("execute ") {
        let sql_start = after.find(": ")?;
        return Some((after.get(sql_start + 2..)?, duration_ms));
    }
    None
}

/// Parse a `Parameters: $1 = 'x', $2 = NULL` detail payload. Values are
/// single-quoted with `''` escapes; parameter indexes are 1-based.
fn detail_parameters(detail: &str) -> Option<Vec<Option<String>>> {
    let list = detail.trim_start().strip_prefix("Parameters: ")?;
    let mut values: Vec<(usize, Option<String>)> = Vec::new();
    let bytes = list.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] != b'$' {
            i += 1;
            continue;
        }
        i += 1;
        let index_start = i;
        while i < bytes.len() && bytes[i].is_ascii_digit() {
            i += 1;
        }
        let Ok(index) = list.get(index_start..i)?.parse::<usize>() else {
            continue;
        };
        let Some(after_eq) = list.get(i..).and_then(|s| s.strip_prefix(" = ")) else {
            continue;
        };
        i = list.len() - after_eq.len();
        if after_eq.starts_with("NULL") {
            values.push((index, None));
            i += 4;
        } else if after_eq.starts_with('\'') {
            let (value, consumed) = quoted_value_parse(after_eq)?;
            values.push((index, Some(value)));
            i += consumed;
        }
    }
    let max_index = values.iter().map(|(i, _)| *i).max().unwrap_or(0);
    let mut out = vec![None; max_index];
    for (index, value) in values {
        if let Some(slot) = out.get_mut(index.checked_sub(1)?) {
            *slot = value;
        }
    }
    Some(out)
}

/// Parse a leading `'...'` value with `''` escapes; returns the unescaped
/// value and the byte length consumed including quotes.
fn quoted_value_parse(s: &str) -> Option<(String, usize)> {
    let inner = s.strip_prefix('\'')?;
    let mut value = String::new();
    let mut i = 0;
    let bytes = inner.as_bytes();
    while i < bytes.len() {
        if bytes[i] == b'\'' {
            if bytes.get(i + 1) == Some(&b'\'') {
                value.push('\'');
                i += 2;
                continue;
            }
            return Some((value, i + 2));
        }
        // Multi-byte UTF-8 sequences pass through byte-wise.
        let ch_len = inner.get(i..).and_then(|s| s.chars().next())?.len_utf8();
        value.push_str(inner.get(i..i + ch_len)?);
        i += ch_len;
    }
    None
}

/// Best-effort stderr-format log reader: `LOG:  statement:` / `LOG:  execute`
/// lines with optional `DETAIL:  Parameters:` lines, grouped by the `[pid]`
/// from the line prefix when present. Continuation lines (leading tab) attach
/// to the most recently started message.
fn stderr_log_read(content: &str) -> Vec<TraceStatement> {
    #[derive(Default)]
    struct Pending {
        sql: String,
        duration_ms: Option<f64>,
        detail: Option<String>,
        session: u64,
    }

    enum LastMessage {
        Statement(u64),
        Detail(u64),
    }

    let mut pending: std::collections::HashMap<u64, Pending> = std::collections::HashMap::new();
    let mut out = Vec::new();
    let mut last_message: Option<LastMessage> = None;

    let flush = |p: Pending, out: &mut Vec<TraceStatement>| {
        let parameters = p.detail.as_deref().and_then(detail_parameters);
        out.push(TraceStatement {
            sql: p.sql,
            parameters: parameters.unwrap_or_default(),
            calls: 1,
            total_time_ms: p.duration_ms,
            session: p.session,
        });
    };

    for line in content.lines() {
        if line.starts_with('\t') || line.starts_with("        ") {
            let continuation = line.trim_start_matches('\t');
            match &last_message {
                Some(LastMessage::Statement(pid)) => {
                    if let Some(p) = pending.get_mut(pid) {
                        p.sql.push('\n');
                        p.sql.push_str(continuation);
                    }
                }
                Some(LastMessage::Detail(pid)) => {
                    if let Some(p) = pending.get_mut(pid)
                        && let Some(d) = &mut p.detail
                    {
                        d.push(' ');
                        d.push_str(continuation.trim_start());
                    }
                }
                None => {}
            }
            continue;
        }
        let Some((pid, severity, rest)) = log_line_split(line) else {
            continue;
        };
        match severity {
            "LOG" => {
                if let Some((sql, duration_ms)) = log_message_statement(rest) {
                    if let Some(previous) = pending.remove(&pid) {
                        flush(previous, &mut out);
                    }
                    pending.insert(
                        pid,
                        Pending {
                            sql: sql.to_owned(),
                            duration_ms,
                            detail: None,
                            session: pid,
                        },
                    );
                    last_message = Some(LastMessage::Statement(pid));
                } else {
                    // Any other LOG message for this backend ends the statement.
                    if let Some(previous) = pending.remove(&pid) {
                        flush(previous, &mut out);
                    }
                    last_message = None;
                }
            }
            "DETAIL" => {
                if rest.trim_start().starts_with("Parameters: ")
                    && let Some(p) = pending.get_mut(&pid)
                {
                    p.detail = Some(rest.trim_start().to_owned());
                    last_message = Some(LastMessage::Detail(pid));
                }
            }
            _ => {
                if let Some(previous) = pending.remove(&pid) {
                    flush(previous, &mut out);
                }
                last_message = None;
            }
        }
    }
    // Flush remaining in stable order for deterministic output.
    let mut rest: Vec<(u64, Pending)> = pending.into_iter().collect();
    rest.sort_by_key(|(pid, _)| *pid);
    for (_, p) in rest {
        flush(p, &mut out);
    }
    out
}

const SEVERITIES: &[&str] = &[
    "LOG",
    "DETAIL",
    "STATEMENT",
    "ERROR",
    "FATAL",
    "PANIC",
    "WARNING",
    "NOTICE",
    "HINT",
    "INFO",
];

/// Split a stderr log line into `([pid], SEVERITY, payload)`. The pid comes
/// from the last `[digits]` group before the severity tag; 0 when absent.
fn log_line_split(line: &str) -> Option<(u64, &str, &str)> {
    let (position, severity) = SEVERITIES
        .iter()
        .filter_map(|s| {
            let tag = format!("{s}:  ");
            line.find(&tag).map(|p| (p, *s))
        })
        .min_by_key(|(p, _)| *p)?;
    let rest = line.get(position + severity.len() + 3..)?;
    let prefix = line.get(..position)?;
    let pid = prefix
        .rfind('[')
        .and_then(|open| {
            let after = prefix.get(open + 1..)?;
            let close = after.find(']')?;
            after.get(..close)?.parse::<u64>().ok()
        })
        .unwrap_or(0);
    Some((pid, severity, rest))
}

// ---------------------------------------------------------------------------
// pg_stat_statements
// ---------------------------------------------------------------------------

fn pgss_read(content: &str) -> anyhow::Result<Vec<TraceStatement>> {
    let mut reader = csv::ReaderBuilder::new()
        .has_headers(true)
        .flexible(true)
        .from_reader(content.as_bytes());
    let headers = reader.headers().context("reading pgss CSV header")?;
    let column = |name: &str| {
        headers
            .iter()
            .position(|h| h.trim().eq_ignore_ascii_case(name))
    };
    let query_column = column("query").context("pgss CSV has no 'query' column")?;
    let calls_column = column("calls");
    let time_column = column("total_exec_time").or_else(|| column("total_time"));

    let mut out = Vec::new();
    for record in reader.records() {
        let record = record.context("reading pgss CSV record")?;
        let Some(sql) = record.get(query_column) else {
            continue;
        };
        let calls = calls_column
            .and_then(|c| record.get(c))
            .and_then(|v| v.trim().parse::<f64>().ok())
            .map_or(1, |v| v.max(1.0) as u64);
        let total_time_ms = time_column
            .and_then(|c| record.get(c))
            .and_then(|v| v.trim().parse::<f64>().ok());
        out.push(TraceStatement {
            sql: sql.to_owned(),
            parameters: Vec::new(),
            calls,
            total_time_ms,
            session: 0,
        });
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// parameter inference
// ---------------------------------------------------------------------------

/// Build `QueryParameters` from logged text values. Logs carry no type OIDs,
/// so the OID is inferred from the value's shape: integers as int8, floats as
/// float8, everything else unknown (substituted as a string literal). Without
/// this, `execute` traffic would never fingerprint-match the same query
/// written with inline literals. Returns the count of numeric inferences for
/// the assumptions block.
pub fn query_parameters_infer(values: &[Option<String>]) -> (QueryParameters, usize) {
    const OID_INT8: u32 = 20;
    const OID_FLOAT8: u32 = 701;

    let mut inferred = 0;
    let mut bytes_values = Vec::with_capacity(values.len());
    let mut oids = Vec::with_capacity(values.len());
    for value in values {
        match value {
            None => {
                bytes_values.push(None);
                oids.push(0);
            }
            Some(text) => {
                let oid = if text.parse::<i64>().is_ok() {
                    inferred += 1;
                    OID_INT8
                } else if text.parse::<f64>().is_ok() {
                    inferred += 1;
                    OID_FLOAT8
                } else {
                    0
                };
                bytes_values.push(Some(Bytes::copy_from_slice(text.as_bytes())));
                oids.push(oid);
            }
        }
    }
    let formats = vec![0; values.len()];
    (
        QueryParameters {
            values: bytes_values,
            formats,
            oids,
        },
        inferred,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sql_file_split() {
        let statements =
            sql_read("SELECT 1;\nSELECT * FROM users WHERE id = 2;\n").expect("split sql");
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].sql, "SELECT 1");
    }

    #[test]
    fn test_detail_parameters_basic() {
        let params =
            detail_parameters("Parameters: $1 = '42', $2 = NULL, $3 = 'it''s'").expect("parse");
        assert_eq!(
            params,
            vec![Some("42".to_owned()), None, Some("it's".to_owned())]
        );
    }

    #[test]
    fn test_log_message_statement_forms() {
        assert_eq!(
            log_message_statement("statement: SELECT 1"),
            Some(("SELECT 1", None))
        );
        assert_eq!(
            log_message_statement("execute <unnamed>: SELECT 1"),
            Some(("SELECT 1", None))
        );
        assert_eq!(
            log_message_statement("duration: 1.234 ms  statement: SELECT 1"),
            Some(("SELECT 1", Some(1.234)))
        );
        assert_eq!(log_message_statement("duration: 1.234 ms"), None);
        assert_eq!(log_message_statement("connection received: host=x"), None);
    }

    #[test]
    fn test_stderr_log_execute_with_parameters() {
        let log = "\
2026-08-28 10:00:00.000 PDT [123] LOG:  execute stmt_1: SELECT * FROM users WHERE id = $1
2026-08-28 10:00:00.001 PDT [123] DETAIL:  Parameters: $1 = '42'
2026-08-28 10:00:00.002 PDT [456] LOG:  statement: SELECT 1
2026-08-28 10:00:00.003 PDT [123] LOG:  execute stmt_1: SELECT * FROM users WHERE id = $1
2026-08-28 10:00:00.004 PDT [123] DETAIL:  Parameters: $1 = '43'
";
        let statements = stderr_log_read(log);
        assert_eq!(statements.len(), 3);
        assert_eq!(statements[0].parameters, vec![Some("42".to_owned())]);
        assert!(statements.iter().any(|s| s.sql == "SELECT 1"));
        assert_eq!(statements[1].parameters, vec![Some("43".to_owned())]);
    }

    #[test]
    fn test_stderr_log_multiline_statement() {
        let log = "\
2026-08-28 10:00:00.000 PDT [123] LOG:  statement: SELECT *
\tFROM users
\tWHERE id = 1
2026-08-28 10:00:00.001 PDT [123] LOG:  disconnection: session time
";
        let statements = stderr_log_read(log);
        assert_eq!(statements.len(), 1);
        assert_eq!(statements[0].sql, "SELECT *\nFROM users\nWHERE id = 1");
    }

    #[test]
    fn test_pgss_csv() {
        let csv = "\
query,calls,total_exec_time
\"SELECT * FROM users WHERE id = $1\",100,12.5
\"UPDATE users SET name = $1 WHERE id = $2\",7,3.25
";
        let statements = pgss_read(csv).expect("parse pgss csv");
        assert_eq!(statements.len(), 2);
        assert_eq!(statements[0].calls, 100);
        assert_eq!(statements[0].total_time_ms, Some(12.5));
    }

    #[test]
    fn test_format_detect() {
        assert_eq!(
            trace_format_detect(std::path::Path::new("queries.sql"), "SELECT 1;"),
            TraceFormat::Sql
        );
        assert_eq!(
            trace_format_detect(
                std::path::Path::new("pgss.csv"),
                "query,calls,total_exec_time\n\"SELECT 1\",1,0.1\n"
            ),
            TraceFormat::PgssCsv
        );
        assert_eq!(
            trace_format_detect(
                std::path::Path::new("trace.log"),
                "2026-08-28 10:00:00.000 PDT [123] LOG:  statement: SELECT 1\n"
            ),
            TraceFormat::StderrLog
        );
    }

    #[test]
    fn test_parameter_inference_oids() {
        let (params, inferred) = query_parameters_infer(&[
            Some("42".to_owned()),
            Some("3.14".to_owned()),
            Some("hello".to_owned()),
            None,
        ]);
        assert_eq!(params.oids, vec![20, 701, 0, 0]);
        assert_eq!(inferred, 2);
        assert_eq!(params.values[3], None);
    }
}
