//! Plain-text rendering of a [`PreflightReport`]. This is the user-facing
//! format: the Docker entrypoint and the try script print it unchanged.

use super::{PreflightReport, Verdict};

fn verdict_label(verdict: Verdict) -> &'static str {
    match verdict {
        Verdict::Pass => "PASS",
        Verdict::Warn => "WARN",
        Verdict::Fail => "FAIL",
    }
}

pub fn report_render(report: &PreflightReport) -> String {
    let mut lines = vec![
        format!("pgcache check: origin {}", report.target),
        String::new(),
    ];

    for result in &report.results {
        lines.push(format!(
            "  {}  {:<22} {}",
            verdict_label(result.verdict),
            result.name,
            result.finding
        ));
        if let Some(remediation) = &result.remediation {
            for (index, line) in remediation.lines().enumerate() {
                let prefix = if index == 0 { "fix: " } else { "     " };
                lines.push(format!("        {prefix}{line}"));
            }
        }
    }

    let passed = report.count(Verdict::Pass);
    let warnings = report.count(Verdict::Warn);
    let failed = report.count(Verdict::Fail);
    lines.push(String::new());
    lines.push(format!(
        "{passed} passed, {warnings} warnings, {failed} failed"
    ));
    lines.push(if failed > 0 {
        "Origin is not ready for pgcache. Fix the failures above and run the check again."
            .to_owned()
    } else if warnings > 0 {
        "Origin is ready for pgcache. The warnings above limit what gets cached.".to_owned()
    } else {
        "Origin is ready for pgcache.".to_owned()
    });
    lines.push(String::new());
    lines.join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::preflight::CheckResult;

    #[test]
    fn test_report_render_lists_remediation_under_failure() {
        let report = PreflightReport {
            target: "app@db:5432/shop".to_owned(),
            results: vec![
                CheckResult::pass("connection", "PostgreSQL 18.1"),
                CheckResult::fail(
                    "wal_level",
                    "wal_level is 'replica'",
                    "ALTER SYSTEM SET wal_level = logical;\nthen restart PostgreSQL",
                ),
                CheckResult::warn("views", "1 view", "query the tables directly"),
            ],
        };
        let text = report_render(&report);
        assert!(text.starts_with("pgcache check: origin app@db:5432/shop\n"));
        assert!(text.contains("  PASS  connection             PostgreSQL 18.1\n"));
        assert!(text.contains("  FAIL  wal_level              wal_level is 'replica'\n"));
        assert!(text.contains("        fix: ALTER SYSTEM SET wal_level = logical;\n"));
        assert!(text.contains("             then restart PostgreSQL\n"));
        assert!(text.contains("1 passed, 1 warnings, 1 failed\n"));
        assert!(text.contains("Origin is not ready for pgcache."));
    }

    #[test]
    fn test_report_render_ready_wording() {
        let mut report = PreflightReport {
            target: "t".to_owned(),
            results: vec![CheckResult::pass("connection", "ok")],
        };
        assert!(report_render(&report).contains("Origin is ready for pgcache.\n"));
        report
            .results
            .push(CheckResult::warn("views", "1 view", "fix"));
        assert!(report_render(&report).contains("The warnings above limit what gets cached."));
    }
}
