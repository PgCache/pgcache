//! Result accumulation: per-statement pass/fail, a JUnit XML report for
//! CI, and an end-of-run failure-bucket summary so a large failing run
//! is diagnosable on one screen.

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use quick_junit::{NonSuccessKind, Report as JunitReport, TestCase, TestCaseStatus, TestSuite};

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Bucket {
    ResultDiff,
    RoutingMismatch,
    SwallowedError,
    CdcTimeout,
}

impl Bucket {
    fn label(self) -> &'static str {
        match self {
            Bucket::ResultDiff => "result diff",
            Bucket::RoutingMismatch => "routing mismatch",
            Bucket::SwallowedError => "swallowed error",
            Bucket::CdcTimeout => "cdc timeout",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Outcome {
    pub suite: String,
    pub statement: String,
    /// `None` = pass; `Some` = failed in that bucket.
    pub bucket: Option<Bucket>,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct Report {
    outcomes: Vec<Outcome>,
}

impl Report {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn record(&mut self, outcome: Outcome) {
        self.outcomes.push(outcome);
    }

    pub fn failed(&self) -> bool {
        self.outcomes.iter().any(|o| o.bucket.is_some())
    }

    pub fn total(&self) -> usize {
        self.outcomes.len()
    }

    pub fn failures(&self) -> usize {
        self.outcomes.iter().filter(|o| o.bucket.is_some()).count()
    }

    pub fn junit_write(&self, path: impl AsRef<Path>) -> Result<()> {
        let mut report = JunitReport::new("pgcache-conformance");
        let mut by_suite: BTreeMap<&str, Vec<&Outcome>> = BTreeMap::new();
        for o in &self.outcomes {
            by_suite.entry(o.suite.as_str()).or_default().push(o);
        }
        for (suite_name, outcomes) in by_suite {
            let mut suite = TestSuite::new(suite_name);
            for (i, o) in outcomes.iter().enumerate() {
                let status = match o.bucket {
                    None => TestCaseStatus::success(),
                    Some(b) => {
                        let mut s = TestCaseStatus::non_success(NonSuccessKind::Failure);
                        s.set_message(format!("[{}] {}", b.label(), o.detail));
                        s
                    }
                };
                let name = format!("{i:04}: {}", truncate(&o.statement, 120));
                suite.add_test_case(TestCase::new(name, status));
            }
            report.add_test_suite(suite);
        }
        let path = path.as_ref();
        let file =
            std::fs::File::create(path).with_context(|| format!("create {}", path.display()))?;
        report
            .serialize(file)
            .with_context(|| format!("write JUnit XML to {}", path.display()))?;
        Ok(())
    }

    /// One-screen failure-bucket summary.
    pub fn summary(&self) -> String {
        let mut counts: BTreeMap<Bucket, usize> = BTreeMap::new();
        for o in &self.outcomes {
            if let Some(b) = o.bucket {
                *counts.entry(b).or_default() += 1;
            }
        }
        let mut out = format!(
            "{} statements, {} passed, {} failed",
            self.total(),
            self.total() - self.failures(),
            self.failures()
        );
        for (b, n) in counts {
            out.push_str(&format!("\n  {:<18} {n}", b.label()));
        }
        for o in self.outcomes.iter().filter(|o| o.bucket.is_some()) {
            out.push_str(&format!(
                "\n  FAIL [{}] {} :: {}",
                o.bucket.map(Bucket::label).unwrap_or(""),
                o.suite,
                truncate(&o.statement, 80)
            ));
        }
        out
    }
}

fn truncate(s: &str, max: usize) -> String {
    let s = s.split_whitespace().collect::<Vec<_>>().join(" ");
    if s.chars().count() <= max {
        s
    } else {
        let mut t: String = s.chars().take(max).collect();
        t.push('…');
        t
    }
}
