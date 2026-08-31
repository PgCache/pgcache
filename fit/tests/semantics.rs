//! CLI-level integration tests for pgcache-fit, split into two groups:
//!
//! * [`stable`] — semantics that must survive the PGC-391 admission-analysis
//!   extraction and the hitrate fidelity fixes unchanged. These pass today
//!   and must keep passing.
//! * [`target`] — writer-faithful semantics the current implementation gets
//!   wrong (the confirmed review drift bugs). Each test asserts the TARGET
//!   behavior, so it fails against the current implementation and flips to
//!   green when the shared admission/serve logic lands (PGC-391, and the
//!   fit-local hitrate gates). A target test going green is the signal the
//!   corresponding drift bug is fixed; do not weaken these to match current
//!   output.
//!
//! Tests drive the built binary over fixture traces and assert on `--json`
//! output, so they are independent of the crate's internal structure.

use std::path::PathBuf;
use std::process::Command;

fn fixture_write(name: &str, content: &str) -> PathBuf {
    let dir = std::env::temp_dir().join("pgcache-fit-semantics");
    std::fs::create_dir_all(&dir).expect("create fixture dir");
    let path = dir.join(name);
    std::fs::write(&path, content).expect("write fixture");
    path
}

struct FitOutput {
    success: bool,
    json: Option<serde_json::Value>,
}

fn fit_run(mode: &str, fixture: &PathBuf) -> FitOutput {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache-fit"))
        .args([mode, "--json"])
        .arg(fixture)
        .output()
        .expect("run pgcache-fit");
    let json = output
        .status
        .success()
        .then(|| serde_json::from_slice(&output.stdout).expect("parse JSON report"));
    FitOutput {
        success: output.status.success(),
        json,
    }
}

fn hitrate_json(name: &str, trace: &str) -> serde_json::Value {
    let fixture = fixture_write(name, trace);
    let output = fit_run("hitrate", &fixture);
    assert!(output.success, "hitrate run succeeds");
    output.json.expect("hitrate JSON present")
}

fn check_json(name: &str, trace: &str) -> serde_json::Value {
    let fixture = fixture_write(name, trace);
    let output = fit_run("check", &fixture);
    assert!(output.success, "check run succeeds");
    output.json.expect("check JSON present")
}

fn count(report: &serde_json::Value, field: &str) -> u64 {
    report[field]
        .as_u64()
        .unwrap_or_else(|| panic!("field {field} present in report"))
}

mod stable {
    use super::*;

    #[test]
    fn test_repeated_literal_is_hit() {
        let report = hitrate_json(
            "stable_repeat.sql",
            "SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 1;\n",
        );
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "hits"), 2);
        assert_eq!(count(&report, "subsumption_hits"), 0);
    }

    #[test]
    fn test_fullscan_parent_subsumes_distinct_literals() {
        let report = hitrate_json(
            "stable_subsume.sql",
            "SELECT * FROM users;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 2;\n\
             SELECT * FROM users WHERE id = 3;\n",
        );
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "subsumption_hits"), 3);
    }

    #[test]
    fn test_limit_parent_never_subsumes() {
        let report = hitrate_json(
            "stable_limit_parent.sql",
            "SELECT * FROM users LIMIT 10;\n\
             SELECT * FROM users WHERE id = 5;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    #[test]
    fn test_or_where_parent_never_subsumes() {
        // OR breaks constraint extraction (where_analysis_complete = false),
        // so the parent must not be admitted as a subsumer (PGC-106).
        let report = hitrate_json(
            "stable_or_parent.sql",
            "SELECT * FROM users WHERE id = 1 OR name = 'a';\n\
             SELECT * FROM users WHERE id = 1;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
    }

    #[test]
    fn test_same_table_self_join_parent_never_subsumes() {
        // PGC-256: a self-joined query's name-collapsed constraints only hold
        // for one join arm, so it is excluded from subsumers.
        let report = hitrate_json(
            "stable_self_join.sql",
            "SELECT * FROM emp e JOIN emp m ON e.manager_id = m.id;\n\
             SELECT * FROM emp WHERE id = 1;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    #[test]
    fn test_join_covered_by_two_single_table_parents() {
        // Writer-faithful: every referenced relation covered by a registered
        // single-table parent (cf. subsumption_test in the main crate).
        let report = hitrate_json(
            "stable_join_covered.sql",
            "SELECT * FROM users;\n\
             SELECT * FROM orders;\n\
             SELECT * FROM users u JOIN orders o ON u.id = o.user_id WHERE u.id = 1;\n",
        );
        assert_eq!(count(&report, "cold_misses"), 2);
        assert_eq!(count(&report, "subsumption_hits"), 1);
    }

    #[test]
    fn test_shrinking_limit_repeat_is_hit() {
        // Fingerprints exclude LIMIT; a repeat asking for LESS than the
        // cached window is a hit both today and under max_limit tracking.
        let report = hitrate_json(
            "stable_limit_shrink.sql",
            "SELECT * FROM products ORDER BY price LIMIT 100;\n\
             SELECT * FROM products ORDER BY price LIMIT 10;\n",
        );
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "hits"), 1);
    }

    #[test]
    fn test_execute_parameters_match_inline_literals() {
        let report = hitrate_json(
            "stable_params.log",
            "2026-08-28 10:00:00.000 PDT [55] LOG:  execute s1: SELECT * FROM users WHERE id = $1\n\
             2026-08-28 10:00:00.001 PDT [55] DETAIL:  Parameters: $1 = '1'\n\
             2026-08-28 10:00:00.002 PDT [55] LOG:  statement: SELECT * FROM users WHERE id = 1\n",
        );
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "hits"), 1);
    }

    #[test]
    fn test_statement_classification_verdicts() {
        let report = check_json(
            "stable_verdicts.sql",
            "SELECT * FROM users WHERE id = 1;\n\
             INSERT INTO users (id, name) VALUES (1, 'a');\n\
             BEGIN;\n\
             SELECT * FROM events WHERE created_at > now();\n\
             SELECT * FROM pg_class;\n",
        );
        let verdicts = report["verdicts"].as_array().expect("verdicts array");
        let verdict = |i: usize| verdicts[i]["verdict"].as_str().expect("verdict string");
        let reason = |i: usize| verdicts[i]["reason"].as_str().expect("reason string");
        assert_eq!(verdict(0), "cacheable");
        assert_eq!(verdict(1), "write");
        assert_eq!(verdict(2), "utility");
        assert_eq!(verdict(3), "passthrough");
        assert_eq!(reason(3), "non-immutable function");
        assert_eq!(verdict(4), "passthrough");
        assert_eq!(reason(4), "system catalog reference");
    }

    #[test]
    fn test_check_shape_funnel_with_subsumption_estimate() {
        let report = check_json(
            "stable_funnel.sql",
            "SELECT * FROM users;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 2;\n",
        );
        assert_eq!(count(&report, "distinct_fingerprints"), 3);
        // The two per-literal fingerprints are subsumed by the full scan.
        assert_eq!(count(&report, "post_subsumption_estimate"), 1);
    }
}

mod target {
    use super::*;

    /// A derived-table query's branch predicate must constrain what it
    /// caches: `FROM (SELECT ... WHERE id = 5)` holds only id=5 rows and
    /// must not subsume other literals. The current whole-query constraint
    /// analysis sees an empty WHERE and registers it as a full-scan
    /// subsumer; the writer's per-table update-query derivation (shared via
    /// the PGC-391 admission extraction) carries the id=5 constraint.
    #[test]
    fn test_derived_table_query_does_not_subsume_base_table() {
        let report = hitrate_json(
            "target_derived.sql",
            "SELECT * FROM (SELECT * FROM users WHERE id = 5) s;\n\
             SELECT * FROM users WHERE id = 7;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    /// Same-named tables in different schemas are different relations; a
    /// cached full scan of one must not subsume queries against the other.
    /// The current registry keys by bare table name; the writer (and the
    /// PGC-391 shared decision) keys per relation oid.
    #[test]
    fn test_cross_schema_same_name_not_subsumed() {
        let report = hitrate_json(
            "target_cross_schema.sql",
            "SELECT * FROM tenant_a.users;\n\
             SELECT * FROM tenant_b.users WHERE id = 5;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    /// A join over same-named tables in two schemas references two
    /// relations; covering one of them is not coverage. Name-level dedup
    /// currently collapses both to one relation and calls the join subsumed.
    #[test]
    fn test_cross_schema_join_requires_both_relations_covered() {
        let report = hitrate_json(
            "target_cross_schema_join.sql",
            "SELECT * FROM tenant_a.users;\n\
             SELECT * FROM tenant_a.users au JOIN tenant_b.users bu ON au.id = bu.ref_id;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    /// The proxy never serves from cache inside an explicit transaction
    /// (relay gates on in_transaction), so a trace whose SELECTs all run
    /// between BEGIN and COMMIT has no cache hits. The current replay keeps
    /// no transaction state and counts the repeats as hits.
    #[test]
    fn test_in_transaction_selects_not_counted_as_hits() {
        let report = hitrate_json(
            "target_txn.sql",
            "BEGIN;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 1;\n\
             COMMIT;\n",
        );
        assert_eq!(
            count(&report, "hits") + count(&report, "subsumption_hits"),
            0
        );
    }

    /// A repeat asking for MORE rows than the cached window is not a hit:
    /// the proxy records a miss and repopulates via a limit bump. The
    /// current replay treats fingerprint membership as sufficient.
    #[test]
    fn test_growing_limit_not_a_hit() {
        let report = hitrate_json(
            "target_limit_grow.sql",
            "SELECT * FROM products ORDER BY price LIMIT 10;\n\
             SELECT * FROM products ORDER BY price LIMIT 100;\n",
        );
        assert_eq!(count(&report, "hits"), 0);
    }

    /// pg_stat_statements rows are pre-normalized ($N), one row per shape;
    /// counting calls-1 of a shape as per-literal hits fabricates the hit
    /// rate. hitrate must either refuse pgss input or report no plain hits
    /// for it. The current implementation reports calls-1 hits per row.
    #[test]
    fn test_hitrate_does_not_fabricate_hits_from_pgss_input() {
        let fixture = fixture_write(
            "target_pgss.csv",
            "query,calls,total_exec_time\n\
             \"SELECT * FROM users WHERE id = $1\",100,12.5\n",
        );
        let output = fit_run("hitrate", &fixture);
        if let Some(report) = &output.json {
            assert_eq!(
                count(report, "hits"),
                0,
                "no per-literal hits from a shape-level row"
            );
        } else {
            assert!(!output.success, "pgss input refused for hitrate");
        }
    }
}
