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
    fit_run_with(mode, fixture, &[])
}

fn fit_run_with(mode: &str, fixture: &PathBuf, extra: &[&str]) -> FitOutput {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache-fit"))
        .args([mode, "--json"])
        .args(extra)
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

fn check_text(name: &str, trace: &str, extra: &[&str]) -> String {
    let fixture = fixture_write(name, trace);
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache-fit"))
        .arg("check")
        .args(extra)
        .arg(fixture)
        .output()
        .expect("run pgcache-fit");
    assert!(output.status.success(), "check run succeeds");
    String::from_utf8(output.stdout).expect("utf-8 report")
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
    fn test_check_verdicts_aggregate_repeated_statements() {
        let report = check_json(
            "stable_verdict_repeats.sql",
            "SELECT * FROM pg_class;\n\
             SELECT * FROM pg_class;\n\
             SELECT * FROM pg_class;\n\
             SELECT * FROM users WHERE id = 1;\n",
        );
        let verdicts = report["verdicts"].as_array().expect("verdicts array");
        assert_eq!(verdicts.len(), 2);
        assert_eq!(verdicts[0]["occurrences"], 3);
        assert_eq!(verdicts[0]["verdict"], "passthrough");
        assert_eq!(verdicts[1]["occurrences"], 1);
        assert_eq!(count(&report, "statements"), 4);
    }

    #[test]
    fn test_check_text_lists_statements_only_on_request() {
        let trace: String = (1..=7)
            .map(|i| format!("SELECT * FROM pg_class WHERE oid = {i};\n"))
            .chain(std::iter::once(
                "SELECT * FROM t TABLESAMPLE SYSTEM (10);\n".to_owned(),
            ))
            .collect();
        let text = check_text("stable_text_default.sql", &trace, &[]);
        assert!(!text.contains("statements:\n"));
        assert!(!text.contains("SELECT * FROM pg_class WHERE oid"));

        let all = check_text("stable_text_all.sql", &trace, &["--statements"]);
        assert!(all.contains("Passthrough statements:"));
        assert!(all.contains("system catalog reference: 7 statements"));
        assert_eq!(all.matches("SELECT * FROM pg_class WHERE oid").count(), 7);
        // Conversion failures carry the converter's detail line.
        assert!(all.contains("  SELECT * FROM t TABLESAMPLE SYSTEM (10)\n    Unsupported"));
    }

    #[test]
    fn test_check_text_statements_flag_groups_every_verdict() {
        let text = check_text(
            "stable_text_groups.sql",
            "SELECT * FROM users WHERE id = 1;\n\
             INSERT INTO users (id) VALUES (2);\n\
             BEGIN;\n",
            &["--statements"],
        );
        assert!(text.contains("Cacheable statements:\n  SELECT * FROM users WHERE id = 1"));
        assert!(text.contains("Write statements:\n\nusers: 1 statement"));
        assert!(text.contains("Utility statements:\n  BEGIN"));
    }

    #[test]
    fn test_check_shape_funnel() {
        let report = check_json(
            "stable_funnel.sql",
            "SELECT * FROM users;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 2;\n",
        );
        assert_eq!(count(&report, "distinct_fingerprints"), 3);
        // Per-literal fingerprints collapse to one shape per predicate form.
        assert_eq!(count(&report, "distinct_shapes"), 2);
        assert!(
            report.get("post_subsumption_estimate").is_none(),
            "subsumption is order-dependent and belongs to hitrate only"
        );
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

    /// The writer pushes outer predicates into derived-table branches before
    /// admission analysis (predicate_pushdown_apply), so the outer-WHERE form
    /// of a derived table caches only id=5 rows too — it must not register as
    /// a full-scan subsumer any more than the inner-WHERE form does.
    #[test]
    fn test_outer_where_derived_table_does_not_subsume_base_table() {
        let report = hitrate_json(
            "target_derived_outer.sql",
            "SELECT * FROM (SELECT * FROM users) s WHERE s.id = 5;\n\
             SELECT * FROM users WHERE id = 7;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    /// Reducer shapes (aggregates, DISTINCT, GROUP BY) force unbounded
    /// population in the proxy (max_limit = None), so repeats with a
    /// different LIMIT are plain hits, never limit bumps.
    #[test]
    fn test_reducer_limit_repeats_are_hits() {
        let report = hitrate_json(
            "target_reducer_limit.sql",
            "SELECT count(*) FROM events LIMIT 3;\n\
             SELECT count(*) FROM events LIMIT 5;\n",
        );
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "hits"), 1);
    }

    /// ROLLBACK TO SAVEPOINT does not end the transaction: SELECTs after it
    /// are still inside the explicit transaction and never served. The
    /// boundary comes from the parsed statement kind, not the first token.
    #[test]
    fn test_rollback_to_savepoint_keeps_transaction_open() {
        let report = hitrate_json(
            "target_savepoint.sql",
            "BEGIN;\n\
             SAVEPOINT s;\n\
             SELECT * FROM users WHERE id = 1;\n\
             ROLLBACK TO SAVEPOINT s;\n\
             SELECT * FROM users WHERE id = 1;\n\
             COMMIT;\n",
        );
        assert_eq!(
            count(&report, "hits") + count(&report, "subsumption_hits"),
            0
        );
        assert_eq!(count(&report, "in_transaction_calls"), 2);
    }

    /// The transaction gate is per backend session: one session's open
    /// transaction must not deny hits to autocommit traffic from another.
    #[test]
    fn test_transaction_gate_is_per_session() {
        let report = hitrate_json(
            "target_txn_sessions.log",
            "2026-08-28 10:00:00.000 PDT [100] LOG:  statement: BEGIN\n\
             2026-08-28 10:00:00.001 PDT [200] LOG:  statement: SELECT * FROM users WHERE id = 1\n\
             2026-08-28 10:00:00.002 PDT [200] LOG:  statement: SELECT * FROM users WHERE id = 1\n\
             2026-08-28 10:00:00.003 PDT [100] LOG:  statement: COMMIT\n",
        );
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "hits"), 1);
        assert_eq!(count(&report, "in_transaction_calls"), 0);
    }

    /// A same-table UNION parent registers one update query per branch, so
    /// the writer's single-relation gate (duplicate-preserving relation_oids)
    /// rejects it as a subsumer even though only one distinct table appears.
    #[test]
    fn test_same_table_union_parent_never_subsumes() {
        let report = hitrate_json(
            "target_union_parent.sql",
            "SELECT * FROM users WHERE id = 1 UNION SELECT * FROM users WHERE name = 'a';\n\
             SELECT * FROM users WHERE id = 1;\n",
        );
        assert_eq!(count(&report, "subsumption_hits"), 0);
        assert_eq!(count(&report, "cold_misses"), 2);
    }

    /// The replay runs the proxy's own serve decision (PGC-392), so its
    /// admission gate is available offline: with `--admission-threshold 2`
    /// the first sighting is forwarded without registering, the second
    /// registers (cold miss), the third hits.
    #[test]
    fn test_admission_threshold_replayed() {
        let fixture = fixture_write(
            "admission_threshold.sql",
            "SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 1;\n\
             SELECT * FROM users WHERE id = 1;\n",
        );
        let output = fit_run_with("hitrate", &fixture, &["--admission-threshold", "2"]);
        assert!(output.success, "hitrate run succeeds");
        let report = output.json.expect("hitrate JSON present");
        assert_eq!(count(&report, "admission_threshold"), 2);
        assert_eq!(count(&report, "pending_forwards"), 1);
        assert_eq!(count(&report, "cold_misses"), 1);
        assert_eq!(count(&report, "hits"), 1);
    }
}
