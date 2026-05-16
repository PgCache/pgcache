//! Guards that shipped `.slt` suites parse and that every query carries
//! a well-formed `# pgcache:` annotation. Runs without a database.

use pgcache_conformance::annotation::{self, Routing};
use sqllogictest::{DefaultColumnType, Record, parse_file};

#[test]
fn aggregates_filter_suite_is_well_formed() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/suites/aggregates_filter.slt"
    );
    let records: Vec<Record<DefaultColumnType>> =
        parse_file(path).expect("parse aggregates_filter.slt");

    let mut routing = Routing::Any;
    let mut queries = 0;
    let mut cached = 0;
    let mut any = 0;
    let mut pgc_102_seen = false;

    for record in records {
        match record {
            Record::Comment(lines) => {
                // A malformed annotation must be a hard parse error here.
                routing = annotation::scan(lines.iter().map(String::as_str))
                    .expect("annotation scans cleanly");
            }
            Record::Query { sql, .. } => {
                queries += 1;
                let is_pgc136 = sql.contains("hundred < 50")
                    && sql.contains("hundred >= 50");
                match routing {
                    // The two-count query stays `any` until PGC-136 is
                    // fixed; everything else is asserted `cached`.
                    Routing::Any => {
                        any += 1;
                        assert!(
                            is_pgc136,
                            "only the PGC-136 query may be `any`: {sql}"
                        );
                    }
                    Routing::Cached => {
                        cached += 1;
                        assert!(
                            !is_pgc136,
                            "PGC-136 query must stay `any`, not `cached`"
                        );
                    }
                    Routing::Passthrough => {
                        panic!("no query should be `passthrough`: {sql}")
                    }
                }
                if sql.contains("count(*) FILTER (WHERE four = 1)") {
                    pgc_102_seen = true;
                }
                routing = Routing::Any;
            }
            _ => {}
        }
    }

    assert_eq!(queries, 10, "expected 10 ported FILTER queries");
    assert_eq!(cached, 9, "expected 9 queries asserted `cached`");
    assert_eq!(any, 1, "expected exactly 1 `any` query (PGC-136)");
    assert!(pgc_102_seen, "the PGC-102 count(*) FILTER shape must be present");
}
