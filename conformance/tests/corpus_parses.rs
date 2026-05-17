//! Guards that shipped `.slt` suites parse and that every query carries
//! a well-formed `# pgcache:` annotation. Runs without a database.

use pgcache_conformance::annotation::{self, Routing};
use sqllogictest::{DefaultColumnType, Record, parse_file};

#[test]
fn aggregates_filter_suite_is_well_formed() {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/suites/aggregates_filter.slt");
    let records: Vec<Record<DefaultColumnType>> =
        parse_file(path).expect("parse aggregates_filter.slt");

    let mut routing = Routing::Any;
    let mut queries = 0;
    let mut cached = 0;
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
                match routing {
                    Routing::Cached => cached += 1,
                    other => panic!("every query must be `cached`, got {other:?}: {sql}"),
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
    assert_eq!(
        cached, 10,
        "every query is asserted `cached` (PGC-136 fixed)"
    );
    assert!(
        pgc_102_seen,
        "the PGC-102 count(*) FILTER shape must be present"
    );
}
