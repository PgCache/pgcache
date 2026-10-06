#![allow(clippy::wildcard_enum_match_arm)]

use iddqd::BiHashMap;
use postgres_types::Type;

use super::extract::analyze_query_constraints;
use super::subsume::table_constraints_subsumed;
use super::*;
use crate::catalog::{ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::{QueryBody, query_expr_parse};
use crate::query::resolved::{ResolvedSelectNode, select_node_resolve};

// Helper function to parse SQL and resolve to ResolvedSelectNode
fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
    let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
    let QueryBody::Select(node) = query_expr.body else {
        panic!("expected SELECT");
    };
    select_node_resolve(&node, tables, &["public"]).expect("resolve")
}

// Helper function to create test table metadata
fn test_table_metadata(name: &str, relation_oid: Oid) -> TableMetadata {
    let columns = ColumnStore::new([
        ColumnMetadata {
            name: "id".into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        },
        ColumnMetadata {
            name: "name".into(),
            position: ColumnPosition::from_raw(2),
            type_oid: TypeOid::from_raw(25),
            data_type: Type::TEXT,
            type_name: "text".into(),
            cache_type_name: "text".into(),
            is_primary_key: false,
        },
    ]);

    TableMetadata {
        replica_identity_full: false,
        relation_oid,
        name: name.into(),
        schema: "public".into(),
        primary_key_columns: vec!["id".into()],
        columns,
        indexes: Vec::new(),
    }
}

/// A catalog of `test_table_metadata` tables, OIDs from 1001 in order.
fn catalog(names: &[&str]) -> BiHashMap<TableMetadata> {
    let mut tables = BiHashMap::new();
    for (oid, name) in (1001..).zip(names) {
        tables.insert_overwrite(test_table_metadata(name, Oid::from_raw(oid)));
    }
    tables
}

fn constraints_of(sql: &str, tables: &BiHashMap<TableMetadata>) -> QueryConstraints {
    analyze_query_constraints(&resolve_sql(sql, tables))
}

/// Whether any constraint on `table` satisfies `matches`.
fn table_constraint_any(
    constraints: &QueryConstraints,
    table: &str,
    matches: impl Fn(&TableConstraint) -> bool,
) -> bool {
    constraints
        .table_constraints
        .get(table)
        .is_some_and(|cs| cs.iter().any(matches))
}

fn has_constraint(constraints: &QueryConstraints, spec: &ComparisonSpec) -> bool {
    table_constraint_any(constraints, spec.table, |tc| {
        matches!(tc, TableConstraint::Comparison(c, o, v)
            if c == spec.column && *o == spec.op && *v == spec.value)
    })
}

fn has_in_constraint(constraints: &QueryConstraints, spec: &InSetSpec) -> bool {
    table_constraint_any(constraints, spec.table, |tc| {
        matches!(tc, TableConstraint::AnyOf(c, vs)
            if c == spec.column
                && spec.values.iter().all(|v| vs.contains(v))
                && vs.len() == spec.values.len())
    })
}

fn int(value: i64) -> LiteralValue {
    LiteralValue::Integer(value)
}

fn text(value: &str) -> LiteralValue {
    LiteralValue::String(value.into())
}

/// An expected `(table, column, op, value)` comparison constraint.
struct ComparisonSpec {
    table: &'static str,
    column: &'static str,
    op: BinaryOp,
    value: LiteralValue,
}

fn cmp(
    table: &'static str,
    column: &'static str,
    op: BinaryOp,
    value: LiteralValue,
) -> ComparisonSpec {
    ComparisonSpec {
        table,
        column,
        op,
        value,
    }
}

/// An expected IN-set constraint (exact membership).
struct InSetSpec {
    table: &'static str,
    column: &'static str,
    values: Vec<LiteralValue>,
}

fn in_set(table: &'static str, column: &'static str, values: Vec<LiteralValue>) -> InSetSpec {
    InSetSpec {
        table,
        column,
        values,
    }
}

/// One expected fact about a query's extracted constraints.
enum Expectation {
    Count(usize),
    TableCount(&'static str, usize),
    Comparison(ComparisonSpec),
    InSet(InSetSpec),
}

fn count(n: usize) -> Expectation {
    Expectation::Count(n)
}

fn table_count(table: &'static str, n: usize) -> Expectation {
    Expectation::TableCount(table, n)
}

impl From<ComparisonSpec> for Expectation {
    fn from(spec: ComparisonSpec) -> Self {
        Expectation::Comparison(spec)
    }
}

impl From<InSetSpec> for Expectation {
    fn from(spec: InSetSpec) -> Self {
        Expectation::InSet(spec)
    }
}

/// The constraint must be present.
fn has(constraint: impl Into<Expectation>) -> Expectation {
    constraint.into()
}

impl Expectation {
    fn check(&self, constraints: &QueryConstraints, context: &str) {
        match self {
            Expectation::Count(n) => assert_eq!(
                constraints.column_constraints.len(),
                *n,
                "{context}: column constraint count"
            ),
            Expectation::TableCount(table, n) => assert_eq!(
                constraints
                    .table_constraints
                    .get(*table)
                    .map_or(0, Vec::len),
                *n,
                "{context}: constraint count on {table}"
            ),
            Expectation::Comparison(spec) => assert!(
                has_constraint(constraints, spec),
                "{context}: expected {}.{} {:?} {:?}",
                spec.table,
                spec.column,
                spec.op,
                spec.value
            ),
            Expectation::InSet(spec) => assert!(
                has_in_constraint(constraints, spec),
                "{context}: expected {}.{} IN {:?}",
                spec.table,
                spec.column,
                spec.values
            ),
        }
    }
}

struct ExtractionCase {
    label: &'static str,
    tables: &'static [&'static str],
    sql: &'static str,
    expectations: Vec<Expectation>,
}

fn extraction_cases_check(cases: Vec<ExtractionCase>) {
    for case in cases {
        let constraints = constraints_of(case.sql, &catalog(case.tables));
        let context = format!("{}: {}", case.label, case.sql);
        for expectation in &case.expectations {
            expectation.check(&constraints, &context);
        }
    }
}

#[test]
fn test_equality_extraction_cases() {
    extraction_cases_check(vec![
        ExtractionCase {
            label: "simple constraint",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id = 1",
            expectations: vec![
                count(1),
                table_count("users", 1),
                has(cmp("users", "id", BinaryOp::Equal, int(1))),
            ],
        },
        ExtractionCase {
            label: "subquery extracts outer constraints",
            tables: &["users", "active_users"],
            sql: "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1",
            expectations: vec![count(1), has(cmp("users", "id", BinaryOp::Equal, int(1)))],
        },
        ExtractionCase {
            label: "scalar subquery extracts outer constraints",
            tables: &["users", "orders"],
            sql: "SELECT id, (SELECT COUNT(*) FROM orders) FROM users WHERE id = 1",
            expectations: vec![count(1), has(cmp("users", "id", BinaryOp::Equal, int(1)))],
        },
        ExtractionCase {
            label: "subquery multiple outer constraints",
            tables: &["users", "active_users"],
            sql: "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1 AND name = 'alice'",
            expectations: vec![
                count(2),
                has(cmp("users", "id", BinaryOp::Equal, int(1))),
                has(cmp("users", "name", BinaryOp::Equal, text("alice"))),
            ],
        },
    ]);
}

#[test]
fn test_inequality_extraction_cases() {
    extraction_cases_check(vec![
        ExtractionCase {
            label: "simple inequality",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id > 5",
            expectations: vec![
                count(1),
                has(cmp("users", "id", BinaryOp::GreaterThan, int(5))),
            ],
        },
        ExtractionCase {
            label: "multiple inequalities same column",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id > 5 AND id < 100",
            expectations: vec![
                count(2),
                has(cmp("users", "id", BinaryOp::GreaterThan, int(5))),
                has(cmp("users", "id", BinaryOp::LessThan, int(100))),
            ],
        },
        // 5 < id is equivalent to id > 5
        ExtractionCase {
            label: "reversed operand",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE 5 < id",
            expectations: vec![
                count(1),
                has(cmp("users", "id", BinaryOp::GreaterThan, int(5))),
            ],
        },
        ExtractionCase {
            label: "not equal",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE name != 'deleted'",
            expectations: vec![
                count(1),
                has(cmp("users", "name", BinaryOp::NotEqual, text("deleted"))),
            ],
        },
        ExtractionCase {
            label: "mixed equality and inequality",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id = 1 AND name != 'deleted'",
            expectations: vec![
                count(2),
                has(cmp("users", "id", BinaryOp::Equal, int(1))),
                has(cmp("users", "name", BinaryOp::NotEqual, text("deleted"))),
            ],
        },
        // Should propagate: a.id > 5 -> b.id > 5
        ExtractionCase {
            label: "inequality propagation through join",
            tables: &["a", "b"],
            sql: "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id > 5",
            expectations: vec![
                count(2),
                has(cmp("a", "id", BinaryOp::GreaterThan, int(5))),
                has(cmp("b", "id", BinaryOp::GreaterThan, int(5))),
            ],
        },
        ExtractionCase {
            label: "or prevents inequality extraction",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id > 5 OR id < 2",
            expectations: vec![count(0)],
        },
    ]);
}

#[test]
fn test_between_extraction_cases() {
    extraction_cases_check(vec![
        ExtractionCase {
            label: "between",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id BETWEEN 100 AND 500",
            expectations: vec![
                count(2),
                has(cmp("users", "id", BinaryOp::GreaterThanOrEqual, int(100))),
                has(cmp("users", "id", BinaryOp::LessThanOrEqual, int(500))),
            ],
        },
        ExtractionCase {
            label: "between with and",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE name = 'alice' AND id BETWEEN 100 AND 500",
            expectations: vec![
                count(3),
                has(cmp("users", "name", BinaryOp::Equal, text("alice"))),
                has(cmp("users", "id", BinaryOp::GreaterThanOrEqual, int(100))),
                has(cmp("users", "id", BinaryOp::LessThanOrEqual, int(500))),
            ],
        },
        // Both tables should get the two BETWEEN constraints
        ExtractionCase {
            label: "between propagation through join",
            tables: &["a", "b"],
            sql: "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id BETWEEN 1 AND 10",
            expectations: vec![
                count(4),
                has(cmp("a", "id", BinaryOp::GreaterThanOrEqual, int(1))),
                has(cmp("a", "id", BinaryOp::LessThanOrEqual, int(10))),
                has(cmp("b", "id", BinaryOp::GreaterThanOrEqual, int(1))),
                has(cmp("b", "id", BinaryOp::LessThanOrEqual, int(10))),
            ],
        },
        // NOT BETWEEN is an OR (id < 100 OR id > 500), so no constraints
        ExtractionCase {
            label: "not between",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id NOT BETWEEN 100 AND 500",
            expectations: vec![count(0)],
        },
        // Bounds are reversed (500, 100) — should normalize to (100, 500)
        ExtractionCase {
            label: "between symmetric reversed bounds",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 500 AND 100",
            expectations: vec![
                count(2),
                has(cmp("users", "id", BinaryOp::GreaterThanOrEqual, int(100))),
                has(cmp("users", "id", BinaryOp::LessThanOrEqual, int(500))),
            ],
        },
        // Bounds already in order — same result as reversed
        ExtractionCase {
            label: "between symmetric already ordered",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 100 AND 500",
            expectations: vec![
                count(2),
                has(cmp("users", "id", BinaryOp::GreaterThanOrEqual, int(100))),
                has(cmp("users", "id", BinaryOp::LessThanOrEqual, int(500))),
            ],
        },
        // Can't compare parameter with literal — skip extraction
        ExtractionCase {
            label: "between symmetric with parameter",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id BETWEEN SYMMETRIC $1 AND 500",
            expectations: vec![count(0)],
        },
        // NOT BETWEEN SYMMETRIC is still an OR — no constraints
        ExtractionCase {
            label: "not between symmetric",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id NOT BETWEEN SYMMETRIC 500 AND 100",
            expectations: vec![count(0)],
        },
        // Non-literal bound (column reference) — skip extraction
        ExtractionCase {
            label: "between with non literal bounds",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id BETWEEN name AND 10",
            expectations: vec![count(0)],
        },
    ]);
}

#[test]
fn test_in_set_extraction_cases() {
    extraction_cases_check(vec![
        ExtractionCase {
            label: "equivalence in where",
            tables: &["a", "b"],
            sql: "SELECT * FROM a, b WHERE a.id = b.id AND a.id = 1",
            expectations: vec![
                count(2),
                has(cmp("a", "id", BinaryOp::Equal, int(1))),
                has(cmp("b", "id", BinaryOp::Equal, int(1))),
            ],
        },
        ExtractionCase {
            label: "in extraction",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id IN (1, 2, 3)",
            expectations: vec![has(in_set("users", "id", vec![int(1), int(2), int(3)]))],
        },
        ExtractionCase {
            label: "in with and",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id IN (1, 2) AND name = 'alice'",
            expectations: vec![
                has(in_set("users", "id", vec![int(1), int(2)])),
                has(cmp("users", "name", BinaryOp::Equal, text("alice"))),
            ],
        },
        // NOT IN → individual NotEqual constraints
        ExtractionCase {
            label: "not in extraction",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id NOT IN (1, 2, 3)",
            expectations: vec![
                has(cmp("users", "id", BinaryOp::NotEqual, int(1))),
                has(cmp("users", "id", BinaryOp::NotEqual, int(2))),
                has(cmp("users", "id", BinaryOp::NotEqual, int(3))),
            ],
        },
        // Should propagate: a.id IN (1, 2) → b.id IN (1, 2)
        ExtractionCase {
            label: "in propagation through join",
            tables: &["a", "b"],
            sql: "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id IN (1, 2)",
            expectations: vec![
                has(in_set("a", "id", vec![int(1), int(2)])),
                has(in_set("b", "id", vec![int(1), int(2)])),
            ],
        },
    ]);
}

#[test]
fn test_subsumption_cases() {
    // (label, cached SQL, new SQL, whether cached subsumes new) over `users`.
    #[rustfmt::skip]
    let cases = [
        // Cached: SELECT * FROM users (no WHERE) → full scan covers everything
        ("cached no constraints", "SELECT * FROM users", "SELECT * FROM users WHERE id = 1", true),
        ("same equality", "SELECT * FROM users WHERE id = 1", "SELECT * FROM users WHERE id = 1", true),
        // Cached has fewer equality constraints → new is narrower. Subsumed.
        ("new narrower", "SELECT * FROM users WHERE id = 1", "SELECT * FROM users WHERE id = 1 AND name = 'alice'", true),
        ("different values", "SELECT * FROM users WHERE id = 1", "SELECT * FROM users WHERE id = 2", false),
        // Cached is narrower than new → not subsumed
        ("cached has extra constraint", "SELECT * FROM users WHERE id = 1 AND name = 'alice'", "SELECT * FROM users WHERE id = 1", false),
        // New has no constraints but cached does → new is broader, not subsumed
        ("new no constraints", "SELECT * FROM users WHERE id = 1", "SELECT * FROM users", false),
        // id > 10 implies id > 5 — subsumed
        ("range tighter lower", "SELECT * FROM users WHERE id > 5", "SELECT * FROM users WHERE id > 10", true),
        // id > 1 does NOT imply id > 3 — not subsumed
        ("range looser lower", "SELECT * FROM users WHERE id > 3", "SELECT * FROM users WHERE id > 1", false),
        // id > 3 (exclusive) is tighter than id >= 3 (inclusive) — subsumed
        ("range exclusive tighter than inclusive", "SELECT * FROM users WHERE id >= 3", "SELECT * FROM users WHERE id > 3", true),
        // id >= 3 subsumed by id >= 3
        ("range same inclusive bound", "SELECT * FROM users WHERE id >= 3", "SELECT * FROM users WHERE id >= 3", true),
        // id BETWEEN 5 AND 8 is contained in id >= 3 AND id <= 10
        ("range containment", "SELECT * FROM users WHERE id >= 3 AND id <= 10", "SELECT * FROM users WHERE id BETWEEN 5 AND 8", true),
        // id > 50 has no upper bound, but cached has id < 100 — not subsumed
        ("range missing upper", "SELECT * FROM users WHERE id > 0 AND id < 100", "SELECT * FROM users WHERE id > 50", false),
        // id = 5 is within id > 3
        ("point in range", "SELECT * FROM users WHERE id > 3", "SELECT * FROM users WHERE id = 5", true),
        // id = 2 is NOT within id > 3
        ("point outside range", "SELECT * FROM users WHERE id > 3", "SELECT * FROM users WHERE id = 2", false),
        // Cached = 5 (single point), new wants id > 3 (a range) — not subsumed
        ("equal not subsumed by range", "SELECT * FROM users WHERE id = 5", "SELECT * FROM users WHERE id > 3", false),
        // Cached != 5, new = 3. 3 ≠ 5, so new's result is within cached's — subsumed
        ("not equal by different equal", "SELECT * FROM users WHERE id != 5", "SELECT * FROM users WHERE id = 3", true),
        // Cached != 5, new = 5 — 5 is excluded by cached. Not subsumed.
        ("not equal by same equal", "SELECT * FROM users WHERE id != 5", "SELECT * FROM users WHERE id = 5", false),
        // Cached != 5, new id > 10 — entire range excludes 5. Subsumed.
        ("not equal by excluding range", "SELECT * FROM users WHERE id != 5", "SELECT * FROM users WHERE id > 10", true),
        // Cached != 5, new id > 3 — range includes 5. Not subsumed.
        ("not equal by including range", "SELECT * FROM users WHERE id != 5", "SELECT * FROM users WHERE id > 3", false),
        // Cached != 5, new != 5 — same exclusion. Subsumed.
        ("not equal same", "SELECT * FROM users WHERE id != 5", "SELECT * FROM users WHERE id != 5", true),
        // Cached != 5, new != 3 — different exclusions. Not subsumed.
        ("not equal different", "SELECT * FROM users WHERE id != 5", "SELECT * FROM users WHERE id != 3", false),
        // Both columns must be subsumed
        ("mixed columns", "SELECT * FROM users WHERE id > 3 AND name = 'alice'", "SELECT * FROM users WHERE id > 5 AND name = 'alice'", true),
        // id subsumed but name differs — not subsumed
        ("mixed columns mismatch", "SELECT * FROM users WHERE id > 3 AND name = 'alice'", "SELECT * FROM users WHERE id > 5 AND name = 'bob'", false),
        // New has contradictory constraints (= 5 AND > 10) → Empty → trivially subsumed
        ("contradictory new", "SELECT * FROM users WHERE id = 5", "SELECT * FROM users WHERE id = 5 AND id > 10", true),
        // Cached has contradictory constraints → Empty → no data, not subsumed
        ("contradictory cached", "SELECT * FROM users WHERE id = 5 AND id > 10", "SELECT * FROM users WHERE id = 5", false),
        // IN (1,2,3) subsumed by IN (1,2) — subset
        ("in subset", "SELECT * FROM users WHERE id IN (1, 2, 3)", "SELECT * FROM users WHERE id IN (1, 2)", true),
        // IN (1,2,3) subsumed by = 2 — point in set
        ("in point", "SELECT * FROM users WHERE id IN (1, 2, 3)", "SELECT * FROM users WHERE id = 2", true),
        // IN (1,2,3) NOT subsumed by IN (1,4) — 4 not in set
        ("in not subset", "SELECT * FROM users WHERE id IN (1, 2, 3)", "SELECT * FROM users WHERE id IN (1, 4)", false),
        // id > 0 subsumed by IN (1,2,3) — all values > 0
        ("range subsumes in", "SELECT * FROM users WHERE id > 0", "SELECT * FROM users WHERE id IN (1, 2, 3)", true),
        // IN (1,2,3) NOT subsumed by id > 0 — set is finite, range is infinite
        ("in not subsumes range", "SELECT * FROM users WHERE id IN (1, 2, 3)", "SELECT * FROM users WHERE id > 0", false),
        ("unconstrained subsumes in", "SELECT * FROM users", "SELECT * FROM users WHERE id IN (1, 2, 3)", true),
        // PGC-106 (option C) headline: cached `ANY([1,2,3])` should
        // subsume new `ANY([1])`.
        ("any subsumes narrower any", "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])", "SELECT * FROM users WHERE id = ANY(ARRAY[1])", true),
        // The PGC-106 option-B scenario stays correct under option C:
        // disjoint arrays can't subsume each other.
        ("any does not subsume disjoint any", "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2])", "SELECT * FROM users WHERE id = ANY(ARRAY[3, 4, 5])", false),
        // Cached `ANY([1,2,3])` should also subsume new `WHERE id = 2`.
        ("any subsumes equality", "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])", "SELECT * FROM users WHERE id = 2", true),
        // `WHERE name::int4 > 100` (cached) subsumes `WHERE name::int4 > 200`
        // (new) — every row in the new is also in the cached.
        ("cast comparison range subsumes tighter range", "SELECT * FROM users WHERE name::int4 > 100", "SELECT * FROM users WHERE name::int4 > 200", true),
        // `WHERE name::int4 > 200` (cached) does NOT subsume
        // `WHERE name::int4 > 100` (new) — new is broader.
        ("cast comparison range does not subsume looser range", "SELECT * FROM users WHERE name::int4 > 200", "SELECT * FROM users WHERE name::int4 > 100", false),
        // `name::int4 = 42` and `name::int8 = 42` are separate value domains;
        // neither query subsumes the other even though the value matches.
        ("different casts do not cross subsume", "SELECT * FROM users WHERE name::int4 = 42", "SELECT * FROM users WHERE name::int8 = 42", false),
        // `name = '42'` (bare text compare) and `name::int4 = 42` are
        // different predicates — bare bytes vs int value. Subsumption must
        // not cross domains.
        ("bare and cast constraints do not cross subsume", "SELECT * FROM users WHERE name = '42'", "SELECT * FROM users WHERE name::int4 = 42", false),
    ];
    let tables = catalog(&["users"]);
    for (label, cached_sql, new_sql, expected) in cases {
        let cached = constraints_of(cached_sql, &tables);
        let new = constraints_of(new_sql, &tables);
        assert_eq!(
            table_constraints_subsumed(&new, &cached, "users"),
            expected,
            "{label}: cached {cached_sql:?} subsumes new {new_sql:?}"
        );
    }
}

// ========== Existing equality tests (updated for new tuple format) ==========

#[test]
fn test_join_propagation() {
    let tables = catalog(&["test", "test_map"]);

    let sql = "SELECT * FROM test t JOIN test_map tm ON tm.id = t.id WHERE t.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Should propagate: t.id = 1 -> tm.id = 1
    assert_eq!(constraints.column_constraints.len(), 2);

    let test_constraints = constraints.table_constraints.get("test").unwrap();
    assert_eq!(test_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        &cmp("test", "id", BinaryOp::Equal, int(1))
    ));

    let test_map_constraints = constraints.table_constraints.get("test_map").unwrap();
    assert_eq!(test_map_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        &cmp("test_map", "id", BinaryOp::Equal, int(1))
    ));

    assert_eq!(constraints.equivalences.len(), 1);
}

#[test]
fn test_transitive_propagation() {
    let mut tables = catalog(&["a", "b"]);

    tables.insert_overwrite(TableMetadata {
        replica_identity_full: false,
        relation_oid: Oid::from_raw(1003),
        name: "c".into(),
        schema: "public".into(),
        primary_key_columns: vec!["id".into()],
        columns: ColumnStore::new([ColumnMetadata {
            name: "id".into(),
            position: ColumnPosition::from_raw(1),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: true,
        }]),
        indexes: Vec::new(),
    });

    let sql = "SELECT * FROM (a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id WHERE a.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Should propagate through: a.id = 1 -> b.id = 1 -> c.id = 1
    assert_eq!(constraints.column_constraints.len(), 3);

    assert!(has_constraint(
        &constraints,
        &cmp("a", "id", BinaryOp::Equal, int(1))
    ));
    assert!(has_constraint(
        &constraints,
        &cmp("b", "id", BinaryOp::Equal, int(1))
    ));
    assert!(has_constraint(
        &constraints,
        &cmp("c", "id", BinaryOp::Equal, int(1))
    ));
}

#[test]
fn test_multiple_constraints() {
    let tables = catalog(&["users"]);

    let sql = "SELECT * FROM users WHERE id = 1 AND name = 'john'";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 2);

    let user_constraints = constraints.table_constraints.get("users").unwrap();
    assert_eq!(user_constraints.len(), 2);

    assert!(has_constraint(
        &constraints,
        &cmp("users", "id", BinaryOp::Equal, int(1))
    ));
    assert!(has_constraint(
        &constraints,
        &cmp("users", "name", BinaryOp::Equal, text("john")),
    ));
}

#[test]
fn test_no_propagation_with_or() {
    let tables = catalog(&["users"]);

    let sql = "SELECT * FROM users WHERE id = 1 OR id = 2";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert_eq!(constraints.column_constraints.len(), 0);
    assert_eq!(constraints.table_constraints.len(), 0);
}

#[test]
fn test_self_join() {
    let tables = catalog(&["test"]);

    let sql = "SELECT * FROM test t1 JOIN test t2 ON t1.id = t2.id WHERE t1.id = 1";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    // Both t1.id and t2.id reference the same column (test.id)
    // So we get 1 unique column with constraint
    assert_eq!(constraints.column_constraints.len(), 1);

    let test_constraints = constraints.table_constraints.get("test").unwrap();
    assert_eq!(test_constraints.len(), 1);
    assert!(has_constraint(
        &constraints,
        &cmp("test", "id", BinaryOp::Equal, int(1))
    ));
}

#[test]
fn test_derived_table_no_outer_constraints() {
    let tables = catalog(&["users"]);

    let sql = "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub";
    let resolved = resolve_sql(sql, &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(
        constraints.column_constraints.is_empty(),
        "Derived table with no outer WHERE should have no constraints"
    );
}

// ========== Inequality tests ==========

// ========== BETWEEN tests ==========

// ========== Subsumption tests ==========

// ========== Range subsumption tests ==========

// ========== IN extraction tests ==========

#[test]
fn test_in_with_parameter_skipped() {
    let tables = catalog(&["users"]);

    let sql = "SELECT * FROM users WHERE id IN (1, $1)";
    let resolved = resolve_sql(sql, &tables);
    let constraints = analyze_query_constraints(&resolved);

    // Parameter in IN list → entire IN skipped
    assert!(constraints.table_constraints.is_empty());
}

// ========== IN subsumption tests ==========

// ========== IN + range intersection in column_range_build ==========

#[test]
fn test_in_set_with_range_filter() {
    // IN (1,2,3,4,5) AND id > 3 → InSet({4, 5})
    let tcs = [
        TableConstraint::AnyOf("id".into(), vec![int(1), int(2), int(3), int(4), int(5)]),
        TableConstraint::Comparison("id".into(), BinaryOp::GreaterThan, int(3)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    match range {
        ColumnRange::InSet(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&int(4)));
            assert!(set.contains(&int(5)));
        }
        _ => panic!("expected InSet, got {range:?}"),
    }
}

#[test]
fn test_in_set_with_equality_match() {
    // IN (1,2,3) AND id = 2 → Equal(2)
    let tcs = [
        TableConstraint::AnyOf("id".into(), vec![int(1), int(2), int(3)]),
        TableConstraint::Comparison("id".into(), BinaryOp::Equal, int(2)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    assert!(matches!(
        range,
        ColumnRange::Equal(LiteralValue::Integer(2))
    ));
}

#[test]
fn test_in_set_with_equality_mismatch() {
    // IN (1,2,3) AND id = 5 → Empty
    let tcs = [
        TableConstraint::AnyOf("id".into(), vec![int(1), int(2), int(3)]),
        TableConstraint::Comparison("id".into(), BinaryOp::Equal, int(5)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    assert!(matches!(range, ColumnRange::Empty));
}

#[test]
fn test_in_set_with_not_equal() {
    // IN (1,2,3) AND id != 2 → InSet({1, 3})
    let tcs = [
        TableConstraint::AnyOf("id".into(), vec![int(1), int(2), int(3)]),
        TableConstraint::Comparison("id".into(), BinaryOp::NotEqual, int(2)),
    ];
    let refs: Vec<&TableConstraint> = tcs.iter().collect();
    let range = column_range_build(&refs);
    match range {
        ColumnRange::InSet(set) => {
            assert_eq!(set.len(), 2);
            assert!(set.contains(&int(1)));
            assert!(set.contains(&int(3)));
        }
        _ => panic!("expected InSet, got {range:?}"),
    }
}

// ========== PGC-106: where_analysis_complete tracking ==========

#[test]
fn test_no_where_clause_is_complete() {
    let tables = catalog(&["users"]);
    let resolved = resolve_sql("SELECT * FROM users", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(
        constraints.where_analysis_complete,
        "no WHERE clause is trivially complete (full table scan)"
    );
}

#[test]
fn test_simple_equality_is_complete() {
    let tables = catalog(&["users"]);
    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(constraints.where_analysis_complete);
}

#[test]
fn test_in_clause_is_complete() {
    let tables = catalog(&["users"]);
    let resolved = resolve_sql("SELECT * FROM users WHERE id IN (1, 2, 3)", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(constraints.where_analysis_complete);
}

#[test]
fn test_any_eq_clause_extracts_inset() {
    // PGC-106 (option C): `WHERE col = ANY(<array>)` is set membership;
    // extracted as `ColumnConstraint::InSet` so cached ANY-queries can
    // subsume narrower ANY-queries on the same column.
    let tables = catalog(&["users"]);
    let resolved = resolve_sql(
        "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
        &tables,
    );

    let constraints = analyze_query_constraints(&resolved);

    assert!(
        constraints.where_analysis_complete,
        "ANY = is now extractable, analysis is complete"
    );
    let users_cs = constraints
        .table_constraints
        .get("users")
        .expect("constraint extracted for users");
    assert_eq!(users_cs.len(), 1);
    assert!(matches!(
        users_cs[0],
        TableConstraint::AnyOf(ref col, ref vs)
            if col == "id" && vs.len() == 3
    ));
}

#[test]
fn test_or_clause_marks_incomplete() {
    // OR is still in the unrecognized-expression bucket; subsumption
    // must continue to fall back to "not subsumed" until/unless the
    // analyzer learns to handle disjunctions.
    let tables = catalog(&["users"]);
    let resolved = resolve_sql("SELECT * FROM users WHERE id = 1 OR id = 2", &tables);

    let constraints = analyze_query_constraints(&resolved);

    assert!(!constraints.where_analysis_complete);
}

#[test]
fn test_subsumption_full_scan_still_subsumes() {
    // Sanity check that the new gate doesn't regress the legitimate
    // "full-scan subsumes everything" case: cached query has no WHERE
    // clause, so analysis is complete AND `table_constraints` is empty.
    let tables = catalog(&["users"]);

    let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
    let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

    assert!(cached.where_analysis_complete);
    assert!(
        table_constraints_subsumed(&new, &cached, "users"),
        "true full-scan cached query should still subsume"
    );
}

// ------------------------------------------------------------------
// PGC-149: identity TypeCast strip in constraint extraction
// ------------------------------------------------------------------

#[test]
fn test_identity_text_cast_extracts_comparison_constraint() {
    // `name::text = 'alice'` on a TEXT column must extract the same
    // ColumnConstraint::Comparison as `name = 'alice'` would.
    let tables = catalog(&["users"]);

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::text = 'alice'",
        &tables,
    ));

    assert!(constraints.where_analysis_complete);
    assert!(has_constraint(
        &constraints,
        &cmp("users", "name", BinaryOp::Equal, text("alice")),
    ));
}

#[test]
fn test_identity_text_cast_enables_subsumption() {
    // Two queries that only differ by a redundant `::text` on a TEXT
    // column should subsume each other.
    let tables = catalog(&["users"]);

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name = 'alice'",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::text = 'alice'",
        &tables,
    ));

    assert!(cached.where_analysis_complete);
    assert!(new.where_analysis_complete);
    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_identity_text_cast_on_int_column_extracts_constraint() {
    // PGC-177: int → ::text is identity, so the constraint must be
    // extracted as `id = 42` for subsumption to work.
    let tables = catalog(&["users"]);

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE id::text = '42'",
        &tables,
    ));

    assert!(constraints.where_analysis_complete);
    assert!(has_constraint(
        &constraints,
        &cmp("users", "id", BinaryOp::Equal, text("42")),
    ));
}

#[test]
fn test_non_identity_text_cast_keeps_analysis_incomplete() {
    // `name::numeric = 42` is a text → numeric coercion, not identity.
    // Analysis must remain incomplete so subsumption stays conservative.
    let tables = catalog(&["users"]);

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::numeric = 42",
        &tables,
    ));

    assert!(
        !constraints.where_analysis_complete,
        "non-identity cast must leave analysis incomplete"
    );
}

// ---------------------------------------------------------------
// PGC-182: subsumption for non-identity casts via CastComparison.
// ---------------------------------------------------------------

fn has_cast_constraint(
    constraints: &QueryConstraints,
    spec: &ComparisonSpec,
    cast: &CastTarget,
) -> bool {
    table_constraint_any(constraints, spec.table, |tc| {
        matches!(tc, TableConstraint::CastComparison(c, k, o, v)
            if c == spec.column && k == cast && *o == spec.op && *v == spec.value)
    })
}

#[test]
fn test_text_to_int4_extracts_cast_comparison() {
    let tables = catalog(&["users"]);

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));

    assert!(constraints.where_analysis_complete);
    assert!(has_cast_constraint(
        &constraints,
        &cmp("users", "name", BinaryOp::Equal, int(42)),
        &CastTarget::Int4
    ));
}

#[test]
fn test_cast_comparison_subsumes_self() {
    // Identical cast queries: both extract the same CastComparison,
    // subsumption holds.
    let tables = catalog(&["users"]);

    let cached = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));
    let new = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users WHERE name::int4 = 42",
        &tables,
    ));

    assert!(cached.where_analysis_complete);
    assert!(new.where_analysis_complete);
    assert!(table_constraints_subsumed(&new, &cached, "users"));
}

#[test]
fn test_cast_comparison_does_not_propagate_through_equivalence() {
    // `name::int4 = 5 AND name = other_name` must NOT yield
    // `other_name::int4 = 5` — the cast doesn't follow equivalences
    // without knowing the other column's storage type is also castable.
    let tables = catalog(&["users", "users2"]);

    let constraints = analyze_query_constraints(&resolve_sql(
        "SELECT * FROM users u JOIN users2 u2 ON u.name = u2.name \
         WHERE u.name::int4 = 5",
        &tables,
    ));

    // u.name::int4 = 5 is extracted, but u2.name has no cast constraint.
    assert!(has_cast_constraint(
        &constraints,
        &cmp("users", "name", BinaryOp::Equal, int(5)),
        &CastTarget::Int4
    ));
    assert!(!has_cast_constraint(
        &constraints,
        &cmp("users2", "name", BinaryOp::Equal, int(5)),
        &CastTarget::Int4
    ));
}
