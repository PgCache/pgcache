#![allow(clippy::wildcard_enum_match_arm, clippy::type_complexity)]

use iddqd::BiHashMap;
use postgres_types::Type;

use super::subsume::table_constraints_subsumed;
use super::*;
use crate::cache::UpdateQuerySource;
use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::decorrelate::query_decorrelate;
use crate::query::resolve::ResolvedQuery;
use crate::query::update::select_branches_with_source;

// Helper function to create test table metadata
fn test_table_metadata(name: &str, relation_oid: Oid) -> TableMetadata {
    let columns = [
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
    ];

    TableMetadata::for_test(name, relation_oid.get(), columns)
}

/// A catalog of `test_table_metadata` tables, OIDs from 1001 in order.
fn catalog(names: &[&str]) -> Catalog {
    let mut tables = BiHashMap::new();
    for (oid, name) in (1001..).zip(names) {
        tables.insert_overwrite(test_table_metadata(name, Oid::from_raw(oid)).into());
    }
    tables
}

/// Parse and bind `sql` over `tables`.
fn resolve_sql(sql: &str, tables: &Catalog) -> ResolvedQuery {
    ResolvedQuery::parse_resolve(sql, tables, &["public"]).expect("resolve")
}

/// The constraints of a bound SELECT.
fn analyze_query_constraints(bound: &ResolvedQuery) -> QueryConstraints {
    query_constraints_analyze(bound, bound.select().expect("a SELECT"))
}

/// The constraints of `sql`.
/// `table_constraints` in a comparable order: the walks visit a hash set,
/// so their per-table lists come out in any order.
fn table_constraints_sorted(constraints: &QueryConstraints) -> Vec<(ecow::EcoString, Vec<String>)> {
    let mut tables: Vec<(ecow::EcoString, Vec<String>)> = constraints
        .table_constraints
        .iter()
        .map(|(table, list)| {
            let mut list: Vec<String> = list.iter().map(|c| format!("{c:?}")).collect();
            list.sort();
            (table.clone(), list)
        })
        .collect();
    tables.sort();
    tables
}

/// What the walk extracts from each shape: the column constraints and
/// equivalences (each as its sorted debug lines), the per-table lists and
/// whether the WHERE was analyzed completely.
#[rustfmt::skip]
const CONSTRAINTS_EXPECTED: &[(&str, &[&str], &[&str], &str, bool)] = &[
    ("SELECT id FROM users WHERE id = 1",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: Equal, value: Integer(1) }"#],
     &[],
     r#"[("users", ["Comparison(\"id\", Equal, Integer(1))"])]"#,
     true),
    ("SELECT id FROM users WHERE 1 = id AND name = 'a'",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: Equal, value: Integer(1) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "name" }, op: Equal, value: String("a") }"#],
     &[],
     r#"[("users", ["Comparison(\"id\", Equal, Integer(1))", "Comparison(\"name\", Equal, String(\"a\"))"])]"#,
     true),
    ("SELECT id FROM users WHERE id > 1 AND id <= 9 AND name <> 'x'",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: GreaterThan, value: Integer(1) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: LessThanOrEqual, value: Integer(9) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "name" }, op: NotEqual, value: String("x") }"#],
     &[],
     r#"[("users", ["Comparison(\"id\", GreaterThan, Integer(1))", "Comparison(\"id\", LessThanOrEqual, Integer(9))", "Comparison(\"name\", NotEqual, String(\"x\"))"])]"#,
     true),
    ("SELECT id FROM users WHERE id IN (3, 1, 2, 1) AND name NOT IN ('a', 'b')",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "name" }, op: NotEqual, value: String("a") }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "name" }, op: NotEqual, value: String("b") }"#, r#"InSet { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, values: [Integer(1), Integer(2), Integer(3)] }"#],
     &[],
     r#"[("users", ["AnyOf(\"id\", [Integer(1), Integer(2), Integer(3)])", "Comparison(\"name\", NotEqual, String(\"a\"))", "Comparison(\"name\", NotEqual, String(\"b\"))"])]"#,
     true),
    ("SELECT id FROM users WHERE id BETWEEN 1 AND 5 AND name BETWEEN SYMMETRIC 'z' AND 'a'",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: GreaterThanOrEqual, value: Integer(1) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: LessThanOrEqual, value: Integer(5) }"#],
     &[],
     r#"[("users", ["Comparison(\"id\", GreaterThanOrEqual, Integer(1))", "Comparison(\"id\", LessThanOrEqual, Integer(5))"])]"#,
     false),
    ("SELECT id FROM users WHERE id = ANY (ARRAY[1, 2]) AND name = ANY ('{a,b}'::text[])",
     &[r#"InSet { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, values: [Integer(1), Integer(2)] }"#],
     &[],
     r#"[("users", ["AnyOf(\"id\", [Integer(1), Integer(2)])"])]"#,
     false),
    ("SELECT id FROM users WHERE id = ANY (ARRAY[id, 2])",
     &[],
     &[],
     "[]",
     false),
    ("SELECT id FROM users WHERE id::text = '1' AND name::int4 = 2 AND id::date = '2020-01-01'",
     &[r#"CastComparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "name" }, cast: Int4, op: Equal, value: Integer(2) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: Equal, value: String("1") }"#],
     &[],
     r#"[("users", ["CastComparison(\"name\", Int4, Equal, Integer(2))", "Comparison(\"id\", Equal, String(\"1\"))"])]"#,
     false),
    ("SELECT id FROM users WHERE id = 1 OR name = 'a'",
     &[],
     &[],
     "[]",
     false),
    ("SELECT id FROM users WHERE NOT (id = 1)",
     &[],
     &[],
     "[]",
     false),
    ("SELECT id FROM users WHERE id IN (SELECT id FROM orders)",
     &[],
     &[],
     "[]",
     false),
    ("SELECT u.id FROM users u JOIN orders o ON u.id = o.id AND o.name = 'x' WHERE u.name = 'y'",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "name" }, op: Equal, value: String("x") }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: Some("u"), column: "name" }, op: Equal, value: String("y") }"#],
     &[r#"ColumnEquivalence { left: ConstraintColumn { schema: "public", table: "users", alias: Some("u"), column: "id" }, right: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "id" } }"#],
     r#"[("orders", ["Comparison(\"name\", Equal, String(\"x\"))"]), ("users", ["Comparison(\"name\", Equal, String(\"y\"))"])]"#,
     true),
    ("SELECT u.id FROM users u JOIN orders o USING (id) WHERE id = 1",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "id" }, op: Equal, value: Integer(1) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: Some("u"), column: "id" }, op: Equal, value: Integer(1) }"#],
     &[r#"ColumnEquivalence { left: ConstraintColumn { schema: "public", table: "users", alias: Some("u"), column: "id" }, right: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "id" } }"#],
     r#"[("orders", ["Comparison(\"id\", Equal, Integer(1))"]), ("users", ["Comparison(\"id\", Equal, Integer(1))"])]"#,
     true),
    ("SELECT u.id FROM users u NATURAL JOIN orders o JOIN items i ON i.id = o.id",
     &[],
     &[r#"ColumnEquivalence { left: ConstraintColumn { schema: "public", table: "items", alias: Some("i"), column: "id" }, right: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "id" } }"#, r#"ColumnEquivalence { left: ConstraintColumn { schema: "public", table: "users", alias: Some("u"), column: "id" }, right: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "id" } }"#, r#"ColumnEquivalence { left: ConstraintColumn { schema: "public", table: "users", alias: Some("u"), column: "name" }, right: ConstraintColumn { schema: "public", table: "orders", alias: Some("o"), column: "name" } }"#],
     "[]",
     true),
    ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id CROSS JOIN items",
     &[],
     &[],
     "[]",
     true),
    ("SELECT id FROM users WHERE id NOT IN (1, name, 2)",
     &[r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: NotEqual, value: Integer(1) }"#, r#"Comparison { column: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, op: NotEqual, value: Integer(2) }"#],
     &[],
     r#"[("users", ["Comparison(\"id\", NotEqual, Integer(1))", "Comparison(\"id\", NotEqual, Integer(2))"])]"#,
     false),
    ("SELECT id FROM users WHERE id IN (1, name)",
     &[],
     &[],
     "[]",
     false),
    ("SELECT id FROM users WHERE id NOT BETWEEN 1 AND 2 AND name LIKE 'a%'",
     &[],
     &[],
     "[]",
     false),
    ("SELECT id FROM users WHERE id = name",
     &[],
     &[r#"ColumnEquivalence { left: ConstraintColumn { schema: "public", table: "users", alias: None, column: "id" }, right: ConstraintColumn { schema: "public", table: "users", alias: None, column: "name" } }"#],
     "[]",
     true),
    ("SELECT v.id FROM (SELECT id FROM users WHERE id = 1) v WHERE v.id = 2",
     &[r#"Comparison { column: ConstraintColumn { schema: "", table: "v", alias: Some("v"), column: "id" }, op: Equal, value: Integer(2) }"#],
     &[],
     r#"[("v", ["Comparison(\"id\", Equal, Integer(2))"])]"#,
     true),
];

fn sorted_debug<T: std::fmt::Debug>(items: impl IntoIterator<Item = T>) -> Vec<String> {
    let mut out: Vec<String> = items.into_iter().map(|item| format!("{item:?}")).collect();
    out.sort();
    out
}

/// Every shape the extractors handle, and the ones they give up on,
/// analyzes to its expected constraints.
#[test]
fn test_constraints_expectations() {
    let tables = catalog(&["users", "orders", "items"]);
    for (sql, columns, equivalences, per_table, complete) in CONSTRAINTS_EXPECTED {
        let actual = analyze_query_constraints(&resolve_sql(sql, &tables));
        assert_eq!(sorted_debug(&actual.column_constraints), *columns, "{sql}");
        assert_eq!(sorted_debug(&actual.equivalences), *equivalences, "{sql}");
        assert_eq!(
            format!("{:?}", table_constraints_sorted(&actual)),
            *per_table,
            "{sql}"
        );
        assert_eq!(actual.where_analysis_complete, *complete, "{sql}");
    }
}

fn constraints_of(sql: &str, tables: &Catalog) -> QueryConstraints {
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

/// Whether `table` carries exactly `expected`.
fn has_table_constraint(
    constraints: &QueryConstraints,
    table: &str,
    expected: &TableConstraint,
) -> bool {
    table_constraint_any(constraints, table, |tc| tc == expected)
}

fn has_constraint(constraints: &QueryConstraints, spec: &ComparisonSpec) -> bool {
    let expected = TableConstraint::Comparison(spec.column.into(), spec.op, spec.value.clone());
    has_table_constraint(constraints, spec.table, &expected)
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
    ]);
}

#[test]
fn test_between_symmetric_extraction_cases() {
    extraction_cases_check(vec![
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
        // A non-literal bound leaves only the literal one
        ExtractionCase {
            label: "between with non literal bounds",
            tables: &["users"],
            sql: "SELECT * FROM users WHERE id BETWEEN name AND 10",
            expectations: vec![
                count(1),
                has(cmp("users", "id", BinaryOp::LessThanOrEqual, int(10))),
            ],
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
        // A redundant ::text on a TEXT column is the same constraint
        ("identity text cast", "SELECT * FROM users WHERE name = 'alice'", "SELECT * FROM users WHERE name::text = 'alice'", true),
        // Identical cast comparisons subsume each other
        ("same cast comparison", "SELECT * FROM users WHERE name::int4 = 42", "SELECT * FROM users WHERE name::int4 = 42", true),
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

    tables.insert_overwrite(
        TableMetadata::for_test(
            "c",
            1003,
            [ColumnMetadata {
                name: "id".into(),
                position: ColumnPosition::from_raw(1),
                type_oid: TypeOid::from_raw(23),
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            }],
        )
        .into(),
    );

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

/// A table name more than one reference binds keeps no per-table
/// constraints and propagates none, which leaves the analysis incomplete;
/// every other table keeps its own (PGC-507).
#[test]
fn test_repeated_table_constraints_dropped() {
    let mut tables = catalog(&["users", "orders"]);
    let mut other = test_table_metadata("users", Oid::from_raw(1101));
    other.schema = "other".into();
    tables.insert_overwrite(other.into());
    #[rustfmt::skip]
    let cases: &[(&str, &[(&str, &[&str])], bool)] = &[
        ("SELECT a.id FROM users a JOIN users b ON b.id = a.id WHERE a.name = 'x'",
         &[], false),
        ("SELECT a.id FROM users a JOIN users b ON b.name = a.name",
         &[], true),
        // a.id = 5 must not reach orders through b.id, which shares its identity.
        ("SELECT a.id FROM users a JOIN users b ON b.id = a.name \
          JOIN orders o ON o.id = b.id WHERE a.id = 5",
         &[], false),
        ("SELECT a.id FROM users a JOIN users b ON b.id = a.id \
          JOIN orders o ON o.id = a.id WHERE o.name = 'x'",
         &[("orders", &[r#"Comparison("name", Equal, String("x"))"#])], true),
        ("SELECT p.id FROM public.users p JOIN other.users o ON o.id = p.id WHERE p.name = 'x'",
         &[], false),
        ("SELECT u.id FROM users u JOIN (SELECT id FROM orders) users ON users.id = u.id \
          WHERE u.name = 'x'",
         &[], false),
        ("SELECT u.id FROM users u JOIN orders o ON o.id = u.id WHERE u.name = 'x'",
         &[("users", &[r#"Comparison("name", Equal, String("x"))"#])], true),
    ];
    for (sql, expected, complete) in cases {
        let constraints = constraints_of(sql, &tables);
        assert_eq!(
            table_constraint_lines(&constraints),
            expected_lines(expected),
            "{sql}"
        );
        assert_eq!(constraints.where_analysis_complete, *complete, "{sql}");
    }
}

/// A subquery branch that reads an outer table of its own table's name binds
/// that name twice.
#[test]
fn test_repeated_table_through_correlation_dropped() {
    let tables = catalog(&["users", "orders"]);
    #[rustfmt::skip]
    let cases: &[(&str, &[(&str, &[&str])], bool)] = &[
        ("SELECT a.id FROM users a WHERE EXISTS \
          (SELECT 1 FROM users b WHERE b.id = a.id AND a.name = 'x' AND b.name = 'y')",
         &[], false),
        ("SELECT a.id FROM users a WHERE EXISTS \
          (SELECT 1 FROM orders o WHERE o.id = a.id AND o.name = 'y')",
         &[("orders", &[r#"Comparison("name", Equal, String("y"))"#])], true),
    ];
    for (sql, expected, complete) in cases {
        let bound = resolve_sql(sql, &tables);
        let (subquery, _) = select_branches_with_source(bound.tree().root())
            .into_iter()
            .find(|(_, source)| *source != UpdateQuerySource::FromClause)
            .expect("a subquery branch");
        let constraints = query_constraints_analyze(&bound, subquery);
        assert_eq!(
            table_constraint_lines(&constraints),
            expected_lines(expected),
            "{sql}"
        );
        assert_eq!(constraints.where_analysis_complete, *complete, "{sql}");
    }
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

/// An `id` constraint for the narrowing table.
fn id_in(values: &[i64]) -> TableConstraint {
    TableConstraint::AnyOf("id".into(), values.iter().copied().map(int).collect())
}

fn id_is(op: BinaryOp, value: i64) -> TableConstraint {
    TableConstraint::Comparison("id".into(), op, int(value))
}

/// A narrowed range spelled for comparison, its IN members sorted.
fn range_described(range: &ColumnRange) -> String {
    let short = |value: &LiteralValue| match value {
        LiteralValue::Integer(n) => n.to_string(),
        other => format!("{other:?}"),
    };
    match range {
        ColumnRange::Empty => "empty".to_owned(),
        ColumnRange::Equal(value) => format!("= {}", short(value)),
        ColumnRange::InSet(set) => {
            let mut members: Vec<String> = set.iter().map(short).collect();
            members.sort();
            format!("in [{}]", members.join(", "))
        }
        other => format!("{other:?}"),
    }
}

/// An IN list AND a comparison on the same column narrows to the values
/// that satisfy both.
#[test]
fn test_in_set_narrowing_cases() {
    #[rustfmt::skip]
    let cases = [
        ("range filter",     vec![id_in(&[1, 2, 3, 4, 5]), id_is(BinaryOp::GreaterThan, 3)], "in [4, 5]"),
        ("equality match",   vec![id_in(&[1, 2, 3]), id_is(BinaryOp::Equal, 2)],             "= 2"),
        ("equality miss",    vec![id_in(&[1, 2, 3]), id_is(BinaryOp::Equal, 5)],             "empty"),
        ("not equal",        vec![id_in(&[1, 2, 3]), id_is(BinaryOp::NotEqual, 2)],          "in [1, 3]"),
    ];
    for (label, constraints, expected) in cases {
        let refs: Vec<&TableConstraint> = constraints.iter().collect();
        assert_eq!(
            range_described(&column_range_build(&refs)),
            expected,
            "{label}"
        );
    }
}

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

/// Only the WHERE decides completeness: an ON the extractor can't read
/// leaves it complete.
#[test]
fn test_where_analysis_complete_ignores_on() {
    let tables = catalog(&["users", "orders"]);
    let cases = [
        (
            "SELECT * FROM users u LEFT JOIN orders o ON o.id = u.id AND o.name = lower(u.name)",
            true,
        ),
        (
            "SELECT * FROM users u JOIN orders o ON o.id = u.id AND o.name = lower(u.name)",
            true,
        ),
        (
            "SELECT * FROM users u JOIN orders o ON o.id = u.id WHERE u.name = lower(o.name)",
            false,
        ),
    ];
    for (sql, complete) in cases {
        let constraints = constraints_of(sql, &tables);
        assert_eq!(constraints.where_analysis_complete, complete, "{sql}");
    }
}

/// A BETWEEN, IN or NOT IN with an operand that isn't a literal filters
/// rows no extracted constraint describes.
#[test]
fn test_where_analysis_incomplete_for_non_literal_operand() {
    let tables = catalog(&["users"]);
    let cases = [
        ("SELECT * FROM users WHERE id BETWEEN 1 AND 5", true),
        ("SELECT * FROM users WHERE id BETWEEN 1 AND id", false),
        ("SELECT * FROM users WHERE id IN (1, 2)", true),
        ("SELECT * FROM users WHERE id IN (1, id)", false),
        ("SELECT * FROM users WHERE id NOT IN (1, 2)", true),
        ("SELECT * FROM users WHERE id NOT IN (id, 1)", false),
        ("SELECT * FROM users WHERE id NOT IN (1, id)", false),
    ];
    for (sql, complete) in cases {
        let constraints = constraints_of(sql, &tables);
        assert_eq!(constraints.where_analysis_complete, complete, "{sql}");
    }
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

/// A cached `a = b` loaded only the rows where the two columns agree, so it
/// is no full scan; on the new side the equality only narrows.
#[test]
fn test_subsumption_same_table_equality_cached() {
    let int_column = |name: &str, position: i16| ColumnMetadata {
        name: name.into(),
        position: ColumnPosition::from_raw(position),
        type_oid: TypeOid::from_raw(23),
        data_type: Type::INT4,
        type_name: "int4".into(),
        cache_type_name: "int4".into(),
        is_primary_key: position == 1,
    };
    let mut tables = BiHashMap::new();
    tables.insert_overwrite(
        TableMetadata::for_test(
            "items",
            1001,
            [int_column("id", 1), int_column("a", 2), int_column("b", 3)],
        )
        .into(),
    );
    let cases = [
        (
            "SELECT * FROM items WHERE a = b",
            "SELECT * FROM items WHERE id = 5",
            false,
        ),
        (
            "SELECT * FROM items WHERE a = b AND id > 0",
            "SELECT * FROM items WHERE id = 5",
            false,
        ),
        (
            "SELECT * FROM items",
            "SELECT * FROM items WHERE a = b",
            true,
        ),
        (
            "SELECT * FROM items WHERE id > 0",
            "SELECT * FROM items WHERE a = b AND id = 5",
            true,
        ),
    ];
    for (cached_sql, new_sql, expected) in cases {
        let cached = constraints_of(cached_sql, &tables);
        let new = constraints_of(new_sql, &tables);
        assert_eq!(
            table_constraints_subsumed(&new, &cached, "items"),
            expected,
            "cached {cached_sql:?} subsumes new {new_sql:?}"
        );
    }
}

// ------------------------------------------------------------------
// PGC-149: identity TypeCast strip in constraint extraction
// ------------------------------------------------------------------

fn cast_comparison(
    column: &str,
    cast: CastTarget,
    op: BinaryOp,
    value: LiteralValue,
) -> TableConstraint {
    TableConstraint::CastComparison(column.into(), cast, op, value)
}

/// A cast on a compared column: an identity cast extracts the plain
/// comparison (PGC-177), a real cast a `CastComparison` (PGC-182), and a
/// coercion the analysis cannot model leaves it incomplete.
#[test]
fn test_cast_extraction_cases() {
    // (label, sql, analysis complete, the constraint on `users`)
    #[rustfmt::skip]
    let cases = [
        ("identity text cast on text column", "SELECT * FROM users WHERE name::text = 'alice'", true,
         Some(TableConstraint::Comparison("name".into(), BinaryOp::Equal, text("alice")))),
        ("identity text cast on int column",  "SELECT * FROM users WHERE id::text = '42'", true,
         Some(TableConstraint::Comparison("id".into(), BinaryOp::Equal, text("42")))),
        ("text to int4",                      "SELECT * FROM users WHERE name::int4 = 42", true,
         Some(cast_comparison("name", CastTarget::Int4, BinaryOp::Equal, int(42)))),
        ("text to numeric",                   "SELECT * FROM users WHERE name::numeric = 42", false, None),
    ];
    for (label, sql, complete, expected) in cases {
        let constraints = constraints_of(sql, &catalog(&["users"]));
        assert_eq!(
            constraints.where_analysis_complete, complete,
            "{label}: completeness"
        );
        if let Some(expected) = expected {
            assert!(
                has_table_constraint(&constraints, "users", &expected),
                "{label}: {expected:?}"
            );
        }
    }
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
    let cast_equal_five = cast_comparison("name", CastTarget::Int4, BinaryOp::Equal, int(5));
    assert!(has_table_constraint(
        &constraints,
        "users",
        &cast_equal_five
    ));
    assert!(!has_table_constraint(
        &constraints,
        "users2",
        &cast_equal_five
    ));
}

/// Two spellings of one column through different aliases are the same
/// constraint column, by equality and by hash.
#[test]
fn test_constraint_column_identity_ignores_alias() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let column = |alias: &str| ConstraintColumn {
        schema: "public".into(),
        table: "users".into(),
        alias: Some(alias.into()),
        column: "id".into(),
    };
    let (u, u2) = (column("u"), column("u2"));
    assert_eq!(u, u2);
    let hash = |c: &ConstraintColumn| {
        let mut hasher = DefaultHasher::new();
        c.hash(&mut hasher);
        hasher.finish()
    };
    assert_eq!(hash(&u), hash(&u2));
}

/// Per-table constraints, each as its debug line, in a comparable order.
fn table_constraint_lines(constraints: &QueryConstraints) -> Vec<(String, Vec<String>)> {
    table_constraints_sorted(constraints)
        .into_iter()
        .map(|(table, list)| (table.to_string(), list))
        .collect()
}

fn expected_lines(expected: &[(&str, &[&str])]) -> Vec<(String, Vec<String>)> {
    expected
        .iter()
        .map(|(table, list)| {
            let mut list: Vec<String> = list.iter().map(|line| line.to_string()).collect();
            list.sort();
            (table.to_string(), list)
        })
        .collect()
}

/// An outer join's ON clause constrains only its nullable side, and an
/// equality there carries constraints from the preserved side only. A
/// NULL-rejecting predicate on the nullable side from WHERE, or from an
/// enclosing ON clause, reduces the join to an inner join (PGC-505).
#[test]
fn test_outer_join_constraints() {
    let tables = catalog(&["users", "orders", "items"]);
    #[rustfmt::skip]
    let cases: &[(&str, &[(&str, &[&str])])] = &[
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id AND u.name = 'x'",
         &[]),
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id AND o.id = 5",
         &[("orders", &[r#"Comparison("id", Equal, Integer(5))"#])]),
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id WHERE u.id = 5",
         &[("orders", &[r#"Comparison("id", Equal, Integer(5))"#]),
           ("users", &[r#"Comparison("id", Equal, Integer(5))"#])]),
        ("SELECT u.id FROM users u LEFT JOIN orders o USING (id) WHERE u.id = 5",
         &[("orders", &[r#"Comparison("id", Equal, Integer(5))"#]),
           ("users", &[r#"Comparison("id", Equal, Integer(5))"#])]),
        ("SELECT u.id FROM orders o RIGHT JOIN users u ON u.id = o.id AND u.name = 'x' AND o.id = 5",
         &[("orders", &[r#"Comparison("id", Equal, Integer(5))"#])]),
        ("SELECT a.id FROM users a LEFT JOIN users b ON a.id = b.id AND a.name = 'x'",
         &[]),
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id \
          LEFT JOIN items i ON o.id = i.id AND u.name = 'x'",
         &[]),
        // Reduced by WHERE: the equality carries both ways.
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id WHERE o.id = 5",
         &[("orders", &[r#"Comparison("id", Equal, Integer(5))"#]),
           ("users", &[r#"Comparison("id", Equal, Integer(5))"#])]),
        // Reduced: a preserved-side ON literal filters too.
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id AND u.name = 'x' \
          WHERE o.name = 'y'",
         &[("orders", &[r#"Comparison("name", Equal, String("y"))"#]),
           ("users", &[r#"Comparison("name", Equal, String("x"))"#])]),
        // IS NULL keeps NULL-padded rows: no reduction.
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id AND o.id = 5 \
          WHERE o.name IS NULL",
         &[("orders", &[r#"Comparison("id", Equal, Integer(5))"#])]),
        // WHERE on the innermost nullable side reduces both joins.
        ("SELECT u.id FROM users u LEFT JOIN (orders o LEFT JOIN items i ON o.id = i.id) \
          ON u.id = o.id WHERE i.id = 7",
         &[("items", &[r#"Comparison("id", Equal, Integer(7))"#]),
           ("orders", &[r#"Comparison("id", Equal, Integer(7))"#]),
           ("users", &[r#"Comparison("id", Equal, Integer(7))"#])]),
        // The enclosing ON reduces the nested join, not its own.
        ("SELECT u.id FROM users u LEFT JOIN (orders o LEFT JOIN items i ON o.id = i.id) \
          ON u.id = o.id AND i.id = 3",
         &[("items", &[r#"Comparison("id", Equal, Integer(3))"#]),
           ("orders", &[r#"Comparison("id", Equal, Integer(3))"#])]),
        // An inner join's ON above reduces the outer join below it.
        ("SELECT u.id FROM users u LEFT JOIN orders o ON u.id = o.id \
          JOIN items i ON o.id = i.id AND i.id = 2",
         &[("items", &[r#"Comparison("id", Equal, Integer(2))"#]),
           ("orders", &[r#"Comparison("id", Equal, Integer(2))"#]),
           ("users", &[r#"Comparison("id", Equal, Integer(2))"#])]),
    ];
    for (sql, expected) in cases {
        let constraints = constraints_of(sql, &tables);
        assert_eq!(
            table_constraint_lines(&constraints),
            expected_lines(expected),
            "{sql}"
        );
    }
}

/// A decorrelated NOT EXISTS is a LEFT JOIN: the subquery's literal on its
/// correlated column constrains only the subquery table.
#[test]
fn test_not_exists_literal_stays_on_subquery_table() {
    let tables = catalog(&["users", "orders"]);
    let bound = resolve_sql(
        "SELECT u.id FROM users u WHERE NOT EXISTS \
         (SELECT 1 FROM orders o WHERE o.id = u.id AND o.id = 5)",
        &tables,
    );
    let decorrelated = query_decorrelate(&bound, &HashSet::new())
        .expect("decorrelate")
        .expect("a NOT EXISTS to decorrelate");
    let constraints = analyze_query_constraints(&decorrelated);
    assert_eq!(
        table_constraint_lines(&constraints),
        expected_lines(&[("orders", &[r#"Comparison("id", Equal, Integer(5))"#])]),
    );
}
