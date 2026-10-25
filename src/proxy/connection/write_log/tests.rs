use std::collections::HashMap;
use std::sync::Arc;

use ecow::EcoString;
use ordered_float::NotNan;

use super::aggregate::{MERGED_PREDICATE_CAP, UPDATE_DELETE_PREDICATE_CAP};
use super::log::WriteLog;
use super::tiers::TableTiers;
use super::{DisjointKinds, RawBlocker, RawDecision, RawForwardReason};
use crate::cache::WriteTargetMap;
use crate::catalog::{RelationName, TableMetadata, WriteAlias};
use crate::oid::Oid;
use crate::pg::Lsn;
use crate::query::ast::rope::QueryTree;
use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::constraints::{ColumnConstraintKind, ColumnRange, TableConstraint};
use crate::query::update::{ReadRelation, ReadRelations, ReferenceRanges};
use crate::query::write::{
    DeleteStatement, InsertRow, InsertStatement, RelationRef, UpdateStatement, WriteClass,
};

fn comparison(column: &str, op: BinaryOp, value: LiteralValue) -> TableConstraint {
    TableConstraint {
        column: column.into(),
        kind: ColumnConstraintKind::Comparison { op, value },
    }
}

fn table(name: &str) -> WriteClass {
    WriteClass::Table(RelationRef {
        schema: None,
        name: name.into(),
    })
}

fn insert(name: &str) -> WriteClass {
    WriteClass::InsertRows(Arc::new(InsertStatement {
        relation: RelationRef {
            schema: None,
            name: name.into(),
        },
        columns: vec!["id".into()],
        rows: vec![InsertRow::new()],
    }))
}

/// The tiers of every pending write the bare name `relation` could denote:
/// the resolved relations of that name and the unresolved entries under it.
fn tiers_named<'a>(log: &'a WriteLog, relation: &'a str) -> impl Iterator<Item = &'a TableTiers> {
    log.pending_named(None, relation)
}

/// Whether `relation` has any pending write (opaque or insert) in any tier.
fn table_pending(log: &WriteLog, relation: &str) -> bool {
    tiers_named(log, relation)
        .flat_map(TableTiers::aggregates)
        .any(|a| a.opaque || a.inserts.is_some())
}

fn connection_pending(log: &WriteLog) -> bool {
    !log.connection.is_empty()
}

fn query(sql: &str) -> QueryTree {
    crate::query::ast::query_tree_parse(sql).expect("parse query")
}

#[test]
fn test_intersects_referenced_table_only() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    assert_eq!(
        log.decide(&query("SELECT * FROM items WHERE id = 1"), None),
        RawDecision::Serve
    );
}

#[test]
fn test_intersects_connection_scope_poisons_all_reads() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&WriteClass::Connection, None, None);
    assert_eq!(
        log.decide(&query("SELECT * FROM whatever"), None),
        RawDecision::Forward(RawForwardReason::Connection, RawBlocker::Unstamped)
    );
}

#[test]
fn test_intersects_covers_joins_and_subqueries() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    assert_eq!(
        log.decide(
            &query("SELECT * FROM users u JOIN orders o ON u.id = o.uid"),
            None
        ),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    assert_eq!(
        log.decide(
            &query("SELECT * FROM users WHERE id IN (SELECT uid FROM orders)"),
            None
        ),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    assert_eq!(
        log.decide(
            &query("SELECT * FROM users u JOIN items i ON u.id = i.uid"),
            None
        ),
        RawDecision::Serve
    );
}

#[test]
fn test_intersects_schema_matching() {
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::Table(RelationRef {
            schema: Some("sales".into()),
            name: "orders".into(),
        }),
        None,
        None,
    );
    assert_eq!(
        log.decide(&query("SELECT * FROM sales.orders"), None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    // Different schema, same name → not the same table.
    assert_eq!(
        log.decide(&query("SELECT * FROM other.orders"), None),
        RawDecision::Serve
    );
    // Unqualified read conservatively matches (search_path unresolvable).
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

fn insert_int(table: &str, col: &str, values: &[i64]) -> WriteClass {
    WriteClass::InsertRows(Arc::new(InsertStatement {
        relation: RelationRef {
            schema: None,
            name: table.into(),
        },
        columns: vec![col.into()],
        rows: values
            .iter()
            .map(|v| [Some(LiteralValue::Integer(*v))].into_iter().collect())
            .collect(),
    }))
}

/// The read's ranges: one reference of `orders` constraining `col`.
fn ranges(col: &str, range: ColumnRange) -> ReadRelations {
    relation_ranges("orders", vec![HashMap::from([(col.into(), range)])])
}

/// Every column the tests' reads may reference (`SELECT *` expands to the
/// whole table), so a pending UPDATE is never dismissed as unreferenced
/// unless a test lists the read's columns itself.
const STAR_COLUMNS: &[&str] = &[
    "id",
    "v",
    "a",
    "b",
    "k",
    "parent",
    "c0",
    "note",
    "label",
    "last_update",
    "actor_id",
    "first_name",
    "film_id",
];

/// A stable OID for a test table name.
fn test_oid(table: &str) -> Oid {
    Oid::from_raw(table.bytes().fold(7u32, |hash, byte| {
        hash.wrapping_mul(31).wrapping_add(u32::from(byte))
    }))
}

/// One relation of a registered read's form: `public.<table>`.
fn read_relation(
    table: &str,
    ranges: Option<Vec<ReferenceRanges>>,
    columns: &[&str],
) -> (Oid, ReadRelation) {
    (
        test_oid(table),
        ReadRelation {
            schema: "public".into(),
            name: table.into(),
            columns: Some(
                columns
                    .iter()
                    .map(|column| EcoString::from(*column))
                    .collect(),
            ),
            ranges,
        },
    )
}

/// The read's form: one relation, `table`, with one map per reference of it.
fn relation_ranges(table: &str, references: Vec<ReferenceRanges>) -> ReadRelations {
    ReadRelations::from_relations([read_relation(table, Some(references), STAR_COLUMNS)])
}

fn id_equal(value: i64) -> HashMap<EcoString, ColumnRange> {
    HashMap::from([(
        EcoString::from("id"),
        ColumnRange::Equal(LiteralValue::Integer(value)),
    )])
}

#[test]
fn test_intersects_insert_row_level_disjointness() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "id", &[2]), None, None);
    let q = query("SELECT * FROM orders WHERE id = 1");

    // Read on id = 5 is disjoint from the inserted id = 2 → serve, and the
    // row-level proof is recorded.
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&q, Some(&r5)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );

    // Read on id = 2 matches the inserted row → forward (no disjoint proof).
    let r2 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(2)));
    assert_eq!(
        log.decide(&q, Some(&r2)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );

    // Without the read's ranges (unregistered / multi-table) → conservative.
    assert_eq!(
        log.decide(&q, None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_intersects_insert_multi_row() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "id", &[2, 3, 4]), None, None);
    let q = query("SELECT * FROM orders");

    // 5 is outside every inserted value → disjoint.
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&q, Some(&r5)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );

    // 3 matches one inserted row → intersects.
    let r3 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(3)));
    assert_eq!(
        log.decide(&q, Some(&r3)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_intersects_insert_int_float_numeric() {
    // A pending INSERT of a float value against an integer-literal read must
    // compare by numeric value, not `LiteralValue` variant (PGC-124): `= 10`
    // is disjoint from `10.0` only when the numbers differ.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            columns: vec!["id".into()],
            rows: vec![
                [Some(LiteralValue::Float(NotNan::new(20.0).unwrap()))]
                    .into_iter()
                    .collect(),
            ],
        })),
        None,
        None,
    );
    let q = query("SELECT * FROM orders WHERE id = 10");

    // id = 10 vs inserted 20.0 → provably disjoint → serve.
    let r10 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(10)));
    assert_eq!(
        log.decide(&q, Some(&r10)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );

    // id = 20 vs inserted 20.0 → numerically equal → intersects.
    let r20 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(20)));
    assert_eq!(
        log.decide(&q, Some(&r20)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_intersects_insert_unknown_cell_forwards() {
    // A row whose predicate-column value is unknown (DEFAULT/expr) can't be
    // proven disjoint.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            columns: vec!["id".into()],
            rows: vec![[None].into_iter().collect()],
        })),
        None,
        None,
    );
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r5)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_insert_overflow_degrades_to_opaque() {
    let mut log = WriteLog::with_enabled(true);
    let first: Vec<i64> = (0..600).collect();
    let second: Vec<i64> = (600..1200).collect(); // 600 + 600 > cap
    log.record(&insert_int("orders", "id", &first), None, None);
    log.record(&insert_int("orders", "id", &second), None, None);
    // Degraded to opaque: even a disjoint read forwards.
    let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_insert_wide_rows_hit_cells_cap() {
    // 20-column rows exhaust the cell budget (8192 / 20 ≈ 409 rows) long
    // before the 1024-row cap — the memory bound, not the row count, must
    // govern wide inserts.
    fn wide_insert(row_count: usize) -> WriteClass {
        let columns: Vec<EcoString> = (0..20).map(|c| EcoString::from(format!("c{c}"))).collect();
        let rows = (0..row_count)
            .map(|r| {
                (0..20)
                    .map(|c| {
                        let cell = i64::try_from(r * 20 + c).expect("cell id fits i64");
                        Some(LiteralValue::Integer(cell))
                    })
                    .collect()
            })
            .collect();
        WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            columns,
            rows,
        }))
    }
    let mut log = WriteLog::with_enabled(true);
    log.record(&wide_insert(300), None, None);
    // Still precise below the budget.
    let r = ranges("c0", ColumnRange::Equal(LiteralValue::Integer(-1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE c0 = -1"), Some(&r)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
    // 300 + 200 = 500 rows × 20 columns = 10000 cells > 8192 → opaque.
    log.record(&wide_insert(200), None, None);
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE c0 = -1"), Some(&r)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_insert_row_at_a_time_stays_precise() {
    // The motivating workload: an ORM inserting rows one statement at a
    // time. Hundreds of single-row INSERTs must keep row-level precision
    // (the old per-statement cap degraded to opaque at 64).
    let mut log = WriteLog::with_enabled(true);
    for i in 0..500 {
        log.record(&insert_int("orders", "id", &[i]), None, None);
    }
    let q = query("SELECT * FROM orders WHERE id = 9999");
    let disjoint = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
    assert_eq!(
        log.decide(&q, Some(&disjoint)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
    let hit = ranges("id", ColumnRange::Equal(LiteralValue::Integer(250)));
    assert_eq!(
        log.decide(&q, Some(&hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_insert_diverging_column_lists() {
    // Statements with different column lists fold into one aggregate; a
    // column a row's statement didn't mention is unknown for that row and
    // can never prove disjointness.
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "a", &[1]), None, None);
    log.record(&insert_int("orders", "b", &[2]), None, None);
    let q = query("SELECT * FROM orders WHERE a = 5");
    // Read on a = 5: the a-row is excluded (1 ≠ 5) but the b-row's `a` cell
    // is unknown → forward.
    let ra = ranges("a", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&q, Some(&ra)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    // Read constraining both columns: each row excluded via its own column.
    let rab = relation_ranges(
        "orders",
        vec![HashMap::from([
            (
                EcoString::from("a"),
                ColumnRange::Equal(LiteralValue::Integer(5)),
            ),
            (
                EcoString::from("b"),
                ColumnRange::Equal(LiteralValue::Integer(5)),
            ),
        ])],
    );
    assert_eq!(
        log.decide(&q, Some(&rab)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
}

#[test]
fn test_non_insert_write_dominates_inserts() {
    // An UPDATE (opaque) after an INSERT forces table-level regardless of the
    // read's disjointness from the earlier inserted rows.
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "id", &[2]), None, None);
    log.record(&table("orders"), None, None);
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r5)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

fn delete_eq(table: &str, col: &str, value: i64) -> WriteClass {
    WriteClass::DeleteRows(Arc::new(DeleteStatement {
        relation: RelationRef {
            schema: None,
            name: table.into(),
        },
        comparisons: vec![comparison(
            col,
            BinaryOp::Equal,
            LiteralValue::Integer(value),
        )],
    }))
}

#[test]
fn test_decide_delete_predicate_disjointness() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_eq("orders", "id", 5), None, None);
    let q = query("SELECT * FROM orders WHERE id = 1");

    // Read on id = 1 is disjoint from the delete's id = 5 → serve.
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&q, Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );

    // Read on id = 5 overlaps the delete predicate → forward.
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&q, Some(&r5)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );

    // Without the read's ranges (multi-table / unregistered) → conservative.
    assert_eq!(
        log.decide(&q, None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_decide_delete_only_forwards_referenced_table() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_eq("orders", "id", 5), None, None);
    // A read of an unrelated table is unaffected.
    assert_eq!(
        log.decide(&query("SELECT * FROM items WHERE id = 5"), None),
        RawDecision::Serve
    );
}

/// A DELETE with a range comparison — the non-mergeable shape that lands in
/// the legacy per-statement predicate list.
fn delete_range(table: &str, col: &str, below: i64) -> WriteClass {
    WriteClass::DeleteRows(Arc::new(DeleteStatement {
        relation: RelationRef {
            schema: None,
            name: table.into(),
        },
        comparisons: vec![comparison(
            col,
            BinaryOp::LessThan,
            LiteralValue::Integer(below),
        )],
    }))
}

#[test]
fn test_delete_row_at_a_time_stays_precise() {
    // The motivating workload: hundreds of single-row equality DELETEs must
    // keep row-level precision (the old per-statement cap degraded at 64).
    let mut log = WriteLog::with_enabled(true);
    for i in 0..500 {
        log.record(&delete_eq("orders", "id", i), None, None);
    }
    let q = query("SELECT * FROM orders WHERE id = 9999");
    let disjoint = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
    assert_eq!(
        log.decide(&q, Some(&disjoint)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );
    let hit = ranges("id", ColumnRange::Equal(LiteralValue::Integer(250)));
    assert_eq!(
        log.decide(&q, Some(&hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_delete_merged_overflow_degrades_to_opaque() {
    let mut log = WriteLog::with_enabled(true);
    for i in 0..=MERGED_PREDICATE_CAP {
        log.record(
            &delete_eq("orders", "id", i64::try_from(i).expect("cap fits i64")),
            None,
            None,
        );
    }
    // Past the merged cap the table is opaque: even a disjoint read forwards.
    let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(999_999)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_delete_legacy_overflow_degrades_to_opaque() {
    let mut log = WriteLog::with_enabled(true);
    // Range deletes don't merge; one past the per-statement cap degrades.
    for i in 0..=UPDATE_DELETE_PREDICATE_CAP {
        log.record(
            &delete_range("orders", "id", i64::try_from(i).expect("cap fits i64")),
            None,
            None,
        );
    }
    let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(999_999)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_delete_mixed_merged_and_legacy_shapes() {
    // Equality deletes (merged) and a range delete (legacy) on one table:
    // the read must be disjoint from both stores to serve.
    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_eq("orders", "id", 5), None, None);
    log.record(&delete_range("orders", "id", 3), None, None); // id < 3
    let q = query("SELECT * FROM orders WHERE id = 10");
    // id = 10 clears both the id = 5 tuple and the id < 3 range.
    let r10 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(10)));
    assert_eq!(
        log.decide(&q, Some(&r10)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );
    // id = 2 clears the tuple but overlaps the range → forward.
    let r2 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(2)));
    assert_eq!(
        log.decide(&q, Some(&r2)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_delete_multi_column_equality_tuples() {
    // WHERE a = .. AND b = ..: tuples exclude per-statement, possibly via
    // different columns per tuple — a per-column value union would be
    // unsound, the tuple check is not.
    fn delete_ab(a: i64, b: i64) -> WriteClass {
        WriteClass::DeleteRows(Arc::new(DeleteStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            comparisons: vec![
                comparison("a", BinaryOp::Equal, LiteralValue::Integer(a)),
                comparison("b", BinaryOp::Equal, LiteralValue::Integer(b)),
            ],
        }))
    }
    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_ab(1, 10), None, None);
    log.record(&delete_ab(2, 20), None, None);
    let q = query("SELECT * FROM orders WHERE a = 2 AND b = 10");
    // a = 2 excludes the first tuple, b = 10 excludes the second.
    let r = relation_ranges(
        "orders",
        vec![HashMap::from([
            (
                EcoString::from("a"),
                ColumnRange::Equal(LiteralValue::Integer(2)),
            ),
            (
                EcoString::from("b"),
                ColumnRange::Equal(LiteralValue::Integer(10)),
            ),
        ])],
    );
    assert_eq!(
        log.decide(&q, Some(&r)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );
    // a = 2, b = 20 matches the second tuple exactly → forward.
    let r_hit = relation_ranges(
        "orders",
        vec![HashMap::from([
            (
                EcoString::from("a"),
                ColumnRange::Equal(LiteralValue::Integer(2)),
            ),
            (
                EcoString::from("b"),
                ColumnRange::Equal(LiteralValue::Integer(20)),
            ),
        ])],
    );
    assert_eq!(
        log.decide(&q, Some(&r_hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_delete_duplicate_column_routes_legacy() {
    // `id = 1 AND id = 2` is contradictory — the legacy range path folds it
    // to `Empty` (matches nothing), so any read is disjoint from it.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::DeleteRows(Arc::new(DeleteStatement {
            relation: RelationRef {
                schema: None,
                name: "orders".into(),
            },
            comparisons: vec![
                comparison("id", BinaryOp::Equal, LiteralValue::Integer(1)),
                comparison("id", BinaryOp::Equal, LiteralValue::Integer(2)),
            ],
        })),
        None,
        None,
    );
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );
}

#[test]
fn test_opaque_write_dominates_delete() {
    // A later whole-table (opaque) write forces table-level regardless of the
    // read's disjointness from an earlier delete predicate.
    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_eq("orders", "id", 5), None, None);
    log.record(&table("orders"), None, None);
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

fn update_eq(
    table: &str,
    where_col: &str,
    where_val: i64,
    set: &[(&str, Option<i64>)],
) -> WriteClass {
    WriteClass::UpdateRows(Arc::new(UpdateStatement {
        relation: RelationRef {
            schema: None,
            name: table.into(),
        },
        where_comparisons: vec![comparison(
            where_col,
            BinaryOp::Equal,
            LiteralValue::Integer(where_val),
        )],
        set: set
            .iter()
            .map(|(c, v)| ((*c).into(), v.map(LiteralValue::Integer)))
            .collect(),
    }))
}

#[test]
fn test_decide_update_disjoint_serves() {
    // UPDATE ... WHERE id = 5; a read on id = 1 is disjoint from both the
    // matched rows and their image → serve.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("orders", "id", 5, &[("v", Some(99))]),
        None,
        None,
    );
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
    // A read on the matched rows (id = 5) overlaps the WHERE → forward.
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 5"), Some(&r5)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_decide_update_grow_forwards() {
    // UPDATE ... SET id = 1 WHERE id = 5 moves a row *into* `id = 1`: the
    // WHERE is disjoint from the read, but the image is not → forward.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("orders", "id", 5, &[("id", Some(1))]),
        None,
        None,
    );
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_decide_update_value_change_forwards() {
    // UPDATE ... SET v = 99 WHERE id = 1 changes a value in the read set.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("orders", "id", 1, &[("v", Some(99))]),
        None,
        None,
    );
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_decide_update_unknown_set_disjoint_serves() {
    // A non-literal SET (unknown image) on id = 5 rows doesn't touch id = 1.
    let mut log = WriteLog::with_enabled(true);
    log.record(&update_eq("orders", "id", 5, &[("v", None)]), None, None);
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
}

#[test]
fn test_update_merged_survives_tier_collision() {
    // Saturating the waiting queue folds the incoming batch into the newest
    // tier; its merged UPDATE tuples must survive the fold — losing them
    // serves stale reads of the still-pending rows.
    let mut log = WriteLog::with_enabled(true);
    for id in 1i64..=9 {
        log.record(
            &update_eq("orders", "id", id, &[("v", Some(0))]),
            None,
            None,
        );
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(id.unsigned_abs() * 100));
    }
    // The 9th stamp folded {9} into the 800-tier under bound 900.
    let r9 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 9"), Some(&r9)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(900))
        ),
        "the id = 9 UPDATE is still pending (folded under the later bound) and must forward"
    );
    let r8 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(8)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 8"), Some(&r8)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(900))
        ),
        "the id = 8 UPDATE absorbed the fold and must survive it"
    );
    // And everything clears once the watermark passes every bound.
    log.purge(Lsn::from_raw(900));
    assert!(log.is_empty());
}

#[test]
fn test_delete_merged_survives_tier_collision() {
    let mut log = WriteLog::with_enabled(true);
    for id in 1i64..=9 {
        log.record(&delete_eq("orders", "id", id), None, None);
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(id.unsigned_abs() * 100));
    }
    let r9 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 9"), Some(&r9)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(900))
        ),
        "the id = 9 DELETE is still pending (folded under the later bound) and must forward"
    );
    let r8 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(8)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 8"), Some(&r8)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(900))
        ),
        "the id = 8 DELETE absorbed the fold and must survive it"
    );
}

#[test]
fn test_update_row_at_a_time_stays_precise() {
    // Hundreds of single-row `UPDATE ... SET v = ? WHERE id = ?` statements
    // must keep row-level precision (the old per-statement cap degraded at
    // 64).
    let mut log = WriteLog::with_enabled(true);
    for i in 0..500 {
        log.record(&update_eq("orders", "id", i, &[("v", Some(i))]), None, None);
    }
    let q = query("SELECT * FROM orders WHERE id = 9999");
    let disjoint = ranges("id", ColumnRange::Equal(LiteralValue::Integer(9999)));
    assert_eq!(
        log.decide(&q, Some(&disjoint)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
    let hit = ranges("id", ColumnRange::Equal(LiteralValue::Integer(250)));
    assert_eq!(
        log.decide(&q, Some(&hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_update_merged_grow_forwards() {
    // The grow case must survive the merged path: with many merged updates
    // pending, one whose SET moves a row *into* the read still forwards.
    let mut log = WriteLog::with_enabled(true);
    for i in 100..200 {
        log.record(&update_eq("orders", "id", i, &[("v", Some(0))]), None, None);
    }
    log.record(
        &update_eq("orders", "id", 500, &[("id", Some(1))]),
        None,
        None,
    );
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_update_set_order_shares_one_shape() {
    // `SET a = ?, b = ?` and `SET b = ?, a = ?` are the same shape; an ORM
    // iterating a hash-ordered dirty set must not fragment the store.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("orders", "id", 1, &[("a", Some(1)), ("b", Some(2))]),
        None,
        None,
    );
    log.record(
        &update_eq("orders", "id", 2, &[("b", Some(3)), ("a", Some(4))]),
        None,
        None,
    );
    let shapes: usize = log
        .tiers()
        .flat_map(TableTiers::aggregates)
        .map(|agg| agg.merged_updates.len())
        .sum();
    assert_eq!(shapes, 1, "reordered SET lists must share one shape");
    // The image check still tracks each tuple's own values.
    let ra = ranges("a", ColumnRange::Equal(LiteralValue::Integer(4)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE a = 4"), Some(&ra)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped),
        "a row updated into a = 4 must forward"
    );
}

#[test]
fn test_update_delete_share_merged_cap() {
    // Merged deletes and updates draw on one combined budget.
    let mut log = WriteLog::with_enabled(true);
    for i in 0..600 {
        log.record(&delete_eq("orders", "id", i), None, None);
    }
    for i in 0..425 {
        log.record(&update_eq("orders", "id", i, &[("v", Some(0))]), None, None);
    }
    // 600 + 425 = 1025 > cap → opaque: even a disjoint read forwards.
    let r = ranges("id", ColumnRange::Equal(LiteralValue::Integer(999_999)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_update_duplicate_set_column_routes_legacy() {
    // `SET v = 1, v = 2` (last wins) can't merge; the legacy map path keeps
    // the override semantics: the final image v = 2 is what the read must
    // be disjoint from.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("orders", "id", 5, &[("v", Some(1)), ("v", Some(2))]),
        None,
        None,
    );
    // Read on v = 1 (the overwritten value) is disjoint from the image
    // v = 2 and the WHERE id = 5 leaves v unconstrained... the read must
    // still check id: unconstrained on id → WHERE not excluded → forward.
    let rv1 = ranges("v", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE v = 1"), Some(&rv1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    // Disjoint via id on both sides → serve.
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
}

#[test]
fn test_intersects_none_after_purge() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("bound pending");
    log.stamp(seq, Lsn::from_raw(100));
    log.purge(Lsn::from_raw(100));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), None),
        RawDecision::Serve
    );
}

#[test]
fn test_record_aggregates_per_table() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    log.record(&table("orders"), None, None);
    log.record(&insert("users"), None, None);
    // Thousands would collapse the same way: one active tier per table.
    assert!(table_pending(&log, "orders"));
    assert!(table_pending(&log, "users"));
    assert!(!table_pending(&log, "items"));
}

#[test]
fn test_disabled_records_nothing() {
    let mut log = WriteLog::with_enabled(false);
    log.record(&table("orders"), None, None);
    assert!(log.is_empty());
}

#[test]
fn test_connection_scope_write() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&WriteClass::Connection, None, None);
    assert!(connection_pending(&log));
}

#[test]
fn test_stamp_then_purge_clears_table() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    // Not yet applied.
    log.purge(Lsn::from_raw(50));
    assert!(table_pending(&log, "orders"));
    // Watermark reaches the bound → clears.
    log.purge(Lsn::from_raw(100));
    assert!(log.is_empty());
}

#[test]
fn test_stamp_partial_on_race() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("bound pending");
    // A write races in after the probe sampled its bound.
    log.record(&table("items"), None, None);
    log.stamp(seq, Lsn::from_raw(100));
    // Per-table stamping: `orders` (recorded before the sample) is bounded
    // and clears; `items` (after) stays unstamped for the next probe.
    log.purge(Lsn::from_raw(1_000));
    assert!(!table_pending(&log, "orders"));
    assert!(table_pending(&log, "items"));
    // The next probe bounds `items`.
    let seq = log.stamp_seq().expect("items awaiting a bound");
    log.stamp(seq, Lsn::from_raw(2_000));
    log.purge(Lsn::from_raw(2_000));
    assert!(log.is_empty());
}

#[test]
fn test_stamp_same_table_race_holds_earlier_writes() {
    // A racing write to the SAME table keeps that table's earlier writes
    // pending too: one active tier per table, guarded by its latest seq.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("bound pending");
    log.record(&table("orders"), None, None);
    log.stamp(seq, Lsn::from_raw(100));
    log.purge(Lsn::from_raw(1_000));
    assert!(table_pending(&log, "orders"));
}

#[test]
fn test_active_waiting_separation() {
    // A fresh write must not gate an older, already-applied batch.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("bound pending");
    log.stamp(seq, Lsn::from_raw(100)); // orders waiting @ 100
    log.record(&table("items"), None, None); // items active, unstamped
    // Watermark passes the waiting tier but not the active writes.
    log.purge(Lsn::from_raw(100));
    assert!(!table_pending(&log, "orders")); // cleared independently
    assert!(table_pending(&log, "items")); // active still pending
}

#[test]
fn test_tables_clear_on_own_bounds() {
    // Per-table bounds: a table stamped low clears without waiting for a
    // table stamped high (the cross-table coupling the segmented log had).
    let mut log = WriteLog::with_enabled(true);
    for (i, tbl) in ["a", "b", "c"].iter().enumerate() {
        log.record(&table(tbl), None, None);
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw((i as u64 + 1) * 100));
    }
    log.purge(Lsn::from_raw(100));
    assert!(!table_pending(&log, "a"));
    assert!(table_pending(&log, "b"));
    assert!(table_pending(&log, "c"));
    log.purge(Lsn::from_raw(300));
    assert!(log.is_empty());
}

#[test]
fn test_waiting_tiers_drain_independently() {
    // Two stamps before anything clears occupy both waiting slots; the
    // older batch drains on its own lower bound, not the newer one's.
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "id", &[1]), None, None);
    let seq = log.stamp_seq().expect("bound pending");
    log.stamp(seq, Lsn::from_raw(100));
    log.record(&insert_int("orders", "id", &[2]), None, None);
    let seq = log.stamp_seq().expect("bound pending");
    log.stamp(seq, Lsn::from_raw(200));
    // Both inserts pending; a read of id = 1 forwards, bound by the tier it
    // intersects (the disjoint 200-tier does not raise it).
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r1)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(100))
        )
    );
    // The 100-bound batch clears alone: id = 1 now serves disjoint-free,
    // id = 2 still forwards.
    log.purge(Lsn::from_raw(100));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
    let r2 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(2)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r2)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(200))
        )
    );
    log.purge(Lsn::from_raw(200));
    assert!(log.is_empty());
}

#[test]
fn test_waiting_saturation_folds_into_newest() {
    // A stamp past the cap folds into the *newest* tier under the later
    // bound: coarsening lands on the freshest writes, while every older
    // tier keeps its anchored bound and drains on schedule (ADR-051).
    let mut log = WriteLog::with_enabled(true);
    for id in 1i64..=9 {
        log.record(&insert_int("orders", "id", &[id]), None, None);
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(id.unsigned_abs() * 100));
    }
    // The oldest batch still clears at its own bound.
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    log.purge(Lsn::from_raw(100));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        }),
        "the oldest bound must stay anchored through saturation"
    );
    // The folded pair ({8, 9} under 900) waits on the later bound.
    let r8 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(8)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r8)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(900))
        )
    );
    log.purge(Lsn::from_raw(800));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r8)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(900))
        )
    );
    log.purge(Lsn::from_raw(900));
    assert!(log.is_empty());
}

#[test]
fn test_saturation_keeps_oldest_anchored_under_sustained_stamping() {
    // The merge-oldest pathology this replaces: under continuous stamping
    // past the cap, the oldest tier's bound must never move — a reader
    // blocked on the oldest write clears as soon as settle passes *its*
    // bound, no matter how many later stamps arrive.
    let mut log = WriteLog::with_enabled(true);
    for id in 1i64..=20 {
        log.record(&insert_int("orders", "id", &[id]), None, None);
        let seq = log.stamp_seq().expect("bound pending");
        log.stamp(seq, Lsn::from_raw(id.unsigned_abs() * 100));
    }
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r1)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(100))
        ),
        "twelve stamps past saturation must not push the oldest bound"
    );
    log.purge(Lsn::from_raw(100));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r1)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
}

#[test]
fn test_unstampable_never_clears() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&WriteClass::ConnectionUnstampable, None, None);
    // A probe cannot bound a 2PC prepare.
    assert_eq!(log.stamp_seq(), None);
    log.purge(Lsn::from_raw(u64::MAX));
    assert!(connection_pending(&log));
}

#[test]
fn test_unstampable_connection_never_takes_a_bound() {
    // Defense in depth for the deleted PendingLsn::Unstampable type guard:
    // even if a future change records a connection-scoped write while
    // unstampable (today `record` refuses), a probe must not bound it.
    let mut log = WriteLog::with_enabled(true);
    log.record(&WriteClass::ConnectionUnstampable, None, None);
    log.connection.active = Some(log.next_seq);
    log.stamp(log.next_seq, Lsn::from_raw(100));
    log.purge(Lsn::from_raw(u64::MAX));
    assert!(connection_pending(&log), "2PC state must survive any probe");
}

#[test]
fn test_unstampable_drops_and_blocks_table_state() {
    // Once the connection is unstampable (2PC prepare), every read forwards
    // until close, so per-table state is unreachable — dropped, and later
    // writes aren't recorded.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    log.record(&WriteClass::ConnectionUnstampable, None, None);
    assert!(!table_pending(&log, "orders"));
    log.record(&table("items"), None, None);
    assert!(!table_pending(&log, "items"));
    assert_eq!(log.stamp_seq(), None);
    log.purge(Lsn::from_raw(u64::MAX));
    assert!(connection_pending(&log));
    assert_eq!(
        log.decide(&query("SELECT * FROM anything"), None),
        RawDecision::Forward(RawForwardReason::Connection, RawBlocker::Unstamped)
    );
}

#[test]
fn test_disable_clears_existing() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    log.disable();
    assert!(log.is_empty());
    log.record(&table("orders"), None, None);
    assert!(log.is_empty());
}

#[test]
fn test_blocker_stamped_tier_carries_its_bound() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), None),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(100))
        )
    );
}

#[test]
fn test_blocker_unstamped_dominates_stamped() {
    // A stamped tier and a fresh unstamped write both block: no watermark
    // advance can clear the unstamped one, so it must win the attribution.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    log.record(&table("orders"), None, None);
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_blocker_disjoint_tier_does_not_bind() {
    // Only the tiers the read actually intersects contribute to the blocker:
    // a later stamped tier whose rows are disjoint must not raise the bound.
    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_eq("orders", "id", 5), None, None);
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    log.record(&delete_eq("orders", "id", 7), None, None);
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(200));
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 5"), Some(&r5)),
        RawDecision::Forward(
            RawForwardReason::Table,
            RawBlocker::Stamped(Lsn::from_raw(100))
        )
    );
}

#[test]
fn test_blocker_connection_stamped_carries_its_bound() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&WriteClass::Connection, None, None);
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    assert_eq!(
        log.decide(&query("SELECT * FROM whatever"), None),
        RawDecision::Forward(
            RawForwardReason::Connection,
            RawBlocker::Stamped(Lsn::from_raw(100))
        )
    );
}

#[test]
fn test_forward_cause_classification() {
    use super::{RawForwardCause, forward_cause};
    let bound = RawBlocker::Stamped(Lsn::from_raw(100));
    // Unstamped is unstamped regardless of cursors.
    assert_eq!(
        forward_cause(RawBlocker::Unstamped, Some(Lsn::from_raw(u64::MAX))),
        RawForwardCause::Unstamped
    );
    // Bound at or below the receive cursor: the WAL is here, apply is behind.
    assert_eq!(
        forward_cause(bound, Some(Lsn::from_raw(100))),
        RawForwardCause::ApplyLag
    );
    assert_eq!(
        forward_cause(bound, Some(Lsn::from_raw(150))),
        RawForwardCause::ApplyLag
    );
    // Bound past the receive cursor: origin hasn't delivered it.
    assert_eq!(
        forward_cause(bound, Some(Lsn::from_raw(99))),
        RawForwardCause::DeliveryLag
    );
    // Cache down/restarting: nothing delivered this generation.
    assert_eq!(forward_cause(bound, None), RawForwardCause::DeliveryLag);
}

/// The read's form with the referenced columns per table and no ranges.
fn relation_columns(tables: &[(&str, &[&str])]) -> ReadRelations {
    ReadRelations::from_relations(
        tables
            .iter()
            .map(|(table, columns)| read_relation(table, None, columns)),
    )
}

#[test]
fn test_update_of_unreferenced_column_serves_read() {
    // UPDATE actor SET last_update = now() WHERE actor_id = 5 cannot change a
    // read that never looks at last_update, whatever the read's predicate:
    // an UPDATE neither adds nor removes rows, and no referenced value moves.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("actor", "actor_id", 5, &[("last_update", None)]),
        None,
        None,
    );
    let join = query(
        "SELECT fa.film_id, a.first_name FROM film_actor fa \
         JOIN actor a ON a.actor_id = fa.actor_id WHERE fa.film_id = 7",
    );
    let unreferenced = relation_columns(&[
        ("actor", &["actor_id", "first_name"]),
        ("film_actor", &["film_id", "actor_id"]),
    ]);
    assert_eq!(
        log.decide(&join, Some(&unreferenced)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
    // The same read referencing the updated column forwards.
    let referenced = relation_columns(&[
        ("actor", &["actor_id", "first_name", "last_update"]),
        ("film_actor", &["film_id", "actor_id"]),
    ]);
    assert_eq!(
        log.decide(&join, Some(&referenced)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    // Without the read's column sets nothing is proven.
    assert_eq!(
        log.decide(&join, None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

/// A relation whose referenced columns are unknown proves nothing about a
/// pending UPDATE: it forwards rather than passing as unreferenced.
#[test]
fn test_update_with_unknown_read_columns_forwards() {
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &update_eq("actor", "actor_id", 5, &[("last_update", None)]),
        None,
        None,
    );
    let read = query("SELECT first_name FROM actor WHERE actor_id = 9");
    let unknown = ReadRelations::from_relations([(
        test_oid("actor"),
        ReadRelation {
            schema: "public".into(),
            name: "actor".into(),
            columns: None,
            ranges: None,
        },
    )]);
    assert_eq!(
        log.decide(&read, Some(&unknown)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_update_of_unreferenced_column_serves_read_for_range_shape() {
    // The non-mergeable (range WHERE) UPDATE path applies the same rule.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::UpdateRows(Arc::new(UpdateStatement {
            relation: RelationRef {
                schema: None,
                name: "actor".into(),
            },
            where_comparisons: vec![comparison(
                "actor_id",
                BinaryOp::GreaterThan,
                LiteralValue::Integer(5),
            )],
            set: vec![("last_update".into(), None)],
        })),
        None,
        None,
    );
    let read = query("SELECT first_name FROM actor WHERE actor_id = 9");
    let unreferenced = relation_columns(&[("actor", &["actor_id", "first_name"])]);
    assert_eq!(
        log.decide(&read, Some(&unreferenced)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
    let referenced = relation_columns(&[("actor", &["actor_id", "last_update"])]);
    assert_eq!(
        log.decide(&read, Some(&referenced)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

// ==================== write targets (PGC-447, PGC-542) ====================

/// A map in which the cached table `public.<table>` (OID 1) is written as
/// itself or through `alias` (in `public`).
fn alias_map(table: &str, alias: &str, is_view: bool) -> WriteTargetMap {
    let mut metadata = TableMetadata::for_test(table, 1, std::iter::empty());
    metadata.write_aliases.push(WriteAlias {
        relation: RelationName {
            schema: "public".into(),
            name: alias.into(),
        },
        is_view,
    });
    WriteTargetMap::from_tables(std::iter::once(&metadata))
}

fn table_opaque(log: &WriteLog, relation: &str) -> bool {
    tiers_named(log, relation)
        .flat_map(TableTiers::aggregates)
        .any(|a| a.opaque)
}

/// An insert into a partition is recorded under the cached root as well,
/// rows intact, so a read of the root forwards (PGC-447).
#[test]
fn test_partition_write_records_under_cached_root_with_rows() {
    let map = alias_map("t", "t_lo", false);
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert("t_lo"), Some(&map), None);
    assert!(table_pending(&log, "t_lo"));
    assert!(table_pending(&log, "t"), "the root must carry the write");
    assert!(!table_opaque(&log, "t"), "a partition alias keeps the rows");
    assert!(matches!(
        log.decide(&query("SELECT id FROM t"), None),
        RawDecision::Forward(RawForwardReason::Table, _)
    ));
}

/// A write through a view is recorded as opaque on the base table: its
/// columns need not be the table's.
#[test]
fn test_view_write_records_opaque_on_base_table() {
    let map = alias_map("t", "v", true);
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert("v"), Some(&map), None);
    assert!(table_opaque(&log, "t"));
    assert!(
        table_pending(&log, "v"),
        "the view's name still matches a relation cached later"
    );
}

/// A schema-qualified write expands only through an alias in that schema; an
/// unqualified one through any.
#[test]
fn test_alias_expansion_matches_schema_like_the_read_side() {
    let map = alias_map("t", "t_lo", false);
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::Table(RelationRef {
            schema: Some("other".into()),
            name: "t_lo".into(),
        }),
        Some(&map),
        None,
    );
    assert!(!table_pending(&log, "t"), "other.t_lo is not the alias");
    log.record(
        &WriteClass::Table(RelationRef {
            schema: Some("public".into()),
            name: "t_lo".into(),
        }),
        Some(&map),
        None,
    );
    assert!(table_pending(&log, "t"));
}

/// Without a map (cache down) a write is recorded under its own name only.
#[test]
fn test_write_without_alias_map_records_own_name_only() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert("t_lo"), None, None);
    assert!(table_pending(&log, "t_lo"));
    assert!(!table_pending(&log, "t"));
}

/// PGC-512: a read with several references of the written table is disjoint
/// from a pending write only when every reference is; a reference without
/// filters proves nothing.
#[test]
fn test_intersects_every_table_reference() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "id", &[2]), None, None);
    let q = query("SELECT * FROM orders a JOIN orders b ON b.id = a.parent WHERE a.id = 5");
    let disjoint = relation_ranges("orders", vec![id_equal(5), id_equal(7)]);
    assert_eq!(
        log.decide(&q, Some(&disjoint)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
    let hit = relation_ranges("orders", vec![id_equal(5), id_equal(2)]);
    assert_eq!(
        log.decide(&q, Some(&hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    let unfiltered = relation_ranges("orders", vec![id_equal(5), HashMap::new()]);
    assert_eq!(
        log.decide(&q, Some(&unfiltered)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

/// PGC-512: a join read's ranges are per relation: the written relation's
/// own references decide; one without ranges proves nothing.
#[test]
fn test_intersects_join_read_per_table() {
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert_int("orders", "id", &[2]), None, None);
    let q = query("SELECT * FROM users u JOIN orders o ON u.id = o.uid WHERE o.id = 5");
    let filtered = ReadRelations::from_relations([
        read_relation("orders", Some(vec![id_equal(5)]), STAR_COLUMNS),
        read_relation("users", None, STAR_COLUMNS),
    ]);
    assert_eq!(
        log.decide(&q, Some(&filtered)),
        RawDecision::ServeDisjoint(DisjointKinds {
            insert: true,
            ..Default::default()
        })
    );
    let unfiltered = ReadRelations::from_relations([
        read_relation("orders", None, STAR_COLUMNS),
        read_relation("users", Some(vec![id_equal(5)]), STAR_COLUMNS),
    ]);
    assert_eq!(
        log.decide(&q, Some(&unfiltered)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    let empty = relation_ranges("orders", Vec::new());
    assert_eq!(
        log.decide(&q, Some(&empty)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

/// PGC-512: merged UPDATE tuples and DELETE tuples are checked against every
/// reference of the written table too.
#[test]
fn test_update_delete_intersect_every_table_reference() {
    let q = query("SELECT * FROM orders a JOIN orders b ON b.id = a.parent WHERE a.id = 5");
    let disjoint = relation_ranges("orders", vec![id_equal(5), id_equal(7)]);
    let hit = relation_ranges("orders", vec![id_equal(5), id_equal(2)]);

    let mut log = WriteLog::with_enabled(true);
    log.record(&update_eq("orders", "id", 2, &[("v", Some(1))]), None, None);
    assert_eq!(
        log.decide(&q, Some(&disjoint)),
        RawDecision::ServeDisjoint(DisjointKinds {
            update: true,
            ..Default::default()
        })
    );
    assert_eq!(
        log.decide(&q, Some(&hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );

    let mut log = WriteLog::with_enabled(true);
    log.record(&delete_eq("orders", "id", 2), None, None);
    assert_eq!(
        log.decide(&q, Some(&disjoint)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );
    assert_eq!(
        log.decide(&q, Some(&hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

// ==================== relation resolution (PGC-542) ====================

/// Cached `orders` tables in `schemas`, OIDs 1.., as the writer publishes
/// them.
fn orders_map(schemas: &[&str]) -> WriteTargetMap {
    let tables: Vec<TableMetadata> = schemas
        .iter()
        .enumerate()
        .map(|(index, schema)| {
            let oid = u32::try_from(index + 1).expect("few schemas");
            let mut table = TableMetadata::for_test("orders", oid, std::iter::empty());
            table.schema = (*schema).into();
            table
        })
        .collect();
    WriteTargetMap::from_tables(tables.iter())
}

/// The registered form of a read of `schema.orders` (OID `oid`) with one map
/// per reference of it, or unfiltered.
fn orders_relation(schema: &str, oid: u32, ranges: Option<Vec<ReferenceRanges>>) -> ReadRelations {
    ReadRelations::from_relations([(
        Oid::from_raw(oid),
        ReadRelation {
            schema: schema.into(),
            name: "orders".into(),
            columns: Some(STAR_COLUMNS.iter().map(|c| EcoString::from(*c)).collect()),
            ranges,
        },
    )])
}

/// The registered form of an unfiltered read of `schema.orders` (OID `oid`).
fn orders_read(schema: &str, oid: u32) -> ReadRelations {
    orders_relation(schema, oid, None)
}

fn path(schemas: &[&str]) -> Arc<[EcoString]> {
    schemas
        .iter()
        .map(|schema| EcoString::from(*schema))
        .collect()
}

fn forwarded(decision: &RawDecision) -> bool {
    matches!(decision, RawDecision::Forward(RawForwardReason::Table, _))
}

/// A qualified write is recorded against the relation of that schema alone;
/// an unregistered read is still matched by the name it types.
#[test]
fn test_qualified_write_resolves_to_its_schema() {
    let map = orders_map(&["s1", "s2"]);
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &WriteClass::Table(RelationRef {
            schema: Some("s1".into()),
            name: "orders".into(),
        }),
        Some(&map),
        None,
    );
    let q = query("SELECT * FROM orders");
    assert!(forwarded(&log.decide(&q, Some(&orders_read("s1", 1)))));
    assert_eq!(
        log.decide(&q, Some(&orders_read("s2", 2))),
        RawDecision::Serve,
        "the other schema's relation is untouched"
    );
    assert!(forwarded(
        &log.decide(&query("SELECT * FROM s1.orders"), None)
    ));
    assert_eq!(
        log.decide(&query("SELECT * FROM s2.orders"), None),
        RawDecision::Serve
    );
    assert!(
        forwarded(&log.decide(&q, None)),
        "an unqualified unregistered read may denote s1.orders"
    );
}

/// An unqualified write is recorded under every cached candidate on the
/// path, since origin's pick among them (the first schema holding the name
/// that the role has USAGE on) is not visible to the proxy; under all of
/// them when the path is unknown.
#[test]
fn test_unqualified_write_records_every_candidate_on_the_path() {
    let map = orders_map(&["s1", "s2", "s3"]);
    let q = query("SELECT * FROM orders");
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), Some(&map), Some(&path(&["s1", "s2"])));
    assert!(forwarded(&log.decide(&q, Some(&orders_read("s1", 1)))));
    assert!(forwarded(&log.decide(&q, Some(&orders_read("s2", 2)))));
    assert_eq!(
        log.decide(&q, Some(&orders_read("s3", 3))),
        RawDecision::Serve,
        "a schema off the path cannot be the target"
    );
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), Some(&map), None);
    for (schema, oid) in [("s1", 1), ("s2", 2), ("s3", 3)] {
        assert!(
            forwarded(&log.decide(&q, Some(&orders_read(schema, oid)))),
            "unknown path: {schema}"
        );
    }
}

/// A write whose name resolves to no cached relation, or whose real target
/// may be an uncached relation earlier on the path, is kept by name: a
/// relation of that name cached after it (its population snapshot may
/// predate the write's commit) still forwards. One already cached at the
/// write, which origin could only have reached as a recorded candidate or
/// not at all, does not match the name entry.
#[test]
fn test_unresolved_write_matches_relations_cached_later() {
    let map = orders_map(&["s2"]);
    let q = query("SELECT * FROM orders");
    // s1 precedes the cached s2 on the path: origin may have written an
    // uncached s1.orders.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), Some(&map), Some(&path(&["s1", "s2"])));
    assert!(
        forwarded(&log.decide(&q, Some(&orders_read("s2", 1)))),
        "the cached candidate"
    );
    assert!(
        forwarded(&log.decide(&q, Some(&orders_read("s1", 9)))),
        "s1.orders cached after the write"
    );
    assert_eq!(
        log.decide(&q, Some(&orders_read("s3", 8))),
        RawDecision::Serve,
        "s3 is off the path"
    );
    // Without s2 on the path the known s2.orders is no candidate and no
    // later-cached relation either.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), Some(&map), Some(&path(&["s1"])));
    assert_eq!(
        log.decide(&q, Some(&orders_read("s2", 1))),
        RawDecision::Serve
    );
    assert!(forwarded(&log.decide(&q, Some(&orders_read("s1", 9)))));
    // No map at all (cache down): any relation of the name.
    let mut log = WriteLog::with_enabled(true);
    log.record(&table("orders"), None, None);
    assert!(forwarded(&log.decide(&q, Some(&orders_read("s1", 9)))));
    assert!(forwarded(&log.decide(&q, Some(&orders_read("s2", 1)))));
}

/// The name entry of an unqualified write keeps the write's rows only when
/// no cached relation does. With a candidate recorded it marks the name
/// opaque: a relation cached later forwards, while the candidate keeps its
/// row-level precision.
#[test]
fn test_name_entry_keeps_rows_only_for_an_unresolved_write() {
    let map = orders_map(&["s1"]);
    let disjoint = |schema: &str, oid: u32| orders_relation(schema, oid, Some(vec![id_equal(7)]));
    let q = query("SELECT * FROM orders WHERE id = 7");
    let served = RawDecision::ServeDisjoint(DisjointKinds {
        insert: true,
        ..Default::default()
    });
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &insert_int("orders", "id", &[5]),
        Some(&map),
        Some(&path(&["s0", "s1"])),
    );
    assert_eq!(
        log.decide(&q, Some(&disjoint("s1", 1))),
        served,
        "the candidate keeps the rows"
    );
    assert!(
        forwarded(&log.decide(&q, Some(&disjoint("s0", 9)))),
        "a relation cached later matches the opaque name entry"
    );
    // With no candidate the name entry is the write's only record.
    let mut log = WriteLog::with_enabled(true);
    log.record(
        &insert_int("orders", "id", &[5]),
        Some(&map),
        Some(&path(&["s0"])),
    );
    assert_eq!(log.decide(&q, Some(&disjoint("s0", 9))), served);
    let hit = orders_relation("s0", 9, Some(vec![id_equal(5)]));
    assert!(forwarded(&log.decide(
        &query("SELECT * FROM orders WHERE id = 5"),
        Some(&hit)
    )));
}

/// An alias resolves along the path like a table: a partition in `public`
/// is no candidate for a write run under a path without `public`.
#[test]
fn test_alias_resolves_along_the_path() {
    let map = alias_map("t", "t_lo", false);
    let mut log = WriteLog::with_enabled(true);
    log.record(&insert("t_lo"), Some(&map), Some(&path(&["other"])));
    assert!(!table_pending(&log, "t"), "public.t_lo is off the path");
    log.record(
        &insert("t_lo"),
        Some(&map),
        Some(&path(&["other", "public"])),
    );
    assert!(table_pending(&log, "t"));
}
