use super::aggregate::{MERGED_PREDICATE_CAP, UPDATE_DELETE_PREDICATE_CAP};
use super::log::WriteLog;
use super::tiers::TableTiers;
use super::{DisjointKinds, RawBlocker, RawDecision, RawForwardReason};
use crate::pg::Lsn;
use crate::query::ast::{BinaryOp, LiteralValue, QueryExpr};
use crate::query::constraints::ColumnRange;
use crate::query::write::{
    DeleteStatement, InsertRow, InsertStatement, RelationRef, UpdateStatement, WriteClass,
};
use ecow::EcoString;
use ordered_float::NotNan;
use std::collections::HashMap;
use std::sync::Arc;

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

/// Whether `relation` has any pending write (opaque or insert) in any tier.
fn table_pending(log: &WriteLog, relation: &str) -> bool {
    log.tables.get(relation).is_some_and(|bucket| {
        bucket
            .values()
            .flat_map(TableTiers::aggregates)
            .any(|a| a.opaque || a.inserts.is_some())
    })
}

fn connection_pending(log: &WriteLog) -> bool {
    !log.connection.is_empty()
}

fn query(sql: &str) -> QueryExpr {
    crate::query::ast::query_expr_parse(sql).expect("parse query")
}

#[test]
fn test_intersects_referenced_table_only() {
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::Connection);
    assert_eq!(
        log.decide(&query("SELECT * FROM whatever"), None),
        RawDecision::Forward(RawForwardReason::Connection, RawBlocker::Unstamped)
    );
}

#[test]
fn test_intersects_covers_joins_and_subqueries() {
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::Table(RelationRef {
        schema: Some("sales".into()),
        name: "orders".into(),
    }));
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

fn ranges(col: &str, range: ColumnRange) -> HashMap<EcoString, ColumnRange> {
    HashMap::from([(col.into(), range)])
}

#[test]
fn test_intersects_insert_row_level_disjointness() {
    let mut log = WriteLog::new(true);
    log.record(&insert_int("orders", "id", &[2]));
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
    let mut log = WriteLog::new(true);
    log.record(&insert_int("orders", "id", &[2, 3, 4]));
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::InsertRows(Arc::new(InsertStatement {
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
    })));
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::InsertRows(Arc::new(InsertStatement {
        relation: RelationRef {
            schema: None,
            name: "orders".into(),
        },
        columns: vec!["id".into()],
        rows: vec![[None].into_iter().collect()],
    })));
    let r5 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), Some(&r5)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_insert_overflow_degrades_to_opaque() {
    let mut log = WriteLog::new(true);
    let first: Vec<i64> = (0..600).collect();
    let second: Vec<i64> = (600..1200).collect(); // 600 + 600 > cap
    log.record(&insert_int("orders", "id", &first));
    log.record(&insert_int("orders", "id", &second));
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
    let mut log = WriteLog::new(true);
    log.record(&wide_insert(300));
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
    log.record(&wide_insert(200));
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
    let mut log = WriteLog::new(true);
    for i in 0..500 {
        log.record(&insert_int("orders", "id", &[i]));
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
    let mut log = WriteLog::new(true);
    log.record(&insert_int("orders", "a", &[1]));
    log.record(&insert_int("orders", "b", &[2]));
    let q = query("SELECT * FROM orders WHERE a = 5");
    // Read on a = 5: the a-row is excluded (1 ≠ 5) but the b-row's `a` cell
    // is unknown → forward.
    let ra = ranges("a", ColumnRange::Equal(LiteralValue::Integer(5)));
    assert_eq!(
        log.decide(&q, Some(&ra)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
    // Read constraining both columns: each row excluded via its own column.
    let rab = HashMap::from([
        (
            EcoString::from("a"),
            ColumnRange::Equal(LiteralValue::Integer(5)),
        ),
        (
            EcoString::from("b"),
            ColumnRange::Equal(LiteralValue::Integer(5)),
        ),
    ]);
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
    let mut log = WriteLog::new(true);
    log.record(&insert_int("orders", "id", &[2]));
    log.record(&table("orders"));
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
        comparisons: vec![(col.into(), BinaryOp::Equal, LiteralValue::Integer(value))],
    }))
}

#[test]
fn test_decide_delete_predicate_disjointness() {
    let mut log = WriteLog::new(true);
    log.record(&delete_eq("orders", "id", 5));
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
    let mut log = WriteLog::new(true);
    log.record(&delete_eq("orders", "id", 5));
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
        comparisons: vec![(col.into(), BinaryOp::LessThan, LiteralValue::Integer(below))],
    }))
}

#[test]
fn test_delete_row_at_a_time_stays_precise() {
    // The motivating workload: hundreds of single-row equality DELETEs must
    // keep row-level precision (the old per-statement cap degraded at 64).
    let mut log = WriteLog::new(true);
    for i in 0..500 {
        log.record(&delete_eq("orders", "id", i));
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
    let mut log = WriteLog::new(true);
    for i in 0..=MERGED_PREDICATE_CAP {
        log.record(&delete_eq(
            "orders",
            "id",
            i64::try_from(i).expect("cap fits i64"),
        ));
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
    let mut log = WriteLog::new(true);
    // Range deletes don't merge; one past the per-statement cap degrades.
    for i in 0..=UPDATE_DELETE_PREDICATE_CAP {
        log.record(&delete_range(
            "orders",
            "id",
            i64::try_from(i).expect("cap fits i64"),
        ));
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
    let mut log = WriteLog::new(true);
    log.record(&delete_eq("orders", "id", 5));
    log.record(&delete_range("orders", "id", 3)); // id < 3
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
                ("a".into(), BinaryOp::Equal, LiteralValue::Integer(a)),
                ("b".into(), BinaryOp::Equal, LiteralValue::Integer(b)),
            ],
        }))
    }
    let mut log = WriteLog::new(true);
    log.record(&delete_ab(1, 10));
    log.record(&delete_ab(2, 20));
    let q = query("SELECT * FROM orders WHERE a = 2 AND b = 10");
    // a = 2 excludes the first tuple, b = 10 excludes the second.
    let r = HashMap::from([
        (
            EcoString::from("a"),
            ColumnRange::Equal(LiteralValue::Integer(2)),
        ),
        (
            EcoString::from("b"),
            ColumnRange::Equal(LiteralValue::Integer(10)),
        ),
    ]);
    assert_eq!(
        log.decide(&q, Some(&r)),
        RawDecision::ServeDisjoint(DisjointKinds {
            delete: true,
            ..Default::default()
        })
    );
    // a = 2, b = 20 matches the second tuple exactly → forward.
    let r_hit = HashMap::from([
        (
            EcoString::from("a"),
            ColumnRange::Equal(LiteralValue::Integer(2)),
        ),
        (
            EcoString::from("b"),
            ColumnRange::Equal(LiteralValue::Integer(20)),
        ),
    ]);
    assert_eq!(
        log.decide(&q, Some(&r_hit)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_delete_duplicate_column_routes_legacy() {
    // `id = 1 AND id = 2` is contradictory — the legacy range path folds it
    // to `Empty` (matches nothing), so any read is disjoint from it.
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::DeleteRows(Arc::new(DeleteStatement {
        relation: RelationRef {
            schema: None,
            name: "orders".into(),
        },
        comparisons: vec![
            ("id".into(), BinaryOp::Equal, LiteralValue::Integer(1)),
            ("id".into(), BinaryOp::Equal, LiteralValue::Integer(2)),
        ],
    })));
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
    let mut log = WriteLog::new(true);
    log.record(&delete_eq("orders", "id", 5));
    log.record(&table("orders"));
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
        where_comparisons: vec![(
            where_col.into(),
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
    let mut log = WriteLog::new(true);
    log.record(&update_eq("orders", "id", 5, &[("v", Some(99))]));
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
    let mut log = WriteLog::new(true);
    log.record(&update_eq("orders", "id", 5, &[("id", Some(1))]));
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_decide_update_value_change_forwards() {
    // UPDATE ... SET v = 99 WHERE id = 1 changes a value in the read set.
    let mut log = WriteLog::new(true);
    log.record(&update_eq("orders", "id", 1, &[("v", Some(99))]));
    let r1 = ranges("id", ColumnRange::Equal(LiteralValue::Integer(1)));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders WHERE id = 1"), Some(&r1)),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_decide_update_unknown_set_disjoint_serves() {
    // A non-literal SET (unknown image) on id = 5 rows doesn't touch id = 1.
    let mut log = WriteLog::new(true);
    log.record(&update_eq("orders", "id", 5, &[("v", None)]));
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
    let mut log = WriteLog::new(true);
    for id in 1i64..=9 {
        log.record(&update_eq("orders", "id", id, &[("v", Some(0))]));
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
    let mut log = WriteLog::new(true);
    for id in 1i64..=9 {
        log.record(&delete_eq("orders", "id", id));
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
    let mut log = WriteLog::new(true);
    for i in 0..500 {
        log.record(&update_eq("orders", "id", i, &[("v", Some(i))]));
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
    let mut log = WriteLog::new(true);
    for i in 100..200 {
        log.record(&update_eq("orders", "id", i, &[("v", Some(0))]));
    }
    log.record(&update_eq("orders", "id", 500, &[("id", Some(1))]));
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
    let mut log = WriteLog::new(true);
    log.record(&update_eq(
        "orders",
        "id",
        1,
        &[("a", Some(1)), ("b", Some(2))],
    ));
    log.record(&update_eq(
        "orders",
        "id",
        2,
        &[("b", Some(3)), ("a", Some(4))],
    ));
    let shapes: usize = log
        .tables
        .values()
        .flat_map(HashMap::values)
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
    let mut log = WriteLog::new(true);
    for i in 0..600 {
        log.record(&delete_eq("orders", "id", i));
    }
    for i in 0..425 {
        log.record(&update_eq("orders", "id", i, &[("v", Some(0))]));
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
    let mut log = WriteLog::new(true);
    log.record(&update_eq(
        "orders",
        "id",
        5,
        &[("v", Some(1)), ("v", Some(2))],
    ));
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    log.record(&table("orders"));
    log.record(&insert("users"));
    // Thousands would collapse the same way: one active tier per table.
    assert!(table_pending(&log, "orders"));
    assert!(table_pending(&log, "users"));
    assert!(!table_pending(&log, "items"));
}

#[test]
fn test_disabled_records_nothing() {
    let mut log = WriteLog::new(false);
    log.record(&table("orders"));
    assert!(log.is_empty());
}

#[test]
fn test_connection_scope_write() {
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::Connection);
    assert!(connection_pending(&log));
}

#[test]
fn test_stamp_then_purge_clears_table() {
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    let seq = log.stamp_seq().expect("bound pending");
    // A write races in after the probe sampled its bound.
    log.record(&table("items"));
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    let seq = log.stamp_seq().expect("bound pending");
    log.record(&table("orders"));
    log.stamp(seq, Lsn::from_raw(100));
    log.purge(Lsn::from_raw(1_000));
    assert!(table_pending(&log, "orders"));
}

#[test]
fn test_active_waiting_separation() {
    // A fresh write must not gate an older, already-applied batch.
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    let seq = log.stamp_seq().expect("bound pending");
    log.stamp(seq, Lsn::from_raw(100)); // orders waiting @ 100
    log.record(&table("items")); // items active, unstamped
    // Watermark passes the waiting tier but not the active writes.
    log.purge(Lsn::from_raw(100));
    assert!(!table_pending(&log, "orders")); // cleared independently
    assert!(table_pending(&log, "items")); // active still pending
}

#[test]
fn test_tables_clear_on_own_bounds() {
    // Per-table bounds: a table stamped low clears without waiting for a
    // table stamped high (the cross-table coupling the segmented log had).
    let mut log = WriteLog::new(true);
    for (i, tbl) in ["a", "b", "c"].iter().enumerate() {
        log.record(&table(tbl));
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
    let mut log = WriteLog::new(true);
    log.record(&insert_int("orders", "id", &[1]));
    let seq = log.stamp_seq().expect("bound pending");
    log.stamp(seq, Lsn::from_raw(100));
    log.record(&insert_int("orders", "id", &[2]));
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
    let mut log = WriteLog::new(true);
    for id in 1i64..=9 {
        log.record(&insert_int("orders", "id", &[id]));
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
    let mut log = WriteLog::new(true);
    for id in 1i64..=20 {
        log.record(&insert_int("orders", "id", &[id]));
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::ConnectionUnstampable);
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::ConnectionUnstampable);
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    log.record(&WriteClass::ConnectionUnstampable);
    assert!(!table_pending(&log, "orders"));
    log.record(&table("items"));
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    log.disable();
    assert!(log.is_empty());
    assert!(!log.is_enabled());
    log.record(&table("orders"));
    assert!(log.is_empty());
}

#[test]
fn test_blocker_stamped_tier_carries_its_bound() {
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
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
    let mut log = WriteLog::new(true);
    log.record(&table("orders"));
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    log.record(&table("orders"));
    assert_eq!(
        log.decide(&query("SELECT * FROM orders"), None),
        RawDecision::Forward(RawForwardReason::Table, RawBlocker::Unstamped)
    );
}

#[test]
fn test_blocker_disjoint_tier_does_not_bind() {
    // Only the tiers the read actually intersects contribute to the blocker:
    // a later stamped tier whose rows are disjoint must not raise the bound.
    let mut log = WriteLog::new(true);
    log.record(&delete_eq("orders", "id", 5));
    let seq = log.stamp_seq().expect("active awaiting a bound");
    log.stamp(seq, Lsn::from_raw(100));
    log.record(&delete_eq("orders", "id", 7));
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
    let mut log = WriteLog::new(true);
    log.record(&WriteClass::Connection);
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
