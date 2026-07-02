#![allow(clippy::wildcard_enum_match_arm)]

#[cfg(test)]
use crate::catalog::Oid;
use crate::pg::protocol::ByteString;
use crate::query::ast::{BinaryOp, LiteralValue, UnaryOp};
use crate::query::cast::{
    CastTarget, canonicalize_comparison, cast_target_coerce_text,
    cast_target_is_coercion_supported, is_canonical_date_literal,
};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedScalarExpr, ResolvedWhereExpr,
};

/// Recursively evaluate a resolved WHERE expression against a single row.
///
/// `row_data` is the set of column values for a row from the table named `table_name`.
/// Column references targeting other tables are treated as NULL/unknown (returns false
/// from comparisons), matching the prior unresolved evaluator's behavior for columns
/// not present in the passed-in table metadata.
pub fn where_expr_evaluate(
    expr: &ResolvedWhereExpr,
    row_data: &[Option<ByteString>],
    table_name: &str,
) -> bool {
    match expr {
        ResolvedWhereExpr::Binary(binary_expr) => match binary_expr.op {
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => {
                expr_comparison_evaluate(binary_expr, row_data, table_name)
            }
            BinaryOp::And => {
                where_expr_evaluate(&binary_expr.lexpr, row_data, table_name)
                    && where_expr_evaluate(&binary_expr.rexpr, row_data, table_name)
            }
            BinaryOp::Or => {
                where_expr_evaluate(&binary_expr.lexpr, row_data, table_name)
                    || where_expr_evaluate(&binary_expr.rexpr, row_data, table_name)
            }
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => {
                // Pattern matching not yet supported
                false
            }
        },
        ResolvedWhereExpr::Unary(unary_expr) => {
            unary_expr_evaluate(&unary_expr.op, &unary_expr.expr, row_data, table_name)
        }
        _ => {
            // Unsupported expression types: Value, Column, Multi, Array, Function, Subquery
            false
        }
    }
}

// `canonicalize_comparison` now lives in `cast.rs` so constraint analysis can
// share the same column-LHS canonical form (including identity-strip and
// op_flip semantics).

/// Evaluate a comparison expression (column op value) against row data.
fn expr_comparison_evaluate(
    binary_expr: &ResolvedBinaryExpr,
    row_data: &[Option<ByteString>],
    table_name: &str,
) -> bool {
    let Some((column_ref, target, op, value)) = canonicalize_comparison(binary_expr) else {
        return false;
    };

    let row_value = column_row_value_get(column_ref, row_data, table_name);

    match (target, row_value) {
        (None, ColumnRowValue::Present(row_value_str)) => {
            where_value_compare_string(value, row_value_str, op)
        }
        (Some(target), ColumnRowValue::Present(row_value_str)) => {
            let Some(coerced) = cast_target_coerce_text(target, row_value_str) else {
                return false;
            };
            literal_compare(&coerced, op, value)
        }
        (_, ColumnRowValue::Null) => {
            // For equality check if filter is also NULL; for other
            // comparisons NULL always returns false (SQL semantics).
            // A casted NULL is still NULL — `NULL::int = 5` is NULL/false.
            target.is_none() && matches!(op, BinaryOp::Equal) && matches!(value, LiteralValue::Null)
        }
        (_, ColumnRowValue::NotInTable) => false,
    }
}

/// Compare two typed `LiteralValue`s. Used by the cast-coercion path: the
/// row's text has been coerced to a typed `LiteralValue` and now needs to be
/// compared against the literal from the predicate.
///
/// Supported pairings: Integer↔Integer, Integer↔string-of-int,
/// Boolean↔Boolean, Boolean↔string-of-bool (equality only). Other pairings
/// return false.
pub fn literal_compare(left: &LiteralValue, op: BinaryOp, right: &LiteralValue) -> bool {
    // Boolean comparisons: equality only — matches `where_value_compare_string`
    // semantics and ORM usage of `::bool`.
    if let Some((a, b)) = literal_pair_as_bool(left, right) {
        return match op {
            BinaryOp::Equal => a == b,
            BinaryOp::NotEqual => a != b,
            _ => false,
        };
    }

    let ordering = match (left, right) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => a.cmp(b),
        (LiteralValue::Integer(a), LiteralValue::String(s)) => {
            let Ok(b) = s.parse::<i64>() else {
                return false;
            };
            a.cmp(&b)
        }
        (LiteralValue::String(s), LiteralValue::Integer(b)) => {
            let Ok(a) = s.parse::<i64>() else {
                return false;
            };
            a.cmp(b)
        }
        // String↔String compares lexicographically. ISO 8601 dates
        // (`YYYY-MM-DD`) sort chronologically by bytes, so this serves the
        // `::date` coercion path; other String↔String comparisons happen on
        // the row-text path (`where_value_compare_string`), not here.
        (LiteralValue::String(a), LiteralValue::String(b)) => a.as_str().cmp(b.as_str()),
        (LiteralValue::String(a), LiteralValue::StringWithCast(b, _))
        | (LiteralValue::StringWithCast(a, _), LiteralValue::String(b)) => {
            a.as_str().cmp(b.as_str())
        }
        _ => return false,
    };
    ordering_satisfies_op(ordering, op)
}

/// Extract a `(bool, bool)` pair from two literals when both sides are
/// bool-resolvable (a raw `Boolean` or a postgres-style bool string).
fn literal_pair_as_bool(left: &LiteralValue, right: &LiteralValue) -> Option<(bool, bool)> {
    let lb = literal_as_bool(left)?;
    let rb = literal_as_bool(right)?;
    Some((lb, rb))
}

fn literal_as_bool(v: &LiteralValue) -> Option<bool> {
    match v {
        LiteralValue::Boolean(b) => Some(*b),
        LiteralValue::String(s) => crate::query::cast::parse_pg_bool(s),
        // Postgres implicitly coerces integer `1`/`0` in comparisons against
        // bool; anything else is a planner error at origin (so unreachable here).
        LiteralValue::Integer(1) => Some(true),
        LiteralValue::Integer(0) => Some(false),
        _ => None,
    }
}

/// Map a three-way `Ordering` into a boolean per the SQL comparison
/// operator. Non-ordering ops (LIKE, AND, OR, …) return false.
fn ordering_satisfies_op(ordering: std::cmp::Ordering, op: BinaryOp) -> bool {
    use std::cmp::Ordering;
    match op {
        BinaryOp::Equal => ordering == Ordering::Equal,
        BinaryOp::NotEqual => ordering != Ordering::Equal,
        BinaryOp::LessThan => ordering == Ordering::Less,
        BinaryOp::LessThanOrEqual => ordering != Ordering::Greater,
        BinaryOp::GreaterThan => ordering == Ordering::Greater,
        BinaryOp::GreaterThanOrEqual => ordering != Ordering::Less,
        _ => false,
    }
}

/// Evaluate a unary expression (IS NULL, IS TRUE, NOT, etc.) against row data.
fn unary_expr_evaluate(
    op: &UnaryOp,
    expr: &ResolvedWhereExpr,
    row_data: &[Option<ByteString>],
    table_name: &str,
) -> bool {
    let value = if let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)) = expr {
        column_value_get(col, row_data, table_name)
    } else {
        None
    };

    match op {
        UnaryOp::Not => !where_expr_evaluate(expr, row_data, table_name),
        UnaryOp::IsNull => value.is_none(),
        UnaryOp::IsNotNull => value.is_some(),
        UnaryOp::IsTrue => matches!(value, Some("t" | "true")),
        UnaryOp::IsNotTrue => !matches!(value, Some("t" | "true")),
        UnaryOp::IsFalse => matches!(value, Some("f" | "false")),
        UnaryOp::IsNotFalse => !matches!(value, Some("f" | "false")),
    }
}

/// Look up a column's value in row data. Returns None for NULL values, or when
/// the column references a different table than the row belongs to.
fn column_value_get<'a>(
    col: &ResolvedColumnNode,
    row_data: &'a [Option<ByteString>],
    table_name: &str,
) -> Option<&'a str> {
    if col.table.as_str() != table_name {
        return None;
    }
    let pos = col.column_metadata.index();
    row_data.get(pos)?.as_deref()
}

/// Three-way result distinguishing "column is in a different table" from
/// "column is NULL in this row" — the comparison evaluator handles them differently.
enum ColumnRowValue<'a> {
    Present(&'a str),
    Null,
    NotInTable,
}

fn column_row_value_get<'a>(
    col: &ResolvedColumnNode,
    row_data: &'a [Option<ByteString>],
    table_name: &str,
) -> ColumnRowValue<'a> {
    if col.table.as_str() != table_name {
        return ColumnRowValue::NotInTable;
    }
    let pos = col.column_metadata.index();
    match row_data.get(pos) {
        Some(Some(v)) => ColumnRowValue::Present(v.as_str()),
        Some(None) => ColumnRowValue::Null,
        None => ColumnRowValue::NotInTable,
    }
}

/// Parse a PostgreSQL boolean wire-text value to a `bool`: the canonical
/// `t`/`f`, plus the spelled-out `true`/`false`. `None` for anything else. The
/// single source of truth for boolean wire-text spellings, shared with the
/// constraint-index row probe so the two can't drift.
pub fn pg_bool_parse(text: &str) -> Option<bool> {
    match text {
        "t" | "true" => Some(true),
        "f" | "false" => Some(false),
        _ => None,
    }
}

/// Compare a string value from row data with a LiteralValue using the specified operator.
pub fn where_value_compare_string(
    filter_value: &LiteralValue,
    row_value_str: &str,
    op: BinaryOp,
) -> bool {
    match filter_value {
        LiteralValue::String(filter_str) => {
            ordering_satisfies_op(row_value_str.cmp(filter_str), op)
        }
        LiteralValue::StringWithCast(filter_str, _cast) => {
            ordering_satisfies_op(row_value_str.cmp(filter_str), op)
        }
        LiteralValue::Integer(filter_int) => row_value_str
            .parse::<i64>()
            .is_ok_and(|row_int| ordering_satisfies_op(row_int.cmp(filter_int), op)),
        LiteralValue::Float(filter_float) => {
            if let Ok(row_float) = row_value_str.parse::<f64>() {
                let filter_f64 = filter_float.into_inner();
                match op {
                    BinaryOp::Equal => (row_float - filter_f64).abs() < f64::EPSILON,
                    BinaryOp::NotEqual => (row_float - filter_f64).abs() >= f64::EPSILON,
                    BinaryOp::LessThan => row_float < filter_f64,
                    BinaryOp::LessThanOrEqual => row_float <= filter_f64,
                    BinaryOp::GreaterThan => row_float > filter_f64,
                    BinaryOp::GreaterThanOrEqual => row_float >= filter_f64,
                    _ => false,
                }
            } else {
                false // Can't parse as float
            }
        }
        LiteralValue::Boolean(filter_bool) => {
            if let Some(row_bool) = pg_bool_parse(row_value_str) {
                match op {
                    BinaryOp::Equal => row_bool == *filter_bool,
                    BinaryOp::NotEqual => row_bool != *filter_bool,
                    _ => false, // Boolean comparisons other than equality don't make sense
                }
            } else {
                false // Can't parse as boolean
            }
        }
        LiteralValue::Null => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::NullWithCast(_) => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::Parameter(_) => false, // Parameters not supported in cache matching
        // Array literals only appear via `MultiOp::Any` / `MultiOp::All`,
        // which the evaluator handles at the WHERE-expr level — never here
        // as a scalar comparison value.
        LiteralValue::Array(_, _) => false,
    }
}

/// Returns true iff `where_expr_evaluate` can decide this expression against a
/// single CDC row. Must stay in lockstep with the evaluator: any shape the
/// evaluator falls through on (returning false unconditionally) is unsupported.
///
/// Used at update-query registration to classify the CDC fast path eligibility;
/// see `UpdateEvalStrategy`.
pub fn resolved_where_expr_supported(expr: &ResolvedWhereExpr) -> bool {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => matches!(
            scalar,
            ResolvedScalarExpr::Column(_) | ResolvedScalarExpr::Literal(_)
        ),
        ResolvedWhereExpr::Binary(binary) => match binary.op {
            BinaryOp::And | BinaryOp::Or => {
                resolved_where_expr_supported(&binary.lexpr)
                    && resolved_where_expr_supported(&binary.rexpr)
            }
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => is_simple_comparison(binary),
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => false,
        },
        ResolvedWhereExpr::Unary(unary) => resolved_where_expr_supported(&unary.expr),
        ResolvedWhereExpr::Multi(_) | ResolvedWhereExpr::Subquery { .. } => false,
    }
}

/// Check if a binary expression is a simple comparison the evaluator can
/// decide locally. Accepted shapes:
/// 1. `column op literal` (or reversed) — direct compare.
/// 2. `identity_cast(column) op literal` — identity casts are stripped by
///    `resolved_where_scalar_leaf` before this check.
/// 3. `coerceable_cast(column) op literal` — e.g. `text_col::int4 = 5`.
///    Admitted only when `cast_target_is_coercion_supported` says the
///    target+base pair has a coercion path.
///
/// For `::date` specifically, the literal must also be in canonical
/// `YYYY-MM-DD` form so lexicographic compare matches calendar order;
/// other literal spellings (`'2024-1-5'`, etc.) fall through to PgEval.
pub fn is_simple_comparison(binary_expr: &ResolvedBinaryExpr) -> bool {
    let Some((col, target, _, literal)) = canonicalize_comparison(binary_expr) else {
        return false;
    };
    let Some(target) = target else {
        return true;
    };
    if !cast_target_is_coercion_supported(target, &col.column_metadata.data_type) {
        return false;
    }
    if *target == CastTarget::Date {
        return literal_is_canonical_date(literal);
    }
    true
}

fn literal_is_canonical_date(literal: &LiteralValue) -> bool {
    let s = match literal {
        LiteralValue::String(s) => s.as_str(),
        LiteralValue::StringWithCast(s, _) => s.as_str(),
        _ => return false,
    };
    is_canonical_date_literal(s)
}

#[cfg(test)]
mod tests {

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{BinaryOp, LiteralValue, MultiOp, SubLinkType, UnaryOp};
    use crate::query::predicate::CompiledPredicate;
    use crate::query::resolved::{
        ResolvedFunctionCall, ResolvedMultiExpr, ResolvedQueryBody, ResolvedQueryExpr,
        ResolvedSelectNode, ResolvedUnaryExpr,
    };
    use ecow::EcoString;
    use ordered_float::NotNan;
    use tokio_postgres::types::Type;

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    fn test_table_metadata() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "integer".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "active".into(),
                position: 3,
                type_oid: 16,
                data_type: Type::BOOL,
                type_name: "boolean".into(),
                cache_type_name: "bool".into(),
                is_primary_key: false,
            },
        ]);

        TableMetadata {
            replica_identity_full: false,
            name: "test_table".into(),
            schema: "public".into(),
            relation_oid: Oid::from_raw(12345),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Sibling fixture with a `created_at TIMESTAMP` column for PGC-180
    /// date-narrowing tests. Row layout: `[id, name, created_at]`.
    fn test_table_metadata_with_timestamp() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "integer".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "created_at".into(),
                position: 3,
                type_oid: 1114,
                data_type: Type::TIMESTAMP,
                type_name: "timestamp".into(),
                cache_type_name: "timestamp".into(),
                is_primary_key: false,
            },
            ColumnMetadata {
                name: "received_at".into(),
                position: 4,
                type_oid: 1184,
                data_type: Type::TIMESTAMPTZ,
                type_name: "timestamptz".into(),
                cache_type_name: "timestamptz".into(),
                is_primary_key: false,
            },
        ]);

        TableMetadata {
            replica_identity_full: false,
            name: "ts_table".into(),
            schema: "public".into(),
            relation_oid: Oid::from_raw(23456),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn resolved_column(table: &TableMetadata, column: &str) -> ResolvedColumnNode {
        let meta = table.columns.get(column).expect("column exists").clone();
        ResolvedColumnNode {
            schema: table.schema.clone(),
            table: table.name.clone(),
            table_alias: None,
            column: column.into(),
            column_metadata: meta,
        }
    }

    fn col_expr(table: &TableMetadata, column: &str) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(resolved_column(table, column)))
    }

    fn val_expr(v: LiteralValue) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(v))
    }

    fn binary(
        op: BinaryOp,
        lexpr: ResolvedWhereExpr,
        rexpr: ResolvedWhereExpr,
    ) -> ResolvedBinaryExpr {
        ResolvedBinaryExpr {
            op,
            lexpr: Box::new(lexpr),
            rexpr: Box::new(rexpr),
        }
    }

    fn binary_expr(
        op: BinaryOp,
        lexpr: ResolvedWhereExpr,
        rexpr: ResolvedWhereExpr,
    ) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Binary(binary(op, lexpr, rexpr))
    }

    fn unary_expr(op: UnaryOp, expr: ResolvedWhereExpr) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Unary(ResolvedUnaryExpr {
            op,
            expr: Box::new(expr),
        })
    }

    const TABLE: &str = "test_table";

    // ------------------------------------------------------------------
    // where_value_compare_string tests (shape-agnostic)
    // ------------------------------------------------------------------

    #[test]
    fn where_value_compare_string_string_match() {
        let filter_value = LiteralValue::String("hello".into());
        assert!(where_value_compare_string(
            &filter_value,
            "hello",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "world",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "world",
            BinaryOp::NotEqual
        ));
    }

    #[test]
    fn where_value_compare_string_integer_match() {
        let filter_value = LiteralValue::Integer(123);
        assert!(where_value_compare_string(
            &filter_value,
            "123",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "124",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "abc",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "100",
            BinaryOp::LessThan
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "150",
            BinaryOp::GreaterThan
        ));
    }

    #[test]
    fn where_value_compare_string_float_match() {
        let filter_value = LiteralValue::Float(NotNan::new(123.45).unwrap());
        assert!(where_value_compare_string(
            &filter_value,
            "123.45",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "123.46",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "invalid",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "100.0",
            BinaryOp::LessThan
        ));
        assert!(where_value_compare_string(
            &filter_value,
            "150.0",
            BinaryOp::GreaterThan
        ));
    }

    #[test]
    fn where_value_compare_string_boolean_match() {
        let filter_value_true = LiteralValue::Boolean(true);
        let filter_value_false = LiteralValue::Boolean(false);

        assert!(where_value_compare_string(
            &filter_value_true,
            "true",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "false",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value_true,
            "t",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "f",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value_false,
            "false",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "true",
            BinaryOp::Equal
        ));
        assert!(where_value_compare_string(
            &filter_value_false,
            "f",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "t",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_true,
            "1",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value_false,
            "0",
            BinaryOp::Equal
        ));
    }

    #[test]
    fn where_value_compare_string_null_never_matches() {
        let filter_value = LiteralValue::Null;
        assert!(!where_value_compare_string(
            &filter_value,
            "anything",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "null",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "NULL",
            BinaryOp::Equal
        ));
    }

    #[test]
    fn where_value_compare_string_parameter_never_matches() {
        let filter_value = LiteralValue::Parameter("$1".into());
        assert!(!where_value_compare_string(
            &filter_value,
            "$1",
            BinaryOp::Equal
        ));
        assert!(!where_value_compare_string(
            &filter_value,
            "anything",
            BinaryOp::Equal
        ));
    }

    // ------------------------------------------------------------------
    // expr_comparison_evaluate tests
    // ------------------------------------------------------------------

    #[test]
    fn expr_comparison_evaluate_string_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("john".into())),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_string_no_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("jane".into())),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_integer_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(123)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), None, Some("true".into())];

        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::Null),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_reverse_order() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        // value = column (reverse order)
        let expr = binary(
            BinaryOp::Equal,
            val_expr(LiteralValue::String("john".into())),
            col_expr(&table, "name"),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // where_expr_evaluate tests
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_simple_equality() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("john".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_and_operation_both_true() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(123)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".into())),
            ),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_and_operation_one_false() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(999)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".into())),
            ),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_or_operation_one_true() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::Or,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(999)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".into())),
            ),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_or_operation_both_false() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::Or,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(999)),
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("jane".into())),
            ),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_greater_than() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::GreaterThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_unsupported_expression_type() {
        let table = test_table_metadata();
        let row_data = vec![Some("123".into()), Some("john".into()), Some("true".into())];

        let expr = ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Function(ResolvedFunctionCall {
            name: EcoString::from("upper"),
            args: vec![],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![],
            agg_filter: None,
            over: None,
        }));

        assert!(!where_expr_evaluate(&expr, &row_data, table.name.as_str()));
    }

    // ------------------------------------------------------------------
    // PGC-149: identity TypeCast strip in comparison eval / classifier
    // ------------------------------------------------------------------

    fn typecast_text(inner: ResolvedScalarExpr) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::TypeCast {
            expr: Box::new(inner),
            target: crate::query::cast::CastTarget::Text,
        })
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_matches() {
        // `name::text = 'john'` on a TEXT column — cast is identity, must match.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("john".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_no_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("alice".into()), Some("true".into())];

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("john".into())),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_on_int_column_matches() {
        // PGC-177: ::text on int column is identity — wire-text matches
        // canonical int→text exactly.
        let table = test_table_metadata();
        let row_data = vec![Some("42".into()), Some("john".into()), Some("true".into())];

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "id")));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("42".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_cast_on_bool_column_is_opaque() {
        // bool wire-text is `t`/`f`; `::text` on bool returns `true`/`false`.
        // Not identity — evaluator must bail back to opaque (return false).
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("t".into())];

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(
            &table, "active",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("true".into())),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_rhs_position() {
        // `'john' = name::text` — cast on RHS, still must match.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(
            BinaryOp::Equal,
            val_expr(LiteralValue::String("john".into())),
            cast_col,
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn resolved_where_expr_supported_admits_identity_text_cast() {
        let table = test_table_metadata();
        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("john".into())),
        );

        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn resolved_where_expr_supported_rejects_non_identity_text_cast() {
        // ::text on a bool column is not identity (wire-text `t`/`f` vs
        // canonical `true`/`false`) → must remain unsupported so the
        // classifier routes through PgEval.
        let table = test_table_metadata();
        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(
            &table, "active",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("true".into())),
        );

        assert!(!resolved_where_expr_supported(&expr));
    }

    // ------------------------------------------------------------------
    // PGC-178: ::int4 / ::int8 text-coercion in comparison eval / classifier
    // ------------------------------------------------------------------

    fn typecast(target: CastTarget, inner: ResolvedScalarExpr) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::TypeCast {
            expr: Box::new(inner),
            target,
        })
    }

    #[test]
    fn where_expr_evaluate_text_to_int4_coercion_matches() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("42".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(42)),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_int4_coercion_no_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("42".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(99)),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_int4_unparseable_row_excluded() {
        // `'abc'::int4` raises in postgres; here the row is excluded.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("abc".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(42)),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_int4_with_string_literal() {
        // ORM-generated `text_col::int = '42'` — string literal whose
        // content parses as int. Must coerce both sides to int and compare.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("42".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("42".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_int4_inequality_compares_numerically() {
        // Numerical compare avoids the lexicographic-string trap:
        // "100" < "42" by bytes, but 100 > 42 by value.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("100".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::GreaterThan,
            cast_col,
            val_expr(LiteralValue::Integer(42)),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_int8_wide_range_matches() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".into()),
            Some("9223372036854775807".into()),
            Some("true".into()),
        ];

        let cast_col = typecast(
            CastTarget::Int8,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(i64::MAX)),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn resolved_where_expr_supported_admits_text_to_int4_coercion() {
        let table = test_table_metadata();
        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(42)),
        );

        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn resolved_where_expr_supported_rejects_int4_cast_on_unsupported_base() {
        // ::int4 on a bool column isn't in the coercion whitelist → unsupported.
        let table = test_table_metadata();
        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "active")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(1)),
        );

        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn literal_compare_integer_against_integer() {
        assert!(literal_compare(
            &LiteralValue::Integer(5),
            BinaryOp::Equal,
            &LiteralValue::Integer(5),
        ));
        assert!(!literal_compare(
            &LiteralValue::Integer(5),
            BinaryOp::Equal,
            &LiteralValue::Integer(6),
        ));
        assert!(literal_compare(
            &LiteralValue::Integer(5),
            BinaryOp::LessThan,
            &LiteralValue::Integer(6),
        ));
    }

    #[test]
    fn literal_compare_integer_against_parseable_string() {
        assert!(literal_compare(
            &LiteralValue::Integer(42),
            BinaryOp::Equal,
            &LiteralValue::String("42".into()),
        ));
        // String "0042" parses to 42 — numeric compare, not lexicographic.
        assert!(literal_compare(
            &LiteralValue::Integer(42),
            BinaryOp::Equal,
            &LiteralValue::String("0042".into()),
        ));
    }

    #[test]
    fn literal_compare_falls_through_on_unparseable_string() {
        assert!(!literal_compare(
            &LiteralValue::Integer(42),
            BinaryOp::Equal,
            &LiteralValue::String("not-a-number".into()),
        ));
    }

    // ------------------------------------------------------------------
    // Literal-LHS op-flip — `WHERE 5 < col` must evaluate the same as
    // `WHERE col > 5`. Bug pre-dated PGC-149 in `where_value_compare_string`
    // and was carried into the cast-coercion path; tests lock both.
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_literal_lhs_less_than_column() {
        // SQL `WHERE 5 < id` with id=10 → true (5 < 10).
        let table = test_table_metadata();
        let row_data = vec![Some("10".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::LessThan,
            val_expr(LiteralValue::Integer(5)),
            col_expr(&table, "id"),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_literal_lhs_greater_than_column() {
        // SQL `WHERE 5 > id` with id=10 → false (5 > 10 is false).
        let table = test_table_metadata();
        let row_data = vec![Some("10".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::GreaterThan,
            val_expr(LiteralValue::Integer(5)),
            col_expr(&table, "id"),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_literal_lhs_less_than_column_no_match() {
        // SQL `WHERE 100 < id` with id=10 → false (100 < 10 is false).
        let table = test_table_metadata();
        let row_data = vec![Some("10".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::LessThan,
            val_expr(LiteralValue::Integer(100)),
            col_expr(&table, "id"),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_literal_lhs_less_than_cast_column() {
        // SQL `WHERE 5 < name::int4` with name="10" → true (5 < 10).
        // Same flip semantics on the cast-coercion path.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("10".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::LessThan,
            val_expr(LiteralValue::Integer(5)),
            cast_col,
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_literal_lhs_greater_than_or_equal_column() {
        // SQL `WHERE 10 >= id` with id=10 → true (10 >= 10).
        let table = test_table_metadata();
        let row_data = vec![Some("10".into()), Some("john".into()), Some("true".into())];

        let expr = binary_expr(
            BinaryOp::GreaterThanOrEqual,
            val_expr(LiteralValue::Integer(10)),
            col_expr(&table, "id"),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // PGC-181: ::bool text-coercion in comparison eval / classifier
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_text_to_bool_coercion_matches() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("true".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Boolean(true)),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_coercion_no_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("false".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Boolean(true)),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_short_forms() {
        let table = test_table_metadata();
        for (stored, literal_b, expected) in [
            ("t", true, true),
            ("yes", true, true),
            ("1", true, true),
            ("on", true, true),
            ("f", false, true),
            ("no", false, true),
            ("0", false, true),
            ("off", false, true),
            ("t", false, false),
            ("garbage", true, false),
        ] {
            let row_data = vec![Some("1".into()), Some(stored.into()), Some("true".into())];
            let cast_col = typecast(
                CastTarget::Bool,
                ResolvedScalarExpr::Column(resolved_column(&table, "name")),
            );
            let expr = binary_expr(
                BinaryOp::Equal,
                cast_col,
                val_expr(LiteralValue::Boolean(literal_b)),
            );
            assert_eq!(
                where_expr_evaluate(&expr, &row_data, TABLE),
                expected,
                "stored {stored:?} = literal {literal_b}"
            );
        }
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_with_string_literal() {
        // ORM-generated `text_col::bool = 't'` — string literal that parses as bool.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("true".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("t".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_with_integer_literal() {
        // Postgres coerces `1` → true / `0` → false in bool comparisons; our
        // evaluator mirrors that so the CDC fast path doesn't silently drop rows.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("true".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(1)),
        );
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_inequality_op_rejected() {
        // `<` on bool isn't supported by the wedge — eval returns false.
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("true".into()), Some("true".into())];

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::LessThan,
            cast_col,
            val_expr(LiteralValue::Boolean(true)),
        );
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_unparseable_row_excluded() {
        let table = test_table_metadata();
        let row_data = vec![
            Some("1".into()),
            Some("garbage".into()),
            Some("true".into()),
        ];

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Boolean(true)),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn resolved_where_expr_supported_admits_text_to_bool_coercion() {
        let table = test_table_metadata();
        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Boolean(true)),
        );

        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn literal_compare_boolean_pairs() {
        assert!(literal_compare(
            &LiteralValue::Boolean(true),
            BinaryOp::Equal,
            &LiteralValue::Boolean(true),
        ));
        assert!(!literal_compare(
            &LiteralValue::Boolean(true),
            BinaryOp::Equal,
            &LiteralValue::Boolean(false),
        ));
        assert!(literal_compare(
            &LiteralValue::Boolean(true),
            BinaryOp::NotEqual,
            &LiteralValue::Boolean(false),
        ));
    }

    #[test]
    fn literal_compare_boolean_against_string_and_integer() {
        // Mixed bool currency: parseable-bool string and integer 0/1.
        assert!(literal_compare(
            &LiteralValue::Boolean(true),
            BinaryOp::Equal,
            &LiteralValue::String("yes".into()),
        ));
        assert!(literal_compare(
            &LiteralValue::Boolean(false),
            BinaryOp::Equal,
            &LiteralValue::Integer(0),
        ));
        assert!(!literal_compare(
            &LiteralValue::Boolean(true),
            BinaryOp::Equal,
            &LiteralValue::Integer(0),
        ));
    }

    // ------------------------------------------------------------------
    // PGC-180: ::date narrowing from timestamp in comparison eval/classifier
    // ------------------------------------------------------------------

    const TS_TABLE: &str = "ts_table";

    fn typecast_date(inner: ResolvedScalarExpr) -> ResolvedWhereExpr {
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::TypeCast {
            expr: Box::new(inner),
            target: CastTarget::Date,
        })
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_coercion_matches() {
        let table = test_table_metadata_with_timestamp();
        let row_data = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-01-15 23:45:00".into()),
            None,
        ];

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("2024-01-15".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_coercion_no_match() {
        let table = test_table_metadata_with_timestamp();
        let row_data = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-01-15 23:45:00".into()),
            None,
        ];

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("2024-01-16".into())),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_inequality_compares_chronologically() {
        let table = test_table_metadata_with_timestamp();
        let row_data = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-03-15 09:00:00".into()),
            None,
        ];

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::GreaterThan,
            cast_col,
            val_expr(LiteralValue::String("2024-01-31".into())),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_literal_lhs_flips() {
        // Locks PGC-186 fix for the date path too: `'2024-01-01' < ts::date`.
        let table = test_table_metadata_with_timestamp();
        let row_data = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-03-15 09:00:00".into()),
            None,
        ];

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::LessThan,
            val_expr(LiteralValue::String("2024-01-01".into())),
            cast_col,
        );

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_with_typed_literal() {
        // ORM-generated `created_at::date = '2024-01-15'::date` arrives as
        // `LiteralValue::StringWithCast(...)`. Classifier must accept it and
        // evaluator must compare it the same as a plain String literal.
        let table = test_table_metadata_with_timestamp();
        let row_data = vec![
            Some("1".into()),
            Some("alice".into()),
            Some("2024-01-15 23:45:00".into()),
            None,
        ];

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::StringWithCast(
                "2024-01-15".into(),
                "date".into(),
            )),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn resolved_where_expr_supported_admits_timestamp_to_date() {
        let table = test_table_metadata_with_timestamp();
        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("2024-01-15".into())),
        );

        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn resolved_where_expr_supported_rejects_timestamptz_to_date() {
        // Deferred until PGC-187 (session-TZ tracking).
        let table = test_table_metadata_with_timestamp();
        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "received_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("2024-01-15".into())),
        );

        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn resolved_where_expr_supported_rejects_non_canonical_date_literal() {
        // `'2024-1-15'` would compare wrong lexicographically; classifier
        // must keep it on the PgEval path.
        let table = test_table_metadata_with_timestamp();
        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::String("2024-1-15".into())),
        );

        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn resolved_where_expr_supported_rejects_non_string_date_literal() {
        let table = test_table_metadata_with_timestamp();
        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::Equal,
            cast_col,
            val_expr(LiteralValue::Integer(20240115)),
        );

        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn literal_compare_string_to_string_lex_order() {
        // ISO 8601 dates compare chronologically by bytes.
        assert!(literal_compare(
            &LiteralValue::String("2024-01-15".into()),
            BinaryOp::Equal,
            &LiteralValue::String("2024-01-15".into()),
        ));
        assert!(literal_compare(
            &LiteralValue::String("2024-01-15".into()),
            BinaryOp::LessThan,
            &LiteralValue::String("2024-02-01".into()),
        ));
        assert!(!literal_compare(
            &LiteralValue::String("2024-03-01".into()),
            BinaryOp::LessThan,
            &LiteralValue::String("2024-02-01".into()),
        ));
    }

    // ------------------------------------------------------------------
    // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE / IS NULL / IS NOT NULL
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_is_true_with_true_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("t".into())];

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_true_with_false_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("f".into())];

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_true_with_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), None];

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        // IS TRUE returns false for NULL
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_false_with_false_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("f".into())];

        let expr = unary_expr(UnaryOp::IsFalse, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_false_with_true_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("t".into())];

        let expr = unary_expr(UnaryOp::IsFalse, col_expr(&table, "active"));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_true_with_false_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("f".into())];

        let expr = unary_expr(UnaryOp::IsNotTrue, col_expr(&table, "active"));

        // IS NOT TRUE returns true for FALSE
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_true_with_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), None];

        let expr = unary_expr(UnaryOp::IsNotTrue, col_expr(&table, "active"));

        // IS NOT TRUE returns true for NULL
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_false_with_true_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("t".into())];

        let expr = unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_false_with_null_value() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), None];

        let expr = unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active"));

        // IS NOT FALSE returns true for NULL
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_null_via_unary() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), None];

        let expr = unary_expr(UnaryOp::IsNull, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_null_via_unary() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("t".into())];

        let expr = unary_expr(UnaryOp::IsNotNull, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Comparison operator coverage
    // ------------------------------------------------------------------

    #[test]
    fn expr_not_equal_evaluate_string_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::NotEqual,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("jane".into())),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_not_equal_evaluate_string_no_match() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::NotEqual,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("john".into())),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_evaluate_integer_true() {
        let table = test_table_metadata();
        let row_data = vec![Some("50".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![Some("150".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_equal() {
        let table = test_table_metadata();
        let row_data = vec![Some("100".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::LessThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_less() {
        let table = test_table_metadata();
        let row_data = vec![Some("50".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::LessThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_less_than_or_equal_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![Some("150".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::LessThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_evaluate_integer_true() {
        let table = test_table_metadata();
        let row_data = vec![Some("150".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![Some("50".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_equal() {
        let table = test_table_metadata();
        let row_data = vec![Some("100".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::GreaterThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_greater() {
        let table = test_table_metadata();
        let row_data = vec![Some("150".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::GreaterThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_greater_than_or_equal_evaluate_integer_false() {
        let table = test_table_metadata();
        let row_data = vec![Some("50".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::GreaterThanOrEqual,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(100)),
        );

        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Type-specific coverage
    // ------------------------------------------------------------------

    #[test]
    fn expr_comparison_evaluate_float_operations() {
        let mut table = test_table_metadata();

        let mut cols: Vec<ColumnMetadata> = table.columns.iter().cloned().collect();
        cols.push(ColumnMetadata {
            name: "price".into(),
            position: 4,
            type_oid: 701,
            data_type: Type::FLOAT8,
            type_name: "double precision".into(),
            cache_type_name: "float8".into(),
            is_primary_key: false,
        });
        table.columns = ColumnStore::new(cols);

        let row_data = vec![
            Some("1".into()),
            Some("john".into()),
            Some("true".into()),
            Some("99.50".into()),
        ];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "price"),
            val_expr(LiteralValue::Float(NotNan::new(100.0).unwrap())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "price"),
            val_expr(LiteralValue::Float(NotNan::new(50.0).unwrap())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_string_operations() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];

        let expr = binary(
            BinaryOp::LessThan,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("zebra".into())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));

        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("alice".into())),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn expr_comparison_evaluate_null_handling() {
        let table = test_table_metadata();
        let row_data = vec![Some("1".into()), None, Some("true".into())];

        // NULL comparisons other than equality return false
        let expr = binary(
            BinaryOp::GreaterThan,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("test".into())),
        );
        assert!(!expr_comparison_evaluate(&expr, &row_data, TABLE));

        // Equality with NULL filter + NULL row value matches
        let expr = binary(
            BinaryOp::Equal,
            col_expr(&table, "name"),
            val_expr(LiteralValue::Null),
        );
        assert!(expr_comparison_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Cross-table column behavior (new)
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_cross_table_column_returns_false() {
        // Column from "other_table" — row_data belongs to "test_table"
        let table = test_table_metadata();
        let mut other_col = resolved_column(&table, "id");
        other_col.table = "other_table".into();

        let expr = binary_expr(
            BinaryOp::Equal,
            ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(other_col)),
            val_expr(LiteralValue::Integer(1)),
        );

        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // resolved_where_expr_supported
    // ------------------------------------------------------------------

    #[test]
    fn supported_bare_equality() {
        let table = test_table_metadata();
        let expr = binary_expr(
            BinaryOp::Equal,
            col_expr(&table, "id"),
            val_expr(LiteralValue::Integer(5)),
        );
        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn supported_nested_and_or() {
        let table = test_table_metadata();
        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Or,
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "id"),
                    val_expr(LiteralValue::Integer(1)),
                ),
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "id"),
                    val_expr(LiteralValue::Integer(2)),
                ),
            ),
            unary_expr(UnaryOp::IsNotNull, col_expr(&table, "name")),
        );
        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn supported_is_null_and_is_true() {
        let table = test_table_metadata();
        assert!(resolved_where_expr_supported(&unary_expr(
            UnaryOp::IsNull,
            col_expr(&table, "active"),
        )));
        assert!(resolved_where_expr_supported(&unary_expr(
            UnaryOp::IsTrue,
            col_expr(&table, "active"),
        )));
    }

    #[test]
    fn unsupported_like() {
        let table = test_table_metadata();
        let expr = binary_expr(
            BinaryOp::Like,
            col_expr(&table, "name"),
            val_expr(LiteralValue::String("j%".into())),
        );
        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn unsupported_column_to_column_comparison() {
        // The evaluator only handles Column op Value / Value op Column.
        // Column op Column falls through to false; classifier must mark unsupported.
        let table = test_table_metadata();
        let expr = binary_expr(
            BinaryOp::Equal,
            col_expr(&table, "id"),
            col_expr(&table, "id"),
        );
        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn unsupported_multi_in() {
        let table = test_table_metadata();
        let expr = ResolvedWhereExpr::Multi(ResolvedMultiExpr {
            op: MultiOp::In,
            exprs: vec![
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(1)),
                val_expr(LiteralValue::Integer(2)),
            ],
        });
        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn unsupported_function() {
        let expr = ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Function(ResolvedFunctionCall {
            name: EcoString::from("upper"),
            args: vec![],
            agg_star: false,
            agg_distinct: false,
            agg_order: vec![],
            agg_filter: None,
            over: None,
        }));
        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn unsupported_subquery() {
        let select_node: Box<ResolvedSelectNode> = Box::default();
        let query = Box::new(ResolvedQueryExpr {
            body: ResolvedQueryBody::Select(select_node),
            order_by: vec![],
            limit: None,
        });
        let expr = ResolvedWhereExpr::Subquery {
            query,
            sublink_type: SubLinkType::Exists,
            test_expr: None,
            outer_refs: vec![],
        };
        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn unsupported_and_short_circuits_on_unsupported_child() {
        // An otherwise-supported AND becomes unsupported if either child is unsupported
        let table = test_table_metadata();
        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(1)),
            ),
            binary_expr(
                BinaryOp::Like,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("j%".into())),
            ),
        );
        assert!(!resolved_where_expr_supported(&expr));
    }

    #[test]
    fn unary_expr_evaluate_cross_table_is_null_returns_true() {
        // IS NULL on a cross-table column: column_value_get returns None,
        // so IS NULL evaluates true. Matches the prior evaluator's behavior
        // for a column absent from the passed-in table metadata.
        let table = test_table_metadata();
        let mut other_col = resolved_column(&table, "id");
        other_col.table = "other_table".into();

        let expr = unary_expr(
            UnaryOp::IsNull,
            ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(other_col)),
        );

        let row_data = vec![Some("1".into()), Some("john".into()), Some("true".into())];
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // PGC-339: CompiledPredicate must evaluate identically to
    // where_expr_evaluate (the oracle) for every shape, since it gates the
    // CDC in-place-vs-invalidate decision.
    // ------------------------------------------------------------------

    /// A representative set of rows to exercise present/NULL/short-row cases.
    fn diff_rows() -> Vec<Vec<Option<ByteString>>> {
        vec![
            vec![Some("1".into()), Some("john".into()), Some("true".into())],
            vec![Some("42".into()), Some("jane".into()), Some("false".into())],
            vec![Some("1".into()), None, Some("t".into())], // NULL name
            vec![None, Some("john".into()), None],          // NULL id + active
            vec![Some("7".into())],                         // short row (missing cols)
            vec![],                                         // empty row
        ]
    }

    /// Build the cross-table variant of a column (belongs to `other_table`).
    fn cross_col(table: &TableMetadata, column: &str) -> ResolvedWhereExpr {
        let mut c = resolved_column(table, column);
        c.table = "other_table".into();
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(c))
    }

    fn assert_compiled_matches_oracle(expr: &ResolvedWhereExpr) {
        let compiled = CompiledPredicate::compile(expr, TABLE);
        for row in diff_rows() {
            assert_eq!(
                compiled.eval(&row),
                where_expr_evaluate(expr, &row, TABLE),
                "compiled vs oracle diverged for {expr:?} on row {row:?}",
            );
        }
    }

    #[test]
    fn compiled_predicate_matches_oracle_across_shapes() {
        let table = test_table_metadata();
        let exprs = vec![
            // bare comparisons (both operand orders), every operator
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".into())),
            ),
            binary_expr(
                BinaryOp::NotEqual,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("john".into())),
            ),
            binary_expr(
                BinaryOp::GreaterThan,
                col_expr(&table, "id"),
                val_expr(LiteralValue::Integer(5)),
            ),
            binary_expr(
                BinaryOp::LessThanOrEqual,
                val_expr(LiteralValue::Integer(5)),
                col_expr(&table, "id"), // literal-on-left → op_flip path
            ),
            binary_expr(
                BinaryOp::Equal,
                col_expr(&table, "name"),
                val_expr(LiteralValue::Null), // col = NULL
            ),
            // AND / OR / NOT
            binary_expr(
                BinaryOp::And,
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "id"),
                    val_expr(LiteralValue::Integer(1)),
                ),
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "name"),
                    val_expr(LiteralValue::String("john".into())),
                ),
            ),
            binary_expr(
                BinaryOp::Or,
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "id"),
                    val_expr(LiteralValue::Integer(99)),
                ),
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "active"),
                    val_expr(LiteralValue::String("true".into())),
                ),
            ),
            unary_expr(
                UnaryOp::Not,
                binary_expr(
                    BinaryOp::Equal,
                    col_expr(&table, "id"),
                    val_expr(LiteralValue::Integer(1)),
                ),
            ),
            // IS [NOT] NULL / TRUE / FALSE on a column and on a cross-table column
            unary_expr(UnaryOp::IsNull, col_expr(&table, "name")),
            unary_expr(UnaryOp::IsNotNull, col_expr(&table, "name")),
            unary_expr(UnaryOp::IsTrue, col_expr(&table, "active")),
            unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active")),
            unary_expr(UnaryOp::IsNull, cross_col(&table, "id")),
            // NOT over an unsupported inner (bare Like) → oracle returns
            // !false = true; compiler must agree via Not(ConstFalse)
            unary_expr(
                UnaryOp::Not,
                binary_expr(
                    BinaryOp::Like,
                    col_expr(&table, "name"),
                    val_expr(LiteralValue::String("j%".into())),
                ),
            ),
            // cast-coercion comparison: text col ::int4 = 42
            binary_expr(
                BinaryOp::Equal,
                typecast(
                    CastTarget::Int4,
                    ResolvedScalarExpr::Column(resolved_column(&table, "name")),
                ),
                val_expr(LiteralValue::Integer(42)),
            ),
            // unsupported shapes the oracle decides false for
            binary_expr(
                BinaryOp::Like,
                col_expr(&table, "name"),
                val_expr(LiteralValue::String("j%".into())),
            ),
            col_expr(&table, "id"), // bare Scalar
        ];
        for expr in &exprs {
            assert_compiled_matches_oracle(expr);
        }
    }
}
