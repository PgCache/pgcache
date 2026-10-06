#![allow(clippy::wildcard_enum_match_arm)]

#[cfg(test)]
use crate::oid::Oid;
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
        LiteralValue::String(s) => crate::query::cast::bool_literal_parse(s),
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

/// Parse a boolean as Postgres *emits* it — row values and CDC tuple text.
/// Only the canonical `t`/`f` and the spelled-out `true`/`false` occur here.
/// `None` for anything else.
///
/// Deliberately narrower than [`bool_literal_parse`](crate::query::cast::bool_literal_parse),
/// which parses boolean text a *user* wrote (`yes`/`on`/`1`/…, per Postgres
/// `boolin`). The two are separate domains, not duplicates: this one is the
/// single source of truth for wire-text spellings, shared with the
/// constraint-index row probe so the two can't drift.
pub fn bool_wire_text_parse(text: &str) -> Option<bool> {
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
            float_text_compare(row_value_str, filter_float.into_inner(), op)
        }
        LiteralValue::Boolean(filter_bool) => bool_text_compare(row_value_str, *filter_bool, op),
        LiteralValue::Null => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::NullWithCast(_) => false, // Row has non-NULL value, filter expects NULL
        LiteralValue::Parameter(_) => false, // Parameters not supported in cache matching
        // Array literals only appear via `MultiOp::Any` / `MultiOp::All`,
        // which the evaluator handles at the WHERE-expr level — never here
        // as a scalar comparison value.
        LiteralValue::Array(_, _) => false,
    }
}

/// A float filter against a row's text: equality within `f64::EPSILON`; an
/// unparseable row never matches.
fn float_text_compare(row_value_str: &str, filter: f64, op: BinaryOp) -> bool {
    let Ok(row_float) = row_value_str.parse::<f64>() else {
        return false;
    };
    match op {
        BinaryOp::Equal => (row_float - filter).abs() < f64::EPSILON,
        BinaryOp::NotEqual => (row_float - filter).abs() >= f64::EPSILON,
        BinaryOp::LessThan => row_float < filter,
        BinaryOp::LessThanOrEqual => row_float <= filter,
        BinaryOp::GreaterThan => row_float > filter,
        BinaryOp::GreaterThanOrEqual => row_float >= filter,
        _ => false,
    }
}

/// A boolean filter against a row's text: only `=` and `<>` are meaningful;
/// an unparseable row never matches.
fn bool_text_compare(row_value_str: &str, filter: bool, op: BinaryOp) -> bool {
    match (bool_wire_text_parse(row_value_str), op) {
        (Some(row_bool), BinaryOp::Equal) => row_bool == filter,
        (Some(row_bool), BinaryOp::NotEqual) => row_bool != filter,
        _ => false,
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

    use ecow::EcoString;
    use ordered_float::NotNan;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::ast::{BinaryOp, LiteralValue, MultiOp, SubLinkType, UnaryOp};
    use crate::query::predicate::CompiledPredicate;
    use crate::query::resolved::{
        ResolvedFunctionCall, ResolvedMultiExpr, ResolvedQueryBody, ResolvedQueryExpr,
        ResolvedSelectNode, ResolvedUnaryExpr,
    };

    // ------------------------------------------------------------------
    // Fixtures
    // ------------------------------------------------------------------

    /// A column whose type OID comes from its `Type`; position 1 is the key.
    fn column(name: &str, position: i16, data_type: Type, names: (&str, &str)) -> ColumnMetadata {
        let (type_name, cache_type_name) = names;
        ColumnMetadata {
            name: name.into(),
            position,
            type_oid: TypeOid::from_type(&data_type),
            data_type,
            type_name: type_name.into(),
            cache_type_name: cache_type_name.into(),
            is_primary_key: position == 1,
        }
    }

    fn table_metadata(
        name: &str,
        relation_oid: u32,
        columns: Vec<ColumnMetadata>,
    ) -> TableMetadata {
        TableMetadata {
            replica_identity_full: false,
            name: name.into(),
            schema: "public".into(),
            relation_oid: Oid::from_raw(relation_oid),
            primary_key_columns: vec!["id".into()],
            columns: ColumnStore::new(columns),
            indexes: Vec::new(),
        }
    }

    /// `test_table`: `[id int4, name text, active bool]`.
    fn test_table_metadata() -> TableMetadata {
        table_metadata(
            "test_table",
            12345,
            vec![
                column("id", 1, Type::INT4, ("integer", "int4")),
                column("name", 2, Type::TEXT, ("text", "text")),
                column("active", 3, Type::BOOL, ("boolean", "bool")),
            ],
        )
    }

    /// Sibling fixture with a `created_at TIMESTAMP` column for PGC-180
    /// date-narrowing tests. Row layout: `[id, name, created_at, received_at]`.
    fn test_table_metadata_with_timestamp() -> TableMetadata {
        table_metadata(
            "ts_table",
            23456,
            vec![
                column("id", 1, Type::INT4, ("integer", "int4")),
                column("name", 2, Type::TEXT, ("text", "text")),
                column("created_at", 3, Type::TIMESTAMP, ("timestamp", "timestamp")),
                column(
                    "received_at",
                    4,
                    Type::TIMESTAMPTZ,
                    ("timestamptz", "timestamptz"),
                ),
            ],
        )
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

    fn text(value: &str) -> LiteralValue {
        LiteralValue::String(value.into())
    }

    fn int(value: i64) -> LiteralValue {
        LiteralValue::Integer(value)
    }

    fn row(values: &[Option<&str>]) -> Vec<Option<ByteString>> {
        values.iter().map(|v| v.map(ByteString::from)).collect()
    }

    /// `column op literal` against `table`.
    fn col_cmp(
        table: &TableMetadata,
        column: &str,
        op: BinaryOp,
        literal: LiteralValue,
    ) -> ResolvedWhereExpr {
        binary_expr(op, col_expr(table, column), val_expr(literal))
    }

    // ------------------------------------------------------------------
    // where_value_compare_string tests (shape-agnostic)
    // ------------------------------------------------------------------

    fn float(v: f64) -> LiteralValue {
        LiteralValue::Float(NotNan::new(v).expect("test float is not NaN"))
    }

    #[test]
    fn where_value_compare_string_cases() {
        use BinaryOp::{Equal, GreaterThan, LessThan, NotEqual};
        use LiteralValue::{Boolean, Integer, Null, Parameter};
        // (label, filter literal, row text, op, expected)
        #[rustfmt::skip]
        let cases = [
            ("string equal",                 text("hello"),           "hello",    Equal,       true),
            ("string unequal",               text("hello"),           "world",    Equal,       false),
            ("string not-equal",             text("hello"),           "world",    NotEqual,    true),
            ("integer equal",                Integer(123),            "123",      Equal,       true),
            ("integer unequal",              Integer(123),            "124",      Equal,       false),
            ("integer vs unparseable row",   Integer(123),            "abc",      Equal,       false),
            ("integer row below",            Integer(123),            "100",      LessThan,    true),
            ("integer row above",            Integer(123),            "150",      GreaterThan, true),
            ("float equal",                  float(123.45),           "123.45",   Equal,       true),
            ("float unequal",                float(123.45),           "123.46",   Equal,       false),
            ("float vs unparseable row",     float(123.45),           "invalid",  Equal,       false),
            ("float row below",              float(123.45),           "100.0",    LessThan,    true),
            ("float row above",              float(123.45),           "150.0",    GreaterThan, true),
            ("bool true = 'true'",           Boolean(true),           "true",     Equal,       true),
            ("bool true = 'false'",          Boolean(true),           "false",    Equal,       false),
            ("bool true = 't'",              Boolean(true),           "t",        Equal,       true),
            ("bool true = 'f'",              Boolean(true),           "f",        Equal,       false),
            ("bool false = 'false'",         Boolean(false),          "false",    Equal,       true),
            ("bool false = 'true'",          Boolean(false),          "true",     Equal,       false),
            ("bool false = 'f'",             Boolean(false),          "f",        Equal,       true),
            ("bool false = 't'",             Boolean(false),          "t",        Equal,       false),
            ("bool true never matches '1'",  Boolean(true),           "1",        Equal,       false),
            ("bool false never matches '0'", Boolean(false),          "0",        Equal,       false),
            ("null filter vs 'anything'",    Null,                    "anything", Equal,       false),
            ("null filter vs 'null'",        Null,                    "null",     Equal,       false),
            ("null filter vs 'NULL'",        Null,                    "NULL",     Equal,       false),
            ("parameter vs its own text",    Parameter("$1".into()),  "$1",       Equal,       false),
            ("parameter vs 'anything'",      Parameter("$1".into()),  "anything", Equal,       false),
        ];
        for (label, filter, row_text, op, expected) in cases {
            let actual = where_value_compare_string(&filter, row_text, op);
            assert_eq!(
                actual, expected,
                "{label}: expected {expected}, got {actual} (row {row_text:?} {op:?} filter {filter:?})"
            );
        }
    }

    #[test]
    fn literal_compare_cases() {
        use BinaryOp::{Equal, LessThan, NotEqual};
        use LiteralValue::{Boolean, Integer};
        // (label, left, op, right, expected). "0042" parses to 42 (numeric, not
        // lexicographic); bools also compare against parseable-bool strings
        // and integer 0/1; ISO 8601 dates compare chronologically by bytes.
        #[rustfmt::skip]
        let cases = [
            ("integers equal",                Integer(5),         Equal,    Integer(5),           true),
            ("integers unequal",              Integer(5),         Equal,    Integer(6),           false),
            ("integer less than",             Integer(5),         LessThan, Integer(6),           true),
            ("integer vs parseable string",   Integer(42),        Equal,    text("42"),           true),
            ("integer vs zero-padded string", Integer(42),        Equal,    text("0042"),         true),
            ("integer vs unparseable string", Integer(42),        Equal,    text("not-a-number"), false),
            ("bools equal",                   Boolean(true),      Equal,    Boolean(true),        true),
            ("bools unequal",                 Boolean(true),      Equal,    Boolean(false),       false),
            ("bools not-equal",               Boolean(true),      NotEqual, Boolean(false),       true),
            ("bool vs 'yes'",                 Boolean(true),      Equal,    text("yes"),          true),
            ("bool false vs integer 0",       Boolean(false),     Equal,    Integer(0),           true),
            ("bool true vs integer 0",        Boolean(true),      Equal,    Integer(0),           false),
            ("same date strings",             text("2024-01-15"), Equal,    text("2024-01-15"),   true),
            ("earlier date string",           text("2024-01-15"), LessThan, text("2024-02-01"),   true),
            ("later date string",             text("2024-03-01"), LessThan, text("2024-02-01"),   false),
        ];
        for (label, left, op, right, expected) in cases {
            let actual = literal_compare(&left, op, &right);
            assert_eq!(
                actual, expected,
                "{label}: expected {expected}, got {actual} ({left:?} {op:?} {right:?})"
            );
        }
    }

    #[test]
    fn expr_comparison_evaluate_cases() {
        use BinaryOp::{
            Equal, GreaterThan, GreaterThanOrEqual, LessThan, LessThanOrEqual, NotEqual,
        };
        use LiteralValue::{Integer, Null};
        // Rows are `test_table` rows `[id, name, active]`: `id` with name
        // 'john', or `None` for a row whose name is NULL. NULL comparisons
        // other than equality are false.
        // (label, row id, column, op, literal, literal written first, expected)
        #[rustfmt::skip]
        let cases = [
            ("name = 'john'",           Some("1"),   "name", Equal,              text("john"),  false, true),
            ("name = 'jane'",           Some("1"),   "name", Equal,              text("jane"),  false, false),
            ("id = 123",                Some("123"), "id",   Equal,              Integer(123),  false, true),
            ("NULL name = NULL",        None,        "name", Equal,              Null,          false, true),
            ("'john' = name",           Some("1"),   "name", Equal,              text("john"),  true,  true),
            ("name <> 'jane'",          Some("1"),   "name", NotEqual,           text("jane"),  false, true),
            ("name <> 'john'",          Some("1"),   "name", NotEqual,           text("john"),  false, false),
            ("id 50 < 100",             Some("50"),  "id",   LessThan,           Integer(100),  false, true),
            ("id 150 < 100",            Some("150"), "id",   LessThan,           Integer(100),  false, false),
            ("id 100 <= 100",           Some("100"), "id",   LessThanOrEqual,    Integer(100),  false, true),
            ("id 50 <= 100",            Some("50"),  "id",   LessThanOrEqual,    Integer(100),  false, true),
            ("id 150 <= 100",           Some("150"), "id",   LessThanOrEqual,    Integer(100),  false, false),
            ("id 150 > 100",            Some("150"), "id",   GreaterThan,        Integer(100),  false, true),
            ("id 50 > 100",             Some("50"),  "id",   GreaterThan,        Integer(100),  false, false),
            ("id 100 >= 100",           Some("100"), "id",   GreaterThanOrEqual, Integer(100),  false, true),
            ("id 150 >= 100",           Some("150"), "id",   GreaterThanOrEqual, Integer(100),  false, true),
            ("id 50 >= 100",            Some("50"),  "id",   GreaterThanOrEqual, Integer(100),  false, false),
            ("name 'john' < 'zebra'",   Some("1"),   "name", LessThan,           text("zebra"), false, true),
            ("name 'john' > 'alice'",   Some("1"),   "name", GreaterThan,        text("alice"), false, true),
            ("NULL name > 'test'",      None,        "name", GreaterThan,        text("test"),  false, false),
        ];
        let table = test_table_metadata();
        for (label, row_id, column, op, literal, literal_first, expected) in cases {
            let row = match row_id {
                Some(id) => [Some(id), Some("john"), Some("true")],
                None => [Some("1"), None, Some("true")],
            };
            let row_data: Vec<Option<ByteString>> =
                row.iter().map(|v| v.map(ByteString::from)).collect();
            let (lhs, rhs) = (col_expr(&table, column), val_expr(literal.clone()));
            let expr = match literal_first {
                true => binary(op, rhs, lhs),
                false => binary(op, lhs, rhs),
            };
            let actual = expr_comparison_evaluate(&expr, &row_data, TABLE);
            assert_eq!(
                actual, expected,
                "{label}: expected {expected}, got {actual} \
                 (column {column} {op:?} {literal:?}, literal first: {literal_first}, row {row:?})"
            );
        }
    }

    // ------------------------------------------------------------------
    // expr_comparison_evaluate tests
    // ------------------------------------------------------------------

    // ------------------------------------------------------------------
    // where_expr_evaluate tests
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_simple_equality() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("true")]);

        let expr = col_cmp(&table, "name", BinaryOp::Equal, text("john"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_and_operation_both_true() {
        let table = test_table_metadata();
        let row_data = row(&[Some("123"), Some("john"), Some("true")]);

        let expr = binary_expr(
            BinaryOp::And,
            col_cmp(&table, "id", BinaryOp::Equal, int(123)),
            col_cmp(&table, "name", BinaryOp::Equal, text("john")),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_and_operation_one_false() {
        let table = test_table_metadata();
        let row_data = row(&[Some("123"), Some("john"), Some("true")]);

        let expr = binary_expr(
            BinaryOp::And,
            col_cmp(&table, "id", BinaryOp::Equal, int(999)),
            col_cmp(&table, "name", BinaryOp::Equal, text("john")),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_or_operation_one_true() {
        let table = test_table_metadata();
        let row_data = row(&[Some("123"), Some("john"), Some("true")]);

        let expr = binary_expr(
            BinaryOp::Or,
            col_cmp(&table, "id", BinaryOp::Equal, int(999)),
            col_cmp(&table, "name", BinaryOp::Equal, text("john")),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_or_operation_both_false() {
        let table = test_table_metadata();
        let row_data = row(&[Some("123"), Some("john"), Some("true")]);

        let expr = binary_expr(
            BinaryOp::Or,
            col_cmp(&table, "id", BinaryOp::Equal, int(999)),
            col_cmp(&table, "name", BinaryOp::Equal, text("jane")),
        );

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_greater_than() {
        let table = test_table_metadata();
        let row_data = row(&[Some("123"), Some("john"), Some("true")]);

        let expr = col_cmp(&table, "id", BinaryOp::GreaterThan, int(100));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_unsupported_expression_type() {
        let table = test_table_metadata();
        let row_data = row(&[Some("123"), Some("john"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("john"), Some("true")]);

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("john")));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_no_match() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("alice"), Some("true")]);

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("john")));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_on_int_column_matches() {
        // PGC-177: ::text on int column is identity — wire-text matches
        // canonical int→text exactly.
        let table = test_table_metadata();
        let row_data = row(&[Some("42"), Some("john"), Some("true")]);

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "id")));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("42")));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_cast_on_bool_column_is_opaque() {
        // bool wire-text is `t`/`f`; `::text` on bool returns `true`/`false`.
        // Not identity — evaluator must bail back to opaque (return false).
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("t")]);

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(
            &table, "active",
        )));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("true")));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_identity_text_cast_rhs_position() {
        // `'john' = name::text` — cast on RHS, still must match.
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("true")]);

        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(BinaryOp::Equal, val_expr(text("john")), cast_col);

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn resolved_where_expr_supported_admits_identity_text_cast() {
        let table = test_table_metadata();
        let cast_col = typecast_text(ResolvedScalarExpr::Column(resolved_column(&table, "name")));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("john")));

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
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("true")));

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
        let row_data = row(&[Some("1"), Some("42"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("42"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("abc"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("42"), Some("true")]);

        let cast_col = typecast(
            CastTarget::Int4,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("42")));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_int4_inequality_compares_numerically() {
        // Numerical compare avoids the lexicographic-string trap:
        // "100" < "42" by bytes, but 100 > 42 by value.
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("100"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("9223372036854775807"), Some("true")]);

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

    // ------------------------------------------------------------------
    // Literal-LHS op-flip — `WHERE 5 < col` must evaluate the same as
    // `WHERE col > 5`. Bug pre-dated PGC-149 in `where_value_compare_string`
    // and was carried into the cast-coercion path; tests lock both.
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_literal_lhs_less_than_column() {
        // SQL `WHERE 5 < id` with id=10 → true (5 < 10).
        let table = test_table_metadata();
        let row_data = row(&[Some("10"), Some("john"), Some("true")]);

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
        let row_data = row(&[Some("10"), Some("john"), Some("true")]);

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
        let row_data = row(&[Some("10"), Some("john"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("10"), Some("true")]);

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
        let row_data = row(&[Some("10"), Some("john"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("true"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("false"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("true"), Some("true")]);

        let cast_col = typecast(
            CastTarget::Bool,
            ResolvedScalarExpr::Column(resolved_column(&table, "name")),
        );
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("t")));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_text_to_bool_with_integer_literal() {
        // Postgres coerces `1` → true / `0` → false in bool comparisons; our
        // evaluator mirrors that so the CDC fast path doesn't silently drop rows.
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("true"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("true"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("garbage"), Some("true")]);

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
        let row_data = row(&[Some("1"), Some("alice"), Some("2024-01-15 23:45:00"), None]);

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("2024-01-15")));

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_coercion_no_match() {
        let table = test_table_metadata_with_timestamp();
        let row_data = row(&[Some("1"), Some("alice"), Some("2024-01-15 23:45:00"), None]);

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("2024-01-16")));

        assert!(!where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_inequality_compares_chronologically() {
        let table = test_table_metadata_with_timestamp();
        let row_data = row(&[Some("1"), Some("alice"), Some("2024-03-15 09:00:00"), None]);

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(
            BinaryOp::GreaterThan,
            cast_col,
            val_expr(text("2024-01-31")),
        );

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_literal_lhs_flips() {
        // Locks PGC-186 fix for the date path too: `'2024-01-01' < ts::date`.
        let table = test_table_metadata_with_timestamp();
        let row_data = row(&[Some("1"), Some("alice"), Some("2024-03-15 09:00:00"), None]);

        let cast_col = typecast_date(ResolvedScalarExpr::Column(resolved_column(
            &table,
            "created_at",
        )));
        let expr = binary_expr(BinaryOp::LessThan, val_expr(text("2024-01-01")), cast_col);

        assert!(where_expr_evaluate(&expr, &row_data, TS_TABLE));
    }

    #[test]
    fn where_expr_evaluate_timestamp_to_date_with_typed_literal() {
        // ORM-generated `created_at::date = '2024-01-15'::date` arrives as
        // `LiteralValue::StringWithCast(...)`. Classifier must accept it and
        // evaluator must compare it the same as a plain String literal.
        let table = test_table_metadata_with_timestamp();
        let row_data = row(&[Some("1"), Some("alice"), Some("2024-01-15 23:45:00"), None]);

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
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("2024-01-15")));

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
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("2024-01-15")));

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
        let expr = binary_expr(BinaryOp::Equal, cast_col, val_expr(text("2024-1-15")));

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

    // ------------------------------------------------------------------
    // IS TRUE / IS FALSE / IS NOT TRUE / IS NOT FALSE / IS NULL / IS NOT NULL
    // ------------------------------------------------------------------

    #[test]
    fn where_expr_evaluate_is_true_with_true_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("t")]);

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_true_with_false_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("f")]);

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_true_with_null_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), None]);

        let expr = unary_expr(UnaryOp::IsTrue, col_expr(&table, "active"));

        // IS TRUE returns false for NULL
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_false_with_false_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("f")]);

        let expr = unary_expr(UnaryOp::IsFalse, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_false_with_true_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("t")]);

        let expr = unary_expr(UnaryOp::IsFalse, col_expr(&table, "active"));

        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_true_with_false_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("f")]);

        let expr = unary_expr(UnaryOp::IsNotTrue, col_expr(&table, "active"));

        // IS NOT TRUE returns true for FALSE
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_true_with_null_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), None]);

        let expr = unary_expr(UnaryOp::IsNotTrue, col_expr(&table, "active"));

        // IS NOT TRUE returns true for NULL
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_false_with_true_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("t")]);

        let expr = unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_false_with_null_value() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), None]);

        let expr = unary_expr(UnaryOp::IsNotFalse, col_expr(&table, "active"));

        // IS NOT FALSE returns true for NULL
        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_null_via_unary() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), None]);

        let expr = unary_expr(UnaryOp::IsNull, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    #[test]
    fn where_expr_evaluate_is_not_null_via_unary() {
        let table = test_table_metadata();
        let row_data = row(&[Some("1"), Some("john"), Some("t")]);

        let expr = unary_expr(UnaryOp::IsNotNull, col_expr(&table, "active"));

        assert!(where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // Comparison operator coverage
    // ------------------------------------------------------------------

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
            type_oid: TypeOid::from_raw(701),
            data_type: Type::FLOAT8,
            type_name: "double precision".into(),
            cache_type_name: "float8".into(),
            is_primary_key: false,
        });
        table.columns = ColumnStore::new(cols);

        let row_data = row(&[Some("1"), Some("john"), Some("true"), Some("99.50")]);

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

        let row_data = row(&[Some("1"), Some("john"), Some("true")]);
        assert!(!where_expr_evaluate(&expr, &row_data, TABLE));
    }

    // ------------------------------------------------------------------
    // resolved_where_expr_supported
    // ------------------------------------------------------------------

    #[test]
    fn supported_bare_equality() {
        let table = test_table_metadata();
        let expr = col_cmp(&table, "id", BinaryOp::Equal, int(5));
        assert!(resolved_where_expr_supported(&expr));
    }

    #[test]
    fn supported_nested_and_or() {
        let table = test_table_metadata();
        let expr = binary_expr(
            BinaryOp::And,
            binary_expr(
                BinaryOp::Or,
                col_cmp(&table, "id", BinaryOp::Equal, int(1)),
                col_cmp(&table, "id", BinaryOp::Equal, int(2)),
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
        let expr = col_cmp(&table, "name", BinaryOp::Like, text("j%"));
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
            col_cmp(&table, "id", BinaryOp::Equal, int(1)),
            col_cmp(&table, "name", BinaryOp::Like, text("j%")),
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

        let row_data = row(&[Some("1"), Some("john"), Some("true")]);
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
            col_cmp(&table, "name", BinaryOp::Equal, text("john")),
            col_cmp(&table, "name", BinaryOp::NotEqual, text("john")),
            col_cmp(&table, "id", BinaryOp::GreaterThan, int(5)),
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
                col_cmp(&table, "id", BinaryOp::Equal, int(1)),
                col_cmp(&table, "name", BinaryOp::Equal, text("john")),
            ),
            binary_expr(
                BinaryOp::Or,
                col_cmp(&table, "id", BinaryOp::Equal, int(99)),
                col_cmp(&table, "active", BinaryOp::Equal, text("true")),
            ),
            unary_expr(UnaryOp::Not, col_cmp(&table, "id", BinaryOp::Equal, int(1))),
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
                col_cmp(&table, "name", BinaryOp::Like, text("j%")),
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
            col_cmp(&table, "name", BinaryOp::Like, text("j%")),
            col_expr(&table, "id"), // bare Scalar
        ];
        for expr in &exprs {
            assert_compiled_matches_oracle(expr);
        }
    }
}
