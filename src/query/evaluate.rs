//! Row-level evaluation helpers shared by the compiled predicate and the
//! registration classifier: literal comparison, wire-text booleans, and
//! whether a WHERE is one the compiled predicate can decide.

#![allow(clippy::wildcard_enum_match_arm)]

use crate::query::ast::rope::{Binary, ScalarKind, Where, WhereKind};
use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::cast::{
    CastTarget, cast_target_is_coercion_supported, comparison_canonicalize,
    is_canonical_date_literal,
};
use crate::query::resolve::ResolvedQuery;
use crate::query::typed_compare::literal_locally_comparable;

/// Compare two typed `LiteralValue`s. Used by the cast-coercion path: the
/// row's text has been coerced to a typed `LiteralValue` and now needs to be
/// compared against the literal from the predicate.
///
/// Supported pairings: Integer↔Integer, Integer↔string-of-int,
/// Boolean↔Boolean, Boolean↔string-of-bool (equality only). Other pairings
/// return false.
pub fn literal_compare(left: &LiteralValue, op: BinaryOp, right: &LiteralValue) -> bool {
    // Boolean comparisons: equality only — matches the typed row path's
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
        // the row-text path (`typed_compare`), not here.
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

/// Whether the compiled predicate can decide `expr` against a single CDC row.
/// Must stay in lockstep with `CompiledPredicate::compile`: any shape it
/// compiles to `ConstFalse` is unsupported.
pub fn where_expr_supported(bound: &ResolvedQuery, expr: Where<'_>) -> bool {
    match expr.kind() {
        WhereKind::Scalar(scalar) => {
            matches!(
                scalar.kind(),
                ScalarKind::Column(_) | ScalarKind::Literal(_)
            )
        }
        WhereKind::Binary(binary) => match binary.op() {
            BinaryOp::And | BinaryOp::Or => {
                where_expr_supported(bound, binary.lexpr())
                    && where_expr_supported(bound, binary.rexpr())
            }
            BinaryOp::Equal
            | BinaryOp::NotEqual
            | BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual => comparison_is_simple(bound, binary),
            BinaryOp::Like | BinaryOp::ILike | BinaryOp::NotLike | BinaryOp::NotILike => false,
        },
        WhereKind::Unary(unary) => where_expr_supported(bound, unary.expr()),
        WhereKind::Multi(_) | WhereKind::Subquery(_) => false,
    }
}

/// Whether a comparison is one the compiled predicate decides locally:
/// `column op literal` (either order), through an identity cast, or through
/// a coercion the cast module supports; a `::date` literal must be canonical.
pub fn comparison_is_simple(bound: &ResolvedQuery, binary: Binary<'_>) -> bool {
    let Some((column, target, op, literal)) = comparison_canonicalize(bound.view(), binary) else {
        return false;
    };
    let Some(base) = bound.column_type_at(column.id()) else {
        return false;
    };
    let Some(target) = target else {
        return literal_locally_comparable(base, literal, op);
    };
    if !cast_target_is_coercion_supported(target, base) {
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

    fn text(value: &str) -> LiteralValue {
        LiteralValue::String(value.into())
    }

    #[test]
    fn test_literal_compare_cases() {
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
}
