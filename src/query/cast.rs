#![allow(clippy::wildcard_enum_match_arm)]

//! TypeCast target representation, identity-cast classification, and
//! text-to-typed coercion.
//!
//! `CastTarget` is the typed form of the canonical string produced by
//! `query::ast::convert::type_name_render`. Storing it as an enum lets the
//! evaluator, classifier, and constraint extractor share one whitelist:
//! only casts the codebase has *deliberately* taught itself to handle become
//! named variants; everything else stays `Other(_)` and is treated opaquely
//! (forwarded to origin via PgEval).
//!
//! Two whitelists drive different fast paths:
//! * `cast_target_is_identity_on` — the cast is a no-op on the column's
//!   wire-text form, so the evaluator can strip it and compare directly.
//! * `cast_target_is_coercion_supported` + `cast_target_coerce_text` — the
//!   cast changes the value, but the codebase knows how to apply the same
//!   coercion locally before comparing.

use ecow::EcoString;
use postgres_types::Type;

use crate::query::ast::rope::{Binary, Column, Scalar, ScalarKind, Where, WhereKind};
use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::resolve::BoundView;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CastTarget {
    /// `::text`
    Text,
    /// `::int4` (also written `::int` or `::integer` — pg_query canonicalizes).
    Int4,
    /// `::int8` (also written `::bigint`).
    Int8,
    /// `::bool` (also written `::boolean`).
    Bool,
    /// `::date`
    Date,
    /// Anything pgcache doesn't recognize yet. Carries the canonical text
    /// from `type_name_render` so `Deparse` is lossless.
    Other(EcoString),
}

/// Map the canonical `target_type` string (as produced by
/// `query::ast::convert::type_name_render`) into a `CastTarget`.
pub fn cast_target_from_canonical(s: &str) -> CastTarget {
    match s {
        "text" => CastTarget::Text,
        "int4" => CastTarget::Int4,
        "int8" => CastTarget::Int8,
        "bool" => CastTarget::Bool,
        "date" => CastTarget::Date,
        _ => CastTarget::Other(EcoString::from(s)),
    }
}

/// Render a `CastTarget` back to its canonical text form for `Deparse`.
pub fn cast_target_deparse(target: &CastTarget) -> &str {
    match target {
        CastTarget::Text => "text",
        CastTarget::Int4 => "int4",
        CastTarget::Int8 => "int8",
        CastTarget::Bool => "bool",
        CastTarget::Date => "date",
        CastTarget::Other(s) => s.as_str(),
    }
}

/// Does `column::target` produce the same byte sequence as the stored
/// wire-text representation of `column`? When true, evaluator and classifier
/// can strip the cast and treat the comparison as `column op literal`.
///
/// `::text` identity covers `text` / `varchar` — trivially the same bytes.
/// An integer column's `::text` has the same bytes too, but compares as text
/// (`'9' > '10'`), so it is an identity only inside an equality against the
/// integer's own spelling ([`comparison_canonicalize`]).
///
/// `::int4` / `::int8` identity covers self-casts (`int4_col::int4` etc.) —
/// the cast is a no-op an ORM sometimes still emits.
pub fn cast_target_is_identity_on(target: &CastTarget, base: &Type) -> bool {
    match target {
        CastTarget::Text => matches!(*base, Type::TEXT | Type::VARCHAR),
        CastTarget::Int4 => matches!(*base, Type::INT4),
        CastTarget::Int8 => matches!(*base, Type::INT8),
        CastTarget::Bool => matches!(*base, Type::BOOL),
        CastTarget::Date => matches!(*base, Type::DATE),
        CastTarget::Other(_) => false,
    }
}

/// Does `cast_target_coerce_text` know how to handle `column::target` for a
/// column whose base type is `base`? Used by the classifier to decide
/// LocalEval vs PgEval — non-identity casts that aren't whitelisted here
/// must keep falling through to PgEval.
///
/// Wedge: `text` / `varchar` columns into `int4` / `int8` / `bool`;
/// plain `timestamp` columns into `date` (timestamptz deferred — needs
/// session-TZ tracking, PGC-187).
pub fn cast_target_is_coercion_supported(target: &CastTarget, base: &Type) -> bool {
    match target {
        CastTarget::Int4 | CastTarget::Int8 | CastTarget::Bool => {
            matches!(*base, Type::TEXT | Type::VARCHAR)
        }
        CastTarget::Date => matches!(*base, Type::TIMESTAMP),
        CastTarget::Text | CastTarget::Other(_) => false,
    }
}

/// Coerce a row's wire-text value into a typed `LiteralValue` according to
/// the cast target. Returns `None` for unsupported targets or when parsing
/// fails (e.g. `'abc'::int4` — postgres would raise; here the row simply
/// doesn't match the predicate).
pub fn cast_target_coerce_text(target: &CastTarget, row_text: &str) -> Option<LiteralValue> {
    match target {
        CastTarget::Int4 | CastTarget::Int8 => {
            row_text.parse::<i64>().ok().map(LiteralValue::Integer)
        }
        CastTarget::Bool => bool_literal_parse(row_text).map(LiteralValue::Boolean),
        CastTarget::Date => {
            timestamp_text_to_date(row_text).map(|s| LiteralValue::String(s.into()))
        }
        CastTarget::Text | CastTarget::Other(_) => None,
    }
}

/// Why the cast of `text` to `target` would fail on origin, for the types
/// whose input rules are plain enough to apply here: the integers, `numeric`
/// without a modifier, `bool` and `uuid`. `None` for an accepted value and
/// for every other type, which is left to origin as before. A rule that has
/// widened across versions (digit underscores and `0x`/`0o`/`0b` prefixes
/// since PostgreSQL 16) is applied in its widest form, so a value accepted
/// here and rejected by an older origin only keeps the behaviour it had.
pub fn cast_target_rejects_text(target: &CastTarget, text: &str) -> Option<&'static str> {
    let bits = match target {
        CastTarget::Int4 => 32,
        CastTarget::Int8 => 64,
        CastTarget::Bool => {
            return bool_literal_parse(text)
                .is_none()
                .then_some("text the boolean type rejects");
        }
        CastTarget::Other(name) => match name.as_str() {
            "int2" => 16,
            "numeric" => {
                return (!numeric_text_accepts(text)).then_some("text the numeric type rejects");
            }
            "uuid" => return (!uuid_text_accepts(text)).then_some("text the uuid type rejects"),
            _ => return None,
        },
        CastTarget::Text | CastTarget::Date => return None,
    };
    let Some(value) = integer_text_parse(text) else {
        return Some("text the integer type rejects");
    };
    let limit: i128 = 1 << (bits - 1);
    (value < -limit || value >= limit).then_some("an integer out of range for the type")
}

/// The integer `text` denotes as `pg_strtoint` reads it (whitespace around,
/// a sign, decimal digits or a `0x`/`0o`/`0b` prefixed form, underscores
/// between digits), or `None` when it denotes none: the caller's "rejected".
/// Returns `Some(0)` with no digits only for text with digits, never for
/// empty text. Values beyond i128 are treated as out of range by the caller.
fn integer_text_parse(text: &str) -> Option<i128> {
    let trimmed = text.trim_matches(|c: char| c.is_ascii_whitespace() || c == '\x0b');
    let (negative, unsigned) = match trimmed.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, trimmed.strip_prefix('+').unwrap_or(trimmed)),
    };
    let prefixed = |prefixes: [&str; 2]| {
        prefixes
            .iter()
            .find_map(|prefix| unsigned.strip_prefix(prefix))
    };
    let (radix, digits) = if let Some(rest) = prefixed(["0x", "0X"]) {
        (16, rest)
    } else if let Some(rest) = prefixed(["0o", "0O"]) {
        (8, rest)
    } else if let Some(rest) = prefixed(["0b", "0B"]) {
        (2, rest)
    } else {
        (10, unsigned)
    };
    let magnitude = digits_with_underscores_parse(digits, radix)?;
    Some(if negative { -magnitude } else { magnitude })
}

/// `digits` in `radix` with single underscores between digits, as the
/// PostgreSQL 16 lexer and input functions allow. `None` when empty, when an
/// underscore leads, trails or doubles, or when a character is no digit.
/// Saturates at the i128 limit, which every integer type is far inside.
fn digits_with_underscores_parse(digits: &str, radix: u32) -> Option<i128> {
    if digits.is_empty() || digits.starts_with('_') || digits.ends_with('_') {
        return None;
    }
    let mut value: i128 = 0;
    let mut previous_underscore = false;
    for c in digits.chars() {
        if c == '_' {
            if previous_underscore {
                return None;
            }
            previous_underscore = true;
            continue;
        }
        previous_underscore = false;
        let digit = c.to_digit(radix)?;
        value = value
            .saturating_mul(i128::from(radix))
            .saturating_add(i128::from(digit));
    }
    Some(value)
}

/// Whether `text` is a `numeric` input: a signed decimal with an optional
/// fraction and exponent, the integer forms with a prefix or underscores, or
/// `NaN` and the infinities, whitespace around.
fn numeric_text_accepts(text: &str) -> bool {
    let trimmed = text.trim_matches(|c: char| c.is_ascii_whitespace() || c == '\x0b');
    let unsigned = trimmed
        .strip_prefix('-')
        .or_else(|| trimmed.strip_prefix('+'))
        .unwrap_or(trimmed);
    if ["nan", "infinity", "inf"]
        .iter()
        .any(|special| unsigned.eq_ignore_ascii_case(special))
    {
        return true;
    }
    if integer_text_parse(unsigned).is_some() {
        return true;
    }
    let (mantissa, exponent) = match unsigned.split_once(['e', 'E']) {
        Some((mantissa, exponent)) => (mantissa, Some(exponent)),
        None => (unsigned, None),
    };
    let (whole, fraction) = match mantissa.split_once('.') {
        Some((whole, fraction)) => (whole, Some(fraction)),
        None => (mantissa, None),
    };
    let digits_ok =
        |part: &str| part.is_empty() || digits_with_underscores_parse(part, 10).is_some();
    let has_digit = !whole.is_empty() || fraction.is_some_and(|part| !part.is_empty());
    has_digit
        && digits_ok(whole)
        && fraction.is_none_or(digits_ok)
        && exponent.is_none_or(|exponent| {
            let digits = exponent
                .strip_prefix('-')
                .or_else(|| exponent.strip_prefix('+'))
                .unwrap_or(exponent);
            !digits.is_empty() && digits.bytes().all(|b| b.is_ascii_digit())
        })
}

/// Whether `text` is a `uuid` input as `uuid_in` reads it: 32 hex digits,
/// optionally in braces, a hyphen allowed after any group of four digits.
fn uuid_text_accepts(text: &str) -> bool {
    let inner = match text.strip_prefix('{') {
        Some(rest) => match rest.strip_suffix('}') {
            Some(inner) => inner,
            None => return false,
        },
        None => text,
    };
    let mut hex_digits = 0usize;
    let mut previous_hyphen = false;
    for c in inner.chars() {
        if c == '-' {
            if previous_hyphen
                || hex_digits == 0
                || !hex_digits.is_multiple_of(4)
                || hex_digits == 32
            {
                return false;
            }
            previous_hyphen = true;
            continue;
        }
        previous_hyphen = false;
        if !c.is_ascii_hexdigit() {
            return false;
        }
        hex_digits += 1;
    }
    hex_digits == 32 && !previous_hyphen
}

/// Extract the `YYYY-MM-DD` prefix of a postgres `timestamp` wire-text
/// value. Returns the date as an owned String when the input begins with a
/// canonical 10-char date followed by either end-of-string or a separator
/// (` ` for date+time, `T` for ISO 8601). Returns `None` for anything else.
fn timestamp_text_to_date(s: &str) -> Option<String> {
    let (date_bytes, rest) = s.as_bytes().split_first_chunk::<10>()?;
    if !is_canonical_date_prefix(date_bytes) {
        return None;
    }
    match rest {
        [] | [b' ', ..] | [b'T', ..] => {
            let date = std::str::from_utf8(date_bytes).ok()?;
            Some(date.to_owned())
        }
        _ => None,
    }
}

/// Cheap shape check for a `YYYY-MM-DD` prefix — eight ASCII digits with two
/// hyphens at the right positions. Doesn't validate the calendar (e.g.
/// `2024-13-99` returns true) — that's postgres's job at registration time.
fn is_canonical_date_prefix(date_bytes: &[u8; 10]) -> bool {
    let [y0, y1, y2, y3, dash1, m0, m1, dash2, d0, d1] = *date_bytes;
    y0.is_ascii_digit()
        && y1.is_ascii_digit()
        && y2.is_ascii_digit()
        && y3.is_ascii_digit()
        && dash1 == b'-'
        && m0.is_ascii_digit()
        && m1.is_ascii_digit()
        && dash2 == b'-'
        && d0.is_ascii_digit()
        && d1.is_ascii_digit()
}

/// Is `s` a canonical date literal — exactly `YYYY-MM-DD`, nothing else?
/// Used by the classifier to gate `WHERE col::date = 'literal'` shapes
/// against literals it knows how to compare lexicographically.
pub fn is_canonical_date_literal(s: &str) -> bool {
    let Ok(chunk) = <&[u8; 10]>::try_from(s.as_bytes()) else {
        return false;
    };
    is_canonical_date_prefix(chunk)
}

/// Parse a boolean literal as a *user* may have written it, mirroring Postgres
/// `boolin`: `true`/`false`/`t`/`f`/`yes`/`no`/`on`/`off`/`1`/`0`,
/// case-insensitive, whitespace-trimmed. Returns `None` for anything else —
/// postgres would raise; locally the row simply doesn't match.
///
/// Deliberately wider than [`bool_wire_text_parse`](crate::query::evaluate::bool_wire_text_parse),
/// which parses boolean text Postgres *emits* (only `t`/`f`/`true`/`false`).
/// The two are separate domains, not duplicates.
pub fn bool_literal_parse(s: &str) -> Option<bool> {
    let trimmed = s.trim();
    for &spelling in &["t", "true", "y", "yes", "on", "1"] {
        if trimmed.eq_ignore_ascii_case(spelling) {
            return Some(true);
        }
    }
    for &spelling in &["f", "false", "n", "no", "off", "0"] {
        if trimmed.eq_ignore_ascii_case(spelling) {
            return Some(false);
        }
    }
    None
}

/// `expr` without an identity cast: `column::text` on a text column is the
/// column itself. Any other cast, or a cast of a non-column, is kept.
pub fn identity_cast_strip<'t>(bound: BoundView<'_>, expr: Scalar<'t>) -> Scalar<'t> {
    let ScalarKind::TypeCast(cast) = expr.kind() else {
        return expr;
    };
    let inner = cast.expr();
    let ScalarKind::Column(column) = inner.kind() else {
        return expr;
    };
    match bound.column_type_at(column.id()) {
        Some(base) if cast_target_is_identity_on(cast.target(), base) => inner,
        Some(_) | None => expr,
    }
}

/// The scalar a WHERE operand holds, without an identity cast.
pub fn scalar_leaf<'t>(bound: BoundView<'_>, expr: Where<'t>) -> Option<Scalar<'t>> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => Some(identity_cast_strip(bound, scalar)),
        WhereKind::Unary(_)
        | WhereKind::Binary(_)
        | WhereKind::Multi(_)
        | WhereKind::Subquery(_) => None,
    }
}

pub fn comparison_canonicalize<'t>(
    bound: BoundView<'_>,
    binary: Binary<'t>,
) -> Option<(
    Column<'t>,
    Option<&'t CastTarget>,
    BinaryOp,
    &'t LiteralValue,
)> {
    let lleaf = scalar_leaf(bound, binary.lexpr());
    let rleaf = scalar_leaf(bound, binary.rexpr());
    let column_target = |scalar: Scalar<'t>| match scalar.kind() {
        ScalarKind::Column(column) => Some((column, None)),
        ScalarKind::TypeCast(cast) => match cast.expr().kind() {
            ScalarKind::Column(column) => Some((column, Some(cast.target()))),
            ScalarKind::Function(_)
            | ScalarKind::Literal(_)
            | ScalarKind::Case(_)
            | ScalarKind::Operator(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => None,
        },
        ScalarKind::Function(_)
        | ScalarKind::Literal(_)
        | ScalarKind::Case(_)
        | ScalarKind::Operator(_)
        | ScalarKind::Subquery(_)
        | ScalarKind::Array(_) => None,
    };
    let literal = |scalar: Option<Scalar<'t>>| match scalar?.kind() {
        ScalarKind::Literal(literal) => Some(literal.value()),
        ScalarKind::Column(_)
        | ScalarKind::Function(_)
        | ScalarKind::Case(_)
        | ScalarKind::Operator(_)
        | ScalarKind::Subquery(_)
        | ScalarKind::Array(_)
        | ScalarKind::TypeCast(_) => None,
    };
    let (column, target, op, value) = if let (Some(left), Some(value)) = (lleaf, literal(rleaf))
        && let Some((column, target)) = column_target(left)
    {
        (column, target, binary.op(), value)
    } else if let (Some(value), Some(right)) = (literal(lleaf), rleaf)
        && let Some((column, target)) = column_target(right)
    {
        (column, target, binary.op().op_flip()?, value)
    } else {
        return None;
    };
    let target = target.filter(|target| {
        !integer_text_equality(bound.column_type_at(column.id()), target, op, value)
    });
    Some((column, target, op, value))
}

/// Whether `integer_column::text op value` holds exactly when
/// `integer_column op value` does: an equality or inequality against the
/// spelling `::text` gives that integer. Text order (`'9' > '10'`) and any
/// other spelling (`'042'` is no integer's text) differ from the integer
/// comparison, and stay a cast.
fn integer_text_equality(
    base: Option<&Type>,
    target: &CastTarget,
    op: BinaryOp,
    value: &LiteralValue,
) -> bool {
    matches!(base, Some(&(Type::INT2 | Type::INT4 | Type::INT8)))
        && *target == CastTarget::Text
        && matches!(op, BinaryOp::Equal | BinaryOp::NotEqual)
        && matches!(value, LiteralValue::String(text)
            if text.parse::<i64>().is_ok_and(|n| n.to_string() == text.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn other(name: &str) -> CastTarget {
        CastTarget::Other(EcoString::from(name))
    }

    /// The integer input rules: whitespace, sign, decimal, prefixed and
    /// underscored forms accepted; anything else and a value past the type's
    /// range rejected.
    #[test]
    fn test_cast_target_rejects_text_for_integers() {
        for accepted in [
            "42", " 42 ", "+7", "-7", "1_000", "0x1A", "0o17", "0b101", "\t9\n",
        ] {
            assert_eq!(
                cast_target_rejects_text(&CastTarget::Int4, accepted),
                None,
                "{accepted}"
            );
        }
        for rejected in [
            "abc", "", " ", "1.5", "1e3", "_1", "1_", "1__0", "0x", "4 2", "٤",
        ] {
            assert!(
                cast_target_rejects_text(&CastTarget::Int4, rejected).is_some(),
                "{rejected}"
            );
        }
        assert_eq!(
            cast_target_rejects_text(&CastTarget::Int4, "2147483647"),
            None
        );
        assert!(cast_target_rejects_text(&CastTarget::Int4, "2147483648").is_some());
        assert_eq!(
            cast_target_rejects_text(&CastTarget::Int4, "-2147483648"),
            None
        );
        assert!(cast_target_rejects_text(&CastTarget::Int4, "-2147483649").is_some());
        assert_eq!(cast_target_rejects_text(&other("int2"), "32767"), None);
        assert!(cast_target_rejects_text(&other("int2"), "32768").is_some());
        assert_eq!(
            cast_target_rejects_text(&CastTarget::Int8, "9223372036854775807"),
            None
        );
        assert!(cast_target_rejects_text(&CastTarget::Int8, "9223372036854775808").is_some());
        assert!(
            cast_target_rejects_text(
                &CastTarget::Int8,
                "99999999999999999999999999999999999999999"
            )
            .is_some()
        );
    }

    /// `numeric`, `bool` and `uuid` input rules; every other type is left to
    /// origin.
    #[test]
    fn test_cast_target_rejects_text_for_numeric_bool_and_uuid() {
        for accepted in [
            "1.5",
            ".5",
            "5.",
            "-0.25",
            "1e5",
            "1.5E-3",
            "NaN",
            "-Infinity",
            "inf",
            " 7 ",
            "1_000.5",
        ] {
            assert_eq!(
                cast_target_rejects_text(&other("numeric"), accepted),
                None,
                "{accepted}"
            );
        }
        for rejected in ["abc", "", "1.2.3", "1e", "e5", ".", "1,5", "1.5 x"] {
            assert!(
                cast_target_rejects_text(&other("numeric"), rejected).is_some(),
                "{rejected}"
            );
        }
        for accepted in ["t", "TRUE", " yes ", "off", "0", "n"] {
            assert_eq!(
                cast_target_rejects_text(&CastTarget::Bool, accepted),
                None,
                "{accepted}"
            );
        }
        for rejected in ["abc", "", "2", "ja"] {
            assert!(
                cast_target_rejects_text(&CastTarget::Bool, rejected).is_some(),
                "{rejected}"
            );
        }
        for accepted in [
            "11111111-1111-1111-1111-111111111111",
            "11111111111111111111111111111111",
            "{11111111-1111-1111-1111-111111111111}",
            "1111-1111-1111-1111-1111-1111-1111-1111",
            "A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11",
        ] {
            assert_eq!(
                cast_target_rejects_text(&other("uuid"), accepted),
                None,
                "{accepted}"
            );
        }
        for rejected in [
            "ann",
            "",
            "11111111-1111-1111-1111-11111111111",
            "11111111-1111-1111-1111-1111111111111",
            "1111111-11111-1111-1111-111111111111",
            "11111111--1111-1111-1111-111111111111",
            "-11111111-1111-1111-1111-111111111111",
            "11111111-1111-1111-1111-111111111111-",
            "{11111111-1111-1111-1111-111111111111",
            " 11111111-1111-1111-1111-111111111111",
        ] {
            assert!(
                cast_target_rejects_text(&other("uuid"), rejected).is_some(),
                "{rejected}"
            );
        }
        for (target, text) in [
            (CastTarget::Text, "anything"),
            (CastTarget::Date, "not a date"),
            (other("numeric(10,2)"), "abc"),
            (other("timestamptz"), "abc"),
            (other("float8"), "abc"),
        ] {
            assert_eq!(
                cast_target_rejects_text(&target, text),
                None,
                "{target:?} {text}"
            );
        }
    }

    #[test]
    fn test_cast_target_from_canonical_text() {
        assert_eq!(cast_target_from_canonical("text"), CastTarget::Text);
    }

    #[test]
    fn test_cast_target_from_canonical_unknown_becomes_other() {
        let target = cast_target_from_canonical("numeric(10,2)");
        assert_eq!(target, CastTarget::Other(EcoString::from("numeric(10,2)")));
    }

    #[test]
    fn test_cast_target_deparse_roundtrip_text() {
        assert_eq!(cast_target_deparse(&CastTarget::Text), "text");
    }

    #[test]
    fn test_cast_target_deparse_roundtrip_other() {
        let raw = "varchar(100)";
        let target = cast_target_from_canonical(raw);
        assert_eq!(cast_target_deparse(&target), raw);
    }

    #[test]
    fn test_identity_text_cast_on_text_column() {
        assert!(cast_target_is_identity_on(&CastTarget::Text, &Type::TEXT));
    }

    #[test]
    fn test_identity_text_cast_on_varchar_column() {
        assert!(cast_target_is_identity_on(
            &CastTarget::Text,
            &Type::VARCHAR
        ));
    }

    /// An integer's `::text` compares as text, so it is no identity on its
    /// own; only an equality against the integer's own spelling is the
    /// integer comparison.
    #[test]
    fn test_integer_text_cast_is_identity_only_in_exact_equality() {
        let text = |s: &str| LiteralValue::String(s.into());
        for base in [Type::INT2, Type::INT4, Type::INT8] {
            assert!(!cast_target_is_identity_on(&CastTarget::Text, &base));
            let holds = |op, value: &LiteralValue| {
                integer_text_equality(Some(&base), &CastTarget::Text, op, value)
            };
            assert!(holds(BinaryOp::Equal, &text("42")));
            assert!(holds(BinaryOp::NotEqual, &text("-7")));
            assert!(!holds(BinaryOp::GreaterThan, &text("10")));
            assert!(!holds(BinaryOp::Equal, &text("042")));
            assert!(!holds(BinaryOp::Equal, &text("+42")));
            assert!(!holds(BinaryOp::Equal, &text(" 42")));
            assert!(!holds(BinaryOp::Equal, &LiteralValue::Integer(42)));
        }
        assert!(!integer_text_equality(
            Some(&Type::NUMERIC),
            &CastTarget::Text,
            BinaryOp::Equal,
            &text("42")
        ));
        assert!(!integer_text_equality(
            Some(&Type::INT4),
            &CastTarget::Int4,
            BinaryOp::Equal,
            &text("42")
        ));
    }

    #[test]
    fn test_identity_text_cast_on_numeric_column_rejected() {
        // Numeric wire-text formatting can differ from `::text` output
        // (trailing zeros, locale). Stays opaque until a later PR teaches it.
        assert!(!cast_target_is_identity_on(
            &CastTarget::Text,
            &Type::NUMERIC
        ));
    }

    #[test]
    fn test_identity_text_cast_on_float_column_rejected() {
        assert!(!cast_target_is_identity_on(
            &CastTarget::Text,
            &Type::FLOAT8
        ));
    }

    #[test]
    fn test_identity_other_target_always_rejected() {
        let target = CastTarget::Other(EcoString::from("numeric"));
        assert!(!cast_target_is_identity_on(&target, &Type::TEXT));
        assert!(!cast_target_is_identity_on(&target, &Type::NUMERIC));
    }

    // ---------------------------------------------------------------
    // PGC-178: int4 / int8 cast targets — canonical mapping, deparse,
    // self-identity, and text-coercion.
    // ---------------------------------------------------------------

    #[test]
    fn test_cast_target_from_canonical_int4() {
        // pg_query canonicalizes INT / INTEGER / INT4 → "int4" before reaching us.
        assert_eq!(cast_target_from_canonical("int4"), CastTarget::Int4);
    }

    #[test]
    fn test_cast_target_from_canonical_int8() {
        assert_eq!(cast_target_from_canonical("int8"), CastTarget::Int8);
    }

    #[test]
    fn test_cast_target_deparse_roundtrip_int4_int8() {
        assert_eq!(cast_target_deparse(&CastTarget::Int4), "int4");
        assert_eq!(cast_target_deparse(&CastTarget::Int8), "int8");
    }

    #[test]
    fn test_identity_int4_self_cast() {
        assert!(cast_target_is_identity_on(&CastTarget::Int4, &Type::INT4));
    }

    #[test]
    fn test_identity_int8_self_cast() {
        assert!(cast_target_is_identity_on(&CastTarget::Int8, &Type::INT8));
    }

    #[test]
    fn test_identity_int4_on_int8_column_rejected() {
        // int8 → int4 narrows and can overflow; not identity even though
        // small int8 values would happen to round-trip.
        assert!(!cast_target_is_identity_on(&CastTarget::Int4, &Type::INT8));
    }

    #[test]
    fn test_identity_int4_on_text_column_rejected() {
        // text → int4 is a coercion, not identity; handled by the coercion
        // path, not by stripping.
        assert!(!cast_target_is_identity_on(&CastTarget::Int4, &Type::TEXT));
    }

    #[test]
    fn test_coercion_supported_int4_on_text_column() {
        assert!(cast_target_is_coercion_supported(
            &CastTarget::Int4,
            &Type::TEXT
        ));
    }

    #[test]
    fn test_coercion_supported_int8_on_varchar_column() {
        assert!(cast_target_is_coercion_supported(
            &CastTarget::Int8,
            &Type::VARCHAR
        ));
    }

    #[test]
    fn test_coercion_unsupported_int4_on_numeric_column() {
        assert!(!cast_target_is_coercion_supported(
            &CastTarget::Int4,
            &Type::NUMERIC
        ));
    }

    #[test]
    fn test_coercion_unsupported_text_target() {
        // ::text is handled by identity-strip, not by coercion. The coercion
        // API explicitly rejects it so the classifier doesn't double-route.
        assert!(!cast_target_is_coercion_supported(
            &CastTarget::Text,
            &Type::TEXT
        ));
    }

    #[test]
    fn test_coerce_text_int4_parses_decimal() {
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Int4, "42"),
            Some(LiteralValue::Integer(42))
        );
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Int4, "-7"),
            Some(LiteralValue::Integer(-7))
        );
    }

    #[test]
    fn test_coerce_text_int8_parses_wide_range() {
        // Beyond int4 range, fits in int8 (we store both as i64).
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Int8, "9223372036854775807"),
            Some(LiteralValue::Integer(i64::MAX))
        );
    }

    #[test]
    fn test_coerce_text_int4_parse_failure_returns_none() {
        // `'abc'::int4` would raise in postgres; locally we say the row
        // simply doesn't match the predicate.
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Int4, "not-an-int"),
            None
        );
    }

    #[test]
    fn test_coerce_text_unsupported_target_returns_none() {
        assert_eq!(cast_target_coerce_text(&CastTarget::Text, "42"), None);
        let other = CastTarget::Other(EcoString::from("date"));
        assert_eq!(cast_target_coerce_text(&other, "2024-01-01"), None);
    }

    // ---------------------------------------------------------------
    // PGC-181: ::bool cast target — canonical mapping, deparse,
    // self-identity, coercion-supported gate, and text-to-bool coercion.
    // ---------------------------------------------------------------

    #[test]
    fn test_cast_target_from_canonical_bool() {
        // pg_query canonicalizes both `bool` and `boolean` to "bool".
        assert_eq!(cast_target_from_canonical("bool"), CastTarget::Bool);
    }

    #[test]
    fn test_cast_target_deparse_roundtrip_bool() {
        assert_eq!(cast_target_deparse(&CastTarget::Bool), "bool");
    }

    #[test]
    fn test_identity_bool_self_cast() {
        assert!(cast_target_is_identity_on(&CastTarget::Bool, &Type::BOOL));
    }

    #[test]
    fn test_identity_bool_on_text_column_rejected() {
        // text→bool is a coercion, not identity (text "t" → bool true, but
        // text "true" → bool true also — different bytes, same logical value).
        assert!(!cast_target_is_identity_on(&CastTarget::Bool, &Type::TEXT));
    }

    #[test]
    fn test_coercion_supported_bool_on_text_column() {
        assert!(cast_target_is_coercion_supported(
            &CastTarget::Bool,
            &Type::TEXT
        ));
    }

    #[test]
    fn test_coercion_supported_bool_on_varchar_column() {
        assert!(cast_target_is_coercion_supported(
            &CastTarget::Bool,
            &Type::VARCHAR
        ));
    }

    #[test]
    fn test_coercion_unsupported_bool_on_int_column() {
        // int→bool needs explicit `1/0` semantics; defer outside the wedge.
        assert!(!cast_target_is_coercion_supported(
            &CastTarget::Bool,
            &Type::INT4
        ));
    }

    #[test]
    fn test_bool_literal_parse_true_spellings() {
        for s in [
            "t", "T", "true", "True", "TRUE", "y", "yes", "YES", "on", "1",
        ] {
            assert_eq!(
                bool_literal_parse(s),
                Some(true),
                "spelling {s:?} should be true"
            );
        }
    }

    #[test]
    fn test_bool_literal_parse_false_spellings() {
        for s in [
            "f", "F", "false", "False", "FALSE", "n", "no", "NO", "off", "0",
        ] {
            assert_eq!(
                bool_literal_parse(s),
                Some(false),
                "spelling {s:?} should be false"
            );
        }
    }

    #[test]
    fn test_bool_literal_parse_trims_whitespace() {
        assert_eq!(bool_literal_parse("  true  "), Some(true));
        assert_eq!(bool_literal_parse("\tno\n"), Some(false));
    }

    #[test]
    fn test_bool_literal_parse_rejects_nonsense() {
        assert_eq!(bool_literal_parse(""), None);
        assert_eq!(bool_literal_parse("   "), None);
        assert_eq!(bool_literal_parse("maybe"), None);
        assert_eq!(bool_literal_parse("2"), None);
    }

    #[test]
    fn test_coerce_text_bool_parses_common_forms() {
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Bool, "true"),
            Some(LiteralValue::Boolean(true))
        );
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Bool, "f"),
            Some(LiteralValue::Boolean(false))
        );
    }

    #[test]
    fn test_coerce_text_bool_parse_failure_returns_none() {
        assert_eq!(cast_target_coerce_text(&CastTarget::Bool, "maybe"), None);
    }

    // ---------------------------------------------------------------
    // PGC-180: ::date cast target — canonical mapping, deparse,
    // self-identity, coercion from `timestamp`, and literal validation.
    //
    // `timestamptz::date` is intentionally not in the coercion table — it
    // depends on session TZ (tracked in PGC-187).
    // ---------------------------------------------------------------

    #[test]
    fn test_cast_target_from_canonical_date() {
        assert_eq!(cast_target_from_canonical("date"), CastTarget::Date);
    }

    #[test]
    fn test_cast_target_deparse_roundtrip_date() {
        assert_eq!(cast_target_deparse(&CastTarget::Date), "date");
    }

    #[test]
    fn test_identity_date_self_cast() {
        assert!(cast_target_is_identity_on(&CastTarget::Date, &Type::DATE));
    }

    #[test]
    fn test_identity_date_on_timestamp_rejected() {
        // timestamp → date narrows; not identity.
        assert!(!cast_target_is_identity_on(
            &CastTarget::Date,
            &Type::TIMESTAMP
        ));
    }

    #[test]
    fn test_coercion_supported_date_on_timestamp() {
        assert!(cast_target_is_coercion_supported(
            &CastTarget::Date,
            &Type::TIMESTAMP
        ));
    }

    #[test]
    fn test_coercion_unsupported_date_on_timestamptz() {
        // Deferred: needs session-TZ tracking (PGC-187).
        assert!(!cast_target_is_coercion_supported(
            &CastTarget::Date,
            &Type::TIMESTAMPTZ
        ));
    }

    #[test]
    fn test_coercion_unsupported_date_on_text() {
        // Parsing arbitrary date text is bigger than the wedge intends to
        // handle; ::date coercion is rooted at `timestamp` only for now.
        assert!(!cast_target_is_coercion_supported(
            &CastTarget::Date,
            &Type::TEXT
        ));
    }

    #[test]
    fn test_coerce_text_date_from_timestamp_with_space() {
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "2024-01-15 23:45:00"),
            Some(LiteralValue::String("2024-01-15".into()))
        );
    }

    #[test]
    fn test_coerce_text_date_from_timestamp_with_t_separator() {
        // ISO 8601 with `T` separator.
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "2024-01-15T09:00:00"),
            Some(LiteralValue::String("2024-01-15".into()))
        );
    }

    #[test]
    fn test_coerce_text_date_from_timestamp_with_fractional_seconds() {
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "2024-01-15 23:45:00.123456"),
            Some(LiteralValue::String("2024-01-15".into()))
        );
    }

    #[test]
    fn test_coerce_text_date_from_bare_date() {
        // A timestamp value can also be sent as just `YYYY-MM-DD` (midnight
        // truncation). Accept that too.
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "2024-01-15"),
            Some(LiteralValue::String("2024-01-15".into()))
        );
    }

    #[test]
    fn test_coerce_text_date_rejects_non_canonical_prefix() {
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "2024-1-15"),
            None
        );
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "20240115 09:00:00"),
            None
        );
    }

    #[test]
    fn test_coerce_text_date_rejects_bad_separator() {
        // 10 chars look like a date but the 11th isn't ` ` or `T`.
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "2024-01-15X09:00"),
            None
        );
    }

    #[test]
    fn test_coerce_text_date_rejects_garbage() {
        assert_eq!(cast_target_coerce_text(&CastTarget::Date, ""), None);
        assert_eq!(
            cast_target_coerce_text(&CastTarget::Date, "not-a-date"),
            None
        );
    }

    #[test]
    fn test_is_canonical_date_literal_accepts_yyyy_mm_dd() {
        assert!(is_canonical_date_literal("2024-01-15"));
        assert!(is_canonical_date_literal("2024-12-31"));
        assert!(is_canonical_date_literal("0001-01-01"));
    }

    #[test]
    fn test_is_canonical_date_literal_rejects_non_canonical() {
        assert!(!is_canonical_date_literal("2024-1-15"));
        assert!(!is_canonical_date_literal("2024-01-15 "));
        assert!(!is_canonical_date_literal("2024/01/15"));
        assert!(!is_canonical_date_literal("01/15/2024"));
        assert!(!is_canonical_date_literal(""));
        assert!(!is_canonical_date_literal("2024-01-15T00:00:00"));
    }
}
