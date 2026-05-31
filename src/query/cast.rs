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
use tokio_postgres::types::Type;

use crate::query::ast::{BinaryOp, LiteralValue};
use crate::query::resolved::{
    ResolvedBinaryExpr, ResolvedColumnNode, ResolvedScalarExpr, ResolvedWhereExpr,
};

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
/// `::text` identity covers:
/// * `text` / `varchar` — trivially the same bytes.
/// * `int2` / `int4` / `int8` — wire-text is decimal digits (with optional
///   leading `-`), exactly what `::text` produces. No locale, no separators.
///
/// `::int4` / `::int8` identity covers self-casts (`int4_col::int4` etc.) —
/// the cast is a no-op an ORM sometimes still emits.
pub fn cast_target_is_identity_on(target: &CastTarget, base: &Type) -> bool {
    match target {
        CastTarget::Text => matches!(
            *base,
            Type::TEXT | Type::VARCHAR | Type::INT2 | Type::INT4 | Type::INT8
        ),
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
        CastTarget::Bool => parse_pg_bool(row_text).map(LiteralValue::Boolean),
        CastTarget::Date => {
            timestamp_text_to_date(row_text).map(|s| LiteralValue::String(s.into()))
        }
        CastTarget::Text | CastTarget::Other(_) => None,
    }
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
    let bytes = s.as_bytes();
    if bytes.len() != 10 {
        return false;
    }
    let chunk: &[u8; 10] = bytes.try_into().expect("len checked above");
    is_canonical_date_prefix(chunk)
}

/// Parse postgres-style boolean text. Accepts the common spellings
/// (`true`/`false`/`t`/`f`/`yes`/`no`/`on`/`off`/`1`/`0`), case-insensitive,
/// with leading/trailing whitespace trimmed. Returns `None` for anything
/// else — postgres would raise; locally the row simply doesn't match.
pub fn parse_pg_bool(s: &str) -> Option<bool> {
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

/// Strip a `TypeCast { Column, target }` wrapper when the cast is an
/// identity coercion on that column's type. Other shapes pass through.
/// Shared by evaluator, classifier, and constraints extractor so they apply
/// the same whitelist.
pub fn resolved_strip_identity_cast(expr: &ResolvedScalarExpr) -> &ResolvedScalarExpr {
    let ResolvedScalarExpr::TypeCast {
        expr: inner,
        target,
    } = expr
    else {
        return expr;
    };
    let ResolvedScalarExpr::Column(col) = inner.as_ref() else {
        return expr;
    };
    if cast_target_is_identity_on(target, &col.column_metadata.data_type) {
        inner.as_ref()
    } else {
        expr
    }
}

/// Unwrap a `ResolvedWhereExpr::Scalar(...)` and strip an identity TypeCast,
/// so callers can pattern-match on a bare Column/Literal leaf even when the
/// source SQL was `col::text op '...'`. Returns `None` for non-Scalar shapes.
pub fn resolved_where_scalar_leaf(expr: &ResolvedWhereExpr) -> Option<&ResolvedScalarExpr> {
    if let ResolvedWhereExpr::Scalar(scalar) = expr {
        Some(resolved_strip_identity_cast(scalar))
    } else {
        None
    }
}

/// Canonicalize a binary comparison into column-LHS form:
/// `(column, optional cast target, op-with-column-on-LHS, literal)`. Identity
/// casts have already been stripped by `resolved_where_scalar_leaf`, so a
/// present `Some(target)` means a real coercion is required at evaluation
/// time. When the literal was on the LHS of the SQL (`5 < col`), the returned
/// `op` is flipped via `BinaryOp::op_flip` so downstream comparison logic can
/// always assume `column op literal`.
pub fn canonicalize_comparison<'a>(
    binary: &'a ResolvedBinaryExpr,
) -> Option<(
    &'a ResolvedColumnNode,
    Option<&'a CastTarget>,
    BinaryOp,
    &'a LiteralValue,
)> {
    let lleaf = resolved_where_scalar_leaf(&binary.lexpr);
    let rleaf = resolved_where_scalar_leaf(&binary.rexpr);

    let column_target = |scalar: &'a ResolvedScalarExpr| match scalar {
        ResolvedScalarExpr::Column(col) => Some((col, None)),
        ResolvedScalarExpr::TypeCast { expr, target } => {
            if let ResolvedScalarExpr::Column(col) = expr.as_ref() {
                Some((col, Some(target)))
            } else {
                None
            }
        }
        _ => None,
    };

    if let (Some(left), Some(ResolvedScalarExpr::Literal(val))) = (lleaf, rleaf)
        && let Some((col, target)) = column_target(left)
    {
        return Some((col, target, binary.op, val));
    }
    if let (Some(ResolvedScalarExpr::Literal(val)), Some(right)) = (lleaf, rleaf)
        && let Some((col, target)) = column_target(right)
    {
        let flipped = binary.op.op_flip()?;
        return Some((col, target, flipped, val));
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

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

    #[test]
    fn test_identity_text_cast_on_int4_column() {
        assert!(cast_target_is_identity_on(&CastTarget::Text, &Type::INT4));
    }

    #[test]
    fn test_identity_text_cast_on_int2_column() {
        assert!(cast_target_is_identity_on(&CastTarget::Text, &Type::INT2));
    }

    #[test]
    fn test_identity_text_cast_on_int8_column() {
        assert!(cast_target_is_identity_on(&CastTarget::Text, &Type::INT8));
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
    fn test_parse_pg_bool_true_spellings() {
        for s in [
            "t", "T", "true", "True", "TRUE", "y", "yes", "YES", "on", "1",
        ] {
            assert_eq!(
                parse_pg_bool(s),
                Some(true),
                "spelling {s:?} should be true"
            );
        }
    }

    #[test]
    fn test_parse_pg_bool_false_spellings() {
        for s in [
            "f", "F", "false", "False", "FALSE", "n", "no", "NO", "off", "0",
        ] {
            assert_eq!(
                parse_pg_bool(s),
                Some(false),
                "spelling {s:?} should be false"
            );
        }
    }

    #[test]
    fn test_parse_pg_bool_trims_whitespace() {
        assert_eq!(parse_pg_bool("  true  "), Some(true));
        assert_eq!(parse_pg_bool("\tno\n"), Some(false));
    }

    #[test]
    fn test_parse_pg_bool_rejects_nonsense() {
        assert_eq!(parse_pg_bool(""), None);
        assert_eq!(parse_pg_bool("   "), None);
        assert_eq!(parse_pg_bool("maybe"), None);
        assert_eq!(parse_pg_bool("2"), None);
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
