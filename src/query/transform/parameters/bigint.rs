//! Decode a Bind value sitting where PostgreSQL requires a bigint (LIMIT,
//! OFFSET). The position fixes the type, so a parameter the client left
//! undeclared is the bigint the server infers, not a string.

use postgres_types::Type as PgType;
use rootcause::Report;

use super::parameter_to_literal;
use super::replace::parameter_index_parse;
use crate::cache::{QueryParameter, QueryParameters};
use crate::oid::TypeOid;
use crate::query::ast::LiteralValue;
use crate::query::transform::{AstTransformError, AstTransformResult};

/// The bigint bound to `placeholder`, `None` for NULL.
pub(crate) fn placeholder_bigint_decode(
    placeholder: &str,
    parameters: &QueryParameters,
) -> AstTransformResult<Option<i64>> {
    let index = parameter_index_parse(placeholder)?;
    let parameter = parameters.get(index).ok_or_else(|| {
        Report::from(AstTransformError::ParameterOutOfBounds {
            index,
            count: parameters.len(),
        })
    })?;
    parameter_bigint_decode(&parameter)
}

fn parameter_bigint_decode(parameter: &QueryParameter) -> AstTransformResult<Option<i64>> {
    let Some(bytes) = &parameter.value else {
        return Ok(None);
    };
    if oid_is_undeclared(parameter.oid) {
        let value = if parameter.format == 0 {
            std::str::from_utf8(bytes).ok().and_then(bigint_text_parse)
        } else {
            <[u8; 8]>::try_from(bytes.as_ref())
                .ok()
                .map(i64::from_be_bytes)
        };
        return value.map(Some).ok_or_else(not_a_bigint);
    }
    // A declared numeric or float is rounded by origin and a declared text is
    // rejected by it, so only the integer types decode.
    match parameter_to_literal(parameter)? {
        LiteralValue::Integer(value) => Ok(Some(value)),
        LiteralValue::String(_)
        | LiteralValue::StringWithCast(..)
        | LiteralValue::Float(_)
        | LiteralValue::Boolean(_)
        | LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_)
        | LiteralValue::SessionTerm(_)
        | LiteralValue::Array(..) => Err(not_a_bigint()),
    }
}

/// A parameter declared `unknown` is typed by its position, as one left
/// undeclared is.
fn oid_is_undeclared(oid: TypeOid) -> bool {
    oid == TypeOid::UNSPECIFIED || oid == TypeOid::from_type(&PgType::UNKNOWN)
}

/// `text` as PostgreSQL's bigint input reads it, for the decimal spelling:
/// an optional sign and digits, with whitespace around them. The hex, octal,
/// binary and underscore spellings are left to origin.
pub(crate) fn bigint_text_parse(text: &str) -> Option<i64> {
    text.trim_matches(|c| matches!(c, ' ' | '\t' | '\n' | '\x0B' | '\x0C' | '\r'))
        .parse()
        .ok()
}

fn not_a_bigint() -> Report<AstTransformError> {
    Report::from(AstTransformError::InvalidParameterValue {
        message: "value in a bigint position is not a bigint".to_owned(),
    })
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use super::*;

    /// Name, value, format, declared type, and the bigint decoded (`None`
    /// for an error).
    type Case<'a> = (&'a str, Option<&'a [u8]>, i16, TypeOid, Option<Option<i64>>);

    fn parameters(value: Option<&[u8]>, format: i16, oid: TypeOid) -> QueryParameters {
        QueryParameters {
            values: vec![value.map(Bytes::copy_from_slice)],
            formats: vec![format],
            oids: vec![oid],
        }
    }

    fn decoded(value: Option<&[u8]>, format: i16, oid: TypeOid) -> AstTransformResult<Option<i64>> {
        placeholder_bigint_decode("$1", &parameters(value, format, oid))
    }

    #[test]
    fn test_placeholder_bigint_decode_cases() {
        let undeclared = TypeOid::UNSPECIFIED;
        let of = |ty: PgType| TypeOid::from_type(&ty);
        let seven = 7i64.to_be_bytes();
        let seven_int4 = 7i32.to_be_bytes();
        #[rustfmt::skip]
        let cases: Vec<Case<'_>> = vec![
            ("undeclared text",            Some(b"1000"),    0, undeclared,        Some(Some(1000))),
            ("undeclared text, signed",    Some(b"-3"),      0, undeclared,        Some(Some(-3))),
            ("undeclared text, padded",    Some(b" 7\n"),    0, undeclared,        Some(Some(7))),
            ("undeclared text, hex",       Some(b"0x10"),    0, undeclared,        None),
            ("undeclared text, decimal",   Some(b"2.5"),     0, undeclared,        None),
            ("undeclared text, word",      Some(b"ten"),     0, undeclared,        None),
            ("undeclared text, empty",     Some(b""),        0, undeclared,        None),
            ("undeclared binary",          Some(&seven),     1, undeclared,        Some(Some(7))),
            ("undeclared binary, 4 bytes", Some(&seven_int4), 1, undeclared,       None),
            ("declared unknown, text",     Some(b"10"),      0, of(PgType::UNKNOWN), Some(Some(10))),
            ("declared unknown, binary",   Some(&seven),     1, of(PgType::UNKNOWN), Some(Some(7))),
            ("null",                       None,             0, undeclared,        Some(None)),
            ("null, declared text",        None,             0, of(PgType::TEXT),  Some(None)),
            ("int4 text",                  Some(b"10"),      0, of(PgType::INT4),  Some(Some(10))),
            ("int4 binary",                Some(&seven_int4), 1, of(PgType::INT4), Some(Some(7))),
            ("int8 binary",                Some(&seven),     1, of(PgType::INT8),  Some(Some(7))),
            ("int4 text, word",            Some(b"ten"),     0, of(PgType::INT4),  None),
            ("declared text",              Some(b"10"),      0, of(PgType::TEXT),  None),
            ("declared numeric",           Some(b"10"),      0, of(PgType::NUMERIC), None),
            ("declared float8",            Some(b"2.5"),     0, of(PgType::FLOAT8), None),
        ];
        for (name, value, format, oid, expected) in cases {
            assert_eq!(decoded(value, format, oid).ok(), expected, "{name}");
        }
    }

    #[test]
    fn test_placeholder_bigint_decode_reports_a_missing_value() {
        assert!(decoded(Some(b"1"), 0, TypeOid::UNSPECIFIED).is_ok());
        let parameters = parameters(Some(b"1"), 0, TypeOid::UNSPECIFIED);
        assert!(placeholder_bigint_decode("$2", &parameters).is_err());
    }
}
