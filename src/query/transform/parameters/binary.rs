//! Binary-format parameter decoding (Bind format=1). Each PG type has its
//! own wire format; we decode to a typed `LiteralValue` (or a
//! `StringWithCast` carrying canonical PG text) so the deparsed SQL
//! re-binds safely without round-tripping through PG's text input parser.

use ecow::EcoString;
use fallible_iterator::FallibleIterator;
use ordered_float::NotNan;
use postgres_protocol::types as pg_types;
use postgres_types::{Kind, Type as PgType};
use rootcause::Report;

use crate::oid::TypeOid;
use crate::query::ast::LiteralValue;
use crate::query::transform::{AstTransformError, AstTransformResult};

mod canonical_text;

use canonical_text::{
    bytea_to_hex_literal, inet_to_text, interval_to_text, macaddr_to_text, numeric_parse_wire,
    numeric_to_text, pg_days_to_ymd, time_micros_to_text, timestamp_micros_to_text, timetz_to_text,
    ymd_to_text,
};

/// A family decoder's answer: `None` when the type isn't one of its own.
type FamilyDecode = Option<AstTransformResult<LiteralValue>>;

fn invalid_value(message: impl Into<String>) -> Report<AstTransformError> {
    Report::from(AstTransformError::InvalidParameterValue {
        message: message.into(),
    })
}

/// Wrap a decode failure for `type_name`'s binary wire format.
fn invalid_param(type_name: &str, e: impl std::fmt::Display) -> Report<AstTransformError> {
    invalid_value(format!("invalid binary {type_name}: {e}"))
}

fn length_invalid(type_name: &str, expected: usize, got: usize) -> Report<AstTransformError> {
    invalid_value(format!(
        "invalid {type_name} length: expected {expected} bytes, got {got}"
    ))
}

fn unsupported(oid: TypeOid) -> Report<AstTransformError> {
    Report::from(AstTransformError::UnsupportedBinaryFormat { oid })
}

/// A wire value that must be exactly `N` bytes.
fn fixed_width<'a, const N: usize>(
    bytes: &'a [u8],
    type_name: &str,
) -> AstTransformResult<&'a [u8; N]> {
    bytes
        .try_into()
        .map_err(|_| length_invalid(type_name, N, bytes.len()))
}

fn be_i64(bytes: &[u8], at: usize) -> Option<i64> {
    let field = bytes.get(at..at.checked_add(8)?)?;
    field.try_into().ok().map(i64::from_be_bytes)
}

fn be_i32(bytes: &[u8], at: usize) -> Option<i32> {
    let field = bytes.get(at..at.checked_add(4)?)?;
    field.try_into().ok().map(i32::from_be_bytes)
}

/// Canonical text carrying an explicit cast to `ty`.
fn cast_literal(text: impl Into<EcoString>, ty: &PgType) -> LiteralValue {
    LiteralValue::StringWithCast(text.into(), ty.name().into())
}

fn float_literal(value: f64) -> AstTransformResult<LiteralValue> {
    NotNan::new(value)
        .map(LiteralValue::Float)
        .map_err(|_| invalid_value("NaN is not a valid float value"))
}

pub(super) fn binary_parameter_to_literal(
    bytes: &[u8],
    oid: TypeOid,
) -> AstTransformResult<LiteralValue> {
    if let Some(ty) = oid.pg_type()
        && matches!(ty.kind(), Kind::Array(_))
    {
        return binary_array_to_literal(bytes, oid);
    }
    binary_parameter_to_literal_scalar(bytes, oid)
}

/// Unknown OIDs and types without a decoder route to
/// `UnsupportedBinaryFormat` so the query falls through to origin uncached.
fn binary_parameter_to_literal_scalar(
    bytes: &[u8],
    oid: TypeOid,
) -> AstTransformResult<LiteralValue> {
    let Some(ty) = oid.pg_type() else {
        return Err(unsupported(oid));
    };
    if let Some(decoded) = kind_dispatch(bytes, &ty) {
        return decoded;
    }
    number_decode(bytes, &ty)
        .or_else(|| text_decode(bytes, &ty))
        .or_else(|| datetime_decode(bytes, &ty))
        .or_else(|| network_decode(bytes, &ty))
        .unwrap_or_else(|| Err(unsupported(oid)))
}

/// Fail-closed Kind dispatch: only `Simple` types reach the per-OID
/// decoders. The previous UTF-8 catch-all silently corrupted SQL whenever a
/// binary wire format happened to be valid UTF-8.
fn kind_dispatch(bytes: &[u8], ty: &PgType) -> FamilyDecode {
    match ty.kind() {
        Kind::Simple => None,
        Kind::Domain(base) => Some(binary_parameter_to_literal_scalar(
            bytes,
            TypeOid::from_type(base),
        )),
        Kind::Enum(_) => Some(
            std::str::from_utf8(bytes)
                .map(|s| LiteralValue::String(s.into()))
                .map_err(|_| invalid_value("binary enum value is not valid UTF-8")),
        ),
        Kind::Array(_)
        | Kind::Composite(_)
        | Kind::Range(_)
        | Kind::Multirange(_)
        | Kind::Pseudo => Some(Err(unsupported(TypeOid::from_type(ty)))),
        // `Kind` is `#[non_exhaustive]`; new variants must be opted in.
        _ => Some(Err(unsupported(TypeOid::from_type(ty)))),
    }
}

fn number_decode(bytes: &[u8], ty: &PgType) -> FamilyDecode {
    let decoded = match *ty {
        PgType::BOOL => pg_types::bool_from_sql(bytes)
            .map(LiteralValue::Boolean)
            .map_err(|e| invalid_param("bool", e)),
        PgType::INT2 => pg_types::int2_from_sql(bytes)
            .map(|value| LiteralValue::Integer(i64::from(value)))
            .map_err(|e| invalid_param("int2", e)),
        PgType::INT4 => pg_types::int4_from_sql(bytes)
            .map(|value| LiteralValue::Integer(i64::from(value)))
            .map_err(|e| invalid_param("int4", e)),
        PgType::INT8 => pg_types::int8_from_sql(bytes)
            .map(LiteralValue::Integer)
            .map_err(|e| invalid_param("int8", e)),
        PgType::FLOAT4 => pg_types::float4_from_sql(bytes)
            .map_err(|e| invalid_param("float4", e))
            .and_then(|value| float_literal(f64::from(value))),
        PgType::FLOAT8 => pg_types::float8_from_sql(bytes)
            .map_err(|e| invalid_param("float8", e))
            .and_then(float_literal),
        PgType::NUMERIC => numeric_decode(bytes),
        _ => return None,
    };
    Some(decoded)
}

fn numeric_decode(bytes: &[u8]) -> AstTransformResult<LiteralValue> {
    let (weight, sign, dscale, digits) = numeric_parse_wire(bytes)?;
    let text = numeric_to_text(weight, sign, dscale, &digits)
        .ok_or_else(|| invalid_value(format!("invalid numeric sign code: 0x{sign:04x}")))?;
    Ok(cast_literal(text, &PgType::NUMERIC))
}

fn text_decode(bytes: &[u8], ty: &PgType) -> FamilyDecode {
    let decoded = match *ty {
        PgType::TEXT
        | PgType::VARCHAR
        | PgType::BPCHAR
        | PgType::NAME
        | PgType::CHAR
        | PgType::UNKNOWN => pg_types::text_from_sql(bytes)
            .map(|value| LiteralValue::String(value.into()))
            .map_err(|e| invalid_param("text", e)),
        PgType::UUID => uuid_decode(bytes),
        // The leading `\` in `\x<hex>` forces `escape_literal` into E-string
        // form so the SQL stays well-formed.
        PgType::BYTEA => Ok(cast_literal(
            bytea_to_hex_literal(pg_types::bytea_from_sql(bytes)),
            ty,
        )),
        // JSON binary format is plain UTF-8 JSON text — no version prefix or
        // other framing — so it round-trips as a string literal, just with an
        // explicit `::json` cast to preserve the column's expected type.
        PgType::JSON => pg_types::text_from_sql(bytes)
            .map(|value| cast_literal(value, ty))
            .map_err(|e| invalid_param("json", e)),
        PgType::JSONB => jsonb_decode(bytes),
        _ => return None,
    };
    Some(decoded)
}

fn uuid_decode(bytes: &[u8]) -> AstTransformResult<LiteralValue> {
    let bytes = fixed_width::<16>(bytes, "UUID")?;
    let uuid_str = format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0],
        bytes[1],
        bytes[2],
        bytes[3],
        bytes[4],
        bytes[5],
        bytes[6],
        bytes[7],
        bytes[8],
        bytes[9],
        bytes[10],
        bytes[11],
        bytes[12],
        bytes[13],
        bytes[14],
        bytes[15]
    );
    Ok(LiteralValue::String(uuid_str.into()))
}

/// JSONB binary format prepends a 1-byte version (currently 0x01); the rest
/// is UTF-8 JSON text. Strip the prefix so the deparsed SQL is
/// `'<json>'::jsonb` rather than carrying the SOH control byte into the
/// literal.
fn jsonb_decode(bytes: &[u8]) -> AstTransformResult<LiteralValue> {
    let Some((&0x01, json)) = bytes.split_first() else {
        return Err(invalid_value("missing or unknown jsonb version byte"));
    };
    let value =
        std::str::from_utf8(json).map_err(|_| invalid_value("jsonb body is not valid UTF-8"))?;
    Ok(cast_literal(value, &PgType::JSONB))
}

fn datetime_decode(bytes: &[u8], ty: &PgType) -> FamilyDecode {
    let decoded = match *ty {
        PgType::TIME => pg_types::time_from_sql(bytes)
            .map(|micros| cast_literal(time_micros_to_text(micros), ty))
            .map_err(|e| invalid_param("time", e)),
        PgType::DATE => pg_types::date_from_sql(bytes)
            .map(|days| cast_literal(date_text(days), ty))
            .map_err(|e| invalid_param("date", e)),
        PgType::TIMESTAMP => pg_types::timestamp_from_sql(bytes)
            .map(|micros| cast_literal(timestamp_text(micros, ""), ty))
            .map_err(|e| invalid_param("timestamp", e)),
        // Emit explicit `+00` so PG re-parses with zone regardless of session
        // `TimeZone` setting. Wire format matches TIMESTAMP.
        PgType::TIMESTAMPTZ => pg_types::timestamp_from_sql(bytes)
            .map(|micros| cast_literal(timestamp_text(micros, "+00"), ty))
            .map_err(|e| invalid_param("timestamptz", e)),
        PgType::TIMETZ => timetz_decode(bytes),
        PgType::INTERVAL => interval_decode(bytes),
        _ => return None,
    };
    Some(decoded)
}

/// ±i32 sentinels are PG14+ `infinity` / `-infinity`.
fn date_text(days: i32) -> String {
    match days {
        i32::MAX => "infinity".to_owned(),
        i32::MIN => "-infinity".to_owned(),
        _ => {
            let (y, m, d) = pg_days_to_ymd(days);
            ymd_to_text(y, m, d)
        }
    }
}

/// ±i64 sentinels are `infinity` / `-infinity`; finite values carry
/// `zone_suffix`.
fn timestamp_text(micros: i64, zone_suffix: &str) -> String {
    match micros {
        i64::MAX => "infinity".to_owned(),
        i64::MIN => "-infinity".to_owned(),
        _ => {
            let mut text = timestamp_micros_to_text(micros);
            text.push_str(zone_suffix);
            text
        }
    }
}

/// 12 bytes: i64 micros-since-midnight + i32 zone-secs-west-of-UTC.
fn timetz_decode(bytes: &[u8]) -> AstTransformResult<LiteralValue> {
    match (bytes.len(), be_i64(bytes, 0), be_i32(bytes, 8)) {
        (12, Some(micros), Some(zone)) => {
            Ok(cast_literal(timetz_to_text(micros, zone), &PgType::TIMETZ))
        }
        _ => Err(length_invalid("timetz", 12, bytes.len())),
    }
}

/// 16 bytes: i64 micros + i32 days + i32 months.
fn interval_decode(bytes: &[u8]) -> AstTransformResult<LiteralValue> {
    match (
        bytes.len(),
        be_i64(bytes, 0),
        be_i32(bytes, 8),
        be_i32(bytes, 12),
    ) {
        (16, Some(micros), Some(days), Some(months)) => Ok(cast_literal(
            interval_to_text(micros, days, months),
            &PgType::INTERVAL,
        )),
        _ => Err(length_invalid("interval", 16, bytes.len())),
    }
}

fn network_decode(bytes: &[u8], ty: &PgType) -> FamilyDecode {
    let decoded = match *ty {
        PgType::MACADDR => pg_types::macaddr_from_sql(bytes)
            .map(|octets| cast_literal(macaddr_to_text(&octets), ty))
            .map_err(|e| invalid_param("macaddr", e)),
        PgType::MACADDR8 => fixed_width::<8>(bytes, "macaddr8")
            .map(|octets| cast_literal(macaddr_to_text(octets), ty)),
        PgType::INET => pg_types::inet_from_sql(bytes)
            .map(|inet| cast_literal(inet_to_text(&inet, false), ty))
            .map_err(|e| invalid_param("inet", e)),
        PgType::CIDR => pg_types::inet_from_sql(bytes)
            .map(|inet| cast_literal(inet_to_text(&inet, true), ty))
            .map_err(|e| invalid_param("cidr", e)),
        _ => return None,
    };
    Some(decoded)
}

/// Decode a binary array into a `LiteralValue::Array`, which deparses as a PG
/// text array literal like `'{1,2,NULL,3}'::int4[]`.
///
/// Multi-dim arrays and arrays whose element type isn't in the supported
/// scalar set (the same set `binary_parameter_to_literal` handles directly)
/// return `UnsupportedBinaryFormat` so the caller can fall through to
/// origin uncached.
fn binary_array_to_literal(bytes: &[u8], oid: TypeOid) -> AstTransformResult<LiteralValue> {
    let array = pg_types::array_from_sql(bytes).map_err(|e| invalid_param("array", e))?;

    // 1-D arrays only. Multi-dim falls through to origin uncached.
    let mut dims = array.dimensions();
    let bad_dim = |_| unsupported(oid);
    let _ = dims.next().map_err(bad_dim)?;
    if dims.next().map_err(bad_dim)?.is_some() {
        return Err(unsupported(oid));
    }

    let element_type = PgType::from_oid(array.element_type()).ok_or_else(|| unsupported(oid))?;
    let element_oid = TypeOid::from_type(&element_type);

    // Element errors are remapped to the array OID — the query falls
    // through to origin uncached either way, but error context names the
    // outer type the caller asked about.
    let mut values = array.values();
    let mut elements: Vec<LiteralValue> = Vec::with_capacity(values.size_hint().0);
    while let Some(value) = values.next().map_err(bad_dim)? {
        let lit = match value {
            None => LiteralValue::Null,
            Some(elem_bytes) => binary_parameter_to_literal_scalar(elem_bytes, element_oid)
                .map_err(|_| unsupported(oid))?,
        };
        elements.push(lit);
    }

    let mut cast = EcoString::from(element_type.name());
    cast.push_str("[]");
    Ok(LiteralValue::Array(elements, cast))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::canonical_text::{
        NUMERIC_NAN, NUMERIC_NEG, NUMERIC_NINF, NUMERIC_PINF, NUMERIC_POS, USECS_PER_DAY,
    };
    use crate::cache::{QueryParameter, QueryParameters};
    use crate::oid::TypeOid;
    use crate::query::ast::{
        Deparse, LiteralValue, QueryBody, SelectNode, query_expr_fingerprint, query_expr_parse,
    };
    use crate::query::transform::AstTransformError;
    use crate::query::transform::parameters::{
        parameter_to_literal, query_expr_parameters_replace, select_node_parameters_replace,
    };

    fn parse_select_node(sql: &str) -> SelectNode {
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => *node,
            _ => panic!("expected SELECT"),
        }
    }

    fn binary_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), TypeOid::from_type(&t)))
            .unzip();
        QueryParameters {
            values,
            formats: vec![1; len],
            oids,
        }
    }

    fn binary_param(bytes: &[u8], oid: TypeOid) -> QueryParameter {
        QueryParameter {
            value: Some(Bytes::copy_from_slice(bytes)),
            format: 1,
            oid,
        }
    }

    fn binary_decode(bytes: &[u8], ty: PgType) -> LiteralValue {
        parameter_to_literal(&binary_param(bytes, TypeOid::from_type(&ty)))
            .expect("decode binary parameter")
    }

    fn assert_binary_decodes(bytes: &[u8], ty: PgType, expected: LiteralValue) {
        assert_eq!(binary_decode(bytes, ty), expected, "wire bytes {bytes:?}");
    }

    fn assert_binary_cast(bytes: &[u8], ty: PgType, text: &str) {
        let cast = ty.name().into();
        assert_binary_decodes(bytes, ty, LiteralValue::StringWithCast(text.into(), cast));
    }

    fn binary_float(bytes: &[u8], ty: PgType) -> f64 {
        match binary_decode(bytes, ty) {
            LiteralValue::Float(f) => f.into_inner(),
            other => panic!("expected Float literal, got {other:?}"),
        }
    }

    /// Substitute one binary parameter into `sql` and deparse it; the result
    /// must never carry a raw NUL from the wire bytes.
    fn binary_query_render(sql: &str, bytes: &[u8], ty: PgType) -> String {
        let mut node = parse_select_node(sql);
        let params = binary_params(vec![(Some(bytes), ty)]);
        select_node_parameters_replace(&mut node, &params).expect("substitute binary parameter");
        let mut buf = String::new();
        node.deparse(&mut buf);
        assert!(
            !buf.as_bytes().contains(&0),
            "deparsed SQL must not contain NUL bytes; got {buf:?}"
        );
        buf
    }

    /// A 1-D binary array of `elem_type` holding `elements`, each
    /// length-prefixed.
    fn array_bytes(elem_type: PgType, elements: &[&[u8]]) -> Vec<u8> {
        let count = i32::try_from(elements.len()).expect("test element count fits in i32");
        let mut buf = array_header_bytes(elem_type, count);
        for element in elements {
            let len = i32::try_from(element.len()).expect("test element fits in i32");
            buf.extend_from_slice(&len.to_be_bytes());
            buf.extend_from_slice(element);
        }
        buf
    }

    /// The decoded form of a 1-D array of `elem_type` values with these
    /// canonical texts.
    fn cast_array(elem_type: PgType, texts: &[&str]) -> LiteralValue {
        let elements = texts
            .iter()
            .map(|text| LiteralValue::StringWithCast((*text).into(), elem_type.name().into()))
            .collect();
        LiteralValue::Array(elements, format!("{}[]", elem_type.name()).into())
    }

    fn binary_decode_error(bytes: &[u8], oid: TypeOid) -> AstTransformError {
        parameter_to_literal(&binary_param(bytes, oid))
            .expect_err("reject binary parameter")
            .into_current_context()
    }

    fn assert_binary_unsupported(bytes: &[u8], oid: TypeOid) {
        let error = binary_decode_error(bytes, oid);
        assert!(
            matches!(error, AstTransformError::UnsupportedBinaryFormat { .. }),
            "expected UnsupportedBinaryFormat, got {error:?}"
        );
    }

    fn assert_binary_invalid(bytes: &[u8], oid: TypeOid) {
        let error = binary_decode_error(bytes, oid);
        assert!(
            matches!(error, AstTransformError::InvalidParameterValue { .. }),
            "expected InvalidParameterValue, got {error:?}"
        );
    }

    /// Build the 20-byte header (`ndim=1, hasnull=0, elemtype, dim_len,
    /// dim_lower`) shared by every 1-D binary array test payload.
    fn array_header_bytes(elem_type: PgType, n_elements: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(20);
        buf.extend_from_slice(&1_i32.to_be_bytes());
        buf.extend_from_slice(&0_i32.to_be_bytes());
        buf.extend_from_slice(&elem_type.oid().to_be_bytes());
        buf.extend_from_slice(&n_elements.to_be_bytes());
        buf.extend_from_slice(&1_i32.to_be_bytes());
        buf
    }

    fn timetz_bytes(micros: i64, zone_secs: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(12);
        buf.extend_from_slice(&micros.to_be_bytes());
        buf.extend_from_slice(&zone_secs.to_be_bytes());
        buf
    }

    fn interval_bytes(micros: i64, days: i32, months: i32) -> Vec<u8> {
        let mut buf = Vec::with_capacity(16);
        buf.extend_from_slice(&micros.to_be_bytes());
        buf.extend_from_slice(&days.to_be_bytes());
        buf.extend_from_slice(&months.to_be_bytes());
        buf
    }

    fn inet_bytes(addr: &[u8], netmask: u8, is_cidr: bool) -> Vec<u8> {
        let family: u8 = if addr.len() == 4 { 2 } else { 3 };
        let mut buf = Vec::with_capacity(4 + addr.len());
        buf.push(family);
        buf.push(netmask);
        buf.push(u8::from(is_cidr));
        buf.push(u8::try_from(addr.len()).expect("test addr fits in u8"));
        buf.extend_from_slice(addr);
        buf
    }

    /// Build a binary `numeric` payload from its component fields.
    fn numeric_bytes(weight: i16, sign: u16, dscale: i16, digits: &[i16]) -> Vec<u8> {
        let ndigits = i16::try_from(digits.len()).expect("test numeric digit count fits in i16");
        let mut buf = Vec::with_capacity(8 + 2 * digits.len());
        buf.extend_from_slice(&ndigits.to_be_bytes());
        buf.extend_from_slice(&weight.to_be_bytes());
        buf.extend_from_slice(&sign.to_be_bytes());
        buf.extend_from_slice(&dscale.to_be_bytes());
        for d in digits {
            buf.extend_from_slice(&d.to_be_bytes());
        }
        buf
    }

    fn assert_numeric(bytes: Vec<u8>, expected: &str) {
        assert_binary_decodes(
            &bytes,
            PgType::NUMERIC,
            LiteralValue::StringWithCast(expected.into(), "numeric".into()),
        );
    }

    /// Encode a single binary text-array element: i32 length prefix
    /// followed by the UTF-8 bytes.
    fn array_text_element_bytes(s: &str) -> Vec<u8> {
        let len = i32::try_from(s.len()).expect("test element fits in i32");
        let mut buf = Vec::with_capacity(4 + s.len());
        buf.extend_from_slice(&len.to_be_bytes());
        buf.extend_from_slice(s.as_bytes());
        buf
    }

    fn binary_int4_array_42_100() -> Vec<u8> {
        vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x02, // dim 0 length = 2
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A, // 42
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x64, // 100
        ]
    }

    #[test]
    fn test_binary_parameter_bool_true() {
        assert_binary_decodes(&[1], PgType::BOOL, LiteralValue::Boolean(true));
    }

    #[test]
    fn test_binary_parameter_bool_false() {
        assert_binary_decodes(&[0], PgType::BOOL, LiteralValue::Boolean(false));
    }

    #[test]
    fn test_binary_parameter_int2() {
        assert_binary_decodes(&[0x00, 0x2A], PgType::INT2, LiteralValue::Integer(42));
    }

    #[test]
    fn test_binary_parameter_int4() {
        assert_binary_decodes(
            &[0x00, 0x00, 0x00, 0x2A],
            PgType::INT4,
            LiteralValue::Integer(42),
        );
    }

    #[test]
    fn test_binary_parameter_int8() {
        assert_binary_decodes(
            &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x2A],
            PgType::INT8,
            LiteralValue::Integer(42),
        );
    }

    #[test]
    fn test_binary_parameter_float4() {
        let value: f32 = 2.73;
        let bytes = value.to_be_bytes();
        assert!((binary_float(&bytes, PgType::FLOAT4) - 2.73).abs() < 0.001);
    }

    #[test]
    fn test_binary_parameter_float8() {
        let value: f64 = 2.73821;
        let bytes = value.to_be_bytes();
        assert!((binary_float(&bytes, PgType::FLOAT8) - 2.73821).abs() < 0.00001);
    }

    #[test]
    fn test_binary_parameter_text() {
        assert_binary_decodes(
            b"hello world",
            PgType::TEXT,
            LiteralValue::String("hello world".into()),
        );
    }

    #[test]
    fn test_binary_parameter_uuid() {
        let uuid_bytes: [u8; 16] = [
            0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4, 0xa7, 0x16, 0x44, 0x66, 0x55, 0x44,
            0x00, 0x00,
        ];
        assert_binary_decodes(
            &uuid_bytes,
            PgType::UUID,
            LiteralValue::String("550e8400-e29b-41d4-a716-446655440000".into()),
        );
    }

    #[test]
    fn test_binary_parameter_unsupported_type_with_invalid_utf8() {
        // POINT has no decoder arm; bytes are arbitrary garbage. The
        // assertion is that the function rejects rather than coercing.
        assert_binary_unsupported(&[0xFF, 0xFE], TypeOid::from_type(&PgType::POINT));
    }

    #[test]
    fn test_binary_parameter_range_rejected() {
        // `Kind::Range(Int4)`: builtin int4range. Even if the binary range
        // wire format happens to be valid UTF-8, the Kind dispatch rejects.
        assert_binary_unsupported(&[0x01], TypeOid::from_type(&PgType::INT4_RANGE));
    }

    #[test]
    fn test_binary_parameter_multirange_rejected() {
        assert_binary_unsupported(
            &[0x00, 0x00, 0x00, 0x00],
            TypeOid::from_type(&PgType::INT4MULTI_RANGE),
        );
    }

    #[test]
    fn test_binary_parameter_pseudo_rejected() {
        // `record` and `any` are both `Kind::Pseudo`; reject either.
        assert_binary_unsupported(
            &[0x00, 0x00, 0x00, 0x01],
            TypeOid::from_type(&PgType::RECORD),
        );
    }

    #[test]
    fn test_binary_parameter_unknown_oid_rejected() {
        // OID that `PgType::from_oid` can't resolve falls through the Kind
        // dispatch into the fail-closed catch-all in the OID match.
        assert_binary_unsupported(b"42", TypeOid::from_raw(999_999));
    }

    #[test]
    fn test_binary_parameter_json() {
        // JSON binary format is plain UTF-8 JSON, no framing.
        let json = br#"{"a":1,"b":[2,3]}"#;
        assert_binary_decodes(
            json,
            PgType::JSON,
            LiteralValue::StringWithCast(r#"{"a":1,"b":[2,3]}"#.into(), "json".into()),
        );
    }

    #[test]
    fn test_binary_parameter_jsonb_strips_version_byte() {
        // JSONB binary: 0x01 version + UTF-8 JSON. The 0x01 SOH byte must
        // not end up in the deparsed SQL literal.
        let mut bytes = vec![0x01u8];
        bytes.extend_from_slice(br#"{"k":"v"}"#);
        assert_binary_decodes(
            &bytes,
            PgType::JSONB,
            LiteralValue::StringWithCast(r#"{"k":"v"}"#.into(), "jsonb".into()),
        );
    }

    #[test]
    fn test_binary_parameter_jsonb_unknown_version_rejected() {
        // Anything other than the documented 0x01 prefix is malformed.
        let bytes = [0x02, b'{', b'}'];
        assert_binary_invalid(&bytes, TypeOid::from_type(&PgType::JSONB));
    }

    #[test]
    fn test_binary_parameter_bytea() {
        assert_binary_cast(&[0xde, 0xad, 0xbe, 0xef], PgType::BYTEA, "\\xdeadbeef");
    }

    #[test]
    fn test_binary_parameter_bytea_empty() {
        assert_binary_cast(&[], PgType::BYTEA, "\\x");
    }

    #[test]
    fn test_binary_bytea_in_query_renders_clean_sql() {
        assert_eq!(
            binary_query_render(
                "SELECT id FROM blobs WHERE data = $1",
                &[0xde, 0xad, 0xbe, 0xef],
                PgType::BYTEA
            ),
            r"SELECT id FROM blobs WHERE data = E'\\xdeadbeef'::bytea"
        );
    }

    #[test]
    fn test_binary_bytea_array() {
        let bytes = array_bytes(PgType::BYTEA, &[&[0x01], &[0xAB]]);
        let literal = binary_decode(&bytes, PgType::BYTEA_ARRAY);
        assert_eq!(literal, cast_array(PgType::BYTEA, &["\\x01", "\\xab"]));

        // Each `\` is doubled twice on the way out: once by PG-array-text
        // quoting (`\x01` → `"\\x01"`), once by SQL E-string escaping
        // (`\\` → `\\\\`). End result: four backslashes per element.
        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(buf, r#"E'{"\\\\x01","\\\\xab"}'::bytea[]"#);
    }

    #[test]
    fn test_binary_parameter_time_noon() {
        let micros: i64 = 12 * 3600 * 1_000_000;
        assert_binary_cast(&micros.to_be_bytes(), PgType::TIME, "12:00:00.000000");
    }

    #[test]
    fn test_binary_parameter_time_with_micros() {
        // 13:45:30.123456 — exercises the fractional path.
        let micros: i64 = (13 * 3600 + 45 * 60 + 30) * 1_000_000 + 123_456;
        assert_binary_cast(&micros.to_be_bytes(), PgType::TIME, "13:45:30.123456");
    }

    #[test]
    fn test_binary_time_in_query_renders_clean_sql() {
        let micros: i64 = 9 * 3600 * 1_000_000;
        assert_eq!(
            binary_query_render(
                "SELECT id FROM events WHERE start = $1",
                &micros.to_be_bytes(),
                PgType::TIME
            ),
            "SELECT id FROM events WHERE start = '09:00:00.000000'::time"
        );
    }

    #[test]
    fn test_binary_time_array() {
        let noon: i64 = 12 * 3600 * 1_000_000;
        let bytes = array_bytes(PgType::TIME, &[&0_i64.to_be_bytes(), &noon.to_be_bytes()]);
        assert_binary_decodes(
            &bytes,
            PgType::TIME_ARRAY,
            cast_array(PgType::TIME, &["00:00:00.000000", "12:00:00.000000"]),
        );
    }

    #[test]
    fn test_binary_parameter_timetz_utc() {
        let micros: i64 = 12 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, 0);
        assert_binary_cast(&bytes, PgType::TIMETZ, "12:00:00.000000+00");
    }

    #[test]
    fn test_binary_parameter_timetz_east_of_utc() {
        // `'12:00:00+05:00'::timetz` — PG stores zone = -18000 (seconds
        // west; +05 east is negative-west).
        let micros: i64 = 12 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, -5 * 3600);
        assert_binary_cast(&bytes, PgType::TIMETZ, "12:00:00.000000+05");
    }

    #[test]
    fn test_binary_parameter_timetz_west_of_utc() {
        let micros: i64 = 12 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, 8 * 3600);
        assert_binary_cast(&bytes, PgType::TIMETZ, "12:00:00.000000-08");
    }

    #[test]
    fn test_binary_parameter_timetz_half_hour_offset() {
        // India: UTC+05:30 → zone = -(5*3600 + 30*60) = -19800.
        let micros: i64 = 9 * 3600 * 1_000_000;
        let bytes = timetz_bytes(micros, -(5 * 3600 + 30 * 60));
        assert_binary_cast(&bytes, PgType::TIMETZ, "09:00:00.000000+05:30");
    }

    #[test]
    fn test_binary_timetz_invalid_length_rejected() {
        assert_binary_invalid(&[0u8; 11], TypeOid::from_type(&PgType::TIMETZ));
    }

    #[test]
    fn test_binary_parameter_interval_zero() {
        let bytes = interval_bytes(0, 0, 0);
        assert_binary_cast(&bytes, PgType::INTERVAL, "0 mons 0 days 00:00:00.000000");
    }

    #[test]
    fn test_binary_parameter_interval_mixed() {
        // 2 months, 3 days, 4h 5m 6.7s
        let micros: i64 = (4 * 3600 + 5 * 60 + 6) * 1_000_000 + 700_000;
        let bytes = interval_bytes(micros, 3, 2);
        assert_binary_cast(&bytes, PgType::INTERVAL, "2 mons 3 days 04:05:06.700000");
    }

    #[test]
    fn test_binary_parameter_interval_negative_components() {
        // Each component is signed independently. -1 month, -2 days, -1 hour.
        let micros: i64 = -3_600_000_000;
        let bytes = interval_bytes(micros, -2, -1);
        assert_binary_cast(&bytes, PgType::INTERVAL, "-1 mons -2 days -01:00:00.000000");
    }

    #[test]
    fn test_binary_interval_in_query_renders_clean_sql() {
        let bytes = interval_bytes(0, 7, 0);
        assert_eq!(
            binary_query_render(
                "SELECT id FROM events WHERE age > $1",
                &bytes,
                PgType::INTERVAL
            ),
            "SELECT id FROM events WHERE age > '0 mons 7 days 00:00:00.000000'::interval"
        );
    }

    #[test]
    fn test_binary_interval_invalid_length_rejected() {
        assert_binary_invalid(&[0u8; 15], TypeOid::from_type(&PgType::INTERVAL));
    }

    #[test]
    fn test_binary_parameter_date_epoch() {
        assert_binary_cast(&0_i32.to_be_bytes(), PgType::DATE, "2000-01-01");
    }

    #[test]
    fn test_binary_parameter_date_next_day() {
        assert_binary_cast(&1_i32.to_be_bytes(), PgType::DATE, "2000-01-02");
    }

    #[test]
    fn test_binary_parameter_date_yesterday() {
        assert_binary_cast(&(-1_i32).to_be_bytes(), PgType::DATE, "1999-12-31");
    }

    #[test]
    fn test_binary_parameter_date_year_1_ad() {
        // 0001-01-01 (proleptic Gregorian) is JDN 1721426; days from
        // 2000-01-01 (JDN 2451545) is -730119.
        assert_binary_cast(&(-730_119_i32).to_be_bytes(), PgType::DATE, "0001-01-01");
    }

    #[test]
    fn test_binary_parameter_date_one_bc() {
        // 1 BC Jan 1 (year 0 in proleptic Gregorian) is JDN 1721060;
        // days from 2000-01-01 = -730485.
        assert_binary_cast(&(-730_485_i32).to_be_bytes(), PgType::DATE, "0001-01-01 BC");
    }

    #[test]
    fn test_binary_parameter_date_infinity() {
        assert_binary_cast(&i32::MAX.to_be_bytes(), PgType::DATE, "infinity");
    }

    #[test]
    fn test_binary_parameter_date_negative_infinity() {
        assert_binary_cast(&i32::MIN.to_be_bytes(), PgType::DATE, "-infinity");
    }

    #[test]
    fn test_binary_parameter_timestamp_epoch() {
        assert_binary_cast(
            &0_i64.to_be_bytes(),
            PgType::TIMESTAMP,
            "2000-01-01 00:00:00.000000",
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_with_time() {
        // 2000-01-02 12:34:56.123456 = 1 day + 12h34m56.123456s.
        let micros: i64 = USECS_PER_DAY + (12 * 3600 + 34 * 60 + 56) * 1_000_000 + 123_456;
        assert_binary_cast(
            &micros.to_be_bytes(),
            PgType::TIMESTAMP,
            "2000-01-02 12:34:56.123456",
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_just_before_epoch() {
        // Negative micros must use floor-division (rem_euclid) so the
        // sub-day component stays in [0, USECS_PER_DAY). Otherwise -1
        // would yield "2000-01-01 -00:00:00.-000001".
        assert_binary_cast(
            &(-1_i64).to_be_bytes(),
            PgType::TIMESTAMP,
            "1999-12-31 23:59:59.999999",
        );
    }

    #[test]
    fn test_binary_parameter_timestamp_infinity() {
        assert_binary_cast(&i64::MAX.to_be_bytes(), PgType::TIMESTAMP, "infinity");
    }

    #[test]
    fn test_binary_parameter_timestamptz_epoch() {
        assert_binary_cast(
            &0_i64.to_be_bytes(),
            PgType::TIMESTAMPTZ,
            "2000-01-01 00:00:00.000000+00",
        );
    }

    #[test]
    fn test_binary_parameter_timestamptz_negative_infinity() {
        assert_binary_cast(&i64::MIN.to_be_bytes(), PgType::TIMESTAMPTZ, "-infinity");
    }

    #[test]
    fn test_binary_date_in_query_renders_clean_sql() {
        let bytes = 1_i32.to_be_bytes();
        assert_eq!(
            binary_query_render("SELECT id FROM events WHERE day = $1", &bytes, PgType::DATE),
            "SELECT id FROM events WHERE day = '2000-01-02'::date"
        );
    }

    #[test]
    fn test_binary_date_array() {
        let bytes = array_bytes(PgType::DATE, &[&0_i32.to_be_bytes(), &1_i32.to_be_bytes()]);
        assert_binary_decodes(
            &bytes,
            PgType::DATE_ARRAY,
            cast_array(PgType::DATE, &["2000-01-01", "2000-01-02"]),
        );
    }

    #[test]
    fn test_binary_parameter_macaddr() {
        assert_binary_cast(
            &[0x00, 0x11, 0x22, 0x33, 0x44, 0x55],
            PgType::MACADDR,
            "00:11:22:33:44:55",
        );
    }

    #[test]
    fn test_binary_parameter_macaddr_invalid_length_rejected() {
        assert_binary_invalid(
            &[0x00, 0x11, 0x22, 0x33, 0x44],
            TypeOid::from_type(&PgType::MACADDR),
        );
    }

    #[test]
    fn test_binary_parameter_macaddr8() {
        assert_binary_cast(
            &[0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77],
            PgType::MACADDR8,
            "00:11:22:33:44:55:66:77",
        );
    }

    #[test]
    fn test_binary_parameter_inet_v4_with_prefix() {
        let bytes = inet_bytes(&[192, 168, 1, 0], 24, false);
        assert_binary_cast(&bytes, PgType::INET, "192.168.1.0/24");
    }

    #[test]
    fn test_binary_parameter_inet_v4_host_omits_prefix() {
        // INET with default mask (32) for v4 omits `/32` to match PG's
        // canonical output.
        let bytes = inet_bytes(&[192, 168, 1, 1], 32, false);
        assert_binary_cast(&bytes, PgType::INET, "192.168.1.1");
    }

    #[test]
    fn test_binary_parameter_inet_v6_with_prefix() {
        // 2001:db8::/64
        let mut addr = [0u8; 16];
        addr[0] = 0x20;
        addr[1] = 0x01;
        addr[2] = 0x0d;
        addr[3] = 0xb8;
        let bytes = inet_bytes(&addr, 64, false);
        assert_binary_cast(&bytes, PgType::INET, "2001:db8::/64");
    }

    #[test]
    fn test_binary_parameter_cidr_v4() {
        let bytes = inet_bytes(&[10, 0, 0, 0], 8, true);
        assert_binary_cast(&bytes, PgType::CIDR, "10.0.0.0/8");
    }

    #[test]
    fn test_binary_parameter_cidr_v4_full_host_keeps_prefix() {
        // Unlike INET, CIDR always emits the prefix even at the default
        // mask — `/32` distinguishes it semantically from a bare host.
        let bytes = inet_bytes(&[192, 168, 1, 1], 32, true);
        assert_binary_cast(&bytes, PgType::CIDR, "192.168.1.1/32");
    }

    #[test]
    fn test_binary_inet_in_query_renders_clean_sql() {
        let bytes = inet_bytes(&[10, 0, 0, 5], 32, false);
        assert_eq!(
            binary_query_render("SELECT id FROM nodes WHERE addr = $1", &bytes, PgType::INET),
            "SELECT id FROM nodes WHERE addr = '10.0.0.5'::inet"
        );
    }

    #[test]
    fn test_binary_macaddr_array() {
        let bytes = array_bytes(
            PgType::MACADDR,
            &[
                &[0x00, 0x11, 0x22, 0x33, 0x44, 0x55],
                &[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff],
            ],
        );
        assert_binary_decodes(
            &bytes,
            PgType::MACADDR_ARRAY,
            cast_array(PgType::MACADDR, &["00:11:22:33:44:55", "aa:bb:cc:dd:ee:ff"]),
        );
    }

    #[test]
    fn test_binary_parameter_numeric_simple_int() {
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 0, &[42]), "42");
    }

    #[test]
    fn test_binary_parameter_numeric_zero_no_scale() {
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 0, &[]), "0");
    }

    #[test]
    fn test_binary_parameter_numeric_zero_with_scale() {
        // dscale > 0 forces trailing zeros after the decimal point.
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 4, &[]), "0.0000");
    }

    #[test]
    fn test_binary_parameter_numeric_decimal() {
        // 42.5: digits at weight 0 and -1, dscale 1 truncates the second
        // digit (5000) to a single character "5".
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 1, &[42, 5000]), "42.5");
    }

    #[test]
    fn test_binary_parameter_numeric_decimal_trailing_zeros() {
        // 1.50 keeps the trailing zero because dscale = 2.
        assert_numeric(numeric_bytes(0, NUMERIC_POS, 2, &[1, 5000]), "1.50");
    }

    #[test]
    fn test_binary_parameter_numeric_negative() {
        assert_numeric(numeric_bytes(0, NUMERIC_NEG, 2, &[3, 1400]), "-3.14");
    }

    #[test]
    fn test_binary_parameter_numeric_big_with_implicit_zeros() {
        // 1.5 × 10⁸ = 150_000_000 — digits[0]=1 at weight 2, digits[1]=5000
        // at weight 1, weight 0 implicit zero.
        assert_numeric(numeric_bytes(2, NUMERIC_POS, 0, &[1, 5000]), "150000000");
    }

    #[test]
    fn test_binary_parameter_numeric_small_fraction() {
        // 0.00001 = 1000 × 10000⁻², single digit at weight -2.
        assert_numeric(numeric_bytes(-2, NUMERIC_POS, 5, &[1000]), "0.00001");
    }

    #[test]
    fn test_binary_parameter_numeric_nan() {
        assert_numeric(numeric_bytes(0, NUMERIC_NAN, 0, &[]), "NaN");
    }

    #[test]
    fn test_binary_parameter_numeric_infinity() {
        assert_numeric(numeric_bytes(0, NUMERIC_PINF, 0, &[]), "Infinity");
    }

    #[test]
    fn test_binary_parameter_numeric_negative_infinity() {
        assert_numeric(numeric_bytes(0, NUMERIC_NINF, 0, &[]), "-Infinity");
    }

    #[test]
    fn test_binary_parameter_numeric_invalid_sign_rejected() {
        // 0xE000 isn't a defined sign code.
        assert_binary_invalid(
            &numeric_bytes(0, 0xE000, 0, &[]),
            TypeOid::from_type(&PgType::NUMERIC),
        );
    }

    #[test]
    fn test_binary_parameter_numeric_negative_ndigits_rejected() {
        // Construct a header with ndigits = -1 directly (the safe builder
        // rejects negative lengths via try_from).
        let mut bytes = Vec::with_capacity(8);
        bytes.extend_from_slice(&(-1_i16).to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());

        assert_binary_invalid(&bytes, TypeOid::from_type(&PgType::NUMERIC));
    }

    #[test]
    fn test_binary_parameter_numeric_truncated_rejected() {
        // Header claims 2 digits but only one digit's bytes follow.
        let mut bytes = Vec::with_capacity(10);
        bytes.extend_from_slice(&2_i16.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&0_u16.to_be_bytes());
        bytes.extend_from_slice(&0_i16.to_be_bytes());
        bytes.extend_from_slice(&42_i16.to_be_bytes());

        assert_binary_invalid(&bytes, TypeOid::from_type(&PgType::NUMERIC));
    }

    #[test]
    fn test_binary_parameter_numeric_digit_out_of_range_rejected() {
        // 10000 is one past the legal max.
        assert_binary_invalid(
            &numeric_bytes(0, NUMERIC_POS, 0, &[10000]),
            TypeOid::from_type(&PgType::NUMERIC),
        );
    }

    #[test]
    fn test_binary_numeric_in_query_renders_clean_sql() {
        let bytes = numeric_bytes(0, NUMERIC_POS, 2, &[3, 1400]);
        assert_eq!(
            binary_query_render(
                "SELECT id FROM ledger WHERE balance > $1",
                &bytes,
                PgType::NUMERIC
            ),
            "SELECT id FROM ledger WHERE balance > '3.14'::numeric"
        );
    }

    #[test]
    fn test_binary_numeric_array() {
        let bytes = array_bytes(
            PgType::NUMERIC,
            &[
                &numeric_bytes(0, NUMERIC_POS, 1, &[42, 5000]), // "42.5"
                &numeric_bytes(0, NUMERIC_NEG, 2, &[3, 1400]),  // "-3.14"
            ],
        );
        assert_binary_decodes(
            &bytes,
            PgType::NUMERIC_ARRAY,
            cast_array(PgType::NUMERIC, &["42.5", "-3.14"]),
        );
    }

    #[test]
    fn test_binary_parameter_unsupported_simple_type_rejected_valid_utf8() {
        // Valid-UTF-8 bytes for an unsupported `Kind::Simple` type must
        // not fall through to a `String` literal — that would silently
        // corrupt SQL. POINT exercises the per-OID match's fail-closed
        // catch-all rather than the Kind-dispatch path.
        assert_binary_unsupported(
            &[0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
            TypeOid::from_type(&PgType::POINT),
        );
    }

    #[test]
    fn test_binary_parameter_null() {
        let param = QueryParameter {
            value: None,
            format: 1,
            oid: TypeOid::from_type(&PgType::INT4),
        };
        let result = parameter_to_literal(&param).expect("to convert parameter");
        assert_eq!(result, LiteralValue::Null);
    }

    #[test]
    fn test_binary_int4_in_query() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");
        let params = binary_params(vec![(Some(&[0x00, 0x00, 0x00, 0x2A]), PgType::INT4)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_binary_int4_array_decoded() {
        assert_binary_decodes(
            &binary_int4_array_42_100(),
            PgType::INT4_ARRAY,
            LiteralValue::Array(
                vec![LiteralValue::Integer(42), LiteralValue::Integer(100)],
                "int4[]".into(),
            ),
        );
    }

    #[test]
    fn test_binary_int4_array_distinct_values_produce_distinct_fingerprints() {
        // Sanity check that two different binary int4[] parameter values
        // substituted into the same query template produce different
        // post-substitution fingerprints. Otherwise pgcache would route
        // them to the same cache entry and one query's results would bleed
        // into the other's.
        let q1 = query_expr_parse("SELECT id FROM widgets WHERE id = ANY($1)")
            .expect("convert to QueryExpr");
        let q2 = q1.clone();

        let arr1 = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02,
        ];
        let arr2 = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00,
            0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03,
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
            0x00, 0x05,
        ];
        let p1 = binary_params(vec![(Some(&arr1), PgType::INT4_ARRAY)]);
        let p2 = binary_params(vec![(Some(&arr2), PgType::INT4_ARRAY)]);

        let r1 = query_expr_parameters_replace(&q1, &p1).expect("replace 1");
        let r2 = query_expr_parameters_replace(&q2, &p2).expect("replace 2");

        let mut s1 = String::new();
        let mut s2 = String::new();
        r1.deparse(&mut s1);
        r2.deparse(&mut s2);
        assert_ne!(s1, s2, "deparsed SQL should differ between arrays");

        let f1 = query_expr_fingerprint(&r1);
        let f2 = query_expr_fingerprint(&r2);
        assert_ne!(
            f1, f2,
            "different binary int4[] values must produce different fingerprints"
        );
    }

    #[test]
    fn test_binary_int4_array_in_query_renders_clean_sql() {
        let array_bytes = binary_int4_array_42_100();
        assert_eq!(
            binary_query_render(
                "SELECT id FROM users WHERE id = ANY($1)",
                &array_bytes,
                PgType::INT4_ARRAY
            ),
            "SELECT id FROM users WHERE id = ANY ('{42,100}'::int4[])"
        );
    }

    #[test]
    fn test_binary_int4_array_empty() {
        let bytes = vec![
            0x00, 0x00, 0x00, 0x00, // ndim = 0
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
        ];
        assert_binary_decodes(
            &bytes,
            PgType::INT4_ARRAY,
            LiteralValue::Array(vec![], "int4[]".into()),
        );
    }

    #[test]
    fn test_binary_int4_array_with_null_element() {
        // `{1, NULL, 3}::int4[]`: NULL elements have length-prefix = -1.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x01, // hasnull = 1
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x03, // dim 0 length = 3
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
            0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0xFF, 0xFF, 0xFF, 0xFF, 0x00, 0x00,
            0x00, 0x04, 0x00, 0x00, 0x00, 0x03,
        ];
        assert_binary_decodes(
            &bytes,
            PgType::INT4_ARRAY,
            LiteralValue::Array(
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Null,
                    LiteralValue::Integer(3),
                ],
                "int4[]".into(),
            ),
        );
    }

    #[test]
    fn test_binary_text_array_quoting() {
        // `text[]` with elements that all need PG-array-text quoting.
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x01, // ndim = 1
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x19, // elemtype = 25 (text)
            0x00, 0x00, 0x00, 0x05, // dim 0 length = 5
            0x00, 0x00, 0x00, 0x01, // dim 0 lower bound = 1
        ];
        for s in ["plain", "with,comma", "has\"quote", "back\\slash", ""] {
            bytes.extend_from_slice(&array_text_element_bytes(s));
        }

        let literal = binary_decode(&bytes, PgType::TEXT_ARRAY);
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::String("plain".into()),
                    LiteralValue::String("with,comma".into()),
                    LiteralValue::String("has\"quote".into()),
                    LiteralValue::String("back\\slash".into()),
                    LiteralValue::String("".into()),
                ],
                "text[]".into()
            )
        );

        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(
            buf,
            r#"E'{plain,"with,comma","has\\"quote","back\\\\slash",""}'::text[]"#
        );
    }

    #[test]
    fn test_binary_text_array_with_null_string_element() {
        // The element value `"null"` (case-insensitive) must be quoted so
        // PG doesn't read it as a NULL marker.
        let mut bytes = vec![
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00,
            0x00, 0x02, 0x00, 0x00, 0x00, 0x01,
        ];
        bytes.extend_from_slice(&array_text_element_bytes("NULL"));
        bytes.extend_from_slice(&array_text_element_bytes("a"));

        let literal = binary_decode(&bytes, PgType::TEXT_ARRAY);
        assert_eq!(
            literal,
            LiteralValue::Array(
                vec![
                    LiteralValue::String("NULL".into()),
                    LiteralValue::String("a".into()),
                ],
                "text[]".into()
            )
        );

        let mut buf = String::new();
        literal.deparse(&mut buf);
        assert_eq!(buf, r#"'{"NULL",a}'::text[]"#);
    }

    #[test]
    fn test_binary_multidim_array_rejected() {
        // 2-D arrays fall through to origin uncached.
        let bytes = vec![
            0x00, 0x00, 0x00, 0x02, // ndim = 2
            0x00, 0x00, 0x00, 0x00, // hasnull = 0
            0x00, 0x00, 0x00, 0x17, // elemtype = 23 (int4)
            0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00,
            0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A,
        ];
        assert_binary_unsupported(&bytes, TypeOid::from_type(&PgType::INT4_ARRAY));
    }

    #[test]
    fn test_binary_array_unsupported_element_type_rejected() {
        // POINT has no scalar arm; element bytes are placeholder garbage.
        let mut bytes = array_header_bytes(PgType::POINT, 1);
        bytes.extend_from_slice(&16_i32.to_be_bytes());
        bytes.extend_from_slice(&[0u8; 16]);

        assert_binary_unsupported(&bytes, TypeOid::from_type(&PgType::POINT_ARRAY));
    }
}
