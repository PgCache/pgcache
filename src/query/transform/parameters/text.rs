//! Text-format parameter decoding (Bind format=0). Wire bytes are valid
//! UTF-8 by protocol guarantee; the OID picks the typed `LiteralValue`.

use ordered_float::NotNan;
use postgres_types::Type as PgType;
use rootcause::Report;

use crate::oid::TypeOid;
use crate::query::ast::LiteralValue;
use crate::query::transform::{AstTransformError, AstTransformResult};

pub(super) fn text_parameter_to_literal(
    bytes: &[u8],
    oid: TypeOid,
) -> AstTransformResult<LiteralValue> {
    let s = std::str::from_utf8(bytes).map_err(|_| Report::from(AstTransformError::InvalidUtf8))?;

    let pg_type = oid.pg_type();

    match pg_type {
        Some(PgType::BOOL) => {
            let value = matches!(s, "t" | "true" | "TRUE" | "1");
            Ok(LiteralValue::Boolean(value))
        }
        Some(PgType::INT2 | PgType::INT4 | PgType::INT8) => {
            let value = s.parse::<i64>().map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid integer '{s}': {e}"),
                })
            })?;
            Ok(LiteralValue::Integer(value))
        }
        Some(PgType::FLOAT4 | PgType::FLOAT8) => {
            let value = s.parse::<f64>().map_err(|e| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("invalid float '{s}': {e}"),
                })
            })?;
            let value = NotNan::new(value).map_err(|_| {
                Report::from(AstTransformError::InvalidParameterValue {
                    message: format!("NaN is not a valid float value: {s}"),
                })
            })?;
            Ok(LiteralValue::Float(value))
        }
        // String-like and pass-through types (UUID, temporal, numeric)
        // round-trip as the original text.
        _ => Ok(LiteralValue::String(s.into())),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use crate::cache::{QueryParameter, QueryParameters};
    use crate::oid::TypeOid;
    use crate::query::ast::query_tree_parse;
    use crate::query::ast::rope::QueryTree;
    use crate::query::transform::parameters::parameter_to_literal;
    use crate::query::transform::query_tree_parameters_replace;
    use crate::query::transform::{AstTransformError, AstTransformResult};

    fn parse_select_node(sql: &str) -> QueryTree {
        query_tree_parse(sql).expect("parse")
    }

    /// Bind `params` into `query` in place.
    fn query_parameters_replace_in_place(
        query: &mut QueryTree,
        params: &QueryParameters,
    ) -> AstTransformResult<()> {
        *query = query_tree_parameters_replace(query, params)?;
        Ok(())
    }

    fn typed_text_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), TypeOid::from_type(&t)))
            .unzip();
        QueryParameters {
            values,
            formats: vec![0; len],
            oids,
        }
    }

    #[test]
    fn test_text_parameter_integer() {
        let mut node = parse_select_node("SELECT id FROM users WHERE id = $1");
        let params = typed_text_params(vec![(Some(b"42"), PgType::INT4)]);
        query_parameters_replace_in_place(&mut node, &params).expect("to replace parameters");

        let buf = node.sql();
        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_text_parameter_boolean() {
        let mut node = parse_select_node("SELECT id FROM users WHERE active = $1");
        let params = typed_text_params(vec![(Some(b"t"), PgType::BOOL)]);
        query_parameters_replace_in_place(&mut node, &params).expect("to replace parameters");

        let buf = node.sql();
        assert_eq!(buf, "SELECT id FROM users WHERE active = true");
    }

    #[test]
    fn test_text_parameter_float() {
        let mut node = parse_select_node("SELECT id FROM users WHERE score > $1");
        let params = typed_text_params(vec![(Some(b"3.14"), PgType::FLOAT8)]);
        query_parameters_replace_in_place(&mut node, &params).expect("to replace parameters");

        let buf = node.sql();
        assert_eq!(buf, "SELECT id FROM users WHERE score > 3.14");
    }

    #[test]
    fn test_text_parameter_invalid_integer() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"not_a_number")),
            format: 0,
            oid: TypeOid::from_type(&PgType::INT4),
        };

        let result = parameter_to_literal(&param).map_err(|e| e.into_current_context());
        assert!(matches!(
            result,
            Err(AstTransformError::InvalidParameterValue { .. })
        ));
    }

    #[test]
    fn test_text_parameter_uuid() {
        let mut node = parse_select_node("SELECT id FROM users WHERE uuid = $1");
        let params = typed_text_params(vec![(
            Some(b"550e8400-e29b-41d4-a716-446655440000"),
            PgType::UUID,
        )]);
        query_parameters_replace_in_place(&mut node, &params).expect("to replace parameters");

        let buf = node.sql();
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE uuid = '550e8400-e29b-41d4-a716-446655440000'"
        );
    }
}
