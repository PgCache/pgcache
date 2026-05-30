//! Text-format parameter decoding (Bind format=0). Wire bytes are valid
//! UTF-8 by protocol guarantee; the OID picks the typed `LiteralValue`.

use ordered_float::NotNan;
use postgres_types::Type as PgType;
use rootcause::Report;

use crate::query::ast::LiteralValue;

use super::super::{AstTransformError, AstTransformResult};

pub(super) fn text_parameter_to_literal(
    bytes: &[u8],
    oid: u32,
) -> AstTransformResult<LiteralValue> {
    let s = std::str::from_utf8(bytes).map_err(|_| Report::from(AstTransformError::InvalidUtf8))?;

    let pg_type = PgType::from_oid(oid);

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
        _ => Ok(LiteralValue::String(s.to_owned())),
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use postgres_types::Type as PgType;
    use tokio_util::bytes::Bytes;

    use crate::cache::{QueryParameter, QueryParameters};
    use crate::query::ast::{Deparse, QueryBody, SelectNode, query_expr_parse};

    use super::super::super::AstTransformError;
    use super::super::{parameter_to_literal, select_node_parameters_replace};

    fn parse_select_node(sql: &str) -> SelectNode {
        let query_expr = query_expr_parse(sql).expect("convert to QueryExpr");
        match query_expr.body {
            QueryBody::Select(node) => *node,
            _ => panic!("expected SELECT"),
        }
    }

    fn typed_text_params(values: Vec<(Option<&[u8]>, PgType)>) -> QueryParameters {
        let len = values.len();
        let (values, oids): (Vec<_>, Vec<_>) = values
            .into_iter()
            .map(|(v, t)| (v.map(Bytes::copy_from_slice), t.oid()))
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
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_text_parameter_boolean() {
        let mut node = parse_select_node("SELECT id FROM users WHERE active = $1");
        let params = typed_text_params(vec![(Some(b"t"), PgType::BOOL)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE active = true");
    }

    #[test]
    fn test_text_parameter_float() {
        let mut node = parse_select_node("SELECT id FROM users WHERE score > $1");
        let params = typed_text_params(vec![(Some(b"3.14"), PgType::FLOAT8)]);
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(buf, "SELECT id FROM users WHERE score > 3.14");
    }

    #[test]
    fn test_text_parameter_invalid_integer() {
        let param = QueryParameter {
            value: Some(Bytes::from_static(b"not_a_number")),
            format: 0,
            oid: PgType::INT4.oid(),
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
        select_node_parameters_replace(&mut node, &params).expect("to replace parameters");

        let mut buf = String::new();
        node.deparse(&mut buf);
        assert_eq!(
            buf,
            "SELECT id FROM users WHERE uuid = '550e8400-e29b-41d4-a716-446655440000'"
        );
    }
}
