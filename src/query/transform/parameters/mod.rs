//! Decode extended-query Bind values for `$N` placeholders. The decoder pipeline forks on wire format: `text` for UTF-8 text
//! values (format=0), `binary` for the per-type wire formats (format=1).

mod binary;
mod replace;
mod text;
#[cfg(feature = "proxy")]
mod write_parameterize;

use super::AstTransformResult;
use crate::cache::QueryParameter;
use crate::query::ast::LiteralValue;

pub(crate) use replace::literal_value_parameters_replace;
#[cfg(feature = "proxy")]
pub(crate) use write_parameterize::{
    delete_statement_parameterize, insert_statement_parameterize, update_statement_parameterize,
};

fn parameter_to_literal(param: &QueryParameter) -> AstTransformResult<LiteralValue> {
    match &param.value {
        None => Ok(LiteralValue::Null),
        Some(bytes) => {
            if param.format == 0 {
                text::text_parameter_to_literal(bytes, param.oid)
            } else {
                binary::binary_parameter_to_literal(bytes, param.oid)
            }
        }
    }
}

/// Whether a literal becomes a bound parameter. Only the four scalar forms whose
/// bind type PG can infer from context are parameterized. Everything else stays
/// inline in the shape SQL:
/// - `StringWithCast`/`Array` carry an explicit `::cast` that disambiguates their
///   type; binding them as text would drop the cast and rely on context inference,
///   which is not always sound (e.g. `'2024-01-01'::date`, `'{1,2}'::int4[]`).
/// - `Null`/`NullWithCast` have no unambiguous bind type.
/// - `Parameter` is already a placeholder.
///
/// Keeping cast/array literals inline means two queries differing only in such a
/// literal get distinct shapes — accepted minor proliferation for those rarer
/// forms in exchange for never binding a value at the wrong type.
pub(crate) fn literal_is_parameterizable(literal: &LiteralValue) -> bool {
    match literal {
        LiteralValue::String(_)
        | LiteralValue::Integer(_)
        | LiteralValue::Float(_)
        | LiteralValue::Boolean(_) => true,
        LiteralValue::StringWithCast(_, _)
        | LiteralValue::Array(_, _)
        | LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_)
        | LiteralValue::SessionTerm(_) => false,
    }
}
