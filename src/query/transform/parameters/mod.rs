//! Substitute extended-query Bind values for `$N` placeholders in a parsed
//! AST. The decoder pipeline forks on wire format: `text` for UTF-8 text
//! values (format=0), `binary` for the per-type wire formats (format=1).

mod binary;
mod replace;
mod resolved_parameterize;
mod text;

use crate::cache::QueryParameter;
use crate::query::ast::LiteralValue;

use super::AstTransformResult;

pub use replace::{query_expr_parameters_replace, select_node_parameters_replace};
pub use resolved_parameterize::resolved_query_expr_parameterize;

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
