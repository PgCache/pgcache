mod constant_fold;
mod parameters;
mod pushdown;
mod select_node;
mod values;
mod walk;

use error_set::error_set;
use rootcause::Report;

error_set! {
    AstTransformError := {
        MissingTable,
        #[display("Parameter index {index} out of bounds (have {count} parameters)")]
        ParameterOutOfBounds { index: usize, count: usize },
        #[display("Invalid parameter placeholder: {placeholder}")]
        InvalidParameterPlaceholder { placeholder: String },
        #[display("Invalid UTF-8 in parameter value")]
        InvalidUtf8,
        #[display("Invalid parameter value: {message}")]
        InvalidParameterValue { message: String },
        #[display("Unsupported binary format for OID {oid}")]
        UnsupportedBinaryFormat { oid: u32 },
    }
}

/// Result type with location-tracking error reports for AST transform operations.
pub type AstTransformResult<T> = Result<T, Report<AstTransformError>>;

pub use constant_fold::query_expr_constant_fold;
pub use parameters::{query_expr_parameters_replace, select_node_parameters_replace};
pub use pushdown::predicate_pushdown_apply;
pub(crate) use pushdown::{where_expr_conjuncts_join, where_expr_conjuncts_split};
pub use select_node::{resolved_select_node_replace, resolved_select_node_update_replace};
pub use values::{
    BATCH_IDX_COLUMN, resolved_select_node_table_replace_with_unnest,
    resolved_select_node_table_replace_with_values,
    resolved_select_node_table_replace_with_values_batch,
};
