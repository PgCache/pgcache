mod constant_fold;
mod literal_canon;
mod parameters;
mod predicate_pushdown;
mod pushdown;
mod select_node;
mod values;
mod walk;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

use crate::oid::TypeOid;

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
        UnsupportedBinaryFormat { oid: TypeOid },
        #[display("Table {table} occurs more than once (self-join); a single-occurrence substitution is not a complete membership test")]
        MultipleOccurrences { table: EcoString },
    }
}

/// Result type with location-tracking error reports for AST transform operations.
pub type AstTransformResult<T> = Result<T, Report<AstTransformError>>;

pub use constant_fold::query_expr_constant_fold;
pub use literal_canon::{
    LiteralCanonOutcome, query_literals_canonicalise, resolved_literals_canonicalise,
};
pub(crate) use parameters::literal_is_parameterizable;
#[cfg(feature = "proxy")]
pub(crate) use parameters::{
    delete_statement_parameterize, insert_statement_parameterize, update_statement_parameterize,
};
pub use parameters::{
    query_expr_parameters_replace, resolved_query_expr_parameterize, select_node_parameters_replace,
};
pub use predicate_pushdown::query_predicate_pushdown;
pub use pushdown::predicate_pushdown_apply;
pub(crate) use pushdown::{where_expr_conjuncts_join, where_expr_conjuncts_split};
pub use select_node::resolved_select_node_update_replace;
pub use values::{
    BATCH_IDX_COLUMN, PgEvalTemplate, TableOccurrence, membership_batch_into,
    membership_unnest_into, membership_values_all_into, membership_values_into,
    table_occurrences_find,
};
