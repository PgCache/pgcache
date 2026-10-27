mod constant_fold;
mod literal_canon;
mod parameters;
pub(crate) mod predicate_pushdown;
mod session_terms;
mod substitute;
mod values;

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
    }
}

/// Result type with location-tracking error reports for AST transform operations.
pub type AstTransformResult<T> = Result<T, Report<AstTransformError>>;

pub(crate) use constant_fold::literal_arithmetic_fold;
pub use literal_canon::{LiteralCanonOutcome, query_literals_canonicalise};
#[cfg(feature = "proxy")]
pub(crate) use parameters::{
    delete_statement_parameterize, insert_statement_parameterize, update_statement_parameterize,
};
pub(crate) use parameters::{literal_is_parameterizable, literal_value_parameters_replace};
pub use predicate_pushdown::query_predicate_pushdown;
pub use session_terms::{
    CustomSettingValue, SessionTermContext, SessionTermError, query_tree_has_session_term,
    query_tree_session_terms_replace, query_tree_setting_names,
};
pub use substitute::query_tree_parameters_replace;
pub use values::{
    BATCH_IDX_COLUMN, PgEvalTemplate, TableOccurrence, membership_batch_into,
    membership_unnest_into, membership_values_into, table_occurrence_at,
};
