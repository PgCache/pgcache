mod convert_raw;
#[cfg(test)]
mod convert_raw_tests;
mod fingerprint;
pub(crate) mod raw;
mod types;
#[cfg(test)]
mod where_clause_tests;

use ecow::EcoString;
use error_set::error_set;

use crate::pg::identifier_needs_quotes;

error_set! {
    AstError := {
        #[display("Unsupported statement type: {statement_type}")]
        UnsupportedStatement { statement_type: String },
        #[display("Multiple statements not supported")]
        MultipleStatements,
        #[display("Missing statement")]
        MissingStatement,
        #[display("Unsupported SELECT feature: {feature}")]
        UnsupportedSelectFeature { feature: String },
        #[display("Unsupported feature: {feature}")]
        UnsupportedFeature { feature: String },
        #[display("Invalid table reference")]
        InvalidTableRef,
        UnsupportedJoinType,
        #[display("Unsupported SubLink type: {sublink_type}")]
        UnsupportedSubLinkType { sublink_type: String },
        WhereParseError(WhereParseError),
    }
}

error_set! {
    WhereParseError := {
        #[display("Unsupported WHERE clause pattern")]
        UnsupportedPattern,
        #[display("Unsupported A expression: {expr}")]
        UnsupportedAExpr { expr: String },
        #[display("Unsupported operator: {operator}")]
        UnsupportedOperator { operator: String },
        #[display("Invalid column reference")]
        InvalidColumnRef,
        #[display("Invalid constant value: {value}")]
        InvalidConstValue { value: String },
        #[display("Complex expression not supported: {expr}")]
        ComplexExpression { expr: String },
        #[display("Missing expression")]
        MissingExpression,
        #[display("{error}")]
        Other { error: String },
        /// Wraps a structural AST conversion failure surfaced while parsing
        /// a WHERE-side scalar (function call, scalar subquery, arithmetic
        /// operand, etc.). Boxed to keep the enum size bounded — `AstError`
        /// already contains `WhereParseError(WhereParseError)`.
        #[display("{0}")]
        Conversion(Box<AstError>),
    }
}

pub trait Deparse {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String;
}

impl Deparse for String {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => {
                buf.push('"');
                buf.push_str(self);
                buf.push('"');
            }
            false => buf.push_str(self),
        };

        buf
    }
}

impl Deparse for &str {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => {
                buf.push('"');
                buf.push_str(self);
                buf.push('"');
            }
            false => buf.push_str(self),
        };

        buf
    }
}

impl Deparse for EcoString {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.as_str().deparse(buf)
    }
}

// Re-export everything public from submodules
pub use convert_raw::query_expr_convert_raw;
#[cfg(test)]
pub(crate) use convert_raw::query_expr_parse;
pub use fingerprint::*;
pub use types::*;
