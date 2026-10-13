pub(crate) mod convert_raw;
mod convert_tree;
#[cfg(test)]
mod convert_tree_tests;
mod deparse;
mod finalize;
mod fingerprint;
mod node_id;
pub(crate) mod raw;
pub mod rope;
mod types;
#[cfg(test)]
mod where_clause_tests;
#[cfg(test)]
mod write_tests;

use error_set::error_set;

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

// Re-export everything public from submodules
#[cfg(test)]
pub(crate) use convert_raw::query_expr_parse;
pub use convert_raw::{RawStatement, query_expr_convert_raw, statement_convert_raw};
pub use convert_tree::query_tree_convert_raw;
#[cfg(test)]
pub(crate) use convert_tree::query_tree_parse;
pub use deparse::{Clause, Deparse, DeparseContext, DeparseWith, Plain};
pub(crate) use deparse::{
    clause_deparse_with, parenthesized_deparse_with, separated_deparse, separated_deparse_with,
};
pub use finalize::query_expr_finalize;
pub use fingerprint::*;
pub use node_id::{NodeId, NodePositions};
pub use types::*;
