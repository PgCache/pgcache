use std::collections::HashSet;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

use crate::query::resolved::{
    ResolvedColumnNode, ResolvedQueryExpr, ResolvedTableSource, ResolvedWhereExpr,
};

mod driver;
mod exists;
mod in_subquery;
mod predicate;
mod scalar;

#[cfg(test)]
mod tests;

pub use driver::query_expr_decorrelate;

error_set! {
    DecorrelateError := {
        #[display("Non-decorrelatable correlated subquery: {reason}")]
        NonDecorrelatable { reason: String },
    }
}

pub type DecorrelateResult<T> = Result<T, Report<DecorrelateError>>;

/// Outcome of decorrelation: the (possibly transformed) resolved query.
pub struct DecorrelateOutcome {
    pub resolved: ResolvedQueryExpr,
    pub transformed: bool,
}

/// A correlation predicate extracted from an inner subquery WHERE.
struct CorrelationPredicate {
    /// Column from the outer query scope
    outer_column: ResolvedColumnNode,
    /// Column from the inner subquery scope
    inner_column: ResolvedColumnNode,
}

/// Non-empty collection of correlation predicates.
/// Constructed by `where_clause_correlation_partition` which returns `None`
/// when no predicates are found, so existence of this type guarantees at
/// least one predicate.
struct CorrelationPredicates {
    first: CorrelationPredicate,
    rest: Vec<CorrelationPredicate>,
}

/// Mutable state threaded through decorrelation to generate unique aliases.
struct DecorrelateState<'a> {
    /// Counter for derived table aliases: _dc1, _dc2, ...
    derived_table_counter: u32,
    /// Counter for scalar column aliases: _ds1, _ds2, ...
    scalar_column_counter: u32,
    /// Aggregate function names from pg_proc (lowercase).
    aggregate_functions: &'a HashSet<EcoString>,
}

/// Result of decorrelating a single scalar subquery.
struct ScalarDecorrelateResult {
    /// The derived table to LEFT JOIN onto the outer query.
    derived_table: ResolvedTableSource,
    /// The JOIN ON condition (correlation predicates mapped to derived table columns).
    join_condition: ResolvedWhereExpr,
    /// A column reference pointing to the scalar result column inside the derived table.
    scalar_column_ref: ResolvedColumnNode,
}
