//! Bind a parsed AST against the catalog: every table and column reference
//! to concrete metadata, as a side table keyed by node id (ADR-057).
//!
//! * [`scope`] — what is visible at each point in the resolution.
//! * [`table`] — FROM entries: catalog lookup, joins, derived tables.
//! * [`column`] — column references, including correlated outer refs.
//! * [`expr`] — WHERE predicates, scalar expressions, window specs.
//! * [`clauses`] — target list, ORDER BY / GROUP BY / HAVING / LIMIT.
//! * [`join_using`] — the `USING`/`NATURAL` merged-column cluster.
//! * [`entry`] — the public entry points.

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

mod analysis;
mod bindings;
mod branch;
mod clauses;
mod column;
mod entry;
mod enum_gate;
mod expr;
mod join_using;
mod render;
mod scope;
mod table;
#[cfg(test)]
mod tests;

error_set! {
    ResolveError := {
        #[display("Table not found: {name}")]
        TableNotFound { name: String },

        #[display("Column '{column}' not found in table '{table}'")]
        ColumnNotFound { table: String, column: String },

        #[display("Ambiguous column reference: '{column}' could refer to multiple tables")]
        AmbiguousColumn { column: String },

        #[display("Schema '{schema}' not found")]
        SchemaNotFound { schema: String },

        #[display("Subquery alias '{alias}' not found")]
        SubqueryAliasNotFound { alias: String },

        #[display("Invalid table reference")]
        InvalidTableRef,

        #[display("Derived table '{alias}' has more columns than PostgreSQL allows")]
        TooManyColumns { alias: String },

        #[display("Unsupported join qualifier (USING/NATURAL not yet cacheable)")]
        UnsupportedJoinQualifier,

        #[display(
            "order-dependent use of enum column '{table}.{column}' in {position} — enums are cached as text"
        )]
        EnumOrderDependentUsage {
            table: EcoString,
            column: EcoString,
            position: &'static str,
        },
    }
}

/// Result type with location-tracking error reports for resolution operations.
pub type ResolveResult<T> = Result<T, Report<ResolveError>>;

pub use analysis::ColumnsByTable;
pub(crate) use bindings::BindingsBuilder;
pub use bindings::{
    Bindings, ColumnBinding, ColumnTarget, MergedColumnBinding, MergedId, ResolvedQuery, SlotId,
    StarItem, TableSlot,
};
pub use branch::QueryBranch;
pub use entry::query_expr_resolve;
pub use render::SourceOverride;
