//! Bind a parsed tree against the catalog: every table and column reference
//! to concrete metadata, as a side table keyed by node id (ADR-057).
//!
//! * [`scope`] — what is visible at each point in the resolution.
//! * [`column`] — column references, including correlated outer refs.
//! * [`join_using`] — the `USING`/`NATURAL` merged-column cluster.
//! * [`rope`] — the walk over the tree's views: FROM entries, predicates,
//!   scalar expressions, the target list and the ordering clauses.

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

mod analysis;
mod bindings;
mod branch;
mod column;
mod enum_gate;
mod join_using;
mod render;
mod rope;
mod scope;
#[cfg(test)]
pub(crate) mod tests;

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
pub use render::SourceOverride;
pub use rope::query_tree_resolve;
