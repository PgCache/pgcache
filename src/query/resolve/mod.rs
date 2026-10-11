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

mod analysis;
mod bindings;
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

pub(crate) use bindings::BindingsBuilder;
pub use bindings::{
    Bindings, ColumnBinding, ColumnTarget, MergedColumnBinding, MergedId, ResolvedQuery, SlotId,
    StarItem, TableSlot,
};
pub use entry::query_expr_resolve_bound;
pub use render::SourceOverride;
