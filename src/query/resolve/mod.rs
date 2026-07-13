//! Resolve a parsed AST against the catalog: bind every table and column
//! reference to concrete metadata.
//!
//! * [`scope`] — what is visible at each point in the resolution.
//! * [`table`] — FROM entries: catalog lookup, joins, derived tables.
//! * [`column`] — column references, including correlated outer refs.
//! * [`expr`] — WHERE predicates, scalar expressions, window specs.
//! * [`clauses`] — target list, ORDER BY / GROUP BY / HAVING / LIMIT.
//! * [`join_using`] — the `USING`/`NATURAL` merged-column cluster.
//! * [`entry`] — the public entry points.

mod clauses;
mod column;
mod entry;
mod expr;
mod join_using;
mod scope;
mod table;
#[cfg(test)]
mod tests;

pub use entry::{query_expr_resolve, select_node_resolve};
