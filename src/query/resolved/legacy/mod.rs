//! The resolver as it was while the resolved tree existed: the oracle the
//! bindings-only resolver is checked against until the tree goes.

mod clauses;
mod column;
mod entry;
mod expr;
mod join_using;
mod scope;
mod table;

pub(crate) use entry::{
    query_expr_resolve, query_expr_resolve_bound as query_expr_resolve_legacy, select_node_resolve,
};
pub(crate) use scope::derived_table_columns_extract;
