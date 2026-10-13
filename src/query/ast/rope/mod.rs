//! The rope (ADR-057): a query tree as a table of chunks over shared
//! pre-order node slices. A node stores its subtree length and no
//! position, so a subtree is valid at any offset; a rewrite keeps the
//! subtrees it does not change as table entries over the base's slices and
//! writes only its own nodes. Consumers read through views, which carry
//! the absolute position a side table such as the bindings is keyed by.

mod build;
pub mod deparse;
mod node;
#[cfg(test)]
pub(crate) mod tests;
mod tree;
mod view;

pub use build::{Open, Remap, Replacement, Replacements, TreeBuilder};
pub use deparse::{DeparseContext, Plain};
pub use node::{FrameBoundKind, JoinQualKind, Node, NodeKind};
pub use tree::QueryTree;
pub use view::{
    Arithmetic, Array, Binary, Body, Case, Children, Column, Cte, CteRef, Frame, FrameBound,
    Function, Join, JoinQual, Literal, Multi, Name, NodeRef, OrderBy, Query, Run, Scalar,
    ScalarKind, ScalarSubquery, Select, SelectExpr, SelectItem, SetOp, Source, Star, Subquery,
    Subtree, Table, TypeCast, Unary, Values, ValuesRow, View, When, Where, WhereKind,
    WhereSubquery, Window,
};
