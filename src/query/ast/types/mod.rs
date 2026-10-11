use std::any::Any;
use std::ops::ControlFlow;

use crate::query::ast::{NodeId, NodePositions};

mod expr;
mod query;
mod table;
mod window;

pub use expr::*;
pub use query::*;
pub use table::*;
pub use window::*;

/// Walk each of `children`'s subtrees in order: a list, an `Option` (zero or
/// one), or an array of same-typed fields.
pub(crate) fn children_visit<'a, T: AstNode + 'a, N: Any, B>(
    children: impl IntoIterator<Item = &'a T>,
    f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
) -> ControlFlow<B> {
    for child in children {
        child.try_for_each_node(f)?;
    }
    ControlFlow::Continue(())
}

/// Finalize each of `children`'s subtrees in order; the mutable twin of
/// [`children_visit`].
pub(crate) fn children_finalize<'a, T: AstNode + 'a>(
    children: impl IntoIterator<Item = &'a mut T>,
    positions: &mut NodePositions,
) {
    for child in children {
        child.finalize(positions);
    }
}

/// Traversable AST node: a zero-allocation pre-order walk. Each type implements
/// only [`AstNode::try_for_each_child`]; visiting the node itself is provided,
/// so no type can skip it or visit out of order.
///
/// Finalizing mirrors visiting: [`AstNode::finalize`] takes the node's
/// pre-order position, stores it through [`AstNode::id_set`] (a no-op for
/// enum wrappers and leaf values, which carry none) and recurses through
/// [`AstNode::children_finalize`], which must cover exactly the children
/// `try_for_each_child` covers, in the same order.
pub trait AstNode: Any + Sized {
    /// Stamp this node's pre-order position, then every descendant's. A
    /// node that already carries an id is being refinalized: the pair is
    /// recorded so side tables keyed on the old id can follow it.
    fn finalize(&mut self, positions: &mut NodePositions) {
        let id = positions.stamp(self.id());
        self.id_set(id);
        self.children_finalize(positions);
    }

    /// This node's id. Types without an `id` field are never positioned.
    fn id(&self) -> NodeId {
        NodeId::PENDING
    }

    /// Store this node's id. Types without an `id` field keep the default.
    fn id_set(&mut self, _id: NodeId) {}

    /// Finalize each direct child's subtree, in `try_for_each_child` order.
    fn children_finalize(&mut self, _positions: &mut NodePositions) {}

    /// Visit this node, then every descendant, in pre-order.
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.try_for_each_child(f)
    }

    /// Walk each direct child's subtree (via its `try_for_each_node`). Leaf
    /// types have none.
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        _f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        ControlFlow::Continue(())
    }

    /// Collect all descendant nodes of type `N` (provided).
    fn nodes<N: Any>(&self) -> impl Iterator<Item = &N> {
        let mut out = Vec::new();
        let _ = self.try_for_each_node::<N, ()>(&mut |n| {
            out.push(n);
            ControlFlow::Continue(())
        });
        out.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use std::mem::size_of;

    use super::*;

    /// Node sizes are a design budget (ADR-057): a variant or field that
    /// grows a common node past its ceiling has to be boxed or justified
    /// here, not absorbed.
    #[test]
    fn test_node_sizes_stay_within_budget() {
        let budget: &[(&str, usize, usize)] = &[
            ("NodeId", size_of::<NodeId>(), 4),
            ("QueryExpr", size_of::<QueryExpr>(), 280),
            ("QueryBody", size_of::<QueryBody>(), 32),
            ("SelectNode", size_of::<SelectNode>(), 432),
            ("SelectColumn", size_of::<SelectColumn>(), 136),
            ("StarNode", size_of::<StarNode>(), 32),
            ("TableSource", size_of::<TableSource>(), 136),
            ("TableNode", size_of::<TableNode>(), 88),
            ("JoinNode", size_of::<JoinNode>(), 136),
            ("ScalarExpr", size_of::<ScalarExpr>(), 112),
            ("WhereExpr", size_of::<WhereExpr>(), 112),
            ("BinaryExpr", size_of::<BinaryExpr>(), 24),
            ("ColumnNode", size_of::<ColumnNode>(), 48),
            ("LiteralValue", size_of::<LiteralValue>(), 40),
            ("FunctionCall", size_of::<FunctionCall>(), 112),
            ("OrderByClause", size_of::<OrderByClause>(), 120),
            ("CteDefinition", size_of::<CteDefinition>(), 328),
        ];
        let over: Vec<String> = budget
            .iter()
            .filter(|(_, actual, ceiling)| actual > ceiling)
            .map(|(name, actual, ceiling)| format!("{name}: {actual} > {ceiling}"))
            .collect();
        assert!(over.is_empty(), "nodes over their size budget: {over:?}");
    }
}
