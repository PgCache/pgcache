use std::any::Any;
use std::ops::ControlFlow;

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

/// Traversable AST node: a zero-allocation pre-order walk. Each type implements
/// only [`AstNode::try_for_each_child`]; visiting the node itself is provided,
/// so no type can skip it or visit out of order.
pub trait AstNode: Any + Sized {
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
