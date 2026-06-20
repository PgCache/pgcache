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

/// Traversable AST node. The zero-allocation `try_for_each_node` visitor is
/// hand-written per type; `nodes()` is a provided collecting wrapper over it.
pub trait AstNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B>;

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
