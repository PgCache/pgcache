//! Views: a tree reference and a position, exposing a node's fields and
//! children as methods. A view is `Copy` and carries the chunk it last
//! read, so a step to a child or sibling searches the table only when it
//! crosses a chunk boundary.

#![allow(clippy::wildcard_enum_match_arm)]

mod expr;
mod query;
mod source;

use std::marker::PhantomData;

use ecow::EcoString;

pub use expr::{FrameBound, ScalarKind, WhereKind};
pub use query::{Body, SelectItem};
pub use source::{JoinQual, Source};

use super::node::{Node, NodeKind};
use super::tree::QueryTree;
use crate::query::ast::{LiteralValue, NodeId};

/// A position in a tree.
#[derive(Clone, Copy)]
pub struct NodeRef<'t> {
    tree: &'t QueryTree,
    id: u32,
    chunk: usize,
}

impl<'t> NodeRef<'t> {
    pub(super) fn root(tree: &'t QueryTree) -> Self {
        Self {
            tree,
            id: 0,
            chunk: 0,
        }
    }

    pub fn tree(&self) -> &'t QueryTree {
        self.tree
    }

    pub fn id(&self) -> NodeId {
        NodeId::from_index(self.id)
    }

    pub fn node(&self) -> &'t Node {
        self.tree.node_in(self.chunk, self.id)
    }

    pub fn kind(&self) -> &'t NodeKind {
        &self.node().kind
    }

    /// The subtree's length, this node included.
    pub fn subtree_len(&self) -> u32 {
        self.node().len
    }

    /// The position after this subtree.
    pub fn end(&self) -> u32 {
        self.id + self.subtree_len()
    }

    fn at(&self, id: u32) -> NodeRef<'t> {
        let chunk = if id < self.tree.len() {
            self.tree.chunk_index_hinted(self.chunk, id)
        } else {
            self.chunk
        };
        NodeRef {
            tree: self.tree,
            id,
            chunk,
        }
    }

    pub fn first_child(&self) -> NodeRef<'t> {
        self.at(self.id + 1)
    }

    pub fn next_sibling(&self) -> NodeRef<'t> {
        self.at(self.end())
    }

    /// This node's children, each the root of a subtree.
    pub fn children(&self) -> Children<'t> {
        Children {
            next: self.first_child(),
            end: self.end(),
        }
    }

    /// `count` children as `V`, starting after the first `skip`.
    fn run<V: View<'t>>(&self, skip: u32, count: u32) -> Run<'t, V> {
        let mut next = self.first_child();
        for _ in 0..skip {
            next = next.next_sibling();
        }
        Run {
            next,
            remaining: count,
            view: PhantomData,
        }
    }

    /// The child after the first `skip`, as `V`.
    fn nth<V: View<'t>>(&self, skip: u32) -> V {
        let mut next = self.first_child();
        for _ in 0..skip {
            next = next.next_sibling();
        }
        V::from_ref(next)
    }
}

/// The children of a node, as positions.
pub struct Children<'t> {
    next: NodeRef<'t>,
    end: u32,
}

impl<'t> Iterator for Children<'t> {
    type Item = NodeRef<'t>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next.id >= self.end {
            return None;
        }
        let current = self.next;
        self.next = current.next_sibling();
        Some(current)
    }
}

/// A fixed number of sibling subtrees, each viewed as `V`.
pub struct Run<'t, V> {
    next: NodeRef<'t>,
    remaining: u32,
    view: PhantomData<V>,
}

impl<'t, V: View<'t>> Iterator for Run<'t, V> {
    type Item = V;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        self.remaining -= 1;
        let current = self.next;
        self.next = current.next_sibling();
        Some(V::from_ref(current))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.remaining as usize;
        (remaining, Some(remaining))
    }
}

impl<'t, V: View<'t>> ExactSizeIterator for Run<'t, V> {}

impl<V> Clone for Run<'_, V> {
    fn clone(&self) -> Self {
        *self
    }
}

impl<V> Copy for Run<'_, V> {}

/// A typed window on a position.
pub trait View<'t>: Copy {
    fn from_ref(node: NodeRef<'t>) -> Self;
    fn node_ref(&self) -> NodeRef<'t>;

    fn id(&self) -> NodeId {
        self.node_ref().id()
    }
}

/// Reached only when a view points at a node of another kind, which the
/// builders never produce.
fn mismatch<T: Default>(view: &str) -> T {
    debug_assert!(false, "a {view} view points at a node of another kind");
    T::default()
}

static NULL: LiteralValue = LiteralValue::Null;
static EMPTY_NAME: EcoString = EcoString::new();

macro_rules! view {
    ($name:ident) => {
        #[derive(Clone, Copy)]
        pub struct $name<'t>(pub(super) NodeRef<'t>);

        impl<'t> View<'t> for $name<'t> {
            fn from_ref(node: NodeRef<'t>) -> Self {
                Self(node)
            }

            fn node_ref(&self) -> NodeRef<'t> {
                self.0
            }
        }

        impl $name<'_> {
            pub fn id(&self) -> NodeId {
                self.0.id()
            }
        }
    };
}

view!(Query);
view!(Cte);
view!(Select);
view!(Values);
view!(ValuesRow);
view!(SetOp);
view!(OrderBy);
view!(SelectExpr);
view!(Star);
view!(Table);
view!(Subquery);
view!(CteRef);
view!(Join);
view!(Function);
view!(Window);
view!(Frame);
view!(Literal);
view!(Column);
view!(Unary);
view!(Binary);
view!(Multi);
view!(WhereSubquery);
view!(Arithmetic);
view!(ScalarSubquery);
view!(Array);
view!(TypeCast);
view!(Case);
view!(When);
view!(Name);
view!(Where);
view!(Scalar);
