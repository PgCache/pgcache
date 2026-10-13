//! The tree: a table of chunks over shared node slices, in position order.

#![allow(clippy::wildcard_enum_match_arm)]

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use super::node::{Node, NodeKind};
use super::view::{NodeRef, Query};
use crate::query::ast::NodeId;

/// Tables up to this many chunks are scanned; longer ones are searched.
/// A fixed crossover: the code runs on many machines, and this is where
/// a branchless scan and a binary search meet for `u32` keys on most.
const LINEAR_SCAN_MAX: usize = 32;

/// A query tree (ADR-057). Cloning bumps one refcount; the chunks and the
/// node slices they point at are shared with every tree built from this one.
#[derive(Debug, Clone)]
pub struct QueryTree {
    table: Arc<ChunkTable>,
    len: u32,
}

#[derive(Debug)]
struct ChunkTable {
    chunks: Box<[Chunk]>,
}

/// A run of positions that lives in one node slice.
#[derive(Debug, Clone)]
pub(super) struct Chunk {
    /// The absolute position of the chunk's first node.
    pub(super) start: u32,
    pub(super) len: u32,
    pub(super) nodes: Arc<[Node]>,
    /// Where in `nodes` the chunk begins.
    pub(super) offset: u32,
}

impl Chunk {
    pub(super) fn end(&self) -> u32 {
        self.start + self.len
    }

    /// Whether `next` is the same slice continuing at the next position.
    pub(super) fn continues_into(&self, next: &Chunk) -> bool {
        Arc::ptr_eq(&self.nodes, &next.nodes)
            && self.end() == next.start
            && self.offset + self.len == next.offset
    }

    fn covers(&self, id: u32) -> bool {
        self.start <= id && id < self.end()
    }

    fn node(&self, id: u32) -> Option<&Node> {
        let index = self.offset.checked_add(id.checked_sub(self.start)?)?;
        self.nodes.get(index as usize)
    }

    fn slice(&self) -> &[Node] {
        let from = self.offset as usize;
        self.nodes
            .get(from..from + self.len as usize)
            .unwrap_or(&[])
    }
}

/// What a lookup outside the tree yields. A view never asks for one, so
/// reaching it is a defect the debug assertion reports.
static OUTSIDE: Node = Node {
    len: 1,
    kind: NodeKind::When,
};

/// The entry covering `id` among entries sorted by `start`; the first
/// starts at 0, so every position in the tree has one.
pub(super) fn chunk_index_in<T>(entries: &[T], start: impl Fn(&T) -> u32, id: u32) -> usize {
    let covering = if entries.len() <= LINEAR_SCAN_MAX {
        entries.iter().filter(|entry| start(entry) <= id).count()
    } else {
        entries.partition_point(|entry| start(entry) <= id)
    };
    covering.saturating_sub(1)
}

impl QueryTree {
    pub(super) fn from_chunks(chunks: Vec<Chunk>, len: u32) -> Self {
        Self {
            table: Arc::new(ChunkTable {
                chunks: chunks.into_boxed_slice(),
            }),
            len,
        }
    }

    /// How many positions the tree has.
    pub fn len(&self) -> u32 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn chunk_count(&self) -> usize {
        self.table.chunks.len()
    }

    pub(super) fn chunks(&self) -> &[Chunk] {
        &self.table.chunks
    }

    pub(super) fn chunk_index(&self, id: u32) -> usize {
        chunk_index_in(&self.table.chunks, |chunk| chunk.start, id)
    }

    /// The chunk covering `id`: `hint` when it still does, else a lookup.
    pub(super) fn chunk_index_hinted(&self, hint: usize, id: u32) -> usize {
        match self.table.chunks.get(hint) {
            Some(chunk) if chunk.covers(id) => hint,
            _ => self.chunk_index(id),
        }
    }

    /// The node at `id`, read through the chunk `hint` names when it
    /// covers the position.
    pub(super) fn node_in(&self, hint: usize, id: u32) -> &Node {
        let index = self.chunk_index_hinted(hint, id);
        match self
            .table
            .chunks
            .get(index)
            .and_then(|chunk| chunk.node(id))
        {
            Some(node) => node,
            None => {
                debug_assert!(false, "position {id} is inside the tree");
                &OUTSIDE
            }
        }
    }

    pub fn node(&self, id: NodeId) -> &Node {
        self.node_in(0, position(id))
    }

    pub fn root(&self) -> Query<'_> {
        Query(NodeRef::root(self))
    }

    /// Every node in position order.
    pub fn nodes(&self) -> impl Iterator<Item = &Node> {
        self.table.chunks.iter().flat_map(Chunk::slice)
    }

    /// Every base table the tree references, subqueries and CTE bodies
    /// included, as `(schema, name)`.
    pub fn table_names(&self) -> impl Iterator<Item = (Option<&str>, &str)> {
        self.nodes().filter_map(|node| match &node.kind {
            NodeKind::Table { schema, name, .. } => Some((
                (!schema.is_empty()).then_some(schema.as_str()),
                name.as_str(),
            )),
            _ => None,
        })
    }

    /// The same tree in one chunk.
    pub fn compacted(&self) -> QueryTree {
        let nodes: Arc<[Node]> = self.nodes().cloned().collect();
        Self::from_chunks(
            vec![Chunk {
                start: 0,
                len: self.len,
                nodes,
                offset: 0,
            }],
            self.len,
        )
    }

    /// Hash what identifies the query to a session: everything but the
    /// root's LIMIT and OFFSET, which the dispatch tracks apart so queries
    /// differing only there share an entry.
    pub fn session_hash<H: Hasher>(&self, state: &mut H) {
        let mut nodes = self.nodes();
        let Some(root) = nodes.next() else {
            return;
        };
        let limit = root.kind.limit_count();
        (root.len - limit).hash(state);
        match &root.kind {
            NodeKind::Query {
                cte_count,
                order_by_count,
                ..
            } => {
                cte_count.hash(state);
                order_by_count.hash(state);
            }
            other => other.hash(state),
        }
        for node in nodes.take((self.len - 1 - limit) as usize) {
            node.hash(state);
        }
    }
}

impl PartialEq for QueryTree {
    fn eq(&self, other: &Self) -> bool {
        self.len == other.len && self.nodes().eq(other.nodes())
    }
}

impl Hash for QueryTree {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for node in self.nodes() {
            node.hash(state);
        }
    }
}

/// A position as the `u32` it is stored as.
pub(super) fn position(id: NodeId) -> u32 {
    u32::try_from(id.index()).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunk_lookup_scans_short_tables_and_searches_long_ones() {
        let at = |starts: &[u32], id: u32| chunk_index_in(starts, |start| *start, id);
        let short = [0, 2, 4, 5, 6];
        assert_eq!(at(&short, 0), 0);
        assert_eq!(at(&short, 1), 0);
        assert_eq!(at(&short, 2), 1);
        assert_eq!(at(&short, 5), 3);
        assert_eq!(at(&short, 9), 4);
        let long: Vec<u32> = (0..100).map(|i| i * 3).collect();
        assert!(long.len() > LINEAR_SCAN_MAX);
        assert_eq!(at(&long, 0), 0);
        assert_eq!(at(&long, 2), 0);
        assert_eq!(at(&long, 3), 1);
        assert_eq!(at(&long, 298), 99);
        assert_eq!(at(&long, 1_000), 99);
    }
}
