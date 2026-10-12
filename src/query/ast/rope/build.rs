//! Building a tree: fresh nodes in pre-order, and subtrees of a base tree
//! placed without copying.

use std::sync::Arc;

use super::node::{Node, NodeKind};
use super::tree::{Chunk, QueryTree, position};
use crate::query::ast::{IdRemap, NodeId};

/// A table is compacted past this many chunks or a quarter of the
/// positions, whichever is larger: lookups and sequential walks stay cheap
/// without copying the common case.
const COMPACT_CHUNK_FLOOR: usize = 16;

/// Builds a tree front to back. `open`/`close` make a node whose children
/// are whatever is built in between; `keep` places a subtree of the base
/// tree as table entries over the base's slices.
pub struct TreeBuilder<'b> {
    base: Option<&'b QueryTree>,
    pending: Vec<Pending>,
    /// Every node this builder makes, in position order.
    fresh: Vec<Node>,
    /// The next position.
    next: u32,
    kept: Vec<Kept>,
    /// `(origin, position)` per node opened with an origin, when recording.
    origins: Option<Vec<(NodeId, NodeId)>>,
}

enum Pending {
    /// Positions kept from the base; `start` is already the new one.
    Base(Chunk),
    /// A run of fresh nodes starting at position `start` and index
    /// `offset` of `fresh`.
    Fresh { start: u32, offset: u32, len: u32 },
}

/// A kept subtree: where it was, where it is now.
#[derive(Debug, Clone, Copy)]
struct Kept {
    old_start: u32,
    old_end: u32,
    new_start: u32,
}

/// A fresh node whose children are being built; `close` sets its length.
#[must_use = "an open node must be closed for its length to be set"]
pub struct Open {
    fresh_index: usize,
    position: u32,
}

impl<'b> TreeBuilder<'b> {
    pub fn new() -> Self {
        Self {
            base: None,
            pending: Vec::new(),
            fresh: Vec::new(),
            next: 0,
            kept: Vec::new(),
            origins: None,
        }
    }

    /// Record where each node opened with an origin lands, for
    /// [`TreeBuilder::finish_with_origins`].
    pub fn recording_origins(mut self) -> Self {
        self.origins = Some(Vec::new());
        self
    }

    /// A builder that can `keep` subtrees of `base`.
    pub fn over(base: &'b QueryTree) -> Self {
        Self {
            base: Some(base),
            ..Self::new()
        }
    }

    /// The position the next node will take.
    pub fn next_id(&self) -> NodeId {
        NodeId::from_index(self.next)
    }

    /// Begin a node at the next position; its children follow.
    pub fn open(&mut self, kind: NodeKind) -> Open {
        let position = self.next;
        let fresh_index = self.fresh.len();
        self.fresh.push(Node { len: 1, kind });
        match self.pending.last_mut() {
            Some(Pending::Fresh { len, .. }) => *len += 1,
            _ => self.pending.push(Pending::Fresh {
                start: position,
                offset: u32::try_from(fresh_index).unwrap_or(u32::MAX),
                len: 1,
            }),
        }
        self.next += 1;
        Open {
            fresh_index,
            position,
        }
    }

    /// End a node: its subtree is everything built since it was opened.
    pub fn close(&mut self, open: Open) -> NodeId {
        if let Some(node) = self.fresh.get_mut(open.fresh_index) {
            node.len = self.next - open.position;
        }
        NodeId::from_index(open.position)
    }

    /// [`TreeBuilder::open`] for a node standing in for `origin` of
    /// another tree, recorded when origins are.
    pub fn open_from(&mut self, kind: NodeKind, origin: NodeId) -> Open {
        let open = self.open(kind);
        if let Some(origins) = &mut self.origins {
            origins.push((origin, NodeId::from_index(open.position)));
        }
        open
    }

    /// A node with no children.
    pub fn leaf(&mut self, kind: NodeKind) -> NodeId {
        let open = self.open(kind);
        self.close(open)
    }

    /// [`TreeBuilder::leaf`] with an origin.
    pub fn leaf_from(&mut self, kind: NodeKind, origin: NodeId) -> NodeId {
        let open = self.open_from(kind, origin);
        self.close(open)
    }

    /// Place `subtree` of the base at the next position: table entries over
    /// its underlying slices, clipped to the subtree, no node copied.
    pub fn keep(&mut self, subtree: NodeId) -> NodeId {
        let new_start = self.next;
        let Some(base) = self.base else {
            debug_assert!(false, "keep needs a base tree");
            return NodeId::from_index(new_start);
        };
        let old_start = position(subtree);
        let old_end = old_start + base.node(subtree).len;
        let chunks = base.chunks();
        for chunk in chunks.iter().skip(base.chunk_index(old_start)) {
            if chunk.start >= old_end {
                break;
            }
            let from = chunk.start.max(old_start);
            let to = chunk.end().min(old_end);
            if from >= to {
                continue;
            }
            self.base_push(Chunk {
                start: new_start + (from - old_start),
                len: to - from,
                nodes: Arc::clone(&chunk.nodes),
                offset: chunk.offset + (from - chunk.start),
            });
        }
        self.kept.push(Kept {
            old_start,
            old_end,
            new_start,
        });
        self.next += old_end - old_start;
        NodeId::from_index(new_start)
    }

    /// Add a kept run, extending the previous entry when it continues the
    /// same slice at the next position, so adjacent keeps make one chunk.
    fn base_push(&mut self, chunk: Chunk) {
        if let Some(Pending::Base(last)) = self.pending.last_mut()
            && last.continues_into(&chunk)
        {
            last.len += chunk.len;
            return;
        }
        self.pending.push(Pending::Base(chunk));
    }

    /// The tree, compacted when its table grew past the threshold, and
    /// where every kept subtree landed.
    pub fn finish(self) -> (QueryTree, ChunkRemap) {
        let (tree, remap, _) = self.finish_with_origins();
        (tree, remap)
    }

    /// [`TreeBuilder::finish`] with where every node opened with an origin
    /// landed; empty unless the builder was recording.
    pub fn finish_with_origins(self) -> (QueryTree, ChunkRemap, OriginMap) {
        let mut pairs = self.origins.unwrap_or_default();
        pairs.sort_by_key(|(origin, _)| origin.index());
        let origins = OriginMap { pairs };
        let fresh: Arc<[Node]> = self.fresh.into();
        let chunks: Vec<Chunk> = self
            .pending
            .into_iter()
            .map(|pending| match pending {
                Pending::Base(chunk) => chunk,
                Pending::Fresh { start, offset, len } => Chunk {
                    start,
                    len,
                    nodes: Arc::clone(&fresh),
                    offset,
                },
            })
            .collect();
        let len = self.next;
        let threshold = COMPACT_CHUNK_FLOOR.max(len as usize / 4);
        let tree = QueryTree::from_chunks(chunks, len);
        let tree = if tree.chunk_count() > threshold {
            tree.compacted()
        } else {
            tree
        };
        (tree, ChunkRemap { kept: self.kept }, origins)
    }
}

/// Where nodes of another tree landed in a built one, by their origin id.
#[derive(Debug, Default, Clone)]
pub struct OriginMap {
    pairs: Vec<(NodeId, NodeId)>,
}

impl OriginMap {
    /// The position of the node that stood in for `origin`.
    pub fn position(&self, origin: NodeId) -> Option<NodeId> {
        self.pairs
            .binary_search_by_key(&origin.index(), |(from, _)| from.index())
            .ok()
            .and_then(|found| self.pairs.get(found))
            .map(|(_, to)| *to)
    }

    pub fn len(&self) -> usize {
        self.pairs.len()
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }

    /// The map as a remap, so side tables keyed by origin ids carry over
    /// through the same path a rewrite's do.
    pub(crate) fn to_remap(&self) -> IdRemap {
        IdRemap::from_sorted_pairs(self.pairs.clone())
    }
}

impl Default for TreeBuilder<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// Where the subtrees a builder kept came from: one range per `keep`, so
/// a position's new ids are its offsets into every range that held it.
#[derive(Debug, Default)]
pub struct ChunkRemap {
    kept: Vec<Kept>,
}

impl ChunkRemap {
    /// Every new position of the node that was at `old`.
    pub fn new_ids(&self, old: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        let old = position(old);
        self.kept
            .iter()
            .filter(move |kept| kept.old_start <= old && old < kept.old_end)
            .map(move |kept| NodeId::from_index(kept.new_start + (old - kept.old_start)))
    }

    /// The new position of the node that was at `old`, when it was kept
    /// in one place.
    pub fn new_id(&self, old: NodeId) -> Option<NodeId> {
        self.new_ids(old).next()
    }

    pub fn is_empty(&self) -> bool {
        self.kept.is_empty()
    }
}
