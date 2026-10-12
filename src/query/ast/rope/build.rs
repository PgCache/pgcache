//! Building a tree: fresh nodes in pre-order, and subtrees of a base tree
//! placed without copying.

use std::sync::Arc;

use super::node::{Node, NodeKind};
use super::tree::{Chunk, QueryTree, position};
use super::view::NodeRef;
use crate::query::ast::{NodeId, NodeRemap};

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
    /// `(origin, position)` per node opened with an origin.
    origins: Vec<(NodeId, NodeId)>,
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
            origins: Vec::new(),
        }
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
    /// another tree, so what was bound to the origin follows it.
    pub fn open_from(&mut self, kind: NodeKind, origin: NodeId) -> Open {
        let open = self.open(kind);
        self.origins
            .push((origin, NodeId::from_index(open.position)));
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

    /// Place `subtree` of the base at the next position with some of its
    /// positions replaced: a subtree holding no replacement is kept whole,
    /// one holding some has its ancestors re-emitted with their origins.
    /// Where each replacement landed is recorded on `replacements`.
    pub fn copy(&mut self, subtree: NodeRef<'b>, replacements: &mut Replacements) {
        replacements.sort();
        self.copy_sorted(subtree, replacements);
    }

    fn copy_sorted(&mut self, node: NodeRef<'b>, replacements: &mut Replacements) {
        let id = position(node.id());
        let from = replacements.first_at_or_after(id);
        if let Some((index, replacement)) = replacements.at(from, id) {
            let placed = match replacement {
                Replacement::Leaf(kind) => self.leaf(kind.clone()),
                Replacement::LeafFrom(kind) => self.leaf_from(kind.clone(), node.id()),
                Replacement::Subtree(other) => self.keep(*other),
            };
            if let Some(slot) = replacements.placed.get_mut(index) {
                *slot = Some(placed);
            }
            return;
        }
        if !replacements.any_before(from, node.end()) {
            self.keep(node.id());
            return;
        }
        let open = self.open_from(node.kind().clone(), node.id());
        for child in node.children() {
            self.copy_sorted(child, replacements);
        }
        self.close(open);
    }

    /// The tree, compacted when its table grew past the threshold, and
    /// where every kept subtree and every node opened with an origin
    /// landed.
    pub fn finish(self) -> (QueryTree, Remap) {
        let mut origins = self.origins;
        origins.sort_by_key(|(origin, _)| origin.index());
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
        (
            tree,
            Remap {
                kept: self.kept,
                origins,
            },
        )
    }
}

impl Default for TreeBuilder<'_> {
    fn default() -> Self {
        Self::new()
    }
}

/// What a position of a copied subtree becomes.
#[derive(Debug, Clone)]
pub enum Replacement {
    /// A fresh leaf bound to nothing.
    Leaf(NodeKind),
    /// A fresh leaf standing in for the node it replaces, so what was bound
    /// to that node follows it.
    LeafFrom(NodeKind),
    /// Another subtree of the base, placed here.
    Subtree(NodeId),
}

/// The replacements of one [`TreeBuilder::copy`], by base position, and
/// where each landed once the copy is made.
#[derive(Debug, Default)]
pub struct Replacements {
    /// `(base position, index into `placed`, replacement)`; sorted by
    /// position once a copy begins.
    entries: Vec<(u32, usize, Replacement)>,
    placed: Vec<Option<NodeId>>,
    sorted: bool,
}

impl Replacements {
    pub fn new() -> Self {
        Self::default()
    }

    /// Replace the subtree at `at`; the index reads back where it landed.
    pub fn push(&mut self, at: NodeId, replacement: Replacement) -> usize {
        let index = self.placed.len();
        self.entries.push((position(at), index, replacement));
        self.placed.push(None);
        self.sorted = false;
        index
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// How many replacements have been pushed: the index the next one
    /// will take.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Where the replacement pushed as `index` landed; `None` before the
    /// copy or when its position was not under the copied subtree.
    pub fn placed(&self, index: usize) -> Option<NodeId> {
        self.placed.get(index).copied().flatten()
    }

    fn sort(&mut self) {
        if !self.sorted {
            self.entries.sort_by_key(|(at, _, _)| *at);
            self.sorted = true;
        }
    }

    fn first_at_or_after(&self, at: u32) -> usize {
        self.entries.partition_point(|(entry, _, _)| *entry < at)
    }

    fn at(&self, from: usize, at: u32) -> Option<(usize, &Replacement)> {
        self.entries
            .get(from)
            .filter(|(entry, _, _)| *entry == at)
            .map(|(_, index, replacement)| (*index, replacement))
    }

    /// Whether an entry at or after `from` sits before `end`.
    fn any_before(&self, from: usize, end: u32) -> bool {
        self.entries.get(from).is_some_and(|(at, _, _)| *at < end)
    }
}

/// Where the nodes of a base tree landed in a built one: the subtrees the
/// builder kept, one range per `keep`, and the nodes it opened with an
/// origin. A position kept in several places has several new ids.
#[derive(Debug, Default, Clone)]
pub struct Remap {
    kept: Vec<Kept>,
    /// `(origin, position)` in origin order.
    origins: Vec<(NodeId, NodeId)>,
}

impl Remap {
    pub fn is_empty(&self) -> bool {
        self.kept.is_empty() && self.origins.is_empty()
    }
}

impl NodeRemap for Remap {
    fn new_ids(&self, old: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        let old = position(old);
        let start = self
            .origins
            .partition_point(|(origin, _)| position(*origin) < old);
        let opened = self
            .origins
            .get(start..)
            .unwrap_or(&[])
            .iter()
            .take_while(move |(origin, _)| position(*origin) == old)
            .map(|(_, to)| *to);
        self.kept
            .iter()
            .filter(move |kept| kept.old_start <= old && old < kept.old_end)
            .map(move |kept| NodeId::from_index(kept.new_start + (old - kept.old_start)))
            .chain(opened)
    }
}
