use std::hash::{Hash, Hasher};

/// A node's pre-order position in its finalized tree (ADR-057). Identity
/// metadata only: it hashes to nothing and compares equal to any other id,
/// so a node's derived `Hash` and `PartialEq` stay structural.
#[derive(Debug, Clone, Copy)]
pub struct NodeId(u32);

impl NodeId {
    /// A node under construction; the finalize pass replaces it.
    pub const PENDING: NodeId = NodeId(u32::MAX);

    /// The `k`th id a rewrite hands a node it builds, so the refinalize
    /// remap reports where that node landed. Counted down from the top so
    /// it never meets a real position.
    pub const fn provisional(k: u32) -> NodeId {
        NodeId(u32::MAX - 1 - k)
    }

    /// The id of the node at pre-order position `index`.
    pub const fn from_index(index: u32) -> NodeId {
        NodeId(index)
    }

    pub const fn index(self) -> usize {
        self.0 as usize
    }

    pub const fn is_pending(self) -> bool {
        self.0 == u32::MAX
    }
}

/// Where each node of a refinalized tree came from: the id it carried
/// before the pass against its new position, in old-id order. A node copied
/// into several places maps to several positions.
#[derive(Debug, Default)]
pub struct IdRemap {
    pairs: Vec<(NodeId, NodeId)>,
}

impl IdRemap {
    /// Every new position of the node that carried `old`.
    pub fn new_ids(&self, old: NodeId) -> impl Iterator<Item = NodeId> + '_ {
        let start = self
            .pairs
            .partition_point(|(from, _)| from.index() < old.index());
        self.pairs
            .get(start..)
            .unwrap_or(&[])
            .iter()
            .take_while(move |(from, _)| from.index() == old.index())
            .map(|(_, to)| *to)
    }

    /// The new position of the node that carried `old`, when it was kept
    /// in one place.
    pub fn new_id(&self, old: NodeId) -> Option<NodeId> {
        self.new_ids(old).next()
    }

    pub fn is_empty(&self) -> bool {
        self.pairs.is_empty()
    }
}

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl PartialEq for NodeId {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NodeId {}

/// The next pre-order position while a tree is being finalized, and the
/// old-to-new pairs of the nodes that already carried one.
#[derive(Debug, Default)]
pub struct NodePositions {
    next: u32,
    remap: IdRemap,
}

impl NodePositions {
    /// Take the next position in pre-order.
    pub fn take(&mut self) -> NodeId {
        self.stamp(NodeId::PENDING)
    }

    /// Take the next position for a node that carried `old`, recording the
    /// pair unless the node was pending.
    pub fn stamp(&mut self, old: NodeId) -> NodeId {
        let id = NodeId(self.next);
        self.next += 1;
        if !old.is_pending() {
            self.remap.pairs.push((old, id));
        }
        id
    }

    /// How many nodes have been positioned so far.
    pub fn count(&self) -> u32 {
        self.next
    }

    /// The recorded pairs, in old-id order.
    pub fn into_remap(self) -> IdRemap {
        let mut remap = self.remap;
        remap.pairs.sort_by_key(|(from, _)| from.index());
        remap
    }
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn test_node_id_is_identity_metadata_not_structure() {
        let mut positions = NodePositions::default();
        let _root = positions.take();
        let first = positions.take();
        let second = positions.take();
        assert_eq!(first, second);

        let mut hasher = DefaultHasher::new();
        first.hash(&mut hasher);
        let hashed_first = hasher.finish();
        let mut hasher = DefaultHasher::new();
        second.hash(&mut hasher);
        assert_eq!(hashed_first, hasher.finish());

        assert!(NodeId::PENDING.is_pending());
        assert!(!first.is_pending());
        assert_eq!(second.index(), 2);
    }

    #[test]
    fn test_node_positions_count_in_order() {
        let mut positions = NodePositions::default();
        assert_eq!(positions.take().index(), 0);
        assert_eq!(positions.take().index(), 1);
        assert_eq!(positions.count(), 2);
    }

    /// Stamping records where every non-pending id landed, a copied node
    /// twice, and pending nodes not at all.
    #[test]
    fn test_stamp_records_old_to_new_pairs() {
        let mut positions = NodePositions::default();
        positions.stamp(NodeId(7));
        positions.stamp(NodeId::PENDING);
        positions.stamp(NodeId(3));
        positions.stamp(NodeId(7));
        positions.stamp(NodeId::provisional(0));
        let remap = positions.into_remap();
        let new_of = |old: NodeId| remap.new_ids(old).map(NodeId::index).collect::<Vec<_>>();
        assert_eq!(new_of(NodeId(7)), vec![0, 3]);
        assert_eq!(new_of(NodeId(3)), vec![2]);
        assert_eq!(new_of(NodeId::provisional(0)), vec![4]);
        assert_eq!(new_of(NodeId(1)), Vec::<usize>::new());
        assert_eq!(remap.new_id(NodeId(7)).map(NodeId::index), Some(0));
        assert!(remap.new_id(NodeId::PENDING).is_none());
    }
}
