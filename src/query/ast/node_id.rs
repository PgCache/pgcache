use std::hash::{Hash, Hasher};

/// A node's pre-order position in its finalized tree (ADR-057). Identity
/// metadata only: it hashes to nothing and compares equal to any other id,
/// so a node's derived `Hash` and `PartialEq` stay structural.
#[derive(Debug, Clone, Copy)]
pub struct NodeId(u32);

impl NodeId {
    /// A node under construction; the finalize pass replaces it.
    pub const PENDING: NodeId = NodeId(u32::MAX);

    pub const fn index(self) -> usize {
        self.0 as usize
    }

    pub const fn is_pending(self) -> bool {
        self.0 == u32::MAX
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

/// The next pre-order position while a tree is being finalized.
#[derive(Debug, Default)]
pub struct NodePositions(u32);

impl NodePositions {
    /// Take the next position in pre-order.
    pub fn take(&mut self) -> NodeId {
        let id = NodeId(self.0);
        self.0 += 1;
        id
    }

    /// How many nodes have been positioned so far.
    pub fn count(&self) -> u32 {
        self.0
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
}
