use std::hash::{Hash, Hasher};

/// A node's pre-order position in its tree (ADR-057). Identity
/// metadata only: it hashes to nothing and compares equal to any other id,
/// so a node's derived `Hash` and `PartialEq` stay structural.
#[derive(Debug, Clone, Copy)]
pub struct NodeId(u32);

impl NodeId {
    /// No position: a node not placed in a tree.
    pub const PENDING: NodeId = NodeId(u32::MAX);

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

impl Hash for NodeId {
    fn hash<H: Hasher>(&self, _state: &mut H) {}
}

impl PartialEq for NodeId {
    fn eq(&self, _other: &Self) -> bool {
        true
    }
}

impl Eq for NodeId {}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;

    use super::*;

    #[test]
    fn test_node_id_is_identity_metadata_not_structure() {
        let first = NodeId::from_index(1);
        let second = NodeId::from_index(2);
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
}
