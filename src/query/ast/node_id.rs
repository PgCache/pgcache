/// A node's pre-order position in its tree (ADR-057). Two ids are equal
/// when they name the same position.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use super::*;

    /// Ids compare and hash by position.
    #[test]
    fn test_node_id_compares_by_position() {
        let first = NodeId::from_index(1);
        let second = NodeId::from_index(2);
        assert_ne!(first, second);
        assert_eq!(first, NodeId::from_index(1));
        let ids: HashSet<NodeId> = [first, second, NodeId::from_index(1)].into_iter().collect();
        assert_eq!(ids.len(), 2);

        assert!(NodeId::PENDING.is_pending());
        assert!(!first.is_pending());
        assert_eq!(second.index(), 2);
    }
}
