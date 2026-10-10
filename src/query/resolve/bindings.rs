//! What resolution adds to a finalized tree (ADR-057): a side table keyed by
//! node id, built beside the resolved tree while the two coexist.

use std::sync::Arc;

use ecow::EcoString;

use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
use crate::query::ast::{ColumnNode, NodeId, QueryExpr, StarNode};

/// Index of a table source in [`Bindings::slots`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SlotId(u32);

impl SlotId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// Index of a `USING`/`NATURAL` merged column in [`Bindings::merged_columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct MergedId(u32);

impl MergedId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

/// One FROM entry of the whole tree, subqueries included. Both variants
/// share their metadata with the catalog or the resolver that built it, so a
/// binding is a slot and a position and nothing is copied per reference.
#[derive(Debug, Clone)]
pub enum TableSlot {
    /// A catalog table.
    Base {
        table: Arc<TableMetadata>,
        alias: Option<EcoString>,
    },
    /// A FROM subquery or CTE reference; `table` is the synthetic metadata
    /// the resolver derives from its output list (name = alias, oid 0).
    Derived {
        node: NodeId,
        table: Arc<TableMetadata>,
    },
}

impl TableSlot {
    pub fn table(&self) -> &TableMetadata {
        match self {
            TableSlot::Base { table, .. } | TableSlot::Derived { table, .. } => table,
        }
    }

    /// The name a reference qualifies this slot by: the alias, else the
    /// table name (a derived table's synthetic name is its alias).
    pub fn qualifier(&self) -> &str {
        match self {
            TableSlot::Base { table, alias } => alias.as_deref().unwrap_or(table.name.as_str()),
            TableSlot::Derived { table, .. } => table.name.as_str(),
        }
    }
}

/// A column reference's resolution: which slot and which column of it. Name
/// and type live once, on the slot's table.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnBinding {
    pub slot: SlotId,
    pub position: ColumnPosition,
}

/// What a [`ColumnNode`] stands for once resolved.
#[derive(Debug, Clone)]
pub enum ColumnTarget {
    Column(ColumnBinding),
    /// An unqualified reference to a `USING`/`NATURAL` merged column.
    Merged(MergedId),
    /// An ORDER BY item naming a select-list output rather than a column.
    Output,
}

/// A `USING`/`NATURAL` join's merged output column: the two side columns it
/// stands for, `COALESCE(left, right)` under an outer join and plain `left`
/// otherwise.
#[derive(Debug, Clone)]
pub struct MergedColumnBinding {
    pub join: NodeId,
    pub name: EcoString,
    pub left: ColumnBinding,
    pub right: ColumnBinding,
    pub outer: bool,
}

/// One column a `*` expands to.
#[derive(Debug, Clone)]
pub enum StarItem {
    Column(ColumnBinding),
    Merged(MergedId),
}

/// The bindings of one finalized tree. Node-keyed vectors are kept in id
/// order so a lookup is a binary search.
#[derive(Debug, Clone, Default)]
pub struct Bindings {
    slots: Vec<TableSlot>,
    sources: Vec<(NodeId, SlotId)>,
    columns: Vec<(NodeId, ColumnTarget)>,
    stars: Vec<(NodeId, Vec<StarItem>)>,
    merged: Vec<MergedColumnBinding>,
    /// Column nodes bound to a slot outside their own query level.
    correlated: Vec<NodeId>,
}

fn by_node<T>(entries: &[(NodeId, T)], id: NodeId) -> Option<&T> {
    entries
        .binary_search_by_key(&id.index(), |(node, _)| node.index())
        .ok()
        .and_then(|found| entries.get(found))
        .map(|(_, entry)| entry)
}

impl Bindings {
    pub fn column(&self, node: &ColumnNode) -> Option<&ColumnTarget> {
        self.column_by_id(node.id)
    }

    /// The catalog column a binding names.
    pub fn binding_column(&self, binding: &ColumnBinding) -> Option<&ColumnMetadata> {
        self.slot(binding.slot)?
            .table()
            .columns
            .by_position(binding.position)
    }

    /// The catalog column a `Column`-bound node names; `None` for a merged
    /// or output reference.
    pub fn column_metadata(&self, node: &ColumnNode) -> Option<&ColumnMetadata> {
        match self.column(node)? {
            ColumnTarget::Column(binding) => self.binding_column(binding),
            ColumnTarget::Merged(_) | ColumnTarget::Output => None,
        }
    }

    /// The slot a `Column`-bound node refers into.
    pub fn column_slot(&self, node: &ColumnNode) -> Option<&TableSlot> {
        match self.column(node)? {
            ColumnTarget::Column(binding) => self.slot(binding.slot),
            ColumnTarget::Merged(_) | ColumnTarget::Output => None,
        }
    }

    pub fn column_by_id(&self, id: NodeId) -> Option<&ColumnTarget> {
        by_node(&self.columns, id)
    }

    /// The slot a `TableNode`, `TableSubqueryNode` or `CteRefNode` was given.
    pub fn source(&self, id: NodeId) -> Option<SlotId> {
        by_node(&self.sources, id).copied()
    }

    pub fn slot(&self, id: SlotId) -> Option<&TableSlot> {
        self.slots.get(id.index())
    }

    pub fn slots(&self) -> &[TableSlot] {
        &self.slots
    }

    pub fn star(&self, node: &StarNode) -> Option<&[StarItem]> {
        by_node(&self.stars, node.id).map(Vec::as_slice)
    }

    pub fn merged(&self, id: MergedId) -> Option<&MergedColumnBinding> {
        self.merged.get(id.index())
    }

    pub fn merged_columns(&self) -> &[MergedColumnBinding] {
        &self.merged
    }

    pub fn is_correlated(&self, node: &ColumnNode) -> bool {
        self.correlated
            .binary_search_by_key(&node.id.index(), |id| id.index())
            .is_ok()
    }

    pub fn correlated(&self) -> &[NodeId] {
        &self.correlated
    }
}

/// Accumulates bindings while the resolver walks the tree; emission order is
/// the resolver's, so [`BindingsBuilder::finish`] sorts by node id.
#[derive(Debug, Default)]
pub(super) struct BindingsBuilder {
    bindings: Bindings,
}

fn narrow(index: usize) -> u32 {
    debug_assert!(u32::try_from(index).is_ok(), "more than u32::MAX bindings");
    u32::try_from(index).unwrap_or(u32::MAX)
}

impl BindingsBuilder {
    pub(super) fn slot_push(&mut self, source: NodeId, slot: TableSlot) -> SlotId {
        let id = SlotId(narrow(self.bindings.slots.len()));
        self.bindings.slots.push(slot);
        self.bindings.sources.push((source, id));
        id
    }

    pub(super) fn column_bind(&mut self, node: NodeId, target: ColumnTarget, correlated: bool) {
        self.bindings.columns.push((node, target));
        if correlated {
            self.bindings.correlated.push(node);
        }
    }

    pub(super) fn star_bind(&mut self, node: NodeId, items: Vec<StarItem>) {
        self.bindings.stars.push((node, items));
    }

    pub(super) fn merged_push(&mut self, merged: MergedColumnBinding) -> MergedId {
        let id = MergedId(narrow(self.bindings.merged.len()));
        self.bindings.merged.push(merged);
        id
    }

    pub(super) fn finish(self) -> Bindings {
        let mut bindings = self.bindings;
        bindings.sources.sort_by_key(|(node, _)| node.index());
        bindings.columns.sort_by_key(|(node, _)| node.index());
        bindings.stars.sort_by_key(|(node, _)| node.index());
        bindings.correlated.sort_by_key(|node| node.index());
        debug_assert!(
            bindings
                .columns
                .iter()
                .zip(bindings.columns.iter().skip(1))
                .all(|(first, second)| first.0.index() < second.0.index())
                && !bindings.columns.iter().any(|(node, _)| node.is_pending())
                && !bindings.sources.iter().any(|(node, _)| node.is_pending())
                && !bindings.stars.iter().any(|(node, _)| node.is_pending()),
            "bindings keyed by a pending or repeated node id"
        );
        bindings
    }
}

/// A finalized tree with its bindings: the only way to hold resolved facts
/// about a plain tree (ADR-057). Cloning shares the tree.
#[derive(Debug, Clone)]
pub struct ResolvedQuery {
    tree: Arc<QueryExpr>,
    bindings: Bindings,
}

impl ResolvedQuery {
    pub fn new(tree: Arc<QueryExpr>, bindings: Bindings) -> Self {
        Self { tree, bindings }
    }

    pub fn tree(&self) -> &QueryExpr {
        &self.tree
    }

    pub fn bindings(&self) -> &Bindings {
        &self.bindings
    }

    pub fn column(&self, node: &ColumnNode) -> Option<&ColumnTarget> {
        self.bindings.column(node)
    }
}
