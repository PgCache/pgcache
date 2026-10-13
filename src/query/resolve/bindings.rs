//! What resolution adds to a finalized tree (ADR-057): a side table keyed by
//! node id, which with the shared tree is the bound query.

use std::sync::Arc;

use ecow::EcoString;

use super::ResolveResult;
use super::rope::query_tree_resolve;
use crate::catalog::Catalog;
use crate::catalog::{ColumnMetadata, ColumnPosition, TableMetadata};
use crate::query::ast::NodeId;
#[cfg(test)]
use crate::query::ast::query_tree_parse;
use crate::query::ast::rope::{QueryTree, Remap};

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
    /// The catalog column a binding names.
    pub fn binding_column(&self, binding: &ColumnBinding) -> Option<&ColumnMetadata> {
        self.slot(binding.slot)?
            .table()
            .columns
            .by_position(binding.position)
    }

    /// The catalog column a `Column`-bound node names; `None` for a merged
    /// or output reference.
    /// The slot a `Column`-bound node refers into.
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

    pub fn star_by_id(&self, id: NodeId) -> Option<&[StarItem]> {
        by_node(&self.stars, id).map(Vec::as_slice)
    }

    pub fn merged(&self, id: MergedId) -> Option<&MergedColumnBinding> {
        self.merged.get(id.index())
    }

    pub fn merged_columns(&self) -> &[MergedColumnBinding] {
        &self.merged
    }

    pub fn correlated(&self) -> &[NodeId] {
        &self.correlated
    }
}

/// Accumulates bindings while the resolver walks the tree, or while a
/// rewrite binds what it added to a remapped copy; emission order is the
/// caller's, so [`BindingsBuilder::finish`] sorts by node id.
#[derive(Debug, Default)]
pub(crate) struct BindingsBuilder {
    bindings: Bindings,
}

fn narrow(index: usize) -> u32 {
    debug_assert!(u32::try_from(index).is_ok(), "more than u32::MAX bindings");
    u32::try_from(index).unwrap_or(u32::MAX)
}

impl BindingsBuilder {
    /// The bindings of a tree a rewrite built from pieces of `source`'s,
    /// carried to the pieces' new positions (ADR-057): a node kept in
    /// several places binds in each, a node the rewrite dropped binds
    /// nowhere. Slots are kept as they are, so what the rewrite added binds
    /// into them by slot and position.
    pub(crate) fn from_remapped(source: &Bindings, remap: &Remap) -> Self {
        // A derived slot names the node it stands for; the slot keeps its
        // index and follows the node.
        let slots = source
            .slots
            .iter()
            .map(|slot| match slot {
                TableSlot::Derived { node, table } => TableSlot::Derived {
                    node: remap.new_id(*node).unwrap_or(*node),
                    table: Arc::clone(table),
                },
                TableSlot::Base { .. } => slot.clone(),
            })
            .collect();
        let mut bindings = Bindings {
            slots,
            ..Bindings::default()
        };
        for (node, slot) in &source.sources {
            bindings
                .sources
                .extend(remap.new_ids(*node).map(|new| (new, *slot)));
        }
        for (node, target) in &source.columns {
            bindings
                .columns
                .extend(remap.new_ids(*node).map(|new| (new, target.clone())));
        }
        for (node, items) in &source.stars {
            bindings
                .stars
                .extend(remap.new_ids(*node).map(|new| (new, items.clone())));
        }
        // A merged column keeps its index, which column and star bindings
        // name; one whose join the rewrite dropped is simply never reached.
        bindings.merged = source
            .merged
            .iter()
            .map(|merged| MergedColumnBinding {
                join: remap.new_id(merged.join).unwrap_or(merged.join),
                ..merged.clone()
            })
            .collect();
        for node in &source.correlated {
            bindings.correlated.extend(remap.new_ids(*node));
        }
        Self { bindings }
    }

    /// A column reference a rewrite moved into the scope it was correlated
    /// with is no longer correlated.
    pub(crate) fn correlated_remove(&mut self, node: NodeId) {
        self.bindings
            .correlated
            .retain(|candidate| candidate.index() != node.index());
    }

    /// The catalog column a binding names; slots are complete as soon as
    /// they are pushed, so this reads them mid-build.
    pub(super) fn binding_column(&self, binding: &ColumnBinding) -> Option<&ColumnMetadata> {
        self.bindings.binding_column(binding)
    }

    pub(crate) fn slot_push(&mut self, source: NodeId, slot: TableSlot) -> SlotId {
        let id = SlotId(narrow(self.bindings.slots.len()));
        self.bindings.slots.push(slot);
        self.bindings.sources.push((source, id));
        id
    }

    pub(crate) fn column_bind(&mut self, node: NodeId, target: ColumnTarget, correlated: bool) {
        self.bindings.columns.push((node, target));
        if correlated {
            self.bindings.correlated.push(node);
        }
    }

    pub(crate) fn star_bind(&mut self, node: NodeId, items: Vec<StarItem>) {
        self.bindings.stars.push((node, items));
    }

    pub(crate) fn merged_push(&mut self, merged: MergedColumnBinding) -> MergedId {
        let id = MergedId(narrow(self.bindings.merged.len()));
        self.bindings.merged.push(merged);
        id
    }

    pub(crate) fn finish(self) -> Bindings {
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

#[cfg(test)]
impl Bindings {
    /// One line per binding, naming slots by content rather than index, so
    /// two resolutions of one tree compare equal whatever order their
    /// slots were made in.
    pub(crate) fn describe(&self) -> Vec<String> {
        self.describe_with(|id| id.index().to_string())
    }

    /// [`Bindings::describe`] with node ids rendered by `id_text`, so the
    /// bindings of one query in two id spaces compare through a mapping.
    pub(crate) fn describe_with(&self, id_text: impl Fn(NodeId) -> String) -> Vec<String> {
        let slot_text = |id: SlotId| match self.slot(id) {
            Some(TableSlot::Base { table, alias }) => {
                let alias = alias
                    .as_ref()
                    .map(|a| format!(" AS {a}"))
                    .unwrap_or_default();
                format!("{}.{}{alias}", table.schema, table.name)
            }
            Some(TableSlot::Derived { table, .. }) => {
                let columns: Vec<String> = table
                    .columns
                    .iter()
                    .map(|c| format!("{}:{}", c.name, c.type_name))
                    .collect();
                format!("derived {}({})", table.name, columns.join(", "))
            }
            None => "?".to_owned(),
        };
        let binding_text =
            |b: &ColumnBinding| format!("{}#{}", slot_text(b.slot), b.position.index());
        let merged_text = |id: MergedId| match self.merged(id) {
            Some(m) => format!(
                "merged {} join {} {} = {} outer={}",
                m.name,
                id_text(m.join),
                binding_text(&m.left),
                binding_text(&m.right),
                m.outer
            ),
            None => "merged ?".to_owned(),
        };
        let mut lines = Vec::new();
        for (node, slot) in &self.sources {
            lines.push(format!("source {} -> {}", id_text(*node), slot_text(*slot)));
        }
        for (node, target) in &self.columns {
            let target = match target {
                ColumnTarget::Column(b) => binding_text(b),
                ColumnTarget::Merged(id) => merged_text(*id),
                ColumnTarget::Output => "output".to_owned(),
            };
            lines.push(format!("column {} -> {target}", id_text(*node)));
        }
        for (node, items) in &self.stars {
            let items: Vec<String> = items
                .iter()
                .map(|item| match item {
                    StarItem::Column(b) => binding_text(b),
                    StarItem::Merged(id) => merged_text(*id),
                })
                .collect();
            lines.push(format!("star {} -> [{}]", id_text(*node), items.join(", ")));
        }
        for node in &self.correlated {
            lines.push(format!("correlated {}", id_text(*node)));
        }
        lines
    }
}

/// A query tree with its bindings: the only way to hold resolved facts
/// about a tree (ADR-057). Cloning shares the tree.
#[derive(Debug, Clone)]
pub struct ResolvedQuery {
    tree: QueryTree,
    bindings: Bindings,
}

impl ResolvedQuery {
    pub fn new(tree: QueryTree, bindings: Bindings) -> Self {
        Self { tree, bindings }
    }

    /// `tree` resolved against `tables` on `search_path`.
    pub fn resolve(tree: QueryTree, tables: &Catalog, search_path: &[&str]) -> ResolveResult<Self> {
        let bindings = query_tree_resolve(&tree, tables, search_path)?;
        Ok(Self { tree, bindings })
    }

    /// `sql` parsed and resolved against `tables` on `search_path`.
    #[cfg(test)]
    pub(crate) fn parse_resolve(
        sql: &str,
        tables: &Catalog,
        search_path: &[&str],
    ) -> ResolveResult<Self> {
        let tree = query_tree_parse(sql).unwrap_or_else(|e| panic!("{sql} parses: {e:?}"));
        Self::resolve(tree, tables, search_path)
    }

    pub fn tree(&self) -> &QueryTree {
        &self.tree
    }

    pub fn bindings(&self) -> &Bindings {
        &self.bindings
    }
}
