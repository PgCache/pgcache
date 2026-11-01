//! Views on a CREATE INDEX statement.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use super::{
    CreateIndex, EMPTY_NAME, IndexKey, IndexOption, Literal, Name, NodeRef, Query, Run, Scalar,
    Table, View, Where, mismatch,
};
use crate::query::ast::rope::node::NodeKind;
use crate::query::ast::{NullOrder, OrderDirection};

/// A tree's root by statement kind.
#[derive(Clone, Copy)]
pub enum Statement<'t> {
    Query(Query<'t>),
    CreateIndex(CreateIndex<'t>),
}

impl<'t> View<'t> for Statement<'t> {
    fn from_ref(node: NodeRef<'t>) -> Self {
        match node.kind() {
            NodeKind::CreateIndex { .. } => Statement::CreateIndex(CreateIndex(node)),
            NodeKind::Query { .. } => Statement::Query(Query(node)),
            _ => {
                debug_assert!(false, "a root is a query or a CREATE INDEX");
                Statement::Query(Query(node))
            }
        }
    }

    fn node_ref(&self) -> NodeRef<'t> {
        match self {
            Statement::Query(query) => query.0,
            Statement::CreateIndex(index) => index.0,
        }
    }
}

#[derive(Default, Clone, Copy)]
struct CreateIndexLayout<'t> {
    name: Option<&'t EcoString>,
    method: Option<&'t EcoString>,
    unique: bool,
    only: bool,
    nulls_not_distinct: bool,
    key_count: u32,
    include_count: u32,
    option_count: u32,
    has_where: bool,
}

impl<'t> CreateIndex<'t> {
    fn layout(&self) -> CreateIndexLayout<'t> {
        match self.0.kind() {
            NodeKind::CreateIndex {
                name,
                method,
                unique,
                only,
                nulls_not_distinct,
                key_count,
                include_count,
                option_count,
                has_where,
            } => CreateIndexLayout {
                name: name.as_ref(),
                method: Some(method),
                unique: *unique,
                only: *only,
                nulls_not_distinct: *nulls_not_distinct,
                key_count: *key_count,
                include_count: *include_count,
                option_count: *option_count,
                has_where: *has_where,
            },
            _ => mismatch("create index"),
        }
    }

    pub fn name(&self) -> Option<&'t EcoString> {
        self.layout().name
    }

    pub fn method(&self) -> &'t EcoString {
        self.layout().method.unwrap_or(&EMPTY_NAME)
    }

    pub fn unique(&self) -> bool {
        self.layout().unique
    }

    /// `ON ONLY`: the index of a partitioned table's parent alone.
    pub fn only(&self) -> bool {
        self.layout().only
    }

    pub fn nulls_not_distinct(&self) -> bool {
        self.layout().nulls_not_distinct
    }

    pub fn table(&self) -> Table<'t> {
        self.0.nth(0)
    }

    pub fn keys(&self) -> Run<'t, IndexKey<'t>> {
        self.0.run(1, self.layout().key_count)
    }

    pub fn includes(&self) -> Run<'t, Name<'t>> {
        let layout = self.layout();
        self.0.run(1 + layout.key_count, layout.include_count)
    }

    pub fn options(&self) -> Run<'t, IndexOption<'t>> {
        let layout = self.layout();
        self.0.run(
            1 + layout.key_count + layout.include_count,
            layout.option_count,
        )
    }

    pub fn predicate(&self) -> Option<Where<'t>> {
        let layout = self.layout();
        layout.has_where.then(|| {
            self.0
                .nth(1 + layout.key_count + layout.include_count + layout.option_count)
        })
    }
}

#[derive(Default, Clone)]
struct IndexKeyLayout {
    direction: Option<OrderDirection>,
    null_order: Option<NullOrder>,
    collation_count: u32,
    opclass_count: u32,
    opclass_option_count: u32,
}

impl<'t> IndexKey<'t> {
    fn layout(&self) -> IndexKeyLayout {
        match self.0.kind() {
            NodeKind::IndexKey {
                direction,
                null_order,
                collation_count,
                opclass_count,
                opclass_option_count,
            } => IndexKeyLayout {
                direction: Some(direction.clone()),
                null_order: Some(*null_order),
                collation_count: *collation_count,
                opclass_count: *opclass_count,
                opclass_option_count: *opclass_option_count,
            },
            _ => mismatch("index key"),
        }
    }

    pub fn expr(&self) -> Scalar<'t> {
        self.0.nth(0)
    }

    pub fn direction(&self) -> OrderDirection {
        self.layout().direction.unwrap_or(OrderDirection::Asc)
    }

    pub fn null_order(&self) -> NullOrder {
        self.layout().null_order.unwrap_or(NullOrder::Default)
    }

    pub fn collation(&self) -> Run<'t, Name<'t>> {
        self.0.run(1, self.layout().collation_count)
    }

    pub fn opclass(&self) -> Run<'t, Name<'t>> {
        let layout = self.layout();
        self.0.run(1 + layout.collation_count, layout.opclass_count)
    }

    pub fn opclass_options(&self) -> Run<'t, IndexOption<'t>> {
        let layout = self.layout();
        self.0.run(
            1 + layout.collation_count + layout.opclass_count,
            layout.opclass_option_count,
        )
    }
}

impl<'t> IndexOption<'t> {
    fn layout(&self) -> (Option<&'t EcoString>, bool) {
        match self.0.kind() {
            NodeKind::IndexOption { name, has_value } => (Some(name), *has_value),
            _ => mismatch("index option"),
        }
    }

    pub fn name(&self) -> &'t EcoString {
        self.layout().0.unwrap_or(&EMPTY_NAME)
    }

    pub fn value(&self) -> Option<Literal<'t>> {
        self.layout().1.then(|| self.0.nth(0))
    }
}
