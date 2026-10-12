//! Views on FROM entries.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use super::{
    CteRef, EMPTY_NAME, Join, Name, NodeRef, Query, Run, Subquery, Table, View, Where, mismatch,
};
use crate::cache::SubqueryKind;
use crate::query::ast::rope::node::{JoinQualKind, NodeKind};
use crate::query::ast::{CteMaterialization, JoinType};

// --- FROM sources ----------------------------------------------------------

/// One FROM entry.
#[derive(Clone, Copy)]
pub enum Source<'t> {
    Table(Table<'t>),
    Subquery(Subquery<'t>),
    Join(Join<'t>),
    CteRef(CteRef<'t>),
}

impl<'t> View<'t> for Source<'t> {
    fn from_ref(node: NodeRef<'t>) -> Self {
        match node.kind() {
            NodeKind::Subquery { .. } => Source::Subquery(Subquery(node)),
            NodeKind::Join { .. } => Source::Join(Join(node)),
            NodeKind::CteRef { .. } => Source::CteRef(CteRef(node)),
            NodeKind::Table { .. } => Source::Table(Table(node)),
            _ => {
                debug_assert!(
                    false,
                    "a FROM entry is a table, subquery, join or CTE reference"
                );
                Source::Table(Table(node))
            }
        }
    }

    fn node_ref(&self) -> NodeRef<'t> {
        match self {
            Source::Table(table) => table.0,
            Source::Subquery(subquery) => subquery.0,
            Source::Join(join) => join.0,
            Source::CteRef(cte_ref) => cte_ref.0,
        }
    }
}

#[derive(Default, Clone, Copy)]
struct TableLayout<'t> {
    schema: Option<&'t EcoString>,
    name: Option<&'t EcoString>,
    alias: Option<&'t EcoString>,
    alias_column_count: u32,
}

impl<'t> Table<'t> {
    fn layout(&self) -> TableLayout<'t> {
        match self.0.kind() {
            NodeKind::Table {
                schema,
                name,
                alias,
                alias_column_count,
            } => TableLayout {
                schema: (!schema.is_empty()).then_some(schema),
                name: Some(name),
                alias: (!alias.is_empty()).then_some(alias),
                alias_column_count: *alias_column_count,
            },
            _ => mismatch("table"),
        }
    }

    pub fn schema(&self) -> Option<&'t EcoString> {
        self.layout().schema
    }

    pub fn name(&self) -> &'t EcoString {
        self.layout().name.unwrap_or(&EMPTY_NAME)
    }

    pub fn alias(&self) -> Option<&'t EcoString> {
        self.layout().alias
    }

    pub fn alias_columns(&self) -> Run<'t, Name<'t>> {
        self.0.run(0, self.layout().alias_column_count)
    }
}

#[derive(Default, Clone, Copy)]
struct SubqueryLayout<'t> {
    lateral: bool,
    alias: Option<&'t EcoString>,
    alias_column_count: u32,
    kind: Option<SubqueryKind>,
}

impl<'t> Subquery<'t> {
    fn layout(&self) -> SubqueryLayout<'t> {
        match self.0.kind() {
            NodeKind::Subquery {
                lateral,
                alias,
                alias_column_count,
                kind,
            } => SubqueryLayout {
                lateral: *lateral,
                alias: alias.as_ref(),
                alias_column_count: *alias_column_count,
                kind: Some(*kind),
            },
            _ => mismatch("subquery"),
        }
    }

    pub fn lateral(&self) -> bool {
        self.layout().lateral
    }

    pub fn alias(&self) -> Option<&'t EcoString> {
        self.layout().alias
    }

    pub fn alias_columns(&self) -> Run<'t, Name<'t>> {
        self.0.run(0, self.layout().alias_column_count)
    }

    pub fn kind(&self) -> SubqueryKind {
        self.layout().kind.unwrap_or(SubqueryKind::Inclusion)
    }

    pub fn query(&self) -> Query<'t> {
        self.0.nth(self.layout().alias_column_count)
    }
}

#[derive(Default, Clone, Copy)]
struct CteRefLayout<'t> {
    cte_name: Option<&'t EcoString>,
    column_alias_count: u32,
    materialization: Option<CteMaterialization>,
    alias: Option<&'t EcoString>,
    alias_column_count: u32,
}

impl<'t> CteRef<'t> {
    fn layout(&self) -> CteRefLayout<'t> {
        match self.0.kind() {
            NodeKind::CteRef {
                cte_name,
                column_alias_count,
                materialization,
                alias,
                alias_column_count,
            } => CteRefLayout {
                cte_name: Some(cte_name),
                column_alias_count: *column_alias_count,
                materialization: Some(*materialization),
                alias: alias.as_ref(),
                alias_column_count: *alias_column_count,
            },
            _ => mismatch("cte reference"),
        }
    }

    pub fn cte_name(&self) -> &'t EcoString {
        self.layout().cte_name.unwrap_or(&EMPTY_NAME)
    }

    /// The CTE's own column aliases.
    pub fn column_aliases(&self) -> Run<'t, Name<'t>> {
        self.0.run(0, self.layout().column_alias_count)
    }

    pub fn materialization(&self) -> CteMaterialization {
        self.layout()
            .materialization
            .unwrap_or(CteMaterialization::Default)
    }

    pub fn alias(&self) -> Option<&'t EcoString> {
        self.layout().alias
    }

    pub fn alias_columns(&self) -> Run<'t, Name<'t>> {
        let layout = self.layout();
        self.0
            .run(layout.column_alias_count, layout.alias_column_count)
    }

    pub fn query(&self) -> Query<'t> {
        let layout = self.layout();
        self.0
            .nth(layout.column_alias_count + layout.alias_column_count)
    }
}

/// A join's qualification.
#[derive(Clone, Copy)]
pub enum JoinQual<'t> {
    On(Where<'t>),
    Using(Run<'t, Name<'t>>),
    Natural,
    Cross,
}

impl<'t> Join<'t> {
    fn layout(&self) -> (JoinType, JoinQualKind) {
        match *self.0.kind() {
            NodeKind::Join { join_type, qual } => (join_type, qual),
            _ => mismatch::<(Option<JoinType>, Option<JoinQualKind>)>("join")
                .0
                .map_or((JoinType::Inner, JoinQualKind::Cross), |t| {
                    (t, JoinQualKind::Cross)
                }),
        }
    }

    pub fn join_type(&self) -> JoinType {
        self.layout().0
    }

    pub fn left(&self) -> Source<'t> {
        self.0.nth(0)
    }

    pub fn right(&self) -> Source<'t> {
        self.0.nth(1)
    }

    pub fn qual(&self) -> JoinQual<'t> {
        match self.layout().1 {
            JoinQualKind::On => JoinQual::On(self.0.nth(2)),
            JoinQualKind::Using(count) => JoinQual::Using(self.0.run(2, count)),
            JoinQualKind::Natural => JoinQual::Natural,
            JoinQualKind::Cross => JoinQual::Cross,
        }
    }
}
