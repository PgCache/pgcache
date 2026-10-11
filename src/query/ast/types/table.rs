use std::any::Any;
use std::borrow::Cow;
use std::ops::ControlFlow;

use ecow::EcoString;

use super::{
    AstNode, CteMaterialization, QueryExpr, SelectNode, WhereExpr, children_finalize,
    children_visit,
};
use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::query::ast::{Clause, Deparse, DeparseContext, DeparseWith, NodeId, NodePositions};

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableAlias {
    pub name: EcoString,
    pub columns: Vec<EcoString>,
}

impl Deparse for TableAlias {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.name.deparse(buf);
        if !self.columns.is_empty() {
            buf.push('(');
            let mut sep = "";
            for column in self.columns.iter().map(|c| c.as_str()) {
                buf.push_str(sep);
                column.deparse(buf);
                sep = ", ";
            }
            buf.push(')');
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum TableSource {
    Table(TableNode),
    Subquery(TableSubqueryNode),
    Join(JoinNode),
    CteRef(CteRefNode),
}

/// A reference to a CTE in a FROM clause or subquery position.
///
/// Contains a clone of the CTE's query body so that traversal methods
/// (nodes, select_nodes, etc.) are self-contained without needing
/// external context.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CteRefNode {
    pub id: NodeId,
    pub cte_name: EcoString,
    pub query: Box<QueryExpr>,
    pub column_aliases: Vec<EcoString>,
    pub materialization: CteMaterialization,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableSource {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            TableSource::Table(table) => table.try_for_each_node(f)?,
            TableSource::Subquery(sub) => sub.try_for_each_node(f)?,
            TableSource::Join(join) => join.try_for_each_node(f)?,
            TableSource::CteRef(cte_ref) => cte_ref.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        match self {
            TableSource::Table(table) => table.finalize(positions),
            TableSource::Subquery(sub) => sub.finalize(positions),
            TableSource::Join(join) => join.finalize(positions),
            TableSource::CteRef(cte_ref) => cte_ref.finalize(positions),
        }
    }
}

impl TableSource {
    /// Collect direct table nodes, traversing JOINs but not subqueries/CTEs.
    pub(super) fn direct_table_nodes_collect<'a>(&'a self, tables: &mut Vec<&'a TableNode>) {
        match self {
            TableSource::Table(table) => tables.push(table),
            TableSource::Subquery(_) | TableSource::CteRef(_) => {} // handled as separate branch
            TableSource::Join(join) => {
                join.left.direct_table_nodes_collect(tables);
                join.right.direct_table_nodes_collect(tables);
            }
        }
    }

    /// Check if this table source contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        match self {
            TableSource::Subquery(_) | TableSource::CteRef(_) => true,
            TableSource::Table(_) => false,
            TableSource::Join(join) => {
                join.left.has_subqueries()
                    || join.right.has_subqueries()
                    || matches!(&join.qual, JoinQual::On(c) if c.has_subqueries())
            }
        }
    }

    /// Collect subquery branches from table sources with source tracking.
    /// FROM subqueries and CteRef inherit the negation context as Inclusion/Exclusion.
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            TableSource::Table(_) => {}
            TableSource::Subquery(sub) => {
                let kind = match (sub.kind, negated) {
                    (SubqueryKind::Scalar, _) => SubqueryKind::Scalar,
                    (SubqueryKind::Inclusion, true) => SubqueryKind::Exclusion,
                    (SubqueryKind::Exclusion, true) => SubqueryKind::Inclusion,
                    (kind, false) => kind,
                };
                sub.query.select_nodes_collect(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            TableSource::CteRef(cte_ref) => {
                let kind = if negated {
                    SubqueryKind::Exclusion
                } else {
                    SubqueryKind::Inclusion
                };
                cte_ref.query.select_nodes_collect(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            TableSource::Join(join) => {
                join.left.subquery_nodes_collect(branches, negated);
                join.right.subquery_nodes_collect(branches, negated);
                if let JoinQual::On(condition) = &join.qual {
                    condition.subquery_nodes_collect(branches, negated);
                }
            }
        }
    }
}

impl DeparseWith for TableSource {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        match self {
            TableSource::Table(table) => table.deparse_with(cx, buf),
            TableSource::Subquery(subquery) => subquery.deparse_with(cx, buf),
            TableSource::Join(join) => join.deparse_with(cx, buf),
            TableSource::CteRef(cte_ref) => cte_ref.deparse_with(cx, buf),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableNode {
    pub id: NodeId,
    pub schema: Option<EcoString>,
    pub name: EcoString,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableNode {
    fn id(&self) -> NodeId {
        self.id
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }
}

impl TableNode {
    /// `schema.name` as written.
    pub(crate) fn name_deparse_plain(&self, buf: &mut String) {
        if let Some(schema) = &self.schema {
            schema.deparse(buf);
            buf.push('.');
        }
        self.name.deparse(buf);
    }
}

impl DeparseWith for TableNode {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        buf.push(' ');
        cx.table_name_deparse(self, buf);
        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableSubqueryNode {
    pub id: NodeId,
    pub lateral: bool,
    pub query: Box<QueryExpr>,
    pub alias: Option<TableAlias>,
    /// What the derived table stands for in CDC terms: a FROM subquery as
    /// written is `Inclusion`; decorrelation marks the derived table it
    /// makes from a scalar subquery `Scalar`, so any change to it still
    /// invalidates.
    pub kind: SubqueryKind,
}

impl AstNode for TableSubqueryNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        self.query.finalize(positions);
    }
}

impl DeparseWith for TableSubqueryNode {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        buf.push(' ');
        if self.lateral {
            buf.push_str("LATERAL ");
        }

        buf.push('(');
        self.query.deparse_with(cx, buf);
        buf.push(')');

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }
    }
}

impl AstNode for CteRefNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        self.query.finalize(positions);
    }
}

impl CteRefNode {
    /// ` cte_name alias` as written.
    pub(crate) fn deparse_plain(&self, buf: &mut String) {
        buf.push(' ');
        self.cte_name.deparse(buf);

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }
    }
}

impl DeparseWith for CteRefNode {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        cx.cte_ref_deparse(self, buf);
    }
}

/// A join's qualifier. `ON` / `USING` / `NATURAL` / cross are mutually
/// exclusive in SQL, so they are one enum rather than independent
/// fields that could encode impossible states. The resolver expands
/// `Using`/`Natural` into an equivalent equi-join condition; `Cross`
/// (and a no-common-column `Natural`) is a cartesian product.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum JoinQual {
    /// `JOIN … ON <expr>`
    On(WhereExpr),
    /// `JOIN … USING (c, …)`
    Using(Vec<EcoString>),
    /// `NATURAL JOIN …`
    Natural,
    /// `CROSS JOIN …` / unqualified inner join — cartesian product.
    Cross,
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct JoinNode {
    pub id: NodeId,
    pub join_type: JoinType,
    pub left: Box<TableSource>,
    pub right: Box<TableSource>,
    pub qual: JoinQual,
}

impl AstNode for JoinNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&*self.left, &*self.right], f)?;
        if let JoinQual::On(c) = &self.qual {
            c.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }

    fn id(&self) -> NodeId {
        self.id
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        children_finalize([&mut *self.left, &mut *self.right], positions);
        if let JoinQual::On(c) = &mut self.qual {
            c.finalize(positions);
        }
    }
}

impl DeparseWith for JoinNode {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        self.left.deparse_with(cx, buf);
        let qual: Cow<'_, JoinQual> = match &self.qual {
            JoinQual::Natural => match cx.natural_join_columns(self) {
                Some(columns) if columns.is_empty() => Cow::Owned(JoinQual::Cross),
                Some(columns) => Cow::Owned(JoinQual::Using(columns)),
                None => Cow::Borrowed(&self.qual),
            },
            JoinQual::On(_) | JoinQual::Using(_) | JoinQual::Cross => Cow::Borrowed(&self.qual),
        };
        match &*qual {
            // CROSS JOIN is always inner; the keyword carries the semantics.
            JoinQual::Cross => buf.push_str(" CROSS JOIN"),
            JoinQual::Natural => {
                buf.push_str(" NATURAL");
                buf.push_str(self.join_type.join_keyword());
            }
            JoinQual::On(_) | JoinQual::Using(_) => {
                buf.push_str(self.join_type.join_keyword());
            }
        }
        self.right.deparse_with(cx, buf);
        match &*qual {
            JoinQual::On(condition) => {
                cx.clause_enter(Clause::JoinOn);
                buf.push_str(" ON ");
                condition.deparse_with(cx, buf);
                cx.clause_leave(Clause::JoinOn);
            }
            JoinQual::Using(cols) => {
                buf.push_str(" USING (");
                for (i, c) in cols.iter().enumerate() {
                    if i > 0 {
                        buf.push_str(", ");
                    }
                    buf.push_str(c);
                }
                buf.push(')');
            }
            JoinQual::Natural | JoinQual::Cross => {}
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum JoinType {
    Inner,
    Left,
    Right,
    Full,
}

impl JoinType {
    /// The SQL keyword for this join type, with a leading space, for
    /// deparse (e.g. `" LEFT JOIN"`). A `NATURAL`/`CROSS` qualifier is
    /// rendered by the caller around this.
    pub(crate) fn join_keyword(self) -> &'static str {
        match self {
            JoinType::Inner => " JOIN",
            JoinType::Left => " LEFT JOIN",
            JoinType::Right => " RIGHT JOIN",
            JoinType::Full => " FULL JOIN",
        }
    }
}
