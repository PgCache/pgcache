use std::any::Any;
use std::ops::ControlFlow;

use ecow::EcoString;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::query::ast::Deparse;

use super::*;

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
    pub cte_name: EcoString,
    pub query: Box<QueryExpr>,
    pub column_aliases: Vec<EcoString>,
    pub materialization: CteMaterialization,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableSource {
    fn try_for_each_node<'a, N: Any, B>(
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
                let kind = if negated {
                    SubqueryKind::Exclusion
                } else {
                    SubqueryKind::Inclusion
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

impl Deparse for TableSource {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            TableSource::Table(table) => table.deparse(buf),
            TableSource::Subquery(subquery) => subquery.deparse(buf),
            TableSource::Join(join) => join.deparse(buf),
            TableSource::CteRef(cte_ref) => cte_ref.deparse(buf),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableNode {
    pub schema: Option<EcoString>,
    pub name: EcoString,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for TableNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if let Some(schema) = &self.schema {
            schema.deparse(buf);
            buf.push('.');
        }

        self.name.deparse(buf);

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct TableSubqueryNode {
    pub lateral: bool,
    pub query: Box<QueryExpr>,
    pub alias: Option<TableAlias>,
}

impl AstNode for TableSubqueryNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for TableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        if self.lateral {
            buf.push_str("LATERAL ");
        }

        buf.push('(');
        self.query.deparse(buf);
        buf.push(')');

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
    }
}

impl AstNode for CteRefNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for CteRefNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.cte_name.deparse(buf);

        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }

        buf
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
    pub join_type: JoinType,
    pub left: Box<TableSource>,
    pub right: Box<TableSource>,
    pub qual: JoinQual,
}

impl AstNode for JoinNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        // Self-visit like every other node type, so `nodes::<JoinNode>()` finds
        // a join called on directly (not only joins nested under a TableSource).
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        if let JoinQual::On(c) = &self.qual {
            c.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for JoinNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        match &self.qual {
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
        self.right.deparse(buf);
        match &self.qual {
            JoinQual::On(condition) => {
                buf.push_str(" ON ");
                condition.deparse(buf);
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
        buf
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
