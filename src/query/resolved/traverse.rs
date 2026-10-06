//! [`AstNode`] for the resolved AST: a uniform pre-order walk over every
//! descendant node.
//!
//! Uniform is the point, and also the limit. Several resolved analyses
//! deliberately do *not* descend uniformly, and routing them through
//! `nodes::<N>()` would silently change their answers — a FROM-clause derived
//! table opens a new name scope ([`ResolvedSelectNode::direct_table_nodes`]),
//! and an aggregate or window nested inside a scalar subquery does not make the
//! *outer* expression aggregating or windowed ([`ResolvedScalarExpr::has_aggregate`],
//! `cache::mv_shape::shape_classify`). Each such site says so at its definition; treat
//! that as load-bearing, not as duplication waiting to be collapsed.

use std::any::Any;
use std::ops::ControlFlow;

use super::{
    ResolvedArithmeticExpr, ResolvedBinaryExpr, ResolvedCaseExpr, ResolvedCaseWhen,
    ResolvedColumnNode, ResolvedFrameBound, ResolvedFunctionCall, ResolvedJoinNode,
    ResolvedMultiExpr, ResolvedOrderByClause, ResolvedQueryBody, ResolvedQueryExpr,
    ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns, ResolvedSelectNode,
    ResolvedSetOpNode, ResolvedTableNode, ResolvedTableSource, ResolvedTableSubqueryNode,
    ResolvedUnaryExpr, ResolvedWhereExpr, ResolvedWindowFrame, ResolvedWindowSpec,
};
use crate::query::ast::{AstNode, children_visit};

impl AstNode for ResolvedTableNode {}

impl AstNode for ResolvedColumnNode {}

impl AstNode for ResolvedUnaryExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedBinaryExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&*self.lexpr, &*self.rexpr], f)
    }
}

impl AstNode for ResolvedMultiExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit(&self.exprs, f)
    }
}

impl AstNode for ResolvedWhereExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => scalar.try_for_each_node(f)?,
            ResolvedWhereExpr::Unary(unary) => unary.try_for_each_node(f)?,
            ResolvedWhereExpr::Binary(binary) => binary.try_for_each_node(f)?,
            ResolvedWhereExpr::Multi(multi) => multi.try_for_each_node(f)?,
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                query.try_for_each_node(f)?;
                if let Some(e) = test_expr {
                    e.try_for_each_node(f)?;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedArithmeticExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&*self.left, &*self.right], f)
    }
}

impl AstNode for ResolvedFunctionCall {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit(&self.args, f)?;
        children_visit(&self.agg_order, f)?;
        children_visit(self.agg_filter.as_deref(), f)?;
        children_visit(self.over.as_ref(), f)
    }
}

impl AstNode for ResolvedWindowSpec {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit(&self.partition_by, f)?;
        children_visit(&self.order_by, f)?;
        children_visit(self.frame.as_ref(), f)
    }
}

impl AstNode for ResolvedWindowFrame {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&self.start, &self.end], f)
    }
}

impl AstNode for ResolvedFrameBound {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            ResolvedFrameBound::OffsetPreceding(e) | ResolvedFrameBound::OffsetFollowing(e) => {
                e.try_for_each_node(f)?;
            }
            ResolvedFrameBound::UnboundedPreceding
            | ResolvedFrameBound::CurrentRow
            | ResolvedFrameBound::UnboundedFollowing => {}
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedScalarExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            ResolvedScalarExpr::Column(col) => col.try_for_each_node(f)?,
            ResolvedScalarExpr::Identifier(_) => {}
            ResolvedScalarExpr::Literal(lit) => lit.try_for_each_node(f)?,
            ResolvedScalarExpr::Function(func) => func.try_for_each_node(f)?,
            ResolvedScalarExpr::Case(case) => case.try_for_each_node(f)?,
            ResolvedScalarExpr::Arithmetic(arith) => arith.try_for_each_node(f)?,
            ResolvedScalarExpr::Subquery(query, _) => query.try_for_each_node(f)?,
            ResolvedScalarExpr::Array(elems) => {
                children_visit(elems, f)?;
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => expr.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedCaseExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit(self.arg.as_deref(), f)?;
        children_visit(&self.whens, f)?;
        children_visit(self.default.as_deref(), f)
    }
}

impl AstNode for ResolvedCaseWhen {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.condition.try_for_each_node(f)?;
        self.result.try_for_each_node(f)
    }
}

impl AstNode for ResolvedSelectColumn {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedSelectColumns {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            ResolvedSelectColumns::None => {}
            ResolvedSelectColumns::Columns(cols) => {
                children_visit(cols, f)?;
            }
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedTableSource {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            ResolvedTableSource::Table(table) => table.try_for_each_node(f)?,
            ResolvedTableSource::Subquery(subquery) => subquery.try_for_each_node(f)?,
            ResolvedTableSource::Join(join) => join.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedTableSubqueryNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.query.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedJoinNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&self.left, &self.right], f)?;
        if let Some(c) = self.predicate() {
            c.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedOrderByClause {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedSelectNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.columns.try_for_each_node(f)?;
        children_visit(&self.from, f)?;
        children_visit(self.where_clause.as_ref(), f)?;
        children_visit(&self.group_by, f)?;
        children_visit(self.having.as_ref(), f)
    }
}

impl AstNode for ResolvedSetOpNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&*self.left, &*self.right], f)
    }
}

impl AstNode for ResolvedQueryBody {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            ResolvedQueryBody::Select(select) => select.try_for_each_node(f)?,
            ResolvedQueryBody::Values(values) => values.try_for_each_node(f)?,
            ResolvedQueryBody::SetOp(set_op) => set_op.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedQueryExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.body.try_for_each_node(f)?;
        children_visit(&self.order_by, f)
    }
}
