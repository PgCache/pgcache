use std::any::Any;
use std::ops::ControlFlow;

use crate::query::ast::AstNode;

use super::*;

impl AstNode for ResolvedTableNode {
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

impl AstNode for ResolvedColumnNode {
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

impl AstNode for ResolvedUnaryExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedBinaryExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.lexpr.try_for_each_node(f)?;
        self.rexpr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedMultiExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for expr in &self.exprs {
            expr.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedWhereExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
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
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedFunctionCall {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for arg in &self.args {
            arg.try_for_each_node(f)?;
        }
        for o in &self.agg_order {
            o.try_for_each_node(f)?;
        }
        if let Some(filter) = &self.agg_filter {
            filter.try_for_each_node(f)?;
        }
        if let Some(w) = &self.over {
            w.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedWindowSpec {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for p in &self.partition_by {
            p.try_for_each_node(f)?;
        }
        for o in &self.order_by {
            o.try_for_each_node(f)?;
        }
        if let Some(frame) = &self.frame {
            frame.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedWindowFrame {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.start.try_for_each_node(f)?;
        self.end.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedFrameBound {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
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
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            ResolvedScalarExpr::Column(col) => col.try_for_each_node(f)?,
            ResolvedScalarExpr::Identifier(_) => {}
            ResolvedScalarExpr::Literal(lit) => lit.try_for_each_node(f)?,
            ResolvedScalarExpr::Function(func) => func.try_for_each_node(f)?,
            ResolvedScalarExpr::Case(case) => case.try_for_each_node(f)?,
            ResolvedScalarExpr::Arithmetic(arith) => arith.try_for_each_node(f)?,
            ResolvedScalarExpr::Subquery(query, _) => query.try_for_each_node(f)?,
            ResolvedScalarExpr::Array(elems) => {
                for e in elems {
                    e.try_for_each_node(f)?;
                }
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => expr.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedCaseExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        if let Some(a) = &self.arg {
            a.try_for_each_node(f)?;
        }
        for w in &self.whens {
            w.try_for_each_node(f)?;
        }
        if let Some(d) = &self.default {
            d.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedCaseWhen {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.condition.try_for_each_node(f)?;
        self.result.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedSelectColumn {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedSelectColumns {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            ResolvedSelectColumns::None => {}
            ResolvedSelectColumns::Columns(cols) => {
                for col in cols {
                    col.try_for_each_node(f)?;
                }
            }
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedTableSource {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            ResolvedTableSource::Table(table) => table.try_for_each_node(f)?,
            ResolvedTableSource::Subquery(subquery) => subquery.try_for_each_node(f)?,
            ResolvedTableSource::Join(join) => join.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedTableSubqueryNode {
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

impl AstNode for ResolvedJoinNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        if let Some(c) = self.predicate() {
            c.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedOrderByClause {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedSelectNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.columns.try_for_each_node(f)?;
        for t in &self.from {
            t.try_for_each_node(f)?;
        }
        if let Some(w) = &self.where_clause {
            w.try_for_each_node(f)?;
        }
        for c in &self.group_by {
            c.try_for_each_node(f)?;
        }
        if let Some(h) = &self.having {
            h.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedSetOpNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedQueryBody {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            ResolvedQueryBody::Select(select) => select.try_for_each_node(f)?,
            ResolvedQueryBody::Values(values) => values.try_for_each_node(f)?,
            ResolvedQueryBody::SetOp(set_op) => set_op.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }
}

impl AstNode for ResolvedQueryExpr {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.body.try_for_each_node(f)?;
        for o in &self.order_by {
            o.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}
