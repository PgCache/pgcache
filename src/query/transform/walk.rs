//! Shared mutable AST walker for `QueryExpr` / `SelectNode`.
//!
//! Both `constant_fold` and `parameters/replace` need to recurse the same
//! tree shape, differing only in what they do at the leaves. The
//! [`QueryWalkerMut`] trait abstracts the leaf operation; the free
//! functions below carry the shared recursion.
//!
//! The trait has two hook points:
//!
//! * [`visit_scalar_post`](QueryWalkerMut::visit_scalar_post) — called
//!   after a `ScalarExpr`'s children have been walked. Bottom-up
//!   replacement of arithmetic-of-literals fits here.
//! * [`visit_literal`](QueryWalkerMut::visit_literal) — called on each
//!   `LiteralValue` reached outside a `ScalarExpr` (currently LIMIT
//!   count/offset and VALUES row entries). Parameter substitution fits
//!   here.
//!
//! Both default to no-ops so a pass only implements the hooks it uses.

use crate::query::ast::{
    FrameBound, JoinQual, LiteralValue, QueryBody, QueryExpr, ScalarExpr, SelectColumn,
    SelectColumns, SelectNode, TableSource, WhereExpr,
};

/// A mutable visitor over a `QueryExpr` tree.
///
/// Implementors override the hooks they care about and pass an instance
/// to [`query_expr_walk_mut`] (or [`select_node_walk_mut`] for a
/// `SelectNode`-scoped walk).
pub trait QueryWalkerMut {
    type Error;

    /// Called after a `ScalarExpr`'s children have been walked. The
    /// implementor may inspect or replace the node.
    fn visit_scalar_post(&mut self, _expr: &mut ScalarExpr) -> Result<(), Self::Error> {
        Ok(())
    }

    /// Called on each `LiteralValue` reached outside a `ScalarExpr`
    /// (LIMIT count/offset, VALUES row entries).
    fn visit_literal(&mut self, _literal: &mut LiteralValue) -> Result<(), Self::Error> {
        Ok(())
    }
}

/// Drive a full `QueryExpr` walk: CTEs, body, LIMIT, top-level ORDER BY.
pub fn query_expr_walk_mut<W: QueryWalkerMut>(
    expr: &mut QueryExpr,
    walker: &mut W,
) -> Result<(), W::Error> {
    for cte in &mut expr.ctes {
        query_expr_walk_mut(&mut cte.query, walker)?;
    }
    query_body_walk_mut(&mut expr.body, walker)?;
    if let Some(limit) = &mut expr.limit {
        if let Some(count) = &mut limit.count {
            walker.visit_literal(count)?;
        }
        if let Some(offset) = &mut limit.offset {
            walker.visit_literal(offset)?;
        }
    }
    for clause in &mut expr.order_by {
        scalar_expr_walk_mut(&mut clause.expr, walker)?;
    }
    Ok(())
}

/// Drive a `SelectNode`-scoped walk: columns, FROM, WHERE, HAVING.
/// Top-level ORDER BY and LIMIT live on `QueryExpr`, not `SelectNode`,
/// and are not reached here.
pub fn select_node_walk_mut<W: QueryWalkerMut>(
    node: &mut SelectNode,
    walker: &mut W,
) -> Result<(), W::Error> {
    if let SelectColumns::Columns(cols) = &mut node.columns {
        for col in cols {
            if let SelectColumn::Expr { expr, .. } = col {
                scalar_expr_walk_mut(expr, walker)?;
            }
        }
    }
    for source in &mut node.from {
        table_source_walk_mut(source, walker)?;
    }
    if let Some(w) = &mut node.where_clause {
        where_expr_walk_mut(w, walker)?;
    }
    if let Some(h) = &mut node.having {
        where_expr_walk_mut(h, walker)?;
    }
    Ok(())
}

fn query_body_walk_mut<W: QueryWalkerMut>(
    body: &mut QueryBody,
    walker: &mut W,
) -> Result<(), W::Error> {
    match body {
        QueryBody::Select(node) => select_node_walk_mut(node, walker)?,
        QueryBody::Values(values) => {
            for row in &mut values.rows {
                for literal in row {
                    walker.visit_literal(literal)?;
                }
            }
        }
        QueryBody::SetOp(set_op) => {
            query_expr_walk_mut(&mut set_op.left, walker)?;
            query_expr_walk_mut(&mut set_op.right, walker)?;
        }
    }
    Ok(())
}

fn table_source_walk_mut<W: QueryWalkerMut>(
    source: &mut TableSource,
    walker: &mut W,
) -> Result<(), W::Error> {
    match source {
        TableSource::Join(join) => {
            if let JoinQual::On(cond) = &mut join.qual {
                where_expr_walk_mut(cond, walker)?;
            }
            table_source_walk_mut(&mut join.left, walker)?;
            table_source_walk_mut(&mut join.right, walker)?;
        }
        TableSource::Subquery(sub) => query_expr_walk_mut(&mut sub.query, walker)?,
        TableSource::CteRef(cte_ref) => query_expr_walk_mut(&mut cte_ref.query, walker)?,
        TableSource::Table(_) => {}
    }
    Ok(())
}

fn where_expr_walk_mut<W: QueryWalkerMut>(
    expr: &mut WhereExpr,
    walker: &mut W,
) -> Result<(), W::Error> {
    match expr {
        WhereExpr::Scalar(scalar) => scalar_expr_walk_mut(scalar, walker)?,
        WhereExpr::Unary(u) => where_expr_walk_mut(&mut u.expr, walker)?,
        WhereExpr::Binary(b) => {
            where_expr_walk_mut(&mut b.lexpr, walker)?;
            where_expr_walk_mut(&mut b.rexpr, walker)?;
        }
        WhereExpr::Multi(m) => {
            for e in &mut m.exprs {
                where_expr_walk_mut(e, walker)?;
            }
        }
        WhereExpr::Subquery {
            query, test_expr, ..
        } => {
            query_expr_walk_mut(query, walker)?;
            if let Some(test) = test_expr {
                scalar_expr_walk_mut(test, walker)?;
            }
        }
    }
    Ok(())
}

fn frame_bound_walk_mut<W: QueryWalkerMut>(
    bound: &mut FrameBound,
    walker: &mut W,
) -> Result<(), W::Error> {
    match bound {
        FrameBound::OffsetPreceding(e) | FrameBound::OffsetFollowing(e) => {
            scalar_expr_walk_mut(e, walker)?;
        }
        FrameBound::UnboundedPreceding
        | FrameBound::CurrentRow
        | FrameBound::UnboundedFollowing => {}
    }
    Ok(())
}

fn scalar_expr_walk_mut<W: QueryWalkerMut>(
    expr: &mut ScalarExpr,
    walker: &mut W,
) -> Result<(), W::Error> {
    match expr {
        ScalarExpr::Column(_) | ScalarExpr::Literal(_) => {}
        ScalarExpr::Arithmetic(arith) => {
            scalar_expr_walk_mut(&mut arith.left, walker)?;
            scalar_expr_walk_mut(&mut arith.right, walker)?;
        }
        ScalarExpr::Function(func) => {
            for arg in &mut func.args {
                scalar_expr_walk_mut(arg, walker)?;
            }
            for clause in &mut func.agg_order {
                scalar_expr_walk_mut(&mut clause.expr, walker)?;
            }
            if let Some(filter) = &mut func.agg_filter {
                where_expr_walk_mut(filter, walker)?;
            }
            if let Some(over) = &mut func.over {
                for col in &mut over.partition_by {
                    scalar_expr_walk_mut(col, walker)?;
                }
                for clause in &mut over.order_by {
                    scalar_expr_walk_mut(&mut clause.expr, walker)?;
                }
                if let Some(frame) = &mut over.frame {
                    frame_bound_walk_mut(&mut frame.start, walker)?;
                    frame_bound_walk_mut(&mut frame.end, walker)?;
                }
            }
        }
        ScalarExpr::Case(case) => {
            if let Some(arg) = &mut case.arg {
                scalar_expr_walk_mut(arg, walker)?;
            }
            for when in &mut case.whens {
                where_expr_walk_mut(&mut when.condition, walker)?;
                scalar_expr_walk_mut(&mut when.result, walker)?;
            }
            if let Some(default) = &mut case.default {
                scalar_expr_walk_mut(default, walker)?;
            }
        }
        ScalarExpr::Subquery(query) => query_expr_walk_mut(query, walker)?,
        ScalarExpr::Array(elems) => {
            for elem in elems {
                scalar_expr_walk_mut(elem, walker)?;
            }
        }
        ScalarExpr::TypeCast { expr, .. } => scalar_expr_walk_mut(expr, walker)?,
    }
    walker.visit_scalar_post(expr)?;
    Ok(())
}
