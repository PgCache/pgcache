use std::collections::HashSet;
use std::ops::ControlFlow;

use crate::query::ast::AstNode;

use super::*;

impl ResolvedWhereExpr {
    /// Compute the maximum subquery nesting depth in this WHERE expression.
    /// Returns 0 if there are no subqueries.
    pub fn subquery_depth(&self) -> usize {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => scalar.subquery_depth(),
            ResolvedWhereExpr::Binary(b) => b.lexpr.subquery_depth().max(b.rexpr.subquery_depth()),
            ResolvedWhereExpr::Unary(u) => u.expr.subquery_depth(),
            ResolvedWhereExpr::Multi(m) => m
                .exprs
                .iter()
                .map(|e| e.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                let inner = 1 + query.subquery_depth();
                let test = test_expr.as_ref().map_or(0, |t| t.subquery_depth());
                inner.max(test)
            }
        }
    }

    /// Count the number of leaf predicates (comparisons) in this expression.
    /// AND/OR nodes are not counted themselves, only their leaf children.
    pub fn predicate_count(&self) -> usize {
        match self {
            ResolvedWhereExpr::Binary(b) => match b.op {
                BinaryOp::And | BinaryOp::Or => {
                    b.lexpr.predicate_count() + b.rexpr.predicate_count()
                }
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual
                | BinaryOp::Like
                | BinaryOp::ILike
                | BinaryOp::NotLike
                | BinaryOp::NotILike => 1,
            },
            ResolvedWhereExpr::Multi(_) => 1, // Multi ops (IN, BETWEEN, etc.) are single predicates
            ResolvedWhereExpr::Unary(u) => u.expr.predicate_count(),
            ResolvedWhereExpr::Scalar(scalar) => match scalar {
                // A bare function/subquery used as predicate counts as one.
                ResolvedScalarExpr::Function(_) | ResolvedScalarExpr::Subquery(_, _) => 1,
                ResolvedScalarExpr::Column(_)
                | ResolvedScalarExpr::Identifier(_)
                | ResolvedScalarExpr::Literal(_)
                | ResolvedScalarExpr::Case(_)
                | ResolvedScalarExpr::Arithmetic(_)
                | ResolvedScalarExpr::Array(_)
                | ResolvedScalarExpr::TypeCast { .. } => 0,
            },
            ResolvedWhereExpr::Subquery { .. } => 1,
        }
    }
}

impl ResolvedScalarExpr {
    /// True when the expression tree contains a `Function` call whose name
    /// appears in `agg_fns`. Walks through CASE branches and arithmetic operands,
    /// but does not descend into scalar subqueries (an aggregate nested inside a
    /// subquery doesn't make the outer expression aggregating).
    pub fn has_aggregate(&self, agg_fns: &HashSet<EcoString>) -> bool {
        match self {
            ResolvedScalarExpr::Function(func) => {
                agg_fns.contains(func.name.as_str())
                    || func.args.iter().any(|a| a.has_aggregate(agg_fns))
            }
            ResolvedScalarExpr::Case(case) => {
                case.arg.as_ref().is_some_and(|a| a.has_aggregate(agg_fns))
                    || case.whens.iter().any(|w| w.result.has_aggregate(agg_fns))
                    || case
                        .default
                        .as_ref()
                        .is_some_and(|d| d.has_aggregate(agg_fns))
            }
            ResolvedScalarExpr::Arithmetic(arith) => {
                arith.left.has_aggregate(agg_fns) || arith.right.has_aggregate(agg_fns)
            }
            ResolvedScalarExpr::Array(elems) => elems.iter().any(|e| e.has_aggregate(agg_fns)),
            ResolvedScalarExpr::TypeCast { expr, .. } => expr.has_aggregate(agg_fns),
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Subquery(_, _) => false,
        }
    }

    /// Compute the maximum subquery nesting depth in this column expression.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Literal(_) => 0,
            ResolvedScalarExpr::Function(func) => func
                .args
                .iter()
                .map(|a| a.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedScalarExpr::Case(case) => {
                let arg_depth = case.arg.as_ref().map_or(0, |a| a.subquery_depth());
                let when_depth = case
                    .whens
                    .iter()
                    .map(|w| w.condition.subquery_depth().max(w.result.subquery_depth()))
                    .max()
                    .unwrap_or(0);
                let default_depth = case.default.as_ref().map_or(0, |d| d.subquery_depth());
                arg_depth.max(when_depth).max(default_depth)
            }
            ResolvedScalarExpr::Arithmetic(arith) => arith
                .left
                .subquery_depth()
                .max(arith.right.subquery_depth()),
            ResolvedScalarExpr::Subquery(query, _) => 1 + query.subquery_depth(),
            ResolvedScalarExpr::Array(elems) => {
                elems.iter().map(|e| e.subquery_depth()).max().unwrap_or(0)
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => expr.subquery_depth(),
        }
    }
}

impl ResolvedSelectColumn {
    /// The column's output name — its alias if present, otherwise inferred
    /// from the expression (the column name for `Column` / `Identifier`).
    /// Returns `None` for unaliased function, literal, case, arithmetic, or
    /// subquery expressions, which have no stable output name (PG reports
    /// `?column?`).
    pub fn output_name(&self) -> Option<&EcoString> {
        if let Some(alias) = &self.alias {
            return Some(alias);
        }
        match &self.expr {
            ResolvedScalarExpr::Column(c) => Some(&c.column),
            ResolvedScalarExpr::Identifier(name) => Some(name),
            ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(_, _)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. } => None,
        }
    }
}

impl ResolvedSelectColumns {
    /// Compute the maximum subquery nesting depth in the SELECT list.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedSelectColumns::Columns(columns) => columns
                .iter()
                .map(|c| c.expr.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedSelectColumns::None => 0,
        }
    }

    /// Find the 1-based position of a SELECT column whose expression is
    /// structurally equal to `expr`. Used to emit positional ORDER BY
    /// (`ORDER BY N`) when serving from an MV table — the MV's columns are
    /// named by the original SELECT-list scope, so source-qualified refs
    /// (`public.orders.status`, `count(orders.id)`) aren't valid against the
    /// MV; positional ORDER BY sidesteps the naming entirely.
    pub fn columns_position_of(&self, expr: &ResolvedScalarExpr) -> Option<usize> {
        // ORDER BY against a SELECT-list alias resolves to `Identifier(name)`;
        // match by output name so positional rewrite still works for MV serving.
        match expr {
            ResolvedScalarExpr::Identifier(name) => self.position_by_output_name(name.as_str()),
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(..)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. } => {
                let Self::Columns(cols) = self else {
                    return None;
                };
                cols.iter().position(|c| c.expr == *expr).map(|i| i + 1)
            }
        }
    }

    /// 1-based position of the first SELECT column whose output name (alias or
    /// inferred — see `ResolvedSelectColumn::output_name`) matches `name`.
    pub fn position_by_output_name(&self, name: &str) -> Option<usize> {
        let Self::Columns(cols) = self else {
            return None;
        };
        cols.iter()
            .position(|c| c.output_name().is_some_and(|n| n == name))
            .map(|i| i + 1)
    }
}

impl ResolvedTableSource {
    /// Collect direct table nodes from this source, traversing JOINs but not subqueries.
    fn direct_table_nodes_collect<'a>(&'a self, tables: &mut Vec<&'a ResolvedTableNode>) {
        match self {
            ResolvedTableSource::Table(table) => tables.push(table),
            ResolvedTableSource::Subquery(_) => {} // handled as separate branch
            ResolvedTableSource::Join(join) => {
                join.left.direct_table_nodes_collect(tables);
                join.right.direct_table_nodes_collect(tables);
            }
        }
    }

    /// Compute the maximum subquery nesting depth from this table source.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedTableSource::Table(_) => 0,
            ResolvedTableSource::Subquery(sub) => 1 + sub.query.subquery_depth(),
            ResolvedTableSource::Join(join) => {
                let condition_depth = join.predicate().map_or(0, |c| c.subquery_depth());
                join.left
                    .subquery_depth()
                    .max(join.right.subquery_depth())
                    .max(condition_depth)
            }
        }
    }
}

impl ResolvedJoinNode {
    /// The join predicate for freshness/invalidation analysis — the
    /// `ON` expression, or the synthesized equi-join for `USING`/
    /// `NATURAL`. `None` for a cartesian (`CROSS`) join.
    pub fn predicate(&self) -> Option<&ResolvedWhereExpr> {
        match &self.qual {
            ResolvedJoinQual::On(e) | ResolvedJoinQual::Using { predicate: e, .. } => Some(e),
            ResolvedJoinQual::Cross => None,
        }
    }

    /// Mutable view of [`Self::predicate`], for in-place rewrites
    /// (e.g. the CDC VALUES-replacement alias update).
    pub fn predicate_mut(&mut self) -> Option<&mut ResolvedWhereExpr> {
        match &mut self.qual {
            ResolvedJoinQual::On(e) | ResolvedJoinQual::Using { predicate: e, .. } => Some(e),
            ResolvedJoinQual::Cross => None,
        }
    }
}

impl ResolvedSelectNode {
    /// Returns table nodes directly in the FROM clause, traversing JOINs but not
    /// entering subqueries (FROM-clause derived tables or WHERE-clause subqueries).
    ///
    /// Use this instead of `nodes::<ResolvedTableNode>()` when you only want the
    /// tables that this branch can directly SELECT from. Subquery tables are handled
    /// as separate branches via `select_nodes_collect`.
    pub fn direct_table_nodes(&self) -> Vec<&ResolvedTableNode> {
        let mut tables = Vec::new();
        for source in &self.from {
            source.direct_table_nodes_collect(&mut tables);
        }
        tables
    }

    /// Check if this SELECT references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [ResolvedTableSource::Table(_)])
    }

    /// Compute the maximum subquery nesting depth in this SELECT.
    /// A flat query returns 0, one level of subquery returns 1, etc.
    pub fn subquery_depth(&self) -> usize {
        let from_depth = self
            .from
            .iter()
            .map(|s| s.subquery_depth())
            .max()
            .unwrap_or(0);
        let where_depth = self.where_clause.as_ref().map_or(0, |w| w.subquery_depth());
        let having_depth = self.having.as_ref().map_or(0, |h| h.subquery_depth());
        let columns_depth = self.columns.subquery_depth();
        from_depth
            .max(where_depth)
            .max(having_depth)
            .max(columns_depth)
    }

    /// Compute a complexity score for this query.
    ///
    /// Higher scores indicate more complex queries. Update queries are sorted
    /// by complexity (ascending) so simpler/inner queries are tried first during
    /// CDC processing — this ensures inner subquery tables are populated in
    /// the cache before outer queries that depend on them.
    ///
    /// Components:
    /// - Joins: each join adds 3 (joins require matching across tables)
    /// - Predicates: each WHERE clause comparison adds 1
    /// - Subquery depth: each nesting level adds 5 (outer queries depend on inner)
    pub fn complexity(&self) -> usize {
        let direct_table_count = self.direct_table_nodes().len();
        let join_count = direct_table_count.saturating_sub(1);
        let predicate_count = self
            .where_clause
            .as_ref()
            .map(|w| w.predicate_count())
            .unwrap_or(0);
        let subquery_depth = self.subquery_depth();
        (join_count * 3) + predicate_count + (subquery_depth * 5)
    }
}

impl ResolvedQueryBody {
    /// SELECT-list columns if this body is a `Select`, else `None`. Set
    /// operations and VALUES bodies have no single SELECT scope.
    pub fn select_columns(&self) -> Option<&ResolvedSelectColumns> {
        match self {
            ResolvedQueryBody::Select(s) => Some(&s.columns),
            ResolvedQueryBody::SetOp(_) | ResolvedQueryBody::Values(_) => None,
        }
    }
}

impl ResolvedQueryExpr {
    /// Check if query only references a single table. Walks via
    /// `try_for_each_node` (short-circuiting on the second table) rather than
    /// `nodes()`, which would collect every table node into a Vec — this runs
    /// per CDC event on the update/delete paths.
    pub fn is_single_table(&self) -> bool {
        let mut seen = false;
        self.try_for_each_node::<ResolvedTableNode, ()>(&mut |_| {
            if seen {
                return ControlFlow::Break(());
            }
            seen = true;
            ControlFlow::Continue(())
        })
        .is_continue()
    }

    /// Check if query has a WHERE clause (only applies to SELECT bodies)
    pub fn has_where_clause(&self) -> bool {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.where_clause.is_some(),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => false,
        }
    }

    /// Get the WHERE clause if it exists (only for SELECT bodies)
    pub fn where_clause(&self) -> Option<&ResolvedWhereExpr> {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.where_clause.as_ref(),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
        }
    }

    /// Get the SELECT body if this is a simple SELECT query
    pub fn as_select(&self) -> Option<&ResolvedSelectNode> {
        match &self.body {
            ResolvedQueryBody::Select(select) => Some(select),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
        }
    }

    /// Compute the maximum subquery nesting depth in this query.
    pub fn subquery_depth(&self) -> usize {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.subquery_depth(),
            ResolvedQueryBody::Values(_) => 0,
            ResolvedQueryBody::SetOp(set_op) => set_op
                .left
                .subquery_depth()
                .max(set_op.right.subquery_depth()),
        }
    }

    /// Compute a complexity score for this query.
    pub fn complexity(&self) -> usize {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.complexity(),
            ResolvedQueryBody::Values(_) => 0,
            ResolvedQueryBody::SetOp(set_op) => {
                set_op.left.complexity() + set_op.right.complexity() + 1
            }
        }
    }
}
