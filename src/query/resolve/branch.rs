//! One SELECT of a bound query, as CDC evaluates a changed row against it.

use std::ops::ControlFlow;

use super::bindings::ResolvedQuery;
use crate::query::ast::{AstNode, NodeId, SelectNode, TableNode};
use crate::query::transform::branch_select_into;

/// One SELECT of a bound query as CDC evaluates a changed row against it:
/// its FROM, WHERE, GROUP BY and HAVING with the select list read as `true`,
/// and no ORDER BY or LIMIT. The branch is a node of the query it came
/// from, so the query's bindings serve it and nothing is copied (ADR-057).
#[derive(Clone, Copy)]
pub struct QueryBranch<'a> {
    pub bound: &'a ResolvedQuery,
    pub select: &'a SelectNode,
}

impl<'a> QueryBranch<'a> {
    /// The branch `id` names in `bound`, when it has one.
    pub fn find(bound: &'a ResolvedQuery, id: NodeId) -> Option<Self> {
        bound.select_node(id).map(|select| Self { bound, select })
    }

    /// Whether the row evaluation reads one table: no join, and no subquery
    /// in FROM, WHERE or HAVING. The select list never renders, so a
    /// subquery there does not count.
    pub fn is_single_table(&self) -> bool {
        let mut count = 0u8;
        let mut visit = |_: &TableNode| {
            count += 1;
            if count >= 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        };
        let from = self
            .select
            .from
            .iter()
            .try_for_each(|source| source.try_for_each_node(&mut visit));
        let where_clause = self
            .select
            .where_clause
            .iter()
            .try_for_each(|expr| expr.try_for_each_node(&mut visit));
        let having = self
            .select
            .having
            .iter()
            .try_for_each(|expr| expr.try_for_each_node(&mut visit));
        from.is_continue() && where_clause.is_continue() && having.is_continue()
    }

    /// The update query as SQL: `SELECT true` over the branch.
    pub fn sql_into(&self, buf: &mut String) {
        branch_select_into(&mut self.bound.deparse_context(), buf, self.select, "true");
    }

    pub fn sql(&self) -> String {
        let mut buf = String::with_capacity(256);
        self.sql_into(&mut buf);
        buf
    }
}
