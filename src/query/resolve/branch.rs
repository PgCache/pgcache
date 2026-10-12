//! One SELECT of a bound query as CDC evaluates a changed row against it
//! (ADR-057): a branch is a position in the shared rope, never a copy.

use super::bindings::ResolvedQuery;
use crate::query::ast::NodeId;
use crate::query::ast::rope::deparse::branch_select_into;
use crate::query::ast::rope::{NodeKind, Select, View};

/// A SELECT of `bound`, by position: the root or any subquery.
#[derive(Clone, Copy)]
pub struct QueryBranch<'a> {
    pub bound: &'a ResolvedQuery,
    pub select: Select<'a>,
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
        let sources = self.select.from().map(|source| source.node_ref());
        let clauses = [self.select.where_clause(), self.select.having()]
            .into_iter()
            .flatten()
            .map(|expr| expr.node_ref());
        for root in sources.chain(clauses) {
            for node in root.subtree() {
                if matches!(node.kind(), NodeKind::Table { .. }) {
                    count += 1;
                    if count >= 2 {
                        return false;
                    }
                }
            }
        }
        true
    }

    /// `SELECT true` over the branch, as the cache serves it.
    pub fn sql_into(&self, buf: &mut String) {
        branch_select_into(&mut self.bound.deparse_context(), buf, self.select, "true");
    }

    pub fn sql(&self) -> String {
        let mut buf = String::with_capacity(256);
        self.sql_into(&mut buf);
        buf
    }
}
