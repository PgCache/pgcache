//! Retarget one table occurrence's column references onto a new alias.
//!
//! When a table source is swapped for a `VALUES` subquery (the CDC
//! membership-test rewrite), every reference to that occurrence has to move to
//! the subquery's alias — otherwise a schema-qualified `schema.table.col`
//! survives while the FROM entry is only `alias`, which Postgres rejects as
//! "invalid reference to FROM-clause entry" (PGC-139).
//!
//! The walk is scope-aware, and that is why it is hand-written rather than
//! routed through the uniform `nodes()` traversal (see `resolved::traverse`):
//!
//! * A FROM-clause **derived table** is its own name scope and cannot reference
//!   the table being replaced, so we do not descend into it.
//! * A WHERE-clause **subquery** can (a correlated reference), so we do.
//!
//! It is also *occurrence*-scoped: matching on bare `(schema, table)` clobbers
//! the other arms of a self-join, so references are matched on the replaced
//! occurrence's alias as well (PGC-256). Where an inner scope re-opens the same
//! table under the *same* alias, its references are indistinguishable from
//! correlated references to the outer one, and the rewrite refuses rather than
//! emit a silently-wrong predicate.

use ecow::EcoString;

use crate::query::resolved::{
    ResolvedColumnNode, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableSource, ResolvedWhereExpr,
};

use super::{AstTransformError, AstTransformResult, TableOccurrence};

struct AliasRewrite<'a> {
    schema: &'a str,
    table: &'a str,
    occurrence: &'a TableOccurrence,
}

/// Rewrite every reference to the `schema.table` occurrence in `resolved` to use
/// its new alias, so it deparses as `alias.col`.
///
/// Fails with [`AstTransformError::ShadowedTable`] if an inner scope re-opens the
/// same table under the same alias — see the module docs.
pub(super) fn resolved_select_node_alias_rewrite(
    resolved: &mut ResolvedSelectNode,
    schema: &str,
    table: &str,
    occurrence: &TableOccurrence,
) -> AstTransformResult<()> {
    AliasRewrite {
        schema,
        table,
        occurrence,
    }
    .select_node(resolved)
}

impl AliasRewrite<'_> {
    /// A reference to the occurrence being replaced: same table, and same alias
    /// (both unaliased counts as a match).
    fn targets_occurrence(&self, col: &ResolvedColumnNode) -> bool {
        col.schema == self.schema
            && col.table == self.table
            && col.table_alias.as_deref() == self.occurrence.explicit_alias.as_deref()
    }

    fn column(&self, col: &mut ResolvedColumnNode) {
        if self.targets_occurrence(col) {
            col.table_alias = Some(self.occurrence.effective_alias.clone());
        }
    }

    /// Whether `select`'s own FROM re-opens the target table under the target's
    /// alias. If it does, references inside `select` bind to *that* instance,
    /// not to the occurrence we are replacing, and the two are indistinguishable
    /// at this level.
    fn shadows_occurrence(&self, select: &ResolvedSelectNode) -> bool {
        fn shadowed(rewrite: &AliasRewrite<'_>, source: &ResolvedTableSource) -> bool {
            match source {
                ResolvedTableSource::Table(table) => {
                    table.schema == rewrite.schema
                        && table.name == rewrite.table
                        && table.alias.as_deref() == rewrite.occurrence.explicit_alias.as_deref()
                }
                ResolvedTableSource::Join(join) => {
                    shadowed(rewrite, &join.left) || shadowed(rewrite, &join.right)
                }
                ResolvedTableSource::Subquery(_) => false,
            }
        }
        select.from.iter().any(|source| shadowed(self, source))
    }

    /// Every clause of a SELECT that can hold a reference to the replaced
    /// occurrence. GROUP BY / HAVING / join quals carry the same
    /// dangling-reference hazard as WHERE (PGC-145, PGC-139) — a clause omitted
    /// here deparses to invalid SQL.
    fn select_node(&self, select: &mut ResolvedSelectNode) -> AstTransformResult<()> {
        self.select_columns(&mut select.columns)?;
        for source in &mut select.from {
            self.table_source(source)?;
        }
        if let Some(where_clause) = &mut select.where_clause {
            self.where_expr(where_clause)?;
        }
        for col in &mut select.group_by {
            self.column(col);
        }
        if let Some(having) = &mut select.having {
            self.where_expr(having)?;
        }
        Ok(())
    }

    /// JOIN conditions only. A derived table is a separate name scope, so its
    /// body is left alone.
    fn table_source(&self, source: &mut ResolvedTableSource) -> AstTransformResult<()> {
        match source {
            ResolvedTableSource::Join(join) => {
                self.table_source(&mut join.left)?;
                self.table_source(&mut join.right)?;
                if let Some(predicate) = join.predicate_mut() {
                    self.where_expr(predicate)?;
                }
            }
            ResolvedTableSource::Table(_) | ResolvedTableSource::Subquery(_) => {}
        }
        Ok(())
    }

    /// A subquery body, which may hold correlated references to the occurrence.
    fn query_expr(&self, query: &mut ResolvedQueryExpr) -> AstTransformResult<()> {
        match &mut query.body {
            ResolvedQueryBody::Select(select) => {
                if self.shadows_occurrence(select) {
                    return Err(AstTransformError::ShadowedTable {
                        table: EcoString::from(self.table),
                    }
                    .into());
                }
                self.select_node(select)?;
            }
            // No column references to retarget.
            ResolvedQueryBody::Values(_) => {}
            ResolvedQueryBody::SetOp(set_op) => {
                self.query_expr(&mut set_op.left)?;
                self.query_expr(&mut set_op.right)?;
            }
        }
        for order_by in &mut query.order_by {
            self.scalar_expr(&mut order_by.expr)?;
        }
        Ok(())
    }

    fn where_expr(&self, expr: &mut ResolvedWhereExpr) -> AstTransformResult<()> {
        match expr {
            ResolvedWhereExpr::Scalar(scalar) => self.scalar_expr(scalar)?,
            ResolvedWhereExpr::Unary(unary) => self.where_expr(&mut unary.expr)?,
            ResolvedWhereExpr::Binary(binary) => {
                self.where_expr(&mut binary.lexpr)?;
                self.where_expr(&mut binary.rexpr)?;
            }
            ResolvedWhereExpr::Multi(multi) => {
                for expr in &mut multi.exprs {
                    self.where_expr(expr)?;
                }
            }
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                self.query_expr(query)?;
                if let Some(test) = test_expr {
                    self.scalar_expr(test)?;
                }
            }
        }
        Ok(())
    }

    fn select_columns(&self, columns: &mut ResolvedSelectColumns) -> AstTransformResult<()> {
        match columns {
            ResolvedSelectColumns::Columns(cols) => {
                for col in cols {
                    self.scalar_expr(&mut col.expr)?;
                }
            }
            ResolvedSelectColumns::None => {}
        }
        Ok(())
    }

    fn scalar_expr(&self, expr: &mut ResolvedScalarExpr) -> AstTransformResult<()> {
        match expr {
            ResolvedScalarExpr::Column(col) => self.column(col),
            ResolvedScalarExpr::Function(func) => {
                for arg in &mut func.args {
                    self.scalar_expr(arg)?;
                }
                for clause in &mut func.agg_order {
                    self.scalar_expr(&mut clause.expr)?;
                }
                if let Some(filter) = &mut func.agg_filter {
                    self.where_expr(filter)?;
                }
                if let Some(window_spec) = &mut func.over {
                    for col in &mut window_spec.partition_by {
                        self.scalar_expr(col)?;
                    }
                    for clause in &mut window_spec.order_by {
                        self.scalar_expr(&mut clause.expr)?;
                    }
                }
            }
            ResolvedScalarExpr::Case(case) => {
                if let Some(arg) = &mut case.arg {
                    self.scalar_expr(arg)?;
                }
                for when in &mut case.whens {
                    self.where_expr(&mut when.condition)?;
                    self.scalar_expr(&mut when.result)?;
                }
                if let Some(default) = &mut case.default {
                    self.scalar_expr(default)?;
                }
            }
            ResolvedScalarExpr::Arithmetic(arith) => {
                self.scalar_expr(&mut arith.left)?;
                self.scalar_expr(&mut arith.right)?;
            }
            ResolvedScalarExpr::Subquery(query, _) => self.query_expr(query)?,
            ResolvedScalarExpr::Array(elems) => {
                for elem in elems {
                    self.scalar_expr(elem)?;
                }
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => self.scalar_expr(expr)?,
            ResolvedScalarExpr::Identifier(_) | ResolvedScalarExpr::Literal(_) => {}
        }
        Ok(())
    }
}
