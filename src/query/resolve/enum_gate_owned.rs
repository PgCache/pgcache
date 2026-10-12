//! The enum order-dependence gate over the owned tree: the oracle for
//! `enum_gate.rs` while the owned tree lasts.

use std::ops::ControlFlow;

use ecow::EcoString;
use rootcause::Report;

use super::bindings::{Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem};
use crate::query::ast::{
    AstNode, BinaryExpr, BinaryOp, CaseExpr, ColumnNode, FunctionCall, JoinQual, LiteralValue,
    MultiExpr, MultiOp, OrderByClause, QueryBody, QueryExpr, ScalarExpr, SelectColumn,
    SelectColumns, SelectNode, TableSource, WhereExpr,
};
use crate::query::resolve::{ResolveError, ResolveResult};

/// The owned gate's verdict, the oracle for the rope gate.
pub(super) fn enum_order_dependence_check_owned(bound: &ResolvedQuery) -> ResolveResult<()> {
    let gate = Gate {
        bindings: bound.bindings(),
    };
    gate.query_check(bound.tree())
}

/// An enum-ordered column reached from a checked position, named as the
/// catalog knows it.
struct EnumColumn {
    table: EcoString,
    column: EcoString,
}

impl EnumColumn {
    fn reject(self, position: &'static str) -> Report<ResolveError> {
        Report::from(ResolveError::EnumOrderDependentUsage {
            table: self.table,
            column: self.column,
            position,
        })
    }
}

struct Gate<'a> {
    bindings: &'a Bindings,
}

impl Gate<'_> {
    /// The enum-ordered column `binding` names, if it is one.
    fn binding_enum(&self, binding: &ColumnBinding) -> Option<EnumColumn> {
        let slot = self.bindings.slot(binding.slot)?;
        let column = self.bindings.binding_column(binding)?;
        column.is_enum_ordered().then(|| EnumColumn {
            table: slot.table().name.clone(),
            column: column.name.clone(),
        })
    }

    /// The enum-ordered column a reference stands for. A merged column
    /// stands for its left column, and under an outer join for the right
    /// one too (`COALESCE(left, right)`).
    fn column_enum(&self, column: &ColumnNode) -> Option<EnumColumn> {
        match self.bindings.column(column)? {
            ColumnTarget::Column(binding) => self.binding_enum(binding),
            ColumnTarget::Merged(id) => {
                let merged = self.bindings.merged(*id)?;
                self.binding_enum(&merged.left).or_else(|| {
                    merged
                        .outer
                        .then(|| self.binding_enum(&merged.right))
                        .flatten()
                })
            }
            ColumnTarget::Output => None,
        }
    }

    /// First enum-ordered column reachable under `node`, if any.
    fn enum_column_find<N: AstNode>(&self, node: &N) -> Option<EnumColumn> {
        match node.try_for_each_node::<ColumnNode, EnumColumn>(&mut |column| match self
            .column_enum(column)
        {
            Some(found) => ControlFlow::Break(found),
            None => ControlFlow::Continue(()),
        }) {
            ControlFlow::Break(found) => Some(found),
            ControlFlow::Continue(()) => None,
        }
    }

    /// Reject at `position` if any of `nodes` reaches an enum-ordered column.
    fn enum_reject<'n, T: AstNode + 'n>(
        &self,
        nodes: impl IntoIterator<Item = &'n T>,
        position: &'static str,
    ) -> ResolveResult<()> {
        match nodes
            .into_iter()
            .find_map(|node| self.enum_column_find(node))
        {
            Some(found) => Err(found.reject(position)),
            None => Ok(()),
        }
    }

    fn query_check(&self, query: &QueryExpr) -> ResolveResult<()> {
        self.order_by_check(&query.order_by, &query.body, "ORDER BY")?;
        // CTE definitions are not resolved; the copy each reference embeds
        // is walked in its place.
        match &query.body {
            QueryBody::Select(select) => self.select_check(select),
            QueryBody::SetOp(set_op) => {
                self.query_check(&set_op.left)?;
                self.query_check(&set_op.right)
            }
            QueryBody::Values(_) => Ok(()),
        }
    }

    fn order_by_check(
        &self,
        clauses: &[OrderByClause],
        body: &QueryBody,
        position: &'static str,
    ) -> ResolveResult<()> {
        for clause in clauses {
            if let Some(found) = self.order_clause_enum_column(clause, body) {
                return Err(found.reject(position));
            }
        }
        Ok(())
    }

    /// The enum column an ORDER BY clause sorts by, if any.
    fn order_clause_enum_column(
        &self,
        clause: &OrderByClause,
        body: &QueryBody,
    ) -> Option<EnumColumn> {
        if let Some(found) = self.enum_column_find(&clause.expr) {
            return Some(found);
        }
        // An ORDER BY naming an output column hides the column behind the
        // output name; check the named output expression instead.
        if let ScalarExpr::Column(column) = &clause.expr
            && matches!(self.bindings.column(column), Some(ColumnTarget::Output))
        {
            return self
                .output_columns(body)?
                .into_iter()
                .find(|output| output.name.as_ref() == Some(&column.column))?
                .enum_column;
        }
        // An ordinal (`ORDER BY 2`) is an integer literal but sorts by the
        // referenced output column.
        if let ScalarExpr::Literal(LiteralValue::Integer(ordinal)) = &clause.expr {
            let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
            return self
                .output_columns(body)?
                .into_iter()
                .nth(index)?
                .enum_column;
        }
        None
    }

    /// The output columns ORDER BY references resolve against — the
    /// SELECT's, or the leftmost SELECT's for a set operation — with stars
    /// expanded: each with its output name and the enum column it carries.
    fn output_columns(&self, body: &QueryBody) -> Option<Vec<OutputColumn>> {
        let select = match body {
            QueryBody::Select(select) => select,
            QueryBody::SetOp(set_op) => return self.output_columns(&set_op.left.body),
            QueryBody::Values(_) => return None,
        };
        let SelectColumns::Columns(columns) = &select.columns else {
            return None;
        };
        let mut outputs = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                SelectColumn::Expr { expr, alias } => {
                    let name = alias.clone().or_else(|| match expr {
                        ScalarExpr::Column(column) => Some(column.column.clone()),
                        ScalarExpr::Function(_)
                        | ScalarExpr::Literal(_)
                        | ScalarExpr::Case(_)
                        | ScalarExpr::Arithmetic(_)
                        | ScalarExpr::Subquery(_)
                        | ScalarExpr::Array(_)
                        | ScalarExpr::TypeCast { .. } => None,
                    });
                    outputs.push(OutputColumn {
                        name,
                        enum_column: self.enum_column_find(expr),
                    });
                }
                SelectColumn::Star(star) => {
                    for item in self.bindings.star(star).unwrap_or(&[]) {
                        outputs.push(match item {
                            StarItem::Column(binding) => OutputColumn {
                                name: self
                                    .bindings
                                    .binding_column(binding)
                                    .map(|c| c.name.clone()),
                                enum_column: self.binding_enum(binding),
                            },
                            StarItem::Merged(id) => {
                                let merged = self.bindings.merged(*id);
                                OutputColumn {
                                    name: merged.map(|m| m.name.clone()),
                                    enum_column: merged.and_then(|m| {
                                        self.binding_enum(&m.left).or_else(|| {
                                            m.outer.then(|| self.binding_enum(&m.right)).flatten()
                                        })
                                    }),
                                }
                            }
                        });
                    }
                }
            }
        }
        Some(outputs)
    }

    fn select_check(&self, select: &SelectNode) -> ResolveResult<()> {
        if let SelectColumns::Columns(columns) = &select.columns {
            for column in columns {
                if let SelectColumn::Expr { expr, .. } = column {
                    self.scalar_check(expr)?;
                }
            }
        }
        for source in &select.from {
            self.table_source_check(source)?;
        }
        if let Some(where_clause) = &select.where_clause {
            self.where_check(where_clause)?;
        }
        // group_by is equality semantics — allowed.
        if let Some(having) = &select.having {
            self.where_check(having)?;
        }
        Ok(())
    }

    fn table_source_check(&self, source: &TableSource) -> ResolveResult<()> {
        match source {
            TableSource::Table(_) => Ok(()),
            TableSource::Subquery(subquery) => self.query_check(&subquery.query),
            TableSource::CteRef(cte_ref) => self.query_check(&cte_ref.query),
            TableSource::Join(join) => {
                self.table_source_check(&join.left)?;
                self.table_source_check(&join.right)?;
                match &join.qual {
                    JoinQual::On(condition) => self.where_check(condition),
                    // USING/NATURAL merge on equality; Cross has no qualifier.
                    JoinQual::Using(_) | JoinQual::Natural | JoinQual::Cross => Ok(()),
                }
            }
        }
    }

    fn where_check(&self, expr: &WhereExpr) -> ResolveResult<()> {
        match expr {
            WhereExpr::Scalar(scalar) => self.scalar_check(scalar),
            WhereExpr::Unary(unary) => self.where_check(&unary.expr),
            WhereExpr::Binary(binary) => self.binary_check(binary),
            WhereExpr::Multi(multi) => self.multi_check(multi),
            // Predicate sublinks carry equality semantics (IN/NOT IN; ALL is
            // restricted to `<>` at AST conversion), so the test expression is
            // safe; the subquery body still needs its own walk.
            WhereExpr::Subquery { query, .. } => self.query_check(query),
        }
    }

    fn binary_check(&self, binary: &BinaryExpr) -> ResolveResult<()> {
        if binary_op_is_range(binary.op) {
            self.enum_reject([&*binary.lexpr, &*binary.rexpr], "range comparison")?;
        }
        self.where_check(&binary.lexpr)?;
        self.where_check(&binary.rexpr)
    }

    fn multi_check(&self, multi: &MultiExpr) -> ResolveResult<()> {
        let order_dependent = multi_op_is_order_dependent(multi.op);
        for expr in &multi.exprs {
            if order_dependent {
                self.enum_reject([expr], "range comparison")?;
            }
            self.where_check(expr)?;
        }
        Ok(())
    }

    fn scalar_check(&self, expr: &ScalarExpr) -> ResolveResult<()> {
        match expr {
            ScalarExpr::Column(_) | ScalarExpr::Literal(_) => Ok(()),
            ScalarExpr::Function(function) => self.function_check(function),
            ScalarExpr::Case(case) => self.case_check(case),
            ScalarExpr::Arithmetic(arithmetic) => {
                self.scalar_check(&arithmetic.left)?;
                self.scalar_check(&arithmetic.right)
            }
            ScalarExpr::Subquery(query) => self.query_check(query),
            ScalarExpr::Array(elems) => {
                for elem in elems {
                    self.scalar_check(elem)?;
                }
                Ok(())
            }
            ScalarExpr::TypeCast { expr, .. } => self.scalar_check(expr),
        }
    }

    fn function_check(&self, function: &FunctionCall) -> ResolveResult<()> {
        self.function_order_positions_check(function)?;
        if let Some(over) = &function.over {
            // partition_by is equality semantics — allowed.
            for expr in &over.partition_by {
                self.scalar_check(expr)?;
            }
        }
        for arg in &function.args {
            self.scalar_check(arg)?;
        }
        if let Some(filter) = &function.agg_filter {
            self.where_check(filter)?;
        }
        Ok(())
    }

    /// The order-dependent positions of a call: min/max arguments, the
    /// aggregate ORDER BY, and the window ORDER BY.
    fn function_order_positions_check(&self, function: &FunctionCall) -> ResolveResult<()> {
        let min_max =
            function.name.eq_ignore_ascii_case("min") || function.name.eq_ignore_ascii_case("max");
        if min_max {
            self.enum_reject(&function.args, "min/max aggregate")?;
        }
        self.enum_reject(
            function.agg_order.iter().map(|clause| &clause.expr),
            "aggregate ORDER BY",
        )?;
        if let Some(over) = &function.over {
            self.enum_reject(
                over.order_by.iter().map(|clause| &clause.expr),
                "window ORDER BY",
            )?;
        }
        Ok(())
    }

    fn case_check(&self, case: &CaseExpr) -> ResolveResult<()> {
        // Simple-CASE arg tests equality against WHEN values — safe.
        if let Some(arg) = &case.arg {
            self.scalar_check(arg)?;
        }
        for when in &case.whens {
            self.where_check(&when.condition)?;
            self.scalar_check(&when.result)?;
        }
        if let Some(default) = &case.default {
            self.scalar_check(default)?;
        }
        Ok(())
    }
}

/// One output column of a select list, as ORDER BY names or numbers it.
struct OutputColumn {
    name: Option<EcoString>,
    enum_column: Option<EnumColumn>,
}

fn binary_op_is_range(op: BinaryOp) -> bool {
    matches!(
        op,
        BinaryOp::LessThan
            | BinaryOp::LessThanOrEqual
            | BinaryOp::GreaterThan
            | BinaryOp::GreaterThanOrEqual
    )
}

/// BETWEEN forms always order; ANY/ALL order when their comparison is a range;
/// IN / NOT IN are equality.
fn multi_op_is_order_dependent(op: MultiOp) -> bool {
    match op {
        MultiOp::Between
        | MultiOp::NotBetween
        | MultiOp::BetweenSymmetric
        | MultiOp::NotBetweenSymmetric => true,
        MultiOp::Any { comparison } | MultiOp::All { comparison } => binary_op_is_range(comparison),
        MultiOp::In | MultiOp::NotIn => false,
    }
}
