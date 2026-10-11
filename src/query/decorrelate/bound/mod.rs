//! Decorrelation on the bound query (ADR-014 to ADR-017, ADR-057): every
//! correlated subquery becomes a join, built from pieces of the original
//! tree. Pieces keep their node ids so their bindings follow through the
//! refinalize remap; the references the rewrite makes (expanded stars,
//! moved merged columns, the `_dcN` derived table and its columns) carry
//! provisional ids and are bound once the tree is placed.
//!
//! * [`scope`] — what a subquery can see: its own slots, its outer references,
//!   and the correlation predicates of its WHERE.
//! * [`join`] — EXISTS / NOT EXISTS / IN / NOT IN as semi- and anti-joins.
//! * [`scalar`] — scalar subqueries as a LEFT JOINed derived table.

mod join;
mod scalar;
mod scope;

use std::collections::HashSet;
use std::sync::Arc;

use ecow::EcoString;
use rootcause::Report;

use self::join::JoinDecorrelation;
use self::scalar::{conjunct_scalar_decorrelate, select_column_decorrelate};
use self::scope::{Scope, scalar_moved, where_has_correlation, where_moved};
use super::{DecorrelateError, DecorrelateResult};
use crate::catalog::{ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use crate::oid::Oid;
use crate::query::ast::{
    BinaryOp, ColumnNode, FunctionCall, LiteralValue, NodeId, QueryBody, QueryExpr, ScalarExpr,
    SelectColumn, SelectColumns, SelectNode, SetOpNode, UnaryOp, WhereExpr, query_expr_refinalize,
};
use crate::query::resolve::{
    BindingsBuilder, ColumnBinding, ColumnTarget, MergedId, ResolvedQuery, StarItem, TableSlot,
};

/// What a reference the rewrite made binds to once the tree is placed.
enum Pending {
    /// A node that binds nothing but must be placed: a derived table's
    /// FROM node, whose slot is made once its position is known.
    None,
    Column(ColumnBinding),
    /// A column of a derived table the rewrite made, by that table's index
    /// in [`Decorrelate::derived`] and the column's position.
    Derived {
        derived: usize,
        position: usize,
    },
}

/// A derived table the rewrite made: the provisional id of its FROM node
/// and the metadata of its output columns.
struct DerivedTable {
    node: NodeId,
    table: Arc<TableMetadata>,
}

/// The rewrite's state: the query it reads facts from, and what it has
/// made so far.
pub(super) struct Decorrelate<'a> {
    pub(super) bound: &'a ResolvedQuery,
    pub(super) aggregate_functions: &'a HashSet<EcoString>,
    derived_counter: u32,
    scalar_counter: u32,
    /// Provisional ids are handed out in order; entry `k` is
    /// `NodeId::provisional(k)`.
    pending: Vec<Pending>,
    derived: Vec<DerivedTable>,
    /// Column nodes copied out of a subquery into the scope they referred
    /// to, by old id: no longer correlated.
    uncorrelated: Vec<NodeId>,
}

/// Decorrelate every correlated subquery of `bound`. `Ok(None)` when it has
/// none; `Err` when one sits where no join form exists.
pub fn query_decorrelate(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
) -> DecorrelateResult<Option<ResolvedQuery>> {
    let mut state = Decorrelate {
        bound,
        aggregate_functions,
        derived_counter: 0,
        scalar_counter: 0,
        pending: Vec::new(),
        derived: Vec::new(),
        uncorrelated: Vec::new(),
    };
    let (mut tree, transformed) = state.query(bound.tree())?;
    if !transformed {
        return Ok(None);
    }
    let remap = query_expr_refinalize(&mut tree);
    let mut builder = BindingsBuilder::from_remapped(bound.bindings(), &remap);
    let mut derived_slots = Vec::with_capacity(state.derived.len());
    for derived in &state.derived {
        let Some(node) = remap.new_id(derived.node) else {
            return Err(unplaced("a derived table"));
        };
        derived_slots.push(builder.slot_push(
            node,
            TableSlot::Derived {
                node,
                table: Arc::clone(&derived.table),
            },
        ));
    }
    for (k, pending) in state.pending.iter().enumerate() {
        let provisional = NodeId::provisional(u32::try_from(k).unwrap_or(u32::MAX));
        let Some(node) = remap.new_id(provisional) else {
            return Err(unplaced("a column reference"));
        };
        let binding = match pending {
            Pending::None => continue,
            Pending::Column(binding) => *binding,
            Pending::Derived { derived, position } => ColumnBinding {
                slot: *derived_slots
                    .get(*derived)
                    .ok_or_else(|| unplaced("a derived table"))?,
                position: ColumnPosition::from_index(*position)
                    .ok_or_else(|| unplaced("a derived column"))?,
            },
        };
        builder.column_bind(node, ColumnTarget::Column(binding), false);
    }
    for old in &state.uncorrelated {
        for node in remap.new_ids(*old) {
            builder.correlated_remove(node);
        }
    }
    Ok(Some(ResolvedQuery::new(Arc::new(tree), builder.finish())))
}

/// The rewrite built a node it then did not place: a defect, reported as
/// non-decorrelatable so the query is cached in its correlated form.
fn unplaced(what: &str) -> Report<DecorrelateError> {
    debug_assert!(false, "decorrelation made {what} it did not place");
    Report::from(DecorrelateError::NonDecorrelatable {
        reason: format!("decorrelation made {what} it did not place"),
    })
}

impl<'a> Decorrelate<'a> {
    pub(super) fn next_derived_alias(&mut self) -> EcoString {
        self.derived_counter += 1;
        EcoString::from(format!("_dc{}", self.derived_counter))
    }

    pub(super) fn next_scalar_alias(&mut self) -> EcoString {
        self.scalar_counter += 1;
        EcoString::from(format!("_ds{}", self.scalar_counter))
    }

    fn provisional(&mut self, pending: Pending) -> NodeId {
        let k = u32::try_from(self.pending.len()).unwrap_or(u32::MAX);
        self.pending.push(pending);
        NodeId::provisional(k)
    }

    /// A reference to a bound column, qualified by its slot.
    pub(super) fn column_for(&mut self, binding: ColumnBinding) -> ColumnNode {
        let bindings = self.bound.bindings();
        let (table, column) = match (
            bindings.slot(binding.slot),
            bindings.binding_column(&binding),
        ) {
            (Some(slot), Some(column)) => (EcoString::from(slot.qualifier()), column.name.clone()),
            _ => {
                debug_assert!(false, "column binding without a slot or column");
                (EcoString::new(), EcoString::new())
            }
        };
        ColumnNode {
            id: self.provisional(Pending::Column(binding)),
            table: Some(table),
            column,
        }
    }

    /// A derived table the rewrite makes, named `alias` with `columns`;
    /// returns its index for [`Decorrelate::derived_column`] and the
    /// provisional id its FROM node must carry.
    pub(super) fn derived_push(
        &mut self,
        alias: &EcoString,
        columns: Vec<ColumnMetadata>,
    ) -> (usize, NodeId) {
        let node = self.provisional(Pending::None);
        self.derived.push(DerivedTable {
            node,
            table: Arc::new(TableMetadata {
                replica_identity_full: false,
                relation_oid: Oid::from_raw(0),
                name: alias.clone(),
                schema: EcoString::new(),
                primary_key_columns: Vec::new(),
                columns: ColumnStore::new(columns),
                indexes: Vec::new(),
            }),
        });
        (self.derived.len() - 1, node)
    }

    /// A reference to column `position` of the derived table `derived`.
    pub(super) fn derived_column(
        &mut self,
        derived: usize,
        alias: &EcoString,
        name: EcoString,
        position: usize,
    ) -> ColumnNode {
        ColumnNode {
            id: self.provisional(Pending::Derived { derived, position }),
            table: Some(alias.clone()),
            column: name,
        }
    }

    pub(super) fn uncorrelated_mark(&mut self, node: &ColumnNode) {
        self.uncorrelated.push(node.id);
    }

    /// The select list with stars written out as the columns they stood
    /// for: a rewrite that changes the FROM must not change what `*` means.
    pub(super) fn columns_expanded(&mut self, columns: &SelectColumns) -> SelectColumns {
        let SelectColumns::Columns(columns) = columns else {
            return SelectColumns::None;
        };
        let mut expanded = Vec::with_capacity(columns.len());
        for column in columns {
            match column {
                SelectColumn::Expr { .. } => expanded.push(column.clone()),
                SelectColumn::Star(star) => {
                    let items: Vec<StarItem> =
                        self.bound.bindings().star(star).unwrap_or(&[]).to_vec();
                    for item in items {
                        expanded.push(match item {
                            StarItem::Column(binding) => SelectColumn::Expr {
                                expr: ScalarExpr::Column(self.column_for(binding)),
                                alias: None,
                            },
                            StarItem::Merged(id) => {
                                let (expr, name) = self.merged_expr(id);
                                SelectColumn::Expr {
                                    expr,
                                    alias: Some(name),
                                }
                            }
                        });
                    }
                }
            }
        }
        SelectColumns::Columns(expanded)
    }

    /// A `USING`/`NATURAL` merged column written out: the left column, or
    /// `coalesce(left, right)` under an outer join; with its name.
    pub(super) fn merged_expr(&mut self, id: MergedId) -> (ScalarExpr, EcoString) {
        let Some(merged) = self.bound.bindings().merged(id).cloned() else {
            debug_assert!(false, "merged column without a binding");
            return (ScalarExpr::Literal(LiteralValue::Null), EcoString::new());
        };
        let left = ScalarExpr::Column(self.column_for(merged.left));
        let expr = if merged.outer {
            let right = ScalarExpr::Column(self.column_for(merged.right));
            ScalarExpr::Function(FunctionCall {
                id: NodeId::PENDING,
                schema: None,
                name: EcoString::from("coalesce"),
                args: vec![left, right],
                agg_star: false,
                agg_distinct: false,
                agg_order: Vec::new(),
                agg_filter: None,
                over: None,
            })
        } else {
            left
        };
        (expr, merged.name)
    }

    fn query(&mut self, query: &QueryExpr) -> DecorrelateResult<(QueryExpr, bool)> {
        match &query.body {
            QueryBody::Select(select) => {
                let (select, transformed) = self.select(select)?;
                let order_by = if transformed {
                    let scope = Scope::of_select(self, &select);
                    query
                        .order_by
                        .iter()
                        .map(|clause| crate::query::ast::OrderByClause {
                            expr: scalar_moved(self, &clause.expr, &scope),
                            ..clause.clone()
                        })
                        .collect()
                } else {
                    query.order_by.clone()
                };
                Ok((
                    QueryExpr {
                        body: QueryBody::Select(Box::new(select)),
                        order_by,
                        ..query.clone()
                    },
                    transformed,
                ))
            }
            QueryBody::SetOp(set_op) => {
                let (left, left_transformed) = self.query(&set_op.left)?;
                let (right, right_transformed) = self.query(&set_op.right)?;
                Ok((
                    QueryExpr {
                        body: QueryBody::SetOp(SetOpNode {
                            left: Box::new(left),
                            right: Box::new(right),
                            ..set_op.clone()
                        }),
                        ..query.clone()
                    },
                    left_transformed || right_transformed,
                ))
            }
            QueryBody::Values(_) => Ok((query.clone(), false)),
        }
    }

    /// One SELECT: scalar subqueries in its select list, then its WHERE
    /// conjuncts; EXISTS / NOT EXISTS / IN / NOT IN flatten to a join, the
    /// rest take the residual path.
    fn select(&mut self, select: &SelectNode) -> DecorrelateResult<(SelectNode, bool)> {
        let mut current = select.clone();
        let columns_transformed = self.select_columns(&mut current)?;
        if let Some(having) = &current.having
            && where_has_correlation(self, having)
        {
            return Err(rejected("correlated subquery in HAVING clause"));
        }
        let Some(where_clause) = current.where_clause.clone() else {
            return Ok((current, columns_transformed));
        };
        let mut pass = WherePass {
            select: current,
            remaining: Vec::new(),
            transformed: columns_transformed,
        };
        for conjunct in where_clause.conjuncts() {
            match JoinDecorrelation::classify(self, &conjunct)? {
                Some(kind) => pass.join_apply(self, kind, &conjunct)?,
                None => pass.residual_apply(self, conjunct)?,
            }
        }
        let (select, transformed) = pass.finish();
        Ok((
            if transformed {
                self.select_requalified(select)
            } else {
                select
            },
            transformed,
        ))
    }

    /// A rewritten SELECT's own clauses written against its new FROM: stars
    /// as the columns they stood for, every reference qualified by its slot,
    /// so the tree still says what it means once other tables sit beside
    /// the ones it named.
    fn select_requalified(&mut self, select: SelectNode) -> SelectNode {
        let scope = Scope::of_select(self, &select);
        let columns = match self.columns_expanded(&select.columns) {
            SelectColumns::Columns(columns) => SelectColumns::Columns(
                columns
                    .into_iter()
                    .map(|column| match column {
                        SelectColumn::Expr { expr, alias } => SelectColumn::Expr {
                            expr: scalar_moved(self, &expr, &scope),
                            alias,
                        },
                        SelectColumn::Star(star) => SelectColumn::Star(star),
                    })
                    .collect(),
            ),
            SelectColumns::None => SelectColumns::None,
        };
        let group_by = select
            .group_by
            .iter()
            .map(
                |column| match scalar_moved(self, &ScalarExpr::Column(column.clone()), &scope) {
                    ScalarExpr::Column(column) => column,
                    // An outer-join merged column has no column form; the
                    // resolver already forwards such a GROUP BY.
                    ScalarExpr::Function(_)
                    | ScalarExpr::Literal(_)
                    | ScalarExpr::Case(_)
                    | ScalarExpr::Arithmetic(_)
                    | ScalarExpr::Subquery(_)
                    | ScalarExpr::Array(_)
                    | ScalarExpr::TypeCast { .. } => column.clone(),
                },
            )
            .collect();
        SelectNode {
            columns,
            where_clause: select
                .where_clause
                .as_ref()
                .map(|where_clause| where_moved(self, where_clause, &scope)),
            group_by,
            having: select
                .having
                .as_ref()
                .map(|having| where_moved(self, having, &scope)),
            ..select
        }
    }

    fn select_columns(&mut self, select: &mut SelectNode) -> DecorrelateResult<bool> {
        let SelectColumns::Columns(columns) = &select.columns else {
            return Ok(false);
        };
        let mut rewritten = Vec::with_capacity(columns.len());
        let mut transformed = false;
        for column in columns {
            let (column, column_transformed) =
                select_column_decorrelate(self, column, &mut select.from)?;
            rewritten.push(column);
            transformed |= column_transformed;
        }
        select.columns = SelectColumns::Columns(rewritten);
        Ok(transformed)
    }
}

pub(super) fn rejected(reason: &str) -> Report<DecorrelateError> {
    Report::from(DecorrelateError::NonDecorrelatable {
        reason: reason.to_owned(),
    })
}

/// What the residual path does with a conjunct that is not join-family.
enum Residual {
    Keep,
    ScalarDecorrelate,
}

/// Classify a non-join conjunct, rejecting correlation under OR or under a
/// NOT the join family does not cover.
fn residual_classify(state: &Decorrelate<'_>, conjunct: &WhereExpr) -> DecorrelateResult<Residual> {
    let correlated = where_has_correlation(state, conjunct);
    match conjunct {
        WhereExpr::Binary(binary) if binary.op == BinaryOp::Or => {
            if correlated {
                return Err(rejected("correlated subquery inside OR"));
            }
            Ok(Residual::Keep)
        }
        WhereExpr::Unary(unary) if unary.op == UnaryOp::Not => {
            if scope::correlated_subquery(state, &unary.expr).is_some() {
                return Err(rejected("correlated NOT-wrapped non-EXISTS subquery"));
            }
            Ok(Residual::Keep)
        }
        WhereExpr::Scalar(_)
        | WhereExpr::Unary(_)
        | WhereExpr::Binary(_)
        | WhereExpr::Multi(_)
        | WhereExpr::Subquery { .. } => Ok(if correlated {
            Residual::ScalarDecorrelate
        } else {
            Residual::Keep
        }),
    }
}

/// The WHERE-conjunct pass over one SELECT: the node as rewritten so far,
/// the conjuncts still to rejoin as its WHERE, and whether anything changed.
struct WherePass {
    select: SelectNode,
    remaining: Vec<WhereExpr>,
    transformed: bool,
}

impl WherePass {
    /// Flatten a join-family conjunct against the node built so far. On
    /// success the rewritten node's WHERE is re-split so later conjuncts
    /// build on top; otherwise the conjunct stays as a residual.
    fn join_apply(
        &mut self,
        state: &mut Decorrelate<'_>,
        kind: JoinDecorrelation<'_>,
        conjunct: &WhereExpr,
    ) -> DecorrelateResult<()> {
        self.select.where_clause = WhereExpr::conjuncts_join(self.remaining.clone());
        match kind.try_decorrelate(state, &self.select)? {
            Some(select) => {
                self.select = select;
                self.remaining = self
                    .select
                    .where_clause
                    .take()
                    .map(WhereExpr::conjuncts)
                    .unwrap_or_default();
                self.transformed = true;
            }
            None => self.remaining.push(conjunct.clone()),
        }
        Ok(())
    }

    fn residual_apply(
        &mut self,
        state: &mut Decorrelate<'_>,
        conjunct: WhereExpr,
    ) -> DecorrelateResult<()> {
        match residual_classify(state, &conjunct)? {
            Residual::Keep => self.remaining.push(conjunct),
            Residual::ScalarDecorrelate => {
                let (conjunct, transformed) =
                    conjunct_scalar_decorrelate(state, &conjunct, &mut self.select)?;
                self.remaining.push(conjunct);
                self.transformed |= transformed;
            }
        }
        Ok(())
    }

    fn finish(mut self) -> (SelectNode, bool) {
        self.select.where_clause = WhereExpr::conjuncts_join(self.remaining);
        (self.select, self.transformed)
    }
}

#[cfg(test)]
mod tests {
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::Catalog;
    use crate::oid::TypeOid;
    use crate::query::ast::query_expr_parse;
    use crate::query::resolve::query_expr_resolve;

    fn test_table(name: &str, relation_oid: u32, column_names: &[&str]) -> TableMetadata {
        let columns = ColumnStore::new(column_names.iter().enumerate().map(|(i, column)| {
            let is_pk = i == 0;
            ColumnMetadata {
                name: (*column).into(),
                position: ColumnPosition::from_index(i).expect("column position in range"),
                type_oid: TypeOid::from_raw(if is_pk { 23 } else { 25 }),
                data_type: if is_pk { Type::INT4 } else { Type::TEXT },
                type_name: if is_pk { "int4" } else { "text" }.into(),
                cache_type_name: if is_pk { "int4" } else { "text" }.into(),
                is_primary_key: is_pk,
            }
        }));
        TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(relation_oid),
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec![column_names[0].into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn test_tables() -> Catalog {
        let mut tables = BiHashMap::new();
        for (name, oid, columns) in [
            (
                "employees",
                1,
                &["id", "name", "dept_id", "status", "manager_id"][..],
            ),
            (
                "orders",
                2,
                &["id", "emp_id", "status", "total", "customer_id"][..],
            ),
            ("departments", 3, &["id", "name", "location", "budget"][..]),
            ("customers", 4, &["id", "name", "region", "emp_id"][..]),
            ("users", 5, &["id", "name", "email", "status"][..]),
            ("active_users", 6, &["id", "user_id"][..]),
            ("projects", 7, &["id", "name", "dept_id", "status"][..]),
        ] {
            tables.insert_overwrite(test_table(name, oid, columns).into());
        }
        tables
    }

    fn aggregates() -> HashSet<EcoString> {
        [
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "array_agg",
            "string_agg",
            "bool_and",
            "bool_or",
        ]
        .into_iter()
        .map(EcoString::from)
        .collect()
    }

    /// Every query the resolved decorrelation's tests exercise.
    const CORPUS: &[&str] = &[
        "SELECT e.id, e.name FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = e.status)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND o.status = 'active')",
        "SELECT e.id FROM employees e JOIN departments d ON d.id = e.dept_id WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT e.id FROM employees e WHERE EXISTS ( SELECT 1 FROM orders o JOIN customers c ON c.id = o.customer_id WHERE o.emp_id = e.id )",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT d.id, d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
        "SELECT d.id FROM departments d WHERE NOT EXISTS ( SELECT 1 FROM employees e WHERE e.dept_id = d.id AND e.status = 'active' )",
        "SELECT d.id FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id > e.id)",
        "SELECT e.id FROM employees e WHERE e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status)",
        "SELECT e.id, (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count FROM employees e",
        "SELECT e.id FROM employees e WHERE e.status = 'active' OR EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id) AND EXISTS (SELECT 1 FROM departments d WHERE d.name = 'Engineering')",
        "SELECT e.id FROM employees e WHERE e.status = 'active' AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT e.id, (SELECT avg(o.total) FROM orders o WHERE o.emp_id = e.id AND o.status = 'done') AS avg_total FROM employees e",
        "SELECT e.id, (SELECT count(*) FROM orders o WHERE o.emp_id = e.id AND o.status = e.status) AS cnt FROM employees e",
        "SELECT e.id, (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count, (SELECT count(*) FROM projects p WHERE p.dept_id = e.dept_id) AS project_count FROM employees e",
        "SELECT e.id, (SELECT d.name FROM departments d WHERE d.id = e.dept_id) AS dept_name FROM employees e",
        "SELECT e.id, (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS order_count FROM employees e WHERE EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
        "SELECT d.id, d.name FROM departments d WHERE d.budget > (SELECT avg(d2.budget) FROM departments d2 WHERE d2.location = d.location)",
        "SELECT e.id FROM employees e WHERE e.id = (SELECT max(o.emp_id) FROM orders o WHERE o.status = e.status)",
        "SELECT e.id FROM employees e WHERE e.id > (SELECT min(o.emp_id) FROM orders o WHERE o.status = e.status) AND e.id < (SELECT max(o.emp_id) FROM orders o WHERE o.status = e.status)",
        "SELECT e.id FROM employees e WHERE e.name = (SELECT d.name FROM departments d WHERE d.id = e.dept_id)",
        "SELECT e.id, (SELECT count(*) FROM employees e2 WHERE e2.manager_id = e.id) AS report_count FROM employees e",
        "SELECT e.id, (SELECT count(*) FROM orders o WHERE o.emp_id = e.id) AS cnt FROM employees e UNION ALL SELECT d.id, (SELECT count(*) FROM projects p WHERE p.dept_id = d.id) AS cnt FROM departments d",
        "SELECT e.id, e.name FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id IN ( SELECT d.id FROM departments d WHERE d.name = e.name AND d.location = 'NYC' )",
        "SELECT e.id FROM employees e WHERE e.status = 'active' AND e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e JOIN orders o ON o.emp_id = e.id WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name) AND EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT e.id, e.name FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id <> ALL (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id NOT IN ( SELECT d.id FROM departments d WHERE d.name = e.name AND d.location = 'NYC' )",
        "SELECT e.id FROM employees e WHERE e.status = 'active' AND e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e JOIN orders o ON o.emp_id = e.id WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name)",
        "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name) AND e.id IN (SELECT o.emp_id FROM orders o WHERE o.status = e.status) AND EXISTS (SELECT 1 FROM projects p WHERE p.dept_id = e.dept_id)",
        "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.location = 'NYC')",
        "SELECT d.id FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id GROUP BY e.status)",
        "SELECT d.id FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id GROUP BY e.status HAVING count(*) > 5)",
        "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name GROUP BY d.id)",
        "SELECT d.id FROM departments d WHERE d.id NOT IN (SELECT e.dept_id FROM employees e WHERE e.name = d.name GROUP BY e.dept_id HAVING count(*) > 5)",
        "SELECT e.id, (SELECT d.name FROM departments d WHERE d.id = e.dept_id LIMIT 1) AS dept_name FROM employees e",
        "SELECT id, name FROM employees WHERE status = 'active'",
        "SELECT id FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE status = 'active')",
        "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.location = 'NYC')",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id GROUP BY o.status)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id GROUP BY o.status HAVING count(*) > 5)",
        "SELECT e.id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id LIMIT 1)",
        "SELECT d.id, d.name FROM departments d WHERE NOT EXISTS (SELECT 1 FROM employees e WHERE e.dept_id = d.id LIMIT 1)",
        "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name GROUP BY d.id)",
        "SELECT e.id FROM employees e WHERE e.dept_id IN (SELECT d.id FROM departments d WHERE d.name = e.name LIMIT 10)",
        "SELECT e.id FROM employees e WHERE e.dept_id NOT IN (SELECT d.id FROM departments d WHERE d.name = e.name LIMIT 10)",
    ];

    /// Shapes the resolved tests leave out: outer references the rewrite
    /// must qualify once other tables join the FROM, stars, merged
    /// columns, ordering and grouping on the rewritten select.
    const EXTRA_CORPUS: &[&str] = &[
        "SELECT id, name FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE orders.emp_id = employees.id) ORDER BY name",
        "SELECT * FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE orders.emp_id = employees.id AND orders.status = 'open')",
        "SELECT e.* FROM employees e WHERE NOT EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id)",
        "SELECT status, count(*) FROM employees WHERE EXISTS (SELECT 1 FROM orders WHERE orders.emp_id = employees.id) GROUP BY status HAVING count(*) > 1",
        "SELECT id FROM employees JOIN departments USING (id) WHERE EXISTS (SELECT 1 FROM orders WHERE orders.emp_id = employees.id)",
        "SELECT name, (SELECT count(*) FROM orders WHERE orders.emp_id = employees.id) AS orders FROM employees ORDER BY orders DESC",
        "SELECT dept_id FROM employees WHERE dept_id IN (SELECT id FROM departments WHERE departments.name = employees.name) ORDER BY dept_id",
        "SELECT u.id FROM users u WHERE u.id IN (SELECT user_id FROM active_users a WHERE a.id = u.id) UNION SELECT id FROM users WHERE status = 'x'",
        "SELECT id FROM employees e WHERE EXISTS (SELECT 1 FROM orders o WHERE o.emp_id = e.id AND EXISTS (SELECT 1 FROM customers c WHERE c.id = o.customer_id AND c.region = e.status))",
    ];

    fn corpus() -> impl Iterator<Item = &'static str> {
        CORPUS.iter().chain(EXTRA_CORPUS).copied()
    }

    fn bound(sql: &str, catalog: &Catalog) -> ResolvedQuery {
        let query = Arc::new(query_expr_parse(sql).expect("parse"));
        let bindings = query_expr_resolve(&query, catalog, &["public"]).expect("resolve");
        ResolvedQuery::new(query, bindings)
    }

    /// The remapped bindings are the ones the resolver makes for the
    /// rewritten tree.
    #[test]
    fn test_bound_decorrelation_bindings_match_resolver() {
        let catalog = test_tables();
        let aggregates = aggregates();
        for sql in corpus() {
            let Ok(Some(rewritten)) = query_decorrelate(&bound(sql, &catalog), &aggregates) else {
                continue;
            };
            let expected = query_expr_resolve(rewritten.tree(), &catalog, &["public"])
                .unwrap_or_else(|e| panic!("{sql}: the rewritten tree resolves: {e:?}"));
            assert_eq!(
                rewritten.bindings().describe(),
                expected.describe(),
                "{sql}"
            );
        }
    }
}
