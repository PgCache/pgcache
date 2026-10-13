//! One SELECT: scalar subqueries in its select list, then its WHERE
//! conjuncts; EXISTS / NOT EXISTS / IN / NOT IN flatten to a join, a
//! correlated scalar subquery becomes a LEFT JOINed derived table, the
//! rest stays. The plan is decided in one pass and written once.

use ecow::EcoString;
use postgres_types::Type;

use super::Planner;
use smallvec::smallvec;

use super::plan::{
    Bind, DerivedPlan, DerivedRef, GroupPlan, ItemPlan, Piece, Planned, PlannedReplacement,
    ScalarPlan, SelectPlan, WherePlan,
};
use super::rejected;
use super::scope::{
    Correlations, Scope, correlated_subquery, query_is_correlated, scalar_has_correlation,
    where_correlation_partition, where_has_correlation, where_scalar_subquery,
};
use crate::catalog::{ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
use crate::oid::{Oid, TypeOid};
use crate::query::ast::rope::{
    Body, Column, NodeKind, Query, Replacement, Scalar, ScalarKind, Select, SelectItem, Star, View,
    Where, WhereKind,
};
use crate::query::ast::{BinaryOp, JoinType, NodeId, SubLinkType, UnaryOp};
use crate::query::decorrelate::DecorrelateResult;
use crate::query::resolve::{ColumnBinding, ColumnTarget, StarItem};

/// A select-list entry while the SELECT is planned.
enum ItemState<'t> {
    Keep(SelectItem<'t>),
    Derived {
        alias: Option<EcoString>,
        reference: DerivedRef,
    },
}

/// A WHERE conjunct while the SELECT is planned: one of the original or a
/// residual carried in, with the scalar subqueries replaced inside it; or
/// the IS NULL an anti-join adds.
enum WhereState<'t> {
    Conjunct {
        root: Where<'t>,
        extra: Vec<PlannedReplacement>,
    },
    IsNull(ScalarPlan<'t>),
}

impl<'t> WhereState<'t> {
    fn conjunct(root: Where<'t>) -> Self {
        WhereState::Conjunct {
            root,
            extra: Vec::new(),
        }
    }
}

/// The SELECT as rewritten so far: its FROM, the conjuncts still to rejoin
/// as its WHERE, the scope of its FROM, and whether it is DISTINCT.
struct SelectState<'t> {
    from: Vec<Piece<'t>>,
    remaining: Planned<WhereState<'t>>,
    scope: Scope,
    distinct: bool,
    transformed: bool,
}

/// A WHERE conjunct that flattens to a join, with the inner query and (for
/// IN / NOT IN) the test expression.
enum JoinKind<'t> {
    Exists { query: Query<'t> },
    NotExists { query: Query<'t> },
    InAny { query: Query<'t>, test: Scalar<'t> },
    NotInAll { query: Query<'t>, test: Scalar<'t> },
}

/// What a join-family conjunct flattens from, once its inner shape is
/// known to have a join form.
struct JoinInputs<'t> {
    inner: Select<'t>,
    anti: bool,
    test: Option<Scalar<'t>>,
    output: Option<Column<'t>>,
    predicates: Correlations<'t>,
    residual: Planned<Where<'t>>,
}

/// What a scalar subquery with a join form is made of.
struct ScalarInputs<'t> {
    inner: Select<'t>,
    scalar: Scalar<'t>,
    predicates: Correlations<'t>,
    residual: Planned<Where<'t>>,
    has_aggregate: bool,
}

/// What one scalar subquery becomes.
struct ScalarJoin<'t> {
    derived: DerivedPlan,
    condition: WherePlan<'t>,
    scalar: DerivedRef,
}

/// What the residual path does with a conjunct that is not join-family.
enum Residual {
    Keep,
    ScalarDecorrelate,
}

/// `from` LEFT JOINed with the derived table; `None` for an empty FROM.
fn left_join_derived<'t>(from: &mut Vec<Piece<'t>>, join: ScalarJoin<'t>) -> Option<DerivedRef> {
    let left = Piece::cross_fold(std::mem::take(from))?;
    *from = vec![Piece::Join {
        join_type: JoinType::Left,
        left: Box::new(left),
        right: Box::new(Piece::Derived(join.derived)),
        on: Some(join.condition),
    }];
    Some(join.scalar)
}

fn derived_kind(reference: &DerivedRef) -> NodeKind {
    NodeKind::Column {
        table: Some(reference.table.clone()),
        column: reference.column.clone(),
    }
}

fn and<'t>(left: WherePlan<'t>, right: WherePlan<'t>) -> WherePlan<'t> {
    WherePlan::And(Box::new(left), Box::new(right))
}

/// A conjunct kept as it is, unless `reject` names why it cannot be.
fn residual_keep_unless(reject: bool, reason: &str) -> DecorrelateResult<Residual> {
    if reject {
        Err(rejected(reason))
    } else {
        Ok(Residual::Keep)
    }
}

/// A semi-join may drop GROUP BY / HAVING / LIMIT (it can only
/// over-invalidate); an anti-join would under-invalidate, so it keeps
/// nothing it cannot honour.
fn anti_join_inner_check(inner: Select<'_>) -> DecorrelateResult<()> {
    if inner.group_by().len() > 0 {
        return Err(rejected("correlated NOT EXISTS with GROUP BY"));
    }
    if inner.having().is_some() {
        return Err(rejected("correlated NOT EXISTS with HAVING"));
    }
    Ok(())
}

fn join_label(anti: bool, test: bool) -> &'static str {
    match (anti, test) {
        (false, false) => "EXISTS",
        (true, false) => "NOT EXISTS",
        (false, true) => "IN",
        (true, true) => "NOT IN",
    }
}

/// The single output column of an IN subquery, which must be a plain
/// column reference.
fn inner_output_column(inner: Select<'_>) -> DecorrelateResult<Column<'_>> {
    let Some(mut items) = inner.items() else {
        return Err(rejected("IN subquery has no output columns"));
    };
    match (items.next(), items.next()) {
        (Some(SelectItem::Expr(item)), None) => match item.expr().kind() {
            ScalarKind::Column(column) => Ok(column),
            ScalarKind::Function(_)
            | ScalarKind::Literal(_)
            | ScalarKind::Case(_)
            | ScalarKind::Arithmetic(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => Err(rejected(
                "IN subquery output is not a simple column reference",
            )),
        },
        (Some(SelectItem::Star(_)), None) => Err(rejected(
            "IN subquery output is not a simple column reference",
        )),
        (None, _) | (Some(_), Some(_)) => {
            Err(rejected("IN subquery must have exactly one output column"))
        }
    }
}

impl<'t> Planner<'t> {
    /// The plan for `select` with the scope of its rewritten FROM, or
    /// `None` when nothing in it was correlated.
    pub(super) fn select_plan(
        &mut self,
        select: Select<'t>,
    ) -> DecorrelateResult<Option<(SelectPlan<'t>, Scope)>> {
        let mut scope = Scope::empty();
        for source in select.from() {
            scope.subtree_add(self, source.node_ref());
        }
        let mut state = SelectState {
            from: select.from().map(|s| Piece::Keep(s.id())).collect(),
            remaining: Planned::new(),
            scope,
            distinct: select.distinct(),
            transformed: false,
        };
        let mut items = select
            .items()
            .map(|items| items.map(ItemState::Keep).collect::<Vec<_>>());
        if let Some(items) = &mut items {
            self.items_plan(items, &mut state)?;
        }
        if let Some(having) = select.having()
            && where_has_correlation(self, having)
        {
            return Err(rejected("correlated subquery in HAVING clause"));
        }
        for conjunct in select
            .where_clause()
            .map(Where::conjuncts)
            .unwrap_or_default()
        {
            self.conjunct_plan(conjunct, &mut state)?;
        }
        if !state.transformed {
            return Ok(None);
        }
        Ok(Some(self.select_requalified(select, items, state)))
    }

    /// A rewritten SELECT's own clauses written against its new FROM: stars
    /// as the columns they stood for, every reference qualified by its slot,
    /// so the tree still says what it means once other tables sit beside
    /// the ones it named.
    fn select_requalified(
        &mut self,
        select: Select<'t>,
        items: Option<Vec<ItemState<'t>>>,
        state: SelectState<'t>,
    ) -> (SelectPlan<'t>, Scope) {
        let scope = state.scope;
        let items = items.map(|items| self.items_requalified(items, &scope));
        let where_items = state
            .remaining
            .into_iter()
            .map(|conjunct| match conjunct {
                WhereState::Conjunct { root, extra } => {
                    WherePlan::Moved(self.moved(root.node_ref(), &scope, extra))
                }
                WhereState::IsNull(scalar) => WherePlan::IsNull(scalar),
            })
            .collect();
        let group_by = select
            .group_by()
            .map(|column| match self.bindings.column_by_id(column.id()) {
                // An outer-join merged column has no column form; the
                // resolver already forwards such a GROUP BY.
                Some(ColumnTarget::Merged(_)) => GroupPlan::Keep(column.id()),
                Some(ColumnTarget::Column(_) | ColumnTarget::Output) | None => {
                    GroupPlan::Moved(self.moved(column.node_ref(), &scope, Vec::new()))
                }
            })
            .collect();
        let having = select
            .having()
            .map(|having| self.moved(having.node_ref(), &scope, Vec::new()));
        (
            SelectPlan {
                select,
                distinct: state.distinct,
                items,
                from: state.from,
                where_items,
                group_by,
                having,
            },
            scope,
        )
    }

    /// Each correlated scalar subquery of the select list becomes a column
    /// of a LEFT JOINed derived table added to the FROM; any other
    /// correlated expression is rejected.
    fn items_plan(
        &mut self,
        items: &mut [ItemState<'t>],
        state: &mut SelectState<'t>,
    ) -> DecorrelateResult<()> {
        for item in items.iter_mut() {
            let ItemState::Keep(SelectItem::Expr(expr)) = *item else {
                continue;
            };
            let scalar = expr.expr();
            if let ScalarKind::Subquery(subquery) = scalar.kind()
                && query_is_correlated(self, subquery.query())
            {
                let join = self.scalar_subquery_plan(subquery.query(), &state.scope)?;
                if let Some(reference) = left_join_derived(&mut state.from, join) {
                    *item = ItemState::Derived {
                        alias: expr.alias().cloned(),
                        reference,
                    };
                    state.transformed = true;
                }
            } else if scalar_has_correlation(self, scalar) {
                return Err(rejected("correlated subquery nested in SELECT expression"));
            }
        }
        Ok(())
    }

    /// The select list written against the rewritten FROM: stars as the
    /// columns they stood for, every expression re-qualified.
    fn items_requalified(&mut self, items: Vec<ItemState<'t>>, scope: &Scope) -> Vec<ItemPlan<'t>> {
        let mut out = Vec::with_capacity(items.len());
        for state in items {
            match state {
                ItemState::Keep(SelectItem::Expr(expr)) => {
                    out.push(ItemPlan::Moved(self.moved(
                        expr.node_ref(),
                        scope,
                        Vec::new(),
                    )));
                }
                ItemState::Keep(SelectItem::Star(star)) => self.star_items_plan(star, &mut out),
                ItemState::Derived { alias, reference } => {
                    out.push(ItemPlan::Derived { alias, reference });
                }
            }
        }
        out
    }

    /// A star written out as the columns it stood for: a rewrite that
    /// changes the FROM must not change what `*` means.
    fn star_items_plan(&mut self, star: Star<'t>, out: &mut Vec<ItemPlan<'t>>) {
        let bindings = self.bindings;
        for item in bindings.star_by_id(star.id()).unwrap_or(&[]) {
            match item {
                StarItem::Column(binding) => out.push(ItemPlan::Column {
                    replacement: Replacement::Leaf(self.column_kind_for(*binding)),
                    bind: Bind::Column(*binding),
                }),
                StarItem::Merged(id) => {
                    if let Some((replacement, bind, alias)) = self.merged_written(*id) {
                        out.push(ItemPlan::Merged {
                            alias,
                            replacement,
                            bind,
                        });
                    }
                }
            }
        }
    }

    /// One conjunct of the WHERE: flattened to a join, carried as it is, or
    /// carried with its scalar subqueries replaced.
    fn conjunct_plan(
        &mut self,
        conjunct: Where<'t>,
        state: &mut SelectState<'t>,
    ) -> DecorrelateResult<()> {
        if let Some(kind) = self.join_kind_classify(conjunct)? {
            if !self.join_plan(kind, state)? {
                state.remaining.push(WhereState::conjunct(conjunct));
            }
            return Ok(());
        }
        let mut extra = Vec::new();
        if let Residual::ScalarDecorrelate = self.residual_classify(conjunct)? {
            let transformed = self.conjunct_scalar_plan(conjunct, state, &mut extra)?;
            state.transformed |= transformed;
        }
        state.remaining.push(WhereState::Conjunct {
            root: conjunct,
            extra,
        });
        Ok(())
    }

    /// Classify a conjunct: correlated EXISTS / NOT EXISTS / IN / NOT IN
    /// (including the `NOT(EXISTS)` / `NOT(ANY)` spellings); `None` for
    /// anything else; `Err` when an IN / NOT IN lacks its test expression.
    fn join_kind_classify(&self, conjunct: Where<'t>) -> DecorrelateResult<Option<JoinKind<'t>>> {
        let (negated, predicate) = match conjunct.kind() {
            WhereKind::Unary(unary) if unary.op() == UnaryOp::Not => (true, unary.expr()),
            WhereKind::Scalar(_)
            | WhereKind::Unary(_)
            | WhereKind::Binary(_)
            | WhereKind::Multi(_)
            | WhereKind::Subquery(_) => (false, conjunct),
        };
        let Some(subquery) = correlated_subquery(self, predicate) else {
            return Ok(None);
        };
        let query = subquery.query;
        let test = |label: &str| {
            subquery
                .test_expr
                .ok_or_else(|| rejected(&format!("correlated {label} without test expression")))
        };
        Ok(Some(match (negated, subquery.sublink_type) {
            (false, SubLinkType::Exists) => JoinKind::Exists { query },
            (true, SubLinkType::Exists) => JoinKind::NotExists { query },
            (false, SubLinkType::Any) => JoinKind::InAny {
                query,
                test: test("IN")?,
            },
            // `x NOT IN (...)` parses as NOT(ANY); `x <> ALL (...)` is the
            // same predicate spelled as a bare ALL.
            (true, SubLinkType::Any) | (false, SubLinkType::All) => JoinKind::NotInAll {
                query,
                test: test("NOT IN")?,
            },
            (false, SubLinkType::Expr) | (true, SubLinkType::All | SubLinkType::Expr) => {
                return Ok(None);
            }
        }))
    }

    /// Classify a non-join conjunct, rejecting correlation under OR or
    /// under a NOT the join family does not cover.
    fn residual_classify(&self, conjunct: Where<'t>) -> DecorrelateResult<Residual> {
        let correlated = where_has_correlation(self, conjunct);
        match conjunct.kind() {
            WhereKind::Binary(binary) if binary.op() == BinaryOp::Or => {
                residual_keep_unless(correlated, "correlated subquery inside OR")
            }
            WhereKind::Unary(unary) if unary.op() == UnaryOp::Not => residual_keep_unless(
                correlated_subquery(self, unary.expr()).is_some(),
                "correlated NOT-wrapped non-EXISTS subquery",
            ),
            WhereKind::Scalar(_)
            | WhereKind::Unary(_)
            | WhereKind::Binary(_)
            | WhereKind::Multi(_)
            | WhereKind::Subquery(_) => Ok(if correlated {
                Residual::ScalarDecorrelate
            } else {
                Residual::Keep
            }),
        }
    }

    /// Flatten a join-family conjunct into the FROM; `Ok(false)` when the
    /// inner shape has no join form (the conjunct stays).
    fn join_plan(
        &mut self,
        kind: JoinKind<'t>,
        state: &mut SelectState<'t>,
    ) -> DecorrelateResult<bool> {
        let Some(inputs) = self.join_inputs(kind)? else {
            return Ok(false);
        };
        let condition = self.join_condition(&inputs, &state.scope);
        let (on, additions) = if inputs.anti {
            self.anti_join_clauses(&inputs, condition, &state.scope)
        } else {
            let residual = inputs
                .residual
                .iter()
                .map(|conjunct| WhereState::conjunct(*conjunct))
                .collect();
            (condition, residual)
        };
        let inner_pieces: Vec<Piece<'t>> = inputs
            .inner
            .from()
            .map(|source| Piece::Moved(self.moved(source.node_ref(), &state.scope, Vec::new())))
            .collect();
        let Some(right) = Piece::cross_fold(inner_pieces) else {
            return Ok(false);
        };
        let Some(left) = Piece::cross_fold(std::mem::take(&mut state.from)) else {
            return Ok(false);
        };
        state.from = vec![Piece::Join {
            join_type: if inputs.anti {
                JoinType::Left
            } else {
                JoinType::Inner
            },
            left: Box::new(left),
            right: Box::new(right),
            on: Some(on),
        }];
        state.distinct = state.distinct || !inputs.anti;
        for source in inputs.inner.from() {
            state.scope.subtree_add(self, source.node_ref());
        }
        state.remaining.extend(additions);
        state.transformed = true;
        Ok(true)
    }

    /// What a join-family conjunct flattens from; `None` when the inner
    /// shape has no join form, `Err` when it has one it cannot honour.
    fn join_inputs(&self, kind: JoinKind<'t>) -> DecorrelateResult<Option<JoinInputs<'t>>> {
        let (query, anti, test) = match kind {
            JoinKind::Exists { query } => (query, false, None),
            JoinKind::NotExists { query } => (query, true, None),
            JoinKind::InAny { query, test } => (query, false, Some(test)),
            JoinKind::NotInAll { query, test } => (query, true, Some(test)),
        };
        let Body::Select(inner) = query.body() else {
            return Ok(None);
        };
        if anti {
            anti_join_inner_check(inner)?;
        }
        let Some(inner_where) = inner.where_clause() else {
            return Ok(None);
        };
        let output = match test {
            Some(_) => Some(inner_output_column(inner)?),
            None => None,
        };
        let inner_scope = Scope::of_subtree(self, query.node_ref());
        let label = join_label(anti, test.is_some());
        let (predicates, residual) =
            where_correlation_partition(self, &inner_scope, inner_where)
                .ok_or_else(|| rejected(&format!("unsupported correlation pattern in {label}")))?;
        Ok(Some(JoinInputs {
            inner,
            anti,
            test,
            output,
            predicates,
            residual,
        }))
    }

    /// The join condition: the correlation predicates, and for IN / NOT IN
    /// the test expression against the inner output column.
    fn join_condition(&mut self, inputs: &JoinInputs<'t>, destination: &Scope) -> WherePlan<'t> {
        let correlation = self.correlations_condition(&inputs.predicates, destination);
        match (inputs.test, inputs.output) {
            (Some(test), Some(output)) => and(
                WherePlan::Equal(
                    self.moved_scalar(test.node_ref(), destination),
                    self.moved_scalar(output.node_ref(), destination),
                ),
                correlation,
            ),
            _ => correlation,
        }
    }

    /// An anti-join's ON and the IS NULL its WHERE gains: residual inner
    /// predicates go into the ON clause to keep the LEFT JOIN's NULL-padded
    /// rows; the IS NULL on an inner correlation column (or the IN output)
    /// filters to them.
    fn anti_join_clauses(
        &mut self,
        inputs: &JoinInputs<'t>,
        condition: WherePlan<'t>,
        destination: &Scope,
    ) -> (WherePlan<'t>, Planned<WhereState<'t>>) {
        let residual: Vec<WherePlan<'t>> = inputs
            .residual
            .iter()
            .map(|conjunct| {
                WherePlan::Moved(self.moved(conjunct.node_ref(), destination, Vec::new()))
            })
            .collect();
        let null_test = inputs.output.unwrap_or(inputs.predicates.first.inner);
        let is_null = WhereState::IsNull(self.moved_scalar(null_test.node_ref(), destination));
        let on = match WherePlan::and_fold(residual) {
            Some(residual) => and(condition, residual),
            None => condition,
        };
        (on, smallvec![is_null])
    }

    fn moved_scalar(
        &mut self,
        root: crate::query::ast::rope::NodeRef<'t>,
        destination: &Scope,
    ) -> ScalarPlan<'t> {
        ScalarPlan::Moved(self.moved(root, destination, Vec::new()))
    }

    /// `inner = outer` for every correlation predicate, AND-folded.
    fn correlations_condition(
        &mut self,
        predicates: &Correlations<'t>,
        destination: &Scope,
    ) -> WherePlan<'t> {
        let mut condition = None;
        for predicate in predicates.iter() {
            let equality = WherePlan::Equal(
                self.moved_scalar(predicate.inner.node_ref(), destination),
                self.moved_scalar(predicate.outer.node_ref(), destination),
            );
            condition = Some(match condition {
                Some(acc) => and(acc, equality),
                None => equality,
            });
        }
        condition.unwrap_or_else(|| {
            WherePlan::Equal(
                self.moved_scalar(predicates.first.inner.node_ref(), destination),
                self.moved_scalar(predicates.first.outer.node_ref(), destination),
            )
        })
    }

    /// A conjunct with each correlated scalar subquery replaced by a
    /// reference to a LEFT JOINed derived table added to the FROM.
    fn conjunct_scalar_plan(
        &mut self,
        expr: Where<'t>,
        state: &mut SelectState<'t>,
        extra: &mut Vec<PlannedReplacement>,
    ) -> DecorrelateResult<bool> {
        if let Some(subquery) = where_scalar_subquery(expr)
            && query_is_correlated(self, subquery.query())
        {
            let join = self.scalar_subquery_plan(subquery.query(), &state.scope)?;
            let Some(reference) = left_join_derived(&mut state.from, join) else {
                return Ok(false);
            };
            extra.push(PlannedReplacement {
                at: expr.id(),
                replacement: Replacement::Leaf(derived_kind(&reference)),
                bind: Some(Bind::Derived {
                    derived: reference.derived,
                    position: reference.position,
                }),
            });
            return Ok(true);
        }
        match expr.kind() {
            WhereKind::Binary(binary) => {
                let left = self.conjunct_scalar_plan(binary.lexpr(), state, extra)?;
                let right = self.conjunct_scalar_plan(binary.rexpr(), state, extra)?;
                Ok(left || right)
            }
            WhereKind::Unary(unary) => self.conjunct_scalar_plan(unary.expr(), state, extra),
            WhereKind::Scalar(_) | WhereKind::Multi(_) | WhereKind::Subquery(_) => Ok(false),
        }
    }

    /// The binding an inner correlation column refers to: its own, or the
    /// left column of a merged inner-join column.
    fn inner_binding(&self, column: Column<'t>) -> Option<ColumnBinding> {
        match self.bindings.column_by_id(column.id())? {
            ColumnTarget::Column(binding) => Some(*binding),
            ColumnTarget::Merged(id) => self
                .bindings
                .merged(*id)
                .filter(|merged| !merged.outer)
                .map(|merged| merged.left),
            ColumnTarget::Output => None,
        }
    }

    /// A correlated scalar subquery as a LEFT JOINed derived table
    /// (ADR-015): `_dcN` selects the correlation key columns and the scalar
    /// as `_dsN`, grouped by the keys when the scalar aggregates; the
    /// outer reference becomes `_dcN._dsN`.
    fn scalar_subquery_plan(
        &mut self,
        query: Query<'t>,
        destination: &Scope,
    ) -> DecorrelateResult<ScalarJoin<'t>> {
        let inputs = self.scalar_inputs(query)?;
        let derived_alias = self.next_derived_alias();
        let scalar_alias = self.next_scalar_alias();
        let (derived, keys, scalar_position) =
            self.derived_table_push(&inputs, &derived_alias, &scalar_alias)?;
        let condition =
            self.derived_condition((derived, &derived_alias), &inputs.predicates, destination)?;
        Ok(ScalarJoin {
            derived: DerivedPlan {
                index: derived,
                alias: derived_alias.clone(),
                keys,
                scalar: inputs.scalar.id(),
                scalar_alias: scalar_alias.clone(),
                from: inputs.inner.from().map(|source| source.id()).collect(),
                residual: inputs
                    .residual
                    .iter()
                    .map(|conjunct| conjunct.id())
                    .collect(),
                grouped: inputs.has_aggregate,
            },
            condition,
            scalar: DerivedRef {
                derived,
                position: scalar_position.index(),
                table: derived_alias,
                column: scalar_alias,
            },
        })
    }

    /// What a scalar subquery is made of, or why it has no join form.
    fn scalar_inputs(&self, query: Query<'t>) -> DecorrelateResult<ScalarInputs<'t>> {
        let Body::Select(inner) = query.body() else {
            return Err(rejected("scalar subquery is not a simple SELECT"));
        };
        let mut items = inner.items().into_iter().flatten();
        let (Some(SelectItem::Expr(item)), None) = (items.next(), items.next()) else {
            return Err(rejected(
                "scalar subquery must have exactly one output column",
            ));
        };
        let scalar = item.expr();
        let inner_where = inner
            .where_clause()
            .ok_or_else(|| rejected("scalar subquery has no WHERE clause"))?;
        let has_aggregate = scalar.has_aggregate(self.aggregate_functions);
        if !has_aggregate && query.has_limit() {
            return Err(rejected("non-aggregate scalar subquery with LIMIT"));
        }
        let inner_scope = Scope::of_subtree(self, query.node_ref());
        let (predicates, residual) =
            where_correlation_partition(self, &inner_scope, inner_where)
                .ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))?;
        Ok(ScalarInputs {
            inner,
            scalar,
            predicates,
            residual,
            has_aggregate,
        })
    }

    /// The derived table's metadata, registered: one column per correlation
    /// key (the inner column as it is) and the scalar as text. Returns its
    /// index, the key column nodes and the scalar's position.
    fn derived_table_push(
        &mut self,
        inputs: &ScalarInputs<'t>,
        derived_alias: &EcoString,
        scalar_alias: &EcoString,
    ) -> DecorrelateResult<(usize, Planned<NodeId>, ColumnPosition)> {
        let mut metadata = Vec::with_capacity(inputs.predicates.len() + 1);
        let mut keys = Planned::with_capacity(inputs.predicates.len());
        for (position, predicate) in inputs.predicates.iter().enumerate() {
            let binding = self
                .inner_binding(predicate.inner)
                .ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))?;
            let column = self
                .bindings
                .binding_column(&binding)
                .ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))?;
            let position = ColumnPosition::from_index(position)
                .ok_or_else(|| rejected("scalar subquery has too many correlation columns"))?;
            metadata.push(ColumnMetadata {
                position,
                ..column.clone()
            });
            keys.push(predicate.inner.id());
        }
        let scalar_position = ColumnPosition::from_index(keys.len())
            .ok_or_else(|| rejected("scalar subquery has too many correlation columns"))?;
        metadata.push(ColumnMetadata {
            name: scalar_alias.clone(),
            position: scalar_position,
            type_oid: TypeOid::from_raw(25),
            data_type: Type::TEXT,
            type_name: EcoString::from("text"),
            cache_type_name: EcoString::from("text"),
            is_primary_key: false,
        });
        let derived = self.derived_push(TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(0),
            name: derived_alias.clone(),
            schema: EcoString::new(),
            primary_key_columns: Vec::new(),
            columns: ColumnStore::new(metadata),
            indexes: Vec::new(),
        });
        Ok((derived, keys, scalar_position))
    }

    /// `_dcN.key = outer` for every correlation predicate.
    fn derived_condition(
        &mut self,
        (derived, derived_alias): (usize, &EcoString),
        predicates: &Correlations<'t>,
        destination: &Scope,
    ) -> DecorrelateResult<WherePlan<'t>> {
        let mut condition = None;
        for (position, predicate) in predicates.iter().enumerate() {
            let key = DerivedRef {
                derived,
                position,
                table: derived_alias.clone(),
                column: predicate.inner.column().clone(),
            };
            let equality = WherePlan::Equal(
                ScalarPlan::Derived(key),
                self.moved_scalar(predicate.outer.node_ref(), destination),
            );
            condition = Some(match condition {
                Some(acc) => and(acc, equality),
                None => equality,
            });
        }
        condition.ok_or_else(|| rejected("unsupported correlation pattern in scalar subquery"))
    }
}
