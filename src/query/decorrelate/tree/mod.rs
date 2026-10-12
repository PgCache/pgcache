//! Decorrelation on the rope (ADR-014 to ADR-017, ADR-057): every
//! correlated subquery becomes a join, planned over views and built over
//! the base tree. A planner decides what each rewritten SELECT holds (the
//! owned rewrite's flatten-one-conjunct-at-a-time result, as one plan),
//! then an emitter writes the plan in pre-order: untouched subtrees kept
//! whole, carried pieces as copies with their references re-qualified,
//! fresh nodes for the joins, the `_dcN` derived tables and the references
//! the rewrite makes, bound by the position they land at.
//!
//! * [`scope`] — what a subquery can see, and how a piece of it is carried.
//! * [`plan`] — the plan the planner builds and the emitter writes.
//! * [`select`] — one SELECT: scalar subqueries in its select list, then its
//!   WHERE conjuncts as semi-, anti- and derived-table joins.
//! * [`emit`] — the plan as nodes.

mod emit;
mod plan;
mod scope;
mod select;

use std::collections::HashSet;
use std::sync::Arc;

use ecow::EcoString;
use rootcause::Report;

use self::emit::Emitter;
use self::plan::{Bind, BodyPlan, QueryPlan};
use super::{DecorrelateError, DecorrelateResult};
use crate::catalog::{ColumnPosition, TableMetadata};
use crate::query::ast::rope::{Body, Query, QueryTree, Remap, View};
use crate::query::ast::{NodeId, NodeRemap};
use crate::query::resolve::{
    Bindings, BindingsBuilder, ColumnBinding, ColumnTarget, ResolvedQuery, SlotId, TableSlot,
};

/// The rewrite's state: the query it reads facts from, and what it has
/// made so far.
pub(super) struct Planner<'t> {
    pub(super) bindings: &'t Bindings,
    pub(super) aggregate_functions: &'t HashSet<EcoString>,
    derived_counter: u32,
    scalar_counter: u32,
    /// The metadata of each derived table the rewrite makes, in order.
    derived: Vec<Arc<TableMetadata>>,
    /// Column nodes carried into the scope they referred to, by base id:
    /// no longer correlated.
    uncorrelated: Vec<NodeId>,
}

/// Decorrelate every correlated subquery of `bound`. `Ok(None)` when it has
/// none; `Err` when one sits where no join form exists.
pub fn query_decorrelate_tree(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
) -> DecorrelateResult<Option<(QueryTree, Bindings)>> {
    let mut planner = Planner {
        bindings: bound.rope_bindings(),
        aggregate_functions,
        derived_counter: 0,
        scalar_counter: 0,
        derived: Vec::new(),
        uncorrelated: Vec::new(),
    };
    let Some(plan) = planner.query_plan(bound.rope().root())? else {
        return Ok(None);
    };
    let mut emitter = Emitter::new(bound.rope());
    emitter.query_emit(&plan);
    let (tree, remap, binds, derived_nodes) = emitter.finish();
    let bindings = bindings_build(
        &planner,
        bound.rope_bindings(),
        &remap,
        binds,
        &derived_nodes,
    )?;
    Ok(Some((tree, bindings)))
}

/// The rewritten tree's bindings: the base's carried to where their nodes
/// landed, a slot per derived table, the fresh references bound, and the
/// references carried beside their table no longer correlated.
fn bindings_build(
    planner: &Planner<'_>,
    base: &Bindings,
    remap: &Remap,
    binds: Vec<(NodeId, Bind)>,
    derived_nodes: &[(usize, NodeId)],
) -> DecorrelateResult<Bindings> {
    let mut builder = BindingsBuilder::from_remapped(base, remap);
    let mut slots = Vec::with_capacity(planner.derived.len());
    for (index, table) in planner.derived.iter().enumerate() {
        let Some(&(_, node)) = derived_nodes.iter().find(|(placed, _)| *placed == index) else {
            return Err(unplaced("a derived table"));
        };
        slots.push(builder.slot_push(
            node,
            TableSlot::Derived {
                node,
                table: Arc::clone(table),
            },
        ));
    }
    for (node, bind) in binds {
        bind_apply(&mut builder, &slots, node, bind)?;
    }
    for old in &planner.uncorrelated {
        for node in remap.new_ids(*old) {
            builder.correlated_remove(node);
        }
    }
    Ok(builder.finish())
}

fn bind_apply(
    builder: &mut BindingsBuilder,
    slots: &[SlotId],
    node: NodeId,
    bind: Bind,
) -> DecorrelateResult<()> {
    match bind {
        Bind::Column(binding) => {
            builder.column_bind(node, ColumnTarget::Column(binding), false);
        }
        Bind::Derived { derived, position } => {
            let binding = ColumnBinding {
                slot: *slots
                    .get(derived)
                    .ok_or_else(|| unplaced("a derived table"))?,
                position: ColumnPosition::from_index(position)
                    .ok_or_else(|| unplaced("a derived column"))?,
            };
            builder.column_bind(node, ColumnTarget::Column(binding), false);
        }
        Bind::Merged { left, right } => {
            let at = |offset: u32| {
                NodeId::from_index(u32::try_from(node.index()).unwrap_or(u32::MAX) + offset)
            };
            builder.column_bind(at(1), ColumnTarget::Column(left), false);
            if let Some(right) = right {
                builder.column_bind(at(2), ColumnTarget::Column(right), false);
            }
        }
    }
    Ok(())
}

/// The rewrite built a node it then did not place: a defect, reported as
/// non-decorrelatable so the query is cached in its correlated form.
fn unplaced(what: &str) -> Report<DecorrelateError> {
    debug_assert!(false, "decorrelation made {what} it did not place");
    Report::from(DecorrelateError::NonDecorrelatable {
        reason: format!("decorrelation made {what} it did not place"),
    })
}

pub(super) fn rejected(reason: &str) -> Report<DecorrelateError> {
    Report::from(DecorrelateError::NonDecorrelatable {
        reason: reason.to_owned(),
    })
}

impl<'t> Planner<'t> {
    pub(super) fn next_derived_alias(&mut self) -> EcoString {
        self.derived_counter += 1;
        EcoString::from(format!("_dc{}", self.derived_counter))
    }

    pub(super) fn next_scalar_alias(&mut self) -> EcoString {
        self.scalar_counter += 1;
        EcoString::from(format!("_ds{}", self.scalar_counter))
    }

    /// Register a derived table the rewrite makes; returns its index.
    pub(super) fn derived_push(&mut self, table: TableMetadata) -> usize {
        self.derived.push(Arc::new(table));
        self.derived.len() - 1
    }

    fn query_plan(&mut self, query: Query<'t>) -> DecorrelateResult<Option<QueryPlan<'t>>> {
        match query.body() {
            Body::Select(select) => {
                let Some((plan, scope)) = self.select_plan(select)? else {
                    return Ok(None);
                };
                let order_by = query
                    .order_by()
                    .map(|clause| self.moved(clause.node_ref(), &scope, Vec::new()))
                    .collect();
                Ok(Some(QueryPlan {
                    query,
                    body: BodyPlan::Select(plan),
                    order_by: Some(order_by),
                }))
            }
            Body::SetOp(set_op) => {
                let left = self.query_plan(set_op.left())?;
                let right = self.query_plan(set_op.right())?;
                if left.is_none() && right.is_none() {
                    return Ok(None);
                }
                Ok(Some(QueryPlan {
                    query,
                    body: BodyPlan::SetOp(left.map(Box::new), right.map(Box::new)),
                    order_by: None,
                }))
            }
            Body::Values(_) => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::query_decorrelate_tree;
    use crate::query::decorrelate::bound::tests::{aggregates, bound, corpus, test_tables};
    use crate::query::decorrelate::query_decorrelate;
    use crate::query::resolve::{query_tree_resolve, query_tree_sql};

    fn outcome_text<E>(outcome: Result<bool, E>) -> &'static str {
        match outcome {
            Ok(true) => "rewritten",
            Ok(false) => "unchanged",
            Err(_) => "rejected",
        }
    }

    /// The rope rewrite rewrites what the owned rewrite rewrites, renders
    /// the same SQL and rejects with the same reason; its carried bindings
    /// are the ones the resolver makes for the rewritten tree.
    #[test]
    fn test_rope_decorrelation_matches_the_owned_rewrite() {
        let catalog = test_tables();
        let aggregates = aggregates();
        let (mut rewritten, mut unchanged, mut rejected) = (0, 0, 0);
        for sql in corpus() {
            let bound = bound(sql, &catalog);
            let owned = query_decorrelate(&bound, &aggregates);
            let rope = query_decorrelate_tree(&bound, &aggregates);
            match (owned, rope) {
                (Ok(Some(owned)), Ok(Some((tree, bindings)))) => {
                    rewritten += 1;
                    assert_eq!(query_tree_sql(&tree, &bindings), owned.sql(), "{sql}");
                    let resolved = query_tree_resolve(&tree, &catalog, &["public"])
                        .unwrap_or_else(|e| panic!("{sql}: the decorrelated rope resolves: {e:?}"));
                    let mut carried = bindings.describe();
                    let mut expected = resolved.describe();
                    carried.sort();
                    expected.sort();
                    assert_eq!(carried, expected, "{sql}");
                }
                (Ok(None), Ok(None)) => unchanged += 1,
                (Err(owned), Err(rope)) => {
                    rejected += 1;
                    assert_eq!(
                        rope.current_context().to_string(),
                        owned.current_context().to_string(),
                        "{sql}"
                    );
                }
                (owned, rope) => panic!(
                    "{sql}: owned {} vs rope {}",
                    outcome_text(owned.as_ref().map(Option::is_some)),
                    outcome_text(rope.as_ref().map(Option::is_some))
                ),
            }
        }
        assert!(
            rewritten > 0 && unchanged > 0 && rejected > 0,
            "the corpus exercises every outcome: {rewritten} rewritten, {unchanged} unchanged, \
             {rejected} rejected"
        );
    }
}
