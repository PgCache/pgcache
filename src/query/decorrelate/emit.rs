//! The plan written as nodes: kept pieces as table entries over the base,
//! carried pieces as copies, everything else fresh. Fresh references and
//! derived tables report where they landed so the entry binds them.

use super::plan::{
    Bind, BodyPlan, DerivedPlan, DerivedRef, GroupPlan, ItemPlan, Moved, Piece, Planned, QueryPlan,
    ScalarPlan, SelectPlan, WherePlan,
};
use crate::cache::SubqueryKind;
use crate::query::ast::rope::{
    Body, JoinQualKind, NodeKind, Query, QueryTree, Remap, Replacement, Replacements, SetOp,
    TreeBuilder, View,
};
use crate::query::ast::{BinaryOp, NodeId, UnaryOp};

pub(super) struct Emitter<'t> {
    builder: TreeBuilder<'t>,
    /// Fresh references by the position they landed at.
    binds: Planned<(NodeId, Bind)>,
    /// Each derived table's index and the position of its FROM node.
    derived_nodes: Planned<(usize, NodeId)>,
}

fn count(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

impl<'t> Emitter<'t> {
    pub(super) fn new(base: &'t QueryTree) -> Self {
        Self {
            builder: TreeBuilder::over(base),
            binds: Planned::new(),
            derived_nodes: Planned::new(),
        }
    }

    #[allow(clippy::type_complexity)]
    pub(super) fn finish(
        self,
    ) -> (
        QueryTree,
        Remap,
        Planned<(NodeId, Bind)>,
        Planned<(usize, NodeId)>,
    ) {
        let (tree, remap) = self.builder.finish();
        (tree, remap, self.binds, self.derived_nodes)
    }

    pub(super) fn query_emit(&mut self, plan: &QueryPlan<'t>) {
        let query = plan.query;
        let open = self
            .builder
            .open_from(query.node_ref().kind().clone(), query.id());
        for cte in query.ctes() {
            self.builder.keep(cte.id());
        }
        match (&plan.body, query.body()) {
            (BodyPlan::Select(select), _) => self.select_emit(select),
            (BodyPlan::SetOp(left, right), Body::SetOp(set_op)) => {
                self.set_op_emit(set_op, left.as_deref(), right.as_deref());
            }
            (BodyPlan::SetOp(..), body) => {
                debug_assert!(false, "a set-operation plan is for a set-operation body");
                self.builder.keep(body.id());
            }
        }
        self.order_by_emit(query, plan.order_by.as_deref());
        if let Some(limit) = query.limit_count() {
            self.builder.keep(limit.id());
        }
        if let Some(offset) = query.limit_offset() {
            self.builder.keep(offset.id());
        }
        self.builder.close(open);
    }

    /// A set operation with each branch rewritten by its plan or kept.
    fn set_op_emit(
        &mut self,
        set_op: SetOp<'t>,
        left: Option<&QueryPlan<'t>>,
        right: Option<&QueryPlan<'t>>,
    ) {
        let open = self
            .builder
            .open_from(set_op.node_ref().kind().clone(), set_op.id());
        for (plan, branch) in [(left, set_op.left()), (right, set_op.right())] {
            match plan {
                Some(plan) => self.query_emit(plan),
                None => {
                    self.builder.keep(branch.id());
                }
            }
        }
        self.builder.close(open);
    }

    /// The ORDER BY clauses as moved, or kept.
    fn order_by_emit(&mut self, query: Query<'t>, moved: Option<&[Moved<'t>]>) {
        match moved {
            Some(moved) => {
                for clause in moved {
                    self.moved_emit(clause);
                }
            }
            None => {
                for clause in query.order_by() {
                    self.builder.keep(clause.id());
                }
            }
        }
    }

    fn select_emit(&mut self, plan: &SelectPlan<'t>) {
        let kind = NodeKind::Select {
            distinct: plan.distinct,
            item_count: plan.items.as_ref().map(|items| count(items.len())),
            from_count: count(plan.from.len()),
            has_where: !plan.where_items.is_empty(),
            group_by_count: count(plan.group_by.len()),
            has_having: plan.having.is_some(),
        };
        let open = self.builder.open_from(kind, plan.select.id());
        for item in plan.items.iter().flatten() {
            self.item_emit(item);
        }
        for piece in &plan.from {
            self.piece_emit(piece);
        }
        self.where_chain_emit(&plan.where_items);
        for group in &plan.group_by {
            match group {
                GroupPlan::Keep(id) => {
                    self.builder.keep(*id);
                }
                GroupPlan::Moved(moved) => self.moved_emit(moved),
            }
        }
        if let Some(having) = &plan.having {
            self.moved_emit(having);
        }
        self.builder.close(open);
    }

    fn item_emit(&mut self, item: &ItemPlan<'t>) {
        match item {
            ItemPlan::Moved(moved) => self.moved_emit(moved),
            ItemPlan::Derived { alias, reference } => {
                let open = self.builder.open(NodeKind::SelectExpr {
                    alias: alias.clone(),
                });
                self.derived_ref_emit(reference);
                self.builder.close(open);
            }
            ItemPlan::Column { replacement, bind } => {
                let open = self.builder.open(NodeKind::SelectExpr { alias: None });
                self.fresh_emit(replacement, bind);
                self.builder.close(open);
            }
            ItemPlan::Merged {
                alias,
                replacement,
                bind,
            } => {
                let open = self.builder.open(NodeKind::SelectExpr {
                    alias: Some(alias.clone()),
                });
                self.fresh_emit(replacement, bind);
                self.builder.close(open);
            }
        }
    }

    /// A fresh leaf or fragment outside a copy, bound where it lands.
    fn fresh_emit(&mut self, replacement: &Replacement, bind: &Bind) {
        let placed = match replacement {
            Replacement::Leaf(kind) | Replacement::LeafFrom(kind) => {
                self.builder.leaf(kind.clone())
            }
            Replacement::Fragment(nodes) => self.builder.fragment(nodes),
            Replacement::Subtree(id) => self.builder.keep(*id),
        };
        self.binds.push((placed, bind.clone()));
    }

    fn piece_emit(&mut self, piece: &Piece<'t>) {
        match piece {
            Piece::Keep(id) => {
                self.builder.keep(*id);
            }
            Piece::Moved(moved) => self.moved_emit(moved),
            Piece::Join {
                join_type,
                left,
                right,
                on,
            } => {
                let open = self.builder.open(NodeKind::Join {
                    join_type: *join_type,
                    qual: if on.is_some() {
                        JoinQualKind::On
                    } else {
                        JoinQualKind::Cross
                    },
                });
                self.piece_emit(left);
                self.piece_emit(right);
                if let Some(on) = on {
                    self.where_emit(on);
                }
                self.builder.close(open);
            }
            Piece::Derived(plan) => self.derived_emit(plan),
        }
    }

    /// `(SELECT keys…, scalar AS _dsN FROM inner WHERE residual GROUP BY
    /// keys) AS _dcN`.
    fn derived_emit(&mut self, plan: &DerivedPlan) {
        let subquery = self.builder.open(NodeKind::Subquery {
            lateral: false,
            alias: Some(plan.alias.clone()),
            alias_column_count: 0,
            kind: SubqueryKind::Scalar,
        });
        let query = self.builder.open(NodeKind::Query {
            cte_count: 0,
            order_by_count: 0,
            has_limit_count: false,
            has_limit_offset: false,
        });
        let select = self.builder.open(NodeKind::Select {
            distinct: false,
            item_count: Some(count(plan.keys.len() + 1)),
            from_count: count(plan.from.len()),
            has_where: !plan.residual.is_empty(),
            group_by_count: if plan.grouped {
                count(plan.keys.len())
            } else {
                0
            },
            has_having: false,
        });
        for key in &plan.keys {
            let item = self.builder.open(NodeKind::SelectExpr { alias: None });
            self.builder.keep(*key);
            self.builder.close(item);
        }
        let item = self.builder.open(NodeKind::SelectExpr {
            alias: Some(plan.scalar_alias.clone()),
        });
        self.builder.keep(plan.scalar);
        self.builder.close(item);
        for source in &plan.from {
            self.builder.keep(*source);
        }
        let residual: Vec<WherePlan<'t>> = plan
            .residual
            .iter()
            .map(|id| WherePlan::Keep(*id))
            .collect();
        self.where_chain_emit(&residual);
        if plan.grouped {
            for key in &plan.keys {
                self.builder.keep(*key);
            }
        }
        self.builder.close(select);
        self.builder.close(query);
        let placed = self.builder.close(subquery);
        self.derived_nodes.push((plan.index, placed));
    }

    /// `items` as a left-associated AND chain; nothing for no items.
    fn where_chain_emit(&mut self, items: &[WherePlan<'t>]) {
        let Some((last, init)) = items.split_last() else {
            return;
        };
        if init.is_empty() {
            self.where_emit(last);
            return;
        }
        let open = self.builder.open(NodeKind::Binary { op: BinaryOp::And });
        self.where_chain_emit(init);
        self.where_emit(last);
        self.builder.close(open);
    }

    fn where_emit(&mut self, plan: &WherePlan<'t>) {
        match plan {
            WherePlan::Keep(id) => {
                self.builder.keep(*id);
            }
            WherePlan::Moved(moved) => self.moved_emit(moved),
            WherePlan::Equal(left, right) => {
                let open = self.builder.open(NodeKind::Binary {
                    op: BinaryOp::Equal,
                });
                self.scalar_emit(left);
                self.scalar_emit(right);
                self.builder.close(open);
            }
            WherePlan::IsNull(scalar) => {
                let open = self.builder.open(NodeKind::Unary {
                    op: UnaryOp::IsNull,
                });
                self.scalar_emit(scalar);
                self.builder.close(open);
            }
            WherePlan::And(left, right) => {
                let open = self.builder.open(NodeKind::Binary { op: BinaryOp::And });
                self.where_emit(left);
                self.where_emit(right);
                self.builder.close(open);
            }
        }
    }

    fn scalar_emit(&mut self, plan: &ScalarPlan<'t>) {
        match plan {
            ScalarPlan::Moved(moved) => self.moved_emit(moved),
            ScalarPlan::Derived(reference) => self.derived_ref_emit(reference),
        }
    }

    fn derived_ref_emit(&mut self, reference: &DerivedRef) {
        let placed = self.builder.leaf(NodeKind::Column {
            table: Some(reference.table.clone()),
            column: reference.column.clone(),
        });
        self.binds.push((
            placed,
            Bind::Derived {
                derived: reference.derived,
                position: reference.position,
            },
        ));
    }

    /// A carried piece: a copy of its subtree with the planned
    /// replacements, each bound where it landed.
    fn moved_emit(&mut self, moved: &Moved<'t>) {
        let (mut replacements, binds) = replacements_planned(moved);
        self.builder.copy(moved.root, &mut replacements);
        for (index, bind) in binds {
            match replacements.placed(index) {
                Some(placed) => self.binds.push((placed, bind)),
                None => debug_assert!(false, "a planned replacement is under the copied piece"),
            }
        }
    }
}

/// A moved piece's replacements for the copier, and which of them bind,
/// by replacement index.
fn replacements_planned(moved: &Moved<'_>) -> (Replacements, Planned<(usize, Bind)>) {
    let mut replacements = Replacements::new();
    let mut binds = Planned::new();
    for planned in &moved.replacements {
        let index = replacements.push(planned.at, planned.replacement.clone());
        if let Some(bind) = &planned.bind {
            binds.push((index, bind.clone()));
        }
    }
    (replacements, binds)
}
