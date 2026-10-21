//! One SELECT branch's FROM tree and predicates, walked into a
//! [`QueryBranchPredicates`].

use std::collections::HashSet;

use super::conjunct::{ConjunctPart, conjunct_parts_collect};
use super::{
    Conjunct, ConjunctForm, ConjunctScope, FromJoin, JoinId, JoinKind, JoinPathStep, JoinSide,
    QueryBranchPredicates, TableReference,
};
use crate::query::ast::JoinType;
use crate::query::ast::rope::{Join, JoinQual, NodeKind, Select, Source, Where};
use crate::query::resolve::{Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, SlotId};

/// The predicates of `select`, a SELECT of `bound`'s rope.
pub fn query_branch_predicates_build(
    bound: &ResolvedQuery,
    select: Select<'_>,
) -> QueryBranchPredicates {
    let mut builder = Builder {
        bound,
        table_references: Vec::new(),
        joins: Vec::new(),
        join_views: Vec::new(),
    };
    let entries: Vec<Source<'_>> = select.from().collect();
    builder.comma_list_walk(&entries, &mut Vec::new());
    let mut branch = QueryBranchPredicates {
        table_references: builder.table_references,
        joins: builder.joins,
        conjuncts: Vec::new(),
        reference_predicates: Vec::new(),
    };
    let mut conjuncts = Vec::new();
    if let Some(where_clause) = select.where_clause() {
        conjuncts_from_where(
            bound,
            &branch,
            ConjunctScope::Where,
            where_clause,
            &mut conjuncts,
        );
    }
    for &(id, join) in &builder.join_views {
        conjuncts_from_join(bound, &branch, id, join, &mut conjuncts);
    }
    branch.conjuncts = conjuncts;
    branch.joins_reduce();
    branch.reference_predicates = branch.reference_predicates_derive(bound.bindings());
    branch
}

struct Builder<'b, 't> {
    bound: &'b ResolvedQuery,
    table_references: Vec<TableReference>,
    joins: Vec<FromJoin>,
    /// The join node behind each of `joins` but the comma joins.
    join_views: Vec<(JoinId, Join<'t>)>,
}

impl<'t> Builder<'_, 't> {
    fn join_push(&mut self, view: Option<Join<'t>>, kind: JoinKind) -> Option<JoinId> {
        let id = JoinId(u32::try_from(self.joins.len()).ok()?);
        self.joins.push(FromJoin {
            node: view.map(|join| join.id()),
            kind,
            reduced: false,
            left: Vec::new(),
            right: Vec::new(),
        });
        self.join_views.extend(view.map(|join| (id, join)));
        Some(id)
    }

    /// A comma list as a left-deep chain of cross joins; the slots under it.
    fn comma_list_walk(
        &mut self,
        entries: &[Source<'t>],
        path: &mut Vec<JoinPathStep>,
    ) -> Vec<SlotId> {
        match entries.split_last() {
            None => Vec::new(),
            Some((last, [])) => self.source_walk(*last, path),
            Some((last, rest)) => {
                let Some(id) = self.join_push(None, JoinKind::Cross) else {
                    return Vec::new();
                };
                self.sides_walk(
                    id,
                    path,
                    |builder, path| builder.comma_list_walk(rest, path),
                    |builder, path| builder.source_walk(*last, path),
                )
            }
        }
    }

    fn source_walk(&mut self, source: Source<'t>, path: &mut Vec<JoinPathStep>) -> Vec<SlotId> {
        let node = match source {
            Source::Join(join) => {
                let kind = match (join.join_type(), join.qual()) {
                    (_, JoinQual::Cross) => JoinKind::Cross,
                    (JoinType::Inner, JoinQual::On(_) | JoinQual::Using(_) | JoinQual::Natural) => {
                        JoinKind::Inner
                    }
                    (JoinType::Left, _) => JoinKind::Left,
                    (JoinType::Right, _) => JoinKind::Right,
                    (JoinType::Full, _) => JoinKind::Full,
                };
                let Some(id) = self.join_push(Some(join), kind) else {
                    return Vec::new();
                };
                return self.sides_walk(
                    id,
                    path,
                    |builder, path| builder.source_walk(join.left(), path),
                    |builder, path| builder.source_walk(join.right(), path),
                );
            }
            Source::Table(table) => table.id(),
            Source::Subquery(subquery) => subquery.id(),
            Source::CteRef(cte_ref) => cte_ref.id(),
        };
        let Some(slot) = self.bound.bindings().source(node) else {
            return Vec::new();
        };
        self.table_references.push(TableReference {
            slot,
            path: path.clone(),
        });
        vec![slot]
    }

    /// Walks a join's two sides with `path` extended by each; the slots
    /// under both.
    fn sides_walk(
        &mut self,
        id: JoinId,
        path: &mut Vec<JoinPathStep>,
        left_walk: impl FnOnce(&mut Self, &mut Vec<JoinPathStep>) -> Vec<SlotId>,
        right_walk: impl FnOnce(&mut Self, &mut Vec<JoinPathStep>) -> Vec<SlotId>,
    ) -> Vec<SlotId> {
        path.push(JoinPathStep {
            join: id,
            side: JoinSide::Left,
        });
        let left = left_walk(self, path);
        path.pop();
        path.push(JoinPathStep {
            join: id,
            side: JoinSide::Right,
        });
        let right = right_walk(self, path);
        path.pop();
        let slots = left.iter().chain(&right).copied().collect();
        if let Some(join) = self.joins.get_mut(id.index()) {
            join.left = left;
            join.right = right;
        }
        slots
    }
}

fn conjuncts_from_where(
    bound: &ResolvedQuery,
    branch: &QueryBranchPredicates,
    scope: ConjunctScope,
    expr: Where<'_>,
    conjuncts: &mut Vec<Conjunct>,
) {
    let mut parts = Vec::new();
    conjunct_parts_collect(bound, expr, &mut parts);
    for part in parts {
        conjuncts.push(conjunct_build(bound, branch, scope, part));
    }
}

/// A join's ON conjuncts, or one equality per `USING`/`NATURAL` merged
/// column.
fn conjuncts_from_join(
    bound: &ResolvedQuery,
    branch: &QueryBranchPredicates,
    id: JoinId,
    join: Join<'_>,
    conjuncts: &mut Vec<Conjunct>,
) {
    let scope = ConjunctScope::On(id);
    match join.qual() {
        JoinQual::On(condition) => conjuncts_from_where(bound, branch, scope, condition, conjuncts),
        JoinQual::Using(_) | JoinQual::Natural => {
            let join_node = join.id();
            for merged in bound
                .bindings()
                .merged_columns()
                .iter()
                .filter(|merged| merged.join.index() == join_node.index())
            {
                conjuncts.push(Conjunct {
                    scope,
                    form: ConjunctForm::Equality(merged.left, merged.right),
                    columns: vec![merged.left, merged.right],
                    external: false,
                    correlated: false,
                });
            }
        }
        JoinQual::Cross => {}
    }
}

/// `part` with the columns its operands mention. A slot sourced inside the
/// operands is a nested subquery's; any other slot outside the branch is an
/// enclosing SELECT's.
fn conjunct_build(
    bound: &ResolvedQuery,
    branch: &QueryBranchPredicates,
    scope: ConjunctScope,
    part: ConjunctPart<'_>,
) -> Conjunct {
    let bindings = bound.bindings();
    let mut columns = Vec::new();
    let mut nested_slots = HashSet::new();
    let mut nested_queries = Vec::new();
    let mut nested_columns = Vec::new();
    for operand in &part.operands {
        for position in operand.subtree() {
            let kind = position.kind();
            let index = position.id().index();
            if matches!(kind, NodeKind::Column { .. }) {
                let target = bindings.column_by_id(position.id());
                for binding in column_bindings(branch, bindings, target) {
                    if nested_queries
                        .iter()
                        .any(|&(start, end)| (start..end).contains(&index))
                    {
                        nested_columns.push(binding);
                    }
                    if !columns.contains(&binding) {
                        columns.push(binding);
                    }
                }
            } else if matches!(kind, NodeKind::Query { .. }) {
                nested_queries.push((index, position.end() as usize));
            } else if matches!(
                kind,
                NodeKind::Table { .. } | NodeKind::Subquery { .. } | NodeKind::CteRef { .. }
            ) {
                nested_slots.extend(bindings.source(position.id()));
            }
        }
    }
    let external = columns.iter().any(|binding| {
        !nested_slots.contains(&binding.slot) && branch.reference_index(binding.slot).is_none()
    });
    let correlated = nested_columns
        .iter()
        .any(|binding| !nested_slots.contains(&binding.slot));
    Conjunct {
        scope,
        form: part.form,
        columns,
        external,
        correlated,
    }
}

/// The columns a reference mentions. A merged column is its left side
/// under an inner or LEFT join, its right side under a RIGHT join, and
/// both under a FULL join or one outside the branch.
fn column_bindings(
    branch: &QueryBranchPredicates,
    bindings: &Bindings,
    target: Option<&ColumnTarget>,
) -> Vec<ColumnBinding> {
    match target {
        Some(ColumnTarget::Column(binding)) => vec![*binding],
        Some(ColumnTarget::Merged(id)) => {
            let Some(merged) = bindings.merged(*id) else {
                return Vec::new();
            };
            let kind = branch
                .joins
                .iter()
                .find(|join| {
                    join.node
                        .is_some_and(|node| node.index() == merged.join.index())
                })
                .map(|join| join.kind);
            match kind {
                Some(JoinKind::Inner | JoinKind::Cross | JoinKind::Left) => vec![merged.left],
                Some(JoinKind::Right) => vec![merged.right],
                Some(JoinKind::Full) | None => vec![merged.left, merged.right],
            }
        }
        Some(ColumnTarget::Output) | None => Vec::new(),
    }
}
