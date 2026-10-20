//! The per-reference facts a branch's predicates imply: which conjuncts
//! filter a reference, which outer joins they reduce, and what each
//! reference's rows must answer to.

use super::{
    BoundColumnConstraint, ColumnConstraintKind, Conjunct, ConjunctForm, ConjunctScope, FromJoin,
    JoinId, JoinKind, JoinPathStep, JoinSide, OuterJoinRole, QueryBranchPredicates,
    RelationPredicates, TableReference, TableReferencePredicates,
};
use crate::catalog::ColumnPosition;
use crate::query::constraints::TableConstraint;
use crate::query::resolve::{Bindings, ColumnBinding, SlotId, TableSlot};

impl FromJoin {
    /// Whether this join's ON restricts the rows of `side`: both sides of an
    /// inner join, only the NULL-filled side of an outer one.
    fn on_reaches(&self, side: JoinSide) -> bool {
        match self.kind {
            JoinKind::Inner | JoinKind::Cross => true,
            JoinKind::Left => self.reduced || side == JoinSide::Right,
            JoinKind::Right => self.reduced || side == JoinSide::Left,
            JoinKind::Full => false,
        }
    }

    /// Whether this join pads `side` with NULLs where it finds no match.
    fn null_fills(&self, side: JoinSide) -> bool {
        !self.reduced
            && match self.kind {
                JoinKind::Left => side == JoinSide::Right,
                JoinKind::Right => side == JoinSide::Left,
                JoinKind::Full => true,
                JoinKind::Inner | JoinKind::Cross => false,
            }
    }

    /// Whether every row of `side` survives this join.
    fn preserves(&self, side: JoinSide) -> bool {
        !self.reduced
            && match self.kind {
                JoinKind::Left => side == JoinSide::Left,
                JoinKind::Right => side == JoinSide::Right,
                JoinKind::Full => true,
                JoinKind::Inner | JoinKind::Cross => false,
            }
    }

    /// The slots an outer join pads with NULLs; none for any other join.
    fn null_side(&self) -> &[SlotId] {
        match self.kind {
            JoinKind::Left => &self.right,
            JoinKind::Right => &self.left,
            JoinKind::Inner | JoinKind::Cross | JoinKind::Full => &[],
        }
    }
}

impl ConjunctForm {
    /// The slots a row must have non-NULL for this conjunct to hold.
    fn null_rejected_slots(&self) -> impl Iterator<Item = SlotId> {
        let (first, second) = match self {
            ConjunctForm::Constraint(constraint) => (Some(constraint.column.slot), None),
            ConjunctForm::Equality(left, right) => (Some(left.slot), Some(right.slot)),
            ConjunctForm::Opaque => (None, None),
        };
        first.into_iter().chain(second)
    }
}

impl Conjunct {
    fn mentions(&self, slot: SlotId) -> bool {
        self.columns.iter().any(|column| column.slot == slot)
    }
}

impl BoundColumnConstraint {
    /// This constraint restated on `other`, a column equal to its own. A
    /// cast comparison stays put: `x::int = 5 AND x = y` says nothing of
    /// `y::int` unless `y` casts alike.
    fn restated_on(&self, other: ColumnBinding) -> Option<Self> {
        match &self.kind {
            ColumnConstraintKind::Comparison { .. } | ColumnConstraintKind::AnyOf(_) => {
                Some(Self {
                    column: other,
                    kind: self.kind.clone(),
                })
            }
            ColumnConstraintKind::CastComparison { .. } => None,
        }
    }
}

impl QueryBranchPredicates {
    pub fn table_references(&self) -> &[TableReference] {
        &self.table_references
    }

    pub fn joins(&self) -> &[FromJoin] {
        &self.joins
    }

    pub fn conjuncts(&self) -> &[Conjunct] {
        &self.conjuncts
    }

    pub fn table_reference_predicates(&self, slot: SlotId) -> Option<&TableReferencePredicates> {
        self.reference_predicates.get(self.reference_index(slot)?)
    }

    /// The predicates of the reference `slot` by column name.
    pub fn relation_predicates(
        &self,
        bindings: &Bindings,
        slot: SlotId,
    ) -> Option<RelationPredicates> {
        let predicates = self.table_reference_predicates(slot)?;
        let filters = predicates
            .filters
            .iter()
            .map(|constraint| {
                let name = bindings.binding_column(&constraint.column)?.name.clone();
                Some(match &constraint.kind {
                    ColumnConstraintKind::Comparison { op, value } => {
                        TableConstraint::Comparison(name, *op, value.clone())
                    }
                    ColumnConstraintKind::AnyOf(values) => {
                        TableConstraint::AnyOf(name, values.clone())
                    }
                    ColumnConstraintKind::CastComparison { cast, op, value } => {
                        TableConstraint::CastComparison(name, cast.clone(), *op, value.clone())
                    }
                })
            })
            .collect::<Option<Vec<_>>>()?;
        let link_columns = predicates
            .link_columns
            .iter()
            .map(|&position| {
                let binding = ColumnBinding { slot, position };
                bindings
                    .binding_column(&binding)
                    .map(|column| column.name.clone())
            })
            .collect::<Option<Vec<_>>>()?;
        Some(RelationPredicates {
            filters,
            link_columns,
            exact: predicates.exact,
        })
    }

    pub fn join_reduced(&self, id: JoinId) -> bool {
        self.join(id).is_some_and(|join| join.reduced)
    }

    /// Whether `conjunct` restricts the real rows of the reference `slot`:
    /// it reaches the reference, and no unreduced outer join within its
    /// reach NULL-fills it. Below such a join a conjunct can hold on the
    /// padding alone, as `WHERE r.id IS NULL` does in an anti-join.
    pub fn conjunct_filters(&self, conjunct: &Conjunct, slot: SlotId) -> bool {
        let Some(path) = self.reference_path(slot) else {
            return false;
        };
        let start = match conjunct.scope {
            ConjunctScope::Where => 0,
            ConjunctScope::On(id) => {
                let Some((position, step)) = path_step(path, id) else {
                    return false;
                };
                if !self.join(id).is_some_and(|join| join.on_reaches(step.side)) {
                    return false;
                }
                position + 1
            }
        };
        path.get(start..)
            .unwrap_or_default()
            .iter()
            .all(|step| !self.step_null_fills(step))
    }

    pub(super) fn reference_index(&self, slot: SlotId) -> Option<usize> {
        self.table_references
            .iter()
            .position(|reference| reference.slot == slot)
    }

    fn reference_path(&self, slot: SlotId) -> Option<&[JoinPathStep]> {
        self.table_references
            .get(self.reference_index(slot)?)
            .map(|reference| reference.path.as_slice())
    }

    fn join(&self, id: JoinId) -> Option<&FromJoin> {
        self.joins.get(id.index())
    }

    fn step_null_fills(&self, step: &JoinPathStep) -> bool {
        self.join(step.join)
            .is_some_and(|join| join.null_fills(step.side))
    }

    /// Marks each outer join a NULL-rejecting conjunct above it reduces to
    /// an inner join. Joins are in pre-order, so an ancestor's reduction,
    /// which decides what its ON reaches, is known before its descendants'.
    pub(super) fn joins_reduce(&mut self) {
        for index in 0..self.joins.len() {
            let Some(join) = self.joins.get(index) else {
                continue;
            };
            let null_side = join.null_side();
            let reduced = self.conjuncts.iter().any(|conjunct| {
                conjunct.form.null_rejected_slots().any(|slot| {
                    null_side.contains(&slot) && self.conjunct_reaches_above(conjunct, slot, index)
                })
            });
            if reduced && let Some(join) = self.joins.get_mut(index) {
                join.reduced = true;
            }
        }
    }

    /// Whether `conjunct` reaches `slot` from above the join at `below`:
    /// from WHERE, or from the ON of a join enclosing it.
    fn conjunct_reaches_above(&self, conjunct: &Conjunct, slot: SlotId, below: usize) -> bool {
        let ConjunctScope::On(id) = conjunct.scope else {
            return true;
        };
        let Some(path) = self.reference_path(slot) else {
            return false;
        };
        let Some(below) = path.iter().position(|step| step.join.index() == below) else {
            return false;
        };
        path_step(path, id).is_some_and(|(position, step)| {
            position < below && self.join(id).is_some_and(|join| join.on_reaches(step.side))
        })
    }

    pub(super) fn reference_predicates_derive(
        &self,
        bindings: &Bindings,
    ) -> Vec<TableReferencePredicates> {
        let filters = self.filters_derive();
        self.table_references
            .iter()
            .zip(filters)
            .map(|(reference, filters)| TableReferencePredicates {
                filters,
                exact: self.reference_exact(bindings, reference),
                link_columns: self.link_columns_derive(reference),
                outer_join_role: self.outer_join_role_derive(reference),
            })
            .collect()
    }

    /// Each reference's constraints from the conjuncts that filter it,
    /// closed under the equalities: an equality carries a constraint to a
    /// column whose reference it filters.
    fn filters_derive(&self) -> Vec<Vec<BoundColumnConstraint>> {
        let mut filters = vec![Vec::new(); self.table_references.len()];
        let mut edges = Vec::new();
        for conjunct in self.conjuncts.iter().filter(|conjunct| !conjunct.external) {
            match &conjunct.form {
                ConjunctForm::Constraint(constraint) => {
                    if self.conjunct_filters(conjunct, constraint.column.slot) {
                        filter_insert(self, &mut filters, constraint.clone());
                    }
                }
                ConjunctForm::Equality(left, right) => {
                    if self.conjunct_filters(conjunct, right.slot) {
                        edges.push((*left, *right));
                    }
                    if self.conjunct_filters(conjunct, left.slot) {
                        edges.push((*right, *left));
                    }
                }
                ConjunctForm::Opaque => {}
            }
        }
        loop {
            let carried: Vec<BoundColumnConstraint> = edges
                .iter()
                .flat_map(|&(from, to)| {
                    self.reference_index(from.slot)
                        .and_then(|index| filters.get(index))
                        .into_iter()
                        .flatten()
                        .filter(move |constraint| constraint.column == from)
                        .filter_map(move |constraint| constraint.restated_on(to))
                })
                .collect();
            let mut changed = false;
            for constraint in carried {
                changed |= filter_insert(self, &mut filters, constraint);
            }
            if !changed {
                return filters;
            }
        }
    }

    /// Whether population loads every row of a base table reference its
    /// filters describe: every join on its path preserves it, every WHERE
    /// conjunct is a constraint on it alone, and nothing outside the branch
    /// restricts it.
    fn reference_exact(&self, bindings: &Bindings, reference: &TableReference) -> bool {
        let slot = reference.slot;
        matches!(bindings.slot(slot), Some(TableSlot::Base { .. }))
            && reference.path.iter().all(|step| {
                self.join(step.join)
                    .is_some_and(|join| join.kind != JoinKind::Full && join.preserves(step.side))
            })
            && self.conjuncts.iter().all(|conjunct| match conjunct.scope {
                ConjunctScope::Where => {
                    matches!(&conjunct.form, ConjunctForm::Constraint(constraint)
                        if constraint.column.slot == slot)
                        && conjunct.columns.iter().all(|column| column.slot == slot)
                }
                ConjunctScope::On(_) => !(conjunct.external && conjunct.mentions(slot)),
            })
    }

    /// The reference's columns in any conjunct that mentions another
    /// reference or an enclosing SELECT, and in the ON of an outer join
    /// that preserves it, whose match a change there can alter.
    fn link_columns_derive(&self, reference: &TableReference) -> Vec<ColumnPosition> {
        let slot = reference.slot;
        let mut link_columns = Vec::new();
        for conjunct in &self.conjuncts {
            let links = conjunct.external
                || conjunct.columns.iter().any(|column| column.slot != slot)
                || match conjunct.scope {
                    ConjunctScope::On(id) => {
                        path_step(&reference.path, id).is_some_and(|(_, step)| {
                            self.join(id).is_some_and(|join| join.preserves(step.side))
                        })
                    }
                    ConjunctScope::Where => false,
                };
            if !links {
                continue;
            }
            for column in conjunct.columns.iter().filter(|column| column.slot == slot) {
                if !link_columns.contains(&column.position) {
                    link_columns.push(column.position);
                }
            }
        }
        link_columns
    }

    /// `Optional` when a conjunct above an unreduced outer join that
    /// NULL-fills the reference mentions it; the join's own ON doesn't
    /// count, an enclosing join's does.
    fn outer_join_role_derive(&self, reference: &TableReference) -> OuterJoinRole {
        let mut role = OuterJoinRole::Kept;
        for (position, step) in reference.path.iter().enumerate() {
            if !self.step_null_fills(step) {
                continue;
            }
            let above = reference.path.get(..position).unwrap_or_default();
            let mentioned = self.conjuncts.iter().any(|conjunct| {
                conjunct.mentions(reference.slot)
                    && match conjunct.scope {
                        ConjunctScope::Where => true,
                        ConjunctScope::On(id) => above.iter().any(|step| step.join == id),
                    }
            });
            if mentioned {
                return OuterJoinRole::Optional;
            }
            role = OuterJoinRole::Terminal;
        }
        role
    }
}

/// The step through join `id`, with its position, when the path has one.
fn path_step(path: &[JoinPathStep], id: JoinId) -> Option<(usize, &JoinPathStep)> {
    path.iter().enumerate().find(|(_, step)| step.join == id)
}

/// Adds `constraint` to its reference's filters; whether it was new.
fn filter_insert(
    branch: &QueryBranchPredicates,
    filters: &mut [Vec<BoundColumnConstraint>],
    constraint: BoundColumnConstraint,
) -> bool {
    let Some(list) = branch
        .reference_index(constraint.column.slot)
        .and_then(|index| filters.get_mut(index))
    else {
        return false;
    };
    if list.contains(&constraint) {
        return false;
    }
    list.push(constraint);
    true
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use iddqd::BiHashMap;
    use postgres_types::Type;

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, TableMetadata};
    use crate::oid::TypeOid;
    use crate::query::ast::NodeId;
    use crate::query::ast::rope::{NodeKind, View};
    use crate::query::constraints::query_constraints_analyze;
    use crate::query::decorrelate::query_decorrelate;
    use crate::query::predicates::query_branch_predicates_build;
    use crate::query::resolve::ResolvedQuery;

    /// Tables `a`, `b` and `c`, each `(id, x, y)` of int4.
    fn catalog() -> Catalog {
        let int_column = |name: &str, position: i16| ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_raw(23),
            data_type: Type::INT4,
            type_name: "int4".into(),
            cache_type_name: "int4".into(),
            is_primary_key: position == 1,
        };
        let mut tables = BiHashMap::new();
        for (oid, name) in (1001..).zip(["a", "b", "c"]) {
            tables.insert_overwrite(
                TableMetadata::for_test(
                    name,
                    oid,
                    [int_column("id", 1), int_column("x", 2), int_column("y", 3)],
                )
                .into(),
            );
        }
        tables
    }

    /// `sql` resolved, then decorrelated when it has a subquery to rewrite.
    fn resolve(sql: &str) -> ResolvedQuery {
        let bound = ResolvedQuery::parse_resolve(sql, &catalog(), &["public"]).expect("resolve");
        query_decorrelate(&bound, &HashSet::new())
            .expect("decorrelate")
            .unwrap_or(bound)
    }

    fn root_predicates(bound: &ResolvedQuery) -> QueryBranchPredicates {
        query_branch_predicates_build(bound, bound.select().expect("a SELECT"))
    }

    /// The slot a qualifier names among `branch`'s references.
    fn slot_of(bound: &ResolvedQuery, branch: &QueryBranchPredicates, name: &str) -> SlotId {
        branch
            .table_references()
            .iter()
            .map(|reference| reference.slot)
            .find(|slot| {
                bound
                    .bindings()
                    .slot(*slot)
                    .is_some_and(|table_slot| table_slot.qualifier() == name)
            })
            .unwrap_or_else(|| panic!("a reference named {name}"))
    }

    fn constraint_text(constraint: &TableConstraint) -> String {
        match constraint {
            TableConstraint::Comparison(column, op, value) => format!("{column} {op:?} {value:?}"),
            TableConstraint::AnyOf(column, values) => format!("{column} AnyOf {values:?}"),
            TableConstraint::CastComparison(column, cast, op, value) => {
                format!("{column}::{cast:?} {op:?} {value:?}")
            }
        }
    }

    /// One line per reference of the root SELECT, in FROM order.
    fn reference_lines(sql: &str) -> Vec<String> {
        let bound = resolve(sql);
        let branch = root_predicates(&bound);
        let bindings = bound.bindings();
        branch
            .table_references()
            .iter()
            .map(|reference| {
                let slot = reference.slot;
                let relation = branch
                    .relation_predicates(bindings, slot)
                    .expect("relation predicates");
                let role = branch
                    .table_reference_predicates(slot)
                    .expect("reference predicates")
                    .outer_join_role;
                let mut filters: Vec<String> =
                    relation.filters.iter().map(constraint_text).collect();
                filters.sort();
                let mut links = relation.link_columns.clone();
                links.sort();
                let name = bindings.slot(slot).expect("slot").qualifier().to_owned();
                format!(
                    "{name} exact={} role={role:?} filters=[{}] links=[{}]",
                    relation.exact,
                    filters.join(", "),
                    links.join(", ")
                )
            })
            .collect()
    }

    fn reduced_joins(sql: &str) -> Vec<bool> {
        let bound = resolve(sql);
        root_predicates(&bound)
            .joins()
            .iter()
            .map(|join| join.reduced)
            .collect()
    }

    /// An outer join is reduced by a NULL-rejecting conjunct in WHERE or an
    /// enclosing ON, never by its own ON, and not by `IS NULL`.
    #[test]
    fn test_outer_join_reduction() {
        #[rustfmt::skip]
        let cases: &[(&str, &[bool])] = &[
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id", &[false]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND b.x = 1", &[false]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x = 1", &[true]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE a.x = 1", &[false]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL", &[false]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x + 1 = 2", &[false]),
            ("SELECT * FROM a RIGHT JOIN b ON b.id = a.id WHERE a.x = 1", &[true]),
            // Pre-order: the outer join first, then the nested one.
            ("SELECT * FROM a LEFT JOIN (b LEFT JOIN c ON c.id = b.id) ON b.id = a.id \
              AND c.x = 1", &[false, true]),
            ("SELECT * FROM a LEFT JOIN (b LEFT JOIN c ON c.id = b.id) ON b.id = a.id \
              WHERE c.x = 1", &[true, true]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id JOIN c ON c.x = b.x",
             &[false, true]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id, c WHERE b.y = c.y",
             &[false, true]),
        ];
        for (sql, expected) in cases {
            assert_eq!(reduced_joins(sql), *expected, "{sql}");
        }
    }

    /// Under an unreduced LEFT join, a WHERE conjunct can hold on the
    /// padding alone, so it filters the preserved side only.
    #[test]
    fn test_anti_join_where_filters_preserved_side_only() {
        let bound = resolve("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL");
        let branch = root_predicates(&bound);
        let (a, b) = (slot_of(&bound, &branch, "a"), slot_of(&bound, &branch, "b"));
        let where_conjunct = branch
            .conjuncts()
            .iter()
            .find(|conjunct| conjunct.scope == ConjunctScope::Where)
            .expect("the WHERE conjunct");
        assert!(branch.conjunct_filters(where_conjunct, a));
        assert!(!branch.conjunct_filters(where_conjunct, b));
        let on_conjunct = branch
            .conjuncts()
            .iter()
            .find(|conjunct| matches!(conjunct.scope, ConjunctScope::On(_)))
            .expect("the ON conjunct");
        assert!(!branch.conjunct_filters(on_conjunct, a));
        assert!(branch.conjunct_filters(on_conjunct, b));
    }

    /// Each reference's filters, exactness, link columns and role.
    #[test]
    fn test_reference_predicates() {
        #[rustfmt::skip]
        let cases: &[(&str, &[&str])] = &[
            ("SELECT * FROM a WHERE x = 1 AND y IN (2, 1)",
             &["a exact=true role=Kept filters=[x Equal Integer(1), y AnyOf [Integer(1), Integer(2)]] links=[]"]),
            ("SELECT * FROM a",
             &["a exact=true role=Kept filters=[] links=[]"]),
            // A same-reference equality describes no row set by itself.
            ("SELECT * FROM a WHERE x = y",
             &["a exact=false role=Kept filters=[] links=[]"]),
            // The literal bound of a BETWEEN holds without the other.
            ("SELECT * FROM a WHERE x BETWEEN 1 AND y",
             &["a exact=false role=Kept filters=[x GreaterThanOrEqual Integer(1)] links=[]"]),
            ("SELECT * FROM a WHERE x NOT IN (1, y, 2)",
             &["a exact=false role=Kept filters=[x NotEqual Integer(1), x NotEqual Integer(2)] links=[]"]),
            // An outer equality carries a constraint from the preserved side only.
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE a.id = 5",
             &["a exact=true role=Kept filters=[id Equal Integer(5)] links=[id]",
               "b exact=false role=Terminal filters=[id Equal Integer(5)] links=[id]"]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND b.x = 5",
             &["a exact=true role=Kept filters=[] links=[id]",
               "b exact=false role=Terminal filters=[x Equal Integer(5)] links=[id]"]),
            // A preserved-side ON literal filters nothing but is a link column.
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id AND a.x = 1",
             &["a exact=true role=Kept filters=[] links=[id, x]",
               "b exact=false role=Terminal filters=[] links=[id]"]),
            // Reduced: the equality carries both ways and b is kept.
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.id = 5",
             &["a exact=false role=Kept filters=[id Equal Integer(5)] links=[id]",
               "b exact=false role=Kept filters=[id Equal Integer(5)] links=[id]"]),
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id WHERE b.x IS NULL",
             &["a exact=false role=Kept filters=[] links=[id]",
               "b exact=false role=Optional filters=[] links=[id]"]),
            ("SELECT * FROM a RIGHT JOIN b ON b.id = a.id WHERE b.x = 1",
             &["a exact=false role=Terminal filters=[] links=[id]",
               "b exact=true role=Kept filters=[x Equal Integer(1)] links=[id]"]),
            // A table referenced from an enclosing ON is optional.
            ("SELECT * FROM a LEFT JOIN b ON b.id = a.id LEFT JOIN c ON c.x = b.x",
             &["a exact=true role=Kept filters=[] links=[id]",
               "b exact=false role=Optional filters=[] links=[id, x]",
               "c exact=false role=Terminal filters=[] links=[x]"]),
            // Expression ON and cross-table WHERE columns are link columns.
            ("SELECT * FROM a JOIN b ON b.id = a.x + 1 WHERE a.y = 1",
             &["a exact=false role=Kept filters=[y Equal Integer(1)] links=[x]",
               "b exact=false role=Kept filters=[] links=[id]"]),
            ("SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x < b.y",
             &["a exact=false role=Kept filters=[] links=[id, x]",
               "b exact=false role=Kept filters=[] links=[id, y]"]),
            // A comma list is a chain of cross joins.
            ("SELECT * FROM a, b, c WHERE a.id = b.id AND b.id = c.id AND c.x = 3",
             &["a exact=false role=Kept filters=[] links=[id]",
               "b exact=false role=Kept filters=[] links=[id]",
               "c exact=false role=Kept filters=[x Equal Integer(3)] links=[id]"]),
            // An inner merged column is its left side; USING is an equality.
            ("SELECT * FROM a JOIN b USING (id) WHERE id = 1",
             &["a exact=false role=Kept filters=[id Equal Integer(1)] links=[id]",
               "b exact=false role=Kept filters=[id Equal Integer(1)] links=[id]"]),
            // An outer merged column isn't recognized yet; it reads the left side.
            ("SELECT * FROM a LEFT JOIN b USING (id) WHERE id = 1",
             &["a exact=false role=Kept filters=[] links=[id]",
               "b exact=false role=Terminal filters=[] links=[id]"]),
            // A derived table is a reference but never exact.
            ("SELECT * FROM a JOIN (SELECT id FROM b WHERE x = 1) d ON d.id = a.id WHERE d.id = 2",
             &["a exact=false role=Kept filters=[id Equal Integer(2)] links=[id]",
               "d exact=false role=Kept filters=[id Equal Integer(2)] links=[id]"]),
            // A subquery's columns make the outer ones it reads link columns.
            ("SELECT * FROM a WHERE x = 1 AND y IN (SELECT b.y FROM b)",
             &["a exact=false role=Kept filters=[x Equal Integer(1)] links=[y]"]),
        ];
        for (sql, expected) in cases {
            assert_eq!(reference_lines(sql), *expected, "{sql}");
        }
    }

    /// Each reference of a self-join keeps its own filters.
    #[test]
    fn test_self_join_filters_per_reference() {
        let bound =
            resolve("SELECT * FROM a a1 JOIN a a2 ON a2.id = a1.x WHERE a1.y = 1 AND a2.y = 2");
        let branch = root_predicates(&bound);
        let filters = |name: &str| {
            let slot = slot_of(&bound, &branch, name);
            let relation = branch
                .relation_predicates(bound.bindings(), slot)
                .expect("relation predicates");
            relation
                .filters
                .iter()
                .map(constraint_text)
                .collect::<Vec<_>>()
        };
        assert_eq!(filters("a1"), ["y Equal Integer(1)"]);
        assert_eq!(filters("a2"), ["y Equal Integer(2)"]);
    }

    /// Each reference of a self-join links on its own side of the equality.
    #[test]
    fn test_self_join_link_columns_per_reference() {
        let bound = resolve("SELECT * FROM a e JOIN a m ON m.id = e.x");
        let branch = root_predicates(&bound);
        let links = |name: &str| {
            let slot = slot_of(&bound, &branch, name);
            branch
                .relation_predicates(bound.bindings(), slot)
                .expect("relation predicates")
                .link_columns
        };
        assert_eq!(links("e"), ["x"]);
        assert_eq!(links("m"), ["id"]);
    }

    /// The SELECTs under the root, by position.
    fn nested_select_ids(bound: &ResolvedQuery) -> Vec<NodeId> {
        bound
            .tree()
            .root()
            .node_ref()
            .subtree()
            .filter(|position| {
                matches!(position.kind(), NodeKind::Select { .. })
                    && bound
                        .select()
                        .is_none_or(|root| root.id().index() != position.id().index())
            })
            .map(|position| position.id())
            .collect()
    }

    /// In a correlated branch, a conjunct on an enclosing SELECT's column
    /// filters nothing, and the branch's columns in it are link columns.
    #[test]
    fn test_correlated_branch_conjuncts_external() {
        let bound = ResolvedQuery::parse_resolve(
            "SELECT * FROM a WHERE EXISTS \
             (SELECT 1 FROM b WHERE b.id = a.id AND b.x = 1 AND a.y = 2)",
            &catalog(),
            &["public"],
        )
        .expect("resolve");
        let [select_id] = nested_select_ids(&bound)[..] else {
            panic!("one nested SELECT");
        };
        let select = bound.select_node(select_id).expect("the subquery SELECT");
        let branch = query_branch_predicates_build(&bound, select);
        let b = slot_of(&bound, &branch, "b");
        let external: Vec<bool> = branch.conjuncts().iter().map(|c| c.external).collect();
        assert_eq!(external, [true, false, true]);
        let relation = branch
            .relation_predicates(bound.bindings(), b)
            .expect("relation predicates");
        let filters: Vec<String> = relation.filters.iter().map(constraint_text).collect();
        assert_eq!(filters, ["x Equal Integer(1)"]);
        assert_eq!(relation.link_columns, ["id"]);
        assert!(!relation.exact);
    }

    /// The model's filters equal the collector's per-table constraints over
    /// queries that name each relation once.
    #[test]
    fn test_agreement_with_constraint_collector() {
        #[rustfmt::skip]
        let corpus: &[&str] = &[
            "SELECT * FROM a WHERE x = 1",
            "SELECT * FROM a WHERE x = 1 AND y > 2",
            "SELECT * FROM a WHERE x IN (3, 1, 2) AND y NOT IN (4, 5)",
            "SELECT * FROM a WHERE x BETWEEN 1 AND 5",
            "SELECT * FROM a WHERE x BETWEEN SYMMETRIC 5 AND 1",
            "SELECT * FROM a WHERE x BETWEEN 1 AND y",
            "SELECT * FROM a WHERE x NOT IN (1, y, 2)",
            "SELECT * FROM a WHERE x = ANY (ARRAY[1, 2])",
            "SELECT * FROM a WHERE x::text = '1'",
            "SELECT * FROM a WHERE x = 1 OR y = 2",
            "SELECT * FROM a WHERE x = y",
            "SELECT * FROM a WHERE NOT (x = 1)",
            "SELECT * FROM a WHERE x IN (SELECT id FROM b)",
            "SELECT * FROM a WHERE EXISTS (SELECT 1 FROM b WHERE b.id = a.id)",
            "SELECT * FROM a WHERE NOT EXISTS (SELECT 1 FROM b WHERE b.id = a.id AND b.id = 5)",
            "SELECT * FROM a JOIN b ON a.id = b.id AND b.x = 1 WHERE a.y = 2",
            "SELECT * FROM a JOIN b USING (id) WHERE id = 1",
            "SELECT * FROM a NATURAL JOIN b JOIN c ON c.id = b.id",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id CROSS JOIN c",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND a.x = 1",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND b.x = 5",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE a.id = 5",
            "SELECT * FROM a LEFT JOIN b USING (id) WHERE a.id = 5",
            "SELECT * FROM b RIGHT JOIN a ON a.id = b.id AND a.x = 1 AND b.id = 5",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id LEFT JOIN c ON b.id = c.id AND a.x = 1",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id WHERE b.id = 5",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND a.x = 1 WHERE b.x = 2",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id AND b.id = 5 WHERE b.x IS NULL",
            "SELECT * FROM a LEFT JOIN (b LEFT JOIN c ON b.id = c.id) ON a.id = b.id \
              WHERE c.id = 7",
            "SELECT * FROM a LEFT JOIN (b LEFT JOIN c ON b.id = c.id) ON a.id = b.id \
              AND c.id = 3",
            "SELECT * FROM a LEFT JOIN b ON a.id = b.id JOIN c ON b.id = c.id AND c.id = 2",
            "SELECT * FROM a RIGHT JOIN b ON a.id = b.id WHERE b.x = 1",
            "SELECT * FROM a, b WHERE a.id = b.id AND a.x = 1",
            "SELECT * FROM a, b, c WHERE a.id = b.id AND b.id = c.id AND c.x = 3",
            "SELECT d.id FROM (SELECT id FROM a WHERE id = 1) d WHERE d.id = 2",
            "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.x::text = '3'",
            "SELECT * FROM a JOIN b ON a.id = b.id JOIN c ON b.x = c.x WHERE a.id IN (1, 2)",
            "SELECT * FROM a JOIN b ON a.id = b.id AND a.x = b.x WHERE b.x = 4",
            "SELECT * FROM a JOIN b ON b.id = a.x + 1 WHERE a.y = 1",
            "SELECT * FROM a JOIN b ON b.id = a.id WHERE a.x < b.y",
        ];
        for sql in corpus {
            let bound = resolve(sql);
            let select = bound.select().expect("a SELECT");
            let branch = query_branch_predicates_build(&bound, select);
            let collected = query_constraints_analyze(&bound, select);
            let bindings = bound.bindings();
            let mut names = HashSet::new();
            for reference in branch.table_references() {
                let name = bindings
                    .slot(reference.slot)
                    .expect("slot")
                    .table()
                    .name
                    .clone();
                assert!(names.insert(name.clone()), "{sql}: {name} named twice");
                let relation = branch
                    .relation_predicates(bindings, reference.slot)
                    .expect("relation predicates");
                let mut model: Vec<String> = relation.filters.iter().map(constraint_text).collect();
                model.sort();
                let mut today: Vec<String> = collected
                    .table_constraints
                    .get(&name)
                    .into_iter()
                    .flatten()
                    .map(constraint_text)
                    .collect();
                today.sort();
                assert_eq!(model, today, "{sql}: filters of {name}");
            }
            for table in collected.table_constraints.keys() {
                assert!(names.contains(table), "{sql}: {table} is no reference");
            }
        }
    }
}
