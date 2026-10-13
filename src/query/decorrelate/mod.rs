//! Decorrelation (ADR-014 to ADR-017, ADR-057): every correlated subquery
//! becomes a join, planned over views and built over the base tree. A
//! planner decides what each rewritten SELECT holds (every correlated
//! conjunct flattened, as one plan),
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
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::sync::Arc;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

use self::emit::Emitter;
use self::plan::{Bind, BodyPlan, Planned, QueryPlan};

error_set! {
    DecorrelateError := {
        #[display("Non-decorrelatable correlated subquery: {reason}")]
        NonDecorrelatable { reason: String },
    }
}

pub type DecorrelateResult<T> = Result<T, Report<DecorrelateError>>;
use crate::catalog::{ColumnPosition, TableMetadata};
use crate::query::ast::NodeId;
use crate::query::ast::rope::{Body, NodeKind, Query, Remap, View};
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
    derived: Planned<Arc<TableMetadata>>,
    /// Column nodes carried into the scope they referred to, by base id:
    /// no longer correlated.
    uncorrelated: Planned<NodeId>,
}

/// Decorrelate every correlated subquery of `bound`. `Ok(None)` when it has
/// none; `Err` when one sits where no join form exists.
pub fn query_decorrelate(
    bound: &ResolvedQuery,
    aggregate_functions: &HashSet<EcoString>,
) -> DecorrelateResult<Option<ResolvedQuery>> {
    // Nothing to plan, and nothing to allocate for, without a subquery.
    if !bound.tree().nodes().any(|node| {
        matches!(
            node.kind,
            NodeKind::WhereSubquery { .. } | NodeKind::ScalarSubquery
        )
    }) {
        return Ok(None);
    }
    let mut planner = Planner {
        bindings: bound.bindings(),
        aggregate_functions,
        derived_counter: 0,
        scalar_counter: 0,
        derived: Planned::new(),
        uncorrelated: Planned::new(),
    };
    let Some(plan) = planner.query_plan(bound.tree().root())? else {
        return Ok(None);
    };
    let mut emitter = Emitter::new(bound.tree());
    emitter.query_emit(&plan);
    let (tree, remap, binds, derived_nodes) = emitter.finish();
    let bindings = bindings_build(&planner, bound.bindings(), &remap, binds, &derived_nodes)?;
    Ok(Some(ResolvedQuery::new(tree, bindings)))
}

/// The rewritten tree's bindings: the base's carried to where their nodes
/// landed, a slot per derived table, the fresh references bound, and the
/// references carried beside their table no longer correlated.
fn bindings_build(
    planner: &Planner<'_>,
    base: &Bindings,
    remap: &Remap,
    binds: Planned<(NodeId, Bind)>,
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
pub(crate) mod fixtures {
    use std::collections::HashSet;

    use ecow::EcoString;
    use iddqd::BiHashMap;
    use postgres_types::Type;

    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::resolve::ResolvedQuery;

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

    pub(crate) fn test_tables() -> Catalog {
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

    pub(crate) fn aggregates() -> HashSet<EcoString> {
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

    pub(crate) fn corpus() -> impl Iterator<Item = &'static str> {
        CORPUS.iter().chain(EXTRA_CORPUS).copied()
    }

    pub(crate) fn bound(sql: &str, catalog: &Catalog) -> ResolvedQuery {
        ResolvedQuery::parse_resolve(sql, catalog, &["public"]).expect("resolve")
    }
}

#[cfg(test)]
mod corpus_tests {
    use super::fixtures::{aggregates, bound, corpus, test_tables};
    use super::query_decorrelate;
    use crate::query::resolve::query_tree_resolve;

    /// Over both corpora the rewrite's carried bindings are the ones the
    /// resolver makes for the rewritten tree; every outcome occurs.
    #[test]
    fn test_decorrelation_bindings_match_resolver() {
        let catalog = test_tables();
        let aggregates = aggregates();
        let (mut rewritten, mut unchanged, mut rejected) = (0, 0, 0);
        for sql in corpus() {
            match query_decorrelate(&bound(sql, &catalog), &aggregates) {
                Ok(Some(result)) => {
                    rewritten += 1;
                    let resolved = query_tree_resolve(result.tree(), &catalog, &["public"])
                        .unwrap_or_else(|e| panic!("{sql}: the decorrelated tree resolves: {e:?}"));
                    let mut carried = result.bindings().describe();
                    let mut expected = resolved.describe();
                    carried.sort();
                    expected.sort();
                    assert_eq!(carried, expected, "{sql}");
                }
                Ok(None) => unchanged += 1,
                Err(_) => rejected += 1,
            }
        }
        assert!(
            rewritten > 0 && unchanged > 0 && rejected > 0,
            "the corpus exercises every outcome: {rewritten} rewritten, {unchanged} unchanged, \
             {rejected} rejected"
        );
    }
}
