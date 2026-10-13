//! Admission gate for order-dependent enum usage (PGC-266) over a
//! [`ResolvedQuery`]: the plain tree, with enum-ness read through the
//! bindings.
//!
//! Enum columns are stored as `text` in the cache DB, which preserves
//! equality but not enum sort order. Order-dependent positions — ORDER BY,
//! range comparisons, BETWEEN, min/max — would serve text-ordered results
//! and mislead the Rust-side predicate machinery (local eval, range
//! subsumption, constraint index), all of which compare string values
//! lexicographically. Queries using an enum column in such a position are
//! rejected at registration and forwarded to origin.
//!
//! The walk is positional by design: it must distinguish an ORDER BY
//! reference from an equality predicate, so it cannot route through the
//! uniform `try_for_each_node` traversal. Inside a checked position the
//! enum test *does* use uniform descent, so an enum reference nested
//! anywhere under that position (casts, arithmetic, subqueries) rejects
//! conservatively. False positives only cost caching — the query forwards.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;
use rootcause::Report;

use super::bindings::{Bindings, ColumnBinding, ColumnTarget, ResolvedQuery, StarItem};
use crate::query::ast::rope::{
    Binary, Body, Case, Column, Function, JoinQual, Multi, NodeKind, NodeRef, OrderBy, Query, Run,
    Scalar, ScalarKind, Select, SelectItem, Source, View, Where, WhereKind,
};
use crate::query::ast::{BinaryOp, LiteralValue, MultiOp};
use crate::query::resolve::{ResolveError, ResolveResult};

impl ResolvedQuery {
    /// Check for order-dependent enum usage. `Err` carries the first
    /// offending column and its position; registration turns it into a
    /// forward.
    pub fn enum_order_dependence_check(&self) -> ResolveResult<()> {
        let gate = Gate {
            bindings: self.bindings(),
        };
        gate.query_check(self.tree().root())
    }
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
    /// The enum column a binding names, when its type is an ordered enum.
    fn binding_enum(&self, binding: &ColumnBinding) -> Option<EnumColumn> {
        let slot = self.bindings.slot(binding.slot)?;
        let column = self.bindings.binding_column(binding)?;
        column.is_enum_ordered().then(|| EnumColumn {
            table: slot.table().name.clone(),
            column: column.name.clone(),
        })
    }

    /// The enum column a reference stands for: a merged column is its left
    /// side, or under an outer join either side.
    fn column_enum(&self, column: Column<'_>) -> Option<EnumColumn> {
        match self.bindings.column_by_id(column.id())? {
            ColumnTarget::Column(binding) => self.binding_enum(binding),
            ColumnTarget::Merged(id) => {
                let merged = self.bindings.merged(*id)?;
                self.merged_enum(merged.outer, &merged.left, &merged.right)
            }
            ColumnTarget::Output => None,
        }
    }

    fn merged_enum(
        &self,
        outer: bool,
        left: &ColumnBinding,
        right: &ColumnBinding,
    ) -> Option<EnumColumn> {
        self.binding_enum(left)
            .or_else(|| outer.then(|| self.binding_enum(right)).flatten())
    }

    /// The first enum column referenced anywhere under `node`.
    fn enum_column_find(&self, node: NodeRef<'_>) -> Option<EnumColumn> {
        node.subtree().find_map(|position| match position.kind() {
            NodeKind::Column { .. } => self.column_enum(Column::from_ref(position)),
            _ => None,
        })
    }

    /// Reject when any of `nodes` references an enum column.
    fn enum_reject<'t>(
        &self,
        nodes: impl IntoIterator<Item = NodeRef<'t>>,
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

    fn query_check(&self, query: Query<'_>) -> ResolveResult<()> {
        self.order_by_check(query.order_by(), query.body(), "ORDER BY")?;
        match query.body() {
            Body::Select(select) => self.select_check(select),
            Body::SetOp(set_op) => {
                self.query_check(set_op.left())?;
                self.query_check(set_op.right())
            }
            Body::Values(_) => Ok(()),
        }
    }

    fn order_by_check<'t>(
        &self,
        clauses: Run<'t, OrderBy<'t>>,
        body: Body<'t>,
        position: &'static str,
    ) -> ResolveResult<()> {
        for clause in clauses {
            if let Some(found) = self.order_clause_enum_column(clause, body) {
                return Err(found.reject(position));
            }
        }
        Ok(())
    }

    /// The enum column an ORDER BY item orders by: in its expression, or
    /// the output it names by name or by ordinal.
    fn order_clause_enum_column(&self, clause: OrderBy<'_>, body: Body<'_>) -> Option<EnumColumn> {
        let expr = clause.expr();
        if let Some(found) = self.enum_column_find(expr.node_ref()) {
            return Some(found);
        }
        match expr.kind() {
            ScalarKind::Column(column)
                if matches!(
                    self.bindings.column_by_id(column.id()),
                    Some(ColumnTarget::Output)
                ) =>
            {
                self.output_columns(body)?
                    .into_iter()
                    .find(|output| output.name.as_ref() == Some(column.column()))?
                    .enum_column
            }
            ScalarKind::Literal(literal) => {
                let LiteralValue::Integer(ordinal) = literal.value() else {
                    return None;
                };
                let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
                self.output_columns(body)?
                    .into_iter()
                    .nth(index)?
                    .enum_column
            }
            _ => None,
        }
    }

    /// The output columns of a body: a set operation's are its left
    /// branch's; VALUES has none the gate can name.
    fn output_columns(&self, body: Body<'_>) -> Option<Vec<OutputColumn>> {
        let select = match body {
            Body::Select(select) => select,
            Body::SetOp(set_op) => return self.output_columns(set_op.left().body()),
            Body::Values(_) => return None,
        };
        let items = select.items()?;
        let mut outputs = Vec::with_capacity(items.len());
        for item in items {
            match item {
                SelectItem::Expr(expr) => {
                    let scalar = expr.expr();
                    let name = expr.alias().cloned().or_else(|| match scalar.kind() {
                        ScalarKind::Column(column) => Some(column.column().clone()),
                        _ => None,
                    });
                    outputs.push(OutputColumn {
                        name,
                        enum_column: self.enum_column_find(scalar.node_ref()),
                    });
                }
                SelectItem::Star(star) => {
                    for item in self.bindings.star_by_id(star.id()).unwrap_or(&[]) {
                        outputs.push(self.star_item_output(item));
                    }
                }
            }
        }
        Some(outputs)
    }

    fn star_item_output(&self, item: &StarItem) -> OutputColumn {
        match item {
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
                    enum_column: merged.and_then(|m| self.merged_enum(m.outer, &m.left, &m.right)),
                }
            }
        }
    }

    fn select_check(&self, select: Select<'_>) -> ResolveResult<()> {
        if let Some(items) = select.items() {
            for item in items {
                if let SelectItem::Expr(expr) = item {
                    self.scalar_check(expr.expr())?;
                }
            }
        }
        for source in select.from() {
            self.source_check(source)?;
        }
        if let Some(where_clause) = select.where_clause() {
            self.where_check(where_clause)?;
        }
        if let Some(having) = select.having() {
            self.where_check(having)?;
        }
        Ok(())
    }

    fn source_check(&self, source: Source<'_>) -> ResolveResult<()> {
        match source {
            Source::Table(_) => Ok(()),
            Source::Subquery(subquery) => self.query_check(subquery.query()),
            Source::CteRef(cte_ref) => self.query_check(cte_ref.query()),
            Source::Join(join) => {
                self.source_check(join.left())?;
                self.source_check(join.right())?;
                match join.qual() {
                    JoinQual::On(condition) => self.where_check(condition),
                    JoinQual::Using(_) | JoinQual::Natural | JoinQual::Cross => Ok(()),
                }
            }
        }
    }

    fn where_check(&self, expr: Where<'_>) -> ResolveResult<()> {
        match expr.kind() {
            WhereKind::Scalar(scalar) => self.scalar_check(scalar),
            WhereKind::Unary(unary) => self.where_check(unary.expr()),
            WhereKind::Binary(binary) => self.binary_check(binary),
            WhereKind::Multi(multi) => self.multi_check(multi),
            WhereKind::Subquery(subquery) => self.query_check(subquery.query()),
        }
    }

    fn binary_check(&self, binary: Binary<'_>) -> ResolveResult<()> {
        if binary_op_is_range(binary.op()) {
            self.enum_reject(
                [binary.lexpr().node_ref(), binary.rexpr().node_ref()],
                "range comparison",
            )?;
        }
        self.where_check(binary.lexpr())?;
        self.where_check(binary.rexpr())
    }

    fn multi_check(&self, multi: Multi<'_>) -> ResolveResult<()> {
        let order_dependent = multi_op_is_order_dependent(multi.op());
        for expr in multi.exprs() {
            if order_dependent {
                self.enum_reject([expr.node_ref()], "range comparison")?;
            }
            self.where_check(expr)?;
        }
        Ok(())
    }

    fn scalar_check(&self, expr: Scalar<'_>) -> ResolveResult<()> {
        match expr.kind() {
            ScalarKind::Column(_) | ScalarKind::Literal(_) => Ok(()),
            ScalarKind::Function(function) => self.function_check(function),
            ScalarKind::Case(case) => self.case_check(case),
            ScalarKind::Arithmetic(arithmetic) => {
                self.scalar_check(arithmetic.left())?;
                self.scalar_check(arithmetic.right())
            }
            ScalarKind::Subquery(subquery) => self.query_check(subquery.query()),
            ScalarKind::Array(array) => {
                for element in array.elements() {
                    self.scalar_check(element)?;
                }
                Ok(())
            }
            ScalarKind::TypeCast(cast) => self.scalar_check(cast.expr()),
        }
    }

    fn function_check(&self, function: Function<'_>) -> ResolveResult<()> {
        self.function_order_positions_check(function)?;
        if let Some(over) = function.over() {
            for expr in over.partition_by() {
                self.scalar_check(expr)?;
            }
        }
        for arg in function.args() {
            self.scalar_check(arg)?;
        }
        if let Some(filter) = function.filter() {
            self.where_check(filter)?;
        }
        Ok(())
    }

    /// `min`/`max` arguments, an aggregate's ORDER BY and a window's ORDER
    /// BY are order-dependent positions.
    fn function_order_positions_check(&self, function: Function<'_>) -> ResolveResult<()> {
        let name = function.name();
        if name.eq_ignore_ascii_case("min") || name.eq_ignore_ascii_case("max") {
            self.enum_reject(
                function.args().map(|arg| arg.node_ref()),
                "min/max aggregate",
            )?;
        }
        self.enum_reject(
            function.agg_order().map(|clause| clause.expr().node_ref()),
            "aggregate ORDER BY",
        )?;
        if let Some(over) = function.over() {
            self.enum_reject(
                over.order_by().map(|clause| clause.expr().node_ref()),
                "window ORDER BY",
            )?;
        }
        Ok(())
    }

    fn case_check(&self, case: Case<'_>) -> ResolveResult<()> {
        if let Some(arg) = case.arg() {
            self.scalar_check(arg)?;
        }
        for when in case.whens() {
            self.where_check(when.condition())?;
            self.scalar_check(when.result())?;
        }
        if let Some(default) = case.default() {
            self.scalar_check(default)?;
        }
        Ok(())
    }
}

/// One output column of a select list: its name, and the enum column it
/// carries when it is one.
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

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use postgres_types::{Kind, Type};

    use super::*;
    use crate::catalog::{Catalog, ColumnMetadata, ColumnPosition, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};

    fn severity_type() -> Type {
        Type::new(
            "severity".to_owned(),
            90001,
            Kind::Enum(vec![
                "low".to_owned(),
                "medium".to_owned(),
                "high".to_owned(),
            ]),
            "public".to_owned(),
        )
    }

    fn column(name: &str, position: i16, data_type: Type, is_primary_key: bool) -> ColumnMetadata {
        ColumnMetadata {
            name: name.into(),
            position: ColumnPosition::from_raw(position),
            type_oid: TypeOid::from_type(&data_type),
            data_type: data_type.clone(),
            type_name: data_type.name().into(),
            cache_type_name: match data_type.kind() {
                Kind::Enum(_) | Kind::Domain(_) => "text".into(),
                _ => data_type.name().into(),
            },
            is_primary_key,
        }
    }

    /// alerts(id int4 pk, sev severity, note text, dsev domain-over-enum,
    /// yr domain-over-int) and tags(id int4 pk, sev severity, label text).
    fn tables() -> Catalog {
        let domain_over_enum = Type::new(
            "sevdom".to_owned(),
            90002,
            Kind::Domain(severity_type()),
            "public".to_owned(),
        );
        let domain_over_int = Type::new(
            "pgc_year".to_owned(),
            90003,
            Kind::Domain(Type::INT4),
            "public".to_owned(),
        );
        let table = |oid: u32, name: &str, columns: Vec<ColumnMetadata>| TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(oid),
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns: ColumnStore::new(columns),
            indexes: Vec::new(),
        };
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(
            table(
                5001,
                "alerts",
                vec![
                    column("id", 1, Type::INT4, true),
                    column("sev", 2, severity_type(), false),
                    column("note", 3, Type::TEXT, false),
                    column("dsev", 4, domain_over_enum, false),
                    column("yr", 5, domain_over_int, false),
                ],
            )
            .into(),
        );
        tables.insert_overwrite(
            table(
                5002,
                "tags",
                vec![
                    column("id", 1, Type::INT4, true),
                    column("sev", 2, severity_type(), false),
                    column("label", 3, Type::TEXT, false),
                ],
            )
            .into(),
        );
        tables
    }

    /// The gate's verdict on one query: "allowed", or the rejection as
    /// displayed.
    fn verdict(sql: &str) -> String {
        let bound = ResolvedQuery::parse_resolve(sql, &tables(), &["public"]).expect("resolve");
        match bound.enum_order_dependence_check() {
            Ok(()) => "allowed".to_owned(),
            Err(e) => e.into_current_context().to_string(),
        }
    }

    const CORPUS: &[(&str, bool)] = &[
        ("SELECT id FROM alerts ORDER BY sev", true),
        ("SELECT id FROM alerts ORDER BY note, sev", true),
        ("SELECT id, sev FROM alerts WHERE sev = 'high'", false),
        ("SELECT id FROM alerts WHERE sev <> 'low'", false),
        ("SELECT id FROM alerts WHERE sev IN ('low', 'high')", false),
        ("SELECT id FROM alerts WHERE sev IS NULL", false),
        ("SELECT id FROM alerts WHERE note > 'a' ORDER BY id", false),
        ("SELECT id FROM alerts WHERE sev > 'medium'", true),
        ("SELECT id FROM alerts WHERE 'medium' < sev", true),
        (
            "SELECT id FROM alerts WHERE sev BETWEEN 'low' AND 'high'",
            true,
        ),
        ("SELECT max(sev) FROM alerts", true),
        ("SELECT min(sev) FROM alerts", true),
        ("SELECT count(sev) FROM alerts", false),
        ("SELECT count(*) FROM alerts", false),
        ("SELECT id FROM alerts ORDER BY dsev", true),
        ("SELECT id FROM alerts ORDER BY yr", false),
        ("SELECT id FROM alerts WHERE yr > 1999", false),
        (
            "SELECT s.id FROM (SELECT id FROM alerts ORDER BY sev LIMIT 5) s",
            true,
        ),
        ("SELECT sev, count(*) FROM alerts GROUP BY sev", false),
        ("SELECT id, sev FROM alerts ORDER BY 2", true),
        (
            "SELECT id, sev FROM alerts WHERE sev = 'low' ORDER BY 1",
            false,
        ),
        ("SELECT id FROM alerts ORDER BY 5", false),
        ("SELECT * FROM alerts ORDER BY 2", true),
        ("SELECT * FROM alerts ORDER BY sev", true),
        ("SELECT * FROM alerts ORDER BY 1", false),
        ("SELECT sev AS s, id FROM alerts ORDER BY s", true),
        ("SELECT note AS sev, id FROM alerts ORDER BY sev", false),
        (
            "SELECT a.* FROM alerts a JOIN tags t USING (sev) ORDER BY sev",
            true,
        ),
        (
            "SELECT * FROM alerts a LEFT JOIN tags t USING (sev) WHERE sev > 'low'",
            true,
        ),
        (
            "SELECT * FROM alerts a JOIN tags t USING (id) WHERE id > 1",
            false,
        ),
        (
            "SELECT s.sev FROM (SELECT sev FROM alerts) s WHERE s.sev > 'low'",
            true,
        ),
        (
            "SELECT s.sev FROM (SELECT sev FROM alerts) s WHERE s.sev = 'low'",
            false,
        ),
        (
            "SELECT id FROM alerts WHERE EXISTS (SELECT 1 FROM tags t WHERE t.sev > alerts.sev)",
            true,
        ),
        (
            "SELECT string_agg(note, ',' ORDER BY sev) FROM alerts",
            true,
        ),
        (
            "SELECT rank() OVER (PARTITION BY sev ORDER BY id) FROM alerts",
            false,
        ),
        ("SELECT rank() OVER (ORDER BY sev) FROM alerts", true),
        (
            "SELECT sev FROM alerts UNION SELECT sev FROM tags ORDER BY sev",
            true,
        ),
        (
            "SELECT id FROM alerts UNION SELECT id FROM tags ORDER BY id",
            false,
        ),
        (
            "SELECT CASE WHEN sev > 'low' THEN 1 ELSE 0 END FROM alerts",
            true,
        ),
        ("SELECT id FROM alerts WHERE sev::text > 'low'", true),
        ("SELECT id FROM alerts WHERE sev = ANY(ARRAY['low'])", false),
        (
            "WITH recent AS (SELECT id, sev FROM alerts WHERE sev > 'low') SELECT id FROM recent",
            true,
        ),
        (
            "WITH recent AS (SELECT id, sev FROM alerts) SELECT id FROM recent ORDER BY sev",
            true,
        ),
    ];

    #[test]
    fn test_gate_verdict_cases() {
        for (sql, rejected) in CORPUS {
            let verdict = verdict(sql);
            assert_eq!(
                verdict != "allowed",
                *rejected,
                "verdict for {sql}: {verdict}"
            );
        }
    }
}
