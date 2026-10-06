//! Admission gate for order-dependent enum usage (PGC-266).
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

use std::ops::ControlFlow;

use rootcause::Report;

use super::{
    ResolveError, ResolveResult, ResolvedBinaryExpr, ResolvedCaseExpr, ResolvedColumnNode,
    ResolvedFunctionCall, ResolvedMultiExpr, ResolvedOrderByClause, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectColumn, ResolvedSelectColumns,
    ResolvedSelectNode, ResolvedTableSource, ResolvedWhereExpr,
};
use crate::query::ast::{AstNode, BinaryOp, LiteralValue, MultiOp};

/// Check a resolved query for order-dependent enum usage. `Err` carries the
/// first offending column and its position; registration turns it into a
/// forward.
pub fn enum_order_dependence_check(query: &ResolvedQueryExpr) -> ResolveResult<()> {
    query_check(query)
}

fn reject(column: &ResolvedColumnNode, position: &'static str) -> Report<ResolveError> {
    Report::from(ResolveError::EnumOrderDependentUsage {
        table: column.table.clone(),
        column: column.column.clone(),
        position,
    })
}

/// First enum-ordered column reachable under `node`, if any.
fn enum_column_find<N: AstNode>(node: &N) -> Option<&ResolvedColumnNode> {
    match node.try_for_each_node::<ResolvedColumnNode, &ResolvedColumnNode>(&mut |c| {
        if c.column_metadata.is_enum_ordered() {
            ControlFlow::Break(c)
        } else {
            ControlFlow::Continue(())
        }
    }) {
        ControlFlow::Break(c) => Some(c),
        ControlFlow::Continue(()) => None,
    }
}

/// Reject at `position` if any of `nodes` reaches an enum-ordered column.
fn enum_reject<'a, T: AstNode + 'a>(
    nodes: impl IntoIterator<Item = &'a T>,
    position: &'static str,
) -> ResolveResult<()> {
    match nodes.into_iter().find_map(|node| enum_column_find(node)) {
        Some(c) => Err(reject(c, position)),
        None => Ok(()),
    }
}

fn order_by_check(
    clauses: &[ResolvedOrderByClause],
    body: &ResolvedQueryBody,
    position: &'static str,
) -> ResolveResult<()> {
    for clause in clauses {
        if let Some(c) = order_clause_enum_column(clause, body) {
            return Err(reject(c, position));
        }
    }
    Ok(())
}

/// The enum column an ORDER BY clause sorts by, if any.
fn order_clause_enum_column<'a>(
    clause: &'a ResolvedOrderByClause,
    body: &'a ResolvedQueryBody,
) -> Option<&'a ResolvedColumnNode> {
    if let Some(c) = enum_column_find(&clause.expr) {
        return Some(c);
    }
    // A set-op ORDER BY referencing an output name hides the column behind
    // an Identifier; check the named output expression instead.
    if let ResolvedScalarExpr::Identifier(name) = &clause.expr {
        return output_enum_column_find(body, name);
    }
    // An ordinal (`ORDER BY 2`) resolves as an integer Literal but sorts by
    // the referenced output column.
    if let ResolvedScalarExpr::Literal(LiteralValue::Integer(ordinal)) = &clause.expr {
        return output_ordinal_enum_column_find(body, *ordinal);
    }
    None
}

/// The select list ORDER BY output references resolve against: the SELECT's,
/// or the leftmost SELECT's for a set operation.
fn leftmost_select_columns(body: &ResolvedQueryBody) -> Option<&[ResolvedSelectColumn]> {
    match body {
        ResolvedQueryBody::Select(select) => match &select.columns {
            ResolvedSelectColumns::Columns(cols) => Some(cols),
            ResolvedSelectColumns::None => None,
        },
        ResolvedQueryBody::SetOp(set_op) => leftmost_select_columns(&set_op.left.body),
        ResolvedQueryBody::Values(_) => None,
    }
}

/// The enum column of the select-list entry at an ORDER BY ordinal (1-based).
fn output_ordinal_enum_column_find(
    body: &ResolvedQueryBody,
    ordinal: i64,
) -> Option<&ResolvedColumnNode> {
    let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
    let column = leftmost_select_columns(body)?.get(index)?;
    enum_column_find(&column.expr)
}

/// The enum column of the select-list entry an ORDER BY output name names.
fn output_enum_column_find<'a>(
    body: &'a ResolvedQueryBody,
    name: &str,
) -> Option<&'a ResolvedColumnNode> {
    let column = leftmost_select_columns(body)?
        .iter()
        .find(|c| c.output_name().is_some_and(|n| n == name))?;
    enum_column_find(&column.expr)
}

fn query_check(query: &ResolvedQueryExpr) -> ResolveResult<()> {
    order_by_check(&query.order_by, &query.body, "ORDER BY")?;
    match &query.body {
        ResolvedQueryBody::Select(select) => select_check(select),
        ResolvedQueryBody::SetOp(set_op) => {
            query_check(&set_op.left)?;
            query_check(&set_op.right)
        }
        ResolvedQueryBody::Values(_) => Ok(()),
    }
}

fn select_check(select: &ResolvedSelectNode) -> ResolveResult<()> {
    if let ResolvedSelectColumns::Columns(cols) = &select.columns {
        for col in cols {
            scalar_check(&col.expr)?;
        }
    }
    for source in &select.from {
        table_source_check(source)?;
    }
    if let Some(where_clause) = &select.where_clause {
        where_check(where_clause)?;
    }
    // group_by is equality semantics — allowed.
    if let Some(having) = &select.having {
        where_check(having)?;
    }
    Ok(())
}

fn table_source_check(source: &ResolvedTableSource) -> ResolveResult<()> {
    match source {
        ResolvedTableSource::Table(_) => Ok(()),
        ResolvedTableSource::Subquery(subquery) => query_check(&subquery.query),
        ResolvedTableSource::Join(join) => {
            table_source_check(&join.left)?;
            table_source_check(&join.right)?;
            match &join.qual {
                super::ResolvedJoinQual::On(cond) => where_check(cond),
                // USING/NATURAL merge and the synthesized predicate are
                // equality semantics; Cross has no qualifier.
                super::ResolvedJoinQual::Using { .. } | super::ResolvedJoinQual::Cross => Ok(()),
            }
        }
    }
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

fn where_check(expr: &ResolvedWhereExpr) -> ResolveResult<()> {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => scalar_check(scalar),
        ResolvedWhereExpr::Unary(unary) => where_check(&unary.expr),
        ResolvedWhereExpr::Binary(binary) => binary_check(binary),
        ResolvedWhereExpr::Multi(multi) => multi_check(multi),
        // Predicate sublinks carry equality semantics (IN/NOT IN; ALL is
        // restricted to `<>` at AST conversion), so the test expression is
        // safe; the subquery body still needs its own walk.
        ResolvedWhereExpr::Subquery { query, .. } => query_check(query),
    }
}

fn binary_check(binary: &ResolvedBinaryExpr) -> ResolveResult<()> {
    if binary_op_is_range(binary.op) {
        enum_reject([&*binary.lexpr, &*binary.rexpr], "range comparison")?;
    }
    where_check(&binary.lexpr)?;
    where_check(&binary.rexpr)
}

fn multi_check(multi: &ResolvedMultiExpr) -> ResolveResult<()> {
    let order_dependent = multi_op_is_order_dependent(multi.op);
    for e in &multi.exprs {
        if order_dependent {
            enum_reject([e], "range comparison")?;
        }
        where_check(e)?;
    }
    Ok(())
}

fn scalar_check(expr: &ResolvedScalarExpr) -> ResolveResult<()> {
    match expr {
        ResolvedScalarExpr::Column(_)
        | ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Literal(_) => Ok(()),
        ResolvedScalarExpr::Function(function) => function_check(function),
        ResolvedScalarExpr::Case(case) => case_check(case),
        ResolvedScalarExpr::Arithmetic(arithmetic) => {
            scalar_check(&arithmetic.left)?;
            scalar_check(&arithmetic.right)
        }
        ResolvedScalarExpr::Subquery(query, _) => query_check(query),
        ResolvedScalarExpr::Array(elems) => {
            for e in elems {
                scalar_check(e)?;
            }
            Ok(())
        }
        ResolvedScalarExpr::TypeCast { expr, .. } => scalar_check(expr),
    }
}

fn function_check(function: &ResolvedFunctionCall) -> ResolveResult<()> {
    function_order_positions_check(function)?;
    if let Some(over) = &function.over {
        // partition_by is equality semantics — allowed.
        for e in &over.partition_by {
            scalar_check(e)?;
        }
    }
    for arg in &function.args {
        scalar_check(arg)?;
    }
    if let Some(filter) = &function.agg_filter {
        where_check(filter)?;
    }
    Ok(())
}

/// The order-dependent positions of a call: min/max arguments, the aggregate
/// ORDER BY, and the window ORDER BY.
fn function_order_positions_check(function: &ResolvedFunctionCall) -> ResolveResult<()> {
    let min_max =
        function.name.eq_ignore_ascii_case("min") || function.name.eq_ignore_ascii_case("max");
    if min_max {
        enum_reject(&function.args, "min/max aggregate")?;
    }
    enum_reject(
        function.agg_order.iter().map(|clause| &clause.expr),
        "aggregate ORDER BY",
    )?;
    if let Some(over) = &function.over {
        enum_reject(
            over.order_by.iter().map(|clause| &clause.expr),
            "window ORDER BY",
        )?;
    }
    Ok(())
}

fn case_check(case: &ResolvedCaseExpr) -> ResolveResult<()> {
    // Simple-CASE arg tests equality against WHEN values — safe.
    if let Some(arg) = &case.arg {
        scalar_check(arg)?;
    }
    for when in &case.whens {
        where_check(&when.condition)?;
        scalar_check(&when.result)?;
    }
    if let Some(default) = &case.default {
        scalar_check(default)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use postgres_types::{Kind, Type};

    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::oid::{Oid, TypeOid};
    use crate::query::ast::query_expr_parse;
    use crate::query::resolved::query_expr_resolve;

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
            position,
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
    /// yr domain-over-int)
    fn tables() -> BiHashMap<TableMetadata> {
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
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(TableMetadata {
            replica_identity_full: false,
            relation_oid: Oid::from_raw(5001),
            name: "alerts".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns: ColumnStore::new([
                column("id", 1, Type::INT4, true),
                column("sev", 2, severity_type(), false),
                column("note", 3, Type::TEXT, false),
                column("dsev", 4, domain_over_enum, false),
                column("yr", 5, domain_over_int, false),
            ]),
            indexes: Vec::new(),
        });
        tables
    }

    fn gate(sql: &str) -> ResolveResult<()> {
        let query = query_expr_parse(sql).expect("parse");
        let resolved = query_expr_resolve(&query, &tables(), &["public"]).expect("resolve");
        enum_order_dependence_check(&resolved)
    }

    fn assert_rejected(sql: &str) {
        let err = gate(sql).expect_err(&format!("expected rejection: {sql}"));
        assert!(matches!(
            err.into_current_context(),
            ResolveError::EnumOrderDependentUsage { .. }
        ));
    }

    fn assert_allowed(sql: &str) {
        gate(sql).unwrap_or_else(|e| panic!("expected allowed: {sql}: {e:?}"));
    }

    #[test]
    fn test_order_by_enum_rejected() {
        assert_rejected("SELECT id FROM alerts ORDER BY sev");
        assert_rejected("SELECT id FROM alerts ORDER BY note, sev");
    }

    #[test]
    fn test_equality_and_in_allowed() {
        assert_allowed("SELECT id, sev FROM alerts WHERE sev = 'high'");
        assert_allowed("SELECT id FROM alerts WHERE sev <> 'low'");
        assert_allowed("SELECT id FROM alerts WHERE sev IN ('low', 'high')");
        assert_allowed("SELECT id FROM alerts WHERE sev IS NULL");
        assert_allowed("SELECT id FROM alerts WHERE note > 'a' ORDER BY id");
    }

    #[test]
    fn test_range_comparisons_rejected() {
        assert_rejected("SELECT id FROM alerts WHERE sev > 'medium'");
        assert_rejected("SELECT id FROM alerts WHERE 'medium' < sev");
        assert_rejected("SELECT id FROM alerts WHERE sev BETWEEN 'low' AND 'high'");
    }

    #[test]
    fn test_min_max_rejected_count_allowed() {
        assert_rejected("SELECT max(sev) FROM alerts");
        assert_rejected("SELECT min(sev) FROM alerts");
        assert_allowed("SELECT count(sev) FROM alerts");
        assert_allowed("SELECT count(*) FROM alerts");
    }

    #[test]
    fn test_domain_over_enum_rejected_plain_domain_allowed() {
        assert_rejected("SELECT id FROM alerts ORDER BY dsev");
        assert_allowed("SELECT id FROM alerts ORDER BY yr");
        assert_allowed("SELECT id FROM alerts WHERE yr > 1999");
    }

    #[test]
    fn test_derived_table_order_by_enum_rejected() {
        assert_rejected("SELECT s.id FROM (SELECT id FROM alerts ORDER BY sev LIMIT 5) s");
    }

    #[test]
    fn test_group_by_enum_allowed() {
        assert_allowed("SELECT sev, count(*) FROM alerts GROUP BY sev");
    }

    #[test]
    fn test_order_by_ordinal_enum_rejected() {
        assert_rejected("SELECT id, sev FROM alerts ORDER BY 2");
        assert_allowed("SELECT id, sev FROM alerts WHERE sev = 'low' ORDER BY 1");
        // Out-of-range ordinal would error on origin; nothing to gate.
        assert_allowed("SELECT id FROM alerts ORDER BY 5");
    }
}
