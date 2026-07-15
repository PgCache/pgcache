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

use crate::query::ast::{AstNode, BinaryOp, LiteralValue, MultiOp};

use super::{
    ResolveError, ResolveResult, ResolvedCaseExpr, ResolvedColumnNode, ResolvedFunctionCall,
    ResolvedOrderByClause, ResolvedQueryBody, ResolvedQueryExpr, ResolvedScalarExpr,
    ResolvedSelectColumns, ResolvedSelectNode, ResolvedTableSource, ResolvedWhereExpr,
};

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
fn enum_column_find<N: AstNode + ?Sized>(node: &N) -> Option<&ResolvedColumnNode> {
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

fn order_by_check(
    clauses: &[ResolvedOrderByClause],
    body: &ResolvedQueryBody,
    position: &'static str,
) -> ResolveResult<()> {
    for clause in clauses {
        if let Some(c) = enum_column_find(&clause.expr) {
            return Err(reject(c, position));
        }
        // A set-op ORDER BY referencing an output name hides the column
        // behind an Identifier; check the named output expression instead.
        if let ResolvedScalarExpr::Identifier(name) = &clause.expr
            && let Some(c) = output_enum_column_find(body, name)
        {
            return Err(reject(c, position));
        }
        // An ordinal (`ORDER BY 2`) resolves as an integer Literal but
        // sorts by the referenced output column.
        if let ResolvedScalarExpr::Literal(LiteralValue::Integer(ordinal)) = &clause.expr
            && let Some(c) = output_ordinal_enum_column_find(body, *ordinal)
        {
            return Err(reject(c, position));
        }
    }
    Ok(())
}

/// Resolve an ORDER BY ordinal (1-based) to the select list (leftmost
/// SELECT of a set operation) and return its enum column, if any.
fn output_ordinal_enum_column_find(
    body: &ResolvedQueryBody,
    ordinal: i64,
) -> Option<&ResolvedColumnNode> {
    match body {
        ResolvedQueryBody::Select(select) => {
            let ResolvedSelectColumns::Columns(cols) = &select.columns else {
                return None;
            };
            let index = usize::try_from(ordinal.checked_sub(1)?).ok()?;
            cols.get(index).and_then(|c| enum_column_find(&c.expr))
        }
        ResolvedQueryBody::SetOp(set_op) => {
            output_ordinal_enum_column_find(&set_op.left.body, ordinal)
        }
        ResolvedQueryBody::Values(_) => None,
    }
}

/// Resolve an ORDER BY output-name reference to the select list (leftmost
/// SELECT of a set operation) and return its enum column, if any.
fn output_enum_column_find<'a>(
    body: &'a ResolvedQueryBody,
    name: &str,
) -> Option<&'a ResolvedColumnNode> {
    match body {
        ResolvedQueryBody::Select(select) => {
            let ResolvedSelectColumns::Columns(cols) = &select.columns else {
                return None;
            };
            cols.iter()
                .find(|c| c.output_name().is_some_and(|n| n == name))
                .and_then(|c| enum_column_find(&c.expr))
        }
        ResolvedQueryBody::SetOp(set_op) => output_enum_column_find(&set_op.left.body, name),
        ResolvedQueryBody::Values(_) => None,
    }
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

fn where_check(expr: &ResolvedWhereExpr) -> ResolveResult<()> {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => scalar_check(scalar),
        ResolvedWhereExpr::Unary(unary) => where_check(&unary.expr),
        ResolvedWhereExpr::Binary(binary) => {
            if binary_op_is_range(binary.op)
                && let Some(c) = enum_column_find(binary.lexpr.as_ref())
                    .or_else(|| enum_column_find(binary.rexpr.as_ref()))
            {
                return Err(reject(c, "range comparison"));
            }
            where_check(&binary.lexpr)?;
            where_check(&binary.rexpr)
        }
        ResolvedWhereExpr::Multi(multi) => {
            let order_dependent = match multi.op {
                MultiOp::Between
                | MultiOp::NotBetween
                | MultiOp::BetweenSymmetric
                | MultiOp::NotBetweenSymmetric => true,
                MultiOp::Any { comparison } | MultiOp::All { comparison } => {
                    binary_op_is_range(comparison)
                }
                MultiOp::In | MultiOp::NotIn => false,
            };
            for e in &multi.exprs {
                if order_dependent && let Some(c) = enum_column_find(e) {
                    return Err(reject(c, "range comparison"));
                }
                where_check(e)?;
            }
            Ok(())
        }
        // Predicate sublinks carry equality semantics (IN/NOT IN; ALL is
        // restricted to `<>` at AST conversion), so the test expression is
        // safe; the subquery body still needs its own walk.
        ResolvedWhereExpr::Subquery { query, .. } => query_check(query),
    }
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
    if function.name.eq_ignore_ascii_case("min") || function.name.eq_ignore_ascii_case("max") {
        for arg in &function.args {
            if let Some(c) = enum_column_find(arg) {
                return Err(reject(c, "min/max aggregate"));
            }
        }
    }
    for clause in &function.agg_order {
        if let Some(c) = enum_column_find(&clause.expr) {
            return Err(reject(c, "aggregate ORDER BY"));
        }
    }
    if let Some(over) = &function.over {
        for clause in &over.order_by {
            if let Some(c) = enum_column_find(&clause.expr) {
                return Err(reject(c, "window ORDER BY"));
            }
        }
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
    use tokio_postgres::types::{Kind, Type};

    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::oid::Oid;
    use crate::query::ast::query_expr_parse;

    use super::super::query_expr_resolve;
    use super::*;

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
            type_oid: data_type.oid(),
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
