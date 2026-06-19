//! AST transform: replace concrete literals in the *predicate* positions of a
//! resolved query with `$N` parameter placeholders, returning the parameterized
//! query and the replaced literals in placeholder order (PGC-294).
//!
//! The serve SQL (`deparsed_sql`) is deparsed from the schema-qualified
//! `ResolvedQueryExpr`, so the shape must be derived from the resolved form. The
//! prepared params are bound with type OID 0 (inferred from context), which is
//! only sound where PG has type context — so the walk is restricted to
//! **WHERE / HAVING / JOIN-ON predicate expressions** (and nested subqueries'
//! predicates). It deliberately leaves these inline:
//!
//! - **SELECT target list** — a bare projected literal (`SELECT 'x'`) has no type
//!   context, so `SELECT $1` fails Parse with "could not determine data type".
//! - **ORDER BY** — `ORDER BY 1` is a positional reference; `ORDER BY $1` would
//!   sort by a constant instead — a silent *wrong-results* change.
//! - **VALUES rows** — a `VALUES ($1)` source has no inferable type context.
//!
//! These positions are not where literal cardinality (the plan-explosion driver)
//! lives; predicate literals are, and they always sit beside a typed column/operator.
//! The top-level LIMIT count/offset are served as separate `$`-params (appended
//! after the shape body) and are likewise not parameterized here.
//!
//! Ordering follows the same invariant as the `QueryExpr` variant: each
//! `Parameter` node carries its number, so `$N` resolves to `literals[N-1]`
//! wherever it sits in the deparsed SQL. The round-trip test is the lock.

use ecow::EcoString;

use crate::query::ast::LiteralValue;
use crate::query::resolved::{
    ResolvedCaseExpr, ResolvedFrameBound, ResolvedFunctionCall, ResolvedQueryBody,
    ResolvedQueryExpr, ResolvedScalarExpr, ResolvedSelectNode, ResolvedTableSource,
    ResolvedWhereExpr, ResolvedWindowSpec,
};

/// Whether a literal becomes a bound parameter. Only the four scalar forms whose
/// bind type PG can infer from context are parameterized. Everything else stays
/// inline in the shape SQL:
/// - `StringWithCast`/`Array` carry an explicit `::cast` that disambiguates their
///   type; binding them as text would drop the cast and rely on context inference,
///   which is not always sound (e.g. `'2024-01-01'::date`, `'{1,2}'::int4[]`).
/// - `Null`/`NullWithCast` have no unambiguous bind type.
/// - `Parameter` is already a placeholder.
///
/// Keeping cast/array literals inline means two queries differing only in such a
/// literal get distinct shapes — accepted minor proliferation for those rarer
/// forms in exchange for never binding a value at the wrong type.
fn literal_is_parameterizable(literal: &LiteralValue) -> bool {
    match literal {
        LiteralValue::String(_)
        | LiteralValue::Integer(_)
        | LiteralValue::Float(_)
        | LiteralValue::Boolean(_) => true,
        LiteralValue::StringWithCast(_, _)
        | LiteralValue::Array(_, _)
        | LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_) => false,
    }
}

/// Replace every parameterizable literal in `resolved` with a `$N` placeholder
/// (numbered `$1..$k` in walk order), returning the parameterized query and the
/// replaced literals in placeholder order (`literals[N-1]` binds to `$N`). The
/// top-level LIMIT clause is left untouched (served separately).
pub fn resolved_query_expr_parameterize(
    resolved: &ResolvedQueryExpr,
) -> (ResolvedQueryExpr, Vec<LiteralValue>) {
    let mut shaped = resolved.clone();
    let mut literals: Vec<LiteralValue> = Vec::new();
    {
        let mut visit = |literal: &mut LiteralValue| {
            if literal_is_parameterizable(literal) {
                // `len()` is the count before this push, so the first literal
                // becomes `$1` and binds to `literals[0]`.
                let placeholder = EcoString::from(format!("${}", literals.len() + 1));
                literals.push(std::mem::replace(
                    literal,
                    LiteralValue::Parameter(placeholder),
                ));
            }
        };
        query_expr_walk(&mut shaped, &mut visit);
    }
    (shaped, literals)
}

fn query_expr_walk<F: FnMut(&mut LiteralValue)>(expr: &mut ResolvedQueryExpr, visit: &mut F) {
    query_body_walk(&mut expr.body, visit);
    // ORDER BY is intentionally skipped: a bare integer there is a positional
    // column reference, and the top-level LIMIT/OFFSET are bound separately.
}

fn query_body_walk<F: FnMut(&mut LiteralValue)>(body: &mut ResolvedQueryBody, visit: &mut F) {
    match body {
        ResolvedQueryBody::Select(node) => select_node_walk(node, visit),
        // VALUES rows have no type context for a bound `$N`; left inline.
        ResolvedQueryBody::Values(_) => {}
        ResolvedQueryBody::SetOp(set_op) => {
            query_expr_walk(&mut set_op.left, visit);
            query_expr_walk(&mut set_op.right, visit);
        }
    }
}

fn select_node_walk<F: FnMut(&mut LiteralValue)>(node: &mut ResolvedSelectNode, visit: &mut F) {
    // The SELECT target list is intentionally skipped: a projected literal has no
    // type context, so a bound `$N` there fails Parse. Only predicate positions
    // (JOIN-ON via `from`, WHERE, HAVING) are parameterized.
    for source in &mut node.from {
        table_source_walk(source, visit);
    }
    if let Some(where_clause) = &mut node.where_clause {
        where_walk(where_clause, visit);
    }
    if let Some(having) = &mut node.having {
        where_walk(having, visit);
    }
    // `group_by` is `Vec<ResolvedColumnNode>` — columns only, no literals.
}

fn table_source_walk<F: FnMut(&mut LiteralValue)>(source: &mut ResolvedTableSource, visit: &mut F) {
    match source {
        ResolvedTableSource::Table(_) => {}
        ResolvedTableSource::Subquery(sub) => query_expr_walk(&mut sub.query, visit),
        ResolvedTableSource::Join(join) => {
            if let crate::query::resolved::ResolvedJoinQual::On(cond) = &mut join.qual {
                where_walk(cond, visit);
            }
            table_source_walk(&mut join.left, visit);
            table_source_walk(&mut join.right, visit);
        }
    }
}

fn where_walk<F: FnMut(&mut LiteralValue)>(expr: &mut ResolvedWhereExpr, visit: &mut F) {
    match expr {
        ResolvedWhereExpr::Scalar(scalar) => scalar_walk(scalar, visit),
        ResolvedWhereExpr::Unary(unary) => where_walk(&mut unary.expr, visit),
        ResolvedWhereExpr::Binary(binary) => {
            where_walk(&mut binary.lexpr, visit);
            where_walk(&mut binary.rexpr, visit);
        }
        ResolvedWhereExpr::Multi(multi) => {
            for e in &mut multi.exprs {
                where_walk(e, visit);
            }
        }
        ResolvedWhereExpr::Subquery {
            query, test_expr, ..
        } => {
            query_expr_walk(query, visit);
            if let Some(test) = test_expr {
                scalar_walk(test, visit);
            }
        }
    }
}

fn scalar_walk<F: FnMut(&mut LiteralValue)>(expr: &mut ResolvedScalarExpr, visit: &mut F) {
    match expr {
        ResolvedScalarExpr::Literal(literal) => visit(literal),
        ResolvedScalarExpr::Column(_) | ResolvedScalarExpr::Identifier(_) => {}
        ResolvedScalarExpr::Arithmetic(arith) => {
            scalar_walk(&mut arith.left, visit);
            scalar_walk(&mut arith.right, visit);
        }
        ResolvedScalarExpr::Function(func) => function_walk(func, visit),
        ResolvedScalarExpr::Case(case) => case_walk(case, visit),
        ResolvedScalarExpr::Subquery(query, _) => query_expr_walk(query, visit),
        ResolvedScalarExpr::Array(elements) => {
            for element in elements {
                scalar_walk(element, visit);
            }
        }
        ResolvedScalarExpr::TypeCast { expr, .. } => scalar_walk(expr, visit),
    }
}

fn function_walk<F: FnMut(&mut LiteralValue)>(func: &mut ResolvedFunctionCall, visit: &mut F) {
    for arg in &mut func.args {
        scalar_walk(arg, visit);
    }
    for clause in &mut func.agg_order {
        scalar_walk(&mut clause.expr, visit);
    }
    if let Some(filter) = &mut func.agg_filter {
        where_walk(filter, visit);
    }
    if let Some(over) = &mut func.over {
        window_walk(over, visit);
    }
}

fn window_walk<F: FnMut(&mut LiteralValue)>(window: &mut ResolvedWindowSpec, visit: &mut F) {
    for partition in &mut window.partition_by {
        scalar_walk(partition, visit);
    }
    for clause in &mut window.order_by {
        scalar_walk(&mut clause.expr, visit);
    }
    if let Some(frame) = &mut window.frame {
        frame_bound_walk(&mut frame.start, visit);
        frame_bound_walk(&mut frame.end, visit);
    }
}

fn case_walk<F: FnMut(&mut LiteralValue)>(case: &mut ResolvedCaseExpr, visit: &mut F) {
    if let Some(arg) = &mut case.arg {
        scalar_walk(arg, visit);
    }
    for when in &mut case.whens {
        where_walk(&mut when.condition, visit);
        scalar_walk(&mut when.result, visit);
    }
    if let Some(default) = &mut case.default {
        scalar_walk(default, visit);
    }
}

fn frame_bound_walk<F: FnMut(&mut LiteralValue)>(bound: &mut ResolvedFrameBound, visit: &mut F) {
    match bound {
        ResolvedFrameBound::OffsetPreceding(expr) | ResolvedFrameBound::OffsetFollowing(expr) => {
            scalar_walk(expr, visit);
        }
        ResolvedFrameBound::UnboundedPreceding
        | ResolvedFrameBound::CurrentRow
        | ResolvedFrameBound::UnboundedFollowing => {}
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{ColumnMetadata, ColumnStore, Oid, TableMetadata};
    use crate::query::ast::{Deparse, query_expr_parse};
    use crate::query::resolve::query_expr_resolve;
    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    fn users_table() -> TableMetadata {
        let columns = ColumnStore::new([
            ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            },
            ColumnMetadata {
                name: "name".into(),
                position: 2,
                type_oid: 25,
                data_type: Type::TEXT,
                type_name: "text".into(),
                cache_type_name: "text".into(),
                is_primary_key: false,
            },
        ]);
        TableMetadata {
            relation_oid: Oid::from_raw(1001),
            name: "users".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".into()],
            columns,
            indexes: Vec::new(),
        }
    }

    fn resolve(sql: &str) -> ResolvedQueryExpr {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(users_table());
        let query_expr = query_expr_parse(sql).expect("parse");
        query_expr_resolve(&query_expr, &tables, &["public"]).expect("resolve")
    }

    fn deparse(query: &ResolvedQueryExpr) -> String {
        let mut buf = String::new();
        query.deparse(&mut buf);
        buf
    }

    /// Parameterize, then re-inject the collected literals at their placeholders
    /// using the same walk; the result must reproduce the original query.
    fn assert_roundtrip(sql: &str, expected_params: usize) {
        let original = resolve(sql);
        let (shaped, literals) = resolved_query_expr_parameterize(&original);
        assert_eq!(literals.len(), expected_params, "param count for: {sql}");

        let mut restored = shaped.clone();
        {
            let mut inject = |literal: &mut LiteralValue| {
                if let LiteralValue::Parameter(placeholder) = literal {
                    let n: usize = placeholder
                        .strip_prefix('$')
                        .and_then(|s| s.parse().ok())
                        .expect("placeholder is $N");
                    *literal = literals[n - 1].clone();
                }
            };
            query_expr_walk(&mut restored, &mut inject);
        }
        assert_eq!(
            deparse(&restored),
            deparse(&original),
            "round-trip mismatch; shape: {}",
            deparse(&shaped),
        );
    }

    #[test]
    fn test_resolved_parameterize_single_equality() {
        assert_roundtrip("SELECT id, name FROM users WHERE id = 42", 1);
    }

    #[test]
    fn test_resolved_parameterize_multi_predicate_order() {
        assert_roundtrip("SELECT id FROM users WHERE id = 7 AND name = 'alice'", 2);
    }

    /// Resolved queries differing only in literal values share a shape — what
    /// the serve-side prepared-statement sharing relies on.
    #[test]
    fn test_resolved_parameterize_shape_shared() {
        let (a, _) =
            resolved_query_expr_parameterize(&resolve("SELECT id FROM users WHERE name = 'user1'"));
        let (b, _) =
            resolved_query_expr_parameterize(&resolve("SELECT id FROM users WHERE name = 'user2'"));
        assert_eq!(deparse(&a), deparse(&b));
        assert!(deparse(&a).contains("$1"));
        assert!(!deparse(&a).contains("user1"));
    }

    #[test]
    fn test_resolved_parameterize_values_in_order() {
        let (_, literals) = resolved_query_expr_parameterize(&resolve(
            "SELECT id FROM users WHERE name = 'first' AND id = 2",
        ));
        assert_eq!(literals.len(), 2);
        assert_eq!(literals[0], LiteralValue::String("first".into()));
    }

    /// A projected literal has no type context for a bound `$N`, so it stays inline.
    #[test]
    fn test_resolved_parameterize_skips_target_list() {
        let (shaped, literals) =
            resolved_query_expr_parameterize(&resolve("SELECT 7, name FROM users WHERE id = 9"));
        // Only the WHERE literal is parameterized; the projected `7` stays inline.
        assert_eq!(literals, vec![LiteralValue::Integer(9)]);
        let sql = deparse(&shaped);
        assert!(sql.contains('7'), "projected literal inline: {sql}");
    }

    /// `ORDER BY 1` is a positional reference — parameterizing it would change the
    /// sort, so ORDER BY is left untouched.
    #[test]
    fn test_resolved_parameterize_skips_order_by() {
        let (_, literals) = resolved_query_expr_parameterize(&resolve(
            "SELECT id, name FROM users WHERE id = 3 ORDER BY 1",
        ));
        assert_eq!(literals, vec![LiteralValue::Integer(3)]);
    }

    /// Pins the ORDER BY split (PGC-322): a predicate-position literal (the
    /// HAVING comparison) is parameterized, while a query-level `ORDER BY 1`
    /// (positional) is left inline. The walk descends predicate-position
    /// expressions, including aggregate internals (args / aggregate ORDER BY)
    /// reached through them, but never the query-level ORDER BY.
    #[test]
    fn test_resolved_parameterize_having_predicate_but_not_order_by() {
        let (shaped, literals) = resolved_query_expr_parameterize(&resolve(
            "SELECT id FROM users GROUP BY id HAVING count(*) > 0 ORDER BY 1",
        ));
        // Only HAVING's `0` is parameterized; the positional `ORDER BY 1` is not.
        assert_eq!(literals, vec![LiteralValue::Integer(0)]);
        let sql = deparse(&shaped);
        assert!(
            sql.contains("ORDER BY 1"),
            "positional ORDER BY stays inline: {sql}"
        );
    }
}
