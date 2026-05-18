//! Constant folding for arithmetic on literal operands.
//!
//! Walks a `QueryExpr` and replaces `ScalarExpr::Arithmetic` nodes whose
//! operands fold to a single `LiteralValue`. The pass is bottom-up: nested
//! arithmetic is folded inside-out so the outer node sees freshly-folded
//! literals at each step.
//!
//! Without this pass, the cache treats e.g. `WHERE user_id = 12345 % 10000 + 1`
//! and `WHERE user_id = 67890 % 10000 + 1` as distinct queries (different
//! fingerprints embedding the literal arithmetic tree), even though both
//! reduce to `WHERE user_id = N + 1`.
//!
//! Fold rules:
//! - `Integer op Integer`: checked arithmetic; overflow → leave unfolded.
//! - `Float op Float` / mixed `Integer`+`Float`: promote to f64; `NaN` → leave
//!   unfolded.
//! - Division or modulo by zero: leave unfolded (origin raises the error).
//! - Any non-numeric operand (string, bool, null, parameter, array): not
//!   folded.

use ordered_float::NotNan;

use crate::query::ast::{
    ArithmeticOp, JoinQual, LiteralValue, QueryBody, QueryExpr, ScalarExpr, SelectColumn,
    SelectColumns, SelectNode, TableSource, WhereExpr,
};

/// Fold pure-literal arithmetic subtrees in `expr` in place.
pub fn query_expr_constant_fold(expr: &mut QueryExpr) {
    query_expr_fold(expr);
}

fn query_expr_fold(expr: &mut QueryExpr) {
    for cte in &mut expr.ctes {
        query_expr_fold(&mut cte.query);
    }
    query_body_fold(&mut expr.body);
    for ob in &mut expr.order_by {
        scalar_expr_fold(&mut ob.expr);
    }
}

fn query_body_fold(body: &mut QueryBody) {
    match body {
        QueryBody::Select(node) => select_node_fold(node),
        QueryBody::Values(_) => {}
        QueryBody::SetOp(set_op) => {
            query_expr_fold(&mut set_op.left);
            query_expr_fold(&mut set_op.right);
        }
    }
}

fn select_node_fold(node: &mut SelectNode) {
    if let SelectColumns::Columns(cols) = &mut node.columns {
        for col in cols {
            if let SelectColumn::Expr { expr, .. } = col {
                scalar_expr_fold(expr);
            }
        }
    }
    for source in &mut node.from {
        table_source_fold(source);
    }
    if let Some(w) = &mut node.where_clause {
        where_expr_fold(w);
    }
    if let Some(h) = &mut node.having {
        where_expr_fold(h);
    }
}

fn table_source_fold(source: &mut TableSource) {
    match source {
        TableSource::Join(join) => {
            if let JoinQual::On(cond) = &mut join.qual {
                where_expr_fold(cond);
            }
            table_source_fold(&mut join.left);
            table_source_fold(&mut join.right);
        }
        TableSource::Subquery(sub) => query_expr_fold(&mut sub.query),
        TableSource::CteRef(cte_ref) => query_expr_fold(&mut cte_ref.query),
        TableSource::Table(_) => {}
    }
}

fn where_expr_fold(expr: &mut WhereExpr) {
    match expr {
        WhereExpr::Scalar(scalar) => scalar_expr_fold(scalar),
        WhereExpr::Unary(u) => where_expr_fold(&mut u.expr),
        WhereExpr::Binary(b) => {
            where_expr_fold(&mut b.lexpr);
            where_expr_fold(&mut b.rexpr);
        }
        WhereExpr::Multi(m) => {
            for e in &mut m.exprs {
                where_expr_fold(e);
            }
        }
        WhereExpr::Subquery {
            query, test_expr, ..
        } => {
            query_expr_fold(query);
            if let Some(test) = test_expr {
                scalar_expr_fold(test);
            }
        }
    }
}

fn scalar_expr_fold(expr: &mut ScalarExpr) {
    match expr {
        ScalarExpr::Arithmetic(arith) => {
            scalar_expr_fold(&mut arith.left);
            scalar_expr_fold(&mut arith.right);
            if let (ScalarExpr::Literal(l), ScalarExpr::Literal(r)) = (&*arith.left, &*arith.right)
                && let Some(folded) = literal_arithmetic_fold(l, arith.op, r)
            {
                *expr = ScalarExpr::Literal(folded);
            }
        }
        ScalarExpr::Function(func) => {
            for arg in &mut func.args {
                scalar_expr_fold(arg);
            }
            for clause in &mut func.agg_order {
                scalar_expr_fold(&mut clause.expr);
            }
            if let Some(filter) = &mut func.agg_filter {
                where_expr_fold(filter);
            }
            if let Some(over) = &mut func.over {
                for col in &mut over.partition_by {
                    scalar_expr_fold(col);
                }
                for clause in &mut over.order_by {
                    scalar_expr_fold(&mut clause.expr);
                }
            }
        }
        ScalarExpr::Case(case) => {
            if let Some(arg) = &mut case.arg {
                scalar_expr_fold(arg);
            }
            for when in &mut case.whens {
                where_expr_fold(&mut when.condition);
                scalar_expr_fold(&mut when.result);
            }
            if let Some(default) = &mut case.default {
                scalar_expr_fold(default);
            }
        }
        ScalarExpr::Subquery(query) => query_expr_fold(query),
        ScalarExpr::Array(elems) => {
            for elem in elems {
                scalar_expr_fold(elem);
            }
        }
        ScalarExpr::TypeCast { expr, .. } => scalar_expr_fold(expr),
        ScalarExpr::Column(_) | ScalarExpr::Literal(_) => {}
    }
}

/// Evaluate `left op right` when both sides are foldable numeric literals.
/// Returns `None` for non-numeric operands, overflow, divide/mod by zero,
/// or NaN — leaving the arithmetic node untouched.
fn literal_arithmetic_fold(
    left: &LiteralValue,
    op: ArithmeticOp,
    right: &LiteralValue,
) -> Option<LiteralValue> {
    match (left, right) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => {
            integer_op(*a, op, *b).map(LiteralValue::Integer)
        }
        (LiteralValue::Float(a), LiteralValue::Float(b)) => {
            float_op(a.into_inner(), op, b.into_inner())
        }
        // Mixed integer/float promotes to float, matching postgres's
        // implicit cast. The cast can lose precision for i64 values whose
        // magnitude exceeds 2^53, but that's the same precision the origin
        // would compute with.
        #[allow(clippy::cast_precision_loss)]
        (LiteralValue::Integer(a), LiteralValue::Float(b)) => {
            float_op(*a as f64, op, b.into_inner())
        }
        #[allow(clippy::cast_precision_loss)]
        (LiteralValue::Float(a), LiteralValue::Integer(b)) => {
            float_op(a.into_inner(), op, *b as f64)
        }
        _ => None,
    }
}

fn integer_op(a: i64, op: ArithmeticOp, b: i64) -> Option<i64> {
    match op {
        ArithmeticOp::Add => a.checked_add(b),
        ArithmeticOp::Subtract => a.checked_sub(b),
        ArithmeticOp::Multiply => a.checked_mul(b),
        // checked_div / checked_rem return None for divisor zero and for the
        // i64::MIN / -1 overflow case — both leave the node unfolded so origin
        // raises the canonical error.
        ArithmeticOp::Divide => a.checked_div(b),
        ArithmeticOp::Modulo => a.checked_rem(b),
    }
}

fn float_op(a: f64, op: ArithmeticOp, b: f64) -> Option<LiteralValue> {
    let result = match op {
        ArithmeticOp::Add => a + b,
        ArithmeticOp::Subtract => a - b,
        ArithmeticOp::Multiply => a * b,
        ArithmeticOp::Divide => a / b,
        ArithmeticOp::Modulo => a % b,
    };
    NotNan::new(result).ok().map(LiteralValue::Float)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::messages::QueryParameters;
    use crate::query::ast::{query_expr_convert, query_expr_fingerprint};
    use crate::query::transform::query_expr_parameters_replace;
    use bytes::Bytes;
    use postgres_types::Type as PgType;

    fn parse_and_fold(sql: &str) -> QueryExpr {
        let ast = pg_query::parse(sql).unwrap();
        let mut q = query_expr_convert(&ast).unwrap();
        query_expr_constant_fold(&mut q);
        q
    }

    fn where_rhs_literal(q: &QueryExpr) -> &LiteralValue {
        let select = q.as_select().expect("select");
        let WhereExpr::Binary(binary) = select.where_clause.as_ref().expect("where") else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(ScalarExpr::Literal(lit)) = binary.rexpr.as_ref() else {
            panic!("expected folded literal RHS, got {:?}", binary.rexpr);
        };
        lit
    }

    #[test]
    fn fold_integer_add() {
        let q = parse_and_fold("SELECT id FROM t WHERE x = 1 + 2");
        assert_eq!(*where_rhs_literal(&q), LiteralValue::Integer(3));
    }

    #[test]
    fn fold_integer_modulo_then_add() {
        // The PGC-118 bench query reduces to a single literal.
        let q = parse_and_fold("SELECT id FROM t WHERE user_id = 12345 % 10000 + 1");
        assert_eq!(*where_rhs_literal(&q), LiteralValue::Integer(2346));
    }

    #[test]
    fn fold_distinct_inputs_same_result() {
        // Two different sid values that fold to the same user_id share a fingerprint.
        let q1 = parse_and_fold("SELECT id FROM t WHERE user_id = 12345 % 10000 + 1");
        let q2 = parse_and_fold("SELECT id FROM t WHERE user_id = 22345 % 10000 + 1");
        assert_eq!(query_expr_fingerprint(&q1), query_expr_fingerprint(&q2));
    }

    #[test]
    fn fold_with_column_left_unfolded() {
        // Mixed literal+column cannot fold.
        let q = parse_and_fold("SELECT id FROM t WHERE x = a + 1");
        let select = q.as_select().expect("select");
        let WhereExpr::Binary(binary) = select.where_clause.as_ref().expect("where") else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(ScalarExpr::Arithmetic(_)) = binary.rexpr.as_ref() else {
            panic!("expected arithmetic to stay unfolded with column operand");
        };
    }

    #[test]
    fn fold_divide_by_zero_left_unfolded() {
        let q = parse_and_fold("SELECT id FROM t WHERE x = 10 / 0");
        let select = q.as_select().expect("select");
        let WhereExpr::Binary(binary) = select.where_clause.as_ref().expect("where") else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(ScalarExpr::Arithmetic(_)) = binary.rexpr.as_ref() else {
            panic!("expected divide-by-zero to stay unfolded");
        };
    }

    #[test]
    fn fold_modulo_by_zero_left_unfolded() {
        let q = parse_and_fold("SELECT id FROM t WHERE x = 10 % 0");
        let select = q.as_select().expect("select");
        let WhereExpr::Binary(binary) = select.where_clause.as_ref().expect("where") else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(ScalarExpr::Arithmetic(_)) = binary.rexpr.as_ref() else {
            panic!("expected modulo-by-zero to stay unfolded");
        };
    }

    #[test]
    fn fold_integer_overflow_left_unfolded() {
        // pg_query parses out-of-i32-range literals as Float, so this case
        // can only arise from binary parameter substitution binding a real
        // i64. Test `literal_arithmetic_fold` directly with i64 values.
        let result = literal_arithmetic_fold(
            &LiteralValue::Integer(i64::MAX),
            ArithmeticOp::Add,
            &LiteralValue::Integer(1),
        );
        assert!(result.is_none(), "i64 overflow should leave node unfolded");

        let result = literal_arithmetic_fold(
            &LiteralValue::Integer(i64::MIN),
            ArithmeticOp::Divide,
            &LiteralValue::Integer(-1),
        );
        assert!(
            result.is_none(),
            "i64::MIN / -1 overflow should leave node unfolded"
        );
    }

    #[test]
    fn fold_integer_plus_float_promotes() {
        let q = parse_and_fold("SELECT id FROM t WHERE x = 1 + 2.5");
        let LiteralValue::Float(f) = where_rhs_literal(&q) else {
            panic!("expected float result from int+float");
        };
        assert!((f.into_inner() - 3.5).abs() < 1e-9);
    }

    #[test]
    fn fold_parameter_left_unfolded() {
        // Pre-bind: $1 is a Parameter literal, not numeric — leave unfolded.
        let q = parse_and_fold("SELECT id FROM t WHERE x = $1 % 10");
        let select = q.as_select().expect("select");
        let WhereExpr::Binary(binary) = select.where_clause.as_ref().expect("where") else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(ScalarExpr::Arithmetic(_)) = binary.rexpr.as_ref() else {
            panic!("expected parameter to stay unfolded");
        };
    }

    fn int8_param(value: i64) -> QueryParameters {
        QueryParameters {
            values: vec![Some(Bytes::from(value.to_string().into_bytes()))],
            formats: vec![0],
            oids: vec![PgType::INT8.oid()],
        }
    }

    #[test]
    fn fold_after_parameter_bind() {
        // Post-bind fold: `$1 % 10000 + 1` with $1=12345 folds to 2346.
        let ast = pg_query::parse("SELECT id FROM t WHERE user_id = $1 % 10000 + 1").unwrap();
        let parsed = query_expr_convert(&ast).unwrap();
        let bound = query_expr_parameters_replace(&parsed, &int8_param(12345)).unwrap();

        let select = bound.as_select().expect("select");
        let WhereExpr::Binary(binary) = select.where_clause.as_ref().expect("where") else {
            panic!("expected binary WHERE");
        };
        let WhereExpr::Scalar(ScalarExpr::Literal(LiteralValue::Integer(n))) =
            binary.rexpr.as_ref()
        else {
            panic!("expected folded literal after bind, got {:?}", binary.rexpr);
        };
        assert_eq!(*n, 2346);
    }

    #[test]
    fn fold_after_parameter_bind_collapses_fingerprints() {
        // Different $1 values that fold to the same user_id share a fingerprint
        // after bind-time fold — the writer-bottleneck fix that PGC-118 targets.
        let ast = pg_query::parse("SELECT id FROM t WHERE user_id = $1 % 10000 + 1").unwrap();
        let parsed = query_expr_convert(&ast).unwrap();

        let bound1 = query_expr_parameters_replace(&parsed, &int8_param(12345)).unwrap();
        let bound2 = query_expr_parameters_replace(&parsed, &int8_param(22345)).unwrap();
        assert_eq!(
            query_expr_fingerprint(&bound1),
            query_expr_fingerprint(&bound2),
            "12345 and 22345 both fold to 2346; fingerprints must match",
        );

        // Sanity: distinct fold results stay distinct.
        let bound3 = query_expr_parameters_replace(&parsed, &int8_param(11111)).unwrap();
        assert_ne!(
            query_expr_fingerprint(&bound1),
            query_expr_fingerprint(&bound3),
        );
    }
}
