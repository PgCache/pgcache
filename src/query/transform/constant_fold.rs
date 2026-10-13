//! Constant folding for arithmetic on literal operands: the fold a
//! conversion and a bind-time substitution apply to an arithmetic node whose
//! operands are both literals.
//!
//! Without it, the cache treats e.g. `WHERE user_id = 12345 % 10000 + 1`
//! and `WHERE user_id = 67890 % 10000 + 1` as distinct queries (different
//! trees embedding the literal arithmetic), even though both reduce to
//! `WHERE user_id = N + 1`.
//!
//! Fold rules:
//! - `Integer op Integer`: checked arithmetic; overflow → leave unfolded.
//! - `Float op Float` / mixed `Integer`+`Float`: promote to f64; `NaN` → leave
//!   unfolded.
//! - Division or modulo by zero: leave unfolded (origin raises the error).
//! - Any non-numeric operand (string, bool, null, parameter, array): not
//!   folded.

use ordered_float::NotNan;

use crate::query::ast::{ArithmeticOp, LiteralValue};

/// Evaluate `left op right` when both sides are foldable numeric literals.
/// Returns `None` for non-numeric operands, overflow, divide/mod by zero,
/// or NaN — leaving the arithmetic node untouched.
pub(crate) fn literal_arithmetic_fold(
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
    #![allow(clippy::wildcard_enum_match_arm)]

    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::*;
    use crate::cache::QueryParameters;
    use crate::oid::TypeOid;
    use crate::query::ast::query_tree_parse;
    use crate::query::ast::rope::{Body, QueryTree, ScalarKind, WhereKind};
    use crate::query::transform::query_tree_parameters_replace;

    /// Conversion folds literal arithmetic as it finishes.
    fn parse_and_fold(sql: &str) -> QueryTree {
        query_tree_parse(sql).expect("parse")
    }

    /// The right operand of a `WHERE col = <rhs>` comparison.
    fn where_rhs(tree: &QueryTree) -> crate::query::ast::rope::Scalar<'_> {
        let Body::Select(select) = tree.root().body() else {
            panic!("expected a SELECT");
        };
        let WhereKind::Binary(binary) = select.where_clause().expect("where").kind() else {
            panic!("expected binary WHERE");
        };
        match binary.rexpr().kind() {
            WhereKind::Scalar(scalar) => scalar,
            _ => panic!("expected a scalar right operand"),
        }
    }

    fn where_rhs_literal(tree: &QueryTree) -> &LiteralValue {
        match where_rhs(tree).kind() {
            ScalarKind::Literal(literal) => literal.value(),
            _ => panic!("expected a folded literal right operand"),
        }
    }

    fn where_rhs_is_arithmetic(tree: &QueryTree) -> bool {
        matches!(where_rhs(tree).kind(), ScalarKind::Arithmetic(_))
    }

    #[test]
    fn test_fold_integer_add() {
        let q = parse_and_fold("SELECT id FROM t WHERE x = 1 + 2");
        assert_eq!(*where_rhs_literal(&q), LiteralValue::Integer(3));
    }

    #[test]
    fn test_fold_integer_modulo_then_add() {
        // The PGC-118 bench query reduces to a single literal.
        let q = parse_and_fold("SELECT id FROM t WHERE user_id = 12345 % 10000 + 1");
        assert_eq!(*where_rhs_literal(&q), LiteralValue::Integer(2346));
    }

    #[test]
    fn test_fold_distinct_inputs_same_result() {
        // Two different sid values that fold to the same user_id give one tree.
        let q1 = parse_and_fold("SELECT id FROM t WHERE user_id = 12345 % 10000 + 1");
        let q2 = parse_and_fold("SELECT id FROM t WHERE user_id = 22345 % 10000 + 1");
        assert_eq!(q1, q2);
    }

    #[test]
    fn test_fold_left_unfolded_cases() {
        for (label, sql) in [
            // Mixed literal+column cannot fold.
            ("column operand", "SELECT id FROM t WHERE x = a + 1"),
            ("divide by zero", "SELECT id FROM t WHERE x = 10 / 0"),
            ("modulo by zero", "SELECT id FROM t WHERE x = 10 % 0"),
            // Pre-bind: $1 is a Parameter literal, not numeric.
            ("parameter", "SELECT id FROM t WHERE x = $1 % 10"),
        ] {
            assert!(
                where_rhs_is_arithmetic(&parse_and_fold(sql)),
                "{label}: {sql}"
            );
        }
    }

    #[test]
    fn test_fold_integer_overflow_left_unfolded() {
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
    fn test_fold_integer_plus_float_promotes() {
        let q = parse_and_fold("SELECT id FROM t WHERE x = 1 + 2.5");
        let LiteralValue::Float(f) = where_rhs_literal(&q) else {
            panic!("expected float result from int+float");
        };
        assert!((f.into_inner() - 3.5).abs() < 1e-9);
    }

    fn int8_param(value: i64) -> QueryParameters {
        QueryParameters {
            values: vec![Some(Bytes::from(value.to_string().into_bytes()))],
            formats: vec![0],
            oids: vec![TypeOid::from_type(&PgType::INT8)],
        }
    }

    #[test]
    fn test_fold_after_parameter_bind() {
        // Post-bind fold: `$1 % 10000 + 1` with $1=12345 folds to 2346.
        let parsed = parse_and_fold("SELECT id FROM t WHERE user_id = $1 % 10000 + 1");
        let bound = query_tree_parameters_replace(&parsed, &int8_param(12345)).expect("bind");
        assert_eq!(*where_rhs_literal(&bound), LiteralValue::Integer(2346));
    }

    #[test]
    fn test_fold_after_parameter_bind_collapses_trees() {
        // Different $1 values that fold to the same user_id give one tree
        // after the bind-time fold: the writer-bottleneck fix PGC-118 targets.
        let parsed = parse_and_fold("SELECT id FROM t WHERE user_id = $1 % 10000 + 1");
        let bound1 = query_tree_parameters_replace(&parsed, &int8_param(12345)).expect("bind");
        let bound2 = query_tree_parameters_replace(&parsed, &int8_param(22345)).expect("bind");
        assert_eq!(bound1, bound2, "12345 and 22345 both fold to 2346");
        // Sanity: distinct fold results stay distinct.
        let bound3 = query_tree_parameters_replace(&parsed, &int8_param(11111)).expect("bind");
        assert_ne!(bound1, bound3);
    }
}
