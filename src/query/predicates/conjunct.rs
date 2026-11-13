//! A predicate split into AND-ed parts, each recognized as a column
//! constraint, a column equality or neither.

use std::cmp::Ordering;

use ecow::EcoString;
use smallvec::{SmallVec, smallvec};

use super::{ColumnConstraint, ConjunctForm};
use crate::catalog::{ColumnMetadata, TextOrder};
use crate::query::ast::rope::{
    Binary, Column, Literal, Multi, NodeRef, Run, Scalar, ScalarKind, Unary, View, Where, WhereKind,
};
use crate::query::ast::{BinaryOp, LiteralValue, MultiOp, NodeId, UnaryOp};
use crate::query::cast::{cast_target_is_coercion_supported, comparison_canonicalize, scalar_leaf};
use crate::query::constraints::{
    ColumnConstraintKind, NullTest, literal_value_canonical_order, literal_value_is_incomparable,
    literal_value_order,
};
use crate::query::resolve::{BoundView, ColumnBinding};
use crate::query::typed_compare::{ColumnCompare, ValueKind};

/// One AND-ed part of a predicate: its form and the operand subtrees it
/// reads. A `BETWEEN` or `NOT IN` is several parts over shared operands.
pub(super) struct ConjunctPart<'t> {
    pub(super) form: ConjunctForm,
    pub(super) node: NodeId,
    pub(super) operands: SmallVec<[NodeRef<'t>; 2]>,
}

impl<'t> ConjunctPart<'t> {
    fn whole(form: ConjunctForm, expr: Where<'t>) -> Self {
        Self {
            form,
            node: expr.id(),
            operands: smallvec![expr.node_ref()],
        }
    }
}

/// The parts of `expr`, left to right.
pub(super) fn conjunct_parts_collect<'t>(
    bound: BoundView<'_>,
    expr: Where<'t>,
    parts: &mut Vec<ConjunctPart<'t>>,
) {
    for conjunct in expr.conjuncts() {
        match conjunct.kind() {
            WhereKind::Binary(binary) if binary.op().is_comparison() => {
                parts.push(ConjunctPart::whole(
                    comparison_form(bound, binary),
                    conjunct,
                ));
            }
            WhereKind::Multi(multi) => multi_parts_collect(bound, conjunct, multi, parts),
            WhereKind::Unary(unary) => {
                parts.push(ConjunctPart::whole(null_test_form(bound, unary), conjunct));
            }
            WhereKind::Scalar(_) | WhereKind::Binary(_) | WhereKind::Subquery(_) => {
                parts.push(ConjunctPart::whole(ConjunctForm::Opaque, conjunct));
            }
        }
    }
}

/// A WHERE operand when it is a bare column or literal.
enum Operand<'t> {
    Column(Column<'t>),
    Literal(Literal<'t>),
    Other,
}

fn operand(expr: Where<'_>) -> Operand<'_> {
    match expr.kind() {
        WhereKind::Scalar(scalar) => match scalar.kind() {
            ScalarKind::Column(column) => Operand::Column(column),
            ScalarKind::Literal(literal) => Operand::Literal(literal),
            ScalarKind::Function(_)
            | ScalarKind::Case(_)
            | ScalarKind::Operator(_)
            | ScalarKind::Subquery(_)
            | ScalarKind::Array(_)
            | ScalarKind::TypeCast(_) => Operand::Other,
        },
        WhereKind::Unary(_)
        | WhereKind::Binary(_)
        | WhereKind::Multi(_)
        | WhereKind::Subquery(_) => Operand::Other,
    }
}

fn column_constraint_form(
    bound: BoundView<'_>,
    column: Column<'_>,
    kind: ColumnConstraintKind,
) -> ConjunctForm {
    match bound.column_binding_at(column.id()) {
        Some(column) => binding_constraint_form(bound, column, kind),
        None => ConjunctForm::Opaque,
    }
}

/// The one place a read's constraint takes its column's type and collation
/// into account, so everything derived from it (ranges, index keys, row
/// matches) agrees on what a literal's bytes decide.
fn binding_constraint_form(
    bound: BoundView<'_>,
    column: ColumnBinding,
    kind: ColumnConstraintKind,
) -> ConjunctForm {
    let typed = match bound.bindings().binding_column(&column) {
        Some(metadata) => constraint_kind_typed(metadata, kind),
        None => Some(kind),
    };
    match typed {
        Some(kind) => ConjunctForm::Constraint(ColumnConstraint { column, kind }),
        None => ConjunctForm::Opaque,
    }
}

/// What a string literal compared to a column becomes in its constraint.
enum StringForm {
    /// Bytes decide equality: linguistically collated text, an enum label.
    Plain,
    /// Bytes decide equality and order: byte-ordered text.
    Ordered,
    /// Two spellings can be one value (`'10.50'` and `'10.5'`, `'a '` and
    /// `'a'` as `bpchar`), so the literal carries its type and compares only
    /// with another of that type, never with a write's bare string.
    Typed(EcoString),
    /// Nondeterministically collated text: no constraint at all.
    Undecidable,
}

fn string_form(column: &ColumnMetadata) -> StringForm {
    let compare = ColumnCompare::of_column(column);
    let cast = match compare.kind() {
        ValueKind::Text => {
            return match column.collation.text_order {
                TextOrder::Bytes => StringForm::Ordered,
                TextOrder::Linguistic => StringForm::Plain,
                TextOrder::Nondeterministic => StringForm::Undecidable,
            };
        }
        ValueKind::Bpchar if column.collation.text_order == TextOrder::Nondeterministic => {
            return StringForm::Undecidable;
        }
        ValueKind::Enum => return StringForm::Plain,
        ValueKind::Bpchar => "bpchar",
        ValueKind::Integer => "int8",
        ValueKind::Decimal => "numeric",
        ValueKind::Bool => "bool",
        ValueKind::Date => "date",
        ValueKind::Timestamp => "timestamp",
        ValueKind::TimestampTz => "timestamptz",
        ValueKind::Uuid => "uuid",
        ValueKind::Unsupported => return StringForm::Typed(column.type_name.clone()),
    };
    StringForm::Typed(cast.into())
}

/// `kind` with its string literals in the form the column gives them. A
/// cast comparison lives in the cast's domain and a null test reads no
/// value, so both stand whatever the column.
fn constraint_kind_typed(
    column: &ColumnMetadata,
    kind: ColumnConstraintKind,
) -> Option<ColumnConstraintKind> {
    let form = string_form(column);
    match kind {
        ColumnConstraintKind::Comparison { op, value } => Some(ColumnConstraintKind::Comparison {
            op,
            value: literal_formed(&form, value)?,
        }),
        ColumnConstraintKind::AnyOf(values) => Some(ColumnConstraintKind::AnyOf(
            values
                .into_iter()
                .map(|value| literal_formed(&form, value))
                .collect::<Option<_>>()?,
        )),
        kind
        @ (ColumnConstraintKind::CastComparison { .. } | ColumnConstraintKind::NullTest(_)) => {
            Some(kind)
        }
    }
}

/// `value` under `form`; `None` when the column decides nothing from it. A
/// string already cast keeps the type it names, except a text cast on
/// byte-ordered text, which is the ordered text itself.
fn literal_formed(form: &StringForm, value: LiteralValue) -> Option<LiteralValue> {
    Some(match (form, value) {
        (StringForm::Undecidable, _) => return None,
        (StringForm::Ordered, LiteralValue::String(text)) => LiteralValue::OrderedText(text),
        (StringForm::Ordered, LiteralValue::StringWithCast(text, cast))
            if ValueKind::of_cast_name(&cast) == ValueKind::Text =>
        {
            LiteralValue::OrderedText(text)
        }
        (StringForm::Typed(cast), LiteralValue::String(text)) => {
            LiteralValue::StringWithCast(text, cast.clone())
        }
        (StringForm::Plain | StringForm::Ordered | StringForm::Typed(_), value) => value,
    })
}

/// `column op value`, `column::cast op value` under a supported coercion,
/// or `column = column`.
fn comparison_form(bound: BoundView<'_>, binary: Binary<'_>) -> ConjunctForm {
    if let Some((column, target, op, value)) = comparison_canonicalize(bound, binary) {
        let Some(base) = bound.column_type_at(column.id()) else {
            return ConjunctForm::Opaque;
        };
        let kind = match target {
            None => ColumnConstraintKind::Comparison {
                op,
                value: value.clone(),
            },
            Some(cast) if cast_target_is_coercion_supported(cast, base) => {
                ColumnConstraintKind::CastComparison {
                    cast: cast.clone(),
                    op,
                    value: value.clone(),
                }
            }
            Some(_) => return ConjunctForm::Opaque,
        };
        return column_constraint_form(bound, column, kind);
    }
    column_pair(bound, binary).map_or(ConjunctForm::Opaque, |(left, right)| {
        ConjunctForm::Equality(left, right)
    })
}

/// `column IS [NOT] NULL`; a null test over anything else is opaque.
fn null_test_form(bound: BoundView<'_>, unary: Unary<'_>) -> ConjunctForm {
    let test = match unary.op() {
        UnaryOp::IsNull => NullTest::IsNull,
        UnaryOp::IsNotNull => NullTest::IsNotNull,
        UnaryOp::Not
        | UnaryOp::IsTrue
        | UnaryOp::IsNotTrue
        | UnaryOp::IsFalse
        | UnaryOp::IsNotFalse => return ConjunctForm::Opaque,
    };
    match operand(unary.expr()) {
        Operand::Column(column) => {
            column_constraint_form(bound, column, ColumnConstraintKind::NullTest(test))
        }
        Operand::Literal(_) | Operand::Other => ConjunctForm::Opaque,
    }
}

fn column_pair(bound: BoundView<'_>, binary: Binary<'_>) -> Option<(ColumnBinding, ColumnBinding)> {
    if binary.op() != BinaryOp::Equal {
        return None;
    }
    let left = scalar_leaf(bound, binary.lexpr())?;
    let right = scalar_leaf(bound, binary.rexpr())?;
    let (ScalarKind::Column(left), ScalarKind::Column(right)) = (left.kind(), right.kind()) else {
        return None;
    };
    Some((
        bound.column_binding_at(left.id())?,
        bound.column_binding_at(right.id())?,
    ))
}

fn multi_parts_collect<'t>(
    bound: BoundView<'_>,
    expr: Where<'t>,
    multi: Multi<'t>,
    parts: &mut Vec<ConjunctPart<'t>>,
) {
    let form = match multi.op() {
        MultiOp::Between | MultiOp::BetweenSymmetric => {
            return between_parts_collect(bound, expr, multi, parts);
        }
        MultiOp::NotIn => return not_in_parts_collect(bound, expr, multi, parts),
        MultiOp::In => column_and_literals(bound, multi.exprs())
            .map_or(ConjunctForm::Opaque, |(column, values)| {
                any_of_form(bound, column, values)
            }),
        // `col = ANY(<array literal>)` is `col IN (<elements>)` for set
        // membership (PGC-106); other comparisons under ANY are not.
        MultiOp::Any {
            comparison: BinaryOp::Equal,
        } => any_eq_array(bound, multi.exprs()).map_or(ConjunctForm::Opaque, |(column, values)| {
            any_of_form(bound, column, values)
        }),
        MultiOp::NotBetween
        | MultiOp::NotBetweenSymmetric
        | MultiOp::Any { .. }
        | MultiOp::All { .. } => ConjunctForm::Opaque,
    };
    parts.push(ConjunctPart::whole(form, expr));
}

fn any_of_form(
    bound: BoundView<'_>,
    column: ColumnBinding,
    mut values: Vec<LiteralValue>,
) -> ConjunctForm {
    values.sort_by(literal_value_canonical_order);
    values.dedup();
    binding_constraint_form(bound, column, ColumnConstraintKind::AnyOf(values))
}

/// `x BETWEEN low AND high` as `x >= low` and `x <= high`. A SYMMETRIC one
/// is only split once its bounds are known to be in order.
fn between_parts_collect<'t>(
    bound: BoundView<'_>,
    expr: Where<'t>,
    multi: Multi<'t>,
    parts: &mut Vec<ConjunctPart<'t>>,
) {
    let mut exprs = multi.exprs();
    let (Some(subject), Some(low), Some(high), None) =
        (exprs.next(), exprs.next(), exprs.next(), exprs.next())
    else {
        parts.push(ConjunctPart::whole(ConjunctForm::Opaque, expr));
        return;
    };
    let (low, high) = if multi.op() == MultiOp::BetweenSymmetric {
        let order = match (operand(low), operand(high)) {
            (Operand::Literal(first), Operand::Literal(second)) => {
                literal_value_order(first.value(), second.value())
            }
            _ => None,
        };
        match order {
            Some(Ordering::Greater) => (high, low),
            Some(_) => (low, high),
            None => {
                parts.push(ConjunctPart::whole(ConjunctForm::Opaque, expr));
                return;
            }
        }
    } else {
        (low, high)
    };
    for (limit, op) in [
        (low, BinaryOp::GreaterThanOrEqual),
        (high, BinaryOp::LessThanOrEqual),
    ] {
        let form = match (operand(subject), operand(limit)) {
            (Operand::Column(column), Operand::Literal(literal)) => column_constraint_form(
                bound,
                column,
                ColumnConstraintKind::Comparison {
                    op,
                    value: literal.value().clone(),
                },
            ),
            _ => ConjunctForm::Opaque,
        };
        parts.push(ConjunctPart {
            form,
            node: expr.id(),
            operands: smallvec![subject.node_ref(), limit.node_ref()],
        });
    }
}

/// `x NOT IN (v1, v2, ...)` as `x <> v1`, `x <> v2`, ...
fn not_in_parts_collect<'t>(
    bound: BoundView<'_>,
    expr: Where<'t>,
    multi: Multi<'t>,
    parts: &mut Vec<ConjunctPart<'t>>,
) {
    let mut exprs = multi.exprs();
    let Some(subject) = exprs.next() else {
        parts.push(ConjunctPart::whole(ConjunctForm::Opaque, expr));
        return;
    };
    for element in exprs {
        let form = match (operand(subject), operand(element)) {
            (Operand::Column(column), Operand::Literal(literal))
                if !literal_value_is_incomparable(literal.value()) =>
            {
                column_constraint_form(
                    bound,
                    column,
                    ColumnConstraintKind::Comparison {
                        op: BinaryOp::NotEqual,
                        value: literal.value().clone(),
                    },
                )
            }
            _ => ConjunctForm::Opaque,
        };
        parts.push(ConjunctPart {
            form,
            node: expr.id(),
            operands: smallvec![subject.node_ref(), element.node_ref()],
        });
    }
}

/// The subject column and the literal values after it, when the operands
/// are exactly that.
fn column_and_literals<'t>(
    bound: BoundView<'_>,
    mut exprs: Run<'t, Where<'t>>,
) -> Option<(ColumnBinding, Vec<LiteralValue>)> {
    let Some(Operand::Column(column)) = exprs.next().map(operand) else {
        return None;
    };
    let column = bound.column_binding_at(column.id())?;
    let mut values = Vec::new();
    for expr in exprs {
        let Operand::Literal(literal) = operand(expr) else {
            return None;
        };
        if literal_value_is_incomparable(literal.value()) {
            return None;
        }
        values.push(literal.value().clone());
    }
    Some((column, values))
}

fn any_eq_array<'t>(
    bound: BoundView<'_>,
    mut exprs: Run<'t, Where<'t>>,
) -> Option<(ColumnBinding, Vec<LiteralValue>)> {
    let (Some(first), Some(second), None) = (exprs.next(), exprs.next(), exprs.next()) else {
        return None;
    };
    let (Operand::Column(column), WhereKind::Scalar(rhs)) = (operand(first), second.kind()) else {
        return None;
    };
    let values = array_literal_values(rhs)?;
    if values.iter().any(literal_value_is_incomparable) {
        return None;
    }
    Some((bound.column_binding_at(column.id())?, values))
}

/// The elements of an array literal or an `ARRAY[...]` of literals.
fn array_literal_values(rhs: Scalar<'_>) -> Option<Vec<LiteralValue>> {
    match rhs.kind() {
        ScalarKind::Literal(literal) => match literal.value() {
            LiteralValue::Array(values, _) => Some(values.clone()),
            LiteralValue::String(_)
            | LiteralValue::StringWithCast(..)
            | LiteralValue::OrderedText(_)
            | LiteralValue::Integer(_)
            | LiteralValue::Float(_)
            | LiteralValue::Boolean(_)
            | LiteralValue::Null
            | LiteralValue::NullWithCast(_)
            | LiteralValue::Parameter(_)
            | LiteralValue::SessionTerm(_) => None,
        },
        ScalarKind::Array(array) => {
            let mut out = Vec::with_capacity(array.elements().len());
            for element in array.elements() {
                let ScalarKind::Literal(literal) = element.kind() else {
                    return None;
                };
                out.push(literal.value().clone());
            }
            Some(out)
        }
        ScalarKind::Column(_)
        | ScalarKind::Function(_)
        | ScalarKind::Case(_)
        | ScalarKind::Operator(_)
        | ScalarKind::Subquery(_)
        | ScalarKind::TypeCast(_) => None,
    }
}
