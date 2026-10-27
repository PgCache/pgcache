//! Index-key forms of a table's constraints (PGC-478): every literal is
//! rewritten to the form a row's wire text keys to for the column's type, so
//! the eval index's point probe and the subsumption index's region probe
//! match on value rather than spelling. A literal the comparator cannot parse
//! for a typed column becomes `Null`, which the index routes to the opaque
//! (wildcard) class: the probe over-returns and the precise check decides.

use crate::catalog::TableMetadata;
use crate::query::ast::LiteralValue;
use crate::query::constraints::{ColumnConstraintKind, TableConstraint};
use crate::query::typed_compare::{ValueKind, literal_index_key};

/// Rewrite `constraints` on `table`'s columns into their index-key forms.
pub(crate) fn table_constraints_keyed(
    constraints: &[TableConstraint],
    table: &TableMetadata,
) -> Vec<TableConstraint> {
    constraints
        .iter()
        .map(|constraint| constraint_keyed(constraint, table))
        .collect()
}

fn constraint_keyed(constraint: &TableConstraint, table: &TableMetadata) -> TableConstraint {
    let kind = table
        .columns
        .get(constraint.column.as_str())
        .map_or(ValueKind::Unsupported, |meta| {
            ValueKind::of_type(&meta.data_type)
        });
    let keyed = match &constraint.kind {
        ColumnConstraintKind::Comparison { op, value } => ColumnConstraintKind::Comparison {
            op: *op,
            value: literal_keyed(kind, value),
        },
        ColumnConstraintKind::AnyOf(values) => ColumnConstraintKind::AnyOf(
            values
                .iter()
                .map(|value| literal_keyed(kind, value))
                .collect(),
        ),
        // A cast comparison lives in the cast's domain, which the index
        // already treats as unkeyable; a null test has no literal.
        ColumnConstraintKind::CastComparison { .. } | ColumnConstraintKind::NullTest(_) => {
            constraint.kind.clone()
        }
    };
    TableConstraint {
        column: constraint.column.clone(),
        kind: keyed,
    }
}

/// The key form of one literal, or `Null` (unkeyable) when the literal does
/// not parse as the column's kind. An untyped column keeps its literal as
/// written, as before.
fn literal_keyed(kind: ValueKind, value: &LiteralValue) -> LiteralValue {
    match value {
        LiteralValue::Null
        | LiteralValue::NullWithCast(_)
        | LiteralValue::Parameter(_)
        | LiteralValue::SessionTerm(_)
        | LiteralValue::Array(..) => value.clone(),
        LiteralValue::String(_)
        | LiteralValue::StringWithCast(..)
        | LiteralValue::Integer(_)
        | LiteralValue::Float(_)
        | LiteralValue::Boolean(_) => {
            if kind == ValueKind::Unsupported {
                return value.clone();
            }
            literal_index_key(kind, value).unwrap_or(LiteralValue::Null)
        }
    }
}
