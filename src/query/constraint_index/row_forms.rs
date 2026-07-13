//! Coerce a CDC row value into the point-probe's candidate forms.

use ordered_float::NotNan;

use crate::catalog::TableMetadata;
use crate::pg::protocol::ByteString;
use crate::query::ast::LiteralValue;
use crate::query::constraints::ColumnRange;
use crate::query::evaluate::bool_wire_text_parse;

use super::ColumnForms;

/// Coerce a CDC row's value for `column` into the point-probe forms: every
/// keyable interpretation of the wire text, as `Equal` ranges. A present
/// value always yields its lexical `String` form, plus a `Float` form when it
/// parses numerically and a `Boolean` form when it is `t`/`f`. Probing all
/// forms (unioned) is what keeps the probe correct regardless of how the
/// matching entry's literal was typed — a numeric column can hold a
/// `String`-keyed entry via an identity `::text` cast (`val::text = '42'`
/// strips to `Comparison(val, Eq, String("42"))`), and a `String` row form
/// finds it while the `Float` form finds the ordinary `val = 42` entry.
///
/// An absent column, SQL NULL, or unchanged-TOAST yields `[Unknown]` — a
/// wildcard that matches every entry constraining the column (conservative,
/// never under-returns). The forms mirror `where_value_compare_string`'s row
/// interpretation, so the precise check downstream agrees.
pub(crate) fn row_value_forms(
    table_metadata: &TableMetadata,
    row_data: &[Option<ByteString>],
    column: &str,
) -> ColumnForms {
    let Some(meta) = table_metadata.columns.get(column) else {
        return [Some(ColumnRange::Unknown), None, None];
    };
    let Some(Some(bytes)) = row_data.get(meta.index()) else {
        return [Some(ColumnRange::Unknown), None, None];
    };
    let text = bytes.as_str();
    // One slot per reinterpretation. A fourth would not fit `[_; 3]` — a
    // deliberate compile-time gate on growing the inline capacity.
    let float = text
        .parse::<f64>()
        .ok()
        .and_then(|x| NotNan::new(x).ok())
        .map(|n| ColumnRange::Equal(LiteralValue::Float(n)));
    let boolean = bool_wire_text_parse(text).map(|b| ColumnRange::Equal(LiteralValue::Boolean(b)));
    [
        Some(ColumnRange::Equal(LiteralValue::String(text.into()))),
        float,
        boolean,
    ]
}
