//! CDC point probe: which indexed entries could a concrete row satisfy. The
//! row is treated as an `Equal`-on-every-column query, with each column value
//! coerced into its keyable forms. Proxy-only — the analysis build never
//! probes by row.

use ordered_float::NotNan;
use smallvec::SmallVec;

use super::value_key::ValueKey;
use super::{ConstraintIndex, IdSet};
use crate::catalog::TableMetadata;
use crate::id_hash::IdHashable;
use crate::pg::protocol::ByteString;
use crate::query::ast::LiteralValue;
use crate::query::constraints::ColumnRange;
use crate::query::evaluate::bool_wire_text_parse;

/// Per-column candidate forms a CDC row value can take: the literal string plus
/// optional float/bool reinterpretations. A fixed array (one slot per form) so
/// the per-row point probe never heap-allocates on this axis (PGC-341), and so
/// adding a fourth reinterpretation is a compile error (`[_; 3]` can't hold it)
/// — forcing a deliberate decision about the capacity rather than a silent
/// heap spill. Empty slots are `None`; iterate with `.iter().flatten()`.
pub(super) type ColumnForms = [Option<ColumnRange>; 3];

/// Per-column `ValueKey`s extracted from a column's `Equal` forms — one slot per
/// `ColumnForms` slot (a form is keyable or it isn't), same fixed-array rules.
type ColumnKeys = [Option<ValueKey>; 3];

/// Per-class collection: one `ColumnForms` per column in the class. Unlike the
/// per-column forms (always ≤3), the column count is data-dependent and
/// unbounded (wide composite predicates), so this is a `SmallVec` — inline for
/// the common 1–2 column case, with a correct heap spill for wider classes,
/// rather than a fixed array (PGC-341).
type ClassForms = SmallVec<[ColumnForms; 2]>;

/// Per-class collection of `ColumnKeys`, same shape and rationale as `ClassForms`.
type ClassKeys = SmallVec<[ColumnKeys; 2]>;

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

/// Cartesian product of per-column key sets, for the point-probe equality
/// lookup. Empty input → one empty tuple (the unconstrained class). Each
/// column carries ≤3 forms and classes have few columns, so the product stays
/// tiny.
fn value_key_product(key_sets: &[ColumnKeys]) -> Vec<Vec<ValueKey>> {
    let mut result: Vec<Vec<ValueKey>> = vec![Vec::new()];
    for ks in key_sets {
        let present = ks.iter().flatten().count();
        let mut next = Vec::with_capacity(result.len() * present);
        for prefix in &result {
            for k in ks.iter().flatten() {
                let mut tuple = prefix.clone();
                tuple.push(k.clone());
                next.push(tuple);
            }
        }
        result = next;
    }
    result
}

impl<K: IdHashable + Copy> ConstraintIndex<K> {
    /// Returning convenience wrapper over [`candidates_point_into`] — production
    /// CDC paths use the `_into` form to reuse a scratch set (PGC-341/344).
    #[cfg(test)]
    pub(crate) fn candidates_point<F>(&self, col_forms_fn: F) -> IdSet<K>
    where
        F: Fn(&str) -> ColumnForms,
    {
        let mut candidates = IdSet::default();
        self.candidates_point_into(col_forms_fn, &mut candidates);
        candidates
    }

    /// Like [`candidates_point`], but fills a caller-provided set (cleared first)
    /// instead of allocating a fresh one — lets the CDC hot path reuse a scratch
    /// set, retaining its (possibly large) capacity across probes (PGC-341/344).
    pub(crate) fn candidates_point_into<F>(&self, col_forms_fn: F, candidates: &mut IdSet<K>)
    where
        F: Fn(&str) -> ColumnForms,
    {
        candidates.clear();
        for (column_set, class) in &self.classes {
            let col_forms: ClassForms = column_set
                .columns()
                .iter()
                .map(|c| col_forms_fn(c.as_str()))
                .collect();
            // Equality-pure entries (in `class.equality`) are reachable only
            // through this bucket. Per column, collect the `ValueKey`s of its
            // `Equal` forms; an empty set (Unknown / non-keyable) is a wildcard
            // for that position. All columns keyed → probe the small cartesian
            // product of joint tuples; any wildcard → scan the bucket, matching
            // non-wildcard positions against their key sets.
            let key_sets: ClassKeys = col_forms
                .iter()
                .map(|forms| {
                    forms.each_ref().map(|slot| {
                        slot.as_ref().and_then(|r| match r {
                            ColumnRange::Equal(v) => ValueKey::try_new(v),
                            ColumnRange::Unknown
                            | ColumnRange::Unconstrained
                            | ColumnRange::Empty
                            | ColumnRange::InSet(_)
                            | ColumnRange::Range { .. } => None,
                        })
                    })
                })
                .collect();
            if key_sets.iter().all(|ks| ks.iter().any(Option::is_some)) {
                for tuple in value_key_product(&key_sets) {
                    if let Some(fps) = class.equality.get(&tuple) {
                        candidates.extend(fps);
                    }
                }
            } else {
                for (tuple, fps) in &class.equality {
                    let matches = key_sets.iter().zip(tuple).all(|(ks, t)| {
                        // No keyable form for this column → wildcard; else the
                        // tuple value must match one of the column's keys.
                        ks.iter().all(Option::is_none) || ks.iter().flatten().any(|k| k == t)
                    });
                    if matches {
                        candidates.extend(fps);
                    }
                }
            }
            candidates.extend(class.complex.candidates_point(&col_forms));
        }
    }
}
