//! `USING` / `NATURAL` join resolution.
//!
//! Postgres exposes a `USING`/`NATURAL` join's common columns *once* rather
//! than per side, so binding them means attributing each name to the input
//! that exposes it, recording the merged column, and threading it back into
//! the scope — where `*` expansion and unqualified references then see a
//! single column (`COALESCE(left, right)` under an outer join).

use ecow::EcoString;
use rootcause::Report;

use super::bindings::{ColumnBinding, MergedColumnBinding, MergedId};
use super::column::{ColumnLookup, column_lookup};
use super::scope::ResolutionScope;
use crate::query::ast::{ColumnNode, JoinType, NodeId};
use crate::query::resolve::{ResolveError, ResolveResult};

/// A `USING`/`NATURAL` join's merged output column. Postgres exposes the
/// join column(s) once (not per side); for an outer join its value is
/// `COALESCE(left, right)`. Tracked in scope so `*` expands to the merged
/// set and an unqualified reference binds to one column.
#[derive(Debug, Clone)]
pub(super) struct MergedJoinColumn {
    /// Join/output column name (e.g. `i` for `USING (i)`).
    pub(super) name: EcoString,
    /// Outer join: merged value is `COALESCE`, not a plain column — so an
    /// unqualified GROUP BY on it is unsupported (forwarded).
    pub(super) outer: bool,
    /// The same column in the bindings.
    pub(super) merged: MergedId,
    /// The left side's column, which an inner join's merged value is.
    pub(super) left: ColumnBinding,
}

/// Scope index ranges delimiting each side of a join, into
/// `scope.entries`, used to attribute a `USING`/`NATURAL` column to the
/// input that exposes it.
#[derive(Clone, Copy)]
pub(super) struct JoinScopeRanges {
    pub(super) left_lo: usize,
    pub(super) mid: usize,
    pub(super) hi: usize,
}

impl JoinScopeRanges {
    /// `scope.entries` index range for the left input.
    pub(super) fn left(self) -> std::ops::Range<usize> {
        self.left_lo..self.mid
    }
    /// `scope.entries` index range for the right input.
    pub(super) fn right(self) -> std::ops::Range<usize> {
        self.mid..self.hi
    }
}

/// The qualifier (alias, else table name) of the single scope entry in
/// the given `scope.entries` range exposing `column`. `None` if zero or
/// more than one expose it — an ambiguous or absent `USING`/`NATURAL`
/// column, which the caller turns into a resolve error so the query is
/// forwarded rather than mis-cached.
fn join_side_qualifier(
    scope: &ResolutionScope<'_>,
    range: std::ops::Range<usize>,
    column: &str,
) -> Option<EcoString> {
    let mut found: Option<EcoString> = None;
    for entry in scope.entries.get(range).unwrap_or(&[]) {
        if entry.metadata().columns.get(column).is_some() {
            if found.is_some() {
                return None;
            }
            found = Some(EcoString::from(entry.qualifier_key()));
        }
    }
    found
}

/// Column names exposed by both the left and right inputs, in left-side
/// order — the `NATURAL JOIN` columns.
pub(super) fn join_natural_common_columns(
    scope: &ResolutionScope<'_>,
    ranges: JoinScopeRanges,
) -> Vec<EcoString> {
    let side_names = |range: std::ops::Range<usize>| {
        let mut names: Vec<&str> = Vec::new();
        for entry in scope.entries.get(range).unwrap_or(&[]) {
            names.extend(entry.metadata().columns.iter().map(|c| c.name.as_str()));
        }
        names
    };
    let right = side_names(ranges.right());
    let mut out: Vec<EcoString> = Vec::new();
    for name in side_names(ranges.left()) {
        if right.contains(&name) && !out.iter().any(|o| o == name) {
            out.push(EcoString::from(name));
        }
    }
    out
}

/// Bind `qual.column` against the join scope. The reference is synthesized,
/// not a node of the tree, so it is looked up without binding.
fn join_side_column_resolve(
    scope: &mut ResolutionScope<'_>,
    qual: &EcoString,
    column: &EcoString,
) -> ResolveResult<ColumnBinding> {
    let reference = ColumnNode {
        id: NodeId::PENDING,
        table: Some(qual.clone()),
        column: column.clone(),
    };
    match column_lookup(&reference, scope)? {
        ColumnLookup::Column { binding, .. } => Ok(binding),
        // Qualified lookups never reach a merged column; forward if one does.
        ColumnLookup::Merged { .. } => Err(Report::from(ResolveError::UnsupportedJoinQualifier)),
    }
}

/// One `USING`/`NATURAL` column bound on both join sides: the merged
/// column, and the per-side `(qualifier, column)` pairs it consumes from
/// `*` expansion.
fn using_column_resolve(
    scope: &mut ResolutionScope<'_>,
    ranges: JoinScopeRanges,
    c: &EcoString,
    outer: bool,
    join: NodeId,
) -> ResolveResult<(MergedJoinColumn, [(EcoString, EcoString); 2])> {
    let unsupported = || Report::from(ResolveError::UnsupportedJoinQualifier);
    let lq = join_side_qualifier(scope, ranges.left(), c).ok_or_else(unsupported)?;
    let rq = join_side_qualifier(scope, ranges.right(), c).ok_or_else(unsupported)?;
    let left = join_side_column_resolve(scope, &lq, c)?;
    let right = join_side_column_resolve(scope, &rq, c)?;
    let merged = scope.bindings.merged_push(MergedColumnBinding {
        join,
        name: c.clone(),
        left,
        right,
        outer,
    });
    Ok((
        MergedJoinColumn {
            name: c.clone(),
            outer,
            merged,
            left,
        },
        [(lq, c.clone()), (rq, c.clone())],
    ))
}

/// Bind a `USING`/`NATURAL` join from its computed `cols`: an empty set is
/// an inner cartesian product — or, for an outer join, an unsupported
/// condition-less join (forwarded); otherwise the merged columns are
/// registered in scope (merged value = the left column for an inner join,
/// `COALESCE(left, right)` for an outer one) with the per-side
/// `(qualifier, column)` pairs they consume from `*` expansion.
pub(super) fn join_using_or_cross(
    scope: &mut ResolutionScope<'_>,
    ranges: JoinScopeRanges,
    cols: Vec<EcoString>,
    join_type: JoinType,
    join: NodeId,
) -> ResolveResult<()> {
    if cols.is_empty() {
        return if join_type == JoinType::Inner {
            Ok(())
        } else {
            Err(Report::from(ResolveError::UnsupportedJoinQualifier))
        };
    }
    let outer = join_type != JoinType::Inner;
    for c in &cols {
        let (merged, consumed) = using_column_resolve(scope, ranges, c, outer, join)?;
        scope.merged_columns.push(merged);
        scope.merged_consumed.extend(consumed);
    }
    Ok(())
}
