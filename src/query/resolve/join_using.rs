//! `USING` / `NATURAL` join resolution.
//!
//! Postgres exposes a `USING`/`NATURAL` join's common columns *once* rather
//! than per side, so resolving them means attributing each name to the input
//! that exposes it, synthesizing the equi-join predicate, and threading the
//! merged columns back into the scope — where `*` expansion and unqualified
//! references then see a single column (`COALESCE(left, right)` under an outer
//! join).

use ecow::EcoString;
use rootcause::Report;

use crate::query::ast::{BinaryOp, ColumnNode, JoinType};
use crate::query::resolved::{
    ResolveError, ResolveResult, ResolvedBinaryExpr, ResolvedColumnNode, ResolvedFunctionCall,
    ResolvedJoinQual, ResolvedScalarExpr, ResolvedWhereExpr,
};
use crate::query::transform::where_expr_conjuncts_join;

use super::column::column_resolve;
use super::scope::ResolutionScope;

/// A `USING`/`NATURAL` join's merged output column. Postgres exposes
/// the join column(s) once (not per side); for an outer join its value
/// is `COALESCE(left, right)`. Tracked in scope so `*` expands to the
/// merged set and an unqualified reference resolves to one column.
#[derive(Debug, Clone)]
pub(super) struct MergedJoinColumn {
    /// Join/output column name (e.g. `i` for `USING (i)`).
    pub(super) name: EcoString,
    /// Resolved value. Invariant: `outer == false` ⟹ this is exactly
    /// `Column(<left column>)`; `outer == true` ⟹ `COALESCE(l, r)`.
    pub(super) expr: ResolvedScalarExpr,
    /// Outer join: merged value is `COALESCE`, not a plain column — so
    /// an unqualified GROUP BY on it is unsupported (forwarded).
    pub(super) outer: bool,
}

/// Scope index ranges delimiting each side of a join, into
/// `scope.tables` / `scope.derived_tables` (`d*`), used to attribute a
/// `USING`/`NATURAL` column to the input that exposes it.
#[derive(Clone, Copy)]
pub(super) struct JoinScopeRanges {
    pub(super) left_lo: usize,
    pub(super) mid: usize,
    pub(super) hi: usize,
    pub(super) dleft_lo: usize,
    pub(super) dmid: usize,
    pub(super) dhi: usize,
}

impl JoinScopeRanges {
    /// `(tables, derived_tables)` index ranges for the left input.
    pub(super) fn left(self) -> (std::ops::Range<usize>, std::ops::Range<usize>) {
        (self.left_lo..self.mid, self.dleft_lo..self.dmid)
    }
    /// `(tables, derived_tables)` index ranges for the right input.
    pub(super) fn right(self) -> (std::ops::Range<usize>, std::ops::Range<usize>) {
        (self.mid..self.hi, self.dmid..self.dhi)
    }
}

/// `join_using_resolve` result: the resolved qualifier, the merged
/// scope columns, and the `(qualifier, column)` pairs they consume.
pub(super) type JoinUsingResolved = (
    ResolvedJoinQual,
    Vec<MergedJoinColumn>,
    Vec<(EcoString, EcoString)>,
);

/// The qualifier (alias, else table name) of the single scope table in
/// the given `tables`/`derived_tables` ranges exposing `column`. `None`
/// if zero or more than one expose it — an ambiguous or absent
/// `USING`/`NATURAL` column, which the caller turns into a resolve
/// error so the query is forwarded rather than mis-cached.
pub(super) fn join_side_qualifier(
    scope: &ResolutionScope<'_>,
    t_range: std::ops::Range<usize>,
    d_range: std::ops::Range<usize>,
    column: &str,
) -> Option<EcoString> {
    let mut found: Option<EcoString> = None;
    for (meta, alias) in scope.tables.get(t_range).unwrap_or(&[]) {
        if meta.columns.get(column).is_some() {
            if found.is_some() {
                return None;
            }
            found = Some(EcoString::from(alias.unwrap_or(meta.name.as_str())));
        }
    }
    for (meta, alias) in scope.derived_tables.get(d_range).unwrap_or(&[]) {
        if meta.columns.get(column).is_some() {
            if found.is_some() {
                return None;
            }
            found = Some(EcoString::from(alias.as_str()));
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
    let side_names = |t: std::ops::Range<usize>, d: std::ops::Range<usize>| {
        let mut names: Vec<&str> = Vec::new();
        for (meta, _) in scope.tables.get(t).unwrap_or(&[]) {
            names.extend(meta.columns.iter().map(|c| c.name.as_str()));
        }
        for (meta, _) in scope.derived_tables.get(d).unwrap_or(&[]) {
            names.extend(meta.columns.iter().map(|c| c.name.as_str()));
        }
        names
    };
    let (lt, ld) = ranges.left();
    let (rt, rd) = ranges.right();
    let right = side_names(rt, rd);
    let mut out: Vec<EcoString> = Vec::new();
    for name in side_names(lt, ld) {
        if right.contains(&name) && !out.iter().any(|o| o == name) {
            out.push(EcoString::from(name));
        }
    }
    out
}

/// Resolve `qual.column = c` against the join scope.
pub(super) fn join_side_column_resolve(
    scope: &mut ResolutionScope<'_>,
    qual: &EcoString,
    column: &EcoString,
) -> ResolveResult<ResolvedColumnNode> {
    column_resolve(
        &ColumnNode {
            table: Some(qual.clone()),
            column: column.clone(),
        },
        scope,
    )
}

/// Resolve a `USING`/`NATURAL` join over `cols`: the verbatim
/// qualifier (deparsed so Postgres merges the columns) plus its
/// equivalent equi-`predicate` for analysis, and the merged-column
/// scope entries (merged value = the left column for an inner join,
/// `COALESCE(left, right)` for an outer one) with the per-side
/// `(qualifier, column)` pairs they consume from `*` expansion.
/// `cols` is non-empty (the caller handles the no-common-column case).
pub(super) fn join_using_resolve(
    scope: &mut ResolutionScope<'_>,
    ranges: JoinScopeRanges,
    cols: &[EcoString],
    join_type: JoinType,
) -> ResolveResult<JoinUsingResolved> {
    let outer = join_type != JoinType::Inner;
    let (lt, ld) = ranges.left();
    let (rt, rd) = ranges.right();
    let mut conjuncts: Vec<ResolvedWhereExpr> = Vec::with_capacity(cols.len());
    let mut merged: Vec<MergedJoinColumn> = Vec::with_capacity(cols.len());
    let mut consumed: Vec<(EcoString, EcoString)> = Vec::with_capacity(cols.len() * 2);

    for c in cols {
        let unsupported = || Report::from(ResolveError::UnsupportedJoinQualifier);
        let lq = join_side_qualifier(scope, lt.clone(), ld.clone(), c).ok_or_else(unsupported)?;
        let rq = join_side_qualifier(scope, rt.clone(), rd.clone(), c).ok_or_else(unsupported)?;
        let left_col = join_side_column_resolve(scope, &lq, c)?;
        let right_col = join_side_column_resolve(scope, &rq, c)?;

        conjuncts.push(ResolvedWhereExpr::Binary(ResolvedBinaryExpr {
            op: BinaryOp::Equal,
            lexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(
                left_col.clone(),
            ))),
            rexpr: Box::new(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(
                right_col.clone(),
            ))),
        }));

        let expr = if outer {
            ResolvedScalarExpr::Function(ResolvedFunctionCall {
                name: EcoString::from("coalesce"),
                args: vec![
                    ResolvedScalarExpr::Column(left_col),
                    ResolvedScalarExpr::Column(right_col),
                ],
                agg_star: false,
                agg_distinct: false,
                agg_order: Vec::new(),
                agg_filter: None,
                over: None,
            })
        } else {
            ResolvedScalarExpr::Column(left_col)
        };
        merged.push(MergedJoinColumn {
            name: c.clone(),
            expr,
            outer,
        });
        consumed.push((lq, c.clone()));
        consumed.push((rq, c.clone()));
    }

    let predicate = where_expr_conjuncts_join(conjuncts)
        .expect("USING/NATURAL resolves at least one join column");
    Ok((
        ResolvedJoinQual::Using {
            columns: cols.to_vec(),
            predicate,
        },
        merged,
        consumed,
    ))
}

/// Resolve a `USING`/`NATURAL` join from its computed `cols`: an empty
/// set is an inner cartesian product (`Cross`) — or, for an outer join,
/// an unsupported condition-less join (forwarded); otherwise the
/// merged-column entries are registered in scope and the `Using`
/// qualifier returned.
pub(super) fn join_using_or_cross(
    scope: &mut ResolutionScope<'_>,
    ranges: JoinScopeRanges,
    cols: Vec<EcoString>,
    join_type: JoinType,
) -> ResolveResult<ResolvedJoinQual> {
    if cols.is_empty() {
        return if join_type == JoinType::Inner {
            Ok(ResolvedJoinQual::Cross)
        } else {
            Err(Report::from(ResolveError::UnsupportedJoinQualifier))
        };
    }
    let (qual, merged, consumed) = join_using_resolve(scope, ranges, &cols, join_type)?;
    scope.merged_columns.extend(merged);
    scope.merged_consumed.extend(consumed);
    Ok(qual)
}
