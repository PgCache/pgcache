//! Walk a resolved query and extract the column constraints and equivalences
//! it implies.
//!
//! The AST half of the module; the value-domain range algebra these constraints
//! reduce to lives in [`range`](super::range).

use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue, MultiOp};
use crate::query::cast::{
    canonicalize_comparison, cast_target_is_coercion_supported, resolved_where_scalar_leaf,
};
use crate::query::resolved::{
    ResolvedScalarExpr, ResolvedSelectNode, ResolvedTableSource, ResolvedWhereExpr,
};

use super::range::{literal_value_is_incomparable, literal_value_order};
use super::{ColumnConstraint, ColumnEquivalence, QueryConstraints, TableConstraint};

/// Extract constraint information from any resolved WHERE expression.
/// Handles equality, inequality, and BETWEEN operators on column-vs-literal comparisons.
///
/// `complete` is set to `false` whenever the analyzer drops an expression
/// without extracting a constraint from it (e.g. `MultiOp::Any`, `OR`,
/// non-comparison binary shapes, subqueries). The caller uses this to
/// gate subsumption: a cached query with a WHERE clause we couldn't
/// fully analyze must not be assumed to be a full table scan.
fn analyze_constraint_expr(
    expr: &ResolvedWhereExpr,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
    complete: &mut bool,
) {
    match expr {
        // Comparison operators: column op value, value op column, column = column
        ResolvedWhereExpr::Binary(binary) if binary.op.is_comparison() => {
            // canonicalize_comparison handles both `col op lit` and
            // `lit op col` (with op_flip), plus stripping identity casts
            // and reporting non-identity cast targets.
            if let Some((col, target, op, val)) = canonicalize_comparison(binary) {
                match target {
                    None => {
                        constraints.insert(ColumnConstraint::Comparison {
                            column: col.clone(),
                            op,
                            value: val.clone(),
                        });
                    }
                    Some(cast)
                        if cast_target_is_coercion_supported(
                            cast,
                            &col.column_metadata.data_type,
                        ) =>
                    {
                        constraints.insert(ColumnConstraint::CastComparison {
                            column: col.clone(),
                            cast: cast.clone(),
                            op,
                            value: val.clone(),
                        });
                    }
                    Some(_) => *complete = false,
                }
                return;
            }
            // column = column (equivalence) — equality only
            if let (Some(ResolvedScalarExpr::Column(left)), Some(ResolvedScalarExpr::Column(right))) = (
                resolved_where_scalar_leaf(&binary.lexpr),
                resolved_where_scalar_leaf(&binary.rexpr),
            ) && binary.op == BinaryOp::Equal
            {
                equivalences.insert(ColumnEquivalence {
                    left: left.clone(),
                    right: right.clone(),
                });
                return;
            }
            *complete = false;
        }

        // AND: recursively analyze both sides
        ResolvedWhereExpr::Binary(binary) if binary.op == BinaryOp::And => {
            analyze_constraint_expr(&binary.lexpr, constraints, equivalences, complete);
            analyze_constraint_expr(&binary.rexpr, constraints, equivalences, complete);
        }

        // BETWEEN / BETWEEN SYMMETRIC: extract as two inequality constraints
        ResolvedWhereExpr::Multi(multi)
            if matches!(multi.op, MultiOp::Between | MultiOp::BetweenSymmetric) =>
        {
            between_constraints_extract(&multi.op, &multi.exprs, constraints);
        }

        // IN: extract as set membership constraint
        ResolvedWhereExpr::Multi(multi) if multi.op == MultiOp::In => {
            in_constraints_extract(&multi.exprs, constraints);
        }

        // NOT IN: extract as individual NotEqual constraints
        ResolvedWhereExpr::Multi(multi) if multi.op == MultiOp::NotIn => {
            not_in_constraints_extract(&multi.exprs, constraints);
        }

        // PGC-106: `col = ANY(<array literal>)` is semantically equivalent
        // to `col IN (<elements>)` for set membership. Extract as `InSet`
        // so the existing per-column range subsumption math handles
        // narrower-array-subsumed-by-wider-array correctly. Only the `=`
        // comparison maps cleanly to set membership; other comparisons
        // (`<`, `<>`, etc.) under ANY have different semantics and stay
        // unhandled, marking the analysis incomplete.
        ResolvedWhereExpr::Multi(multi) if matches!(multi.op, MultiOp::Any { comparison } if comparison == BinaryOp::Equal) =>
        {
            any_eq_array_constraints_extract(&multi.exprs, constraints, complete);
        }

        // Everything else: OR, NOT BETWEEN, ANY/ALL, subqueries, function
        // calls, etc. — cannot extract constraints. Mark the analysis
        // incomplete so subsumption falls back to "not subsumed".
        ResolvedWhereExpr::Scalar(_)
        | ResolvedWhereExpr::Unary(_)
        | ResolvedWhereExpr::Binary(_)
        | ResolvedWhereExpr::Multi(_)
        | ResolvedWhereExpr::Subquery { .. } => {
            *complete = false;
        }
    }
}

/// Extract two inequality constraints from a BETWEEN or BETWEEN SYMMETRIC expression.
/// BETWEEN: column >= low AND column <= high
/// BETWEEN SYMMETRIC: same, but bounds are normalized to (min, max) first.
fn between_constraints_extract(
    op: &MultiOp,
    exprs: &[ResolvedWhereExpr],
    constraints: &mut HashSet<ColumnConstraint>,
) {
    // exprs layout: [subject, low, high]
    let [
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)),
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(low)),
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(high)),
    ] = exprs
    else {
        return;
    };

    let (low, high) = if *op == MultiOp::BetweenSymmetric {
        match literal_value_order(low, high) {
            Some(Ordering::Greater) => (high, low),
            Some(_) => (low, high),
            None => return, // can't compare bounds (Parameter, Null, mixed types)
        }
    } else {
        (low, high)
    };

    constraints.insert(ColumnConstraint::Comparison {
        column: col.clone(),
        op: BinaryOp::GreaterThanOrEqual,
        value: low.clone(),
    });
    constraints.insert(ColumnConstraint::Comparison {
        column: col.clone(),
        op: BinaryOp::LessThanOrEqual,
        value: high.clone(),
    });
}

/// Extract an IN constraint from `column IN (v1, v2, ...)`.
/// exprs layout: [subject, val1, val2, ..., valN]
fn in_constraints_extract(
    exprs: &[ResolvedWhereExpr],
    constraints: &mut HashSet<ColumnConstraint>,
) {
    let Some(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col))) = exprs.first() else {
        return;
    };
    let values = exprs.get(1..).unwrap_or_default();

    // All values must be literals, no Parameters or Nulls
    let mut literal_values: Vec<LiteralValue> = Vec::with_capacity(values.len());
    for expr in values {
        let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(v)) = expr else {
            return;
        };
        if literal_value_is_incomparable(v) {
            return;
        }
        literal_values.push(v.clone());
    }

    // Sort for deterministic Hash on ColumnConstraint::InSet
    literal_values.sort_by(|a, b| literal_value_order(a, b).unwrap_or(Ordering::Equal));
    literal_values.dedup();

    constraints.insert(ColumnConstraint::InSet {
        column: col.clone(),
        values: literal_values,
    });
}

/// Extract an `InSet` constraint from `column = ANY(<array>)`.
/// `exprs` layout: `[subject, rhs]` where `rhs` is one of:
///   - `Literal(LiteralValue::Array(elements, _))` — produced by binary
///     array parameter substitution (PGC-103)
///   - `Array(elements)` — produced by `pg_query`'s `AArrayExpr` for
///     literal `ARRAY[v1, v2, …]` syntax in the original SQL
///
/// Other shapes (subqueries, columns, etc.) leave the constraint set
/// unchanged and mark the WHERE-clause analysis incomplete so
/// subsumption refuses to treat the cached query as a full scan.
fn any_eq_array_constraints_extract(
    exprs: &[ResolvedWhereExpr],
    constraints: &mut HashSet<ColumnConstraint>,
    complete: &mut bool,
) {
    let [
        ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col)),
        ResolvedWhereExpr::Scalar(rhs),
    ] = exprs
    else {
        *complete = false;
        return;
    };

    // Collect element values from either AST shape.
    let mut literal_values: Vec<LiteralValue> = match rhs {
        ResolvedScalarExpr::Literal(LiteralValue::Array(values, _)) => values.clone(),
        ResolvedScalarExpr::Array(elems) => {
            let mut out = Vec::with_capacity(elems.len());
            for elem in elems {
                let ResolvedScalarExpr::Literal(v) = elem else {
                    *complete = false;
                    return;
                };
                out.push(v.clone());
            }
            out
        }
        ResolvedScalarExpr::Literal(_)
        | ResolvedScalarExpr::Column(_)
        | ResolvedScalarExpr::Identifier(_)
        | ResolvedScalarExpr::Function(_)
        | ResolvedScalarExpr::Case(_)
        | ResolvedScalarExpr::Arithmetic(_)
        | ResolvedScalarExpr::Subquery(_, _)
        | ResolvedScalarExpr::TypeCast { .. } => {
            *complete = false;
            return;
        }
    };

    if literal_values.iter().any(literal_value_is_incomparable) {
        *complete = false;
        return;
    }

    // Sort for deterministic Hash on ColumnConstraint::InSet (matching
    // `in_constraints_extract`).
    literal_values.sort_by(|a, b| literal_value_order(a, b).unwrap_or(Ordering::Equal));
    literal_values.dedup();

    constraints.insert(ColumnConstraint::InSet {
        column: col.clone(),
        values: literal_values,
    });
}

/// Extract NOT IN as individual NotEqual constraints.
/// `NOT IN (1, 2, 3)` = `!= 1 AND != 2 AND != 3`
fn not_in_constraints_extract(
    exprs: &[ResolvedWhereExpr],
    constraints: &mut HashSet<ColumnConstraint>,
) {
    let Some(ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Column(col))) = exprs.first() else {
        return;
    };
    let values = exprs.get(1..).unwrap_or_default();

    for expr in values {
        let ResolvedWhereExpr::Scalar(ResolvedScalarExpr::Literal(v)) = expr else {
            return;
        };
        if literal_value_is_incomparable(v) {
            return;
        }
        constraints.insert(ColumnConstraint::Comparison {
            column: col.clone(),
            op: BinaryOp::NotEqual,
            value: v.clone(),
        });
    }
}

/// Collect constraints and equivalences from a table source (handles JOINs recursively)
fn collect_from_table_source(
    source: &ResolvedTableSource,
    constraints: &mut HashSet<ColumnConstraint>,
    equivalences: &mut HashSet<ColumnEquivalence>,
    complete: &mut bool,
) {
    if let ResolvedTableSource::Join(join) = source {
        // Analyze this join's predicate (the ON expr, or the
        // synthesized equi-join for USING/NATURAL).
        if let Some(condition) = join.predicate() {
            analyze_constraint_expr(condition, constraints, equivalences, complete);
        }

        // Recurse into nested joins
        collect_from_table_source(&join.left, constraints, equivalences, complete);
        collect_from_table_source(&join.right, constraints, equivalences, complete);
    }
}

/// Collect all constraints and equivalences from the entire query
pub(super) fn collect_query_constraints(
    resolved: &ResolvedSelectNode,
) -> (HashSet<ColumnConstraint>, HashSet<ColumnEquivalence>, bool) {
    let mut constraints = HashSet::new();
    let mut equivalences = HashSet::new();
    // No WHERE clause is trivially complete — the cache holds the full
    // table for that source. Only set to false when the analyzer hits an
    // expression it can't extract constraints from.
    let mut complete = true;

    if let Some(where_expr) = &resolved.where_clause {
        analyze_constraint_expr(
            where_expr,
            &mut constraints,
            &mut equivalences,
            &mut complete,
        );
    }

    // Analyze JOIN conditions
    for table_source in &resolved.from {
        collect_from_table_source(
            table_source,
            &mut constraints,
            &mut equivalences,
            &mut complete,
        );
    }

    (constraints, equivalences, complete)
}

/// Propagate constraints through column equivalences using fixpoint iteration
pub(super) fn propagate_constraints(
    mut constraints: HashSet<ColumnConstraint>,
    equivalences: &HashSet<ColumnEquivalence>,
) -> HashSet<ColumnConstraint> {
    // Fixpoint iteration: propagate until no changes
    let mut changed = true;
    while changed {
        changed = false;

        let mut new_constraints = Vec::new();

        for equiv in equivalences {
            // Collect constraints on either side and propagate to the other
            for constraint in &constraints {
                let other = if *constraint.column() == equiv.left {
                    &equiv.right
                } else if *constraint.column() == equiv.right {
                    &equiv.left
                } else {
                    continue;
                };
                let propagated = match constraint {
                    ColumnConstraint::Comparison { op, value, .. } => {
                        ColumnConstraint::Comparison {
                            column: other.clone(),
                            op: *op,
                            value: value.clone(),
                        }
                    }
                    ColumnConstraint::InSet { values, .. } => ColumnConstraint::InSet {
                        column: other.clone(),
                        values: values.clone(),
                    },
                    // CastComparison doesn't propagate — `val::int = 5 AND val = other_val`
                    // does NOT imply `other_val::int = 5` unless we also know
                    // `other_val` is cast-compatible. Conservative skip.
                    ColumnConstraint::CastComparison { .. } => continue,
                };
                new_constraints.push(propagated);
            }
        }

        for constraint in new_constraints {
            if constraints.insert(constraint) {
                changed = true;
            }
        }
    }

    constraints
}

/// Analyze a resolved query to determine all constant constraints on columns.
///
/// Subquery terms in WHERE clauses are naturally skipped by `analyze_constraint_expr`,
/// so outer constraints (e.g., `AND tenant_id = 1`) are still correctly extracted
/// even when subqueries are present.
pub fn analyze_query_constraints(resolved: &ResolvedSelectNode) -> QueryConstraints {
    // Step 1: Collect all constraint information (constraints + equivalences)
    let (constraints, equivalences, where_analysis_complete) = collect_query_constraints(resolved);

    // Step 2: Propagate constraints through equivalences
    let column_constraints = propagate_constraints(constraints, &equivalences);

    // Step 3: Organize by table for quick lookup
    let mut table_constraints: HashMap<EcoString, Vec<TableConstraint>> = HashMap::new();
    for constraint in &column_constraints {
        let tc = match constraint {
            ColumnConstraint::Comparison {
                column, op, value, ..
            } => TableConstraint::Comparison(column.column.clone(), *op, value.clone()),
            ColumnConstraint::InSet { column, values, .. } => {
                TableConstraint::AnyOf(column.column.clone(), values.clone())
            }
            ColumnConstraint::CastComparison {
                column,
                cast,
                op,
                value,
            } => TableConstraint::CastComparison(
                column.column.clone(),
                cast.clone(),
                *op,
                value.clone(),
            ),
        };
        table_constraints
            .entry(constraint.column().table.clone())
            .or_default()
            .push(tc);
    }

    QueryConstraints {
        column_constraints,
        equivalences,
        table_constraints,
        where_analysis_complete,
    }
}
