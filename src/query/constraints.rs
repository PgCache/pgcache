use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};

use ecow::EcoString;

use crate::query::ast::{BinaryOp, LiteralValue, MultiOp};
use crate::query::resolved::{
    ResolvedColumnNode, ResolvedScalarExpr, ResolvedSelectNode, ResolvedTableSource,
    ResolvedWhereExpr,
};

/// Unwrap a `WhereExpr::Scalar` to its inner `ResolvedScalarExpr` for pattern matching.
fn binary_scalar_leaf(expr: &ResolvedWhereExpr) -> Option<&ResolvedScalarExpr> {
    if let ResolvedWhereExpr::Scalar(scalar) = expr {
        Some(scalar)
    } else {
        None
    }
}

/// A column constraint extracted from WHERE/JOIN conditions
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ColumnConstraint {
    /// Single comparison: column op value
    Comparison {
        column: ResolvedColumnNode,
        op: BinaryOp,
        value: LiteralValue,
    },
    /// Set membership: column IN (v1, v2, ...)
    /// Values are sorted for deterministic Hash/Eq.
    InSet {
        column: ResolvedColumnNode,
        values: Vec<LiteralValue>,
    },
}

impl ColumnConstraint {
    pub fn column(&self) -> &ResolvedColumnNode {
        match self {
            ColumnConstraint::Comparison { column, .. }
            | ColumnConstraint::InSet { column, .. } => column,
        }
    }
}

/// An equivalence between two columns
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnEquivalence {
    pub left: ResolvedColumnNode,
    pub right: ResolvedColumnNode,
}

impl ColumnEquivalence {
    /// Returns true if this equivalence represents a join condition:
    /// columns from different tables, or same table with different aliases (self-join)
    pub fn is_join(&self) -> bool {
        self.left.table != self.right.table || self.left.table_alias != self.right.table_alias
    }
}

/// A constraint clause organized for per-table CDC matching and subsumption.
/// All clauses within a table's Vec are AND-connected.
#[derive(Debug, Clone, PartialEq)]
pub enum TableConstraint {
    /// Single comparison: column op value
    Comparison(EcoString, BinaryOp, LiteralValue),
    /// At least one must match (IN semantics): column = v1 OR column = v2 OR ...
    AnyOf(EcoString, Vec<LiteralValue>),
}

/// Analysis results for a query showing all constant constraints
#[derive(Debug, Clone)]
pub struct QueryConstraints {
    /// All column constraints (from WHERE + propagated through JOINs)
    pub column_constraints: HashSet<ColumnConstraint>,

    /// Column equivalences from JOIN conditions and WHERE clause
    pub equivalences: HashSet<ColumnEquivalence>,

    /// Constraints organized by table for quick lookup
    pub table_constraints: HashMap<EcoString, Vec<TableConstraint>>,

    /// True when the analyzer recognized every expression in the WHERE
    /// clause; false if it encountered any expression it can't extract
    /// constraints from (e.g. `MultiOp::Any`, `OR`, `Unary`, function
    /// calls, subqueries). PGC-106: subsumption uses this to distinguish
    /// "no WHERE clause / full table scan" (complete=true, empty
    /// `table_constraints`) from "WHERE clause we couldn't analyze"
    /// (complete=false, empty `table_constraints`). Without this, a
    /// cached query with `WHERE id = ANY(...)` was wrongly treated as a
    /// full scan and incorrectly subsumed unrelated queries.
    pub where_analysis_complete: bool,
}

impl Default for QueryConstraints {
    fn default() -> Self {
        Self {
            column_constraints: HashSet::new(),
            equivalences: HashSet::new(),
            table_constraints: HashMap::new(),
            where_analysis_complete: true,
        }
    }
}

impl QueryConstraints {
    /// Returns column names involved in join conditions for the given table
    pub fn table_join_columns<'a>(&'a self, table_name: &'a str) -> impl Iterator<Item = &'a str> {
        self.equivalences
            .iter()
            .filter(|eq| eq.is_join())
            .filter_map(move |eq| {
                if eq.left.table == table_name {
                    Some(eq.left.column.as_str())
                } else if eq.right.table == table_name {
                    Some(eq.right.column.as_str())
                } else {
                    None
                }
            })
    }
}

// ============================================================================
// ColumnRange: per-column constraint reduction for subsumption
// ============================================================================

/// One end of a column's value range
#[derive(Debug, Clone)]
struct RangeBound {
    value: LiteralValue,
    inclusive: bool, // true = >= or <=, false = > or <
}

/// Canonical representation of all constraints on a single column, reduced
/// from a set of (BinaryOp, LiteralValue) pairs. Used by subsumption checking.
#[derive(Debug, Clone)]
enum ColumnRange {
    /// Values are incomparable (Parameter, Null, mixed types) — can't reason
    Unknown,
    /// No constraints — any value matches
    Unconstrained,
    /// Contradictory constraints — no value can satisfy (e.g., = 5 AND > 10)
    Empty,
    /// Exactly one value: column = v
    Equal(LiteralValue),
    /// Finite set of allowed values: column IN (v1, v2, ...)
    InSet(HashSet<LiteralValue>),
    /// Bounded interval with possible exclusions
    Range {
        lower: Option<RangeBound>,
        upper: Option<RangeBound>,
        not_equal: Vec<LiteralValue>,
    },
}

/// Returns true if the value is incomparable for range analysis (Parameter, Null, NullWithCast).
fn literal_value_is_incomparable(v: &LiteralValue) -> bool {
    matches!(
        v,
        LiteralValue::Parameter(_) | LiteralValue::Null | LiteralValue::NullWithCast(_)
    )
}

/// Tighten a lower bound: keep the higher (more restrictive) of the two.
/// At equal values, exclusive (>) is tighter than inclusive (>=).
/// Returns None if values are incomparable.
fn lower_bound_tighten(existing: &RangeBound, candidate: &RangeBound) -> Option<RangeBound> {
    literal_value_order(&existing.value, &candidate.value).map(|ord| {
        match ord {
            // candidate is higher → tighter
            Ordering::Less => candidate.clone(),
            // existing is higher → keep it
            Ordering::Greater => existing.clone(),
            // same value: exclusive wins
            Ordering::Equal => RangeBound {
                value: existing.value.clone(),
                inclusive: existing.inclusive && candidate.inclusive,
            },
        }
    })
}

/// Tighten an upper bound: keep the lower (more restrictive) of the two.
/// At equal values, exclusive (<) is tighter than inclusive (<=).
/// Returns None if values are incomparable.
fn upper_bound_tighten(existing: &RangeBound, candidate: &RangeBound) -> Option<RangeBound> {
    literal_value_order(&existing.value, &candidate.value).map(|ord| {
        match ord {
            // candidate is lower → tighter
            Ordering::Greater => candidate.clone(),
            // existing is lower → keep it
            Ordering::Less => existing.clone(),
            // same value: exclusive wins
            Ordering::Equal => RangeBound {
                value: existing.value.clone(),
                inclusive: existing.inclusive && candidate.inclusive,
            },
        }
    })
}

/// Check if a value satisfies a lower bound (value > bound or value >= bound).
/// Returns None if values are incomparable.
fn value_satisfies_lower(value: &LiteralValue, bound: &RangeBound) -> Option<bool> {
    literal_value_order(value, &bound.value).map(|ord| match ord {
        Ordering::Greater => true,
        Ordering::Equal => bound.inclusive,
        Ordering::Less => false,
    })
}

/// Check if a value satisfies an upper bound (value < bound or value <= bound).
/// Returns None if values are incomparable.
fn value_satisfies_upper(value: &LiteralValue, bound: &RangeBound) -> Option<bool> {
    literal_value_order(value, &bound.value).map(|ord| match ord {
        Ordering::Less => true,
        Ordering::Equal => bound.inclusive,
        Ordering::Greater => false,
    })
}

/// Build a ColumnRange from all constraints on a single column.
fn column_range_build(constraints: &[&TableConstraint]) -> ColumnRange {
    if constraints.is_empty() {
        return ColumnRange::Unconstrained;
    }

    // Separate comparisons from in-sets
    let mut comparisons: Vec<(BinaryOp, &LiteralValue)> = Vec::new();
    let mut in_set: Option<&[LiteralValue]> = None;

    for tc in constraints {
        match tc {
            TableConstraint::Comparison(_, op, value) => {
                comparisons.push((*op, value));
            }
            TableConstraint::AnyOf(_, values) => {
                // Multiple AnyOf on same column: intersect sets
                in_set = Some(match in_set {
                    None => values.as_slice(),
                    Some(_existing) => {
                        // Rare case — for now treat as Unknown
                        return ColumnRange::Unknown;
                    }
                });
            }
        }
    }

    // If we have an in-set, integrate with any comparisons
    if let Some(set_values) = in_set {
        return in_set_range_build(set_values, &comparisons);
    }

    // No in-set — pure comparison logic
    comparison_range_build(&comparisons)
}

/// Build a ColumnRange from an IN-set, optionally intersected with comparisons.
fn in_set_range_build(
    set_values: &[LiteralValue],
    comparisons: &[(BinaryOp, &LiteralValue)],
) -> ColumnRange {
    if set_values.is_empty() {
        return ColumnRange::Empty;
    }

    // Any incomparable value in the set makes it unknowable
    if set_values.iter().any(literal_value_is_incomparable) {
        return ColumnRange::Unknown;
    }

    // If no comparisons, return the set directly
    if comparisons.is_empty() {
        return ColumnRange::InSet(set_values.iter().cloned().collect());
    }

    // Build a temporary range from comparisons and filter the set
    let filter_range = comparison_range_build(comparisons);

    match filter_range {
        ColumnRange::Unknown => ColumnRange::Unknown,
        ColumnRange::Empty => ColumnRange::Empty,
        ColumnRange::Unconstrained => ColumnRange::InSet(set_values.iter().cloned().collect()),
        ColumnRange::Equal(v) => {
            if set_values.contains(&v) {
                ColumnRange::Equal(v)
            } else {
                ColumnRange::Empty
            }
        }
        ColumnRange::InSet(_) => unreachable!("comparison_range_build never produces InSet"),
        ColumnRange::Range {
            ref lower,
            ref upper,
            ref not_equal,
        } => {
            let mut iter = set_values
                .iter()
                .filter(|v| range_contains_value(lower, upper, not_equal, v))
                .cloned();
            match iter.next() {
                None => ColumnRange::Empty,
                Some(first) => match iter.next() {
                    None => ColumnRange::Equal(first),
                    Some(second) => {
                        let mut set: HashSet<LiteralValue> = HashSet::from_iter([first, second]);
                        set.extend(iter);
                        ColumnRange::InSet(set)
                    }
                },
            }
        }
    }
}

/// Build a ColumnRange from comparison-only constraints (no in-sets).
fn comparison_range_build(comparisons: &[(BinaryOp, &LiteralValue)]) -> ColumnRange {
    if comparisons.is_empty() {
        return ColumnRange::Unconstrained;
    }

    let mut equal_value: Option<&LiteralValue> = None;
    let mut lower: Option<RangeBound> = None;
    let mut upper: Option<RangeBound> = None;
    let mut not_equal: Vec<LiteralValue> = Vec::new();

    for &(op, value) in comparisons {
        if literal_value_is_incomparable(value) {
            return ColumnRange::Unknown;
        }
        match op {
            BinaryOp::Equal => match equal_value {
                None => equal_value = Some(value),
                Some(existing) if *existing == *value => {} // duplicate
                Some(_) => return ColumnRange::Empty,       // contradictory: = 5 AND = 3
            },
            BinaryOp::NotEqual => {
                not_equal.push(value.clone());
            }
            BinaryOp::GreaterThan | BinaryOp::GreaterThanOrEqual => {
                let candidate = RangeBound {
                    value: value.clone(),
                    inclusive: op == BinaryOp::GreaterThanOrEqual,
                };
                lower = Some(match lower {
                    None => candidate,
                    Some(existing) => match lower_bound_tighten(&existing, &candidate) {
                        Some(tighter) => tighter,
                        None => return ColumnRange::Unknown,
                    },
                });
            }
            BinaryOp::LessThan | BinaryOp::LessThanOrEqual => {
                let candidate = RangeBound {
                    value: value.clone(),
                    inclusive: op == BinaryOp::LessThanOrEqual,
                };
                upper = Some(match upper {
                    None => candidate,
                    Some(existing) => match upper_bound_tighten(&existing, &candidate) {
                        Some(tighter) => tighter,
                        None => return ColumnRange::Unknown,
                    },
                });
            }
            BinaryOp::And
            | BinaryOp::Or
            | BinaryOp::Like
            | BinaryOp::ILike
            | BinaryOp::NotLike
            | BinaryOp::NotILike => return ColumnRange::Unknown,
        }
    }

    // If we have an equality, validate it against bounds and not-equals
    if let Some(eq_val) = equal_value {
        if let Some(ref lb) = lower {
            match value_satisfies_lower(eq_val, lb) {
                Some(true) => {}
                Some(false) => return ColumnRange::Empty,
                None => return ColumnRange::Unknown,
            }
        }
        if let Some(ref ub) = upper {
            match value_satisfies_upper(eq_val, ub) {
                Some(true) => {}
                Some(false) => return ColumnRange::Empty,
                None => return ColumnRange::Unknown,
            }
        }
        if not_equal.contains(eq_val) {
            return ColumnRange::Empty;
        }
        return ColumnRange::Equal(eq_val.clone());
    }

    // Check that bounds aren't contradictory (lower > upper)
    if let (Some(lb), Some(ub)) = (&lower, &upper) {
        match literal_value_order(&lb.value, &ub.value) {
            Some(Ordering::Greater) => return ColumnRange::Empty,
            Some(Ordering::Equal) => {
                if !lb.inclusive || !ub.inclusive {
                    return ColumnRange::Empty;
                }
                // Both inclusive at same value: degenerate range → single point
                if not_equal.contains(&lb.value) {
                    return ColumnRange::Empty;
                }
                return ColumnRange::Equal(lb.value.clone());
            }
            Some(Ordering::Less) => {} // valid range
            None => return ColumnRange::Unknown,
        }
    }

    ColumnRange::Range {
        lower,
        upper,
        not_equal,
    }
}

/// Check if a value falls within a range (satisfies bounds and isn't excluded).
fn range_contains_value(
    lower: &Option<RangeBound>,
    upper: &Option<RangeBound>,
    not_equal: &[LiteralValue],
    value: &LiteralValue,
) -> bool {
    if let Some(lb) = lower {
        match value_satisfies_lower(value, lb) {
            Some(true) => {}
            _ => return false, // fails bound or incomparable
        }
    }
    if let Some(ub) = upper {
        match value_satisfies_upper(value, ub) {
            Some(true) => {}
            _ => return false,
        }
    }
    !not_equal.contains(value)
}

/// Check if a lower bound `a` is at least as tight as lower bound `b`.
/// "At least as tight" means a >= b (a excludes fewer values on the low end).
fn lower_bound_at_least_as_tight(a: &RangeBound, b: &RangeBound) -> Option<bool> {
    literal_value_order(&a.value, &b.value).map(|ord| match ord {
        Ordering::Greater => true,
        Ordering::Less => false,
        // Same value: a is at least as tight if a is exclusive or both are inclusive
        Ordering::Equal => !a.inclusive || b.inclusive,
    })
}

/// Check if an upper bound `a` is at least as tight as upper bound `b`.
/// "At least as tight" means a <= b.
fn upper_bound_at_least_as_tight(a: &RangeBound, b: &RangeBound) -> Option<bool> {
    literal_value_order(&a.value, &b.value).map(|ord| match ord {
        Ordering::Less => true,
        Ordering::Greater => false,
        Ordering::Equal => !a.inclusive || b.inclusive,
    })
}

/// Check if new's range is contained within cached's range, and all cached
/// exclusions are satisfied by new.
fn range_subsumes_range(
    cached_lower: &Option<RangeBound>,
    cached_upper: &Option<RangeBound>,
    cached_not_equal: &[LiteralValue],
    new_lower: &Option<RangeBound>,
    new_upper: &Option<RangeBound>,
    new_not_equal: &[LiteralValue],
) -> bool {
    // Cached has lower bound → new must have one that's at least as tight
    if let Some(cl) = cached_lower {
        match new_lower {
            None => return false, // new is open-ended below
            Some(nl) => match lower_bound_at_least_as_tight(nl, cl) {
                Some(true) => {}
                _ => return false,
            },
        }
    }

    // Cached has upper bound → new must have one that's at least as tight
    if let Some(cu) = cached_upper {
        match new_upper {
            None => return false, // new is open-ended above
            Some(nu) => match upper_bound_at_least_as_tight(nu, cu) {
                Some(true) => {}
                _ => return false,
            },
        }
    }

    // Each cached not_equal must be excluded by new: either in new's not_equal
    // list, or outside new's range entirely
    for excluded in cached_not_equal {
        if new_not_equal.contains(excluded) {
            continue;
        }
        // Check if the excluded value is outside new's range
        if !range_contains_value(new_lower, new_upper, &[], excluded) {
            continue;
        }
        // The value is inside new's range and not in new's exclusion list
        return false;
    }

    true
}

/// Check if cached's ColumnRange subsumes new's ColumnRange.
/// Returns true if every value matching new also matches cached.
fn column_range_subsumes(cached: &ColumnRange, new: &ColumnRange) -> bool {
    match (cached, new) {
        // Unknown: can't reason
        (ColumnRange::Unknown, _) | (_, ColumnRange::Unknown) => false,

        // Empty cached: no data to serve from
        (ColumnRange::Empty, _) => false,

        // Empty new: returns nothing, trivially covered
        (_, ColumnRange::Empty) => true,

        // Unconstrained cached: loaded all rows
        (ColumnRange::Unconstrained, _) => true,

        // Unconstrained new: wants everything, cached is restricted
        (_, ColumnRange::Unconstrained) => false,

        // Equal vs Equal
        (ColumnRange::Equal(a), ColumnRange::Equal(b)) => *a == *b,

        // Equal cached can't subsume anything broader
        (ColumnRange::Equal(_), ColumnRange::Range { .. } | ColumnRange::InSet(_)) => false,

        // InSet cached, InSet new: subset check
        (ColumnRange::InSet(cached_set), ColumnRange::InSet(new_set)) => {
            new_set.is_subset(cached_set)
        }

        // InSet cached, Equal new: point in set
        (ColumnRange::InSet(set), ColumnRange::Equal(v)) => set.contains(v),

        // InSet cached, Range new: set is finite, range may be infinite — not subsumed
        (ColumnRange::InSet(_), ColumnRange::Range { .. }) => false,

        // Range cached, InSet new: check all values in the set are within range
        (
            ColumnRange::Range {
                lower,
                upper,
                not_equal,
            },
            ColumnRange::InSet(set),
        ) => set
            .iter()
            .all(|v| range_contains_value(lower, upper, not_equal, v)),

        // Range cached, Equal new: check point within interval
        (
            ColumnRange::Range {
                lower,
                upper,
                not_equal,
            },
            ColumnRange::Equal(v),
        ) => range_contains_value(lower, upper, not_equal, v),

        // Range vs Range: full containment check
        (
            ColumnRange::Range {
                lower: cl,
                upper: cu,
                not_equal: cne,
            },
            ColumnRange::Range {
                lower: nl,
                upper: nu,
                not_equal: nne,
            },
        ) => range_subsumes_range(cl, cu, cne, nl, nu, nne),
    }
}

/// Group table constraints by column name for per-column range building.
fn constraints_group_by_column<'a>(
    constraints: &'a [TableConstraint],
) -> HashMap<&'a str, Vec<&'a TableConstraint>> {
    let mut grouped: HashMap<&'a str, Vec<&'a TableConstraint>> = HashMap::new();
    for tc in constraints {
        let col = match tc {
            TableConstraint::Comparison(col, _, _) | TableConstraint::AnyOf(col, _) => col.as_str(),
        };
        grouped.entry(col).or_default().push(tc);
    }
    grouped
}

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
            match (
                binary_scalar_leaf(&binary.lexpr),
                binary_scalar_leaf(&binary.rexpr),
            ) {
                // column op literal
                (Some(ResolvedScalarExpr::Column(col)), Some(ResolvedScalarExpr::Literal(val))) => {
                    constraints.insert(ColumnConstraint::Comparison {
                        column: col.clone(),
                        op: binary.op,
                        value: val.clone(),
                    });
                }
                // literal op column → column op_flip literal
                (Some(ResolvedScalarExpr::Literal(val)), Some(ResolvedScalarExpr::Column(col))) => {
                    if let Some(flipped) = binary.op.op_flip() {
                        constraints.insert(ColumnConstraint::Comparison {
                            column: col.clone(),
                            op: flipped,
                            value: val.clone(),
                        });
                    } else {
                        *complete = false;
                    }
                }
                // column = column (equivalence) — equality only
                (
                    Some(ResolvedScalarExpr::Column(left)),
                    Some(ResolvedScalarExpr::Column(right)),
                ) if binary.op == BinaryOp::Equal => {
                    equivalences.insert(ColumnEquivalence {
                        left: left.clone(),
                        right: right.clone(),
                    });
                }
                _ => *complete = false,
            }
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

/// Compare two literal values for ordering. Returns None if the values
/// are not comparable (different types, Parameters, Nulls).
fn literal_value_order(a: &LiteralValue, b: &LiteralValue) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (LiteralValue::Integer(a), LiteralValue::Integer(b)) => Some(a.cmp(b)),
        (LiteralValue::Float(a), LiteralValue::Float(b)) => Some(a.cmp(b)),
        (LiteralValue::String(a), LiteralValue::String(b)) => Some(a.cmp(b)),
        (LiteralValue::StringWithCast(a, _), LiteralValue::StringWithCast(b, _)) => Some(a.cmp(b)),
        _ => None,
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
fn collect_query_constraints(
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
fn propagate_constraints(
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

/// Returns true if the cached query's constraints on `table` are implied
/// by the new query's constraints. Per-column range reduction: each column
/// cached constrains must have a new range that fits within the cached range.
///
/// When the cached query has no constraints on a table, it loaded all rows — subsumed.
/// When the cached query has constraints but the new query doesn't for that table,
/// the new query is broader — not subsumed.
pub fn table_constraints_subsumed(
    new: &QueryConstraints,
    cached: &QueryConstraints,
    table: &str,
) -> bool {
    // PGC-106: if the analyzer couldn't fully understand the cached
    // query's WHERE clause, an empty `table_constraints` doesn't mean
    // "full table scan" — it means "we don't know what the cache holds".
    // Refuse to subsume in that case so the new query falls through and
    // gets its own cache entry.
    if !cached.where_analysis_complete {
        return false;
    }

    let cached_for_table = cached.table_constraints.get(table);
    let new_for_table = new.table_constraints.get(table);

    match (cached_for_table, new_for_table) {
        // Cached has no constraints on this table → full scan, all rows loaded. Subsumed.
        (None, _) => true,
        // Cached has constraints but new doesn't → new is broader than cached.
        (Some(_), None) => false,
        // Both have constraints → per-column range subsumption.
        (Some(cached_cs), Some(new_cs)) => {
            let cached_by_col = constraints_group_by_column(cached_cs);
            let new_by_col = constraints_group_by_column(new_cs);

            cached_by_col.iter().all(|(col, cached_col_cs)| {
                let cached_range = column_range_build(cached_col_cs.as_slice());
                let new_range = new_by_col
                    .get(col)
                    .map_or(ColumnRange::Unconstrained, |cs| {
                        column_range_build(cs.as_slice())
                    });
                column_range_subsumes(&cached_range, &new_range)
            })
        }
    }
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

#[cfg(test)]
mod tests {

    #![allow(clippy::wildcard_enum_match_arm)]

    use iddqd::BiHashMap;
    use tokio_postgres::types::Type;

    use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata};
    use crate::query::ast::{QueryBody, query_expr_convert};
    use crate::query::resolved::select_node_resolve;

    use super::*;

    // Helper function to parse SQL and resolve to ResolvedSelectNode
    fn resolve_sql(sql: &str, tables: &BiHashMap<TableMetadata>) -> ResolvedSelectNode {
        let ast = pg_query::parse(sql).expect("parse SQL");
        let query_expr = query_expr_convert(&ast).expect("convert to QueryExpr");
        let QueryBody::Select(node) = query_expr.body else {
            panic!("expected SELECT");
        };
        select_node_resolve(&node, tables, &["public"]).expect("resolve")
    }

    // Helper function to create test table metadata
    fn test_table_metadata(name: &str, relation_oid: u32) -> TableMetadata {
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
            relation_oid,
            name: name.into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".to_owned()],
            columns,
            indexes: Vec::new(),
        }
    }

    /// Helper to check if table_constraints contains a specific (column, op, value) triple
    fn has_constraint(
        constraints: &QueryConstraints,
        table: &str,
        column: &str,
        op: BinaryOp,
        value: LiteralValue,
    ) -> bool {
        constraints.table_constraints.get(table).is_some_and(|cs| {
            cs.iter().any(|tc| match tc {
                TableConstraint::Comparison(c, o, v) => c == column && *o == op && *v == value,
                TableConstraint::AnyOf(..) => false,
            })
        })
    }

    fn has_in_constraint(
        constraints: &QueryConstraints,
        table: &str,
        column: &str,
        values: &[LiteralValue],
    ) -> bool {
        constraints.table_constraints.get(table).is_some_and(|cs| {
            cs.iter().any(|tc| match tc {
                TableConstraint::AnyOf(c, vs) => {
                    c == column && values.iter().all(|v| vs.contains(v)) && vs.len() == values.len()
                }
                TableConstraint::Comparison(..) => false,
            })
        })
    }

    // ========== Existing equality tests (updated for new tuple format) ==========

    #[test]
    fn test_simple_constraint() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert_eq!(constraints.table_constraints.get("users").unwrap().len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_join_propagation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("test", 1001));
        tables.insert_overwrite(test_table_metadata("test_map", 1002));

        let sql = "SELECT * FROM test t JOIN test_map tm ON tm.id = t.id WHERE t.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate: t.id = 1 -> tm.id = 1
        assert_eq!(constraints.column_constraints.len(), 2);

        let test_constraints = constraints.table_constraints.get("test").unwrap();
        assert_eq!(test_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "test",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));

        let test_map_constraints = constraints.table_constraints.get("test_map").unwrap();
        assert_eq!(test_map_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "test_map",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));

        assert_eq!(constraints.equivalences.len(), 1);
    }

    #[test]
    fn test_transitive_propagation() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        tables.insert_overwrite(TableMetadata {
            relation_oid: 1003,
            name: "c".into(),
            schema: "public".into(),
            primary_key_columns: vec!["id".to_owned()],
            columns: ColumnStore::new([ColumnMetadata {
                name: "id".into(),
                position: 1,
                type_oid: 23,
                data_type: Type::INT4,
                type_name: "int4".into(),
                cache_type_name: "int4".into(),
                is_primary_key: true,
            }]),
            indexes: Vec::new(),
        });

        let sql = "SELECT * FROM (a JOIN b ON a.id = b.id) JOIN c ON b.id = c.id WHERE a.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate through: a.id = 1 -> b.id = 1 -> c.id = 1
        assert_eq!(constraints.column_constraints.len(), 3);

        assert!(has_constraint(
            &constraints,
            "a",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1)
        ));
        assert!(has_constraint(
            &constraints,
            "b",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1)
        ));
        assert!(has_constraint(
            &constraints,
            "c",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1)
        ));
    }

    #[test]
    fn test_multiple_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 AND name = 'john'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);

        let user_constraints = constraints.table_constraints.get("users").unwrap();
        assert_eq!(user_constraints.len(), 2);

        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("john".to_owned()),
        ));
    }

    #[test]
    fn test_equivalence_in_where() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a, b WHERE a.id = b.id AND a.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "a",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1)
        ));
        assert!(has_constraint(
            &constraints,
            "b",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1)
        ));
    }

    #[test]
    fn test_no_propagation_with_or() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 OR id = 2";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
        assert_eq!(constraints.table_constraints.len(), 0);
    }

    #[test]
    fn test_self_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("test", 1001));

        let sql = "SELECT * FROM test t1 JOIN test t2 ON t1.id = t2.id WHERE t1.id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Both t1.id and t2.id reference the same column (test.id)
        // So we get 1 unique column with constraint
        assert_eq!(constraints.column_constraints.len(), 1);

        let test_constraints = constraints.table_constraints.get("test").unwrap();
        assert_eq!(test_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "test",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_subquery_extracts_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_derived_table_no_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM (SELECT id FROM users WHERE id = 1) AS sub";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert!(
            constraints.column_constraints.is_empty(),
            "Derived table with no outer WHERE should have no constraints"
        );
    }

    #[test]
    fn test_scalar_subquery_extracts_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("orders", 1002));

        let sql = "SELECT id, (SELECT COUNT(*) FROM orders) FROM users WHERE id = 1";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
    }

    #[test]
    fn test_subquery_multiple_outer_constraints() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        tables.insert_overwrite(test_table_metadata("active_users", 1002));

        let sql = "SELECT * FROM users WHERE id IN (SELECT id FROM active_users) AND id = 1 AND name = 'alice'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("alice".to_owned()),
        ));
    }

    // ========== Inequality tests ==========

    #[test]
    fn test_simple_inequality() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id > 5";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
    }

    #[test]
    fn test_multiple_inequalities_same_column() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id > 5 AND id < 100";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThan,
            LiteralValue::Integer(100),
        ));
    }

    #[test]
    fn test_reversed_operand() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // 5 < id is equivalent to id > 5
        let sql = "SELECT * FROM users WHERE 5 < id";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
    }

    #[test]
    fn test_not_equal() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE name != 'deleted'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 1);
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::NotEqual,
            LiteralValue::String("deleted".to_owned()),
        ));
    }

    #[test]
    fn test_mixed_equality_and_inequality() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id = 1 AND name != 'deleted'";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::Equal,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::NotEqual,
            LiteralValue::String("deleted".to_owned()),
        ));
    }

    #[test]
    fn test_inequality_propagation_through_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id > 5";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Should propagate: a.id > 5 -> b.id > 5
        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "a",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
        assert!(has_constraint(
            &constraints,
            "b",
            "id",
            BinaryOp::GreaterThan,
            LiteralValue::Integer(5),
        ));
    }

    #[test]
    fn test_or_prevents_inequality_extraction() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id > 5 OR id < 2";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    // ========== BETWEEN tests ==========

    #[test]
    fn test_between() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id BETWEEN 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_with_and() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE name = 'alice' AND id BETWEEN 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 3);
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("alice".to_owned()),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_propagation_through_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id BETWEEN 1 AND 10";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        // Both tables should get the two BETWEEN constraints
        assert_eq!(constraints.column_constraints.len(), 4);
        assert!(has_constraint(
            &constraints,
            "a",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(1)
        ));
        assert!(has_constraint(
            &constraints,
            "a",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(10)
        ));
        assert!(has_constraint(
            &constraints,
            "b",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(1)
        ));
        assert!(has_constraint(
            &constraints,
            "b",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(10)
        ));
    }

    #[test]
    fn test_not_between() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // NOT BETWEEN is an OR (id < 100 OR id > 500), so no constraints
        let sql = "SELECT * FROM users WHERE id NOT BETWEEN 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    #[test]
    fn test_between_symmetric_reversed_bounds() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Bounds are reversed (500, 100) — should normalize to (100, 500)
        let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 500 AND 100";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_symmetric_already_ordered() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Bounds already in order — same result as reversed
        let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC 100 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 2);
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::GreaterThanOrEqual,
            LiteralValue::Integer(100),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::LessThanOrEqual,
            LiteralValue::Integer(500),
        ));
    }

    #[test]
    fn test_between_symmetric_with_parameter() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Can't compare parameter with literal — skip extraction
        let sql = "SELECT * FROM users WHERE id BETWEEN SYMMETRIC $1 AND 500";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    #[test]
    fn test_not_between_symmetric() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // NOT BETWEEN SYMMETRIC is still an OR — no constraints
        let sql = "SELECT * FROM users WHERE id NOT BETWEEN SYMMETRIC 500 AND 100";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    // ========== Subsumption tests ==========

    #[test]
    fn test_subsumption_cached_no_constraints() {
        // Cached: SELECT * FROM users (no WHERE) → full scan covers everything
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_same_equality() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_new_narrower() {
        // Cached has fewer equality constraints → new is narrower. Subsumed.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = 1 AND name = 'alice'",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_different_values() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_cached_has_extra_constraint() {
        // Cached is narrower than new → not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = 1 AND name = 'alice'",
            &tables,
        ));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_new_no_constraints() {
        // New has no constraints but cached does → new is broader, not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 1", &tables));
        let new = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_range_tighter_lower() {
        // id > 10 implies id > 5 — subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 10", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_between_with_non_literal_bounds() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        // Non-literal bound (column reference) — skip extraction
        let sql = "SELECT * FROM users WHERE id BETWEEN name AND 10";
        let resolved = resolve_sql(sql, &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert_eq!(constraints.column_constraints.len(), 0);
    }

    // ========== Range subsumption tests ==========

    #[test]
    fn test_subsumption_range_looser_lower() {
        // id > 1 does NOT imply id > 3 — not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 1", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_range_exclusive_tighter_than_inclusive() {
        // id > 3 (exclusive) is tighter than id >= 3 (inclusive) — subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id >= 3", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_range_same_inclusive_bound() {
        // id >= 3 subsumed by id >= 3
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id >= 3", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id >= 3", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_range_containment() {
        // id BETWEEN 5 AND 8 is contained in id >= 3 AND id <= 10
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id >= 3 AND id <= 10",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id BETWEEN 5 AND 8",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_range_missing_upper() {
        // id > 50 has no upper bound, but cached has id < 100 — not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id > 0 AND id < 100",
            &tables,
        ));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 50", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_point_in_range() {
        // id = 5 is within id > 3
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_point_outside_range() {
        // id = 2 is NOT within id > 3
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_equal_not_subsumed_by_range() {
        // Cached = 5 (single point), new wants id > 3 (a range) — not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_not_equal_by_different_equal() {
        // Cached != 5, new = 3. 3 ≠ 5, so new's result is within cached's — subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 3", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_not_equal_by_same_equal() {
        // Cached != 5, new = 5 — 5 is excluded by cached. Not subsumed.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_not_equal_by_excluding_range() {
        // Cached != 5, new id > 10 — entire range excludes 5. Subsumed.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 10", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_not_equal_by_including_range() {
        // Cached != 5, new id > 3 — range includes 5. Not subsumed.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 3", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_not_equal_same() {
        // Cached != 5, new != 5 — same exclusion. Subsumed.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_not_equal_different() {
        // Cached != 5, new != 3 — different exclusions. Not subsumed.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 5", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id != 3", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_mixed_columns() {
        // Both columns must be subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id > 3 AND name = 'alice'",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id > 5 AND name = 'alice'",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_mixed_columns_mismatch() {
        // id subsumed but name differs — not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id > 3 AND name = 'alice'",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id > 5 AND name = 'bob'",
            &tables,
        ));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_contradictory_new() {
        // New has contradictory constraints (= 5 AND > 10) → Empty → trivially subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = 5 AND id > 10",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_contradictory_cached() {
        // Cached has contradictory constraints → Empty → no data, not subsumed
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = 5 AND id > 10",
            &tables,
        ));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    // ========== ColumnRange unit tests ==========

    /// Helper: build a ColumnRange from comparison tuples (convenience for tests)
    fn range_from_comparisons(comparisons: &[(BinaryOp, LiteralValue)]) -> ColumnRange {
        let tcs: Vec<TableConstraint> = comparisons
            .iter()
            .map(|(op, val)| TableConstraint::Comparison("col".into(), *op, val.clone()))
            .collect();
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        column_range_build(&refs)
    }

    #[test]
    fn test_column_range_build_unconstrained() {
        let range = range_from_comparisons(&[]);
        assert!(matches!(range, ColumnRange::Unconstrained));
    }

    #[test]
    fn test_column_range_build_equal() {
        let range = range_from_comparisons(&[(BinaryOp::Equal, LiteralValue::Integer(5))]);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
    }

    #[test]
    fn test_column_range_build_contradictory_equals() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::Equal, LiteralValue::Integer(3)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_equal_with_contradictory_bound() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::GreaterThan, LiteralValue::Integer(10)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_equal_with_consistent_bound() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::GreaterThan, LiteralValue::Integer(3)),
        ]);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
    }

    #[test]
    fn test_column_range_build_equal_with_not_equal_contradiction() {
        let range = range_from_comparisons(&[
            (BinaryOp::Equal, LiteralValue::Integer(5)),
            (BinaryOp::NotEqual, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_bounds_contradictory() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThan, LiteralValue::Integer(10)),
            (BinaryOp::LessThan, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_bounds_equal_exclusive() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThan, LiteralValue::Integer(5)),
            (BinaryOp::LessThan, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_column_range_build_bounds_equal_inclusive() {
        // >= 5 AND <= 5 → collapses to Equal(5)
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThanOrEqual, LiteralValue::Integer(5)),
            (BinaryOp::LessThanOrEqual, LiteralValue::Integer(5)),
        ]);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(5))
        ));
    }

    #[test]
    fn test_column_range_build_parameter_unknown() {
        let range =
            range_from_comparisons(&[(BinaryOp::Equal, LiteralValue::Parameter("$1".to_owned()))]);
        assert!(matches!(range, ColumnRange::Unknown));
    }

    #[test]
    fn test_column_range_build_null_unknown() {
        let range = range_from_comparisons(&[(BinaryOp::Equal, LiteralValue::Null)]);
        assert!(matches!(range, ColumnRange::Unknown));
    }

    #[test]
    fn test_column_range_build_lower_tightening() {
        let range = range_from_comparisons(&[
            (BinaryOp::GreaterThan, LiteralValue::Integer(3)),
            (BinaryOp::GreaterThan, LiteralValue::Integer(7)),
        ]);
        match range {
            ColumnRange::Range {
                lower: Some(lb),
                upper: None,
                ..
            } => {
                assert_eq!(lb.value, LiteralValue::Integer(7));
                assert!(!lb.inclusive);
            }
            _ => panic!("expected Range with lower bound"),
        }
    }

    #[test]
    fn test_column_range_build_upper_tightening() {
        let range = range_from_comparisons(&[
            (BinaryOp::LessThan, LiteralValue::Integer(10)),
            (BinaryOp::LessThan, LiteralValue::Integer(5)),
        ]);
        match range {
            ColumnRange::Range {
                lower: None,
                upper: Some(ub),
                ..
            } => {
                assert_eq!(ub.value, LiteralValue::Integer(5));
                assert!(!ub.inclusive);
            }
            _ => panic!("expected Range with upper bound"),
        }
    }

    // ========== IN extraction tests ==========

    #[test]
    fn test_in_extraction() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let resolved = resolve_sql(sql, &tables);
        let constraints = analyze_query_constraints(&resolved);

        assert!(has_in_constraint(
            &constraints,
            "users",
            "id",
            &[
                LiteralValue::Integer(1),
                LiteralValue::Integer(2),
                LiteralValue::Integer(3),
            ],
        ));
    }

    #[test]
    fn test_in_with_and() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id IN (1, 2) AND name = 'alice'";
        let resolved = resolve_sql(sql, &tables);
        let constraints = analyze_query_constraints(&resolved);

        assert!(has_in_constraint(
            &constraints,
            "users",
            "id",
            &[LiteralValue::Integer(1), LiteralValue::Integer(2)],
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "name",
            BinaryOp::Equal,
            LiteralValue::String("alice".to_owned()),
        ));
    }

    #[test]
    fn test_in_with_parameter_skipped() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id IN (1, $1)";
        let resolved = resolve_sql(sql, &tables);
        let constraints = analyze_query_constraints(&resolved);

        // Parameter in IN list → entire IN skipped
        assert!(constraints.table_constraints.is_empty());
    }

    #[test]
    fn test_not_in_extraction() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let sql = "SELECT * FROM users WHERE id NOT IN (1, 2, 3)";
        let resolved = resolve_sql(sql, &tables);
        let constraints = analyze_query_constraints(&resolved);

        // NOT IN → individual NotEqual constraints
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::NotEqual,
            LiteralValue::Integer(1),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::NotEqual,
            LiteralValue::Integer(2),
        ));
        assert!(has_constraint(
            &constraints,
            "users",
            "id",
            BinaryOp::NotEqual,
            LiteralValue::Integer(3),
        ));
    }

    #[test]
    fn test_in_propagation_through_join() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("a", 1001));
        tables.insert_overwrite(test_table_metadata("b", 1002));

        let sql = "SELECT * FROM a JOIN b ON a.id = b.id WHERE a.id IN (1, 2)";
        let resolved = resolve_sql(sql, &tables);
        let constraints = analyze_query_constraints(&resolved);

        // Should propagate: a.id IN (1, 2) → b.id IN (1, 2)
        assert!(has_in_constraint(
            &constraints,
            "a",
            "id",
            &[LiteralValue::Integer(1), LiteralValue::Integer(2)],
        ));
        assert!(has_in_constraint(
            &constraints,
            "b",
            "id",
            &[LiteralValue::Integer(1), LiteralValue::Integer(2)],
        ));
    }

    // ========== IN subsumption tests ==========

    #[test]
    fn test_subsumption_in_subset() {
        // IN (1,2,3) subsumed by IN (1,2) — subset
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2)",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_in_point() {
        // IN (1,2,3) subsumed by = 2 — point in set
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            &tables,
        ));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_in_not_subset() {
        // IN (1,2,3) NOT subsumed by IN (1,4) — 4 not in set
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 4)",
            &tables,
        ));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_range_subsumes_in() {
        // id > 0 subsumed by IN (1,2,3) — all values > 0
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 0", &tables));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_in_not_subsumes_range() {
        // IN (1,2,3) NOT subsumed by id > 0 — set is finite, range is infinite
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            &tables,
        ));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id > 0", &tables));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_unconstrained_subsumes_in() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id IN (1, 2, 3)",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    // ========== IN + range intersection in column_range_build ==========

    #[test]
    fn test_in_set_with_range_filter() {
        // IN (1,2,3,4,5) AND id > 3 → InSet({4, 5})
        let tcs = [
            TableConstraint::AnyOf(
                "id".into(),
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Integer(2),
                    LiteralValue::Integer(3),
                    LiteralValue::Integer(4),
                    LiteralValue::Integer(5),
                ],
            ),
            TableConstraint::Comparison(
                "id".into(),
                BinaryOp::GreaterThan,
                LiteralValue::Integer(3),
            ),
        ];
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        let range = column_range_build(&refs);
        match range {
            ColumnRange::InSet(set) => {
                assert_eq!(set.len(), 2);
                assert!(set.contains(&LiteralValue::Integer(4)));
                assert!(set.contains(&LiteralValue::Integer(5)));
            }
            _ => panic!("expected InSet, got {range:?}"),
        }
    }

    #[test]
    fn test_in_set_with_equality_match() {
        // IN (1,2,3) AND id = 2 → Equal(2)
        let tcs = [
            TableConstraint::AnyOf(
                "id".into(),
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Integer(2),
                    LiteralValue::Integer(3),
                ],
            ),
            TableConstraint::Comparison("id".into(), BinaryOp::Equal, LiteralValue::Integer(2)),
        ];
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        let range = column_range_build(&refs);
        assert!(matches!(
            range,
            ColumnRange::Equal(LiteralValue::Integer(2))
        ));
    }

    #[test]
    fn test_in_set_with_equality_mismatch() {
        // IN (1,2,3) AND id = 5 → Empty
        let tcs = [
            TableConstraint::AnyOf(
                "id".into(),
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Integer(2),
                    LiteralValue::Integer(3),
                ],
            ),
            TableConstraint::Comparison("id".into(), BinaryOp::Equal, LiteralValue::Integer(5)),
        ];
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        let range = column_range_build(&refs);
        assert!(matches!(range, ColumnRange::Empty));
    }

    #[test]
    fn test_in_set_with_not_equal() {
        // IN (1,2,3) AND id != 2 → InSet({1, 3})
        let tcs = [
            TableConstraint::AnyOf(
                "id".into(),
                vec![
                    LiteralValue::Integer(1),
                    LiteralValue::Integer(2),
                    LiteralValue::Integer(3),
                ],
            ),
            TableConstraint::Comparison("id".into(), BinaryOp::NotEqual, LiteralValue::Integer(2)),
        ];
        let refs: Vec<&TableConstraint> = tcs.iter().collect();
        let range = column_range_build(&refs);
        match range {
            ColumnRange::InSet(set) => {
                assert_eq!(set.len(), 2);
                assert!(set.contains(&LiteralValue::Integer(1)));
                assert!(set.contains(&LiteralValue::Integer(3)));
            }
            _ => panic!("expected InSet, got {range:?}"),
        }
    }

    // ========== PGC-106: where_analysis_complete tracking ==========

    #[test]
    fn test_no_where_clause_is_complete() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        let resolved = resolve_sql("SELECT * FROM users", &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert!(
            constraints.where_analysis_complete,
            "no WHERE clause is trivially complete (full table scan)"
        );
    }

    #[test]
    fn test_simple_equality_is_complete() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1", &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert!(constraints.where_analysis_complete);
    }

    #[test]
    fn test_in_clause_is_complete() {
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        let resolved = resolve_sql("SELECT * FROM users WHERE id IN (1, 2, 3)", &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert!(constraints.where_analysis_complete);
    }

    #[test]
    fn test_any_eq_clause_extracts_inset() {
        // PGC-106 (option C): `WHERE col = ANY(<array>)` is set membership;
        // extracted as `ColumnConstraint::InSet` so cached ANY-queries can
        // subsume narrower ANY-queries on the same column.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        let resolved = resolve_sql(
            "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
            &tables,
        );

        let constraints = analyze_query_constraints(&resolved);

        assert!(
            constraints.where_analysis_complete,
            "ANY = is now extractable, analysis is complete"
        );
        let users_cs = constraints
            .table_constraints
            .get("users")
            .expect("constraint extracted for users");
        assert_eq!(users_cs.len(), 1);
        assert!(matches!(
            users_cs[0],
            TableConstraint::AnyOf(ref col, ref vs)
                if col == "id" && vs.len() == 3
        ));
    }

    #[test]
    fn test_or_clause_marks_incomplete() {
        // OR is still in the unrecognized-expression bucket; subsumption
        // must continue to fall back to "not subsumed" until/unless the
        // analyzer learns to handle disjunctions.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));
        let resolved = resolve_sql("SELECT * FROM users WHERE id = 1 OR id = 2", &tables);

        let constraints = analyze_query_constraints(&resolved);

        assert!(!constraints.where_analysis_complete);
    }

    #[test]
    fn test_subsumption_any_subsumes_narrower_any() {
        // PGC-106 (option C) headline: cached `ANY([1,2,3])` should
        // subsume new `ANY([1])`.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = ANY(ARRAY[1])",
            &tables,
        ));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_any_does_not_subsume_disjoint_any() {
        // The PGC-106 option-B scenario stays correct under option C:
        // disjoint arrays can't subsume each other.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2])",
            &tables,
        ));
        let new = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = ANY(ARRAY[3, 4, 5])",
            &tables,
        ));

        assert!(!table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_any_subsumes_equality() {
        // Cached `ANY([1,2,3])` should also subsume new `WHERE id = 2`.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql(
            "SELECT * FROM users WHERE id = ANY(ARRAY[1, 2, 3])",
            &tables,
        ));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 2", &tables));

        assert!(table_constraints_subsumed(&new, &cached, "users"));
    }

    #[test]
    fn test_subsumption_full_scan_still_subsumes() {
        // Sanity check that the new gate doesn't regress the legitimate
        // "full-scan subsumes everything" case: cached query has no WHERE
        // clause, so analysis is complete AND `table_constraints` is empty.
        let mut tables = BiHashMap::new();
        tables.insert_overwrite(test_table_metadata("users", 1001));

        let cached = analyze_query_constraints(&resolve_sql("SELECT * FROM users", &tables));
        let new =
            analyze_query_constraints(&resolve_sql("SELECT * FROM users WHERE id = 5", &tables));

        assert!(cached.where_analysis_complete);
        assert!(
            table_constraints_subsumed(&new, &cached, "users"),
            "true full-scan cached query should still subsume"
        );
    }
}
