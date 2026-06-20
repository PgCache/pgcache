use crate::cache::UpdateQuerySource;

use super::*;

impl ResolvedWhereExpr {
    /// Recursively collect subquery branches with source tracking.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion for
    /// EXISTS/ANY subqueries. ALL is already Exclusion (NOT IN).
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => {
                scalar.subquery_nodes_collect_with_source(branches);
            }
            ResolvedWhereExpr::Binary(binary) => {
                binary
                    .lexpr
                    .subquery_nodes_collect_with_source(branches, negated);
                binary
                    .rexpr
                    .subquery_nodes_collect_with_source(branches, negated);
            }
            ResolvedWhereExpr::Unary(unary) => {
                let child_negated = if unary.op == UnaryOp::Not {
                    !negated
                } else {
                    negated
                };
                unary
                    .expr
                    .subquery_nodes_collect_with_source(branches, child_negated);
            }
            ResolvedWhereExpr::Multi(multi) => {
                for expr in &multi.exprs {
                    expr.subquery_nodes_collect_with_source(branches, negated);
                }
            }
            ResolvedWhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
                ..
            } => {
                let kind = match sublink_type {
                    SubLinkType::Expr => SubqueryKind::Scalar,
                    SubLinkType::Any | SubLinkType::Exists => {
                        if negated {
                            SubqueryKind::Exclusion
                        } else {
                            SubqueryKind::Inclusion
                        }
                    }
                    SubLinkType::All => {
                        if negated {
                            SubqueryKind::Inclusion
                        } else {
                            SubqueryKind::Exclusion
                        }
                    }
                };
                let source = UpdateQuerySource::Subquery(kind);
                query.select_nodes_collect_with_source(branches, source, negated);
                if let Some(test) = test_expr {
                    test.subquery_nodes_collect_with_source(branches);
                }
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in this WHERE expression.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => scalar.subquery_nodes_collect(branches),
            ResolvedWhereExpr::Binary(binary) => {
                binary.lexpr.subquery_nodes_collect(branches);
                binary.rexpr.subquery_nodes_collect(branches);
            }
            ResolvedWhereExpr::Unary(unary) => {
                unary.expr.subquery_nodes_collect(branches);
            }
            ResolvedWhereExpr::Multi(multi) => {
                for expr in &multi.exprs {
                    expr.subquery_nodes_collect(branches);
                }
            }
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                query.select_nodes_collect(branches);
                if let Some(test) = test_expr {
                    test.subquery_nodes_collect(branches);
                }
            }
        }
    }

    /// Like `subquery_nodes_collect_with_source` but force every collected
    /// subquery to `SubqueryKind::Scalar` regardless of `SubLinkType`. The
    /// truth value feeds an enclosing scalar (aggregate FILTER, CASE WHEN
    /// condition), so shrink-skip rules are unsound (PGC-107).
    fn subquery_nodes_collect_as_scalar<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
    ) {
        let start = branches.len();
        self.subquery_nodes_collect_with_source(branches, false);
        for (_, source) in branches.iter_mut().skip(start) {
            if let UpdateQuerySource::Subquery(kind) = source {
                *kind = SubqueryKind::Scalar;
            }
        }
    }
}

impl ResolvedScalarExpr {
    /// Collect subquery branches from column expressions with source tracking.
    /// All subqueries within column expressions are Scalar.
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
    ) {
        match self {
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Literal(_) => {}
            ResolvedScalarExpr::Function(func) => {
                for arg in &func.args {
                    arg.subquery_nodes_collect_with_source(branches);
                }
                for clause in &func.agg_order {
                    clause.expr.subquery_nodes_collect_with_source(branches);
                }
                if let Some(filter) = &func.agg_filter {
                    filter.subquery_nodes_collect_as_scalar(branches);
                }
                if let Some(over) = &func.over {
                    for col in &over.partition_by {
                        col.subquery_nodes_collect_with_source(branches);
                    }
                    for clause in &over.order_by {
                        clause.expr.subquery_nodes_collect_with_source(branches);
                    }
                }
            }
            ResolvedScalarExpr::Case(case) => {
                if let Some(arg) = &case.arg {
                    arg.subquery_nodes_collect_with_source(branches);
                }
                for when in &case.whens {
                    when.condition.subquery_nodes_collect_as_scalar(branches);
                    when.result.subquery_nodes_collect_with_source(branches);
                }
                if let Some(default) = &case.default {
                    default.subquery_nodes_collect_with_source(branches);
                }
            }
            ResolvedScalarExpr::Arithmetic(arith) => {
                arith.left.subquery_nodes_collect_with_source(branches);
                arith.right.subquery_nodes_collect_with_source(branches);
            }
            ResolvedScalarExpr::Subquery(query, _) => {
                let source = UpdateQuerySource::Subquery(SubqueryKind::Scalar);
                query.select_nodes_collect_with_source(branches, source, false);
            }
            ResolvedScalarExpr::Array(elems) => {
                for elem in elems {
                    elem.subquery_nodes_collect_with_source(branches);
                }
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => {
                expr.subquery_nodes_collect_with_source(branches);
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in this column expression.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match self {
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Literal(_) => {}
            ResolvedScalarExpr::Function(func) => {
                for arg in &func.args {
                    arg.subquery_nodes_collect(branches);
                }
                for clause in &func.agg_order {
                    clause.expr.subquery_nodes_collect(branches);
                }
                if let Some(filter) = &func.agg_filter {
                    filter.subquery_nodes_collect(branches);
                }
                if let Some(over) = &func.over {
                    for col in &over.partition_by {
                        col.subquery_nodes_collect(branches);
                    }
                    for clause in &over.order_by {
                        clause.expr.subquery_nodes_collect(branches);
                    }
                }
            }
            ResolvedScalarExpr::Case(case) => {
                if let Some(arg) = &case.arg {
                    arg.subquery_nodes_collect(branches);
                }
                for when in &case.whens {
                    when.condition.subquery_nodes_collect(branches);
                    when.result.subquery_nodes_collect(branches);
                }
                if let Some(default) = &case.default {
                    default.subquery_nodes_collect(branches);
                }
            }
            ResolvedScalarExpr::Arithmetic(arith) => {
                arith.left.subquery_nodes_collect(branches);
                arith.right.subquery_nodes_collect(branches);
            }
            ResolvedScalarExpr::Subquery(query, _) => {
                query.select_nodes_collect(branches);
            }
            ResolvedScalarExpr::Array(elems) => {
                for elem in elems {
                    elem.subquery_nodes_collect(branches);
                }
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => {
                expr.subquery_nodes_collect(branches);
            }
        }
    }
}

impl ResolvedSelectColumns {
    /// Collect subquery branches from SELECT list with source tracking.
    /// All subqueries in a SELECT list are Scalar (must return single value).
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
    ) {
        if let ResolvedSelectColumns::Columns(columns) = self {
            for col in columns {
                col.expr.subquery_nodes_collect_with_source(branches);
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in the SELECT list.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        if let ResolvedSelectColumns::Columns(columns) = self {
            for col in columns {
                col.expr.subquery_nodes_collect(branches);
            }
        }
    }
}

impl ResolvedTableSource {
    /// Collect subquery branches from table sources with source tracking.
    /// FROM subqueries inherit the negation context as Inclusion/Exclusion.
    fn subquery_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
        negated: bool,
    ) {
        match self {
            ResolvedTableSource::Table(_) => {}
            ResolvedTableSource::Subquery(sub) => {
                let kind = match (sub.subquery_kind, negated) {
                    (SubqueryKind::Scalar, _) => SubqueryKind::Scalar,
                    (SubqueryKind::Inclusion, true) => SubqueryKind::Exclusion,
                    (SubqueryKind::Exclusion, true) => SubqueryKind::Inclusion,
                    (kind, false) => kind,
                };
                sub.query.select_nodes_collect_with_source(
                    branches,
                    UpdateQuerySource::Subquery(kind),
                    negated,
                );
            }
            ResolvedTableSource::Join(join) => {
                join.left
                    .subquery_nodes_collect_with_source(branches, negated);
                join.right
                    .subquery_nodes_collect_with_source(branches, negated);
                if let Some(condition) = join.predicate() {
                    condition.subquery_nodes_collect_with_source(branches, negated);
                }
            }
        }
    }

    /// Recursively collect SELECT branches from subqueries in this table source.
    fn subquery_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match self {
            ResolvedTableSource::Table(_) => {}
            ResolvedTableSource::Subquery(sub) => {
                sub.query.select_nodes_collect(branches);
            }
            ResolvedTableSource::Join(join) => {
                join.left.subquery_nodes_collect(branches);
                join.right.subquery_nodes_collect(branches);
                if let Some(condition) = join.predicate() {
                    condition.subquery_nodes_collect(branches);
                }
            }
        }
    }
}

impl ResolvedQueryExpr {
    /// Extract all SELECT branches with source tracking (FromClause, Subquery, etc.).
    ///
    /// Mirrors `QueryExpr::select_nodes_with_source()` but for the resolved AST.
    /// Top-level branches are FromClause; subquery/CTE branches carry their
    /// Inclusion/Exclusion/Scalar classification.
    pub fn select_nodes_with_source(&self) -> Vec<(&ResolvedSelectNode, UpdateQuerySource)> {
        let mut branches = Vec::new();
        self.select_nodes_collect_with_source(&mut branches, UpdateQuerySource::FromClause, false);
        branches
    }

    /// Collects branches with source tracking.
    /// `outer_source` is the source assigned to this query's body branches.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion.
    fn select_nodes_collect_with_source<'a>(
        &'a self,
        branches: &mut Vec<(&'a ResolvedSelectNode, UpdateQuerySource)>,
        outer_source: UpdateQuerySource,
        negated: bool,
    ) {
        match &self.body {
            ResolvedQueryBody::Select(select) => {
                branches.push((select, outer_source));
                for source in &select.from {
                    source.subquery_nodes_collect_with_source(branches, negated);
                }
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect_with_source(branches, negated);
                }
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect_with_source(branches, negated);
                }
                select.columns.subquery_nodes_collect_with_source(branches);
            }
            ResolvedQueryBody::Values(_) => {}
            ResolvedQueryBody::SetOp(set_op) => {
                set_op
                    .left
                    .select_nodes_collect_with_source(branches, outer_source, negated);
                set_op
                    .right
                    .select_nodes_collect_with_source(branches, outer_source, negated);
            }
        }
    }

    /// Extract all SELECT branches from this query expression.
    ///
    /// For a simple SELECT query, returns a single-element vector.
    /// For set operations (UNION/INTERSECT/EXCEPT), recursively extracts
    /// all SELECT branches from both sides.
    /// VALUES clauses are skipped (they don't reference tables).
    pub fn select_nodes(&self) -> Vec<&ResolvedSelectNode> {
        let mut branches = Vec::new();
        self.select_nodes_collect(&mut branches);
        branches
    }

    /// Helper to recursively collect SELECT branches.
    fn select_nodes_collect<'a>(&'a self, branches: &mut Vec<&'a ResolvedSelectNode>) {
        match &self.body {
            ResolvedQueryBody::Select(select) => {
                branches.push(select);
                // Descend into subqueries in FROM clause
                for source in &select.from {
                    source.subquery_nodes_collect(branches);
                }
                // Descend into subqueries in WHERE clause
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect(branches);
                }
                // Descend into subqueries in HAVING clause
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect(branches);
                }
                // Descend into subqueries in SELECT list
                select.columns.subquery_nodes_collect(branches);
            }
            ResolvedQueryBody::Values(_) => {
                // VALUES clauses don't reference tables, skip
            }
            ResolvedQueryBody::SetOp(set_op) => {
                set_op.left.select_nodes_collect(branches);
                set_op.right.select_nodes_collect(branches);
            }
        }
    }
}
