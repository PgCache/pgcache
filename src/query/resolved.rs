use std::any::Any;
use std::collections::HashSet;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

use crate::cache::{SubqueryKind, UpdateQuerySource};
use crate::catalog::ColumnMetadata;
use crate::query::ast::{
    ArithmeticOp, AstNode, BinaryOp, Deparse, JoinType, LiteralValue, MultiOp, NullOrder,
    OrderDirection, SetOpType, SubLinkType, TableAlias, UnaryOp, ValuesClause,
};
use crate::query::cast::{CastTarget, cast_target_deparse};

error_set! {
    ResolveError := {
        #[display("Table not found: {name}")]
        TableNotFound { name: String },

        #[display("Column '{column}' not found in table '{table}'")]
        ColumnNotFound { table: String, column: String },

        #[display("Ambiguous column reference: '{column}' could refer to multiple tables")]
        AmbiguousColumn { column: String },

        #[display("Schema '{schema}' not found")]
        SchemaNotFound { schema: String },

        #[display("Subquery alias '{alias}' not found")]
        SubqueryAliasNotFound { alias: String },

        #[display("Invalid table reference")]
        InvalidTableRef,

        #[display("Unsupported join qualifier (USING/NATURAL not yet cacheable)")]
        UnsupportedJoinQualifier,
    }
}

/// Result type with location-tracking error reports for resolution operations.
pub type ResolveResult<T> = Result<T, Report<ResolveError>>;

/// Resolved table reference with complete metadata
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedTableNode {
    /// Full schema name (resolved from 'public' default if needed)
    pub schema: EcoString,
    /// Table name
    pub name: EcoString,
    /// Optional alias used in query
    pub alias: Option<EcoString>,
    /// Relation OID from catalog
    pub relation_oid: u32,
}

impl ResolvedTableNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for ResolvedTableNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.schema.deparse(buf);
        buf.push('.');
        self.name.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push(' ');
            alias.deparse(buf);
        }
        buf
    }
}

/// Resolved column reference with type information
///
/// Note: PartialEq and Hash are implemented manually to exclude `table_alias`
/// since aliases are for deparsing only and don't affect column identity.
///
/// String fields use `EcoString`: short identifiers (schema, table, column names)
/// are stored inline; the clone cost is a fixed 16-byte memcpy regardless of
/// string length for inline values.
#[derive(Debug, Clone, Eq)]
pub struct ResolvedColumnNode {
    /// Schema name where the table is located
    pub schema: EcoString,
    /// Table name (not alias) where column is defined
    pub table: EcoString,
    /// Table alias if one was used in the query (for deparsing only, not included in equality)
    pub table_alias: Option<EcoString>,
    /// Column name
    pub column: EcoString,
    /// Column metadata from catalog (includes type info, position, primary key status, etc.)
    pub column_metadata: ColumnMetadata,
}

impl PartialEq for ResolvedColumnNode {
    fn eq(&self, other: &Self) -> bool {
        // Exclude table_alias from equality - it's only for deparsing
        self.schema == other.schema
            && self.table == other.table
            && self.column == other.column
            && self.column_metadata == other.column_metadata
    }
}

impl std::hash::Hash for ResolvedColumnNode {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Exclude table_alias from hash - it's only for deparsing
        self.schema.hash(state);
        self.table.hash(state);
        self.column.hash(state);
        self.column_metadata.hash(state);
    }
}

impl ResolvedColumnNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        (self as &dyn Any).downcast_ref::<N>().into_iter()
    }
}

impl Deparse for ResolvedColumnNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        // Use alias if available, otherwise use schema.table
        if let Some(alias) = &self.table_alias {
            alias.deparse(buf);
        } else {
            self.schema.deparse(buf);
            buf.push('.');
            self.table.deparse(buf);
        }
        buf.push('.');
        self.column.deparse(buf);
        buf
    }
}

/// Resolved unary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedUnaryExpr {
    pub op: UnaryOp,
    pub expr: Box<ResolvedWhereExpr>,
}

impl ResolvedUnaryExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

/// Resolved binary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedBinaryExpr {
    pub op: BinaryOp,
    pub lexpr: Box<ResolvedWhereExpr>,
    pub rexpr: Box<ResolvedWhereExpr>,
}

impl ResolvedBinaryExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.lexpr.nodes().chain(self.rexpr.nodes());
        current.chain(children)
    }
}

/// Resolved multi-operand expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMultiExpr {
    pub op: MultiOp,
    pub exprs: Vec<ResolvedWhereExpr>,
}

impl ResolvedMultiExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.exprs.iter().flat_map(|expr| expr.nodes());
        current.chain(children)
    }
}

/// Resolved WHERE expression with fully qualified references.
/// Scalar leaves are wrapped in `Scalar(ResolvedScalarExpr)`.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedWhereExpr {
    /// Scalar leaf — literal, column, function call, arithmetic, array,
    /// scalar subquery, etc.
    Scalar(ResolvedScalarExpr),
    /// Unary expression
    Unary(ResolvedUnaryExpr),
    /// Binary expression
    Binary(ResolvedBinaryExpr),
    /// Multi-operand expression
    Multi(ResolvedMultiExpr),
    /// Predicate sublink (EXISTS, IN, ANY, ALL).
    /// Scalar subqueries appear via `Scalar(ResolvedScalarExpr::Subquery(...))`.
    Subquery {
        query: Box<ResolvedQueryExpr>,
        sublink_type: SubLinkType,
        /// Left-hand expression for IN/ANY/ALL (e.g., `id` in `id IN (SELECT ...)`)
        test_expr: Option<Box<ResolvedScalarExpr>>,
        /// Columns from the outer query scope referenced inside this subquery.
        /// Empty for non-correlated subqueries.
        outer_refs: Vec<ResolvedColumnNode>,
    },
}

impl ResolvedWhereExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedWhereExpr::Scalar(scalar) => Box::new(scalar.nodes()),
            ResolvedWhereExpr::Unary(unary) => Box::new(unary.nodes()),
            ResolvedWhereExpr::Binary(binary) => Box::new(binary.nodes()),
            ResolvedWhereExpr::Multi(multi) => Box::new(multi.nodes()),
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                let query_nodes = query.nodes();
                let test_nodes = test_expr.iter().flat_map(|e| e.nodes());
                Box::new(query_nodes.chain(test_nodes))
            }
        };
        current.chain(children)
    }

    /// Compute the maximum subquery nesting depth in this WHERE expression.
    /// Returns 0 if there are no subqueries.
    pub fn subquery_depth(&self) -> usize {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => scalar.subquery_depth(),
            ResolvedWhereExpr::Binary(b) => b.lexpr.subquery_depth().max(b.rexpr.subquery_depth()),
            ResolvedWhereExpr::Unary(u) => u.expr.subquery_depth(),
            ResolvedWhereExpr::Multi(m) => m
                .exprs
                .iter()
                .map(|e| e.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedWhereExpr::Subquery {
                query, test_expr, ..
            } => {
                let inner = 1 + query.subquery_depth();
                let test = test_expr.as_ref().map_or(0, |t| t.subquery_depth());
                inner.max(test)
            }
        }
    }

    /// Count the number of leaf predicates (comparisons) in this expression.
    /// AND/OR nodes are not counted themselves, only their leaf children.
    pub fn predicate_count(&self) -> usize {
        match self {
            ResolvedWhereExpr::Binary(b) => match b.op {
                BinaryOp::And | BinaryOp::Or => {
                    b.lexpr.predicate_count() + b.rexpr.predicate_count()
                }
                BinaryOp::Equal
                | BinaryOp::NotEqual
                | BinaryOp::LessThan
                | BinaryOp::LessThanOrEqual
                | BinaryOp::GreaterThan
                | BinaryOp::GreaterThanOrEqual
                | BinaryOp::Like
                | BinaryOp::ILike
                | BinaryOp::NotLike
                | BinaryOp::NotILike => 1,
            },
            ResolvedWhereExpr::Multi(_) => 1, // Multi ops (IN, BETWEEN, etc.) are single predicates
            ResolvedWhereExpr::Unary(u) => u.expr.predicate_count(),
            ResolvedWhereExpr::Scalar(scalar) => match scalar {
                // A bare function/subquery used as predicate counts as one.
                ResolvedScalarExpr::Function(_) | ResolvedScalarExpr::Subquery(_, _) => 1,
                ResolvedScalarExpr::Column(_)
                | ResolvedScalarExpr::Identifier(_)
                | ResolvedScalarExpr::Literal(_)
                | ResolvedScalarExpr::Case(_)
                | ResolvedScalarExpr::Arithmetic(_)
                | ResolvedScalarExpr::Array(_)
                | ResolvedScalarExpr::TypeCast { .. } => 0,
            },
            ResolvedWhereExpr::Subquery { .. } => 1,
        }
    }

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

impl Deparse for ResolvedWhereExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedWhereExpr::Scalar(scalar) => scalar.deparse(buf),
            ResolvedWhereExpr::Unary(unary) => {
                match unary.op {
                    UnaryOp::IsNull
                    | UnaryOp::IsNotNull
                    | UnaryOp::IsTrue
                    | UnaryOp::IsNotTrue
                    | UnaryOp::IsFalse
                    | UnaryOp::IsNotFalse => {
                        // Postfix operators: expr IS NULL, expr IS TRUE, etc.
                        unary.expr.deparse(buf);
                        buf.push(' ');
                        unary.op.deparse(buf);
                    }
                    UnaryOp::Not => {
                        // Prefix operator: NOT expr
                        // NOT has higher precedence than AND/OR, so NOT applied
                        // to a logical binary expression needs parentheses.
                        let needs_parens = matches!(
                            unary.expr.as_ref(),
                            ResolvedWhereExpr::Binary(child) if child.op.is_logical()
                        );
                        unary.op.deparse(buf);
                        buf.push(' ');
                        if needs_parens {
                            buf.push('(');
                        }
                        unary.expr.deparse(buf);
                        if needs_parens {
                            buf.push(')');
                        }
                    }
                }
                buf
            }
            ResolvedWhereExpr::Binary(binary) => {
                let left_needs_parens = matches!(
                    (&binary.op, binary.lexpr.as_ref()),
                    (BinaryOp::And, ResolvedWhereExpr::Binary(child)) if child.op == BinaryOp::Or
                );
                let right_needs_parens = matches!(
                    (&binary.op, binary.rexpr.as_ref()),
                    (BinaryOp::And, ResolvedWhereExpr::Binary(child)) if child.op == BinaryOp::Or
                );

                if left_needs_parens {
                    buf.push('(');
                }
                binary.lexpr.deparse(buf);
                if left_needs_parens {
                    buf.push(')');
                }

                buf.push(' ');
                binary.op.deparse(buf);
                buf.push(' ');

                if right_needs_parens {
                    buf.push('(');
                }
                binary.rexpr.deparse(buf);
                if right_needs_parens {
                    buf.push(')');
                }

                buf
            }
            ResolvedWhereExpr::Multi(multi) => {
                // Format: column IN (value1, value2, ...) or column NOT IN (...)
                let [first, rest @ ..] = multi.exprs.as_slice() else {
                    return buf;
                };

                // First expression is the column/left side
                first.deparse(buf);

                match multi.op {
                    MultiOp::In => buf.push_str(" IN ("),
                    MultiOp::NotIn => buf.push_str(" NOT IN ("),
                    MultiOp::Between
                    | MultiOp::NotBetween
                    | MultiOp::BetweenSymmetric
                    | MultiOp::NotBetweenSymmetric => {
                        buf.push(' ');
                        multi.op.deparse(buf);
                        buf.push(' ');
                        // BETWEEN low AND high — exactly 2 bounds
                        let mut sep = "";
                        for expr in rest {
                            buf.push_str(sep);
                            expr.deparse(buf);
                            sep = " AND ";
                        }
                        return buf;
                    }
                    MultiOp::Any { .. } | MultiOp::All { .. } => {
                        buf.push(' ');
                        multi.op.deparse(buf);
                        buf.push_str(" (");
                    }
                }

                // Remaining expressions are the values
                let mut sep = "";
                for expr in rest {
                    buf.push_str(sep);
                    expr.deparse(buf);
                    sep = ", ";
                }
                buf.push(')');
                buf
            }
            ResolvedWhereExpr::Subquery {
                query,
                sublink_type,
                test_expr,
                ..
            } => {
                match sublink_type {
                    SubLinkType::Exists => {
                        buf.push_str("EXISTS (");
                        query.deparse(buf);
                        buf.push(')');
                    }
                    SubLinkType::Any => {
                        // IN is a special case of ANY
                        if let Some(test) = test_expr {
                            test.deparse(buf);
                            buf.push_str(" IN (");
                            query.deparse(buf);
                            buf.push(')');
                        } else {
                            buf.push('(');
                            query.deparse(buf);
                            buf.push(')');
                        }
                    }
                    SubLinkType::All => {
                        if let Some(test) = test_expr {
                            test.deparse(buf);
                            buf.push_str(" <> ALL (");
                            query.deparse(buf);
                            buf.push(')');
                        } else {
                            buf.push_str("ALL (");
                            query.deparse(buf);
                            buf.push(')');
                        }
                    }
                    SubLinkType::Expr => {
                        // Scalar subquery - just parenthesized query
                        buf.push('(');
                        query.deparse(buf);
                        buf.push(')');
                    }
                }
                buf
            }
        }
    }
}

/// Resolved arithmetic expression: `left op right`
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedArithmeticExpr {
    pub left: Box<ResolvedScalarExpr>,
    pub op: ArithmeticOp,
    pub right: Box<ResolvedScalarExpr>,
}

impl ResolvedArithmeticExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_children = self.left.nodes();
        let right_children = self.right.nodes();
        current.chain(left_children).chain(right_children)
    }
}

impl Deparse for ResolvedArithmeticExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        self.left.deparse(buf);
        buf.push(' ');
        buf.push_str(self.op.as_ref());
        buf.push(' ');
        self.right.deparse(buf);
        buf.push(')');
        buf
    }
}

/// Resolved scalar expression — mirror of AST `ScalarExpr`. Appears in
/// SELECT columns, function args, arithmetic operands, ARRAY elements,
/// CASE arms, TypeCast inner, scalar subqueries, and WHERE leaves.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedScalarExpr {
    Column(ResolvedColumnNode),
    /// Unqualified column name used in set-op ORDER BY, which references
    /// SELECT-list output names rather than source columns.
    Identifier(EcoString),
    Function(ResolvedFunctionCall),
    Literal(LiteralValue),
    Case(ResolvedCaseExpr),
    Arithmetic(ResolvedArithmeticExpr),
    /// Second tuple element is the set of outer-scope columns referenced
    /// inside this subquery; empty for non-correlated subqueries.
    Subquery(Box<ResolvedQueryExpr>, Vec<ResolvedColumnNode>),
    Array(Vec<ResolvedScalarExpr>),
    /// `target` classified by `query::cast::cast_target_from_canonical`.
    TypeCast {
        expr: Box<ResolvedScalarExpr>,
        target: CastTarget,
    },
}

/// Resolved function call — mirrors `query::ast::FunctionCall`.
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedFunctionCall {
    pub name: EcoString,
    pub args: Vec<ResolvedScalarExpr>,
    pub agg_star: bool,
    pub agg_distinct: bool,
    pub agg_order: Vec<ResolvedOrderByClause>,
    pub agg_filter: Option<Box<ResolvedWhereExpr>>,
    pub over: Option<ResolvedWindowSpec>,
}

impl ResolvedFunctionCall {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let arg_nodes = self.args.iter().flat_map(|arg| arg.nodes());
        let agg_order_nodes = self.agg_order.iter().flat_map(|o| o.nodes());
        let filter_nodes = self.agg_filter.iter().flat_map(|f| f.nodes());
        let over_nodes = self.over.iter().flat_map(|w| w.nodes());
        current
            .chain(arg_nodes)
            .chain(agg_order_nodes)
            .chain(filter_nodes)
            .chain(over_nodes)
    }
}

impl Deparse for ResolvedFunctionCall {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(&self.name);
        buf.push('(');
        if self.agg_distinct {
            buf.push_str("DISTINCT ");
        }
        if self.agg_star {
            buf.push('*');
        } else {
            let mut sep = "";
            for arg in &self.args {
                buf.push_str(sep);
                arg.deparse(buf);
                sep = ", ";
            }
        }
        if !self.agg_order.is_empty() {
            buf.push_str(" ORDER BY ");
            let mut sep = "";
            for clause in &self.agg_order {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        if let Some(filter) = &self.agg_filter {
            buf.push_str(" FILTER (WHERE ");
            filter.deparse(buf);
            buf.push(')');
        }
        if let Some(window_spec) = &self.over {
            buf.push_str(" OVER ");
            window_spec.deparse(buf);
        }
        buf
    }
}

/// Resolved window specification for OVER clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedWindowSpec {
    /// PARTITION BY columns
    pub partition_by: Vec<ResolvedScalarExpr>,
    /// ORDER BY clauses
    pub order_by: Vec<ResolvedOrderByClause>,
}

impl ResolvedWindowSpec {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let partition_children = self.partition_by.iter().flat_map(|p| p.nodes());
        let order_children = self.order_by.iter().flat_map(|o| o.nodes());
        current.chain(partition_children).chain(order_children)
    }
}

impl Deparse for ResolvedWindowSpec {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push('(');
        if !self.partition_by.is_empty() {
            buf.push_str("PARTITION BY ");
            let mut sep = "";
            for col in &self.partition_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }
        if !self.order_by.is_empty() {
            if !self.partition_by.is_empty() {
                buf.push(' ');
            }
            buf.push_str("ORDER BY ");
            let mut sep = "";
            for clause in &self.order_by {
                buf.push_str(sep);
                clause.deparse(buf);
                sep = ", ";
            }
        }
        buf.push(')');
        buf
    }
}

/// Resolved CASE expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCaseExpr {
    /// For simple CASE, the expression being tested
    pub arg: Option<Box<ResolvedScalarExpr>>,
    /// List of WHEN clauses
    pub whens: Vec<ResolvedCaseWhen>,
    /// ELSE result
    pub default: Option<Box<ResolvedScalarExpr>>,
}

/// Resolved CASE WHEN clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedCaseWhen {
    /// The condition (for searched CASE) or value (for simple CASE)
    pub condition: ResolvedWhereExpr,
    /// The result if condition is true/matches
    pub result: ResolvedScalarExpr,
}

impl ResolvedScalarExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedScalarExpr::Column(col) => Box::new(col.nodes()),
            ResolvedScalarExpr::Identifier(_) => Box::new(std::iter::empty()),
            ResolvedScalarExpr::Literal(lit) => Box::new(lit.nodes()),
            ResolvedScalarExpr::Function(func) => Box::new(func.nodes()),
            ResolvedScalarExpr::Case(case) => Box::new(case.nodes()),
            ResolvedScalarExpr::Arithmetic(arith) => Box::new(arith.nodes()),
            ResolvedScalarExpr::Subquery(query, _) => Box::new(query.nodes()),
            ResolvedScalarExpr::Array(elems) => Box::new(elems.iter().flat_map(|e| e.nodes())),
            ResolvedScalarExpr::TypeCast { expr, .. } => Box::new(expr.nodes()),
        };
        current.chain(children)
    }

    /// True when the expression tree contains a `Function` call whose name
    /// appears in `agg_fns`. Walks through CASE branches and arithmetic operands,
    /// but does not descend into scalar subqueries (an aggregate nested inside a
    /// subquery doesn't make the outer expression aggregating).
    pub fn has_aggregate(&self, agg_fns: &HashSet<EcoString>) -> bool {
        match self {
            ResolvedScalarExpr::Function(func) => {
                agg_fns.contains(func.name.as_str())
                    || func.args.iter().any(|a| a.has_aggregate(agg_fns))
            }
            ResolvedScalarExpr::Case(case) => {
                case.arg.as_ref().is_some_and(|a| a.has_aggregate(agg_fns))
                    || case.whens.iter().any(|w| w.result.has_aggregate(agg_fns))
                    || case
                        .default
                        .as_ref()
                        .is_some_and(|d| d.has_aggregate(agg_fns))
            }
            ResolvedScalarExpr::Arithmetic(arith) => {
                arith.left.has_aggregate(agg_fns) || arith.right.has_aggregate(agg_fns)
            }
            ResolvedScalarExpr::Array(elems) => elems.iter().any(|e| e.has_aggregate(agg_fns)),
            ResolvedScalarExpr::TypeCast { expr, .. } => expr.has_aggregate(agg_fns),
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Subquery(_, _) => false,
        }
    }

    /// Compute the maximum subquery nesting depth in this column expression.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Identifier(_)
            | ResolvedScalarExpr::Literal(_) => 0,
            ResolvedScalarExpr::Function(func) => func
                .args
                .iter()
                .map(|a| a.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedScalarExpr::Case(case) => {
                let arg_depth = case.arg.as_ref().map_or(0, |a| a.subquery_depth());
                let when_depth = case
                    .whens
                    .iter()
                    .map(|w| w.condition.subquery_depth().max(w.result.subquery_depth()))
                    .max()
                    .unwrap_or(0);
                let default_depth = case.default.as_ref().map_or(0, |d| d.subquery_depth());
                arg_depth.max(when_depth).max(default_depth)
            }
            ResolvedScalarExpr::Arithmetic(arith) => arith
                .left
                .subquery_depth()
                .max(arith.right.subquery_depth()),
            ResolvedScalarExpr::Subquery(query, _) => 1 + query.subquery_depth(),
            ResolvedScalarExpr::Array(elems) => {
                elems.iter().map(|e| e.subquery_depth()).max().unwrap_or(0)
            }
            ResolvedScalarExpr::TypeCast { expr, .. } => expr.subquery_depth(),
        }
    }

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

impl ResolvedCaseExpr {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let arg_nodes = self.arg.iter().flat_map(|a| a.nodes());
        let when_nodes = self.whens.iter().flat_map(|w| w.nodes());
        let default_nodes = self.default.iter().flat_map(|d| d.nodes());
        current
            .chain(arg_nodes)
            .chain(when_nodes)
            .chain(default_nodes)
    }
}

impl ResolvedCaseWhen {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let condition_nodes = self.condition.nodes();
        let result_nodes = self.result.nodes();
        condition_nodes.chain(result_nodes)
    }
}

impl Deparse for ResolvedScalarExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedScalarExpr::Column(col) => col.deparse(buf),
            ResolvedScalarExpr::Identifier(name) => name.deparse(buf),
            ResolvedScalarExpr::Literal(lit) => lit.deparse(buf),
            ResolvedScalarExpr::Function(func) => func.deparse(buf),
            ResolvedScalarExpr::Case(case) => case.deparse(buf),
            ResolvedScalarExpr::Arithmetic(arith) => arith.deparse(buf),
            ResolvedScalarExpr::Subquery(query, _) => {
                buf.push('(');
                query.deparse(buf);
                buf.push(')');
                buf
            }
            ResolvedScalarExpr::Array(elems) => {
                buf.push_str("ARRAY[");
                let mut sep = "";
                for elem in elems {
                    buf.push_str(sep);
                    elem.deparse(buf);
                    sep = ", ";
                }
                buf.push(']');
                buf
            }
            ResolvedScalarExpr::TypeCast { expr, target } => {
                buf.push('(');
                expr.deparse(buf);
                buf.push_str(")::");
                buf.push_str(cast_target_deparse(target));
                buf
            }
        }
    }
}

impl Deparse for ResolvedCaseExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("CASE");
        if let Some(arg) = &self.arg {
            buf.push(' ');
            arg.deparse(buf);
        }
        for when in &self.whens {
            buf.push_str(" WHEN ");
            when.condition.deparse(buf);
            buf.push_str(" THEN ");
            when.result.deparse(buf);
        }
        if let Some(default) = &self.default {
            buf.push_str(" ELSE ");
            default.deparse(buf);
        }
        buf.push_str(" END");
        buf
    }
}

/// Resolved SELECT column with optional alias
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectColumn {
    pub expr: ResolvedScalarExpr,
    pub alias: Option<EcoString>,
}

impl ResolvedSelectColumn {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }

    /// The column's output name — its alias if present, otherwise inferred
    /// from the expression (the column name for `Column` / `Identifier`).
    /// Returns `None` for unaliased function, literal, case, arithmetic, or
    /// subquery expressions, which have no stable output name (PG reports
    /// `?column?`).
    pub fn output_name(&self) -> Option<&EcoString> {
        if let Some(alias) = &self.alias {
            return Some(alias);
        }
        match &self.expr {
            ResolvedScalarExpr::Column(c) => Some(&c.column),
            ResolvedScalarExpr::Identifier(name) => Some(name),
            ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(_, _)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. } => None,
        }
    }
}

impl Deparse for ResolvedSelectColumn {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        self.expr.deparse(buf);
        if let Some(alias) = &self.alias {
            buf.push_str(" AS ");
            buf.push_str(alias);
        }
        buf
    }
}

/// Resolved SELECT columns list
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedSelectColumns {
    /// No columns (empty SELECT)
    None,
    /// Specific columns (stars are expanded to explicit columns during resolution)
    Columns(Vec<ResolvedSelectColumn>),
}

impl ResolvedSelectColumns {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedSelectColumns::None => Box::new(std::iter::empty()),
            ResolvedSelectColumns::Columns(cols) => {
                Box::new(cols.iter().flat_map(|col| col.nodes()))
            }
        };
        current.chain(children)
    }

    /// Compute the maximum subquery nesting depth in the SELECT list.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedSelectColumns::Columns(columns) => columns
                .iter()
                .map(|c| c.expr.subquery_depth())
                .max()
                .unwrap_or(0),
            ResolvedSelectColumns::None => 0,
        }
    }

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

    /// Find the 1-based position of a SELECT column whose expression is
    /// structurally equal to `expr`. Used to emit positional ORDER BY
    /// (`ORDER BY N`) when serving from an MV table — the MV's columns are
    /// named by the original SELECT-list scope, so source-qualified refs
    /// (`public.orders.status`, `count(orders.id)`) aren't valid against the
    /// MV; positional ORDER BY sidesteps the naming entirely.
    pub fn columns_position_of(&self, expr: &ResolvedScalarExpr) -> Option<usize> {
        // ORDER BY against a SELECT-list alias resolves to `Identifier(name)`;
        // match by output name so positional rewrite still works for MV serving.
        match expr {
            ResolvedScalarExpr::Identifier(name) => self.position_by_output_name(name.as_str()),
            ResolvedScalarExpr::Column(_)
            | ResolvedScalarExpr::Function(_)
            | ResolvedScalarExpr::Literal(_)
            | ResolvedScalarExpr::Case(_)
            | ResolvedScalarExpr::Arithmetic(_)
            | ResolvedScalarExpr::Subquery(..)
            | ResolvedScalarExpr::Array(_)
            | ResolvedScalarExpr::TypeCast { .. } => {
                let Self::Columns(cols) = self else {
                    return None;
                };
                cols.iter().position(|c| c.expr == *expr).map(|i| i + 1)
            }
        }
    }

    /// 1-based position of the first SELECT column whose output name (alias or
    /// inferred — see `ResolvedSelectColumn::output_name`) matches `name`.
    pub fn position_by_output_name(&self, name: &str) -> Option<usize> {
        let Self::Columns(cols) = self else {
            return None;
        };
        cols.iter()
            .position(|c| c.output_name().is_some_and(|n| n == name))
            .map(|i| i + 1)
    }
}

impl Deparse for ResolvedSelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedSelectColumns::None => buf.push(' '),
            ResolvedSelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ",";
                }
            }
        }
        buf
    }
}

/// Resolved table source
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedTableSource {
    /// Direct table reference
    Table(ResolvedTableNode),
    /// Resolved subquery
    Subquery(ResolvedTableSubqueryNode),
    /// Resolved join
    Join(Box<ResolvedJoinNode>),
}

impl ResolvedTableSource {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &'_ N>> = match self {
            ResolvedTableSource::Table(table) => Box::new(table.nodes()),
            ResolvedTableSource::Subquery(subquery) => Box::new(subquery.nodes()),
            ResolvedTableSource::Join(join) => Box::new(join.nodes()),
        };
        current.chain(children)
    }

    /// Collect direct table nodes from this source, traversing JOINs but not subqueries.
    fn direct_table_nodes_collect<'a>(&'a self, tables: &mut Vec<&'a ResolvedTableNode>) {
        match self {
            ResolvedTableSource::Table(table) => tables.push(table),
            ResolvedTableSource::Subquery(_) => {} // handled as separate branch
            ResolvedTableSource::Join(join) => {
                join.left.direct_table_nodes_collect(tables);
                join.right.direct_table_nodes_collect(tables);
            }
        }
    }

    /// Compute the maximum subquery nesting depth from this table source.
    fn subquery_depth(&self) -> usize {
        match self {
            ResolvedTableSource::Table(_) => 0,
            ResolvedTableSource::Subquery(sub) => 1 + sub.query.subquery_depth(),
            ResolvedTableSource::Join(join) => {
                let condition_depth = join.predicate().map_or(0, |c| c.subquery_depth());
                join.left
                    .subquery_depth()
                    .max(join.right.subquery_depth())
                    .max(condition_depth)
            }
        }
    }

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

impl Deparse for ResolvedTableSource {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedTableSource::Table(table) => table.deparse(buf),
            ResolvedTableSource::Join(join) => join.deparse(buf),
            ResolvedTableSource::Subquery(subquery) => subquery.deparse(buf),
        }
    }
}

/// Resolved subquery table source
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedTableSubqueryNode {
    pub query: Box<ResolvedQueryExpr>,
    pub alias: TableAlias,
    /// What role this subquery plays for CDC invalidation purposes.
    /// Scalar subqueries always invalidate; Inclusion/Exclusion are flipped
    /// by negation context during traversal.
    pub subquery_kind: SubqueryKind,
}

impl ResolvedTableSubqueryNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.query.nodes();
        Box::new(current.chain(children))
    }
}

impl Deparse for ResolvedTableSubqueryNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(" (");
        self.query.deparse(buf);
        buf.push_str(") ");
        self.alias.deparse(buf);
        buf
    }
}

/// A resolved join's qualifier — mutually exclusive in SQL, so one
/// enum rather than fields that could encode impossible states.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedJoinQual {
    /// `JOIN … ON <expr>`
    On(ResolvedWhereExpr),
    /// `JOIN … USING (c, …)` / `NATURAL JOIN` (resolved to its common
    /// columns). `columns` is emitted verbatim so Postgres performs the
    /// column merge; `predicate` is the equivalent equi-join, used only
    /// for freshness/invalidation analysis, never emitted.
    Using {
        columns: Vec<EcoString>,
        predicate: ResolvedWhereExpr,
    },
    /// Cartesian product — `CROSS JOIN` (or an inner `NATURAL` with no
    /// common columns). Any filtering lives in WHERE.
    Cross,
}

/// Resolved JOIN node
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedJoinNode {
    pub join_type: JoinType,
    pub left: ResolvedTableSource,
    pub right: ResolvedTableSource,
    pub qual: ResolvedJoinQual,
}

impl ResolvedJoinNode {
    /// The join predicate for freshness/invalidation analysis — the
    /// `ON` expression, or the synthesized equi-join for `USING`/
    /// `NATURAL`. `None` for a cartesian (`CROSS`) join.
    pub fn predicate(&self) -> Option<&ResolvedWhereExpr> {
        match &self.qual {
            ResolvedJoinQual::On(e) | ResolvedJoinQual::Using { predicate: e, .. } => Some(e),
            ResolvedJoinQual::Cross => None,
        }
    }

    /// Mutable view of [`Self::predicate`], for in-place rewrites
    /// (e.g. the CDC VALUES-replacement alias update).
    pub fn predicate_mut(&mut self) -> Option<&mut ResolvedWhereExpr> {
        match &mut self.qual {
            ResolvedJoinQual::On(e) | ResolvedJoinQual::Using { predicate: e, .. } => Some(e),
            ResolvedJoinQual::Cross => None,
        }
    }

    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_nodes = self.left.nodes();
        let right_nodes = self.right.nodes();
        let condition_nodes = self.predicate().into_iter().flat_map(|c| c.nodes());
        current
            .chain(left_nodes)
            .chain(right_nodes)
            .chain(condition_nodes)
    }
}

impl Deparse for ResolvedJoinNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);

        // A cartesian join is `CROSS JOIN`, never a bare `JOIN` (a
        // syntax error). `Cross` only ever has an inner join type
        // (an outer join always carries ON/USING/NATURAL).
        if matches!(self.qual, ResolvedJoinQual::Cross) {
            buf.push_str(" CROSS JOIN");
        } else {
            buf.push_str(self.join_type.join_keyword());
        }

        self.right.deparse(buf);

        // `USING (c,…)` is emitted verbatim so Postgres merges the join
        // columns (matching origin's `SELECT *` width / unqualified
        // refs); the equi-`predicate` is internal-only and not emitted.
        match &self.qual {
            ResolvedJoinQual::On(condition) => {
                buf.push_str(" ON ");
                condition.deparse(buf);
            }
            ResolvedJoinQual::Using { columns, .. } => {
                buf.push_str(" USING (");
                for (i, c) in columns.iter().enumerate() {
                    if i > 0 {
                        buf.push_str(", ");
                    }
                    buf.push_str(c);
                }
                buf.push(')');
            }
            ResolvedJoinQual::Cross => {}
        }

        buf
    }
}

/// Resolved ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOrderByClause {
    pub expr: ResolvedScalarExpr,
    pub direction: OrderDirection,
    pub null_order: NullOrder,
}

impl ResolvedOrderByClause {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children = self.expr.nodes();
        current.chain(children)
    }
}

impl Deparse for ResolvedOrderByClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.expr.deparse(buf);
        match self.direction {
            OrderDirection::Asc => buf.push_str(" ASC"),
            OrderDirection::Desc => buf.push_str(" DESC"),
        }
        self.null_order.deparse(buf);
        buf
    }
}

/// Resolved LIMIT clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedLimitClause {
    pub count: Option<LiteralValue>,
    pub offset: Option<LiteralValue>,
}

// ============================================================================
// New Resolved Query Type Hierarchy (parallel to QueryExpr/QueryBody/etc.)
// ============================================================================

/// Resolved core SELECT (without ORDER BY/LIMIT - those go on ResolvedQueryExpr)
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectNode {
    pub distinct: bool,
    pub columns: ResolvedSelectColumns,
    pub from: Vec<ResolvedTableSource>,
    pub where_clause: Option<ResolvedWhereExpr>,
    pub group_by: Vec<ResolvedColumnNode>,
    pub having: Option<ResolvedWhereExpr>,
}

impl Default for ResolvedSelectNode {
    fn default() -> Self {
        Self {
            distinct: false,
            columns: ResolvedSelectColumns::None,
            from: Vec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
        }
    }
}

impl ResolvedSelectNode {
    pub fn nodes<N: Any>(&self) -> impl Iterator<Item = &'_ N> + '_ {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let columns_nodes = self.columns.nodes();
        let from_nodes = self.from.iter().flat_map(|t| t.nodes());
        let where_nodes = self.where_clause.iter().flat_map(|w| w.nodes());
        let group_by_nodes = self.group_by.iter().flat_map(|c| c.nodes());
        let having_nodes = self.having.iter().flat_map(|h| h.nodes());

        current
            .chain(columns_nodes)
            .chain(from_nodes)
            .chain(where_nodes)
            .chain(group_by_nodes)
            .chain(having_nodes)
    }

    /// Returns table nodes directly in the FROM clause, traversing JOINs but not
    /// entering subqueries (FROM-clause derived tables or WHERE-clause subqueries).
    ///
    /// Use this instead of `nodes::<ResolvedTableNode>()` when you only want the
    /// tables that this branch can directly SELECT from. Subquery tables are handled
    /// as separate branches via `select_nodes_collect`.
    pub fn direct_table_nodes(&self) -> Vec<&ResolvedTableNode> {
        let mut tables = Vec::new();
        for source in &self.from {
            source.direct_table_nodes_collect(&mut tables);
        }
        tables
    }

    /// Check if this SELECT references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [ResolvedTableSource::Table(_)])
    }

    /// Compute the maximum subquery nesting depth in this SELECT.
    /// A flat query returns 0, one level of subquery returns 1, etc.
    pub fn subquery_depth(&self) -> usize {
        let from_depth = self
            .from
            .iter()
            .map(|s| s.subquery_depth())
            .max()
            .unwrap_or(0);
        let where_depth = self.where_clause.as_ref().map_or(0, |w| w.subquery_depth());
        let having_depth = self.having.as_ref().map_or(0, |h| h.subquery_depth());
        let columns_depth = self.columns.subquery_depth();
        from_depth
            .max(where_depth)
            .max(having_depth)
            .max(columns_depth)
    }

    /// Compute a complexity score for this query.
    ///
    /// Higher scores indicate more complex queries. Update queries are sorted
    /// by complexity (ascending) so simpler/inner queries are tried first during
    /// CDC processing — this ensures inner subquery tables are populated in
    /// the cache before outer queries that depend on them.
    ///
    /// Components:
    /// - Joins: each join adds 3 (joins require matching across tables)
    /// - Predicates: each WHERE clause comparison adds 1
    /// - Subquery depth: each nesting level adds 5 (outer queries depend on inner)
    pub fn complexity(&self) -> usize {
        let direct_table_count = self.direct_table_nodes().len();
        let join_count = direct_table_count.saturating_sub(1);
        let predicate_count = self
            .where_clause
            .as_ref()
            .map(|w| w.predicate_count())
            .unwrap_or(0);
        let subquery_depth = self.subquery_depth();
        (join_count * 3) + predicate_count + (subquery_depth * 5)
    }
}

impl Deparse for ResolvedSelectNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("SELECT");
        if self.distinct {
            buf.push_str(" DISTINCT");
        }
        self.columns.deparse(buf);

        if !self.from.is_empty() {
            buf.push_str(" FROM");
            let mut sep = "";
            for table in &self.from {
                buf.push_str(sep);
                table.deparse(buf);
                sep = ",";
            }
        }

        if let Some(expr) = &self.where_clause {
            buf.push_str(" WHERE ");
            expr.deparse(buf);
        }

        if !self.group_by.is_empty() {
            buf.push_str(" GROUP BY ");
            let mut sep = "";
            for col in &self.group_by {
                buf.push_str(sep);
                col.deparse(buf);
                sep = ", ";
            }
        }

        if let Some(expr) = &self.having {
            buf.push_str(" HAVING ");
            expr.deparse(buf);
        }

        buf
    }
}

/// Resolved set operation node
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSetOpNode {
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<ResolvedQueryExpr>,
    pub right: Box<ResolvedQueryExpr>,
}

impl ResolvedSetOpNode {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let left_nodes = self.left.nodes();
        let right_nodes = self.right.nodes();
        Box::new(current.chain(left_nodes).chain(right_nodes))
    }
}

impl Deparse for ResolvedSetOpNode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.left.deparse(buf);
        buf.push(' ');
        self.op.deparse(buf);
        if self.all {
            buf.push_str(" ALL");
        }
        buf.push(' ');
        self.right.deparse(buf);
        buf
    }
}

/// The body of a resolved query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedQueryBody {
    Select(Box<ResolvedSelectNode>),
    Values(ValuesClause), // No resolution needed for literals
    SetOp(ResolvedSetOpNode),
}

impl ResolvedQueryBody {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let children: Box<dyn Iterator<Item = &N> + '_> = match self {
            ResolvedQueryBody::Select(select) => Box::new(select.nodes()),
            ResolvedQueryBody::Values(values) => Box::new(values.nodes()),
            ResolvedQueryBody::SetOp(set_op) => set_op.nodes(),
        };
        Box::new(current.chain(children))
    }

    /// SELECT-list columns if this body is a `Select`, else `None`. Set
    /// operations and VALUES bodies have no single SELECT scope.
    pub fn select_columns(&self) -> Option<&ResolvedSelectColumns> {
        match self {
            ResolvedQueryBody::Select(s) => Some(&s.columns),
            ResolvedQueryBody::SetOp(_) | ResolvedQueryBody::Values(_) => None,
        }
    }
}

impl Deparse for ResolvedQueryBody {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            ResolvedQueryBody::Select(select) => select.deparse(buf),
            ResolvedQueryBody::Values(values) => values.deparse(buf),
            ResolvedQueryBody::SetOp(set_op) => set_op.deparse(buf),
        }
    }
}

/// A complete resolved query expression with optional ordering/limiting
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedQueryExpr {
    pub body: ResolvedQueryBody,
    pub order_by: Vec<ResolvedOrderByClause>,
    pub limit: Option<ResolvedLimitClause>,
}

impl Default for ResolvedQueryExpr {
    fn default() -> Self {
        Self {
            body: ResolvedQueryBody::Values(ValuesClause::default()),
            order_by: Vec::new(),
            limit: None,
        }
    }
}

impl ResolvedQueryExpr {
    pub fn nodes<N: Any>(&self) -> Box<dyn Iterator<Item = &'_ N> + '_> {
        let current = (self as &dyn Any).downcast_ref::<N>().into_iter();
        let body_nodes = self.body.nodes();
        let order_by_nodes = self.order_by.iter().flat_map(|o| o.nodes());
        Box::new(current.chain(body_nodes).chain(order_by_nodes))
    }

    /// Check if query only references a single table
    pub fn is_single_table(&self) -> bool {
        self.nodes::<ResolvedTableNode>().nth(1).is_none()
    }

    /// Check if query has a WHERE clause (only applies to SELECT bodies)
    pub fn has_where_clause(&self) -> bool {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.where_clause.is_some(),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => false,
        }
    }

    /// Get the WHERE clause if it exists (only for SELECT bodies)
    pub fn where_clause(&self) -> Option<&ResolvedWhereExpr> {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.where_clause.as_ref(),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
        }
    }

    /// Get the SELECT body if this is a simple SELECT query
    pub fn as_select(&self) -> Option<&ResolvedSelectNode> {
        match &self.body {
            ResolvedQueryBody::Select(select) => Some(select),
            ResolvedQueryBody::Values(_) | ResolvedQueryBody::SetOp(_) => None,
        }
    }

    /// Compute the maximum subquery nesting depth in this query.
    pub fn subquery_depth(&self) -> usize {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.subquery_depth(),
            ResolvedQueryBody::Values(_) => 0,
            ResolvedQueryBody::SetOp(set_op) => set_op
                .left
                .subquery_depth()
                .max(set_op.right.subquery_depth()),
        }
    }

    /// Compute a complexity score for this query.
    pub fn complexity(&self) -> usize {
        match &self.body {
            ResolvedQueryBody::Select(select) => select.complexity(),
            ResolvedQueryBody::Values(_) => 0,
            ResolvedQueryBody::SetOp(set_op) => {
                set_op.left.complexity() + set_op.right.complexity() + 1
            }
        }
    }

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

impl Deparse for ResolvedQueryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.body.deparse(buf);

        if !self.order_by.is_empty() {
            buf.push_str(" ORDER BY");
            let mut sep = "";
            for order in &self.order_by {
                buf.push_str(sep);
                buf.push(' ');
                order.deparse(buf);
                sep = ",";
            }
        }

        if let Some(limit) = &self.limit {
            if let Some(count) = &limit.count {
                buf.push_str(" LIMIT ");
                count.deparse(buf);
            }
            if let Some(offset) = &limit.offset {
                buf.push_str(" OFFSET ");
                offset.deparse(buf);
            }
        }

        buf
    }
}

// Re-export public resolution functions so existing imports continue to work
pub use super::resolve::{query_expr_resolve, select_node_resolve};
