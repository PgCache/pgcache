use crate::catalog::Oid;

use ecow::EcoString;
use error_set::error_set;
use rootcause::Report;

use crate::cache::SubqueryKind;
use crate::catalog::ColumnMetadata;
use crate::query::ast::{
    ArithmeticOp, BinaryOp, FrameExclusion, FrameMode, JoinType, LiteralValue, MultiOp, NullOrder,
    OrderDirection, SetOpType, SubLinkType, TableAlias, UnaryOp, ValuesClause,
};
use crate::query::cast::CastTarget;

mod analysis;
mod deparse;
mod subquery_collect;
mod traverse;

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
    pub relation_oid: Oid,
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

/// Resolved unary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedUnaryExpr {
    pub op: UnaryOp,
    pub expr: Box<ResolvedWhereExpr>,
}

/// Resolved binary expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedBinaryExpr {
    pub op: BinaryOp,
    pub lexpr: Box<ResolvedWhereExpr>,
    pub rexpr: Box<ResolvedWhereExpr>,
}

/// Resolved multi-operand expression
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedMultiExpr {
    pub op: MultiOp,
    pub exprs: Vec<ResolvedWhereExpr>,
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

/// Resolved arithmetic expression: `left op right`
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedArithmeticExpr {
    pub left: Box<ResolvedScalarExpr>,
    pub op: ArithmeticOp,
    pub right: Box<ResolvedScalarExpr>,
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

/// Resolved window specification for OVER clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedWindowSpec {
    /// PARTITION BY columns
    pub partition_by: Vec<ResolvedScalarExpr>,
    /// ORDER BY clauses
    pub order_by: Vec<ResolvedOrderByClause>,
    /// Frame clause; `None` is the SQL default frame (see `ast::WindowSpec`).
    pub frame: Option<ResolvedWindowFrame>,
}

/// Resolved window frame — mirrors `ast::WindowFrame`. `mode`/`exclusion` are
/// the shared AST enums (no expressions to resolve).
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedWindowFrame {
    pub mode: FrameMode,
    pub start: ResolvedFrameBound,
    pub end: ResolvedFrameBound,
    pub exclusion: FrameExclusion,
}

/// Resolved frame bound — mirrors `ast::FrameBound`.
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedFrameBound {
    UnboundedPreceding,
    OffsetPreceding(Box<ResolvedScalarExpr>),
    CurrentRow,
    OffsetFollowing(Box<ResolvedScalarExpr>),
    UnboundedFollowing,
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

/// Resolved SELECT column with optional alias
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSelectColumn {
    pub expr: ResolvedScalarExpr,
    pub alias: Option<EcoString>,
}

/// Resolved SELECT columns list
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedSelectColumns {
    /// No columns (empty SELECT)
    None,
    /// Specific columns (stars are expanded to explicit columns during resolution)
    Columns(Vec<ResolvedSelectColumn>),
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

/// Resolved ORDER BY clause
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedOrderByClause {
    pub expr: ResolvedScalarExpr,
    pub direction: OrderDirection,
    pub null_order: NullOrder,
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

/// Resolved set operation node
#[derive(Debug, Clone, PartialEq)]
pub struct ResolvedSetOpNode {
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<ResolvedQueryExpr>,
    pub right: Box<ResolvedQueryExpr>,
}

/// The body of a resolved query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq)]
pub enum ResolvedQueryBody {
    Select(Box<ResolvedSelectNode>),
    Values(ValuesClause), // No resolution needed for literals
    SetOp(ResolvedSetOpNode),
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

// Re-export public resolution functions so existing imports continue to work
pub use super::resolve::{query_expr_resolve, select_node_resolve};
