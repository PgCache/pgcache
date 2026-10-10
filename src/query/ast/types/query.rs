use std::any::Any;
use std::ops::ControlFlow;

use ecow::EcoString;
use smallvec::SmallVec;

use super::{
    AstNode, ColumnNode, LiteralValue, ScalarExpr, TableNode, TableSource, WhereExpr,
    children_finalize, children_visit,
};
use crate::cache::UpdateQuerySource;
use crate::query::ast::{
    Clause, Deparse, DeparseContext, DeparseWith, NodeId, NodePositions, clause_deparse_with,
    separated_deparse, separated_deparse_with,
};

// ============================================================================
// New Query Type Hierarchy (for UNION/INTERSECT/EXCEPT support)
// ============================================================================

/// Set operation type for UNION/INTERSECT/EXCEPT
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SetOpType {
    Union,
    Intersect,
    Except,
}

impl Deparse for SetOpType {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SetOpType::Union => buf.push_str("UNION"),
            SetOpType::Intersect => buf.push_str("INTERSECT"),
            SetOpType::Except => buf.push_str("EXCEPT"),
        }
        buf
    }
}

/// VALUES clause with typed rows - represents `VALUES (1, 'a'), (2, 'b')`
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct ValuesClause {
    pub id: NodeId,
    pub rows: Vec<Vec<LiteralValue>>,
}

impl Default for ValuesClause {
    fn default() -> Self {
        Self {
            id: NodeId::PENDING,
            rows: Vec::new(),
        }
    }
}

impl AstNode for ValuesClause {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        for row in &self.rows {
            children_visit(row, f)?;
        }
        ControlFlow::Continue(())
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        for row in &mut self.rows {
            children_finalize(row, positions);
        }
    }
}

impl ValuesClause {
    pub fn has_subqueries(&self) -> bool {
        false // VALUES clauses contain only literals
    }
}

impl DeparseWith for ValuesClause {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        cx.clause_enter(Clause::Values);
        buf.push_str("VALUES ");
        let mut row_sep = "";
        buf.push('(');
        for row in &self.rows {
            buf.push_str(row_sep);
            let mut sep = "";
            for value in row {
                buf.push_str(sep);
                cx.literal_deparse(value, buf);
                sep = ", ";
            }
            row_sep = "), (";
        }
        buf.push(')');
        cx.clause_leave(Clause::Values);
    }
}

/// Core SELECT without ORDER BY/LIMIT (those go on the parent query)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectNode {
    pub id: NodeId,
    pub distinct: bool,
    pub columns: SelectColumns,
    pub from: SmallVec<[TableSource; 1]>,
    pub where_clause: Option<WhereExpr>,
    pub group_by: Vec<ColumnNode>,
    pub having: Option<WhereExpr>,
}

impl Default for SelectNode {
    fn default() -> Self {
        Self {
            id: NodeId::PENDING,
            distinct: false,
            columns: SelectColumns::None,
            from: SmallVec::new(),
            where_clause: None,
            group_by: Vec::new(),
            having: None,
        }
    }
}

impl AstNode for SelectNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.columns.try_for_each_node(f)?;
        children_visit(&self.from, f)?;
        children_visit(self.where_clause.as_ref(), f)?;
        children_visit(&self.group_by, f)?;
        children_visit(self.having.as_ref(), f)
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        self.columns.finalize(positions);
        children_finalize(&mut self.from, positions);
        children_finalize(self.where_clause.as_mut(), positions);
        children_finalize(&mut self.group_by, positions);
        children_finalize(self.having.as_mut(), positions);
    }
}

impl SelectNode {
    /// Return table nodes directly in this SELECT's FROM clause.
    /// Traverses JOINs but does NOT descend into subqueries.
    pub fn direct_table_nodes(&self) -> Vec<&TableNode> {
        let mut tables = Vec::with_capacity(self.from.len());
        for source in &self.from {
            source.direct_table_nodes_collect(&mut tables);
        }
        tables
    }

    /// Check if this SELECT references only a single table
    pub fn is_single_table(&self) -> bool {
        matches!(self.from.as_slice(), [TableSource::Table(_)])
    }

    /// Check if this SELECT contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        // Check columns for subqueries
        if let SelectColumns::Columns(columns) = &self.columns
            && columns.iter().any(SelectColumn::has_subqueries)
        {
            return true;
        }

        // Check FROM clause for subqueries
        if self.from.iter().any(TableSource::has_subqueries) {
            return true;
        }

        // Check WHERE clause for subqueries
        if let Some(where_clause) = &self.where_clause
            && where_clause.has_subqueries()
        {
            return true;
        }

        // Check HAVING clause for subqueries
        if let Some(having) = &self.having
            && having.has_subqueries()
        {
            return true;
        }

        false
    }
}

impl DeparseWith for SelectNode {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        buf.push_str("SELECT");
        if self.distinct {
            buf.push_str(" DISTINCT");
        }
        cx.clause_enter(Clause::SelectList);
        self.columns.deparse_with(cx, buf);
        cx.clause_leave(Clause::SelectList);
        if !self.from.is_empty() {
            cx.clause_enter(Clause::From);
            buf.push_str(" FROM");
            separated_deparse_with(cx, buf, &self.from, ",");
            cx.clause_leave(Clause::From);
        }
        clause_deparse_with(
            cx,
            buf,
            Clause::Where,
            " WHERE ",
            self.where_clause.as_ref(),
        );
        if !self.group_by.is_empty() {
            cx.clause_enter(Clause::GroupBy);
            buf.push_str(" GROUP BY ");
            separated_deparse_with(cx, buf, &self.group_by, ", ");
            cx.clause_leave(Clause::GroupBy);
        }
        clause_deparse_with(cx, buf, Clause::Having, " HAVING ", self.having.as_ref());
    }
}

/// Set operation node: `left UNION/INTERSECT/EXCEPT [ALL] right`
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SetOpNode {
    pub id: NodeId,
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<QueryExpr>,
    pub right: Box<QueryExpr>,
}

impl AstNode for SetOpNode {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        children_visit([&*self.left, &*self.right], f)
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        children_finalize([&mut *self.left, &mut *self.right], positions);
    }
}

impl SetOpNode {
    pub fn has_subqueries(&self) -> bool {
        self.left.has_subqueries() || self.right.has_subqueries()
    }
}

impl DeparseWith for SetOpNode {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        self.left.deparse_with(cx, buf);
        buf.push(' ');
        self.op.deparse(buf);
        if self.all {
            buf.push_str(" ALL");
        }
        buf.push(' ');
        self.right.deparse_with(cx, buf);
    }
}

/// The body of a query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum QueryBody {
    Select(Box<SelectNode>),
    Values(ValuesClause),
    SetOp(SetOpNode),
}

impl AstNode for QueryBody {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            QueryBody::Select(select) => select.try_for_each_node(f)?,
            QueryBody::Values(values) => values.try_for_each_node(f)?,
            QueryBody::SetOp(set_op) => set_op.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        match self {
            QueryBody::Select(select) => select.finalize(positions),
            QueryBody::Values(values) => values.finalize(positions),
            QueryBody::SetOp(set_op) => set_op.finalize(positions),
        }
    }
}

impl QueryBody {
    pub fn has_subqueries(&self) -> bool {
        match self {
            QueryBody::Select(select) => select.has_subqueries(),
            QueryBody::Values(values) => values.has_subqueries(),
            QueryBody::SetOp(set_op) => set_op.has_subqueries(),
        }
    }
}

impl DeparseWith for QueryBody {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        match self {
            QueryBody::Select(select) => select.deparse_with(cx, buf),
            QueryBody::Values(values) => values.deparse_with(cx, buf),
            QueryBody::SetOp(set_op) => set_op.deparse_with(cx, buf),
        }
    }
}

/// A complete query expression with optional ordering/limiting
///
/// This represents a complete query that can be:
/// - A simple SELECT
/// - A VALUES clause
/// - A set operation (UNION/INTERSECT/EXCEPT)
///
/// ORDER BY and LIMIT apply to the entire query expression.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct QueryExpr {
    pub id: NodeId,
    pub ctes: Vec<CteDefinition>,
    pub body: QueryBody,
    pub order_by: SmallVec<[OrderByClause; 1]>,
    pub limit: Option<LimitClause>,
}

impl Default for QueryExpr {
    fn default() -> Self {
        Self {
            id: NodeId::PENDING,
            ctes: Vec::new(),
            body: QueryBody::Values(ValuesClause::default()),
            order_by: SmallVec::new(),
            limit: None,
        }
    }
}

/// Materialization hint for a CTE definition.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum CteMaterialization {
    /// No keyword -- optimizer decides
    Default,
    /// AS MATERIALIZED -- evaluate once, results reused
    Materialized,
    /// AS NOT MATERIALIZED -- optimizer may inline
    NotMaterialized,
}

/// A CTE definition from a WITH clause.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct CteDefinition {
    pub name: EcoString,
    pub query: QueryExpr,
    pub column_aliases: Vec<EcoString>,
    pub materialization: CteMaterialization,
}

impl AstNode for QueryExpr {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        for c in &self.ctes {
            c.query.try_for_each_node(f)?;
        }
        self.body.try_for_each_node(f)?;
        children_visit(&self.order_by, f)
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        for c in &mut self.ctes {
            c.query.finalize(positions);
        }
        self.body.finalize(positions);
        children_finalize(&mut self.order_by, positions);
    }
}

impl QueryExpr {
    /// Check if query only references a single table.
    ///
    /// Short-circuits on the second table via the visitor's early-exit rather
    /// than collecting every `TableNode` into a `Vec` (which `nodes()` would).
    pub fn is_single_table(&self) -> bool {
        let mut count = 0u8;
        self.try_for_each_node::<TableNode, ()>(&mut |_| {
            count += 1;
            if count >= 2 {
                ControlFlow::Break(())
            } else {
                ControlFlow::Continue(())
            }
        })
        .is_continue()
    }

    /// Check if query has a WHERE clause (only applies to SELECT bodies)
    pub fn has_where_clause(&self) -> bool {
        match &self.body {
            QueryBody::Select(select) => select.where_clause.is_some(),
            QueryBody::Values(_) | QueryBody::SetOp(_) => false,
        }
    }

    /// Get the WHERE clause if it exists (only for SELECT bodies)
    pub fn where_clause(&self) -> Option<&WhereExpr> {
        match &self.body {
            QueryBody::Select(select) => select.where_clause.as_ref(),
            QueryBody::Values(_) | QueryBody::SetOp(_) => None,
        }
    }

    /// Check if this query contains subqueries
    pub fn has_subqueries(&self) -> bool {
        !self.ctes.is_empty()
            || self.body.has_subqueries()
            || self.order_by.iter().any(|o| o.expr.has_subqueries())
    }

    /// Get the SELECT body if this is a simple SELECT query
    pub fn as_select(&self) -> Option<&SelectNode> {
        match &self.body {
            QueryBody::Select(select) => Some(select),
            QueryBody::Values(_) | QueryBody::SetOp(_) => None,
        }
    }

    /// Extract all SELECT branches from this query expression.
    ///
    /// For a simple SELECT query, returns a single-element vector.
    /// For set operations (UNION/INTERSECT/EXCEPT), recursively extracts
    /// all SELECT branches from both sides.
    /// VALUES clauses are skipped (they don't reference tables).
    /// CTE definitions are collected via CteRef references, not eagerly.
    pub fn select_nodes(&self) -> Vec<&SelectNode> {
        self.select_nodes_with_source()
            .into_iter()
            .map(|(node, _)| node)
            .collect()
    }

    /// Extract all SELECT branches with their source context (FromClause vs Subquery).
    ///
    /// Tracks whether each branch came from the top-level body (FromClause) or
    /// from a subquery context (Subquery with kind). CTE definitions are not
    /// collected directly -- their branches are collected when referenced via
    /// CteRef, inheriting the reference site's source context.
    pub fn select_nodes_with_source(&self) -> Vec<(&SelectNode, UpdateQuerySource)> {
        let mut branches = Vec::new();
        self.select_nodes_collect(&mut branches, UpdateQuerySource::FromClause, false);
        branches
    }

    /// Collects branches with source tracking.
    /// `outer_source` is the source assigned to this query's body branches.
    /// `negated` tracks NOT-wrapping to flip Inclusion/Exclusion.
    pub(crate) fn select_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
        outer_source: UpdateQuerySource,
        negated: bool,
    ) {
        match &self.body {
            QueryBody::Select(select) => {
                branches.push((select, outer_source));
                for source in &select.from {
                    source.subquery_nodes_collect(branches, negated);
                }
                if let Some(where_clause) = &select.where_clause {
                    where_clause.subquery_nodes_collect(branches, negated);
                }
                if let Some(having) = &select.having {
                    having.subquery_nodes_collect(branches, negated);
                }
                select.columns.subquery_nodes_collect(branches);
            }
            QueryBody::SetOp(set_op) => {
                set_op
                    .left
                    .select_nodes_collect(branches, outer_source, negated);
                set_op
                    .right
                    .select_nodes_collect(branches, outer_source, negated);
            }
            QueryBody::Values(_) => {}
        }
    }
}

impl QueryExpr {
    /// The query without its LIMIT/OFFSET: what identifies it (see
    /// `query_expr_hasher`).
    pub(crate) fn unlimited_deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        if !self.ctes.is_empty() && cx.with_clause_renders() {
            cx.clause_enter(Clause::With);
            buf.push_str("WITH ");
            separated_deparse_with(cx, buf, &self.ctes, ", ");
            buf.push(' ');
            cx.clause_leave(Clause::With);
        }
        self.body.deparse_with(cx, buf);
        if !self.order_by.is_empty() {
            cx.clause_enter(Clause::OrderBy);
            buf.push_str(" ORDER BY ");
            separated_deparse_with(cx, buf, &self.order_by, ", ");
            cx.clause_leave(Clause::OrderBy);
        }
    }
}

impl DeparseWith for QueryExpr {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        self.unlimited_deparse_with(cx, buf);
        if let Some(limit) = &self.limit {
            cx.clause_enter(Clause::Limit);
            limit.deparse_with(cx, buf);
            cx.clause_leave(Clause::Limit);
        }
    }
}

impl DeparseWith for CteDefinition {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        self.name.deparse(buf);
        if !self.column_aliases.is_empty() {
            buf.push('(');
            separated_deparse(buf, &self.column_aliases, ", ");
            buf.push(')');
        }
        buf.push_str(" AS ");
        match self.materialization {
            CteMaterialization::Default => {}
            CteMaterialization::Materialized => buf.push_str("MATERIALIZED "),
            CteMaterialization::NotMaterialized => buf.push_str("NOT MATERIALIZED "),
        }
        buf.push('(');
        self.query.deparse_with(cx, buf);
        buf.push(')');
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumns {
    None,
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ... (includes Star entries)
}

impl AstNode for SelectColumns {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            SelectColumns::None => {}
            SelectColumns::Columns(columns) => {
                children_visit(columns, f)?;
            }
        }
        ControlFlow::Continue(())
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        match self {
            SelectColumns::None => {}
            SelectColumns::Columns(columns) => children_finalize(columns, positions),
        }
    }
}

impl SelectColumns {
    /// Collect subquery branches from SELECT list with source tracking.
    /// All subqueries in a SELECT list are Scalar (must return single value).
    pub(crate) fn subquery_nodes_collect<'a>(
        &'a self,
        branches: &mut Vec<(&'a SelectNode, UpdateQuerySource)>,
    ) {
        if let SelectColumns::Columns(columns) = self {
            for col in columns {
                if let SelectColumn::Expr { expr, .. } = col {
                    expr.subquery_nodes_collect(branches);
                }
            }
        }
    }
}

impl DeparseWith for SelectColumns {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        match self {
            SelectColumns::None => buf.push(' '),
            SelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    let start = buf.len();
                    buf.push_str(sep);
                    col.deparse_with(cx, buf);
                    // A star that expands to nothing leaves no column behind.
                    if buf.len() == start + sep.len() + 1 {
                        buf.truncate(start);
                    } else {
                        sep = ",";
                    }
                }
            }
        };
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumn {
    /// A scalar-valued column, optionally aliased.
    Expr {
        expr: ScalarExpr,
        alias: Option<EcoString>,
    },
    /// `*` or `<qualifier>.*`. Cannot be aliased.
    Star(StarNode),
}

/// `*` or `<qualifier>.*` in a select list. A node of its own so resolution
/// can bind the expansion to it (ADR-057).
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct StarNode {
    pub id: NodeId,
    pub qualifier: Option<EcoString>,
}

impl AstNode for StarNode {
    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }
}

impl StarNode {
    /// `*` or `qualifier.*` as written.
    pub(crate) fn deparse_plain(&self, buf: &mut String) {
        if let Some(table) = &self.qualifier {
            buf.push_str(table);
            buf.push('.');
        }
        buf.push('*');
    }
}

impl SelectColumn {
    /// Inner scalar expression, or `None` for a `Star` column.
    pub fn expr(&self) -> Option<&ScalarExpr> {
        match self {
            SelectColumn::Expr { expr, .. } => Some(expr),
            SelectColumn::Star(_) => None,
        }
    }

    /// Alias of an `Expr` column. `Star` columns cannot be aliased.
    pub fn alias(&self) -> Option<&EcoString> {
        match self {
            SelectColumn::Expr { alias, .. } => alias.as_ref(),
            SelectColumn::Star(_) => None,
        }
    }

    pub fn has_subqueries(&self) -> bool {
        match self {
            SelectColumn::Expr { expr, .. } => expr.has_subqueries(),
            SelectColumn::Star(_) => false,
        }
    }
}

impl AstNode for SelectColumn {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        match self {
            SelectColumn::Expr { expr, .. } => expr.try_for_each_node(f)?,
            SelectColumn::Star(star) => star.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        match self {
            SelectColumn::Expr { expr, .. } => expr.finalize(positions),
            SelectColumn::Star(star) => star.finalize(positions),
        }
    }
}

impl DeparseWith for SelectColumn {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        buf.push(' ');
        match self {
            SelectColumn::Expr { expr, alias } => {
                expr.deparse_with(cx, buf);
                if let Some(alias) = alias {
                    buf.push_str(" AS ");
                    alias.deparse(buf);
                }
            }
            SelectColumn::Star(star) => cx.star_deparse(star, buf),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct OrderByClause {
    pub id: NodeId,
    pub expr: ScalarExpr,
    pub direction: OrderDirection,
    pub null_order: NullOrder,
}

impl AstNode for OrderByClause {
    fn try_for_each_child<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }

    fn id_set(&mut self, id: NodeId) {
        self.id = id;
    }

    fn children_finalize(&mut self, positions: &mut NodePositions) {
        self.expr.finalize(positions);
    }
}

impl DeparseWith for OrderByClause {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        self.expr.deparse_with(cx, buf);
        buf.push(' ');
        self.direction.deparse(buf);
        self.null_order.deparse(buf);
    }
}

/// Explicit NULLS ordering on an `ORDER BY` item. `Default` emits nothing so
/// PostgreSQL applies its built-in ASC=last / DESC=first defaults (PGC-144).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum NullOrder {
    Default,
    NullsFirst,
    NullsLast,
}

impl AstNode for NullOrder {}

impl Deparse for NullOrder {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            NullOrder::Default => {}
            NullOrder::NullsFirst => buf.push_str(" NULLS FIRST"),
            NullOrder::NullsLast => buf.push_str(" NULLS LAST"),
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum OrderDirection {
    Asc,
    Desc,
}

impl AstNode for OrderDirection {}

impl Deparse for OrderDirection {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            OrderDirection::Asc => buf.push_str("ASC"),
            OrderDirection::Desc => buf.push_str("DESC"),
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LimitClause {
    pub count: Option<LiteralValue>,
    pub offset: Option<LiteralValue>,
}

impl DeparseWith for LimitClause {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String) {
        if let Some(count) = &self.count {
            buf.push_str(" LIMIT ");
            cx.literal_deparse(count, buf);
        }
        if let Some(offset) = &self.offset {
            buf.push_str(" OFFSET ");
            cx.literal_deparse(offset, buf);
        }
    }
}
