use std::any::Any;
use std::ops::ControlFlow;

use ecow::EcoString;
use smallvec::SmallVec;

use crate::cache::UpdateQuerySource;
use crate::query::ast::Deparse;

use super::*;

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
#[derive(Debug, Clone, PartialEq, Hash, Default)]
pub struct ValuesClause {
    pub rows: Vec<Vec<LiteralValue>>,
}

impl AstNode for ValuesClause {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        for row in &self.rows {
            for v in row {
                v.try_for_each_node(f)?;
            }
        }
        ControlFlow::Continue(())
    }
}

impl ValuesClause {
    pub fn has_subqueries(&self) -> bool {
        false // VALUES clauses contain only literals
    }
}

impl Deparse for ValuesClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str("VALUES ");
        let mut row_sep = "";
        buf.push('(');
        for row in &self.rows {
            buf.push_str(row_sep);
            let mut sep = "";
            for value in row {
                buf.push_str(sep);
                value.deparse(buf);
                sep = ", ";
            }
            row_sep = "), (";
        }
        buf.push(')');
        buf
    }
}

/// Core SELECT without ORDER BY/LIMIT (those go on the parent query)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SelectNode {
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
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        self.columns.try_for_each_node(f)?;
        for t in &self.from {
            t.try_for_each_node(f)?;
        }
        if let Some(w) = &self.where_clause {
            w.try_for_each_node(f)?;
        }
        for c in &self.group_by {
            c.try_for_each_node(f)?;
        }
        if let Some(h) = &self.having {
            h.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
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

impl Deparse for SelectNode {
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

/// Set operation node: `left UNION/INTERSECT/EXCEPT [ALL] right`
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct SetOpNode {
    pub op: SetOpType,
    pub all: bool,
    pub left: Box<QueryExpr>,
    pub right: Box<QueryExpr>,
}

impl AstNode for SetOpNode {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.left.try_for_each_node(f)?;
        self.right.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl SetOpNode {
    pub fn has_subqueries(&self) -> bool {
        self.left.has_subqueries() || self.right.has_subqueries()
    }
}

impl Deparse for SetOpNode {
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

/// The body of a query - SELECT, VALUES, or set operation
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum QueryBody {
    Select(Box<SelectNode>),
    Values(ValuesClause),
    SetOp(SetOpNode),
}

impl AstNode for QueryBody {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            QueryBody::Select(select) => select.try_for_each_node(f)?,
            QueryBody::Values(values) => values.try_for_each_node(f)?,
            QueryBody::SetOp(set_op) => set_op.try_for_each_node(f)?,
        }
        ControlFlow::Continue(())
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

impl Deparse for QueryBody {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            QueryBody::Select(select) => select.deparse(buf),
            QueryBody::Values(values) => values.deparse(buf),
            QueryBody::SetOp(set_op) => set_op.deparse(buf),
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
    pub ctes: Vec<CteDefinition>,
    pub body: QueryBody,
    pub order_by: SmallVec<[OrderByClause; 1]>,
    pub limit: Option<LimitClause>,
}

impl Default for QueryExpr {
    fn default() -> Self {
        Self {
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
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for c in &self.ctes {
            c.query.try_for_each_node(f)?;
        }
        self.body.try_for_each_node(f)?;
        for o in &self.order_by {
            o.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
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

impl Deparse for QueryExpr {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if !self.ctes.is_empty() {
            buf.push_str("WITH ");
            for (i, cte) in self.ctes.iter().enumerate() {
                if i > 0 {
                    buf.push_str(", ");
                }
                cte.name.deparse(buf);
                if !cte.column_aliases.is_empty() {
                    buf.push('(');
                    for (j, col) in cte.column_aliases.iter().enumerate() {
                        if j > 0 {
                            buf.push_str(", ");
                        }
                        col.deparse(buf);
                    }
                    buf.push(')');
                }
                buf.push_str(" AS ");
                match cte.materialization {
                    CteMaterialization::Default => {}
                    CteMaterialization::Materialized => buf.push_str("MATERIALIZED "),
                    CteMaterialization::NotMaterialized => buf.push_str("NOT MATERIALIZED "),
                }
                buf.push('(');
                cte.query.deparse(buf);
                buf.push(')');
            }
            buf.push(' ');
        }

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

#[derive(Debug, Clone, PartialEq, Hash)]
pub enum SelectColumns {
    None,
    Columns(Vec<SelectColumn>), // SELECT col1, col2, ... (includes Star entries)
}

impl AstNode for SelectColumns {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            SelectColumns::None => {}
            SelectColumns::Columns(columns) => {
                for col in columns {
                    col.try_for_each_node(f)?;
                }
            }
        }
        ControlFlow::Continue(())
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

impl Deparse for SelectColumns {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            SelectColumns::None => buf.push(' '),
            SelectColumns::Columns(cols) => {
                let mut sep = "";
                for col in cols {
                    buf.push_str(sep);
                    col.deparse(buf);
                    sep = ",";
                }
            }
        };

        buf
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
    Star(Option<EcoString>),
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
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            SelectColumn::Expr { expr, .. } => expr.try_for_each_node(f)?,
            SelectColumn::Star(_) => {}
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for SelectColumn {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push(' ');
        match self {
            SelectColumn::Expr { expr, alias } => {
                expr.deparse(buf);
                if let Some(alias) = alias {
                    buf.push_str(" AS ");
                    alias.deparse(buf);
                }
            }
            SelectColumn::Star(qualifier) => {
                if let Some(table) = qualifier {
                    buf.push_str(table);
                    buf.push('.');
                }
                buf.push('*');
            }
        }
        buf
    }
}

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct OrderByClause {
    pub expr: ScalarExpr,
    pub direction: OrderDirection,
    pub null_order: NullOrder,
}

impl AstNode for OrderByClause {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.expr.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for OrderByClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.expr.deparse(buf);
        buf.push(' ');
        self.direction.deparse(buf);
        self.null_order.deparse(buf);
        buf
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

impl AstNode for NullOrder {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        ControlFlow::Continue(())
    }
}

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

impl AstNode for OrderDirection {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        ControlFlow::Continue(())
    }
}

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

impl Deparse for LimitClause {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        if let Some(count) = &self.count {
            buf.push_str(" LIMIT ");
            count.deparse(buf);
        }
        if let Some(offset) = &self.offset {
            buf.push_str(" OFFSET ");
            offset.deparse(buf);
        }
        buf
    }
}
