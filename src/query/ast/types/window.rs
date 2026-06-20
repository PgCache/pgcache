use std::any::Any;
use std::ops::ControlFlow;

use ecow::EcoString;

use crate::query::ast::Deparse;

use super::*;

#[derive(Debug, Clone, PartialEq, Hash)]
pub struct FunctionCall {
    pub name: EcoString,
    pub args: Vec<ScalarExpr>,
    pub agg_star: bool,                     // COUNT(*)
    pub agg_distinct: bool,                 // COUNT(DISTINCT col)
    pub agg_order: Vec<OrderByClause>, // ORDER BY inside aggregate: string_agg(x, ',' ORDER BY x). Vec (not SmallVec): OrderByClause is recursively reachable here, so inline storage would be infinitely sized.
    pub agg_filter: Option<Box<WhereExpr>>, // FILTER (WHERE ...) per-aggregate predicate
    pub over: Option<WindowSpec>,      // Window function OVER clause
}

impl AstNode for FunctionCall {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for arg in &self.args {
            arg.try_for_each_node(f)?;
        }
        for o in &self.agg_order {
            o.try_for_each_node(f)?;
        }
        if let Some(filter) = &self.agg_filter {
            filter.try_for_each_node(f)?;
        }
        if let Some(over) = &self.over {
            over.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl FunctionCall {
    /// Check if this function call contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        self.args.iter().any(|arg| arg.has_subqueries())
            || self.agg_order.iter().any(|o| o.expr.has_subqueries())
            || self.agg_filter.as_ref().is_some_and(|f| f.has_subqueries())
            || self.over.as_ref().is_some_and(|w| w.has_subqueries())
    }
}

impl Deparse for FunctionCall {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(&self.name.to_uppercase());
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
        if let Some(over) = &self.over {
            buf.push_str(" OVER ");
            over.deparse(buf);
        }
        buf
    }
}

/// Window specification for OVER clause: OVER (PARTITION BY ... ORDER BY ...)
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct WindowSpec {
    /// PARTITION BY columns
    pub partition_by: Vec<ScalarExpr>,
    /// ORDER BY clauses. Vec (not SmallVec): recursively reachable, see `agg_order`.
    pub order_by: Vec<OrderByClause>,
    /// Frame clause (ROWS/RANGE/GROUPS BETWEEN ...). `None` is the SQL default
    /// frame (RANGE UNBOUNDED PRECEDING .. CURRENT ROW); dropping a non-default
    /// frame silently changes results (PGC-279), so it must round-trip.
    pub frame: Option<WindowFrame>,
    /// Converter-internal: the name in `OVER w` before it is resolved against
    /// the SELECT's `WINDOW` clause (PGC-280). Always `None` on a WindowSpec
    /// that escapes conversion — `query_expr_convert_raw` forwards any query
    /// with an unresolved reference rather than serving `OVER ()`.
    pub ref_name: Option<EcoString>,
}

impl AstNode for WindowSpec {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        for p in &self.partition_by {
            p.try_for_each_node(f)?;
        }
        for o in &self.order_by {
            o.try_for_each_node(f)?;
        }
        if let Some(frame) = &self.frame {
            frame.try_for_each_node(f)?;
        }
        ControlFlow::Continue(())
    }
}

impl WindowSpec {
    /// Check if this window spec contains sublinks/subqueries
    pub fn has_subqueries(&self) -> bool {
        self.partition_by.iter().any(|p| p.has_subqueries())
            || self.order_by.iter().any(|o| o.expr.has_subqueries())
            || self
                .frame
                .as_ref()
                .is_some_and(|f| f.start.has_subqueries() || f.end.has_subqueries())
    }
}

/// Window frame clause: `{ROWS|RANGE|GROUPS} BETWEEN start AND end [EXCLUDE ...]`.
/// Always deparsed in the explicit BETWEEN form (the single-bound shorthand is
/// semantically identical), so a query that wrote `ROWS UNBOUNDED PRECEDING`
/// round-trips as `ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW`.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct WindowFrame {
    pub mode: FrameMode,
    pub start: FrameBound,
    pub end: FrameBound,
    pub exclusion: FrameExclusion,
}

impl AstNode for WindowFrame {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        self.start.try_for_each_node(f)?;
        self.end.try_for_each_node(f)?;
        ControlFlow::Continue(())
    }
}

impl Deparse for WindowFrame {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.mode.deparse(buf);
        buf.push_str(" BETWEEN ");
        self.start.deparse(buf);
        buf.push_str(" AND ");
        self.end.deparse(buf);
        self.exclusion.deparse(buf);
        buf
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameMode {
    Rows,
    Range,
    Groups,
}

impl Deparse for FrameMode {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        buf.push_str(match self {
            FrameMode::Rows => "ROWS",
            FrameMode::Range => "RANGE",
            FrameMode::Groups => "GROUPS",
        });
        buf
    }
}

/// One end of a window frame.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum FrameBound {
    UnboundedPreceding,
    OffsetPreceding(Box<ScalarExpr>),
    CurrentRow,
    OffsetFollowing(Box<ScalarExpr>),
    UnboundedFollowing,
}

impl FrameBound {
    fn has_subqueries(&self) -> bool {
        match self {
            FrameBound::OffsetPreceding(e) | FrameBound::OffsetFollowing(e) => e.has_subqueries(),
            FrameBound::UnboundedPreceding
            | FrameBound::CurrentRow
            | FrameBound::UnboundedFollowing => false,
        }
    }
}

impl AstNode for FrameBound {
    fn try_for_each_node<'a, N: Any, B>(
        &'a self,
        f: &mut impl FnMut(&'a N) -> ControlFlow<B>,
    ) -> ControlFlow<B> {
        if let Some(r) = (self as &dyn Any).downcast_ref::<N>() {
            f(r)?;
        }
        match self {
            FrameBound::OffsetPreceding(e) | FrameBound::OffsetFollowing(e) => {
                e.try_for_each_node(f)?;
            }
            FrameBound::UnboundedPreceding
            | FrameBound::CurrentRow
            | FrameBound::UnboundedFollowing => {}
        }
        ControlFlow::Continue(())
    }
}

impl Deparse for FrameBound {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            FrameBound::UnboundedPreceding => buf.push_str("UNBOUNDED PRECEDING"),
            FrameBound::CurrentRow => buf.push_str("CURRENT ROW"),
            FrameBound::UnboundedFollowing => buf.push_str("UNBOUNDED FOLLOWING"),
            FrameBound::OffsetPreceding(e) => {
                e.deparse(buf);
                buf.push_str(" PRECEDING");
            }
            FrameBound::OffsetFollowing(e) => {
                e.deparse(buf);
                buf.push_str(" FOLLOWING");
            }
        }
        buf
    }
}

/// Frame `EXCLUDE` option. `NoOthers` is the default and emits nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameExclusion {
    NoOthers,
    CurrentRow,
    Group,
    Ties,
}

impl Deparse for FrameExclusion {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match self {
            FrameExclusion::NoOthers => {}
            FrameExclusion::CurrentRow => buf.push_str(" EXCLUDE CURRENT ROW"),
            FrameExclusion::Group => buf.push_str(" EXCLUDE GROUP"),
            FrameExclusion::Ties => buf.push_str(" EXCLUDE TIES"),
        }
        buf
    }
}

impl Deparse for WindowSpec {
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
        if let Some(frame) = &self.frame {
            if !self.partition_by.is_empty() || !self.order_by.is_empty() {
                buf.push(' ');
            }
            frame.deparse(buf);
        }
        buf.push(')');
        buf
    }
}
