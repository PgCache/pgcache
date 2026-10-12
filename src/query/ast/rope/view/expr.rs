//! Views on expressions, functions and windows.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use super::{
    Arithmetic, Array, Binary, Case, Column, EMPTY_NAME, Frame, Function, Literal, Multi, NULL,
    Name, OrderBy, Query, Run, Scalar, ScalarSubquery, TypeCast, Unary, When, Where, WhereSubquery,
    Window, mismatch,
};
use crate::query::ast::rope::node::{FrameBoundKind, NodeKind};
use crate::query::ast::{
    ArithmeticOp, BinaryOp, FrameExclusion, FrameMode, LiteralValue, MultiOp, SubLinkType, UnaryOp,
};
use crate::query::cast::CastTarget;

// --- Functions and windows ---------------------------------------------------

#[derive(Default, Clone, Copy)]
struct FunctionLayout<'t> {
    schema: Option<&'t EcoString>,
    name: Option<&'t EcoString>,
    arg_count: u32,
    agg_star: bool,
    agg_distinct: bool,
    order_count: u32,
    has_filter: bool,
    has_over: bool,
}

impl<'t> Function<'t> {
    fn layout(&self) -> FunctionLayout<'t> {
        match self.0.kind() {
            NodeKind::Function {
                schema,
                name,
                arg_count,
                agg_star,
                agg_distinct,
                order_count,
                has_filter,
                has_over,
            } => FunctionLayout {
                schema: schema.as_ref(),
                name: Some(name),
                arg_count: *arg_count,
                agg_star: *agg_star,
                agg_distinct: *agg_distinct,
                order_count: *order_count,
                has_filter: *has_filter,
                has_over: *has_over,
            },
            _ => mismatch("function"),
        }
    }

    pub fn schema(&self) -> Option<&'t EcoString> {
        self.layout().schema
    }

    pub fn name(&self) -> &'t EcoString {
        self.layout().name.unwrap_or(&EMPTY_NAME)
    }

    pub fn args(&self) -> Run<'t, Scalar<'t>> {
        self.0.run(0, self.layout().arg_count)
    }

    pub fn agg_star(&self) -> bool {
        self.layout().agg_star
    }

    pub fn agg_distinct(&self) -> bool {
        self.layout().agg_distinct
    }

    pub fn agg_order(&self) -> Run<'t, OrderBy<'t>> {
        let layout = self.layout();
        self.0.run(layout.arg_count, layout.order_count)
    }

    pub fn filter(&self) -> Option<Where<'t>> {
        let layout = self.layout();
        layout
            .has_filter
            .then(|| self.0.nth(layout.arg_count + layout.order_count))
    }

    pub fn over(&self) -> Option<Window<'t>> {
        let layout = self.layout();
        layout.has_over.then(|| {
            self.0
                .nth(layout.arg_count + layout.order_count + u32::from(layout.has_filter))
        })
    }
}

#[derive(Default, Clone, Copy)]
struct WindowLayout<'t> {
    partition_count: u32,
    order_count: u32,
    has_frame: bool,
    ref_name: Option<&'t EcoString>,
}

impl<'t> Window<'t> {
    fn layout(&self) -> WindowLayout<'t> {
        match self.0.kind() {
            NodeKind::Window {
                partition_count,
                order_count,
                has_frame,
                ref_name,
            } => WindowLayout {
                partition_count: *partition_count,
                order_count: *order_count,
                has_frame: *has_frame,
                ref_name: ref_name.as_ref(),
            },
            _ => mismatch("window"),
        }
    }

    pub fn partition_by(&self) -> Run<'t, Scalar<'t>> {
        self.0.run(0, self.layout().partition_count)
    }

    pub fn order_by(&self) -> Run<'t, OrderBy<'t>> {
        let layout = self.layout();
        self.0.run(layout.partition_count, layout.order_count)
    }

    pub fn frame(&self) -> Option<Frame<'t>> {
        let layout = self.layout();
        layout
            .has_frame
            .then(|| self.0.nth(layout.partition_count + layout.order_count))
    }

    pub fn ref_name(&self) -> Option<&'t EcoString> {
        self.layout().ref_name
    }
}

/// A frame bound with its offset expression, when it has one.
#[derive(Clone, Copy)]
pub enum FrameBound<'t> {
    UnboundedPreceding,
    OffsetPreceding(Scalar<'t>),
    CurrentRow,
    OffsetFollowing(Scalar<'t>),
    UnboundedFollowing,
}

#[derive(Clone, Copy)]
struct FrameLayout {
    mode: FrameMode,
    start: FrameBoundKind,
    end: FrameBoundKind,
    exclusion: FrameExclusion,
}

impl Default for FrameLayout {
    fn default() -> Self {
        Self {
            mode: FrameMode::Rows,
            start: FrameBoundKind::UnboundedPreceding,
            end: FrameBoundKind::CurrentRow,
            exclusion: FrameExclusion::NoOthers,
        }
    }
}

impl<'t> Frame<'t> {
    fn layout(&self) -> FrameLayout {
        match *self.0.kind() {
            NodeKind::Frame {
                mode,
                start,
                end,
                exclusion,
            } => FrameLayout {
                mode,
                start,
                end,
                exclusion,
            },
            _ => mismatch("frame"),
        }
    }

    pub fn mode(&self) -> FrameMode {
        self.layout().mode
    }

    pub fn exclusion(&self) -> FrameExclusion {
        self.layout().exclusion
    }

    fn bound(&self, kind: FrameBoundKind, offset_index: u32) -> FrameBound<'t> {
        match kind {
            FrameBoundKind::UnboundedPreceding => FrameBound::UnboundedPreceding,
            FrameBoundKind::OffsetPreceding => {
                FrameBound::OffsetPreceding(self.0.nth(offset_index))
            }
            FrameBoundKind::CurrentRow => FrameBound::CurrentRow,
            FrameBoundKind::OffsetFollowing => {
                FrameBound::OffsetFollowing(self.0.nth(offset_index))
            }
            FrameBoundKind::UnboundedFollowing => FrameBound::UnboundedFollowing,
        }
    }

    pub fn start(&self) -> FrameBound<'t> {
        self.bound(self.layout().start, 0)
    }

    pub fn end(&self) -> FrameBound<'t> {
        let layout = self.layout();
        self.bound(layout.end, u32::from(layout.start.has_offset()))
    }
}

// --- Expressions -------------------------------------------------------------

/// What a WHERE position holds.
#[derive(Clone, Copy)]
pub enum WhereKind<'t> {
    Scalar(Scalar<'t>),
    Unary(Unary<'t>),
    Binary(Binary<'t>),
    Multi(Multi<'t>),
    Subquery(WhereSubquery<'t>),
}

impl<'t> Where<'t> {
    pub fn kind(&self) -> WhereKind<'t> {
        match self.0.kind() {
            NodeKind::Unary { .. } => WhereKind::Unary(Unary(self.0)),
            NodeKind::Binary { .. } => WhereKind::Binary(Binary(self.0)),
            NodeKind::Multi { .. } => WhereKind::Multi(Multi(self.0)),
            NodeKind::WhereSubquery { .. } => WhereKind::Subquery(WhereSubquery(self.0)),
            _ => WhereKind::Scalar(Scalar(self.0)),
        }
    }
}

/// What a scalar position holds.
#[derive(Clone, Copy)]
pub enum ScalarKind<'t> {
    Column(Column<'t>),
    Function(Function<'t>),
    Literal(Literal<'t>),
    Case(Case<'t>),
    Arithmetic(Arithmetic<'t>),
    Subquery(ScalarSubquery<'t>),
    Array(Array<'t>),
    TypeCast(TypeCast<'t>),
}

impl<'t> Scalar<'t> {
    pub fn kind(&self) -> ScalarKind<'t> {
        match self.0.kind() {
            NodeKind::Column { .. } => ScalarKind::Column(Column(self.0)),
            NodeKind::Function { .. } => ScalarKind::Function(Function(self.0)),
            NodeKind::Case { .. } => ScalarKind::Case(Case(self.0)),
            NodeKind::Arithmetic { .. } => ScalarKind::Arithmetic(Arithmetic(self.0)),
            NodeKind::ScalarSubquery => ScalarKind::Subquery(ScalarSubquery(self.0)),
            NodeKind::Array { .. } => ScalarKind::Array(Array(self.0)),
            NodeKind::TypeCast { .. } => ScalarKind::TypeCast(TypeCast(self.0)),
            NodeKind::Literal(_) => ScalarKind::Literal(Literal(self.0)),
            _ => {
                debug_assert!(false, "a scalar position holds a scalar node");
                ScalarKind::Literal(Literal(self.0))
            }
        }
    }
}

impl<'t> Literal<'t> {
    pub fn value(&self) -> &'t LiteralValue {
        match self.0.kind() {
            NodeKind::Literal(value) => value,
            _ => mismatch::<Option<&LiteralValue>>("literal").unwrap_or(&NULL),
        }
    }
}

impl<'t> Column<'t> {
    fn layout(&self) -> (Option<&'t EcoString>, Option<&'t EcoString>) {
        match self.0.kind() {
            NodeKind::Column { table, column } => (table.as_ref(), Some(column)),
            _ => mismatch("column"),
        }
    }

    pub fn table(&self) -> Option<&'t EcoString> {
        self.layout().0
    }

    pub fn column(&self) -> &'t EcoString {
        self.layout().1.unwrap_or(&EMPTY_NAME)
    }
}

impl<'t> Unary<'t> {
    pub fn op(&self) -> UnaryOp {
        match *self.0.kind() {
            NodeKind::Unary { op } => op,
            _ => mismatch::<Option<UnaryOp>>("unary").unwrap_or(UnaryOp::Not),
        }
    }

    pub fn expr(&self) -> Where<'t> {
        self.0.nth(0)
    }
}

impl<'t> Binary<'t> {
    pub fn op(&self) -> BinaryOp {
        match *self.0.kind() {
            NodeKind::Binary { op } => op,
            _ => mismatch::<Option<BinaryOp>>("binary").unwrap_or(BinaryOp::And),
        }
    }

    pub fn lexpr(&self) -> Where<'t> {
        self.0.nth(0)
    }

    pub fn rexpr(&self) -> Where<'t> {
        self.0.nth(1)
    }
}

impl<'t> Multi<'t> {
    fn layout(&self) -> (MultiOp, u32) {
        match *self.0.kind() {
            NodeKind::Multi { op, count } => (op, count),
            _ => mismatch::<(Option<MultiOp>, u32)>("multi")
                .0
                .map_or((MultiOp::In, 0), |op| (op, 0)),
        }
    }

    pub fn op(&self) -> MultiOp {
        self.layout().0
    }

    /// The operands, the subject first.
    pub fn exprs(&self) -> Run<'t, Where<'t>> {
        self.0.run(0, self.layout().1)
    }
}

impl<'t> WhereSubquery<'t> {
    fn layout(&self) -> (SubLinkType, bool) {
        match *self.0.kind() {
            NodeKind::WhereSubquery {
                sublink_type,
                has_test_expr,
            } => (sublink_type, has_test_expr),
            _ => mismatch::<(Option<SubLinkType>, bool)>("where subquery")
                .0
                .map_or((SubLinkType::Exists, false), |t| (t, false)),
        }
    }

    pub fn sublink_type(&self) -> SubLinkType {
        self.layout().0
    }

    pub fn query(&self) -> Query<'t> {
        self.0.nth(0)
    }

    pub fn test_expr(&self) -> Option<Scalar<'t>> {
        self.layout().1.then(|| self.0.nth(1))
    }
}

impl<'t> Arithmetic<'t> {
    pub fn op(&self) -> ArithmeticOp {
        match *self.0.kind() {
            NodeKind::Arithmetic { op } => op,
            _ => mismatch::<Option<ArithmeticOp>>("arithmetic").unwrap_or(ArithmeticOp::Add),
        }
    }

    pub fn left(&self) -> Scalar<'t> {
        self.0.nth(0)
    }

    pub fn right(&self) -> Scalar<'t> {
        self.0.nth(1)
    }
}

impl<'t> ScalarSubquery<'t> {
    pub fn query(&self) -> Query<'t> {
        self.0.nth(0)
    }
}

impl<'t> Array<'t> {
    pub fn elements(&self) -> Run<'t, Scalar<'t>> {
        let count = match *self.0.kind() {
            NodeKind::Array { count } => count,
            _ => mismatch("array"),
        };
        self.0.run(0, count)
    }
}

impl<'t> TypeCast<'t> {
    pub fn target(&self) -> &'t CastTarget {
        match self.0.kind() {
            NodeKind::TypeCast { target } => target,
            _ => {
                static TEXT: CastTarget = CastTarget::Text;
                mismatch::<Option<&CastTarget>>("type cast").unwrap_or(&TEXT)
            }
        }
    }

    pub fn expr(&self) -> Scalar<'t> {
        self.0.nth(0)
    }
}

#[derive(Default, Clone, Copy)]
struct CaseLayout {
    has_arg: bool,
    when_count: u32,
    has_default: bool,
}

impl<'t> Case<'t> {
    fn layout(&self) -> CaseLayout {
        match *self.0.kind() {
            NodeKind::Case {
                has_arg,
                when_count,
                has_default,
            } => CaseLayout {
                has_arg,
                when_count,
                has_default,
            },
            _ => mismatch("case"),
        }
    }

    pub fn arg(&self) -> Option<Scalar<'t>> {
        self.layout().has_arg.then(|| self.0.nth(0))
    }

    pub fn whens(&self) -> Run<'t, When<'t>> {
        let layout = self.layout();
        self.0.run(u32::from(layout.has_arg), layout.when_count)
    }

    pub fn default(&self) -> Option<Scalar<'t>> {
        let layout = self.layout();
        layout
            .has_default
            .then(|| self.0.nth(u32::from(layout.has_arg) + layout.when_count))
    }
}

impl<'t> When<'t> {
    pub fn condition(&self) -> Where<'t> {
        self.0.nth(0)
    }

    pub fn result(&self) -> Scalar<'t> {
        self.0.nth(1)
    }
}

impl<'t> Name<'t> {
    pub fn text(&self) -> &'t EcoString {
        match self.0.kind() {
            NodeKind::Name(text) => text,
            _ => mismatch::<Option<&EcoString>>("name").unwrap_or(&EMPTY_NAME),
        }
    }
}
