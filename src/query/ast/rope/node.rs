//! The nodes of a rope tree: one slice element each, in pre-order.

#![allow(clippy::wildcard_enum_match_arm)]

use ecow::EcoString;

use crate::cache::SubqueryKind;
use crate::query::ast::{
    ArithmeticOp, BinaryOp, CteMaterialization, FrameExclusion, FrameMode, JoinType, LiteralValue,
    MultiOp, NullOrder, OrderDirection, SetOpType, SubLinkType, UnaryOp,
};
use crate::query::cast::CastTarget;

/// One node in pre-order. `len` counts the subtree, itself included, so the
/// first child is at self + 1 and the next sibling at self + len. Nothing
/// in a node is an absolute position, which is what lets a subtree sit at
/// any offset in any tree.
#[derive(Debug, Clone, PartialEq, Hash)]
pub struct Node {
    pub len: u32,
    pub kind: NodeKind,
}

/// What a node is, with the counts and flags that say which children follow
/// it. Children come in the order the variant's doc lists; a count is that
/// many subtrees, a flag is one subtree when set.
#[derive(Debug, Clone, PartialEq, Hash)]
pub enum NodeKind {
    /// Children: `cte_count` × `Cte`, the body, `order_by_count` ×
    /// `OrderBy`, then the LIMIT count and offset literals when flagged.
    Query {
        cte_count: u32,
        order_by_count: u32,
        has_limit_count: bool,
        has_limit_offset: bool,
    },
    /// Children: `alias_count` × `Name`, then the query.
    Cte {
        name: EcoString,
        alias_count: u32,
        materialization: CteMaterialization,
    },
    /// Children: the select items when `item_count` is set, `from_count`
    /// sources, WHERE, `group_by_count` × `Column`, HAVING.
    Select {
        distinct: bool,
        item_count: Option<u32>,
        from_count: u32,
        has_where: bool,
        group_by_count: u32,
        has_having: bool,
    },
    /// Children: `row_count` × `ValuesRow`.
    Values {
        row_count: u32,
    },
    /// Children: `width` literals.
    ValuesRow {
        width: u32,
    },
    /// Children: the left query, the right query.
    SetOp {
        op: SetOpType,
        all: bool,
    },
    /// Children: the expression.
    OrderBy {
        direction: OrderDirection,
        null_order: NullOrder,
    },
    /// Children: the expression.
    SelectExpr {
        alias: Option<EcoString>,
    },
    Star {
        qualifier: Option<EcoString>,
    },
    /// Children: `alias_column_count` × `Name`; only with an alias. An
    /// empty `schema` or `alias` is none: an identifier is never empty, and
    /// the two `Option`s would put the node past its size budget.
    Table {
        schema: EcoString,
        name: EcoString,
        alias: EcoString,
        alias_column_count: u32,
    },
    /// Children: `alias_column_count` × `Name`, then the query.
    Subquery {
        lateral: bool,
        alias: Option<EcoString>,
        alias_column_count: u32,
        kind: SubqueryKind,
    },
    /// Children: `column_alias_count` × `Name` (the CTE's own),
    /// `alias_column_count` × `Name` (the reference's), then the query.
    CteRef {
        cte_name: EcoString,
        column_alias_count: u32,
        materialization: CteMaterialization,
        alias: Option<EcoString>,
        alias_column_count: u32,
    },
    /// Children: the left source, the right source, then `Using(n)` ×
    /// `Name` or the `On` condition.
    Join {
        join_type: JoinType,
        qual: JoinQualKind,
    },
    /// Children: `arg_count` expressions, `order_count` × `OrderBy`, the
    /// FILTER condition, the OVER window.
    Function {
        schema: Option<EcoString>,
        name: EcoString,
        arg_count: u32,
        agg_star: bool,
        agg_distinct: bool,
        order_count: u32,
        has_filter: bool,
        has_over: bool,
    },
    /// Children: `partition_count` expressions, `order_count` × `OrderBy`,
    /// the frame.
    Window {
        partition_count: u32,
        order_count: u32,
        has_frame: bool,
        ref_name: Option<EcoString>,
    },
    /// Children: the start offset, then the end offset, each when its
    /// bound has one.
    Frame {
        mode: FrameMode,
        start: FrameBoundKind,
        end: FrameBoundKind,
        exclusion: FrameExclusion,
    },
    Literal(LiteralValue),
    Column {
        table: Option<EcoString>,
        column: EcoString,
    },
    /// Children: the operand.
    Unary {
        op: UnaryOp,
    },
    /// Children: the left operand, the right operand.
    Binary {
        op: BinaryOp,
    },
    /// Children: `count` operands, the subject first.
    Multi {
        op: MultiOp,
        count: u32,
    },
    /// Children: the query, then the test expression when flagged.
    WhereSubquery {
        sublink_type: SubLinkType,
        has_test_expr: bool,
    },
    /// Children: the left operand, the right operand.
    Arithmetic {
        op: ArithmeticOp,
    },
    /// Children: the query.
    ScalarSubquery,
    /// Children: `count` elements.
    Array {
        count: u32,
    },
    /// Children: the expression.
    TypeCast {
        target: CastTarget,
    },
    /// Children: the argument when flagged, `when_count` × `When`, the
    /// default when flagged.
    Case {
        has_arg: bool,
        when_count: u32,
        has_default: bool,
    },
    /// Children: the condition, the result.
    When,
    /// One entry of a name list: an alias column, a USING column.
    Name(EcoString),
}

/// A join's qualification, with the USING column count; the columns are
/// `Name` children and an ON condition is a child.
#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub enum JoinQualKind {
    On,
    Using(u32),
    Natural,
    Cross,
}

/// A frame bound without its offset, which is a child when the bound has
/// one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FrameBoundKind {
    UnboundedPreceding,
    OffsetPreceding,
    CurrentRow,
    OffsetFollowing,
    UnboundedFollowing,
}

impl FrameBoundKind {
    pub fn has_offset(self) -> bool {
        matches!(
            self,
            FrameBoundKind::OffsetPreceding | FrameBoundKind::OffsetFollowing
        )
    }
}

impl NodeKind {
    /// How many LIMIT literals end a query node's subtree.
    pub(super) fn limit_count(&self) -> u32 {
        match self {
            NodeKind::Query {
                has_limit_count,
                has_limit_offset,
                ..
            } => u32::from(*has_limit_count) + u32::from(*has_limit_offset),
            _ => 0,
        }
    }
}
