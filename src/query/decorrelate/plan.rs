//! What the rewrite decided for one query, as a plan the emitter writes in
//! pre-order: pieces of the base tree kept whole, pieces carried into the
//! rewritten SELECT with their column references re-qualified, and the
//! joins, derived tables and references the rewrite makes.

use smallvec::SmallVec;

use ecow::EcoString;

use crate::query::ast::rope::{NodeRef, Query, Replacement, Select};
use crate::query::ast::{JoinType, NodeId};
use crate::query::resolve::ColumnBinding;

/// A reference to column `position` of the `derived`th derived table the
/// rewrite makes, spelled `table.column`.
#[derive(Debug, Clone)]
pub(super) struct DerivedRef {
    pub(super) derived: usize,
    pub(super) position: usize,
    pub(super) table: EcoString,
    pub(super) column: EcoString,
}

/// What a fresh node binds to once placed.
#[derive(Debug, Clone)]
pub(super) enum Bind {
    Column(ColumnBinding),
    Derived {
        derived: usize,
        position: usize,
    },
    /// A merged column written out: the left column at the placed
    /// position plus one, the right at plus two when present.
    Merged {
        left: ColumnBinding,
        right: Option<ColumnBinding>,
    },
}

/// A plan's lists: a rewrite moves a few things, so they live inline.
pub(super) type Planned<T> = SmallVec<[T; 4]>;

/// A replacement inside a moved subtree, with what it binds to.
#[derive(Debug, Clone)]
pub(super) struct PlannedReplacement {
    pub(super) at: NodeId,
    pub(super) replacement: Replacement,
    pub(super) bind: Option<Bind>,
}

/// A base subtree carried into the rewritten SELECT: a copy with its column
/// references re-qualified by their slot, and whatever else the rewrite
/// replaced inside it.
pub(super) struct Moved<'t> {
    pub(super) root: NodeRef<'t>,
    pub(super) replacements: Vec<PlannedReplacement>,
}

pub(super) enum ScalarPlan<'t> {
    Moved(Moved<'t>),
    Derived(DerivedRef),
}

pub(super) enum WherePlan<'t> {
    Keep(NodeId),
    Moved(Moved<'t>),
    Equal(ScalarPlan<'t>, ScalarPlan<'t>),
    IsNull(ScalarPlan<'t>),
    And(Box<WherePlan<'t>>, Box<WherePlan<'t>>),
}

impl<'t> WherePlan<'t> {
    /// `items` AND-ed left to right.
    pub(super) fn and_fold(items: Vec<WherePlan<'t>>) -> Option<WherePlan<'t>> {
        items
            .into_iter()
            .reduce(|left, right| WherePlan::And(Box::new(left), Box::new(right)))
    }
}

/// A FROM entry of the rewritten SELECT.
pub(super) enum Piece<'t> {
    Keep(NodeId),
    Moved(Moved<'t>),
    /// `on` is `None` for a cross join.
    Join {
        join_type: JoinType,
        left: Box<Piece<'t>>,
        right: Box<Piece<'t>>,
        on: Option<WherePlan<'t>>,
    },
    Derived(DerivedPlan),
}

impl<'t> Piece<'t> {
    /// `pieces` as one source: a chain of cross joins, or the one piece.
    pub(super) fn cross_fold(pieces: Vec<Piece<'t>>) -> Option<Piece<'t>> {
        pieces.into_iter().reduce(|left, right| Piece::Join {
            join_type: JoinType::Inner,
            left: Box::new(left),
            right: Box::new(right),
            on: None,
        })
    }
}

/// The `_dcN` derived table of a scalar subquery: the correlation key
/// columns and the scalar as `_dsN`, over the inner FROM and residual WHERE,
/// grouped by the keys when the scalar aggregates.
pub(super) struct DerivedPlan {
    pub(super) index: usize,
    pub(super) alias: EcoString,
    pub(super) keys: Planned<NodeId>,
    pub(super) scalar: NodeId,
    pub(super) scalar_alias: EcoString,
    pub(super) from: Planned<NodeId>,
    pub(super) residual: Planned<NodeId>,
    pub(super) grouped: bool,
}

/// One select-list entry of the rewritten SELECT.
pub(super) enum ItemPlan<'t> {
    Moved(Moved<'t>),
    /// A scalar subquery replaced by its derived table's column.
    Derived {
        alias: Option<EcoString>,
        reference: DerivedRef,
    },
    /// A column a star stood for.
    Column {
        replacement: Replacement,
        bind: Bind,
    },
    /// A merged column a star stood for, written out under its name.
    Merged {
        alias: EcoString,
        replacement: Replacement,
        bind: Bind,
    },
}

pub(super) enum GroupPlan<'t> {
    Keep(NodeId),
    Moved(Moved<'t>),
}

pub(super) struct SelectPlan<'t> {
    pub(super) select: Select<'t>,
    pub(super) distinct: bool,
    /// `None` when the SELECT has no select list at all.
    pub(super) items: Option<Vec<ItemPlan<'t>>>,
    pub(super) from: Vec<Piece<'t>>,
    pub(super) where_items: Vec<WherePlan<'t>>,
    pub(super) group_by: Vec<GroupPlan<'t>>,
    pub(super) having: Option<Moved<'t>>,
}

pub(super) enum BodyPlan<'t> {
    Select(SelectPlan<'t>),
    /// A branch is `None` when it is kept as it was.
    SetOp(Option<Box<QueryPlan<'t>>>, Option<Box<QueryPlan<'t>>>),
}

pub(super) struct QueryPlan<'t> {
    pub(super) query: Query<'t>,
    pub(super) body: BodyPlan<'t>,
    /// The ORDER BY clauses moved into the rewritten SELECT's scope;
    /// `None` keeps them as they were.
    pub(super) order_by: Option<Vec<Moved<'t>>>,
}
