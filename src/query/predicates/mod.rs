//! What a SELECT branch's predicates say about each of its table
//! references, keyed by slot so names and aliases never enter it.
//!
//! * [`conjunct`] — splits a predicate into AND-ed conjuncts and recognizes
//!   each one's form.
//! * [`branch`] — walks one branch's FROM tree and predicates into a
//!   [`QueryBranchPredicates`].
//! * [`derive`] — the per-reference facts read off that model: necessary
//!   condition, exact description, dependency footprint, outer-join role.

use ecow::EcoString;

use crate::catalog::ColumnPosition;
use crate::query::ast::NodeId;
use crate::query::constraints::{Constraint, TableConstraint, TableRanges};
use crate::query::resolve::{ColumnBinding, SlotId};

mod branch;
mod conjunct;
mod derive;
#[cfg(test)]
mod tests;

pub use branch::query_branch_predicates_build;

/// Index of a join in [`QueryBranchPredicates::joins`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct JoinId(u32);

impl JoinId {
    pub fn index(self) -> usize {
        self.0 as usize
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinSide {
    Left,
    Right,
}

/// One join on the way from the FROM root down to a reference.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct JoinPathStep {
    pub join: JoinId,
    pub side: JoinSide,
}

/// A FROM entry: base table, derived table or CTE reference.
#[derive(Debug, Clone)]
pub struct TableReference {
    pub slot: SlotId,
    /// The joins from the FROM root down to it, outermost first.
    pub path: Vec<JoinPathStep>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinKind {
    Inner,
    Cross,
    Left,
    Right,
    Full,
}

/// A join of the FROM tree; a comma list is a chain of cross joins.
#[derive(Debug, Clone)]
pub struct FromJoin {
    /// `None` for an implicit comma join.
    pub node: Option<NodeId>,
    pub kind: JoinKind,
    /// An outer join made inner by a NULL-rejecting conjunct above it.
    pub reduced: bool,
    pub left: Vec<SlotId>,
    pub right: Vec<SlotId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConjunctScope {
    Where,
    On(JoinId),
}

/// One AND-ed conjunct of WHERE or of one join's ON. A `USING`/`NATURAL`
/// merged column is an `Equality` conjunct of its join.
#[derive(Debug, Clone)]
pub struct Conjunct {
    pub scope: ConjunctScope,
    pub form: ConjunctForm,
    /// Every column it mentions, a nested subquery's included.
    pub columns: Vec<ColumnBinding>,
    /// Mentions a column of an enclosing SELECT.
    pub external: bool,
    /// A nested subquery mentions a column from outside itself, so its rows
    /// depend on the rows it is compared against.
    pub correlated: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ConjunctForm {
    /// A single-column constraint: a comparison, `IN` / `= ANY`, one bound
    /// of a `BETWEEN`, one element of a `NOT IN`, or a supported cast.
    Constraint(ColumnConstraint),
    /// `column = column`.
    Equality(ColumnBinding, ColumnBinding),
    /// Anything else: kept for its columns, never for its meaning.
    Opaque,
}

/// A constraint on one column of one reference.
pub type ColumnConstraint = Constraint<ColumnBinding>;

/// How an unreduced outer join that NULL-fills a reference affects it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OuterJoinRole {
    /// No unreduced outer join NULL-fills it.
    Kept,
    /// NULL-filled, and no conjunct above the join mentions it.
    Terminal,
    /// NULL-filled, and a conjunct above the join mentions it.
    Optional,
}

/// The facts one reference's rows must answer to.
#[derive(Debug, Clone)]
pub struct TableReferencePredicates {
    /// Necessary condition: every row of it in the result satisfies these.
    pub filters: Vec<ColumnConstraint>,
    /// Exact description: population loads every row satisfying `filters`.
    pub exact: bool,
    /// Dependency footprint: columns whose change can alter the result
    /// other than through `filters`.
    pub link_columns: Vec<ColumnPosition>,
    /// A conjunct the model couldn't read mentions it, so `filters` may not
    /// be all that restricts its rows.
    pub unrecognized: bool,
    pub outer_join_role: OuterJoinRole,
}

/// A reference's predicates by column name, as CDC rows are matched.
#[derive(Debug, Clone)]
pub struct RelationPredicates {
    pub filters: Vec<TableConstraint>,
    /// `filters` reduced once to per-column ranges, for subsumption and
    /// read-after-write.
    pub ranges: TableRanges,
    pub link_columns: Vec<EcoString>,
    pub unrecognized: bool,
    pub exact: bool,
}

/// The predicates of one SELECT branch over its table references.
#[derive(Debug, Clone)]
pub struct QueryBranchPredicates {
    table_references: Vec<TableReference>,
    joins: Vec<FromJoin>,
    conjuncts: Vec<Conjunct>,
    /// One per reference, in `table_references` order.
    reference_predicates: Vec<TableReferencePredicates>,
}
