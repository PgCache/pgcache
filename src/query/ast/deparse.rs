//! Render a tree as SQL. There is one walk; a [`DeparseContext`] decides
//! what the references in it render as, so the plain tree and a bound tree
//! (ADR-057) share it.

use ecow::EcoString;

use super::{ColumnNode, CteRefNode, FunctionCall, JoinNode, LiteralValue, StarNode, TableNode};
use crate::pg::{identifier_needs_quotes, identifier_quote_into};

pub trait Deparse {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String;
}

/// A node that renders through a [`DeparseContext`]; its [`Deparse`] is the
/// walk under [`Plain`].
pub trait DeparseWith {
    fn deparse_with<C: DeparseContext>(&self, cx: &mut C, buf: &mut String);
}

impl<T: DeparseWith> Deparse for T {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.deparse_with(&mut Plain, buf);
        buf
    }
}

/// A clause the walk is inside, reported so a context can render a literal
/// by its position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Clause {
    With,
    SelectList,
    From,
    JoinOn,
    Where,
    GroupBy,
    Having,
    OrderBy,
    Limit,
    Values,
}

/// What the walk asks its context at each reference. Every hook defaults
/// to the node as written.
pub trait DeparseContext: Sized {
    /// `schema.name` of a table reference; the walk adds the alias.
    fn table_name_deparse(&mut self, node: &TableNode, buf: &mut String) {
        node.name_deparse_plain(buf);
    }

    fn column_deparse(&mut self, node: &ColumnNode, buf: &mut String) {
        node.deparse_plain(buf);
    }

    /// A select-list star. An expansion into several columns separates
    /// them with `, ` and takes no leading space.
    fn star_deparse(&mut self, node: &StarNode, buf: &mut String) {
        node.deparse_plain(buf);
    }

    /// A CTE reference in FROM, with its leading space and alias.
    fn cte_ref_deparse(&mut self, node: &CteRefNode, buf: &mut String) {
        node.deparse_plain(buf);
    }

    /// Whether a query's `WITH` clause renders.
    fn with_clause_renders(&self) -> bool {
        true
    }

    /// The columns a `NATURAL` join merges: `None` keeps the keyword as
    /// written, an empty list renders `CROSS JOIN`, otherwise `USING (…)`.
    fn natural_join_columns(&mut self, _join: &JoinNode) -> Option<Vec<EcoString>> {
        None
    }

    /// The name of a function call, before its parenthesised arguments.
    fn function_name_deparse(&mut self, call: &FunctionCall, buf: &mut String) {
        call.name_deparse_plain(buf);
    }

    fn literal_deparse(&mut self, literal: &LiteralValue, buf: &mut String) {
        literal.deparse(buf);
    }

    fn clause_enter(&mut self, _clause: Clause) {}

    fn clause_leave(&mut self, _clause: Clause) {}
}

/// The context that renders a tree as written.
pub struct Plain;

impl DeparseContext for Plain {}

/// Deparse `items` with `separator` between them.
pub(crate) fn separated_deparse<T: Deparse>(buf: &mut String, items: &[T], separator: &str) {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            buf.push_str(separator);
        }
        item.deparse(buf);
    }
}

/// [`separated_deparse`] under a context.
pub(crate) fn separated_deparse_with<C: DeparseContext, T: DeparseWith>(
    cx: &mut C,
    buf: &mut String,
    items: &[T],
    separator: &str,
) {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            buf.push_str(separator);
        }
        item.deparse_with(cx, buf);
    }
}

/// `keyword` then `expr`, when present.
pub(crate) fn clause_deparse(buf: &mut String, keyword: &str, expr: Option<&impl Deparse>) {
    if let Some(expr) = expr {
        buf.push_str(keyword);
        expr.deparse(buf);
    }
}

/// `keyword` then `expr`, when present, reported to the context as `clause`.
pub(crate) fn clause_deparse_with<C: DeparseContext, T: DeparseWith>(
    cx: &mut C,
    buf: &mut String,
    clause: Clause,
    keyword: &str,
    expr: Option<&T>,
) {
    if let Some(expr) = expr {
        cx.clause_enter(clause);
        buf.push_str(keyword);
        expr.deparse_with(cx, buf);
        cx.clause_leave(clause);
    }
}

/// Deparse `item`, wrapped in parentheses when `parens` (precedence needs it).
pub(crate) fn parenthesized_deparse(buf: &mut String, item: &impl Deparse, parens: bool) {
    if parens {
        buf.push('(');
    }
    item.deparse(buf);
    if parens {
        buf.push(')');
    }
}

/// [`parenthesized_deparse`] under a context.
pub(crate) fn parenthesized_deparse_with<C: DeparseContext, T: DeparseWith>(
    cx: &mut C,
    buf: &mut String,
    item: &T,
    parens: bool,
) {
    if parens {
        buf.push('(');
    }
    item.deparse_with(cx, buf);
    if parens {
        buf.push(')');
    }
}

impl Deparse for String {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.as_str().deparse(buf)
    }
}

impl Deparse for &str {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        match identifier_needs_quotes(self) {
            true => identifier_quote_into(self, buf),
            false => buf.push_str(self),
        };

        buf
    }
}

impl Deparse for EcoString {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String {
        self.as_str().deparse(buf)
    }
}
