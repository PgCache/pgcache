//! Rendering: the trait a value renders through, the clause positions a
//! rendering context is told about, and how identifiers quote.

use ecow::EcoString;

use crate::pg::{identifier_needs_quotes, identifier_quote_into};

pub trait Deparse {
    fn deparse<'b>(&self, buf: &'b mut String) -> &'b mut String;
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

/// Deparse `items` with `separator` between them.
pub(crate) fn separated_deparse<T: Deparse>(buf: &mut String, items: &[T], separator: &str) {
    for (i, item) in items.iter().enumerate() {
        if i > 0 {
            buf.push_str(separator);
        }
        item.deparse(buf);
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
