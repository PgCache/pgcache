//! search_path handling, split by safety profile.
//!
//! * [`mutations`] — unsafe FFI classification of search_path-mutating
//!   statements over a raw pg_query parse tree.
//! * [`value`] — the pure, safe `SearchPath` value: parse and resolve.

mod mutations;
mod value;

pub use mutations::{MutationKind, SearchPathMutations, search_path_mutations_raw};
pub use value::{SearchPath, SearchPathEntry};
