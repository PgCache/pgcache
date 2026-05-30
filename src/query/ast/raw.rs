//! Low-level accessors for walking PostgreSQL's raw parse tree (the C node
//! structs exposed via `pg_query::pg_nodes`). All `unsafe` pointer handling for
//! the raw-tree converter is centralized here so [`super::convert_raw`] reads
//! as ordinary tree-walking code.
//!
//! Every accessor assumes it is called while the parse tree is alive — i.e.
//! inside the `pg_query::parse_raw_scoped` callback. Pointers must not escape.

// Matching PostgreSQL's ~260-variant NodeTag always needs a catch-all arm.
#![allow(clippy::wildcard_enum_match_arm)]

use std::ffi::CStr;
use std::os::raw::c_char;

use pg_query::pg_nodes as pg;
use pg_query::pg_nodes::{List, ListCell, Node, NodeTag};

/// A borrowed pointer into the raw parse tree. Null is a valid value (an
/// absent optional child), distinguished from a present node.
pub(crate) type NodePtr = *const Node;

/// Read a node's tag. Caller guarantees `node` is non-null and points at a
/// node that begins with `NodeTag` (every PG node does).
#[inline]
pub(crate) unsafe fn node_tag(node: NodePtr) -> NodeTag {
    unsafe { (*node).type_ }
}

/// Reinterpret a node pointer as a pointer to a concrete node struct. Caller
/// guarantees the tag matches `T` before dereferencing the result.
#[inline]
pub(crate) fn cast<T>(node: NodePtr) -> *const T {
    node as *const T
}

/// Borrowing iterator over the cells of a PG node-`List` (PG13+ array layout),
/// yielding each cell's `ptr_value` as a [`NodePtr`]. Zero-allocation — it
/// indexes `List.elements` directly. Empty for a NULL list (PG's `NIL`) or a
/// non-pointer list (`T_IntList`/`T_OidList`/`T_XidList`), so a caller can never
/// mis-read a packed integer as a pointer.
pub(crate) struct ListIter {
    elements: *const ListCell,
    front: usize,
    back: usize,
}

impl Iterator for ListIter {
    type Item = NodePtr;

    fn next(&mut self) -> Option<NodePtr> {
        if self.front >= self.back {
            return None;
        }
        let node = unsafe { (*self.elements.add(self.front)).ptr_value as NodePtr };
        self.front += 1;
        Some(node)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let len = self.back - self.front;
        (len, Some(len))
    }
}

impl ExactSizeIterator for ListIter {}

impl DoubleEndedIterator for ListIter {
    fn next_back(&mut self) -> Option<NodePtr> {
        if self.front >= self.back {
            return None;
        }
        self.back -= 1;
        Some(unsafe { (*self.elements.add(self.back)).ptr_value as NodePtr })
    }
}

/// Iterate the node pointers of a (possibly null) PG `List *`.
pub(crate) unsafe fn list_nodes(list: *const List) -> ListIter {
    unsafe {
        if list.is_null() || (*list).type_ != pg::NodeTag_T_List {
            return ListIter {
                elements: std::ptr::null(),
                front: 0,
                back: 0,
            };
        }
        let len = usize::try_from((*list).length).unwrap_or(0);
        ListIter {
            elements: (*list).elements,
            front: 0,
            back: len,
        }
    }
}

/// Whether a (possibly null) PG `List *` is empty (NIL or zero-length).
pub(crate) unsafe fn list_is_empty(list: *const List) -> bool {
    list.is_null() || unsafe { (*list).length } <= 0
}

/// Borrow a C string as `&str`; a null pointer becomes `""` (matching the
/// protobuf path, where absent strings decode to empty). Invalid UTF-8 also
/// yields `""`.
pub(crate) unsafe fn cstr<'a>(p: *const c_char) -> &'a str {
    if p.is_null() {
        return "";
    }
    unsafe { CStr::from_ptr(p).to_str().unwrap_or("") }
}

/// If `node` is a `String` value node, return its text; otherwise `None`.
pub(crate) unsafe fn string_node_value<'a>(node: NodePtr) -> Option<&'a str> {
    if node.is_null() {
        return None;
    }
    unsafe {
        match node_tag(node) {
            pg::NodeTag_T_String => Some(cstr((*cast::<pg_query::pg_nodes::String>(node)).sval)),
            _ => None,
        }
    }
}
