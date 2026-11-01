//! Low-level accessors for walking PostgreSQL's raw parse tree (the C node
//! structs exposed via `pg_query::pg_nodes`). All `unsafe` pointer handling for
//! the raw-tree converter is centralized here so [`super::convert_raw`] reads
//! as ordinary tree-walking code.
//!
//! Every accessor assumes it is called while the parse tree is alive — i.e.
//! inside the `pg_query::parse_raw_scoped` callback. Pointers must not escape.

// Matching PostgreSQL's ~260-variant NodeTag always needs a catch-all arm.
#![allow(clippy::wildcard_enum_match_arm)]

use std::borrow::Cow;
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

/// Reader-facing name for a parse-tree node, in SQL terms where one exists.
/// `NodeTag` is a bare C integer whose values shift between PostgreSQL
/// versions, so error messages must not print it raw; matching on the named
/// constants keeps this table correct across pg_query upgrades. Nodes the
/// SELECT converter can't meet fall through to a labelled number.
pub(crate) fn node_tag_name(tag: NodeTag) -> Cow<'static, str> {
    let name = match tag {
        // Statements
        pg::NodeTag_T_SelectStmt => "SELECT",
        pg::NodeTag_T_InsertStmt => "INSERT",
        pg::NodeTag_T_UpdateStmt => "UPDATE",
        pg::NodeTag_T_DeleteStmt => "DELETE",
        pg::NodeTag_T_MergeStmt => "MERGE",
        pg::NodeTag_T_SetOperationStmt => "UNION/INTERSECT/EXCEPT",
        pg::NodeTag_T_ExplainStmt => "EXPLAIN",
        pg::NodeTag_T_CopyStmt => "COPY",
        pg::NodeTag_T_TransactionStmt => "transaction control",
        pg::NodeTag_T_VariableSetStmt => "SET",
        pg::NodeTag_T_VariableShowStmt => "SHOW",
        pg::NodeTag_T_PrepareStmt => "PREPARE",
        pg::NodeTag_T_ExecuteStmt => "EXECUTE",
        pg::NodeTag_T_DeclareCursorStmt => "DECLARE CURSOR",
        pg::NodeTag_T_CreateTableAsStmt => "CREATE TABLE AS",
        pg::NodeTag_T_IndexStmt => "CREATE INDEX",
        pg::NodeTag_T_IndexElem => "index column",
        pg::NodeTag_T_DefElem => "option",
        // FROM-side
        pg::NodeTag_T_RangeVar => "table reference",
        pg::NodeTag_T_RangeSubselect => "subquery in FROM",
        pg::NodeTag_T_JoinExpr => "JOIN",
        pg::NodeTag_T_RangeFunction => "function call",
        pg::NodeTag_T_RangeTableFunc => "XMLTABLE",
        pg::NodeTag_T_RangeTableSample => "TABLESAMPLE",
        pg::NodeTag_T_JsonTable => "JSON_TABLE",
        pg::NodeTag_T_WithClause => "WITH clause",
        pg::NodeTag_T_CommonTableExpr => "common table expression",
        pg::NodeTag_T_LockingClause => "locking clause (FOR UPDATE/FOR SHARE)",
        // Expressions
        pg::NodeTag_T_ColumnRef => "column reference",
        pg::NodeTag_T_ParamRef => "parameter placeholder",
        pg::NodeTag_T_A_Const => "constant",
        pg::NodeTag_T_A_Expr => "operator expression",
        pg::NodeTag_T_A_Star => "*",
        pg::NodeTag_T_A_Indices => "subscript",
        pg::NodeTag_T_A_Indirection => "subscript or field access (x[1], (x).f)",
        pg::NodeTag_T_A_ArrayExpr => "ARRAY constructor",
        pg::NodeTag_T_RowExpr => "row constructor",
        pg::NodeTag_T_TypeCast => "type cast",
        pg::NodeTag_T_TypeName => "type name",
        pg::NodeTag_T_CollateClause => "COLLATE",
        pg::NodeTag_T_FuncCall => "function call",
        pg::NodeTag_T_NamedArgExpr => "named function argument",
        pg::NodeTag_T_SubLink => "subquery expression",
        pg::NodeTag_T_BoolExpr => "AND/OR/NOT expression",
        pg::NodeTag_T_NullTest => "IS NULL test",
        pg::NodeTag_T_BooleanTest => "IS TRUE/IS FALSE test",
        pg::NodeTag_T_CaseExpr => "CASE",
        pg::NodeTag_T_CaseWhen => "CASE WHEN arm",
        pg::NodeTag_T_CoalesceExpr => "COALESCE",
        pg::NodeTag_T_MinMaxExpr => "GREATEST/LEAST",
        pg::NodeTag_T_SQLValueFunction => "CURRENT_DATE/CURRENT_USER-style value function",
        pg::NodeTag_T_XmlExpr | pg::NodeTag_T_XmlSerialize => "XML expression",
        pg::NodeTag_T_JsonObjectConstructor
        | pg::NodeTag_T_JsonArrayConstructor
        | pg::NodeTag_T_JsonArrayQueryConstructor
        | pg::NodeTag_T_JsonObjectAgg
        | pg::NodeTag_T_JsonArrayAgg
        | pg::NodeTag_T_JsonFuncExpr
        | pg::NodeTag_T_JsonIsPredicate
        | pg::NodeTag_T_JsonParseExpr
        | pg::NodeTag_T_JsonScalarExpr
        | pg::NodeTag_T_JsonSerializeExpr
        | pg::NodeTag_T_JsonValueExpr => "JSON expression",
        pg::NodeTag_T_GroupingSet => "GROUPING SETS / ROLLUP / CUBE",
        pg::NodeTag_T_GroupingFunc => "GROUPING()",
        pg::NodeTag_T_SetToDefault => "DEFAULT",
        pg::NodeTag_T_CurrentOfExpr => "CURRENT OF",
        pg::NodeTag_T_MergeSupportFunc => "MERGE_ACTION()",
        pg::NodeTag_T_MultiAssignRef => "multi-column assignment",
        // Clause pieces
        pg::NodeTag_T_ResTarget => "select-list item",
        pg::NodeTag_T_SortBy => "ORDER BY item",
        pg::NodeTag_T_WindowDef => "window definition",
        pg::NodeTag_T_List => "list",
        pg::NodeTag_T_Integer
        | pg::NodeTag_T_Float
        | pg::NodeTag_T_Boolean
        | pg::NodeTag_T_String
        | pg::NodeTag_T_BitString => "literal",
        other => return Cow::Owned(format!("node tag {other}")),
    };
    Cow::Borrowed(name)
}

/// Reader-facing name for an `A_Expr` kind the converter doesn't handle.
pub(crate) fn aexpr_kind_name(kind: pg::A_Expr_Kind) -> Cow<'static, str> {
    let name = match kind {
        pg::A_Expr_Kind_AEXPR_OP => "operator",
        pg::A_Expr_Kind_AEXPR_OP_ANY => "= ANY (array)",
        pg::A_Expr_Kind_AEXPR_OP_ALL => "= ALL (array)",
        pg::A_Expr_Kind_AEXPR_DISTINCT => "IS DISTINCT FROM",
        pg::A_Expr_Kind_AEXPR_NOT_DISTINCT => "IS NOT DISTINCT FROM",
        pg::A_Expr_Kind_AEXPR_NULLIF => "NULLIF",
        pg::A_Expr_Kind_AEXPR_IN => "IN list",
        pg::A_Expr_Kind_AEXPR_LIKE => "LIKE",
        pg::A_Expr_Kind_AEXPR_ILIKE => "ILIKE",
        pg::A_Expr_Kind_AEXPR_SIMILAR => "SIMILAR TO",
        pg::A_Expr_Kind_AEXPR_BETWEEN | pg::A_Expr_Kind_AEXPR_BETWEEN_SYM => "BETWEEN",
        pg::A_Expr_Kind_AEXPR_NOT_BETWEEN | pg::A_Expr_Kind_AEXPR_NOT_BETWEEN_SYM => "NOT BETWEEN",
        other => return Cow::Owned(format!("expression kind {other}")),
    };
    Cow::Borrowed(name)
}

/// Reader-facing name for a SubLink type the converter doesn't handle.
pub(crate) fn sublink_type_name(t: pg::SubLinkType) -> Cow<'static, str> {
    let name = match t {
        pg::SubLinkType_EXISTS_SUBLINK => "EXISTS",
        pg::SubLinkType_ANY_SUBLINK => "IN / = ANY (subquery)",
        pg::SubLinkType_ALL_SUBLINK => "ALL (subquery)",
        pg::SubLinkType_EXPR_SUBLINK => "scalar subquery",
        pg::SubLinkType_ROWCOMPARE_SUBLINK => "row comparison subquery",
        pg::SubLinkType_MULTIEXPR_SUBLINK => "multi-column subquery",
        pg::SubLinkType_ARRAY_SUBLINK => "ARRAY(subquery)",
        pg::SubLinkType_CTE_SUBLINK => "CTE subquery",
        other => return Cow::Owned(format!("subquery kind {other}")),
    };
    Cow::Borrowed(name)
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

/// The length of a (possibly null) PG node `List *`; zero for NIL or a
/// non-pointer list, as [`list_nodes`] iterates it.
pub(crate) unsafe fn list_len(list: *const List) -> u32 {
    unsafe { u32::try_from(list_nodes(list).len()).unwrap_or(u32::MAX) }
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
