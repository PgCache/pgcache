//! Raw-tree write classification for read-after-write tracking (PGC-124).
//!
//! Unlike the SELECT converter, classification is total: anything the walker
//! does not recognize classifies as a connection-scoped write — the failure
//! direction is always "assume it wrote", never "assume it didn't".

use std::sync::Arc;

use ecow::EcoString;

use pg_query::pg_nodes as pg;

use crate::query::write::{INSERT_MAX_ROWS, InsertRow, InsertStatement, RelationRef, WriteClass};

use super::super::LiteralValue;
use super::super::raw::{NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag};
use super::where_clause::{const_value_extract, param_ref_extract};

/// Classification of a non-SELECT root statement.
pub(super) enum NonSelectClass {
    Write(WriteClass),
    /// Provably cannot modify table data (transaction control, SET, SHOW,
    /// FETCH, ...). Kept to an explicit whitelist; everything else is a write.
    ReadOnly,
}

/// Classify a non-`SelectStmt` root statement.
pub(super) unsafe fn non_select_classify(stmt: NodePtr) -> NonSelectClass {
    use NonSelectClass::{ReadOnly, Write};
    unsafe {
        match node_tag(stmt) {
            pg::NodeTag_T_InsertStmt => Write(insert_classify(cast::<pg::InsertStmt>(stmt))),
            pg::NodeTag_T_UpdateStmt => {
                let s = cast::<pg::UpdateStmt>(stmt);
                Write(dml_table_classify((*s).relation, (*s).withClause))
            }
            pg::NodeTag_T_DeleteStmt => {
                let s = cast::<pg::DeleteStmt>(stmt);
                Write(dml_table_classify((*s).relation, (*s).withClause))
            }
            pg::NodeTag_T_MergeStmt => {
                let s = cast::<pg::MergeStmt>(stmt);
                Write(dml_table_classify((*s).relation, (*s).withClause))
            }
            pg::NodeTag_T_CopyStmt => {
                let s = cast::<pg::CopyStmt>(stmt);
                if (*s).is_from {
                    Write(dml_table_classify((*s).relation, std::ptr::null()))
                } else if copy_to_query_writes((*s).query as NodePtr) {
                    // COPY (WITH x AS (INSERT ...) ...) TO — the query writes.
                    Write(WriteClass::Connection)
                } else {
                    ReadOnly
                }
            }
            pg::NodeTag_T_TruncateStmt => {
                let s = cast::<pg::TruncateStmt>(stmt);
                let mut relations = list_nodes((*s).relations);
                match (relations.next(), relations.next()) {
                    (Some(only), None) if node_tag(only) == pg::NodeTag_T_RangeVar => {
                        Write(table_class(cast::<pg::RangeVar>(only)))
                    }
                    _ => Write(WriteClass::Connection),
                }
            }
            pg::NodeTag_T_TransactionStmt => {
                match (*cast::<pg::TransactionStmt>(stmt)).kind {
                    pg::TransactionStmtKind_TRANS_STMT_BEGIN
                    | pg::TransactionStmtKind_TRANS_STMT_START
                    | pg::TransactionStmtKind_TRANS_STMT_COMMIT
                    | pg::TransactionStmtKind_TRANS_STMT_ROLLBACK
                    | pg::TransactionStmtKind_TRANS_STMT_SAVEPOINT
                    | pg::TransactionStmtKind_TRANS_STMT_RELEASE
                    | pg::TransactionStmtKind_TRANS_STMT_ROLLBACK_TO => ReadOnly,
                    // PREPARE TRANSACTION commits later, possibly from another
                    // session — the entry must never be LSN-stamped.
                    pg::TransactionStmtKind_TRANS_STMT_PREPARE => {
                        Write(WriteClass::ConnectionUnstampable)
                    }
                    _ => Write(WriteClass::Connection),
                }
            }
            pg::NodeTag_T_VariableSetStmt
            | pg::NodeTag_T_VariableShowStmt
            | pg::NodeTag_T_DiscardStmt
            | pg::NodeTag_T_DeallocateStmt
            | pg::NodeTag_T_ClosePortalStmt
            | pg::NodeTag_T_FetchStmt
            | pg::NodeTag_T_PrepareStmt
            | pg::NodeTag_T_ListenStmt
            | pg::NodeTag_T_UnlistenStmt
            | pg::NodeTag_T_NotifyStmt => ReadOnly,
            // ExecuteStmt runs a SQL-level prepared statement whose body may be
            // DML; ExplainStmt with ANALYZE executes its argument. Everything
            // else (DDL, CALL, DO, unknown) is a potential write.
            _ => Write(WriteClass::Connection),
        }
    }
}

/// Write classification for a root `SelectStmt` whose conversion failed:
/// `Some` when its WITH clause contains data-modifying CTEs (the "select"
/// writes). One DML CTE with a known relation → that table; anything murkier
/// → connection scope.
pub(super) unsafe fn select_cte_write_class(select: *const pg::SelectStmt) -> Option<WriteClass> {
    unsafe {
        let with = (*select).withClause;
        if with.is_null() {
            return None;
        }
        let mut dml_relation: Option<*const pg::RangeVar> = None;
        for cte_node in list_nodes((*with).ctes) {
            if node_tag(cte_node) != pg::NodeTag_T_CommonTableExpr {
                return Some(WriteClass::Connection);
            }
            let inner = (*cast::<pg::CommonTableExpr>(cte_node)).ctequery as NodePtr;
            if inner.is_null() {
                return Some(WriteClass::Connection);
            }
            let relation = match node_tag(inner) {
                pg::NodeTag_T_SelectStmt => continue,
                pg::NodeTag_T_InsertStmt => (*cast::<pg::InsertStmt>(inner)).relation,
                pg::NodeTag_T_UpdateStmt => (*cast::<pg::UpdateStmt>(inner)).relation,
                pg::NodeTag_T_DeleteStmt => (*cast::<pg::DeleteStmt>(inner)).relation,
                pg::NodeTag_T_MergeStmt => (*cast::<pg::MergeStmt>(inner)).relation,
                _ => return Some(WriteClass::Connection),
            };
            if relation.is_null() || dml_relation.is_some() {
                return Some(WriteClass::Connection);
            }
            dml_relation = Some(relation);
        }
        dml_relation.map(|rv| table_class(rv))
    }
}

/// Whether a WITH clause contains (or might contain) a data-modifying CTE.
unsafe fn with_clause_has_dml(with: *const pg::WithClause) -> bool {
    unsafe {
        !with.is_null()
            && list_nodes((*with).ctes).any(|cte_node| {
                if node_tag(cte_node) != pg::NodeTag_T_CommonTableExpr {
                    return true;
                }
                let inner = (*cast::<pg::CommonTableExpr>(cte_node)).ctequery as NodePtr;
                inner.is_null() || node_tag(inner) != pg::NodeTag_T_SelectStmt
            })
    }
}

/// Whether a `COPY (query) TO` query can modify table data.
unsafe fn copy_to_query_writes(query: NodePtr) -> bool {
    unsafe {
        if query.is_null() {
            return false;
        }
        match node_tag(query) {
            pg::NodeTag_T_SelectStmt => {
                with_clause_has_dml((*cast::<pg::SelectStmt>(query)).withClause)
            }
            _ => true,
        }
    }
}

/// Table-level class for an UPDATE/DELETE/MERGE/COPY-FROM target. A DML CTE
/// in the statement's WITH clause targets a *different* table, so its
/// presence widens the scope to the whole connection.
unsafe fn dml_table_classify(
    relation: *const pg::RangeVar,
    with: *const pg::WithClause,
) -> WriteClass {
    unsafe {
        if relation.is_null() || with_clause_has_dml(with) {
            return WriteClass::Connection;
        }
        table_class(relation)
    }
}

unsafe fn table_class(relation: *const pg::RangeVar) -> WriteClass {
    unsafe {
        let name = cstr((*relation).relname);
        if name.is_empty() {
            return WriteClass::Connection;
        }
        let schema = cstr((*relation).schemaname);
        WriteClass::Table(RelationRef {
            schema: (!schema.is_empty()).then(|| EcoString::from(schema)),
            name: EcoString::from(name),
        })
    }
}

unsafe fn insert_classify(insert: *const pg::InsertStmt) -> WriteClass {
    unsafe {
        let relation = (*insert).relation;
        if relation.is_null() {
            return WriteClass::Connection;
        }
        // A data-modifying CTE targets a different table than the INSERT.
        if with_clause_has_dml((*insert).withClause) {
            return WriteClass::Connection;
        }
        let table = table_class(relation);
        let WriteClass::Table(relation_ref) = &table else {
            return table;
        };

        // ON CONFLICT can touch existing rows; an omitted column list needs
        // catalog column order the proxy doesn't have.
        if !(*insert).onConflictClause.is_null() || list_is_empty((*insert).cols) {
            return table;
        }
        let mut columns: Vec<EcoString> = Vec::with_capacity(list_nodes((*insert).cols).len());
        for col_node in list_nodes((*insert).cols) {
            if node_tag(col_node) != pg::NodeTag_T_ResTarget {
                return table;
            }
            let res = cast::<pg::ResTarget>(col_node);
            let name = cstr((*res).name);
            // Indirection (`INSERT INTO t (a[1])`) writes part of a value.
            if name.is_empty() || !list_is_empty((*res).indirection) {
                return table;
            }
            columns.push(EcoString::from(name));
        }

        // The VALUES/SELECT arm: `DEFAULT VALUES` has no select; a select
        // without valuesLists is INSERT...SELECT.
        let select = (*insert).selectStmt as NodePtr;
        if select.is_null() || node_tag(select) != pg::NodeTag_T_SelectStmt {
            return table;
        }
        let select = cast::<pg::SelectStmt>(select);
        if list_is_empty((*select).valuesLists) {
            return table;
        }
        let row_nodes = list_nodes((*select).valuesLists);
        if row_nodes.len() > INSERT_MAX_ROWS {
            return table;
        }
        let mut rows: Vec<InsertRow> = Vec::with_capacity(row_nodes.len());
        for row_node in row_nodes {
            if node_tag(row_node) != pg::NodeTag_T_List {
                return table;
            }
            let cells = list_nodes(row_node as *const pg::List);
            if cells.len() != columns.len() {
                return table;
            }
            rows.push(cells.map(|cell| insert_cell_extract(cell)).collect());
        }

        WriteClass::InsertRows(Arc::new(InsertStatement {
            relation: relation_ref.clone(),
            columns,
            rows,
        }))
    }
}

/// Extract one VALUES cell. `None` = value unknown — it must never read as
/// SQL NULL, which could falsely prove disjointness against a predicate.
unsafe fn insert_cell_extract(cell: NodePtr) -> Option<LiteralValue> {
    unsafe {
        match node_tag(cell) {
            pg::NodeTag_T_A_Const => {
                let c = cast::<pg::A_Const>(cell);
                if (*c).isnull {
                    return Some(LiteralValue::Null);
                }
                match (*c).val.node.type_ {
                    pg::NodeTag_T_Integer
                    | pg::NodeTag_T_Float
                    | pg::NodeTag_T_Boolean
                    | pg::NodeTag_T_String
                    | pg::NodeTag_T_BitString => const_value_extract(c).ok(),
                    // An unrecognized const kind is unknown, NOT NULL.
                    _ => None,
                }
            }
            pg::NodeTag_T_ParamRef => Some(param_ref_extract(cast::<pg::ParamRef>(cell))),
            // DEFAULT keyword, casts, function calls, expressions, subqueries.
            _ => None,
        }
    }
}
