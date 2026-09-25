//! Raw-tree write classification for read-after-write tracking (PGC-124).
//!
//! Unlike the SELECT converter, classification is total: anything the walker
//! does not recognize classifies as a connection-scoped write — the failure
//! direction is always "assume it wrote", never "assume it didn't".

use std::sync::Arc;

use ecow::EcoString;

use pg_query::pg_nodes as pg;

use crate::query::write::{
    DeleteStatement, INSERT_MAX_ROWS, InsertRow, InsertStatement, IsolationEffect, IsolationLevel,
    RelationRef, SetAssignment, TransactionBoundary, UpdateStatement, WriteClass, WriteComparison,
};

use super::super::raw::{NodePtr, cast, cstr, list_is_empty, list_nodes, node_tag};
use super::super::{BinaryExpr, BinaryOp, LiteralValue, ScalarExpr, WhereExpr};
use super::where_clause::{const_value_extract, param_ref_extract, where_expr_convert};

/// Classification of a non-SELECT root statement.
pub(super) enum NonSelectClass {
    Write(WriteClass),
    /// Provably cannot modify table data (transaction control, SET, SHOW,
    /// FETCH, ...). Kept to an explicit whitelist; everything else is a write.
    /// Carries the transaction boundary for transaction-control statements and
    /// the statement's effect on isolation-level state (PGC-387).
    ReadOnly {
        transaction: Option<TransactionBoundary>,
        isolation: IsolationEffect,
    },
}

/// A read-only classification with no isolation effect.
fn read_only(transaction: Option<TransactionBoundary>) -> NonSelectClass {
    NonSelectClass::ReadOnly {
        transaction,
        isolation: IsolationEffect::None,
    }
}

/// The isolation level named by an `A_Const` string argument (`'read
/// committed'` and friends); `None` for anything else.
unsafe fn isolation_level_of_arg(arg: NodePtr) -> Option<IsolationLevel> {
    unsafe {
        if arg.is_null() || node_tag(arg) != pg::NodeTag_T_A_Const {
            return None;
        }
        match const_value_extract(cast::<pg::A_Const>(arg)) {
            Ok(LiteralValue::String(value)) => IsolationLevel::parse(&value),
            _ => None,
        }
    }
}

/// Scan a `DefElem` option list (`BEGIN`/`SET TRANSACTION`/`SET SESSION
/// CHARACTERISTICS`) for `transaction_isolation`. `None` when absent;
/// `Some(None)` when present but unreadable.
unsafe fn isolation_defelem_find(options: *const pg::List) -> Option<Option<IsolationLevel>> {
    unsafe {
        for node in list_nodes(options) {
            if node_tag(node) != pg::NodeTag_T_DefElem {
                continue;
            }
            let def = cast::<pg::DefElem>(node);
            if cstr((*def).defname).eq_ignore_ascii_case("transaction_isolation") {
                return Some(isolation_level_of_arg((*def).arg as NodePtr));
            }
        }
        None
    }
}

/// Isolation effect of a `BEGIN` / `START TRANSACTION` option list.
unsafe fn begin_isolation_effect(options: *const pg::List) -> IsolationEffect {
    unsafe {
        match isolation_defelem_find(options) {
            None => IsolationEffect::None,
            Some(Some(level)) => IsolationEffect::Transaction(level),
            // An unreadable level can only be stricter than we can prove.
            Some(None) => IsolationEffect::Transaction(IsolationLevel::Serializable),
        }
    }
}

/// Isolation effect of a `SET` / `RESET` statement.
unsafe fn variable_set_isolation_effect(s: *const pg::VariableSetStmt) -> IsolationEffect {
    unsafe {
        let name = cstr((*s).name);
        let kind = (*s).kind;
        if kind == pg::VariableSetKind_VAR_RESET_ALL {
            return IsolationEffect::SessionUnknown;
        }
        if kind == pg::VariableSetKind_VAR_SET_MULTI {
            // SET TRANSACTION ... / SET SESSION CHARACTERISTICS AS TRANSACTION ...
            let scope_transaction = name.eq_ignore_ascii_case("TRANSACTION");
            let scope_session = name.eq_ignore_ascii_case("SESSION CHARACTERISTICS");
            if !scope_transaction && !scope_session {
                return IsolationEffect::None;
            }
            return match isolation_defelem_find((*s).args) {
                None => IsolationEffect::None,
                Some(Some(level)) if scope_transaction => IsolationEffect::Transaction(level),
                Some(Some(level)) => IsolationEffect::SessionDefault(level),
                Some(None) if scope_transaction => {
                    IsolationEffect::Transaction(IsolationLevel::Serializable)
                }
                Some(None) => IsolationEffect::SessionUnknown,
            };
        }
        let default_guc = name.eq_ignore_ascii_case("default_transaction_isolation");
        let transaction_guc = name.eq_ignore_ascii_case("transaction_isolation");
        if !default_guc && !transaction_guc {
            return IsolationEffect::None;
        }
        if kind != pg::VariableSetKind_VAR_SET_VALUE || (*s).is_local {
            // RESET, SET ... TO DEFAULT, SET FROM CURRENT, SET LOCAL: the
            // resulting value is not readable from the statement.
            return if transaction_guc {
                IsolationEffect::Transaction(IsolationLevel::Serializable)
            } else {
                IsolationEffect::SessionUnknown
            };
        }
        let level = list_nodes((*s).args)
            .next()
            .and_then(|arg| isolation_level_of_arg(arg));
        match (level, transaction_guc) {
            (Some(level), true) => IsolationEffect::Transaction(level),
            (Some(level), false) => IsolationEffect::SessionDefault(level),
            (None, true) => IsolationEffect::Transaction(IsolationLevel::Serializable),
            (None, false) => IsolationEffect::SessionUnknown,
        }
    }
}

/// Classify a non-`SelectStmt` root statement.
pub(super) unsafe fn non_select_classify(stmt: NodePtr) -> NonSelectClass {
    use NonSelectClass::{ReadOnly, Write};
    unsafe {
        match node_tag(stmt) {
            pg::NodeTag_T_InsertStmt => Write(insert_classify(cast::<pg::InsertStmt>(stmt))),
            pg::NodeTag_T_UpdateStmt => Write(update_classify(cast::<pg::UpdateStmt>(stmt))),
            pg::NodeTag_T_DeleteStmt => Write(delete_classify(cast::<pg::DeleteStmt>(stmt))),
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
                    read_only(None)
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
                let s = cast::<pg::TransactionStmt>(stmt);
                match (*s).kind {
                    pg::TransactionStmtKind_TRANS_STMT_BEGIN
                    | pg::TransactionStmtKind_TRANS_STMT_START => ReadOnly {
                        transaction: Some(TransactionBoundary::Begin),
                        isolation: begin_isolation_effect((*s).options),
                    },
                    // AND CHAIN immediately re-enters a transaction.
                    pg::TransactionStmtKind_TRANS_STMT_COMMIT
                    | pg::TransactionStmtKind_TRANS_STMT_ROLLBACK => {
                        read_only(Some(if (*s).chain {
                            TransactionBoundary::Begin
                        } else {
                            TransactionBoundary::End
                        }))
                    }
                    pg::TransactionStmtKind_TRANS_STMT_SAVEPOINT
                    | pg::TransactionStmtKind_TRANS_STMT_RELEASE
                    | pg::TransactionStmtKind_TRANS_STMT_ROLLBACK_TO => read_only(None),
                    // PREPARE TRANSACTION commits later, possibly from another
                    // session — the entry must never be LSN-stamped.
                    pg::TransactionStmtKind_TRANS_STMT_PREPARE => {
                        Write(WriteClass::ConnectionUnstampable)
                    }
                    _ => Write(WriteClass::Connection),
                }
            }
            pg::NodeTag_T_VariableSetStmt => ReadOnly {
                transaction: None,
                isolation: variable_set_isolation_effect(cast::<pg::VariableSetStmt>(stmt)),
            },
            pg::NodeTag_T_DiscardStmt => ReadOnly {
                transaction: None,
                isolation: match (*cast::<pg::DiscardStmt>(stmt)).target {
                    pg::DiscardMode_DISCARD_ALL => IsolationEffect::SessionUnknown,
                    _ => IsolationEffect::None,
                },
            },
            pg::NodeTag_T_VariableShowStmt
            | pg::NodeTag_T_DeallocateStmt
            | pg::NodeTag_T_ClosePortalStmt
            | pg::NodeTag_T_FetchStmt
            | pg::NodeTag_T_PrepareStmt
            | pg::NodeTag_T_ListenStmt
            | pg::NodeTag_T_UnlistenStmt
            | pg::NodeTag_T_NotifyStmt => read_only(None),
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

/// Classify a `DELETE`. A single-table delete with a bare-column, AND-only
/// WHERE predicate becomes row-enumerable [`WriteClass::DeleteRows`] so a read
/// provably disjoint from the delete's predicate can still be served (PGC-381).
/// A DML CTE, `USING` join, missing/whole-table WHERE, or any predicate the
/// walker can't reduce degrades to table-level opaque.
unsafe fn delete_classify(s: *const pg::DeleteStmt) -> WriteClass {
    unsafe {
        let relation = (*s).relation;
        if relation.is_null() || with_clause_has_dml((*s).withClause) {
            return WriteClass::Connection;
        }
        let table = table_class(relation);
        let WriteClass::Table(relation_ref) = &table else {
            return table;
        };
        // USING joins other tables into the predicate — not single-table.
        if !list_is_empty((*s).usingClause) {
            return table;
        }
        let where_node = (*s).whereClause as NodePtr;
        if where_node.is_null() {
            return table; // whole-table delete: any read of it intersects
        }
        let Ok(where_expr) = where_expr_convert(where_node) else {
            return table; // unconvertible WHERE → opaque
        };
        match where_expr_comparisons(&where_expr) {
            Some(comparisons) if !comparisons.is_empty() => {
                WriteClass::DeleteRows(Arc::new(DeleteStatement {
                    relation: relation_ref.clone(),
                    comparisons,
                }))
            }
            _ => table, // non-extractable predicate → opaque
        }
    }
}

/// Classify an `UPDATE`. A single-table update with a bare-column AND-only WHERE
/// and an extractable SET list becomes row-enumerable [`WriteClass::UpdateRows`]
/// so a read disjoint from both the affected rows and their post-update image
/// can still be served (PGC-382). A DML CTE, `FROM` join, missing/whole-table
/// WHERE, or a SET target the walker can't reduce degrades to table-level opaque.
unsafe fn update_classify(s: *const pg::UpdateStmt) -> WriteClass {
    unsafe {
        let relation = (*s).relation;
        if relation.is_null() || with_clause_has_dml((*s).withClause) {
            return WriteClass::Connection;
        }
        let table = table_class(relation);
        let WriteClass::Table(relation_ref) = &table else {
            return table;
        };
        // A FROM clause joins other tables into the predicate — not single-table.
        if !list_is_empty((*s).fromClause) {
            return table;
        }
        let Some(set) = update_set_extract((*s).targetList) else {
            return table; // multi-assign / subscripted target → opaque
        };
        let where_node = (*s).whereClause as NodePtr;
        if where_node.is_null() {
            return table; // whole-table update: any read of it intersects
        }
        let Ok(where_expr) = where_expr_convert(where_node) else {
            return table;
        };
        match where_expr_comparisons(&where_expr) {
            Some(where_comparisons) if !where_comparisons.is_empty() => {
                WriteClass::UpdateRows(Arc::new(UpdateStatement {
                    relation: relation_ref.clone(),
                    where_comparisons,
                    set,
                }))
            }
            _ => table,
        }
    }
}

/// Extract a SET list into `(column, Option<literal>)` assignments, or `None` if
/// any target isn't a plain single-column assignment (multi-column `SET (a,b) =
/// …`, subscripted `SET a[1] = …`). A non-literal RHS keeps the column with a
/// `None` value — its post-update value is unknown (PGC-382).
unsafe fn update_set_extract(target_list: *const pg::List) -> Option<Vec<SetAssignment>> {
    unsafe {
        let mut set = Vec::new();
        for node in list_nodes(target_list) {
            if node_tag(node) != pg::NodeTag_T_ResTarget {
                return None;
            }
            let res = cast::<pg::ResTarget>(node);
            let name = cstr((*res).name);
            if name.is_empty() || !list_is_empty((*res).indirection) {
                return None;
            }
            set.push((
                EcoString::from(name),
                insert_cell_extract((*res).val as NodePtr),
            ));
        }
        (!set.is_empty()).then_some(set)
    }
}

/// Reduce a WHERE to its bare-column AND-conjunct comparisons, or `None` if any
/// part isn't a single-column `col op literal` comparison (OR, NOT, IN/BETWEEN,
/// subquery, function, cast, cross-column) — the whole predicate then degrades
/// to opaque. Shared by DELETE (and UPDATE, PGC-382).
fn where_expr_comparisons(where_expr: &WhereExpr) -> Option<Vec<WriteComparison>> {
    let mut out = Vec::new();
    where_expr_comparisons_collect(where_expr, &mut out).then_some(out)
}

fn where_expr_comparisons_collect(where_expr: &WhereExpr, out: &mut Vec<WriteComparison>) -> bool {
    match where_expr {
        WhereExpr::Binary(b) if b.op == BinaryOp::And => {
            where_expr_comparisons_collect(&b.lexpr, out)
                && where_expr_comparisons_collect(&b.rexpr, out)
        }
        WhereExpr::Binary(b) => match comparison_extract(b) {
            Some(comparison) => {
                out.push(comparison);
                true
            }
            None => false,
        },
        _ => false,
    }
}

/// Extract `column op literal` (or `literal op column`, flipped) from a binary
/// comparison; `None` for logical/LIKE ops or non-bare-column operands.
fn comparison_extract(b: &BinaryExpr) -> Option<WriteComparison> {
    // `op_flip` returns `None` for non-comparison ops (AND/OR/LIKE/…); the
    // flipped op also normalizes `literal op column` to `column op literal`.
    let flipped = b.op.op_flip()?;
    match (b.lexpr.as_ref(), b.rexpr.as_ref()) {
        (
            WhereExpr::Scalar(ScalarExpr::Column(col)),
            WhereExpr::Scalar(ScalarExpr::Literal(lit)),
        ) => Some((col.column.clone(), b.op, lit.clone())),
        (
            WhereExpr::Scalar(ScalarExpr::Literal(lit)),
            WhereExpr::Scalar(ScalarExpr::Column(col)),
        ) => Some((col.column.clone(), flipped, lit.clone())),
        _ => None,
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
