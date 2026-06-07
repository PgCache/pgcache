use std::os::raw::c_void;

use pg_query::pg_nodes as pg;

use crate::query::ast::raw::{NodePtr, cast, cstr, list_nodes, node_tag};

/// Classification of a parsed statement with respect to search_path state.
///
/// On pre-PG18, the proxy maintains a cached view of the session's search_path
/// that is only refreshed via `SHOW search_path`. Detecting these statement
/// kinds lets the proxy mark its view stale before the next cacheable query.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MutationKind {
    /// `SET search_path`, `RESET search_path`, or `RESET ALL`. Safe to
    /// piggyback (PG runs these without a txn restriction).
    SearchPathSet,
    /// `DISCARD ALL`. Also invalidates the cached search_path, but must be
    /// issued alone because some sub-operations it performs (DEALLOCATE ALL,
    /// DISCARD TEMP, DISCARD SEQUENCES) cannot run inside a transaction —
    /// including the implicit transaction PG wraps multi-statement
    /// simple-query strings in. So the piggyback path must skip it and rely
    /// on the lazy SHOW-on-RFQ fallback.
    DiscardAll,
    /// `COMMIT` or `ROLLBACK`. `SET LOCAL` and any `SET` inside an aborted
    /// transaction revert at this point, so the cached search_path is
    /// conservatively marked stale on every txn end.
    TxnEnd,
}

impl MutationKind {
    /// Whether it's safe to rewrite a single-statement Query message to
    /// `<stmt>; SHOW search_path` and strip the SHOW response — i.e. the
    /// statement tolerates running inside PG's implicit multi-statement
    /// transaction.
    fn piggybackable(self) -> bool {
        match self {
            Self::SearchPathSet | Self::TxnEnd => true,
            Self::DiscardAll => false,
        }
    }
}

/// Per-query search_path classification computed from a raw parse tree.
#[derive(Debug, Clone, Copy, Default)]
pub struct SearchPathMutations {
    /// Some statement in the parse invalidates the cached search_path — mark
    /// state stale regardless of piggyback eligibility or multi-statement
    /// structure.
    pub any: bool,
    /// The parse is exactly one statement AND it is a mutation that can safely
    /// ride in PG's implicit multi-statement transaction — gates the piggyback
    /// rewrite.
    pub single_piggybackable: Option<MutationKind>,
}

/// Classify a single raw statement node with respect to search_path state.
unsafe fn stmt_classify_raw(stmt: NodePtr) -> Option<MutationKind> {
    unsafe {
        match node_tag(stmt) {
            pg::NodeTag_T_VariableSetStmt => {
                let s = cast::<pg::VariableSetStmt>(stmt);
                if (*s).kind == pg::VariableSetKind_VAR_RESET_ALL
                    || cstr((*s).name).eq_ignore_ascii_case("search_path")
                {
                    Some(MutationKind::SearchPathSet)
                } else {
                    None
                }
            }
            pg::NodeTag_T_DiscardStmt => match (*cast::<pg::DiscardStmt>(stmt)).target {
                pg::DiscardMode_DISCARD_ALL => Some(MutationKind::DiscardAll),
                _ => None,
            },
            pg::NodeTag_T_TransactionStmt => match (*cast::<pg::TransactionStmt>(stmt)).kind {
                pg::TransactionStmtKind_TRANS_STMT_COMMIT
                | pg::TransactionStmtKind_TRANS_STMT_ROLLBACK
                | pg::TransactionStmtKind_TRANS_STMT_COMMIT_PREPARED
                | pg::TransactionStmtKind_TRANS_STMT_ROLLBACK_PREPARED => {
                    Some(MutationKind::TxnEnd)
                }
                _ => None,
            },
            _ => None,
        }
    }
}

/// Classify every statement in a raw parse tree (`List *` of `RawStmt`).
///
/// # Safety
/// `tree_root` must be the live tree handed to a `pg_query::parse_raw_scoped`
/// callback, valid for the duration of this call.
pub unsafe fn search_path_mutations_raw(tree_root: *const c_void) -> SearchPathMutations {
    unsafe {
        // Single pass: track whether any statement mutates, and the kind of the
        // sole statement (for the piggyback gate, which requires exactly one).
        let mut any = false;
        let mut count = 0usize;
        let mut first = None;
        for raw in list_nodes(tree_root as *const pg::List) {
            let stmt = (*cast::<pg::RawStmt>(raw)).stmt as NodePtr;
            let kind = if stmt.is_null() {
                None
            } else {
                stmt_classify_raw(stmt)
            };
            any |= kind.is_some();
            if count == 0 {
                first = kind;
            }
            count += 1;
        }
        SearchPathMutations {
            any,
            single_piggybackable: match (count, first) {
                (1, Some(k)) if k.piggybackable() => Some(k),
                _ => None,
            },
        }
    }
}

/// Represents a single entry in the PostgreSQL search_path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SearchPathEntry {
    /// Literal schema name (includes quoted "$user" which is a literal schema name)
    Schema(String),
    /// Unquoted $user - resolves to session_user at query time
    SessionUser,
}

/// Parsed search_path from PostgreSQL.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct SearchPath(pub Vec<SearchPathEntry>);

impl SearchPath {
    /// Parse a search_path string from PostgreSQL.
    ///
    /// Handles:
    /// - Unquoted `$user` -> `SessionUser`
    /// - Quoted `"$user"` -> `Schema("$user")` (literal schema name)
    /// - Regular identifiers -> `Schema(name)` (lowercased)
    /// - Double-quoted identifiers -> `Schema(name)` (preserves case)
    pub fn parse(s: &str) -> Self {
        let entries = s
            .split(',')
            .map(|entry| entry.trim())
            .filter(|entry| !entry.is_empty())
            .map(|entry| {
                if let Some(quoted) = entry.strip_prefix('"').and_then(|s| s.strip_suffix('"')) {
                    // Quoted identifier: preserve case, unescape double quotes
                    let unescaped = quoted.replace("\"\"", "\"");
                    SearchPathEntry::Schema(unescaped)
                } else if entry == "$user" {
                    // Unquoted $user is the special session user token
                    SearchPathEntry::SessionUser
                } else {
                    // Unquoted identifier: lowercase
                    SearchPathEntry::Schema(entry.to_lowercase())
                }
            })
            .collect();

        Self(entries)
    }

    /// Resolve the search path to a list of schema names.
    ///
    /// Expands `SessionUser` to the provided session_user value.
    /// Skips `SessionUser` entries if session_user is `None`.
    pub fn resolve<'a>(&'a self, session_user: Option<&'a str>) -> impl Iterator<Item = &'a str> {
        self.0.iter().filter_map(move |entry| match entry {
            SearchPathEntry::Schema(name) => Some(name.as_str()),
            SearchPathEntry::SessionUser => session_user,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_empty() {
        let path = SearchPath::parse("");
        assert_eq!(path.0, vec![]);
    }

    #[test]
    fn test_parse_single_schema() {
        let path = SearchPath::parse("public");
        assert_eq!(path.0, vec![SearchPathEntry::Schema("public".to_owned())]);
    }

    #[test]
    fn test_parse_multiple_schemas() {
        let path = SearchPath::parse("myschema, public");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("myschema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_user_unquoted() {
        let path = SearchPath::parse("$user, public");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::SessionUser,
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_user_quoted() {
        // Quoted "$user" is a literal schema name, not the special token
        let path = SearchPath::parse(r#""$user", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("$user".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_user_mixed() {
        // First $user is unquoted (SessionUser), second is quoted (literal)
        let path = SearchPath::parse(r#"$user, "$user", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::SessionUser,
                SearchPathEntry::Schema("$user".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_quoted_preserves_case() {
        let path = SearchPath::parse(r#""MySchema", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("MySchema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_unquoted_lowercases() {
        let path = SearchPath::parse("MySchema, PUBLIC");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("myschema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_extra_whitespace() {
        let path = SearchPath::parse("  $user  ,  public  ");
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::SessionUser,
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_quoted_with_spaces() {
        let path = SearchPath::parse(r#""my schema", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema("my schema".to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_parse_quoted_with_escaped_quote() {
        let path = SearchPath::parse(r#""my""schema", public"#);
        assert_eq!(
            path.0,
            vec![
                SearchPathEntry::Schema(r#"my"schema"#.to_owned()),
                SearchPathEntry::Schema("public".to_owned()),
            ]
        );
    }

    #[test]
    fn test_resolve_with_session_user() {
        let path = SearchPath::parse("$user, public");
        let resolved: Vec<&str> = path.resolve(Some("alice")).collect();
        assert_eq!(resolved, vec!["alice", "public"]);
    }

    #[test]
    fn test_resolve_without_session_user() {
        let path = SearchPath::parse("$user, public");
        let resolved: Vec<&str> = path.resolve(None).collect();
        assert_eq!(resolved, vec!["public"]);
    }

    #[test]
    fn test_resolve_no_session_user_entry() {
        let path = SearchPath::parse("myschema, public");
        let resolved: Vec<&str> = path.resolve(Some("alice")).collect();
        assert_eq!(resolved, vec!["myschema", "public"]);
    }

    #[test]
    fn test_resolve_quoted_user_not_expanded() {
        // Quoted "$user" should stay as literal "$user", not expand to session user
        let path = SearchPath::parse(r#""$user", public"#);
        let resolved: Vec<&str> = path.resolve(Some("alice")).collect();
        assert_eq!(resolved, vec!["$user", "public"]);
    }

    fn mutates_any(sql: &str) -> bool {
        pg_query::parse_raw_scoped(sql, |tree| unsafe { search_path_mutations_raw(tree) })
            .expect("parse SQL")
            .any
    }

    fn single_piggybackable(sql: &str) -> Option<MutationKind> {
        pg_query::parse_raw_scoped(sql, |tree| unsafe { search_path_mutations_raw(tree) })
            .expect("parse SQL")
            .single_piggybackable
    }

    #[test]
    fn test_mutates_any_set_search_path() {
        assert!(mutates_any("SET search_path = myapp, public"));
        assert!(mutates_any("SET search_path TO myapp"));
        assert!(mutates_any("SET LOCAL search_path = myapp"));
        assert!(mutates_any("SET SESSION search_path = myapp"));
        assert!(mutates_any("SET search_path TO DEFAULT"));
        // Name comparison is case-insensitive.
        assert!(mutates_any("SET Search_Path = myapp"));
    }

    #[test]
    fn test_mutates_any_reset() {
        assert!(mutates_any("RESET search_path"));
        assert!(mutates_any("RESET ALL"));
    }

    #[test]
    fn test_mutates_any_discard() {
        assert!(mutates_any("DISCARD ALL"));
        assert!(!mutates_any("DISCARD TEMP"));
        assert!(!mutates_any("DISCARD PLANS"));
    }

    #[test]
    fn test_mutates_any_txn_end() {
        assert!(mutates_any("COMMIT"));
        assert!(mutates_any("ROLLBACK"));
        assert!(mutates_any("END"));
        assert!(mutates_any("ABORT"));
    }

    #[test]
    fn test_mutates_any_ignored_statements() {
        assert!(!mutates_any("SELECT 1"));
        assert!(!mutates_any("SET work_mem = 1"));
        assert!(!mutates_any("RESET work_mem"));
        assert!(!mutates_any("BEGIN"));
        assert!(!mutates_any("SAVEPOINT sp"));
        assert!(!mutates_any("ROLLBACK TO SAVEPOINT sp"));
    }

    #[test]
    fn test_mutates_any_multi_statement() {
        assert!(mutates_any("SELECT 1; COMMIT"));
        assert!(mutates_any("COMMIT; SET search_path = x"));
    }

    #[test]
    fn test_mutates_single_piggybackable_only_single_statement() {
        assert_eq!(single_piggybackable("COMMIT"), Some(MutationKind::TxnEnd));
        assert_eq!(
            single_piggybackable("SET search_path = x"),
            Some(MutationKind::SearchPathSet)
        );
        // DISCARD ALL can't run in an implicit multi-statement txn, so it is
        // detected by mutates_any (see test above) but never piggybackable.
        assert_eq!(single_piggybackable("DISCARD ALL"), None);
        // Multi-statement: ineligible even if it contains a mutation.
        assert_eq!(single_piggybackable("SELECT 1; COMMIT"), None);
        assert_eq!(single_piggybackable("COMMIT; SET search_path = x"), None);
    }
}
