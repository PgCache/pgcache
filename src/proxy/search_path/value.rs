//! The parsed `search_path` value: pure, safe string parsing and resolution.
//!
//! The unsafe parse-tree classification of search_path-*mutating* statements
//! lives in the sibling [`mutations`](super::mutations) module.

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
}
