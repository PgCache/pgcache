//! Session-term substitution on the rope (PGC-113): each `current_user`,
//! `session_user` and `current_setting(…)` literal becomes the session's
//! value, in one copy of the tree. A tree with no term comes back by
//! refcount.

use ecow::EcoString;
use error_set::error_set;

use crate::query::ast::rope::{
    NodeKind, NodeRef, QueryTree, Replacement, Replacements, TreeBuilder, View,
};
use crate::query::ast::{LiteralValue, NodeId, SessionTerm};

/// A custom setting's value as the session holds it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CustomSettingValue {
    /// Not sampled yet: a probe is on its way.
    Unknown,
    /// Never set in this session: `current_setting` errors on the name
    /// without `missing_ok`, and is NULL with it.
    Unset,
    Known(EcoString),
}

/// The session values a query's terms are substituted from: the roles in
/// force and the custom settings the query's policies read, in name order
/// so two sessions with the same values hash alike.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Default)]
pub struct SessionTermContext {
    /// The effective role; `None` while unknown.
    pub current_user: Option<EcoString>,
    /// The authenticated role; `None` while unknown.
    pub session_user: Option<EcoString>,
    pub settings: Vec<(EcoString, CustomSettingValue)>,
}

impl SessionTermContext {
    /// The value of `name`, `Unknown` when the context does not list it.
    pub fn setting(&self, name: &str) -> &CustomSettingValue {
        self.settings
            .iter()
            .find(|(setting, _)| setting == name)
            .map_or(&CustomSettingValue::Unknown, |(_, value)| value)
    }
}

error_set! {
    SessionTermError := {
        #[display("the role in force is unknown")]
        UserUnknown,
        #[display("setting {name} is not known")]
        SettingUnknown {
            name: EcoString,
        },
        #[display("setting {name} is not set")]
        SettingUnset {
            name: EcoString,
        },
    }
}

/// Whether any literal of `tree` is a session term.
pub fn query_tree_has_session_term(tree: &QueryTree) -> bool {
    tree.nodes()
        .any(|node| matches!(node.kind, NodeKind::Literal(LiteralValue::SessionTerm(_))))
}

/// The custom setting names `tree` reads.
pub fn query_tree_setting_names(tree: &QueryTree) -> impl Iterator<Item = &EcoString> {
    tree.nodes().filter_map(|node| {
        if let NodeKind::Literal(LiteralValue::SessionTerm(SessionTerm::CurrentSetting {
            name,
            ..
        })) = &node.kind
        {
            Some(name)
        } else {
            None
        }
    })
}

/// `tree` with every session term replaced by its value in `context`.
pub fn query_tree_session_terms_replace(
    tree: &QueryTree,
    context: &SessionTermContext,
) -> Result<QueryTree, SessionTermError> {
    let mut replacements = Replacements::new();
    let root = tree.root().node_ref();
    let end = root.end();
    let mut next = u32::try_from(root.id().index()).unwrap_or(u32::MAX);
    while next < end {
        let position: NodeRef<'_> = root.at_position(NodeId::from_index(next));
        if let NodeKind::Literal(LiteralValue::SessionTerm(term)) = position.kind() {
            let value = term_value(term, context)?;
            replacements.push(position.id(), Replacement::Leaf(NodeKind::Literal(value)));
        }
        next += 1;
    }
    if replacements.is_empty() {
        return Ok(tree.clone());
    }
    let mut builder = TreeBuilder::over(tree);
    builder.copy(root, &mut replacements);
    Ok(builder.finish().0)
}

/// The literal a term stands for in `context`.
fn term_value(
    term: &SessionTerm,
    context: &SessionTermContext,
) -> Result<LiteralValue, SessionTermError> {
    match term {
        SessionTerm::CurrentUser => context
            .current_user
            .clone()
            .map(LiteralValue::String)
            .ok_or(SessionTermError::UserUnknown),
        SessionTerm::SessionUser => context
            .session_user
            .clone()
            .map(LiteralValue::String)
            .ok_or(SessionTermError::UserUnknown),
        SessionTerm::CurrentSetting { name, missing_ok } => match context.setting(name) {
            CustomSettingValue::Known(value) => Ok(LiteralValue::String(value.clone())),
            CustomSettingValue::Unset if *missing_ok => Ok(LiteralValue::Null),
            CustomSettingValue::Unset => Err(SessionTermError::SettingUnset { name: name.clone() }),
            CustomSettingValue::Unknown => {
                Err(SessionTermError::SettingUnknown { name: name.clone() })
            }
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::query_tree_parse;
    use crate::query::ast::rope::Plain;
    use crate::query::ast::rope::deparse::query_deparse_with;

    fn plain(tree: &QueryTree) -> String {
        let mut buf = String::new();
        query_deparse_with(tree.root(), &mut Plain, &mut buf);
        buf
    }

    fn context(user: Option<&str>, settings: &[(&str, CustomSettingValue)]) -> SessionTermContext {
        SessionTermContext {
            current_user: user.map(EcoString::from),
            session_user: user.map(EcoString::from),
            settings: settings
                .iter()
                .map(|(name, value)| (EcoString::from(*name), value.clone()))
                .collect(),
        }
    }

    fn known(value: &str) -> CustomSettingValue {
        CustomSettingValue::Known(value.into())
    }

    #[test]
    fn test_terms_are_replaced_by_the_session_values() {
        let tree = query_tree_parse(
            "SELECT id FROM docs WHERE owner = current_user \
             AND tenant = current_setting('app.tenant') \
             AND region = current_setting('app.region', true)",
        )
        .expect("parse");
        assert!(query_tree_has_session_term(&tree));
        assert_eq!(
            query_tree_setting_names(&tree)
                .map(EcoString::as_str)
                .collect::<Vec<_>>(),
            ["app.tenant", "app.region"]
        );
        let replaced = query_tree_session_terms_replace(
            &tree,
            &context(
                Some("alice"),
                &[
                    ("app.tenant", known("7")),
                    ("app.region", CustomSettingValue::Unset),
                ],
            ),
        )
        .expect("every term known");
        assert_eq!(
            plain(&replaced),
            "SELECT id FROM docs WHERE owner = 'alice' AND tenant = '7' AND region = NULL"
        );
        assert!(!query_tree_has_session_term(&replaced));
    }

    fn error_of(tree: &QueryTree, context: &SessionTermContext) -> String {
        match query_tree_session_terms_replace(tree, context) {
            Ok(_) => panic!("a term cannot be substituted"),
            Err(error) => error.to_string(),
        }
    }

    #[test]
    fn test_unknown_or_unset_terms_are_errors() {
        let tree =
            query_tree_parse("SELECT id FROM docs WHERE tenant = current_setting('app.tenant')")
                .expect("parse");
        assert_eq!(
            error_of(&tree, &context(Some("alice"), &[])),
            "setting app.tenant is not known"
        );
        assert_eq!(
            error_of(
                &tree,
                &context(Some("alice"), &[("app.tenant", CustomSettingValue::Unset)])
            ),
            "setting app.tenant is not set"
        );
        let tree =
            query_tree_parse("SELECT id FROM docs WHERE owner = session_user").expect("parse");
        assert_eq!(
            error_of(&tree, &context(None, &[])),
            "the role in force is unknown"
        );
    }

    #[test]
    fn test_tree_without_terms_is_returned_unchanged() {
        let tree = query_tree_parse("SELECT id FROM docs WHERE id = 1").expect("parse");
        assert!(!query_tree_has_session_term(&tree));
        let same = query_tree_session_terms_replace(&tree, &context(None, &[])).expect("no term");
        assert_eq!(plain(&same), plain(&tree));
    }
}
