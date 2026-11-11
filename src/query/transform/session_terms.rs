//! Session terms and their values (PGC-113): what the session holds for
//! `current_user`, `session_user` and each custom setting, the value a term
//! stands for under it, and what a fragment over terms folds to for the key
//! and the writer's substitution (`term_eval`).

use ecow::EcoString;
use error_set::error_set;

use crate::query::ast::rope::{NodeKind, QueryTree};
use crate::query::ast::{LiteralValue, SessionTerm};

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
    #[derive(Clone, PartialEq, Eq, Hash)]
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
        /// The session's value is one origin's own evaluation rejects (a
        /// claims setting that is not json): the read is origin's error.
        #[display("the session value is one origin rejects: {reason}")]
        ValueRejected {
            reason: &'static str,
        },
    }
}

/// What a fragment stands for under the session: for the key, and for the
/// writer's substitution.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum FoldedTerm {
    /// The literal the fragment folds to.
    Value(LiteralValue),
    /// The fragment is a shape origin evaluates (a typed `nullif`, a json
    /// value compared whole): its bare terms, in pre-order, are substituted
    /// on their own, the raw values keying the read.
    Terms(Vec<LiteralValue>),
    /// A term the session cannot supply, or a value origin would reject:
    /// the reason the read forwards when the fragment is needed.
    Unavailable(SessionTermError),
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

/// The literal a term stands for in `context`.
#[cfg(feature = "proxy")]
pub(super) fn term_value(
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

#[cfg(all(test, feature = "proxy"))]
mod tests {
    use super::*;
    use crate::query::ast::query_tree_parse;

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

    fn setting(name: &str, missing_ok: bool) -> SessionTerm {
        SessionTerm::CurrentSetting {
            name: name.into(),
            missing_ok,
        }
    }

    #[test]
    fn test_terms_take_the_session_values() {
        let tree = query_tree_parse(
            "SELECT id FROM docs WHERE owner = current_user \
             AND tenant = current_setting('app.tenant') \
             AND region = current_setting('app.region', true)",
        )
        .expect("parse");
        assert_eq!(
            query_tree_setting_names(&tree)
                .map(EcoString::as_str)
                .collect::<Vec<_>>(),
            ["app.tenant", "app.region"]
        );
        let ctx = context(
            Some("alice"),
            &[
                ("app.tenant", known("7")),
                ("app.region", CustomSettingValue::Unset),
            ],
        );
        assert_eq!(
            term_value(&SessionTerm::CurrentUser, &ctx),
            Ok(LiteralValue::String("alice".into()))
        );
        assert_eq!(
            term_value(&setting("app.tenant", false), &ctx),
            Ok(LiteralValue::String("7".into()))
        );
        assert_eq!(
            term_value(&setting("app.region", true), &ctx),
            Ok(LiteralValue::Null)
        );
    }

    fn error_of(term: &SessionTerm, context: &SessionTermContext) -> String {
        term_value(term, context)
            .expect_err("a term the session cannot supply")
            .to_string()
    }

    #[test]
    fn test_unknown_or_unset_terms_are_errors() {
        let tenant = setting("app.tenant", false);
        assert_eq!(
            error_of(&tenant, &context(Some("alice"), &[])),
            "setting app.tenant is not known"
        );
        assert_eq!(
            error_of(
                &tenant,
                &context(Some("alice"), &[("app.tenant", CustomSettingValue::Unset)])
            ),
            "setting app.tenant is not set"
        );
        assert_eq!(
            error_of(&SessionTerm::SessionUser, &context(None, &[])),
            "the role in force is unknown"
        );
    }
}
