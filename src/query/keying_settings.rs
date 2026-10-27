//! The session inputs a cacheable read is keyed under (PGC-405, PGC-287):
//! the resolved search_path that decides which table a bare name is, and the
//! literal settings that decide what a date-time literal means. Built once
//! per read on the connection and carried, unchanged, through dispatch, the
//! writer's resolve and register commands, and the read-after-write gate.
//! A read over a row-level-secured relation also carries the session values
//! its policies read (PGC-113), so each tenant and role keys apart.

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use ecow::EcoString;

use crate::cache::QueryParameters;
use crate::query::SessionKey;
use crate::query::ast::rope::QueryTree;
use crate::query::ast::{query_tree_session_key, query_tree_session_key_with_binds};
use crate::query::literal_settings::LiteralSettings;
use crate::query::transform::SessionTermContext;

/// What a read over a row-level-secured relation is keyed under besides its
/// text (PGC-113): the roles in force, the custom settings the relation's
/// policies read, and `row_security`. The writer inlines the policies under
/// these values, so two reads with equal values share the inlined entry.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RlsKeying {
    pub terms: SessionTermContext,
    /// `row_security` as on/off; `None` while unknown.
    pub row_security: Option<bool>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyingSettings {
    /// Resolved, `$user` expanded (ADR-006).
    pub search_path: Arc<[EcoString]>,
    pub literals: LiteralSettings,
    /// Set for a read over a relation with row-level security.
    pub rls: Option<Arc<RlsKeying>>,
}

impl KeyingSettings {
    /// The proxy-side key of `query` under these settings.
    pub fn session_key(&self, query: &QueryTree) -> SessionKey {
        self.rls_fold(query_tree_session_key(
            query,
            &self.search_path,
            &self.literals,
        ))
    }

    /// `key` with the RLS context folded in, when the read has one.
    fn rls_fold(&self, key: SessionKey) -> SessionKey {
        let Some(rls) = &self.rls else {
            return key;
        };
        let mut hasher = DefaultHasher::new();
        key.get().hash(&mut hasher);
        rls.hash(&mut hasher);
        SessionKey::from_raw(hasher.finish())
    }

    /// The key of a parameterized `query` with `parameters` still to bind.
    pub fn session_key_with_parameters(
        &self,
        query: &QueryTree,
        parameters: &QueryParameters,
    ) -> SessionKey {
        self.rls_fold(query_tree_session_key_with_binds(
            query,
            &(&parameters.values, &parameters.formats, &parameters.oids),
            &self.search_path,
            &self.literals,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::ast::query_tree_parse;
    use crate::query::transform::CustomSettingValue;

    fn keying(rls: Option<RlsKeying>) -> KeyingSettings {
        KeyingSettings {
            search_path: Arc::from([EcoString::from("public")]),
            literals: LiteralSettings::default(),
            rls: rls.map(Arc::new),
        }
    }

    fn rls(user: &str, tenant: CustomSettingValue, row_security: Option<bool>) -> RlsKeying {
        RlsKeying {
            terms: SessionTermContext {
                current_user: Some(user.into()),
                session_user: Some(user.into()),
                settings: vec![("app.tenant".into(), tenant)],
            },
            row_security,
        }
    }

    /// The RLS context folds into the key: equal contexts key alike, and
    /// a different role, setting value or `row_security` keys apart.
    #[test]
    fn test_rls_context_folds_into_the_session_key() {
        let tree = query_tree_parse("SELECT id FROM docs").expect("parse");
        let known = |value: &str| CustomSettingValue::Known(value.into());
        let plain = keying(None).session_key(&tree);
        let alice_7 = keying(Some(rls("alice", known("7"), Some(true)))).session_key(&tree);
        assert_ne!(plain, alice_7);
        assert_eq!(
            alice_7,
            keying(Some(rls("alice", known("7"), Some(true)))).session_key(&tree)
        );
        for other in [
            rls("bob", known("7"), Some(true)),
            rls("alice", known("8"), Some(true)),
            rls("alice", CustomSettingValue::Unset, Some(true)),
            rls("alice", CustomSettingValue::Unknown, Some(true)),
            rls("alice", known("7"), Some(false)),
        ] {
            assert_ne!(alice_7, keying(Some(other)).session_key(&tree));
        }
    }
}
