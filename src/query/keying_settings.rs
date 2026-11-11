//! The session inputs a cacheable read is keyed under (PGC-405, PGC-287):
//! the resolved search_path that decides which table a bare name is, and the
//! literal settings that decide what a date-time literal means. Built once
//! per read on the connection and carried, unchanged, through dispatch, the
//! writer's resolve and register commands, and the read-after-write gate.
//! A read over a row-level-secured relation also carries the policies' term
//! fragments folded under the session (PGC-113), so each tenant and role
//! keys apart; a read with session terms of its own carries them folded the
//! same way (PGC-580).

use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;

use ecow::EcoString;

use crate::cache::QueryParameters;
use crate::oid::Oid;
use crate::query::SessionKey;
use crate::query::ast::rope::QueryTree;
use crate::query::ast::{query_tree_session_key, query_tree_session_key_with_binds};
use crate::query::literal_settings::LiteralSettings;
use crate::query::transform::FoldedTerm;

/// What a read over a row-level-secured relation is keyed under besides its
/// text (PGC-113): the role in force, `row_security`, and each secured
/// relation's policy fragments folded under the session. The writer inlines
/// the policies under these values, so two reads with equal values share
/// the inlined entry whatever else their settings differ in.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RlsKeying {
    /// The effective role; `None` while unknown.
    pub current_user: Option<EcoString>,
    /// `row_security` as on/off; `None` while unknown.
    pub row_security: Option<bool>,
    /// The secured relations the read names, by relation oid.
    pub relations: Vec<RlsRelationFold>,
}

/// One secured relation's policies folded for a read, one per published
/// policy.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RlsRelationFold {
    pub relation_oid: Oid,
    /// The publication the fragments were taken from; the writer refuses a
    /// fold of another version, and the read forwards once.
    pub version: u64,
    pub policies: Vec<RlsPolicyFold>,
}

/// One policy's fragments folded under the session, in pre-order, paired
/// back to the policy by name.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RlsPolicyFold {
    pub name: EcoString,
    pub values: Vec<FoldedTerm>,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyingSettings {
    /// Resolved, `$user` expanded (ADR-006).
    pub search_path: Arc<[EcoString]>,
    pub literals: LiteralSettings,
    /// Set for a read over a relation with row-level security.
    pub rls: Option<Arc<RlsKeying>>,
    /// The query's own session-term fragments folded under the session's
    /// values, in pre-order (PGC-580): part of the key, and what the writer
    /// substitutes before binding. Empty for a query reading no term.
    pub term_values: Vec<FoldedTerm>,
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

    /// `key` with the RLS context and the folded term values folded in,
    /// when the read has either.
    fn rls_fold(&self, key: SessionKey) -> SessionKey {
        if self.rls.is_none() && self.term_values.is_empty() {
            return key;
        }
        let mut hasher = DefaultHasher::new();
        key.get().hash(&mut hasher);
        self.rls.hash(&mut hasher);
        self.term_values.hash(&mut hasher);
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
    use crate::query::ast::{LiteralValue, query_tree_parse};
    use crate::query::transform::SessionTermError;

    fn keying(rls: Option<RlsKeying>) -> KeyingSettings {
        KeyingSettings {
            search_path: Arc::from([EcoString::from("public")]),
            literals: LiteralSettings::default(),
            rls: rls.map(Arc::new),
            term_values: Vec::new(),
        }
    }

    /// The folded term values key the read: equal values key alike, a
    /// different value or none keys apart.
    #[test]
    fn test_term_values_fold_into_the_session_key() {
        let tree = query_tree_parse(
            "SELECT id FROM docs WHERE tenant = current_setting('app.tenant')::int",
        )
        .expect("parse");
        let with = |values: Vec<FoldedTerm>| KeyingSettings {
            term_values: values,
            ..keying(None)
        };
        let tenant = |value: &str| {
            FoldedTerm::Value(LiteralValue::StringWithCast(value.into(), "int4".into()))
        };
        let key = with(vec![tenant("7")]).session_key(&tree);
        assert_eq!(key, with(vec![tenant("7")]).session_key(&tree));
        assert_ne!(key, with(vec![tenant("8")]).session_key(&tree));
        assert_ne!(
            key,
            with(vec![FoldedTerm::Terms(vec![LiteralValue::String(
                "7".into()
            )])])
            .session_key(&tree)
        );
        assert_ne!(key, keying(None).session_key(&tree));
    }

    fn rls(user: &str, tenant: FoldedTerm, row_security: Option<bool>) -> RlsKeying {
        RlsKeying {
            current_user: Some(user.into()),
            row_security,
            relations: vec![RlsRelationFold {
                relation_oid: Oid::from_raw(16384),
                version: 1,
                policies: vec![RlsPolicyFold {
                    name: "tenant_policy".into(),
                    values: vec![tenant],
                }],
            }],
        }
    }

    /// The RLS fold keys the read: equal folds key alike, and a different
    /// role, folded value, publication or `row_security` keys apart.
    #[test]
    fn test_rls_fold_keys_the_session_key() {
        let tree = query_tree_parse("SELECT id FROM docs").expect("parse");
        let known = |value: &str| FoldedTerm::Value(LiteralValue::String(value.into()));
        let plain = keying(None).session_key(&tree);
        let alice_7 = keying(Some(rls("alice", known("7"), Some(true)))).session_key(&tree);
        assert_ne!(plain, alice_7);
        assert_eq!(
            alice_7,
            keying(Some(rls("alice", known("7"), Some(true)))).session_key(&tree)
        );
        let mut republished = rls("alice", known("7"), Some(true));
        republished.relations[0].version = 2;
        for other in [
            rls("bob", known("7"), Some(true)),
            rls("alice", known("8"), Some(true)),
            rls("alice", FoldedTerm::Value(LiteralValue::Null), Some(true)),
            rls(
                "alice",
                FoldedTerm::Unavailable(SessionTermError::SettingUnset {
                    name: "app.tenant".into(),
                }),
                Some(true),
            ),
            rls("alice", known("7"), Some(false)),
            republished,
        ] {
            assert_ne!(alice_7, keying(Some(other)).session_key(&tree));
        }
    }
}
