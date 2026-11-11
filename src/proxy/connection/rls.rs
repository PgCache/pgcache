//! The proxy's part of session-dependent reads. A read naming a relation
//! origin applies policies to is keyed on the session values those policies
//! read (PGC-113): the writer publishes each policy's term fragments, the
//! proxy folds them under the session, and the writer inlines the policies
//! with the same literals, so two sessions whose values fold alike share one
//! entry whatever else their settings differ in. A read with session terms
//! of its own (`current_setting(…)`, `current_user`) is keyed on the values
//! its fragments fold to the same way (PGC-580). The proxy resolves no name
//! and applies no policy; it widens the key and folds what it can evaluate
//! itself.

use std::sync::Arc;

use ecow::EcoString;

use super::ConnectionState;
use crate::cache::RlsRelationTerms;
use crate::cache::query::CacheableQuery;
use crate::query::keying_settings::{KeyingSettings, RlsKeying, RlsPolicyFold, RlsRelationFold};
use crate::query::transform::{SessionTermError, query_tree_term_fragments_fold};

impl ConnectionState {
    /// `keying` with the session context added when `query` names a
    /// relation with row-level security or reads a session term: the
    /// effective role, `row_security`, each secured relation's policy
    /// fragments folded, and the query's own fragments folded to literals.
    /// The connection tracks the settings they read from now on. A term of
    /// the query's own the session cannot supply (a setting unknown or
    /// unset, the role unknown) is the reason the read forwards; a policy's
    /// is carried for the writer, which knows whether the policy applies.
    /// Any other read keeps its keying as it is.
    pub(super) fn session_keying_apply(
        &mut self,
        keying: Arc<KeyingSettings>,
        query: &CacheableQuery,
        in_block: bool,
    ) -> Result<Arc<KeyingSettings>, SessionTermError> {
        let own_terms = query.has_session_terms();
        let mut secured: Vec<Arc<RlsRelationTerms>> = Vec::new();
        let mut names: Vec<EcoString> = Vec::new();
        if let Some(relations) = self.dispatch_handle.rls_relations()
            && !relations.is_empty()
        {
            for (_, table) in query.tree().table_names() {
                for relation in relations.relations_of(table).unwrap_or_default() {
                    if secured
                        .iter()
                        .any(|known| known.relation_oid == relation.relation_oid)
                    {
                        continue;
                    }
                    names.extend(relation.settings().iter().cloned());
                    secured.push(Arc::clone(relation));
                }
            }
        }
        if secured.is_empty() && !own_terms {
            return Ok(keying);
        }
        secured.sort_by_key(|relation| relation.relation_oid);
        if own_terms {
            names.extend(query.setting_names().iter().cloned());
        }
        names.sort();
        names.dedup();
        self.session_settings.custom_require(&names);
        let terms = self.session_settings.session_term_context(&names, in_block);
        let term_values = if own_terms {
            query.term_fragments_fold(&terms)?
        } else {
            Vec::new()
        };
        let rls = (!secured.is_empty()).then(|| {
            let relations = secured
                .iter()
                .map(|relation| RlsRelationFold {
                    relation_oid: relation.relation_oid,
                    version: relation.version,
                    policies: relation
                        .policies
                        .iter()
                        .map(|policy| RlsPolicyFold {
                            name: policy.name.clone(),
                            values: query_tree_term_fragments_fold(
                                &policy.tree,
                                &policy.fragments,
                                &terms,
                            ),
                        })
                        .collect(),
                })
                .collect();
            Arc::new(RlsKeying {
                current_user: terms.current_user.clone(),
                row_security: self.session_settings.row_security(in_block),
                relations,
            })
        });
        Ok(Arc::new(KeyingSettings {
            search_path: Arc::clone(&keying.search_path),
            literals: keying.literals.clone(),
            rls,
            term_values,
        }))
    }
}
