//! The proxy's part of session-dependent reads. A read naming a relation
//! origin applies policies to is keyed on the session values those policies
//! read (PGC-113), so the writer inlines them per tenant and role and two
//! sessions with the same values share one entry. A read with session terms
//! of its own (`current_setting(…)`, `current_user`) is keyed on the values
//! its fragments fold to (PGC-580), the writer substituting the same
//! literals before binding. The proxy resolves no name and applies no
//! policy; it widens the key and folds what it can evaluate itself.

use std::sync::Arc;

use ecow::EcoString;

use super::ConnectionState;
use crate::cache::query::CacheableQuery;
use crate::query::keying_settings::{KeyingSettings, RlsKeying};
use crate::query::transform::TermFoldError;

impl ConnectionState {
    /// `keying` with the session context added when `query` names a
    /// relation with row-level security or reads a session term: the
    /// effective role, `row_security`, the custom settings the relation's
    /// policies read, and the query's own fragments folded to literals. The
    /// connection tracks the settings from now on. A fragment the session
    /// cannot supply a value for (a setting unknown or unset, the role
    /// unknown) is the reason the read forwards. Any other read keeps its
    /// keying as it is.
    pub(super) fn session_keying_apply(
        &mut self,
        keying: Arc<KeyingSettings>,
        query: &CacheableQuery,
        in_block: bool,
    ) -> Result<Arc<KeyingSettings>, TermFoldError> {
        let own_terms = query.has_session_terms();
        let mut secured = false;
        let mut names: Vec<EcoString> = Vec::new();
        if let Some(relations) = self.dispatch_handle.rls_relations()
            && !relations.is_empty()
        {
            for (_, table) in query.tree().table_names() {
                if let Some(settings) = relations.settings_of(table) {
                    secured = true;
                    names.extend(settings.iter().cloned());
                }
            }
        }
        if !secured && !own_terms {
            return Ok(keying);
        }
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
        let rls = secured.then(|| {
            Arc::new(RlsKeying {
                terms,
                row_security: self.session_settings.row_security(in_block),
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
