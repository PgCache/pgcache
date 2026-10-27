//! The proxy's part of row-level security (PGC-113): a read naming a
//! relation origin applies policies to is keyed on the session values those
//! policies read, so the writer inlines them per tenant and role and two
//! sessions with the same values share one entry. The proxy resolves no
//! name and applies no policy; it only widens the key.

use std::sync::Arc;

use ecow::EcoString;

use super::ConnectionState;
use crate::query::ast::rope::QueryTree;
use crate::query::keying_settings::{KeyingSettings, RlsKeying};

impl ConnectionState {
    /// `keying` with the RLS context added when `tree` names a relation
    /// with row-level security: the effective role, `row_security`, and the
    /// custom settings the relation's policies read, which the connection
    /// tracks from now on. Any other read keeps its keying as it is.
    pub(super) fn rls_keying_apply(
        &mut self,
        keying: Arc<KeyingSettings>,
        tree: &QueryTree,
        in_block: bool,
    ) -> Arc<KeyingSettings> {
        let Some(relations) = self.dispatch_handle.rls_relations() else {
            return keying;
        };
        if relations.is_empty() {
            return keying;
        }
        let mut secured = false;
        let mut names: Vec<EcoString> = Vec::new();
        for (_, table) in tree.table_names() {
            if let Some(settings) = relations.settings_of(table) {
                secured = true;
                names.extend(settings.iter().cloned());
            }
        }
        if !secured {
            return keying;
        }
        names.sort();
        names.dedup();
        self.session_settings.custom_require(&names);
        let rls = RlsKeying {
            terms: self.session_settings.rls_context(&names, in_block),
            row_security: self.session_settings.row_security(in_block),
        };
        Arc::new(KeyingSettings {
            search_path: Arc::clone(&keying.search_path),
            literals: keying.literals.clone(),
            rls: Some(Arc::new(rls)),
        })
    }
}
