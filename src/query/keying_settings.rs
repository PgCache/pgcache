//! The session inputs a cacheable read is keyed under (PGC-405, PGC-287):
//! the resolved search_path that decides which table a bare name is, and the
//! literal settings that decide what a date-time literal means. Built once
//! per read on the connection and carried, unchanged, through dispatch, the
//! writer's resolve and register commands, and the read-after-write gate.

use std::sync::Arc;

use ecow::EcoString;

use crate::cache::QueryParameters;
use crate::query::SessionKey;
use crate::query::ast::{QueryExpr, query_expr_session_key, query_expr_session_key_with_binds};
use crate::query::literal_settings::LiteralSettings;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct KeyingSettings {
    /// Resolved, `$user` expanded (ADR-006).
    pub search_path: Arc<[EcoString]>,
    pub literals: LiteralSettings,
}

impl KeyingSettings {
    /// The proxy-side key of `query` under these settings.
    pub fn session_key(&self, query: &QueryExpr) -> SessionKey {
        query_expr_session_key(query, &self.search_path, &self.literals)
    }

    /// The key of a parameterized `query` with `parameters` still to bind.
    pub fn session_key_with_parameters(
        &self,
        query: &QueryExpr,
        parameters: &QueryParameters,
    ) -> SessionKey {
        query_expr_session_key_with_binds(
            query,
            &(&parameters.values, &parameters.formats, &parameters.oids),
            &self.search_path,
            &self.literals,
        )
    }
}
