//! A cacheable read as dispatched: the query as parsed and the bind values
//! it still holds placeholders for. A hit needs the session key and the
//! LIMIT, and both come from the values without substituting; the
//! substituted tree is built for the writer on a miss.

use std::sync::Arc;

use crate::cache::query::{CacheableQuery, QueryParameters};
use crate::query::SessionKey;
use crate::query::ast::{LimitClause, QueryExpr};
use crate::query::keying_settings::KeyingSettings;
use crate::query::transform::{AstTransformResult, literal_value_parameters_replace};

#[derive(Debug)]
pub struct RequestQuery {
    cacheable: Arc<CacheableQuery>,
    /// `None` when the query has no values to substitute.
    parameters: Option<QueryParameters>,
}

impl RequestQuery {
    pub fn new(cacheable: Arc<CacheableQuery>, parameters: QueryParameters) -> Self {
        let parameters = (!parameters.is_empty()).then_some(parameters);
        Self {
            cacheable,
            parameters,
        }
    }

    pub fn unparameterized(cacheable: Arc<CacheableQuery>) -> Self {
        Self {
            cacheable,
            parameters: None,
        }
    }

    /// The query as parsed, placeholders included: what a table-level read
    /// such as the allowlist check needs.
    pub fn query(&self) -> &QueryExpr {
        self.cacheable.query()
    }

    /// The proxy's key for this read under `keying`; the one the alias map
    /// is filled under, so it must match what the connection computed.
    pub fn session_key(&self, keying: &KeyingSettings) -> SessionKey {
        match &self.parameters {
            None => keying.session_key(self.query()),
            Some(parameters) => keying.session_key_with_parameters(self.query(), parameters),
        }
    }

    /// The LIMIT with its bind values in place.
    pub fn limit(&self) -> AstTransformResult<Option<LimitClause>> {
        let Some(clause) = &self.query().limit else {
            return Ok(None);
        };
        let mut clause = clause.clone();
        if let Some(parameters) = &self.parameters {
            if let Some(count) = &mut clause.count {
                literal_value_parameters_replace(count, parameters)?;
            }
            if let Some(offset) = &mut clause.offset {
                literal_value_parameters_replace(offset, parameters)?;
            }
        }
        Ok(Some(clause))
    }

    /// The query with its bind values in place: what the writer resolves
    /// and registers.
    pub fn substituted(&self) -> AstTransformResult<Arc<CacheableQuery>> {
        match &self.parameters {
            None => Ok(Arc::clone(&self.cacheable)),
            Some(parameters) => self.cacheable.parameters_replace(parameters).map(Arc::new),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::*;
    use crate::oid::TypeOid;
    use crate::query::ast::{Deparse, LiteralValue, query_expr_parse};

    fn cacheable(sql: &str) -> Arc<CacheableQuery> {
        let query = query_expr_parse(sql).expect("parse query");
        Arc::new(CacheableQuery::try_new(query, &HashMap::new()).expect("cacheable query"))
    }

    fn int_params(values: &[&[u8]]) -> QueryParameters {
        QueryParameters {
            values: values
                .iter()
                .map(|v| Some(Bytes::copy_from_slice(v)))
                .collect(),
            formats: vec![0; values.len()],
            oids: vec![TypeOid::from_type(&PgType::INT4); values.len()],
        }
    }

    fn empty_params() -> QueryParameters {
        QueryParameters {
            values: Vec::new(),
            formats: Vec::new(),
            oids: Vec::new(),
        }
    }

    #[test]
    fn test_limit_reads_bound_values_without_substituting_the_tree() {
        let request = RequestQuery::new(
            cacheable("SELECT id FROM users WHERE id = $1 LIMIT $2 OFFSET $3"),
            int_params(&[b"7", b"10", b"5"]),
        );
        let limit = request
            .limit()
            .expect("limit decodes")
            .expect("limit present");
        assert_eq!(limit.count, Some(LiteralValue::Integer(10)));
        assert_eq!(limit.offset, Some(LiteralValue::Integer(5)));
        let mut sql = String::new();
        request.query().deparse(&mut sql);
        assert_eq!(sql, "SELECT id FROM users WHERE id = $1 LIMIT $2 OFFSET $3");
    }

    #[test]
    fn test_limit_without_values_is_the_clause_as_written() {
        let request = RequestQuery::new(cacheable("SELECT id FROM users LIMIT 3"), empty_params());
        let limit = request
            .limit()
            .expect("limit decodes")
            .expect("limit present");
        assert_eq!(limit.count, Some(LiteralValue::Integer(3)));
        assert_eq!(limit.offset, None);
        assert!(
            RequestQuery::unparameterized(cacheable("SELECT id FROM users"))
                .limit()
                .expect("limit decodes")
                .is_none()
        );
    }

    #[test]
    fn test_limit_reports_a_value_that_does_not_decode() {
        let request = RequestQuery::new(
            cacheable("SELECT id FROM users LIMIT $1"),
            int_params(&[b"ten"]),
        );
        assert!(request.limit().is_err());
    }

    #[test]
    fn test_substituted_shares_the_query_when_nothing_binds() {
        let query = cacheable("SELECT id FROM users WHERE id = 1");
        let request = RequestQuery::new(Arc::clone(&query), empty_params());
        let substituted = request.substituted().expect("substitute");
        assert!(Arc::ptr_eq(&substituted, &query));
    }

    #[test]
    fn test_substituted_binds_the_values() {
        let request = RequestQuery::new(
            cacheable("SELECT id FROM users WHERE id = $1"),
            int_params(&[b"42"]),
        );
        let substituted = request.substituted().expect("substitute");
        let mut sql = String::new();
        substituted.query().deparse(&mut sql);
        assert_eq!(sql, "SELECT id FROM users WHERE id = 42");
    }

    #[test]
    fn test_session_key_is_the_connection_key() {
        let keying = KeyingSettings::default();
        let query = cacheable("SELECT id FROM users WHERE id = $1");
        let bound = RequestQuery::new(Arc::clone(&query), int_params(&[b"1"]));
        let other = RequestQuery::new(Arc::clone(&query), int_params(&[b"2"]));
        assert_eq!(
            bound.session_key(&keying),
            keying.session_key_with_parameters(query.query(), &int_params(&[b"1"]))
        );
        assert_ne!(bound.session_key(&keying), other.session_key(&keying));
        let plain = RequestQuery::new(Arc::clone(&query), empty_params());
        assert_eq!(
            plain.session_key(&keying),
            keying.session_key(query.query())
        );
    }
}
