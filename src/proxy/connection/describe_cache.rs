use std::num::NonZeroUsize;

use tokio_util::bytes::{BufMut, Bytes, BytesMut};

use super::ConnectionState;
use crate::oid::TypeOid;
use crate::pg::protocol::{
    ByteString,
    encode::{NO_DATA_MSG, PARSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG},
    extended::parse_parameter_description,
    session::StatementType,
};

/// Bounded per connection so dynamic-SQL workloads can't grow it unbounded.
pub(super) const DESCRIBE_CACHE_CAPACITY: NonZeroUsize = NonZeroUsize::new(256).unwrap();

/// A given SQL can have different `ParameterDescription` responses depending
/// on the OID hints the client supplied in its `Parse` message, so both go
/// into the key.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(super) struct DescribeKey {
    pub(super) sql: ByteString,
    pub(super) parameter_oids: Vec<TypeOid>,
}

/// `row_description` is `None` when origin returned `NoData`.
#[derive(Debug, Clone)]
pub(super) struct DescribeCacheEntry {
    pub(super) parameter_description: Bytes,
    pub(super) row_description: Option<Bytes>,
    /// Origin-resolved parameter OIDs, parsed once from `parameter_description`
    /// at populate time (`None` if it didn't parse).
    pub(super) parameter_oids: Option<Vec<TypeOid>>,
    /// Pre-assembled ParseComplete + ParameterDescription + (RowDescription |
    /// NoData) + ReadyForQuery('I') — a synth hit serves a refcount clone of
    /// this instead of building the response per hit.
    pub(super) describe_response: Bytes,
}

impl ConnectionState {
    /// Populate `describe_cache` from a freshly-Described statement. No-op for
    /// non-cacheable statements and for statements where origin errored before
    /// returning a parameter description.
    pub(super) fn describe_cache_populate(&mut self, stmt_name: &str) {
        let Some(stmt) = self.prepared_statements.get(stmt_name) else {
            return;
        };
        if !matches!(stmt.sql_type, StatementType::Cacheable(_)) {
            return;
        }
        let Some(parameter_description) = stmt.parameter_description.clone() else {
            return;
        };
        let key = DescribeKey {
            sql: stmt.sql.clone(),
            parameter_oids: stmt.client_parameter_oids.clone(),
        };
        let row_description = stmt.row_description.clone();
        let mut describe_response = BytesMut::with_capacity(
            PARSE_COMPLETE_MSG.len()
                + parameter_description.len()
                + row_description
                    .as_ref()
                    .map_or(NO_DATA_MSG.len(), Bytes::len)
                + READY_FOR_QUERY_IDLE_MSG.len(),
        );
        describe_response.put_slice(PARSE_COMPLETE_MSG);
        describe_response.put_slice(&parameter_description);
        match &row_description {
            Some(row_desc) => describe_response.put_slice(row_desc),
            None => describe_response.put_slice(NO_DATA_MSG),
        }
        // RFQ('I') is safe to bake in: the synth path only fires outside a
        // transaction (synth_eligible).
        describe_response.put_slice(READY_FOR_QUERY_IDLE_MSG);
        let entry = DescribeCacheEntry {
            parameter_oids: parse_parameter_description(&parameter_description)
                .ok()
                .map(|p| p.parameter_oids),
            parameter_description,
            row_description,
            describe_response: describe_response.freeze(),
        };
        let was_at_capacity = self.describe_cache.len() == DESCRIBE_CACHE_CAPACITY.get();
        let replaced = self.describe_cache.put(key, entry).is_some();
        if !replaced && was_at_capacity {
            crate::metrics::handles()
                .conn
                .describe_evictions
                .increment(1);
        }
    }
}
