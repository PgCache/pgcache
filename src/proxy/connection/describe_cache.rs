use tokio_util::bytes::{BufMut, Bytes, BytesMut};

use crate::pg::protocol::{
    ByteString,
    encode::{NO_DATA_MSG, PARSE_COMPLETE_MSG, READY_FOR_QUERY_IDLE_MSG},
    extended::{StatementType, parse_parameter_description},
};

use super::*;

/// A given SQL can have different `ParameterDescription` responses depending
/// on the OID hints the client supplied in its `Parse` message, so both go
/// into the key.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub(in crate::proxy::connection) struct DescribeKey {
    pub(in crate::proxy::connection) sql: ByteString,
    pub(in crate::proxy::connection) parameter_oids: Vec<u32>,
}

/// `row_description` is `None` when origin returned `NoData`.
#[derive(Debug, Clone)]
pub(in crate::proxy::connection) struct DescribeCacheEntry {
    pub(in crate::proxy::connection) parameter_description: Bytes,
    pub(in crate::proxy::connection) row_description: Option<Bytes>,
    /// Origin-resolved parameter OIDs, parsed once from `parameter_description`
    /// at populate time (`None` if it didn't parse).
    pub(in crate::proxy::connection) parameter_oids: Option<Vec<u32>>,
    /// Pre-assembled ParseComplete + ParameterDescription + (RowDescription |
    /// NoData) + ReadyForQuery('I') — a synth hit serves a refcount clone of
    /// this instead of building the response per hit.
    pub(in crate::proxy::connection) describe_response: Bytes,
}

impl ConnectionState {
    /// Populate `describe_cache` from a freshly-Described statement. No-op for
    /// non-cacheable statements and for statements where origin errored before
    /// returning a parameter description.
    pub(in crate::proxy::connection) fn describe_cache_populate(&mut self, stmt_name: &str) {
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
