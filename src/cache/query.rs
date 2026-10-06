use std::collections::HashMap;

use bytes::Bytes;
use ecow::EcoString;
use error_set::error_set;

use crate::catalog::FunctionVolatility;
use crate::oid::TypeOid;
use crate::query::ast::{QueryExpr, SelectNode};
use crate::query::transform::{AstTransformResult, query_expr_parameters_replace};

mod cacheability;
mod limit;
mod outer_join;

pub use cacheability::query_has_volatile_function;
use cacheability::{is_cacheable_body, references_system_catalog};

pub use limit::{limit_is_sufficient, limit_rows_needed};
pub use outer_join::outer_join_optional_tables;

// Each variant names the item that triggered it: pgcache-fit's per-statement
// detail and the proxy's passthrough log both print it (never the SQL body).
error_set! {
    CacheabilityError := {
        #[display("Unsupported query type: {kind}")]
        UnsupportedQueryType {
            kind: &'static str,
        },
        #[display("Unsupported FROM clause: {construct}")]
        UnsupportedFrom {
            construct: &'static str,
        },
        #[display("Unsupported subquery: {kind}")]
        UnsupportedSubquery {
            kind: &'static str,
        },
        #[display("Non-immutable function: {function}")]
        NonImmutableFunction {
            function: EcoString,
        },
        HasLimit,
        #[display("System catalog reference: {relation}")]
        SystemCatalogReference {
            relation: EcoString,
        },
    }
}

/// Type alias for the function volatility map passed through cacheability checks.
type FunctionVolatilityMap = HashMap<EcoString, FunctionVolatility>;

/// A query that passed the cacheability check. The field is private so
/// `try_new` (and the invariant-preserving `parameters_replace`) are the only
/// ways to construct one — a `CacheableQuery` in hand is proof of validation.
#[derive(Debug, Clone)]
pub struct CacheableQuery {
    query: QueryExpr,
}

impl CacheableQuery {
    pub fn query(&self) -> &QueryExpr {
        &self.query
    }

    /// Get the SELECT body of this query, if it is a simple SELECT.
    ///
    /// Returns `Some` if the query body is a SELECT statement, `None` if it's
    /// a set operation (UNION/INTERSECT/EXCEPT) or VALUES clause.
    pub fn as_select(&self) -> Option<&SelectNode> {
        self.query.as_select()
    }

    /// Substitute bind parameters, producing the per-literal form of this
    /// query. Substitution only replaces `Parameter` literals and
    /// constant-folds, so the result inherits this query's validation.
    pub fn parameters_replace(
        &self,
        parameters: &QueryParameters,
    ) -> AstTransformResult<CacheableQuery> {
        query_expr_parameters_replace(&self.query, parameters).map(|query| CacheableQuery { query })
    }
}

impl CacheableQuery {
    /// Whether a query is cacheable, without consuming it. Lets a caller decide
    /// cacheability before choosing whether to build the (owning) value or run
    /// further analysis on the same borrow.
    ///
    /// Validates the query structure and ensures all functions in WHERE/FROM
    /// clauses are immutable. Functions in SELECT lists are always allowed.
    pub fn cacheable(
        query: &QueryExpr,
        fv: &FunctionVolatilityMap,
    ) -> Result<(), CacheabilityError> {
        // System catalogs (pg_catalog, pg_toast, unqualified pg_* relations) can't
        // be logically replicated, so registration against the cache db fails with
        // "unacceptable schema name". Reject up front and forward to origin —
        // e.g. psql's \d, which queries pg_class/pg_namespace.
        references_system_catalog(query)?;
        is_cacheable_body(&query.body, fv)
    }

    /// Build a cacheable query, validating it first. Fails with the same
    /// verdict as [`Self::cacheable`].
    pub fn try_new(
        query: QueryExpr,
        fv: &FunctionVolatilityMap,
    ) -> Result<Self, CacheabilityError> {
        Self::cacheable(&query, fv)?;
        // Take ownership of the (freshly built, ephemeral) query rather than
        // cloning the whole AST — the caller has no further use for it.
        Ok(CacheableQuery { query })
    }
}

#[cfg(test)]
mod tests {
    use crate::query::ast::{query_expr_fingerprint, query_expr_parse};

    #[test]
    fn test_limit_offset_fingerprint_match() {
        let base = "SELECT * FROM orders WHERE tenant_id = 1";
        let with_limit = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10";
        let with_offset = "SELECT * FROM orders WHERE tenant_id = 1 OFFSET 5";
        let with_both = "SELECT * FROM orders WHERE tenant_id = 1 LIMIT 10 OFFSET 5";

        let fp_base = { query_expr_fingerprint(&query_expr_parse(base).unwrap()) };
        let fp_limit = { query_expr_fingerprint(&query_expr_parse(with_limit).unwrap()) };
        let fp_offset = { query_expr_fingerprint(&query_expr_parse(with_offset).unwrap()) };
        let fp_both = { query_expr_fingerprint(&query_expr_parse(with_both).unwrap()) };

        assert_eq!(fp_base, fp_limit, "LIMIT should not affect fingerprint");
        assert_eq!(fp_base, fp_offset, "OFFSET should not affect fingerprint");
        assert_eq!(
            fp_base, fp_both,
            "LIMIT+OFFSET should not affect fingerprint"
        );
    }
}

/// Parameters passed into an extended query
#[derive(Debug)]
pub struct QueryParameters {
    pub values: Vec<Option<Bytes>>,
    pub formats: Vec<i16>,
    pub oids: Vec<TypeOid>,
}

impl QueryParameters {
    pub fn get(&self, index: usize) -> Option<QueryParameter> {
        let value = self.values.get(index)?;

        // Per the extended query protocol, format codes and OIDs may have fewer
        // entries than there are parameters:
        //   0 entries  → apply the default (text format / unspecified OID) to all
        //   1 entry    → apply that single value to all parameters
        //   N entries  → one entry per parameter
        let format = match self.formats.as_slice() {
            [] => 0,
            [single] => *single,
            codes => *codes.get(index)?,
        };
        let oid = match self.oids.as_slice() {
            [] => TypeOid::UNSPECIFIED,
            [single] => *single,
            oids => *oids.get(index)?,
        };

        Some(QueryParameter {
            value: value.clone(),
            format,
            oid,
        })
    }

    pub fn len(&self) -> usize {
        self.values.len()
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

#[derive(Debug)]
pub struct QueryParameter {
    pub value: Option<Bytes>,
    pub format: i16,
    pub oid: TypeOid,
}
