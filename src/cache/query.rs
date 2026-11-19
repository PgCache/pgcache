use bytes::Bytes;
use ecow::EcoString;
use error_set::error_set;

use crate::catalog::FunctionFacts;
use crate::oid::TypeOid;
use crate::query::ast::NodeId;
use crate::query::ast::rope::{Body, QueryTree, Select};
use crate::query::transform::{
    AstTransformResult, FunctionInlineError, Inlined, InlinedCall, query_tree_functions_inline,
    query_tree_parameters_replace_remapped, query_tree_setting_names, query_tree_term_fragments,
};
#[cfg(feature = "proxy")]
use crate::query::transform::{
    FoldedTerm, SessionTermContext, SessionTermError, query_tree_term_fragments_fold,
};

mod cacheability;
mod limit;
mod request;

use cacheability::{query_cacheable, references_system_catalog};
#[cfg(feature = "proxy")]
pub(crate) use cacheability::{
    query_tree_has_stable_call, query_tree_has_user_function,
    query_tree_has_volatile_function_except,
};

pub use limit::{RowLimit, limit_is_sufficient, limit_rows_needed};
pub use request::RequestQuery;

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
        /// A STABLE call, or one no catalog row answers, where only an
        /// immutable one is cacheable.
        #[display("Non-immutable function: {function}")]
        NonImmutableFunction {
            function: EcoString,
        },
        /// A VOLATILE call anywhere: its result may differ per call and it
        /// may have side effects, neither of which a replay on the cache
        /// database reproduces (PGC-589).
        #[display("Volatile function: {function}")]
        VolatileFunction {
            function: EcoString,
        },
        /// An operator with a STABLE overload (`@@`, `||`), under the rule
        /// for stable functions.
        #[display("Non-immutable operator: {operator}")]
        NonImmutableOperator {
            operator: EcoString,
        },
        /// A value the session supplies, left unsubstituted: `current_user`
        /// or `current_setting(…)` in the client's own query (PGC-113).
        #[display("Session term: {term}")]
        SessionTerm {
            term: EcoString,
        },
        /// A sql function whose body, inlined, still calls an inlinable
        /// function after the last pass: recursion (PGC-579).
        #[display("Function body nests too deeply: {function}")]
        FunctionInlineDepth {
            function: EcoString,
        },
        HasLimit,
        #[display("System catalog reference: {relation}")]
        SystemCatalogReference {
            relation: EcoString,
        },
    }
}

/// A query that passed the cacheability check, held as the rope (ADR-057).
/// The field is private so `try_new` (and the invariant-preserving
/// `parameters_replace`) are the only ways to construct one — a
/// `CacheableQuery` in hand is proof of validation.
#[derive(Debug, Clone)]
pub struct CacheableQuery {
    tree: QueryTree,
    /// The roots of the session-term fragments (PGC-580), in pre-order:
    /// the proxy folds each to a literal under the session's values and
    /// the writer substitutes them before binding. Found once on the
    /// verdict's tree and carried through parameter substitution, so the
    /// writer replaces exactly what the proxy folded.
    term_fragments: Vec<NodeId>,
    /// The custom settings the fragments read, sorted and distinct.
    setting_names: Vec<EcoString>,
    /// The sql functions whose bodies replaced their calls (PGC-579), in
    /// inlining order: the writer checks each resolves on the session's
    /// search_path to the function inlined and records its oid.
    inlined_calls: Vec<InlinedCall>,
}

impl CacheableQuery {
    pub fn tree(&self) -> &QueryTree {
        &self.tree
    }

    /// Whether the query reads the session (`current_setting`,
    /// `current_user`): it is then keyed on the folded values.
    pub fn has_session_terms(&self) -> bool {
        !self.term_fragments.is_empty()
    }

    /// The roots of the session-term fragments, in pre-order.
    pub fn term_fragments(&self) -> &[NodeId] {
        &self.term_fragments
    }

    /// The custom settings the query's terms read, sorted and distinct.
    pub fn setting_names(&self) -> &[EcoString] {
        &self.setting_names
    }

    /// The calls whose bodies were inlined, in inlining order.
    pub fn inlined_calls(&self) -> &[InlinedCall] {
        &self.inlined_calls
    }

    /// The query's fragments folded under `context`, in pre-order, for the
    /// key and for the writer's substitution; a term the session cannot
    /// supply is the reason the read forwards.
    #[cfg(feature = "proxy")]
    pub(crate) fn term_fragments_fold(
        &self,
        context: &SessionTermContext,
    ) -> Result<Vec<FoldedTerm>, SessionTermError> {
        let values = query_tree_term_fragments_fold(&self.tree, &self.term_fragments, context);
        for value in &values {
            if let FoldedTerm::Unavailable(error) = value {
                return Err(error.clone());
            }
        }
        Ok(values)
    }

    fn from_tree(tree: QueryTree, inlined_calls: Vec<InlinedCall>) -> Self {
        let term_fragments = query_tree_term_fragments(&tree);
        let mut setting_names: Vec<EcoString> = if term_fragments.is_empty() {
            Vec::new()
        } else {
            query_tree_setting_names(&tree).cloned().collect()
        };
        setting_names.sort();
        setting_names.dedup();
        CacheableQuery {
            tree,
            term_fragments,
            setting_names,
            inlined_calls,
        }
    }

    /// The SELECT body of this query, if it is a simple SELECT: `None` for
    /// a set operation (UNION/INTERSECT/EXCEPT) or VALUES clause.
    pub fn as_select(&self) -> Option<Select<'_>> {
        match self.tree.root().body() {
            Body::Select(select) => Some(select),
            Body::SetOp(_) | Body::Values(_) => None,
        }
    }

    /// Substitute bind parameters, producing the per-literal form of this
    /// query. Substitution only replaces `Parameter` literals and
    /// constant-folds, so the result inherits this query's validation. The
    /// fragments follow their nodes to the substituted tree: none holds a
    /// parameter, so each lands in one place.
    pub fn parameters_replace(
        &self,
        parameters: &QueryParameters,
    ) -> AstTransformResult<CacheableQuery> {
        let (tree, remap) = query_tree_parameters_replace_remapped(&self.tree, parameters)?;
        let term_fragments = self
            .term_fragments
            .iter()
            .map(|id| remap.new_id(*id).unwrap_or(*id))
            .collect();
        Ok(CacheableQuery {
            tree,
            term_fragments,
            setting_names: self.setting_names.clone(),
            inlined_calls: self.inlined_calls.clone(),
        })
    }
}

impl CacheableQuery {
    /// Whether a query is cacheable, without consuming it. Lets a caller decide
    /// cacheability before choosing whether to build the (owning) value or run
    /// further analysis on the same borrow.
    ///
    /// Validates the query structure and the volatility of every function
    /// and operator in every clause.
    pub fn cacheable(
        tree: &QueryTree,
        functions: &dyn FunctionFacts,
    ) -> Result<(), CacheabilityError> {
        // System catalogs (pg_catalog, pg_toast, unqualified pg_* relations) can't
        // be logically replicated, so registration against the cache db fails with
        // "unacceptable schema name". Reject up front and forward to origin —
        // e.g. psql's \d, which queries pg_class/pg_namespace.
        references_system_catalog(tree)?;
        query_cacheable(tree.root(), functions)
    }

    /// Build a cacheable query: the inlinable sql function bodies take
    /// their calls' places first (PGC-579), so the verdict and the term
    /// fragments are those of what the body computes; what remains is
    /// validated as [`Self::cacheable`] does.
    pub fn try_new(
        tree: QueryTree,
        functions: &dyn FunctionFacts,
    ) -> Result<Self, CacheabilityError> {
        let inlined = Self::inline(&tree, functions)?;
        Self::validated(
            inlined.unwrap_or(Inlined {
                tree,
                calls: Vec::new(),
            }),
            functions,
        )
    }

    /// [`Self::try_new`] over a borrowed tree, for a caller that keeps the
    /// tree as written when the verdict refuses it.
    pub fn classify(
        tree: &QueryTree,
        functions: &dyn FunctionFacts,
    ) -> Result<Self, CacheabilityError> {
        let inlined = Self::inline(tree, functions)?;
        Self::validated(
            inlined.unwrap_or_else(|| Inlined {
                tree: tree.clone(),
                calls: Vec::new(),
            }),
            functions,
        )
    }

    fn inline(
        tree: &QueryTree,
        functions: &dyn FunctionFacts,
    ) -> Result<Option<Inlined>, CacheabilityError> {
        query_tree_functions_inline(tree, functions).map_err(|e| match e {
            FunctionInlineError::TooDeep { function, .. } => {
                CacheabilityError::FunctionInlineDepth { function }
            }
        })
    }

    fn validated(
        Inlined { tree, calls }: Inlined,
        functions: &dyn FunctionFacts,
    ) -> Result<Self, CacheabilityError> {
        Self::cacheable(&tree, functions)?;
        Ok(CacheableQuery::from_tree(tree, calls))
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

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use postgres_types::Type as PgType;

    use super::*;
    use crate::catalog::FunctionCatalog;
    use crate::query::ast::rope::Plain;
    use crate::query::ast::rope::deparse::query_deparse_with;
    use crate::query::ast::{LiteralValue, query_tree_parse};
    use crate::query::transform::{
        CustomSettingValue, query_tree_term_fragments_fold, query_tree_term_fragments_replace,
    };

    fn params(values: &[(&[u8], PgType)]) -> QueryParameters {
        QueryParameters {
            values: values
                .iter()
                .map(|(v, _)| Some(Bytes::copy_from_slice(v)))
                .collect(),
            formats: vec![0; values.len()],
            oids: values
                .iter()
                .map(|(_, ty)| TypeOid::from_type(ty))
                .collect(),
        }
    }

    fn plain(tree: &QueryTree) -> String {
        let mut buf = String::new();
        query_deparse_with(tree.root(), &mut Plain, &mut buf);
        buf
    }

    /// The fragments the proxy folds on the placeholder tree are the ones
    /// the writer replaces on the substituted tree: a parameter beside a
    /// term keeps the term the fragment, and folded arithmetic ahead of it
    /// moves its position without losing it.
    #[test]
    fn test_term_fragments_follow_parameter_substitution() {
        let tree = query_tree_parse(
            "SELECT id FROM t WHERE id > $1 + 1 \
             AND tenant = coalesce(current_setting('app.tenant', true), $2)",
        )
        .expect("parse");
        let query = CacheableQuery::try_new(tree, &FunctionCatalog::default()).expect("cacheable");
        assert_eq!(query.term_fragments().len(), 1);
        assert_eq!(query.setting_names(), ["app.tenant"]);
        let context = SessionTermContext {
            current_user: None,
            session_user: None,
            settings: vec![("app.tenant".into(), CustomSettingValue::Unset)],
        };
        let values = query.term_fragments_fold(&context).expect("fold");
        assert_eq!(values, [FoldedTerm::Value(LiteralValue::Null)]);

        let bound = query
            .parameters_replace(&params(&[(b"1", PgType::INT4), (b"9", PgType::TEXT)]))
            .expect("bind");
        assert_eq!(bound.setting_names(), ["app.tenant"]);
        let replaced =
            query_tree_term_fragments_replace(bound.tree(), bound.term_fragments(), &values)
                .expect("replace");
        assert_eq!(
            plain(&replaced),
            "SELECT id FROM t WHERE id > 2 AND tenant = COALESCE(NULL, '9')"
        );
        assert_eq!(
            query_tree_term_fragments_fold(bound.tree(), bound.term_fragments(), &context),
            values
        );
    }
}
