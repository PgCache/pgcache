use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
    sync::Arc,
};

use ecow::EcoString;
use tokio_util::bytes::BytesMut;
use tracing::{debug, trace};

use crate::{
    cache::query::CacheableQuery,
    catalog::FunctionVolatility,
    query::ast::{AstError, query_expr_convert_raw},
};

use super::ParseError;

#[derive(Debug, Clone, Copy)]
pub(super) enum ForwardReason {
    UnsupportedStatement,
    UncacheableSelect,
    Invalid,
}

pub(super) enum Action {
    Forward(ForwardReason),
    CacheCheck(Arc<CacheableQuery>),
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn handle_query(
    data: &BytesMut,
    fp_cache: &mut HashMap<u64, Result<Arc<CacheableQuery>, ForwardReason>>,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Result<Action, ParseError> {
    let len_bytes: [u8; 4] = data
        .get(1..5)
        .and_then(|s| s.try_into().ok())
        .ok_or(ParseError::InvalidUtf8)?;
    let msg_len = u32::from_be_bytes(len_bytes) as usize;
    let query = data
        .get(5..msg_len)
        .and_then(|b| str::from_utf8(b).ok())
        .ok_or(ParseError::InvalidUtf8)?;

    let mut hasher = DefaultHasher::new();
    query.hash(&mut hasher);
    let fingerprint = hasher.finish();

    match fp_cache.get(&fingerprint) {
        Some(Ok(cacheable_query)) => {
            trace!("cache hit: cacheable true");
            Ok(Action::CacheCheck(Arc::clone(cacheable_query)))
        }
        Some(Err(reason)) => {
            trace!("cache hit: cacheable false");
            Ok(Action::Forward(*reason))
        }
        None => {
            // Build the QueryExpr straight off the raw parse tree, skipping the
            // protobuf serialize/decode round-trip (PGC-192).
            let convert_result =
                pg_query::parse_raw_scoped(query, |tree| unsafe { query_expr_convert_raw(tree) })?;

            match convert_result {
                Ok(query) => {
                    // Successfully parsed as SELECT
                    match CacheableQuery::try_new(query, func_volatility) {
                        Ok(cacheable_query) => {
                            let cacheable_query = Arc::new(cacheable_query);
                            fp_cache.insert(fingerprint, Ok(Arc::clone(&cacheable_query)));
                            Ok(Action::CacheCheck(cacheable_query))
                        }
                        Err(cacheability_error) => {
                            debug!(%cacheability_error, "uncacheable SELECT");
                            let reason = ForwardReason::UncacheableSelect;
                            fp_cache.insert(fingerprint, Err(reason));
                            Ok(Action::Forward(reason))
                        }
                    }
                }
                Err(ast_error) => {
                    let reason = match &ast_error {
                        AstError::UnsupportedStatement { .. } => {
                            // Not a SELECT statement (INSERT, UPDATE, DELETE, DDL, etc.)
                            ForwardReason::UnsupportedStatement
                        }
                        AstError::UnsupportedSelectFeature { .. }
                        | AstError::UnsupportedFeature { .. }
                        | AstError::UnsupportedJoinType
                        | AstError::UnsupportedSubLinkType { .. }
                        | AstError::WhereParseError(_) => {
                            debug!(%ast_error, "forwarding query: AST conversion failed");
                            ForwardReason::UncacheableSelect
                        }
                        AstError::MultipleStatements
                        | AstError::MissingStatement
                        | AstError::InvalidTableRef => {
                            debug!(%ast_error, "forwarding query: invalid");
                            ForwardReason::Invalid
                        }
                    };

                    fp_cache.insert(fingerprint, Err(reason));

                    Ok(Action::Forward(reason))
                }
            }
        }
    }
}
