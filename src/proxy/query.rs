use std::{collections::HashMap, sync::Arc};

use ecow::EcoString;
use tokio_util::bytes::BytesMut;
use tracing::debug;

use super::ParseError;
use super::cacheability_memo::{CacheabilityCache, SqlTextHash};
use super::explain::{ExplainSpec, explain_spec_extract};
use crate::{
    cache::query::{CacheableQuery, query_tree_has_volatile_function},
    catalog::FunctionVolatility,
    query::ast::rope::{Function, NodeKind, QueryTree, View},
    query::ast::{AstError, LiteralValue, RawStatement, statement_convert_raw},
    query::guc::Guc,
    query::write::{
        CustomSettingEffect, CustomSettingEffects, IsolationEffect, SettingsEffect,
        StatementEffects, WriteClass,
    },
};

/// The settings effect of the `set_config` calls in a converted statement:
/// each call with a literal name touches that setting (an untracked name is
/// a custom setting, with its value when that is a literal too), scoped by
/// a literal `is_local`; a call
/// whose name is not a literal may change anything. The caller only asks
/// when the text mentions `set_config`, so finding no call means it hides
/// somewhere the converter does not model and the answer is likewise
/// everything.
fn set_config_calls_effect(tree: &QueryTree) -> (SettingsEffect, CustomSettingEffects) {
    let mut effect = SettingsEffect::NONE;
    let mut custom = CustomSettingEffects::new();
    let mut calls = 0usize;
    for position in tree.root().node_ref().subtree() {
        let NodeKind::Function { name, .. } = position.kind() else {
            continue;
        };
        if !name.eq_ignore_ascii_case("set_config") {
            continue;
        }
        calls += 1;
        let mut args = Function::from_ref(position).args();
        let Some(LiteralValue::String(name)) = args.next().and_then(|arg| arg.literal()) else {
            return (SettingsEffect::all(), CustomSettingEffects::new());
        };
        let value = args.next().and_then(|arg| arg.literal()).cloned();
        let local = args.next().and_then(|arg| arg.literal()) == Some(&LiteralValue::Boolean(true));
        if let Some(guc) = Guc::parse(name.as_str()) {
            effect = effect.union(SettingsEffect::guc(guc, local));
        } else {
            custom.push(CustomSettingEffect {
                name: name.to_ascii_lowercase(),
                value: match value {
                    Some(LiteralValue::String(value)) => Some(value),
                    _ => None,
                },
                local,
            });
        }
    }
    if calls == 0 {
        return (SettingsEffect::all(), CustomSettingEffects::new());
    }
    (effect, custom)
}

#[derive(Debug, Clone, Copy)]
pub(super) enum ForwardReason {
    UnsupportedStatement,
    UncacheableSelect,
    Invalid,
}

/// Map an AST conversion failure to its forward reason (metric bucket).
fn ast_error_forward_reason(ast_error: &AstError) -> ForwardReason {
    match ast_error {
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
        AstError::MultipleStatements | AstError::MissingStatement | AstError::InvalidTableRef => {
            debug!(%ast_error, "forwarding query: invalid");
            ForwardReason::Invalid
        }
    }
}

/// Verdict from [`analyze`]. Cloned cheaply from the memo on a hit (`CacheCheck`
/// and `Explain` are `Arc`s; `Forward` is `Copy`).
#[derive(Clone)]
pub(super) enum Action {
    /// A forwarded statement, with everything it does to the connection's
    /// tracked state (write log, isolation, block boundary).
    Forward(ForwardReason, StatementEffects),
    CacheCheck(Arc<CacheableQuery>),
    /// `SELECT pgcache_explain(...)` — route to the cache to explain a cached
    /// query's cache-side plan rather than forward to origin (PGC-345).
    Explain(Arc<ExplainSpec>),
}

#[cfg_attr(feature = "hotpath", hotpath::measure)]
pub(super) async fn handle_query(
    data: &BytesMut,
    cacheability_cache: &mut CacheabilityCache,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Result<Action, ParseError> {
    let query = query_sql_extract(data).ok_or(ParseError::InvalidUtf8)?;
    analyze(query, cacheability_cache, func_volatility)
}

/// Whether the statement text contains a `set_config` call (case-insensitive),
/// which can change any GUC — including the isolation default — from inside a
/// query.
fn sql_mentions_set_config(sql: &str) -> bool {
    const NEEDLE: &[u8] = b"set_config";
    sql.as_bytes()
        .windows(NEEDLE.len())
        .any(|window| window.eq_ignore_ascii_case(NEEDLE))
}

/// Extract the SQL text from a simple-query (`Q`) frame: tag (1) + length (4) +
/// the null-terminated query string. Returns `None` on a malformed frame or
/// non-UTF-8 body.
pub(super) fn query_sql_extract(data: &BytesMut) -> Option<&str> {
    let len_bytes: [u8; 4] = data.get(1..5).and_then(|s| s.try_into().ok())?;
    let msg_len = u32::from_be_bytes(len_bytes) as usize;
    data.get(5..msg_len).and_then(|b| str::from_utf8(b).ok())
}

/// Cacheability analysis for a SQL string, memoized on a hash of the text. On a
/// hit the parse/convert/classify work is skipped entirely. Shared by the
/// simple-query and extended (Parse) paths.
///
/// Two tiers. The per-connection LRU is the hot path and needs no
/// synchronization. On a miss the shared [`CacheabilityStore`] is consulted,
/// which costs a sharded-map lookup and an `Arc` clone rather than a
/// re-analysis; only a miss in both tiers parses. The verdict is interned there
/// so every connection shares one payload.
pub(super) fn analyze(
    sql: &str,
    cacheability_cache: &mut CacheabilityCache,
    func_volatility: &HashMap<EcoString, FunctionVolatility>,
) -> Result<Action, ParseError> {
    cacheability_cache.epoch_reconcile();

    let key = SqlTextHash::of(sql);
    if let Some(action) = cacheability_cache.lookup(key) {
        return Ok(action);
    }

    // Build the tree straight off the raw parse tree, skipping the protobuf
    // serialize/decode round-trip (PGC-192). This is the only parse/convert; the
    // explain interception, cacheability classification, and write
    // classification (PGC-124) all read this one pass.
    let convert_result = pg_query::parse_raw_scoped(sql, |tree| unsafe {
        statement_convert_raw(tree, sql.len())
    })?;

    // `set_config('default_transaction_isolation', ...)` can hide in any
    // forwarded statement (a SELECT, a DO block, a CALL); a text scan is the
    // conservative catch-all, and a false positive only costs a re-probe.
    let mentions_set_config = sql_mentions_set_config(sql);
    let isolation = if mentions_set_config {
        IsolationEffect::SessionUnknown
    } else {
        IsolationEffect::None
    };
    // Same scan for the tracked session settings (PGC-287); a converted SELECT
    // narrows it to the settings its literal `set_config` names below.
    let mut settings = if mentions_set_config {
        SettingsEffect::all()
    } else {
        SettingsEffect::NONE
    };
    let mut custom_settings = CustomSettingEffects::new();

    let mut action = match convert_result {
        Ok(RawStatement::Select {
            converted: Ok(query),
            ..
        }) => {
            if mentions_set_config {
                (settings, custom_settings) = set_config_calls_effect(&query);
            }
            // `SELECT pgcache_explain(...)` is intercepted before cacheability
            // classification (it would otherwise be an uncacheable unknown
            // function) and routed to the cache to explain a cached plan.
            if let Some(spec) = explain_spec_extract(&query) {
                Action::Explain(Arc::new(spec))
            } else {
                match CacheableQuery::cacheable(&query, func_volatility) {
                    // `cacheable` just passed, so `try_new` (same validation)
                    // cannot fail; forward conservatively if it ever disagrees.
                    Ok(()) => match CacheableQuery::try_new(query, func_volatility) {
                        Ok(cacheable_query) => Action::CacheCheck(Arc::new(cacheable_query)),
                        Err(_) => Action::Forward(
                            ForwardReason::UncacheableSelect,
                            StatementEffects::read_only(isolation, None),
                        ),
                    },
                    Err(cacheability_error) => {
                        debug!(%cacheability_error, "uncacheable SELECT");
                        // A volatile (or unknown) function anywhere in the query
                        // can modify the database, so an uncacheable SELECT
                        // carrying one is a potential write (PGC-124). Scanned
                        // only now that the query is known uncacheable — the
                        // cacheable path never pays for it.
                        let effects = if query_tree_has_volatile_function(&query, func_volatility) {
                            StatementEffects::write(WriteClass::Connection, isolation)
                        } else {
                            StatementEffects::read_only(isolation, None)
                        };
                        Action::Forward(ForwardReason::UncacheableSelect, effects)
                    }
                }
            }
        }
        Ok(RawStatement::Select {
            converted: Err(ast_error),
            cte_write,
        }) => {
            let reason = ast_error_forward_reason(&ast_error);
            match cte_write {
                // A data-modifying CTE: the "select" writes.
                Some(class) => Action::Forward(reason, StatementEffects::write(class, isolation)),
                // No converted tree to scan for volatile (writing) functions,
                // and a partial raw-tree scan would be unsound in exactly the
                // way it is meant to prevent — so the failure direction is
                // "assume it wrote", the same policy as a parse failure
                // (PGC-449). Conversion failures are rare, exotic SELECTs;
                // the converted-but-uncacheable arm keeps the precise scan.
                None => Action::Forward(
                    reason,
                    StatementEffects::write(WriteClass::Connection, isolation),
                ),
            }
        }
        Ok(RawStatement::Write(class)) => Action::Forward(
            ForwardReason::UnsupportedStatement,
            StatementEffects::write(class, isolation),
        ),
        Ok(RawStatement::ReadOnlyUtility {
            transaction,
            isolation: statement_isolation,
            settings: statement_settings,
            custom_settings: statement_custom_settings,
        }) => Action::Forward(
            ForwardReason::UnsupportedStatement,
            // Only a whitelisted utility carries a parsed effect; the
            // set_config scan still applies (a SET can't call it, so this is
            // just the general rule).
            StatementEffects::read_only(
                if statement_isolation == IsolationEffect::None {
                    isolation
                } else {
                    statement_isolation
                },
                transaction,
            )
            .with_settings(statement_settings)
            .with_custom_settings(statement_custom_settings),
        ),
        Err(ast_error) => {
            let reason = ast_error_forward_reason(&ast_error);
            // Any statement of a multi-statement batch can write.
            let effects = if matches!(ast_error, AstError::MultipleStatements) {
                StatementEffects::write(WriteClass::Connection, isolation)
            } else {
                StatementEffects::read_only(isolation, None)
            };
            Action::Forward(reason, effects)
        }
    };
    if let Action::Forward(_, effects) = &mut action {
        effects.settings = effects.settings.union(settings);
        effects.custom_settings.extend(custom_settings);
    }

    // Don't memoize writes: DML text is almost always unique (fresh literals
    // per statement), so a write entry would rarely be re-hit, and its class
    // can carry a per-statement payload (INSERT rows). Interning them would
    // grow the per-connection memo and the shared store with data they never
    // serve. Reads intern as before.
    if matches!(&action, Action::Forward(_, effects) if effects.write.is_some()) {
        return Ok(action);
    }
    Ok(cacheability_cache.intern(key, action))
}

#[cfg(test)]
mod tests {
    #![allow(clippy::wildcard_enum_match_arm)]

    use std::sync::Arc;

    use super::*;
    use crate::proxy::cacheability_memo::CACHEABILITY_MEMO_CAPACITY;
    use crate::proxy::cacheability_store::CacheabilityStore;

    fn empty_volatility() -> HashMap<EcoString, FunctionVolatility> {
        HashMap::new()
    }

    /// Two connections analyzing the same text must share one interned payload.
    /// This is the defect behind GitHub issue #4: previously each connection
    /// called `Arc::new` on its own analysis, so N connections retained N ASTs.
    #[test]
    fn test_repeat_text_across_connections_interns_once() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let sql = "SELECT a FROM t WHERE id = 1";

        let mut first = CacheabilityCache::new(Arc::clone(&store));
        let mut second = CacheabilityCache::new(Arc::clone(&store));

        let a = analyze(sql, &mut first, &fv).expect("analyze on the first connection");
        let b = analyze(sql, &mut second, &fv).expect("analyze on the second connection");

        let (Action::CacheCheck(a), Action::CacheCheck(b)) = (a, b) else {
            panic!("expected a cacheable verdict for a plain SELECT");
        };
        assert!(
            Arc::ptr_eq(&a, &b),
            "both connections must share one interned payload"
        );
    }

    /// Distinct texts must not accumulate past the per-connection bound, and
    /// what falls out must be released so the store can reclaim it. Without a
    /// bound a pooled connection accumulates every text it ever sees.
    #[test]
    fn test_memo_is_bounded_and_releases_on_eviction() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let mut memo = CacheabilityCache::new(Arc::clone(&store));

        let overshoot = CACHEABILITY_MEMO_CAPACITY.get() * 2;
        for i in 0..overshoot {
            let sql = format!("SELECT a FROM t WHERE id = {i}");
            analyze(&sql, &mut memo, &fv).expect("analyze a distinct literal");
        }

        assert_eq!(
            memo.entry_count(),
            CACHEABILITY_MEMO_CAPACITY.get(),
            "per-connection memo must stay at its cap"
        );
        assert_eq!(
            store.len(),
            CACHEABILITY_MEMO_CAPACITY.get(),
            "evicted texts must be released, not retained by the store"
        );
    }

    /// Closing a connection releases everything it held, so nothing survives a
    /// connection's lifetime unless another connection still wants it.
    #[test]
    fn test_connection_close_reclaims_its_entries() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();

        let mut memo = CacheabilityCache::new(Arc::clone(&store));
        analyze("SELECT a FROM t WHERE id = 1", &mut memo, &fv).expect("analyze");
        analyze("SELECT a FROM t WHERE id = 2", &mut memo, &fv).expect("analyze");
        assert_eq!(store.len(), 2);

        drop(memo);
        assert_eq!(
            store.len(),
            0,
            "closing the connection reclaims its entries"
        );
    }

    /// A text two connections share survives one of them closing.
    #[test]
    fn test_shared_entry_survives_one_connection_closing() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let sql = "SELECT a FROM t WHERE id = 1";

        let mut first = CacheabilityCache::new(Arc::clone(&store));
        let mut second = CacheabilityCache::new(Arc::clone(&store));
        analyze(sql, &mut first, &fv).expect("analyze");
        analyze(sql, &mut second, &fv).expect("analyze");

        drop(first);
        assert_eq!(store.len(), 1, "the other connection still references it");
        drop(second);
        assert_eq!(store.len(), 0);
    }

    /// A pressure drop must actually free: clearing the store alone frees
    /// nothing while connections still hold handles, so connections drain on
    /// the epoch move.
    #[test]
    fn test_pressure_drop_drains_connection_memos() {
        let store = Arc::new(CacheabilityStore::new());
        let fv = empty_volatility();
        let mut memo = CacheabilityCache::new(Arc::clone(&store));
        analyze("SELECT a FROM t WHERE id = 1", &mut memo, &fv).expect("analyze");

        store.pressure_observed(true);
        assert_eq!(store.len(), 0, "store cleared");
        assert_eq!(memo.entry_count(), 1, "connection has not noticed yet");

        // Next query on that connection reconciles the epoch and drains.
        analyze("SELECT a FROM t WHERE id = 2", &mut memo, &fv).expect("analyze");
        assert_eq!(memo.entry_count(), 1, "drained, then re-populated with one");
        assert_eq!(store.len(), 1);
    }

    fn analyze_fresh(sql: &str, fv: &HashMap<EcoString, FunctionVolatility>) -> Action {
        let store = Arc::new(CacheabilityStore::new());
        analyze(sql, &mut CacheabilityCache::new(store), fv).expect("analyze sql")
    }

    #[test]
    fn test_analyze_dml_classifies_as_write() {
        let fv = HashMap::new();
        match analyze_fresh("INSERT INTO t (a) VALUES (1)", &fv) {
            Action::Forward(
                ForwardReason::UnsupportedStatement,
                StatementEffects {
                    write: Some(WriteClass::InsertRows(_)),
                    ..
                },
            ) => {}
            other => panic!(
                "expected InsertRows write, got {:?}",
                discriminant_name(&other)
            ),
        }
        // No WHERE → whole-table opaque.
        match analyze_fresh("UPDATE t SET a = 1", &fv) {
            Action::Forward(
                ForwardReason::UnsupportedStatement,
                StatementEffects {
                    write: Some(WriteClass::Table(_)),
                    ..
                },
            ) => {}
            other => panic!("expected Table write, got {:?}", discriminant_name(&other)),
        }
        match analyze_fresh("UPDATE t SET a = 1 WHERE id = 2", &fv) {
            Action::Forward(
                ForwardReason::UnsupportedStatement,
                StatementEffects {
                    write: Some(WriteClass::UpdateRows(_)),
                    ..
                },
            ) => {}
            other => panic!(
                "expected UpdateRows write, got {:?}",
                discriminant_name(&other)
            ),
        }
        match analyze_fresh("DELETE FROM t WHERE id = 1", &fv) {
            Action::Forward(
                ForwardReason::UnsupportedStatement,
                StatementEffects {
                    write: Some(WriteClass::DeleteRows(_)),
                    ..
                },
            ) => {}
            other => panic!(
                "expected DeleteRows write, got {:?}",
                discriminant_name(&other)
            ),
        }
    }

    fn settings_of(sql: &str) -> SettingsEffect {
        match analyze_fresh(sql, &HashMap::new()) {
            Action::Forward(_, effects) => effects.settings,
            _ => panic!("expected a forward for {sql:?}"),
        }
    }

    /// PGC-287: a literal `set_config` names the setting it changes and its
    /// scope; anything less certain may change every tracked setting.
    #[test]
    fn test_analyze_settings_effect_of_set_config() {
        #[rustfmt::skip]
        let cases = [
            ("SELECT set_config('timezone', 'UTC', true)", SettingsEffect::guc(Guc::TimeZone, true)),
            ("SELECT set_config('search_path', 'a', false), set_config('role', 'r', false)",
             SettingsEffect::guc(Guc::SearchPath, false).union(SettingsEffect::guc(Guc::Role, false))),
            // Untracked names are no effect; a non-literal name may be anything.
            ("SELECT set_config('request.jwt.claims', '{}', true)", SettingsEffect::NONE),
            ("SELECT set_config($1, 'x', false)", SettingsEffect::all()),
            // Hidden from the converter: the text scan's answer stands.
            ("DO $$ BEGIN PERFORM SET_CONFIG('x', 'y', false); END $$", SettingsEffect::all()),
            ("SELECT now()", SettingsEffect::NONE),
            ("SET LOCAL search_path = a", SettingsEffect::guc(Guc::SearchPath, true)),
        ];
        for (sql, expected) in cases {
            assert_eq!(settings_of(sql), expected, "{sql}");
        }
        assert!(StatementEffects::unknown().settings.all);
    }

    fn custom_settings_of(sql: &str) -> Vec<CustomSettingEffect> {
        match analyze_fresh(sql, &HashMap::new()) {
            Action::Forward(_, effects) => effects.custom_settings.into_vec(),
            _ => panic!("expected a forward for {sql:?}"),
        }
    }

    /// PGC-113: a literal `set_config` of a dotted name carries the custom
    /// setting and its value when that is a literal; a `SET` of one likewise.
    #[test]
    fn test_analyze_custom_setting_effects() {
        let effect = |name: &str, value: Option<&str>, local: bool| CustomSettingEffect {
            name: name.into(),
            value: value.map(Into::into),
            local,
        };
        #[rustfmt::skip]
        let cases = [
            ("SELECT set_config('request.jwt.claims', '{}', true)", vec![effect("request.jwt.claims", Some("{}"), true)]),
            ("SELECT set_config('app.tenant', $1, false)", vec![effect("app.tenant", None, false)]),
            ("SELECT set_config('timezone', 'UTC', true)", vec![]),
            ("SET app.tenant = '7'", vec![effect("app.tenant", Some("7"), false)]),
            ("SELECT now()", vec![]),
        ];
        for (sql, expected) in cases {
            assert_eq!(custom_settings_of(sql), expected, "{sql}");
        }
    }

    #[test]
    fn test_analyze_isolation_effects() {
        // (sql, the forward's reason, write class and isolation effect)
        #[rustfmt::skip]
        let cases = [
            ("SELECT set_config('default_transaction_isolation', 'serializable', false)",
             "UncacheableSelect write=Some(Connection) isolation=SessionUnknown"),
            ("DO $$ BEGIN PERFORM SET_CONFIG('x', 'y', false); END $$",
             "UnsupportedStatement write=Some(Connection) isolation=SessionUnknown"),
            ("BEGIN ISOLATION LEVEL SERIALIZABLE",
             "UnsupportedStatement write=None isolation=Transaction(Serializable)"),
            ("SET search_path TO public",
             "UnsupportedStatement write=None isolation=None"),
            ("SELECT * FROM t WHERE id = 1", "CacheCheck"),
        ];
        for (sql, expected) in cases {
            assert_eq!(
                action_described(&analyze_fresh(sql, &HashMap::new())),
                expected,
                "{sql}"
            );
        }
    }

    #[test]
    fn test_analyze_txn_control_is_plain_forward() {
        let fv = HashMap::new();
        for sql in ["BEGIN", "COMMIT", "SET search_path TO public"] {
            assert!(
                matches!(
                    analyze_fresh(sql, &fv),
                    Action::Forward(ForwardReason::UnsupportedStatement, _)
                ),
                "for {sql:?}"
            );
        }
    }

    #[test]
    fn test_analyze_volatile_where_function_is_a_write() {
        // Unknown and volatile functions in WHERE can modify the database;
        // stable ones cannot (PGC-124).
        let mut fv = HashMap::new();
        fv.insert(EcoString::from("f_volatile"), FunctionVolatility::Volatile);
        fv.insert(EcoString::from("f_stable"), FunctionVolatility::Stable);

        for sql in [
            "SELECT * FROM t WHERE f_volatile(a) = 1",
            "SELECT * FROM t WHERE f_unknown(a) = 1",
        ] {
            assert!(
                matches!(
                    analyze_fresh(sql, &fv),
                    Action::Forward(
                        ForwardReason::UncacheableSelect,
                        StatementEffects {
                            write: Some(WriteClass::Connection),
                            ..
                        }
                    )
                ),
                "for {sql:?}"
            );
        }
        assert!(matches!(
            analyze_fresh("SELECT * FROM t WHERE f_stable(a) = 1", &fv),
            Action::Forward(
                ForwardReason::UncacheableSelect,
                StatementEffects { write: None, .. }
            )
        ));
    }

    #[test]
    fn test_analyze_volatile_nested_in_stable_is_a_write() {
        // A volatile nested inside a stable function still executes and can
        // modify the DB. The cacheability check short-circuits on the outer
        // stable, so write detection must walk the whole tree independently.
        let mut fv = HashMap::new();
        fv.insert(EcoString::from("f_volatile"), FunctionVolatility::Volatile);
        fv.insert(EcoString::from("f_stable"), FunctionVolatility::Stable);

        assert!(
            matches!(
                analyze_fresh("SELECT * FROM t WHERE f_stable(f_volatile(a)) = 1", &fv),
                Action::Forward(
                    ForwardReason::UncacheableSelect,
                    StatementEffects {
                        write: Some(WriteClass::Connection),
                        ..
                    }
                )
            ),
            "stable wrapping volatile must be a write"
        );
        // Stable wrapping stable is still just a read.
        assert!(matches!(
            analyze_fresh("SELECT * FROM t WHERE f_stable(f_stable(a)) = 1", &fv),
            Action::Forward(
                ForwardReason::UncacheableSelect,
                StatementEffects { write: None, .. }
            )
        ));
    }

    #[test]
    fn test_analyze_multi_statement_is_a_write() {
        assert!(matches!(
            analyze_fresh("SELECT 1; SELECT 2", &HashMap::new()),
            Action::Forward(
                ForwardReason::Invalid,
                StatementEffects {
                    write: Some(WriteClass::Connection),
                    ..
                }
            )
        ));
    }

    #[test]
    fn test_writes_are_not_memoized() {
        let fv = HashMap::new();
        let mut cache = CacheabilityCache::new(Arc::new(CacheabilityStore::new()));
        // (sql, its action, memo entries afterwards): a write is classified
        // but left out of the memo (DML text is ~unique and its class can
        // carry a row payload); a read verdict is memoized as before.
        let cases = [
            ("INSERT INTO t (a) VALUES (1)", "ForwardWrite", 0),
            ("SELECT * FROM t WHERE a = 1", "CacheCheck", 1),
        ];
        for (sql, expected, memoized) in cases {
            let action = analyze(sql, &mut cache, &fv).expect("analyze");
            assert_eq!(
                (discriminant_name(&action), cache.entry_count()),
                (expected, memoized),
                "{sql}"
            );
        }
    }

    /// A forward's reason and effects, or the other actions' names.
    fn action_described(action: &Action) -> String {
        match action {
            Action::Forward(reason, effects) => format!(
                "{reason:?} write={:?} isolation={:?}",
                effects.write, effects.isolation
            ),
            other => discriminant_name(other).to_owned(),
        }
    }

    fn discriminant_name(action: &Action) -> &'static str {
        match action {
            Action::Forward(_, StatementEffects { write: None, .. }) => "Forward",
            Action::Forward(_, StatementEffects { write: Some(_), .. }) => "ForwardWrite",
            Action::CacheCheck(_) => "CacheCheck",
            Action::Explain(_) => "Explain",
        }
    }
}
