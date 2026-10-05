use std::sync::Arc;

use ecow::EcoString;
use tokio_util::bytes::{BufMut, Bytes, BytesMut};
use tracing::{debug, trace};

use super::ConnectionState;
use super::extended_buffer::{
    CacheCandidate, ExecuteEntry, ExecuteSnapshot, ExtendedBuffer, buffer_effects,
};
use super::forward_lazy_parse_install;
use crate::cache::QueryParameters;
use crate::cache::messages::PipelineDescribe;
use crate::pg::protocol::backend::TransactionStatus;
use crate::pg::protocol::encode::CLOSE_COMPLETE_MSG;
use crate::pg::protocol::extended::{
    ParsedBindMessage, ParsedCloseMessage, ParsedParseMessage, parse_bind_message,
    parse_close_message, parse_describe_message, parse_execute_message, parse_parse_message,
};
use crate::pg::protocol::frontend::PgFrontendMessage;
use crate::pg::protocol::session::{Portal, PreparedStatement, ResultFormats, StatementType};
use crate::proxy::query::{Action, ForwardReason, analyze};
use crate::query::transform::{
    delete_statement_parameterize, insert_statement_parameterize, update_statement_parameterize,
};
use crate::query::write::{StatementEffects, WriteClass};

/// What Parse-time analysis decided about a statement.
struct StatementAnalysis {
    sql_type: StatementType,
    /// What forwarding an Execute of it does to the connection's tracked
    /// state.
    effects: StatementEffects,
}

/// Resolve a parameterized INSERT's `$N` cells against a portal's bind values so
/// it keeps row-level precision (PGC-370). Any other class passes through. On a
/// substitution failure (out-of-bounds, undecodable, or missing OIDs) the INSERT
/// degrades to table-conservative rather than trusting an unsubstituted cell.
fn write_class_bind(class: &WriteClass, portal: &Portal, stmt: &PreparedStatement) -> WriteClass {
    // Substitute `$n` bind values into row-enumerable write predicates so the
    // read-after-write gate reasons about concrete values (PGC-370/386). A
    // substitution error (out-of-range index, undecodable value) degrades the
    // write to table-level: a partially-bound predicate must never be trusted.
    // Non-row writes carry nothing to substitute, so params are built lazily.
    let parameters = || QueryParameters {
        values: portal.parameter_values.clone(),
        formats: portal.parameter_formats.clone(),
        oids: stmt.parameter_oids.clone(),
    };
    match class {
        WriteClass::InsertRows(insert) => {
            match insert_statement_parameterize(insert, &parameters()) {
                Ok(substituted) => WriteClass::InsertRows(Arc::new(substituted)),
                Err(_) => WriteClass::Table(insert.relation.clone()),
            }
        }
        WriteClass::DeleteRows(delete) => {
            match delete_statement_parameterize(delete, &parameters()) {
                Ok(substituted) => WriteClass::DeleteRows(Arc::new(substituted)),
                Err(_) => WriteClass::Table(delete.relation.clone()),
            }
        }
        WriteClass::UpdateRows(update) => {
            match update_statement_parameterize(update, &parameters()) {
                Ok(substituted) => WriteClass::UpdateRows(Arc::new(substituted)),
                Err(_) => WriteClass::Table(update.relation.clone()),
            }
        }
        WriteClass::Table(_) | WriteClass::Connection | WriteClass::ConnectionUnstampable => {
            class.clone()
        }
    }
}

impl ConnectionState {
    /// Flush any buffered extended protocol messages to origin.
    pub(super) fn extended_buffer_flush_to_origin(&mut self) {
        // A Flush forwards the whole buffer, including sealed Executes — apply
        // their effects before the buffer is consumed.
        if let Some(effects) = self.extended.buffer_peek().map(buffer_effects) {
            for entry_effects in effects {
                self.forwarded_effects_apply(&entry_effects);
            }
        }
        if let Some(bytes) = self.extended.buffer_flush() {
            self.origin_write_buf.push_back(bytes);
        }
    }

    /// Forward an extended buffer to origin, appending the trailing message bytes (Sync or Flush).
    /// Records metrics for any Execute in the buffer.
    pub(super) fn extended_buffer_forward_to_origin(
        &mut self,
        buffer: ExtendedBuffer,
        trailing_bytes: &[u8],
    ) {
        if let Some(first) = buffer.entries.first() {
            self.forward_metrics_record(first);
        }
        if let Some(stmt_name) = self.forward_lazy_parse(&buffer) {
            forward_lazy_parse_install(
                &stmt_name,
                &self.prepared_statements,
                &mut self.origin_write_buf,
                &mut self.origin_intercept,
            );
        }

        for entry_effects in buffer_effects(&buffer) {
            self.forwarded_effects_apply(&entry_effects);
        }

        let bytes = self.extended.buffer_forward(buffer, trailing_bytes);
        self.origin_dispatch(bytes, None);
    }

    /// Count a forwarded batch by its first Execute.
    fn forward_metrics_record(&self, first: &ExecuteEntry) {
        let m = crate::metrics::handles();
        m.query.uncacheable.increment(1);
        // A cacheable read in a failed block forwards so origin reports the
        // aborted-transaction error.
        if first.candidate.is_some() && self.transaction_status == TransactionStatus::Failed {
            m.txn.forward_failed.increment(1);
        }
        let statement = first
            .portal_name
            .as_deref()
            .and_then(|portal_name| self.portal_statement(portal_name));
        match statement.map(|(_, stmt)| &stmt.sql_type) {
            Some(StatementType::NonSelect) => m.query.unsupported.increment(1),
            Some(StatementType::ParseError) => m.query.invalid.increment(1),
            Some(StatementType::Cacheable(_) | StatementType::UncacheableSelect) | None => {}
        }
    }

    /// The statement a forwarded Bind-without-Parse batch must lazily Parse
    /// first: the first Execute's statement, when origin doesn't know it yet.
    fn forward_lazy_parse(&self, buffer: &ExtendedBuffer) -> Option<EcoString> {
        if buffer.any_has_parse() {
            return None;
        }
        let portal_name = buffer.entries.first()?.portal_name.as_deref()?;
        let (portal, stmt) = self.portal_statement(portal_name)?;
        (!stmt.origin_prepared).then(|| portal.statement_name.clone())
    }

    /// The portal named `portal_name` and the prepared statement it binds.
    fn portal_statement(&self, portal_name: &str) -> Option<(&Portal, &PreparedStatement)> {
        let portal = self.portals.get(portal_name)?;
        let stmt = self.prepared_statements.get(&portal.statement_name)?;
        Some((portal, stmt))
    }

    /// Handle Parse message — analyze cacheability, store statement, buffer bytes.
    pub(super) fn handle_parse_message(&mut self, msg: PgFrontendMessage) {
        // Freeze the codec's zero-copy slice up front so the parsed SQL can be
        // a refcounted view into the frame instead of a fresh String.
        let data = msg.data.freeze();
        let Ok(parsed) = parse_parse_message(&data) else {
            // Parse failed: forward raw. No views of `data` exist on this path,
            // so try_into_mut reclaims the buffer without copying.
            self.origin_write_buf.push_back(
                data.try_into_mut()
                    .unwrap_or_else(|b| BytesMut::from(&b[..])),
            );
            return;
        };
        let analysis = self.statement_analyze(&parsed.sql);
        let statement_name = parsed.statement_name.clone();
        self.statement_store(parsed, analysis, data.clone());

        let seg = &mut self.extended.buffer_get_or_create().pending;
        if seg.has_parse {
            seg.dirty = true;
        }
        seg.has_parse = true;
        seg.parse_statement_names.push(statement_name);
        seg.bytes.push(data);
        trace!("net: Parse buffered");
    }

    /// Cacheability analysis is memoized in `cacheability_cache` (shared with
    /// the simple-query path); a hit skips the parse/convert/classify entirely.
    /// search_path mutation detection — which the inline parse used to fold in
    /// — isn't captured by that cache, so it's replayed for the non-SELECT
    /// statements that can mutate it (no piggyback for extended; a standalone
    /// SHOW is issued via the lazy path on RFQ).
    fn statement_analyze(&mut self, sql: &str) -> StatementAnalysis {
        let analyzed = analyze(sql, &mut self.cacheability_cache, &self.func_volatility);
        let (sql_type, effects) = match analyzed {
            Ok(Action::CacheCheck(ast)) => {
                (StatementType::Cacheable(ast), StatementEffects::default())
            }
            // `pgcache_explain(...)` is only intercepted on the simple-query
            // path; over the extended protocol it forwards to origin (which has
            // no such function), preserving pre-PGC-345 behavior.
            Ok(Action::Explain(_)) => (
                StatementType::UncacheableSelect,
                StatementEffects::default(),
            ),
            Ok(Action::Forward(ForwardReason::UncacheableSelect, effects)) => {
                (StatementType::UncacheableSelect, effects)
            }
            Ok(Action::Forward(
                ForwardReason::UnsupportedStatement | ForwardReason::Invalid,
                effects,
            )) => {
                self.search_path_parse_inspect(sql);
                (StatementType::NonSelect, effects)
            }
            // pg_query failed but origin may still parse it (parser version
            // skew): any Execute of this statement could be a write, so record
            // conservatively at connection scope — the same failure direction
            // as the simple path (PGC-448).
            Err(_) => (StatementType::ParseError, StatementEffects::unknown()),
        };
        StatementAnalysis { sql_type, effects }
    }

    /// Handle Bind message — store portal, buffer bytes.
    pub(super) fn handle_bind_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_bind_message(&msg.data) {
            self.portal_store(parsed);

            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.has_bind {
                seg.dirty = true;
            }
            seg.has_bind = true;
            seg.bytes.push(msg.data.freeze());
            trace!("net: Bind buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Handle Execute message — record metrics, parse portal name, buffer bytes.
    /// Decision-making deferred to Sync.
    pub(super) fn handle_execute_message(&mut self, msg: PgFrontendMessage) {
        let m = crate::metrics::handles();
        m.query.total.increment(1);
        m.conn.extended_queries.increment(1);
        self.telemetry.query_receive();

        let portal_name = parse_execute_message(&msg.data).ok().map(|p| p.portal_name);

        // Snapshot the cache candidate now, while the portal/statement still
        // reflect this execute's Bind/Parse (a later execute may rebind the
        // same — usually unnamed — portal). The current segment's Describe
        // governs the ParameterDescription requirement.
        let describe = self
            .extended
            .buffer
            .as_ref()
            .map_or(PipelineDescribe::None, |b| b.pending.describe);
        let snapshot = ExecuteSnapshot {
            candidate: self.execute_cache_candidate(portal_name.as_deref(), describe),
            effects: self.execute_effects(portal_name.as_deref()),
            portal_name,
        };
        self.extended
            .buffer_get_or_create()
            .pending_seal(msg.data.freeze(), snapshot);
        trace!("net: Execute buffered");
    }

    /// Snapshot the effects of the statement an Execute targets, with this
    /// execute's own bind values substituted into a row-enumerable write —
    /// captured now for the same reason as `execute_cache_candidate`: a later
    /// execute in the batch may rebind the portal, and the write log must never
    /// alias one execute's values onto another (PGC-445). An unresolvable
    /// portal/statement (or an unparseable Execute) could be anything, so it
    /// snapshots the conservative unknown effects.
    fn execute_effects(&self, portal_name: Option<&str>) -> StatementEffects {
        let Some((portal, stmt)) = portal_name.and_then(|p| self.portal_statement(p)) else {
            return StatementEffects::unknown();
        };
        StatementEffects {
            write: stmt
                .effects
                .write
                .as_ref()
                .map(|class| write_class_bind(class, portal, stmt)),
            isolation: stmt.effects.isolation,
            transaction: stmt.effects.transaction,
        }
    }

    /// Snapshot a cacheable-query candidate for the portal an Execute targets.
    /// Returns None when the portal/statement doesn't resolve to a cacheable
    /// SELECT with uniform result formats (and, for Describe('S'), a cached
    /// ParameterDescription). Global cache gating is checked separately at Sync.
    pub(super) fn execute_cache_candidate(
        &self,
        portal_name: Option<&str>,
        describe: PipelineDescribe,
    ) -> Option<CacheCandidate> {
        let (portal, stmt) = self.portal_statement(portal_name?)?;

        // Only handle implicit or uniform result formats
        if let ResultFormats::PerColumn(_) = portal.result_formats {
            trace!("result format is not implicit or uniform");
            return None;
        }

        let cacheable_query = match &stmt.sql_type {
            StatementType::Cacheable(query) => Arc::clone(query),
            StatementType::NonSelect
            | StatementType::UncacheableSelect
            | StatementType::ParseError => return None,
        };

        // Describe('S'): require a cached parameter_description
        if describe == PipelineDescribe::Statement && stmt.parameter_description.is_none() {
            return None;
        }

        Some(CacheCandidate {
            cacheable_query,
            parameters: QueryParameters {
                values: portal.parameter_values.clone(),
                formats: portal.parameter_formats.clone(),
                oids: stmt.parameter_oids.clone(),
            },
            result_formats: portal.result_formats.clone(),
            parameter_description: if describe == PipelineDescribe::Statement {
                stmt.parameter_description.clone()
            } else {
                None
            },
            statement_name: portal.statement_name.clone(),
            origin_prepared: stmt.origin_prepared,
        })
    }

    /// Handle Describe message — buffer bytes and track describe metadata.
    pub(super) fn handle_describe_message(&mut self, msg: PgFrontendMessage) {
        if let Ok(parsed) = parse_describe_message(&msg.data) {
            let seg = &mut self.extended.buffer_get_or_create().pending;
            if seg.describe != PipelineDescribe::None {
                seg.dirty = true;
            }

            match parsed.describe_type {
                b'S' => {
                    seg.describe = PipelineDescribe::Statement;
                    seg.describe_statement_names.push(parsed.name);
                }
                b'P' => {
                    seg.describe = PipelineDescribe::Portal;
                }
                _ => {}
            }

            seg.bytes.push(msg.data.freeze());
            trace!("net: Describe buffered");
            return;
        }
        self.origin_write_buf.push_back(msg.data);
    }

    /// Emit any deferred `CloseComplete`s (PGC-234: locally-handled Closes of
    /// statements the origin never prepared) as one ordered synth slot, so they
    /// keep their place ahead of whatever origin/cache response follows. Returns
    /// the count flushed.
    pub(super) fn deferred_close_completes_flush(&mut self) -> u32 {
        let n = self.extended.deferred_close_completes;
        if n == 1 {
            self.extended.deferred_close_completes = 0;
            self.egress
                .synth_push(Bytes::from_static(CLOSE_COMPLETE_MSG));
        } else if n > 1 {
            self.extended.deferred_close_completes = 0;
            let mut out = BytesMut::with_capacity(CLOSE_COMPLETE_MSG.len() * n as usize);
            for _ in 0..n {
                out.put_slice(CLOSE_COMPLETE_MSG);
            }
            self.egress.synth_push(out.freeze());
        }
        n
    }

    /// Handle Close message. A `Close(statement)` for a statement that was served
    /// from cache and never prepared on the origin is handled locally — see
    /// [`Self::close_locally_handled`]. Everything else forwards as before.
    pub(super) fn handle_close_message(&mut self, msg: PgFrontendMessage) {
        let parsed = parse_close_message(&msg.data).ok();
        if let Some(parsed) = &parsed
            && self.close_locally_handled(parsed)
        {
            self.statement_close(&parsed.name);
            self.extended.deferred_close_completes += 1;
            crate::metrics::handles().conn.close_local.increment(1);
            return;
        }
        self.deferred_close_completes_flush();
        self.extended_buffer_flush_to_origin();
        match parsed.as_ref().map(|p| (p.close_type, p.name.as_str())) {
            Some((b'S', name)) => self.statement_close(name),
            Some((b'P', name)) => self.portal_close(name),
            _ => {}
        }
        self.extended.group_origin_forwarded = true;
        self.origin_write_buf.push_back(msg.data);
    }

    /// Whether a Close can be answered locally (PGC-234): a statement the
    /// origin never prepared (`origin_prepared == false`) — forwarding the
    /// Close (and its paired Sync) would be a useless round-trip, so the
    /// `CloseComplete` is deferred to the next Sync. Not mid-batch (`buffer`
    /// present), and not once anything has already been forwarded this group,
    /// so deferred completions can't reorder ahead of it.
    fn close_locally_handled(&self, parsed: &ParsedCloseMessage) -> bool {
        let group_local = self.extended.buffer.is_none() && !self.extended.group_origin_forwarded;
        if parsed.close_type != b'S' || !group_local {
            return false;
        }
        self.prepared_statements
            .get(parsed.name.as_str())
            .is_some_and(|s| !s.origin_prepared)
    }

    /// Handle Sync message — all cache vs. forward decision-making happens here.
    ///
    /// If every Execute in the batch is an independently cacheable read, each is
    /// dispatched as its own cache slot (in order). Otherwise the batch is
    /// synthesized (Parse-only) or forwarded whole to origin.
    pub(super) fn handle_sync_message(&mut self, msg: PgFrontendMessage) {
        // Emit any deferred CloseCompletes (locally-handled Closes) as an ordered
        // synth slot before this Sync's responses (PGC-234).
        let local_closes = self.deferred_close_completes_flush();
        let group_origin_forwarded = self.extended.group_origin_forwarded;
        self.extended.group_origin_forwarded = false;

        let Some(buffer) = self.extended.buffer_take() else {
            // Bare Sync. If this group only handled Closes locally (nothing was
            // forwarded to origin), the origin has no pending work to ack —
            // synthesize the ReadyForQuery instead of a useless round-trip.
            if local_closes > 0 && !group_origin_forwarded {
                trace!("net: bare Sync → synth ReadyForQuery (local closes only)");
                self.egress.synth_push(Bytes::from_static(
                    self.transaction_status.ready_for_query_message(),
                ));
            } else {
                trace!("net: proxy→origin Sync (no buffer)");
                self.egress.origin_open();
                self.origin_write_buf.push_back(msg.data);
            }
            return;
        };

        if self.cache_batch_eligible(&buffer) {
            let entries = buffer.entries;
            trace!("net: Sync → cache batch ({} executes)", entries.len());
            self.cache_batch_dispatch(entries);
        } else if self.try_synthesize_parse_describe_response(&buffer) {
            trace!("net: Sync → synthesized ParseComplete+Describe response");
        } else {
            self.extended_buffer_forward_to_origin(buffer, &msg.data);
            trace!("net: Sync → origin (forwarded buffer)");
        }
    }

    /// Handle Flush message — forward buffer to origin, no cache attempt.
    /// Handles JDBC pattern: Parse/Bind/Describe/Flush then Execute/Sync.
    pub(super) fn handle_flush_message(&mut self, msg: PgFrontendMessage) {
        // Anything reaching origin must come after any deferred CloseCompletes,
        // and marks the group as having origin work (PGC-234).
        self.deferred_close_completes_flush();
        self.extended.group_origin_forwarded = true;
        let Some(buffer) = self.extended.buffer_take() else {
            self.origin_write_buf.push_back(msg.data);
            return;
        };

        self.extended_buffer_forward_to_origin(buffer, &msg.data);
        // The forwarded batch ends in Flush, not Sync — origin will send the
        // describe response with no ReadyForQuery. Mark the opened egress slot
        // so the next client message seals it (see `handle_client_message`).
        self.flush_describe_pending = true;
        trace!("net: Flush → origin (forwarded buffer)");
    }

    /// Store a prepared statement in connection state.
    ///
    /// For unnamed statements (empty name), always overwrite — the protocol allows reuse of
    /// the unnamed slot with a new Parse. For named statements, `or_insert` preserves existing
    /// metadata (parameter_description, origin_prepared) accumulated during the cold path.
    fn statement_store(
        &mut self,
        parsed: ParsedParseMessage,
        analysis: StatementAnalysis,
        parse_bytes: Bytes,
    ) {
        let client_parameter_oids = parsed.parameter_oids.clone();
        let stmt = PreparedStatement {
            sql: parsed.sql,
            parameter_oids: parsed.parameter_oids,
            client_parameter_oids,
            sql_type: analysis.sql_type,
            parameter_description: None,
            row_description: None,
            describe_no_data: false,
            origin_prepared: false,
            parse_bytes: Some(parse_bytes),
            effects: analysis.effects,
        };
        debug!("parsed statement insert {}", parsed.statement_name);

        if parsed.statement_name.is_empty() {
            // Unnamed statement: always overwrite per protocol spec
            self.prepared_statements.insert(parsed.statement_name, stmt);
        } else {
            // Named statement: preserve existing metadata from first cold path
            if !self
                .prepared_statements
                .contains_key(&parsed.statement_name)
            {
                crate::metrics::handles()
                    .conn
                    .prepared_statements
                    .increment(1.0);
            }
            self.prepared_statements
                .entry(parsed.statement_name)
                .or_insert(stmt);
        }
    }

    /// Store a portal in connection state.
    pub(super) fn portal_store(&mut self, parsed: ParsedBindMessage) {
        let portal = Portal {
            statement_name: parsed.statement_name,
            parameter_values: parsed.parameter_values,
            parameter_formats: parsed.parameter_formats,
            result_formats: parsed.result_formats,
        };

        debug!("parsed portal insert {:?}", portal);
        self.portals.insert(parsed.portal_name, portal);
    }

    /// Remove a prepared statement from connection state.
    pub(super) fn statement_close(&mut self, name: &str) {
        if self.prepared_statements.remove(name).is_some() {
            crate::metrics::handles()
                .conn
                .prepared_statements
                .decrement(1.0);
        }
    }

    /// Remove a portal from connection state.
    pub(super) fn portal_close(&mut self, name: &str) {
        self.portals.remove(name);
    }

    /// Clear all prepared statements from connection state.
    #[expect(unused)]
    pub(super) fn statements_clear(&mut self) {
        self.prepared_statements.clear();
    }

    /// Clear all portals from connection state.
    #[expect(unused)]
    pub(super) fn portals_clear(&mut self) {
        self.portals.clear();
    }
}
