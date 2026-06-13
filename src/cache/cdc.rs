#![allow(dead_code)]

use std::time::{Duration, Instant, UNIX_EPOCH};

use ecow::EcoString;
use postgres_replication::{
    LogicalReplicationStream,
    protocol::{
        BeginBody, CommitBody, DeleteBody, InsertBody, LogicalReplicationMessage, OriginBody,
        PrimaryKeepAliveBody, RelationBody, ReplicationMessage, TruncateBody, TupleData, TypeBody,
        UpdateBody, XLogDataBody,
    },
};
use postgres_types::PgLsn;
use tokio::{
    pin,
    time::{Interval, interval},
};
use tokio_postgres::{Client, Error};
use tokio_stream::StreamExt;
use tokio_util::bytes::Bytes;
use tokio_util::sync::CancellationToken;

use std::sync::Arc;

use tokio::sync::Notify;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{debug, error, trace};

use crate::catalog::{ColumnMetadata, ColumnStore, TableMetadata, cache_type_name_resolve};
use crate::pg::cdc::connect_replication;
use crate::pg::protocol::ByteString;
use crate::result::error_chain_format;
use crate::settings::Settings;

use super::{
    CacheError, CacheResult, MapIntoReport, ReportExt,
    messages::{CdcCommand, CdcValue},
    types::ActiveRelations,
};

/// Test-only CDC delivery delay (fault-injection feature, PGC-250 Slice B).
/// Once a relation is tracked, sleeps before processing each replication
/// message, so the writer's apply watermark lags behind origin — exercising the
/// deferred-Ready gate (a freshly populated query whose snapshot LSN is ahead of
/// the watermark must forward to origin, not serve a transiently-stale cache).
/// Gated on `active_relations` so setup (before any cached query) is unaffected.
/// Compiled out without the feature.
#[cfg(feature = "fault-injection")]
async fn fault_cdc_deliver_delay(active_relations: &ActiveRelations) {
    if active_relations.load().is_empty() {
        return;
    }
    if let Some(ms) = std::env::var("PGCACHE_FAULT_CDC_DELIVER_DELAY_MS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .filter(|ms| *ms > 0)
    {
        tokio::time::sleep(Duration::from_millis(ms)).await;
    }
}
#[cfg(not(feature = "fault-injection"))]
async fn fault_cdc_deliver_delay(_active_relations: &ActiveRelations) {}

/// Handles Change Data Capture (CDC) processing from PostgreSQL logical replication.
/// Processes replication messages and synchronizes changes with the cache database.
pub struct CdcProcessor {
    cdc_client: Client,
    publication_name: EcoString,
    slot_name: EcoString,

    cdc_tx: UnboundedSender<CdcCommand>,
    /// Shared set of relation OIDs with active cached queries (maintained by writer).
    active_relations: ActiveRelations,

    last_received_lsn: u64,
    /// Highest LSN whose XLogData has been decoded by this processor and
    /// whose CdcCommands have been enqueued to the writer. Distinct from
    /// the writer's `last_applied_lsn`, which only advances after the
    /// writer has actually executed the resulting cache mutations.
    last_decoded_lsn: u64,
    last_flushed_lsn: u64,
    keep_alive_timer: Interval,
    last_flush_sent: Option<Instant>,
    keep_alive_sent_count: u64,
    /// Signalled by the writer when a populated query is gated on the apply
    /// watermark; prompts an immediate reply-requested standby status update so
    /// the server returns its current `wal_end`, advancing the watermark within
    /// a round-trip (PGC-250 Slice B).
    watermark_nudge: Arc<Notify>,
}

impl CdcProcessor {
    /// Creates a new CdcProcessor with the provided CDC client and cache.
    pub async fn new(
        settings: &Settings,
        cdc_tx: UnboundedSender<CdcCommand>,
        active_relations: ActiveRelations,
        watermark_nudge: Arc<Notify>,
    ) -> CacheResult<Self> {
        let origin_cdc_client = connect_replication(&settings.replication, "CDC replication")
            .await
            .map_into_report::<CacheError>()
            .attach_loc("connecting to origin for CDC")?;

        // Create keep-alive timer with 30-second interval (hardcoded for POC)
        let mut timer = interval(Duration::from_secs(30));
        timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        Ok(Self {
            cdc_client: origin_cdc_client,
            publication_name: settings.cdc.publication_name.as_str().into(),
            slot_name: settings.cdc.slot_name.as_str().into(),
            cdc_tx,
            active_relations,
            last_received_lsn: 0,
            last_decoded_lsn: 0,
            last_flushed_lsn: 0,
            keep_alive_timer: timer,
            last_flush_sent: None,
            keep_alive_sent_count: 0,
            watermark_nudge,
        })
    }

    /// Returns the last LSN acknowledged to PostgreSQL.
    pub fn last_flushed_lsn(&self) -> u64 {
        self.last_flushed_lsn
    }

    /// Starts the CDC replication stream and processes incoming messages.
    /// Uses tokio::select! for concurrent message processing and periodic keep-alives.
    /// Returns Ok(()) on graceful shutdown via cancellation token.
    pub async fn run(&mut self, cancel: CancellationToken) -> Result<(), Error> {
        // Start replication stream
        let slot = self.slot_name.as_str();
        let publ = self.publication_name.as_str();
        let query = format!(
            "START_REPLICATION SLOT {slot} LOGICAL 0/0 (proto_version '4', publication_names '{publ}')"
        );

        let copy_stream = self.cdc_client.copy_both_simple::<Bytes>(&query).await?;

        let stream = LogicalReplicationStream::new(copy_stream);
        pin!(stream);

        debug!("Starting CDC replication stream with keep-alive intervals");

        loop {
            tokio::select! {
                _ = cancel.cancelled() => {
                    debug!("CDC shutdown signal received");
                    break;
                }
                // Handle incoming replication messages
                msg_result = stream.next() => {
                    fault_cdc_deliver_delay(&self.active_relations).await;
                    trace!("cdc stream result {msg_result:?}");
                    match msg_result {
                        Some(Ok(msg)) => {
                            if let Err(e) = self.process_replication_message(msg, stream.as_mut()).await {
                                error!(
                                    "Error processing replication message: {}",
                                    error_chain_format(&e),
                                );
                                // Continue processing despite errors
                            }
                            // Flush LSN progress to PostgreSQL periodically while processing
                            self.status_update_throttled(stream.as_mut()).await;
                        }
                        Some(Err(e)) => {
                            error!("Replication stream error: {}", error_chain_format(&e));
                            return Err(e);
                        }
                        None => {
                            debug!("Replication stream ended");
                            break;
                        }
                    }
                }
                // Handle periodic keep-alive timer
                _ = self.keep_alive_timer.tick() => {
                    debug!("Keep-alive timer tick - sending periodic status update");
                    if let Err(e) = self.send_standby_status_update(stream.as_mut(), false).await {
                        error!(
                            "Error sending periodic keep-alive: {}",
                            error_chain_format(&e),
                        );
                        // Continue despite keep-alive errors (graceful degradation)
                    }
                }
                // Writer is waiting on the apply watermark (a gated population).
                // Request an immediate server keepalive so `wal_end` (hence the
                // watermark) advances within a round-trip (PGC-250 Slice B).
                _ = self.watermark_nudge.notified() => {
                    if let Err(e) = self.send_standby_status_update(stream.as_mut(), true).await {
                        error!(
                            "Error sending reply-requested keep-alive: {}",
                            error_chain_format(&e),
                        );
                    }
                }
            }
        }

        Ok(())
    }

    /// Updates the last received LSN from XLogData message and records lag metrics.
    async fn update_lsn(&mut self, xlog_data: &XLogDataBody<LogicalReplicationMessage>) {
        let lsn = xlog_data.wal_start();
        self.last_received_lsn = lsn;
        // LSNs past 2^53 lose precision in f64 (~9 PB of WAL — irrelevant).
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles().cdc.received_lsn.set(lsn as f64);

        // Calculate time-based lag: difference between server timestamp and our current time
        let server_timestamp = xlog_data.timestamp();
        let now = Self::get_pg_timestamp();
        let lag_micros = now.saturating_sub(server_timestamp);
        // Lag values are floats for Prometheus; precision past 2^53 µs (~285 yr) is irrelevant.
        #[allow(clippy::cast_precision_loss)]
        let lag_seconds = lag_micros as f64 / 1_000_000.0;
        crate::metrics::handles().cdc.lag_seconds.set(lag_seconds);

        // Byte-based lag: difference between WAL end and our last acknowledged position
        let wal_end = xlog_data.wal_end();
        let lag_bytes = wal_end.saturating_sub(self.last_flushed_lsn);
        #[allow(clippy::cast_precision_loss)]
        crate::metrics::handles()
            .cdc
            .lag_bytes
            .set(lag_bytes as f64);

        // How long since we last acknowledged our position to PostgreSQL
        let flush_staleness = self
            .last_flush_sent
            .map_or(0.0, |t| t.elapsed().as_secs_f64());
        crate::metrics::handles()
            .cdc
            .flush_staleness
            .set(flush_staleness);
    }

    /// Marks the current LSN as decoded after successful processing.
    /// Only advances forward — keep-alive acknowledgments may have already moved
    /// past this position when the decoder read ahead of buffered transaction output.
    fn mark_lsn_decoded(&mut self) {
        let received = self.last_received_lsn;
        if received > self.last_decoded_lsn {
            self.last_decoded_lsn = received;
        }
    }

    /// Send a standby status update if at least 5 seconds have elapsed since the last one.
    async fn status_update_throttled(
        &mut self,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
    ) {
        let should_send = self
            .last_flush_sent
            .is_none_or(|t| t.elapsed() >= Duration::from_secs(5));

        if should_send && let Err(e) = self.send_standby_status_update(stream, false).await {
            error!(
                "Error sending throttled status update: {}",
                error_chain_format(&e),
            );
        }
    }

    /// Gets the current PostgreSQL timestamp in microseconds since PostgreSQL epoch (2000-01-01).
    fn get_pg_timestamp() -> i64 {
        let pg_epoch = UNIX_EPOCH + Duration::from_secs(946_684_800); // PostgreSQL epoch: 2000-01-01
        let micros = pg_epoch.elapsed().unwrap_or(Duration::ZERO).as_micros();
        i64::try_from(micros).unwrap_or(i64::MAX)
    }

    /// Sends a standby status update to PostgreSQL with current LSN progress.
    /// Returns Ok(()) on success, logs warnings on failure and continues gracefully.
    async fn send_standby_status_update(
        &mut self,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
        reply_requested: bool,
    ) -> Result<(), Error> {
        let decoded_lsn = self.last_decoded_lsn;
        let pg_lsn = PgLsn::from(decoded_lsn);
        let timestamp = Self::get_pg_timestamp();
        let reply_flag = if reply_requested { 1 } else { 0 };

        // Send standby status update: write_lsn, flush_lsn, apply_lsn all set to decoded_lsn
        match stream
            .standby_status_update(pg_lsn, pg_lsn, pg_lsn, timestamp, reply_flag)
            .await
        {
            Ok(()) => {
                // Update tracking on successful send
                self.last_flushed_lsn = decoded_lsn;
                #[allow(clippy::cast_precision_loss)]
                crate::metrics::handles()
                    .cdc
                    .flushed_lsn
                    .set(decoded_lsn as f64);
                self.last_flush_sent = Some(Instant::now());
                self.keep_alive_sent_count += 1;
                let count = self.keep_alive_sent_count;

                debug!("Sent keep-alive #{count} (LSN: {decoded_lsn}, reply: {reply_flag})");

                Ok(())
            }
            Err(e) => {
                // Log warning but don't terminate CDC processing for keep-alive failures
                error!(
                    "Warning: Failed to send standby status update (LSN: {decoded_lsn}, reply: {reply_flag}): {e}"
                );
                error!("CDC processing will continue despite keep-alive failure");
                Ok(()) // Return Ok to continue processing
            }
        }
    }

    /// Handles incoming keep-alive messages and sends appropriate responses.
    ///
    /// The keep-alive's `wal_end` is the logical decoder's `sentPtr` — the position of
    /// the last WAL record read by the decoder. This may be ahead of data sent to us
    /// (in-progress transactions are buffered in PostgreSQL's ReorderBuffer), but it is
    /// safe to acknowledge: on reconnect, PostgreSQL re-decodes from `confirmed_flush`
    /// and replays any buffered transactions. Advancing here keeps our LSN tracking
    /// accurate for reconnect decisions and allows the server to release retained WAL.
    async fn handle_keep_alive(
        &mut self,
        keep_alive: &PrimaryKeepAliveBody,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
    ) -> Result<(), Error> {
        let reply_requested = keep_alive.reply() == 1;
        let wal_end = keep_alive.wal_end();

        debug!(
            "Received keep-alive from PostgreSQL (wal_end: {}, reply_requested: {})",
            wal_end, reply_requested
        );

        // Advance our position to the decoder's read cursor. All WAL up to this point
        // has been processed by the logical decoder — any pending output is buffered
        // server-side and will be re-decoded on reconnect.
        if wal_end > self.last_decoded_lsn {
            self.last_received_lsn = wal_end;
            self.last_decoded_lsn = wal_end;
            #[allow(clippy::cast_precision_loss)]
            crate::metrics::handles()
                .cdc
                .received_lsn
                .set(wal_end as f64);

            // Forward a keep-alive mark to the writer so its applied-LSN
            // watermark can advance during idle periods (no published-table
            // transactions). Channel-send failure means the writer is gone;
            // the next stream operation will surface that.
            if let Err(e) = self.cdc_tx.send(CdcCommand::KeepAliveMark { lsn: wal_end }) {
                error!("Failed to forward keep-alive mark to writer: {e:?}");
            }
        }

        if reply_requested {
            debug!("PostgreSQL requested immediate keep-alive response");
            self.send_standby_status_update(stream, false).await?;
        }

        Ok(())
    }

    /// Processes a single replication message.
    async fn process_replication_message(
        &mut self,
        msg: ReplicationMessage<LogicalReplicationMessage>,
        stream: std::pin::Pin<&mut LogicalReplicationStream>,
    ) -> Result<(), Error> {
        match msg {
            ReplicationMessage::XLogData(xlog_data) => {
                crate::metrics::handles().cdc.events_processed.increment(1);
                self.update_lsn(&xlog_data).await;
                let result = match xlog_data.into_data() {
                    LogicalReplicationMessage::Begin(body) => self.process_begin(&body).await,
                    LogicalReplicationMessage::Commit(body) => self.process_commit(&body).await,
                    LogicalReplicationMessage::Origin(body) => self.process_origin(&body).await,
                    LogicalReplicationMessage::Relation(body) => self.process_relation(&body).await,
                    LogicalReplicationMessage::Type(body) => self.process_type(&body).await,
                    LogicalReplicationMessage::Insert(body) => self.process_insert(body).await,
                    LogicalReplicationMessage::Update(body) => self.process_update(body).await,
                    LogicalReplicationMessage::Delete(body) => self.process_delete(body).await,
                    LogicalReplicationMessage::Truncate(body) => self.process_truncate(&body).await,
                    LogicalReplicationMessage::Message(_) | _ => {
                        debug!("unhandled replication message type");
                        Ok(())
                    }
                };
                if result.is_ok() {
                    self.mark_lsn_decoded();
                }
                result
            }
            ReplicationMessage::PrimaryKeepAlive(keep_alive) => {
                self.handle_keep_alive(&keep_alive, stream).await
            }
            _ => {
                error!("ReplicationMessage error: {:?}", &msg);
                Ok(())
            }
        }
    }

    /// Gets keep-alive statistics for debugging purposes.
    fn get_keep_alive_stats(&self) -> (Option<Instant>, u64) {
        let last_sent = self.last_flush_sent;
        let count = self.keep_alive_sent_count;
        (last_sent, count)
    }

    /// Processes transaction begin messages.
    ///
    /// Forwards a `Begin` marker so the writer enters a source-transaction
    /// frame explicitly. The watermark does not advance here (only at
    /// `CommitMark`/`KeepAliveMark`); this is purely the frame delimiter.
    async fn process_begin(&self, body: &BeginBody) -> Result<(), Error> {
        let xid = body.xid();
        if let Err(e) = self.cdc_tx.send(CdcCommand::Begin { xid }) {
            error!("Failed to forward begin (xid {xid}) to writer: {e:?}");
        }
        Ok(())
    }

    /// Processes transaction commit messages.
    ///
    /// Forwards a `CommitMark` to the writer carrying the commit's `end_lsn`.
    /// The writer advances its `last_applied_lsn` watermark on this mark,
    /// guaranteeing the watermark is transaction-aligned: by the time the
    /// writer processes the mark, every mutation enqueued earlier in this
    /// transaction has been applied (mpsc preserves order).
    async fn process_commit(&self, body: &CommitBody) -> Result<(), Error> {
        let lsn = body.end_lsn();
        if let Err(e) = self.cdc_tx.send(CdcCommand::CommitMark { lsn }) {
            error!("Failed to forward commit mark (lsn {lsn}) to writer: {e:?}");
        }
        Ok(())
    }

    /// Processes origin messages.
    async fn process_origin(&self, _body: &OriginBody) -> Result<(), Error> {
        // origin messages are not relevant to the cache
        Ok(())
    }

    /// Processes relation (table schema) messages.
    async fn process_relation(&self, body: &RelationBody) -> Result<(), Error> {
        // Parse RelationBody into TableMetadata
        let table_metadata = self.parse_relation_to_table_metadata(body);

        // Register table metadata in cache
        if let Err(e) = self.cdc_tx.send(CdcCommand::TableRegister(table_metadata)) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to register table from CDC: {e:?}");
        }

        Ok(())
    }

    /// Processes type definition messages.
    async fn process_type(&self, _body: &TypeBody) -> Result<(), Error> {
        Ok(())
    }

    /// Processes insert messages with query-aware filtering.
    async fn process_insert(&mut self, body: InsertBody) -> Result<(), Error> {
        crate::metrics::handles().cdc.inserts.increment(1);
        let relation_oid = body.rel_id();

        // Check if there are any cached queries for this table
        if !self.is_relation_active(relation_oid) {
            return Ok(()); // No cached queries for this table
        }

        // Parse row data from InsertBody
        let row_data = self.parse_insert_row_data(body)?;

        // Let cache handle the insert with query-aware filtering
        if let Err(e) = self.cdc_tx.send(CdcCommand::Insert {
            relation_oid,
            row_data,
        }) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle INSERT for relation {relation_oid}: {e:?}");
        }

        Ok(())
    }

    /// Processes update messages with query-aware filtering.
    async fn process_update(&mut self, body: UpdateBody) -> Result<(), Error> {
        crate::metrics::handles().cdc.updates.increment(1);
        let relation_oid = body.rel_id();

        // Check if there are any cached queries for this table
        if !self.is_relation_active(relation_oid) {
            return Ok(()); // No cached queries for this table
        }

        // Parse old and new row data from UpdateBody
        let (key_data, new_row_data) = self.parse_update_row_data(body)?;

        // Let cache handle the update with query-aware filtering
        if let Err(e) = self.cdc_tx.send(CdcCommand::Update {
            relation_oid,
            key_data,
            row_data: new_row_data,
        }) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle UPDATE for relation {relation_oid}: {e:?}");
        }

        Ok(())
    }

    /// Processes delete messages with query-aware filtering.
    async fn process_delete(&mut self, body: DeleteBody) -> Result<(), Error> {
        crate::metrics::handles().cdc.deletes.increment(1);
        let relation_oid = body.rel_id();

        // Check if there are any cached queries for this table
        if !self.is_relation_active(relation_oid) {
            return Ok(()); // No cached queries for this table
        }

        // Parse row data from DeleteBody
        let row_data = self.parse_delete_row_data(body)?;

        // Let cache handle the delete with query-aware filtering
        if let Err(e) = self.cdc_tx.send(CdcCommand::Delete {
            relation_oid,
            row_data,
        }) {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle DELETE for relation {relation_oid}: {e:?}");
        }

        Ok(())
    }

    /// Processes truncate messages.
    async fn process_truncate(&mut self, body: &TruncateBody) -> Result<(), Error> {
        let mut ids: Vec<u32> = Vec::with_capacity(body.rel_ids().len());
        for &id in body.rel_ids() {
            if self.is_relation_active(id) {
                ids.push(id);
            }
        }
        if let Err(e) = self
            .cdc_tx
            .send(CdcCommand::Truncate { relation_oids: ids })
        {
            //todo, halt use of cache and fallback to proxy only mode
            error!("Failed to handle truncate from CDC: {e:?}");
        }
        Ok(())
    }

    /// Parse RelationBody into TableMetadata for cache registration.
    fn parse_relation_to_table_metadata(&self, relation_body: &RelationBody) -> TableMetadata {
        let relation_oid = relation_body.rel_id();
        let table_name: EcoString = relation_body.name().unwrap_or("unknown_table").into();
        let schema_name: EcoString = relation_body.namespace().unwrap_or("unknown_schema").into();

        // Build column metadata from relation body
        let mut columns = Vec::new();
        let mut primary_key_columns = Vec::new();

        for (idx, column) in relation_body.columns().iter().enumerate() {
            let is_primary_key = column.flags() == 1; // flags field is 1 when column is part of primary key

            let type_oid = column.type_id().cast_unsigned();
            let data_type = tokio_postgres::types::Type::from_oid(type_oid)
                .unwrap_or(tokio_postgres::types::Type::TEXT); // Fallback for unknown types

            let type_name = data_type.name().to_owned();
            // For CDC-discovered types, use the resolved cache type name.
            // Unknown types fall back to TEXT, which resolves to "text" for cache.
            let cache_type_name =
                cache_type_name_resolve(&data_type).unwrap_or_else(|_| "text".to_owned());

            let column_metadata = ColumnMetadata {
                name: column.name().unwrap_or("unknown_column").into(),
                position: i16::try_from(idx + 1).expect("column position fits in i16"),
                type_oid,
                data_type,
                type_name: type_name.into(),
                cache_type_name: cache_type_name.into(),
                is_primary_key,
            };

            if is_primary_key {
                primary_key_columns.push(column.name().unwrap_or("unknown_column").into());
            }

            columns.push(column_metadata);
        }

        TableMetadata {
            name: table_name,
            schema: schema_name,
            relation_oid,
            primary_key_columns,
            columns: ColumnStore::new(columns),
            indexes: Vec::new(), // Indexes are queried separately in cache_table_register
        }
    }

    /// Parse row data from InsertBody into a Vec of column values indexed by position.
    fn parse_insert_row_data(&self, body: InsertBody) -> Result<Vec<CdcValue>, Error> {
        Ok(tuple_data_parse(body.into_tuple().into_data()))
    }

    /// Parse old and new row data from UpdateBody into Vecs of column values indexed by position.
    #[allow(clippy::type_complexity)]
    fn parse_update_row_data(
        &self,
        body: UpdateBody,
    ) -> Result<(Vec<CdcValue>, Vec<CdcValue>), Error> {
        let (key_tuple, _old_tuple, new_tuple) = body.into_tuples();
        let new_row_data = tuple_data_parse(new_tuple.into_data());

        let key_data = key_tuple
            .map(|kt| tuple_data_parse(kt.into_data()))
            .unwrap_or_default();

        Ok((key_data, new_row_data))
    }

    /// Parse row data from DeleteBody into a Vec of column values indexed by position.
    fn parse_delete_row_data(&self, body: DeleteBody) -> Result<Vec<CdcValue>, Error> {
        // DeleteBody contains either key_tuple (for tables with REPLICA IDENTITY USING INDEX)
        // or old_tuple (for tables with REPLICA IDENTITY FULL)
        let (key_tuple, old_tuple) = body.into_tuples();
        let Some(tuple) = key_tuple.or(old_tuple) else {
            // No tuple data available (REPLICA IDENTITY NOTHING)
            error!("DELETE operation requires REPLICA IDENTITY FULL or USING INDEX, found NOTHING");
            return Ok(Vec::new());
        };

        Ok(tuple_data_parse(tuple.into_data()))
    }

    /// Check if there are any cached queries for a specific table by relation OID.
    /// Reads the shared active relations set maintained by the writer — no messaging needed.
    fn is_relation_active(&self, relation_oid: u32) -> bool {
        self.active_relations.load().contains(&relation_oid)
    }
}

/// `tuple_data_parse` reuses the source Vec's allocation via the in-place
/// collect specialization, which holds only while the element layouts match.
/// A size change downgrades it to a silent per-event allocation — fail the
/// build instead so the regression is visible.
const _: () = assert!(
    std::mem::size_of::<TupleData>() == std::mem::size_of::<CdcValue>()
        && std::mem::align_of::<TupleData>() == std::mem::align_of::<CdcValue>(),
);

/// Convert replication TupleData into column values, preserving the
/// unchanged-toast marker distinctly from NULL (PGC-264). Consumes the Vec so
/// the conversion is in place — no per-event allocation (see the layout
/// assertion above).
fn tuple_data_parse(columns: Vec<TupleData>) -> Vec<CdcValue> {
    columns
        .into_iter()
        .map(|col| match col {
            TupleData::Null => CdcValue::Null,
            TupleData::UnchangedToast => CdcValue::Toasted,
            // Zero-copy: the value is a refcounted view of the replication
            // frame. PG sends valid UTF-8 in text format; an invalid sequence
            // falls back to a lossy copy rather than dropping the event.
            TupleData::Text(data) => CdcValue::Text(
                ByteString::from_utf8(data.clone())
                    .unwrap_or_else(|_| ByteString::from(String::from_utf8_lossy(&data).as_ref())),
            ),
            TupleData::Binary(_) => unreachable!("pgcache uses text-format replication"),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tuple_data_parse_preserves_unchanged_toast() {
        let columns = vec![
            TupleData::Null,
            TupleData::UnchangedToast,
            TupleData::Text("abc".as_bytes().into()),
        ];
        assert_eq!(
            tuple_data_parse(columns),
            vec![
                CdcValue::Null,
                CdcValue::Toasted,
                CdcValue::Text("abc".into()),
            ]
        );
    }
}
