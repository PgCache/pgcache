# pgcache/src Structure & Cohesion Review

This review covers 73 files under `src/`. It found 45 structural findings: 16 high, 22 medium, and 7 low priority. The structural debt concentrates in three subsystems: the cache **writer** (`cdc.rs`, `core.rs`, `registration.rs` together account for ~7,700 production lines in three god-impl files), the **query** layer (`resolved.rs`, `decorrelate.rs`, `ast/types.rs`, plus two mis-named AST test files), and the **proxy connection** (`connection.rs` is a 2,616-line god struct). Cross-cutting concerns — duplicated traversal/parsers and code parked under misleading filenames — recur across the AST and constraint modules. Two files (`ast/convert.rs`, `ast/expr_parse.rs`) are misleadingly named: their bulk is inline tests while the production logic lives elsewhere.

## High priority

### `src/cache/writer/cdc.rs`
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **length**: ~4,114 production lines (only ~215 tests), ~6x the 700-line smell threshold and the largest production file in the tree. One `impl WriterCdc` bundles at least five distinct subsystems (CDC source-txn frame state machine, segment/membership batched re-evaluation, TOAST value repair, grow/shrink invalidation rules, per-DML handlers); a reader cannot hold all concerns at once and unrelated edits collide. → Promote to a `writer/cdc/` directory: `mod.rs` = `WriterCdc` struct + re-exports; split subsystems into the responsibility submodules below.
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **cohesion**: the PGC-241 batched-eval machinery (`SegmentMembership`, `RelationBatch`, `BatchEvalView`, `PreparedEvalKey`, `SegmentRows`, `segment_eval`, membership/row-change chunk builders) is a self-contained ~600-line concern with its own types and SQL builders, fully separable from the frame state machine and the per-op handlers that only consume the matrix. → Extract into `writer/cdc/segment_eval.rs`.
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **cohesion**: the PGC-264 TOAST-repair concern (`PendingRepairSlot`, `ToastResolution`, `toast_repair_events`, overlay helpers, `toast_fallback_build`, `toast_lookup_batch`, `toast_unexpected_invalidate`) is a ~450–600 line two-pass algorithm with its own types, unrelated to membership eval or the txn lifecycle; MEMORY records this as a recognized seam. → Extract into `writer/cdc/toast_repair.rs`, moving pass-2 chain resolution into a `toast_lookup_resolve` helper.
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **cohesion**: the grow/shrink invalidation decision rules (`row_uncached_invalidation_check`, `row_cached_invalidation_check`, `update_queries_check_invalidate`, `toast_fallback_structural_invalidate`, `row_constraints_match`, `join_membership_unchanged`) are the consistency-model policy layer — pure decision logic with no I/O — interleaved with I/O-heavy handlers and SQL builders, so consistency rules cannot be reasoned about in isolation. → Extract the predicates into `writer/cdc/invalidation.rs`; drop the thin `row_constraints_match` wrapper and call the free fn directly.

### `src/cache/writer/core.rs`
- [`src/cache/writer/core.rs`](src/cache/writer/core.rs) — **god_struct**: ~1,890 production lines. One `impl WriterCore` mixes at least six unrelated responsibilities (CDC frame/batch buffer pools, active-relations + publication sync, state_view transitions, Prometheus gauge emission, disk-pressure + eviction, generation GC + status). Several clusters share almost no state, so the file reads as a grab-bag. → Keep the struct + `writer_run` loop; extract disk/eviction into `eviction.rs`, publication/active-relations into `publication.rs`, gauges+status into `status.rs`, the frame-buffer pool into `frame.rs`.

### `src/cache/runtime.rs`
- [`src/cache/runtime.rs`](src/cache/runtime.rs) — **cohesion**: ~1,386 production lines (no tests) mixing 4–5 unrelated long-lived subsystems behind no common type — startup DB reset, thread/supervisor scaffolding, the serve event loop + pool, two AIMD control loops (`memory_monitor`, `reg_gate_controller`), and the CDC reconnect driver. The `runtime` name hides all of these; a reader cannot find the registration-rate controller by filename. → Split into `cache/runtime/` submodules: `serve_pool.rs`, `reg_gate.rs`/`memory_monitor.rs`, `cdc_driver.rs`, `reset.rs`; leave supervision/thread setup in `runtime.rs`.
- [`src/cache/runtime.rs`](src/cache/runtime.rs) — **function_length**: `reg_gate_controller` and `memory_monitor` are each ~110–176 lines of dense control-loop logic (constants, EWMA/window state, per-tick decision math) inline in a lifecycle file, each carrying a large doc-comment for a standalone algorithm. → Move each controller (with constants and helper math) into its own module, leaving runtime to just spawn them.

### `src/cache/query_cache.rs`
- [`src/cache/query_cache.rs`](src/cache/query_cache.rs) — **god_struct**: ~1,430 production lines anchored on `CacheDispatch` with a sprawling impl — the ~210-line `query_dispatch` state machine plus serve-building (`hit_serve`, `pool_serve*`, `memo_serve*`), coalesce drain, registration, and CAS/admission helpers — making the dispatch decision logic hard to isolate from serve-emission and drain. → Keep `CacheDispatch` + `query_dispatch` here; extract coalesce-drain and serve-emission into `cache/dispatch/serve.rs` and `cache/dispatch/coalesce.rs`.

### `src/cache/types.rs`
- [`src/cache/types.rs`](src/cache/types.rs) — **cohesion**: all ~673 lines are production and the file is a grab-bag rather than the house-style "types + glue" — the cached-query family, the update-query/CDC-classification family (`UpdateQuery`/`UpdateQueries` with real accounting logic), the `Cache` struct + impl, `RegGate` (a full BBR-lite controller), `QueryMetrics`, and `CacheStateView`. → Extract `RegGate` into `reg_gate.rs` and the update-query family into `update_query.rs`, leaving `CachedQuery`/`CacheStateView`/`Cache`/`QueryMetrics` in `types.rs`.

### `src/query/resolved.rs`
- [`src/query/resolved.rs`](src/query/resolved.rs) — **cohesion**: ~2,148 production lines (no tests). Three unrelated concerns interleave type-by-type across ~20 `Resolved*` types — SQL re-emission (`Deparse` impls), CDC subquery-branch collection, and complexity/analysis — so a reader fixing deparse parenthesization must wade past CDC negation-flipping on the same enum. → Promote to a `query/resolved/` directory: `mod.rs` = type defs + traversal + accessors; `resolved/deparse.rs` for `Deparse` impls; `resolved/subquery_collect.rs` for the collection family.

### `src/query/decorrelate.rs`
- [`src/query/decorrelate.rs`](src/query/decorrelate.rs) — **cohesion**: ~1,537 production lines in one flat file holding shared types/state, correlation-predicate extraction and outer-ref matching, correlation-detection walkers, inner-query prepare/validation, four transform families (EXISTS/NOT EXISTS, IN/NOT IN, scalar), and the top-level dispatcher. Sibling complex modules (`query/ast/`, `query/transform/`) are split by responsibility; decorrelate is the outlier. → Promote to a `decorrelate/` directory: `mod.rs` (error/outcomes/predicates/state + re-exports), `predicate.rs`, `exists.rs`, `in_subquery.rs`, `scalar.rs`, `driver.rs`.

### `src/query/ast/types.rs`
- [`src/query/ast/types.rs`](src/query/ast/types.rs) — **length**: ~2,519 production lines (no tests), ~4x the threshold. Cohesive at one altitude but it concatenates four distinct node families (predicate/scalar expression, query-structure, FROM/table, window/function), each a self-contained struct/enum + `AstNode` + `Deparse` + helpers; the three trait impls per type are scattered far apart. → Split into a `types/` submodule by node family: `expr.rs`, `query.rs`, `table.rs`, `window.rs`; keep `types/mod.rs` as re-exports.

### `src/query/ast/convert.rs`
- [`src/query/ast/convert.rs`](src/query/ast/convert.rs) — **naming**: 3,835 lines but only lines 1–27 are production (`select_node_fingerprint`, `query_expr_fingerprint`); the rest is one `#[cfg(test)] mod tests`. The actual conversion lives in `convert_raw.rs`, so the filename (and CLAUDE.md) actively misleads. **The file's bulk is inline tests.** → Move the two fingerprint fns to `ast/fingerprint.rs` and relocate the test block to a module named for what it covers, retiring the stale `convert.rs` name; update `ast/mod.rs`.

### `src/query/ast/expr_parse.rs`
- [`src/query/ast/expr_parse.rs`](src/query/ast/expr_parse.rs) — **naming**: the filename and CLAUDE.md promise expression/WHERE parsing, but the file is 100% a `#[cfg(test)] mod tests` block (~1,376 lines of parse assertions) with zero production code; the actual parsing lives in `convert_raw.rs`. **The file is entirely inline tests.** → Rename to read as tests and co-locate with the parser (inline `#[cfg(test)] mod tests` in `convert_raw.rs` or a sibling `convert_raw_tests.rs`); update `mod.rs`.

### `src/proxy/connection.rs`
- [`src/proxy/connection.rs`](src/proxy/connection.rs) — **god_struct**: 2,616 production lines (no tests), ~4x the threshold and the hardest file in the module to navigate. `ConnectionState` is a god struct with ~24 fields and ~35 methods spanning origin/egress buffers, the extended-query pipeline, search_path discovery, a describe-response LRU cache, telemetry, and the core relay — all through one type with a single >1,000-line impl. → Split into `proxy/connection/` submodules operating on the shared state: `extended.rs`, `search_path_intercept.rs`, `describe_cache.rs`, `telemetry.rs`, leaving the relay loop + fields in `connection.rs`/`state.rs`.
- [`src/proxy/connection.rs`](src/proxy/connection.rs) — **cohesion**: at least five independently-testable sub-models are bundled inline — the extended-query buffering model (`Segment`, `ExecuteEntry`, `ExtendedBuffer`, `ExtendedPending`, `DispatchContext`, `CacheCandidate` + handlers), search_path interception state machines, the per-connection describe-response cache, query telemetry, and the event loop + origin TCP/TLS connect. → Extract the extended-protocol types+handlers into `connection/extended.rs` (the largest cohesive cluster), search_path intercept into `connection/search_path_intercept.rs`, the describe cache into `connection/describe_cache.rs`, telemetry into `connection/telemetry.rs`.

### `src/settings.rs`
- [`src/settings.rs`](src/settings.rs) — **length**: ~1,256 production lines mixing four responsibilities — config value types, the `DynamicConfig` + ArcSwap runtime handle with log-reload/patch/apply, TOML persistence via `toml_edit`, and the full CLI layer (`CliArgs`, `cli_args_parse`, `settings_build_*`, `print_usage`, `*_resolve`). A reader chasing the CLI parser wades through ArcSwap internals and toml_edit serialization. → Promote to a `settings/` directory: `mod.rs` = value types + re-exports; `cli.rs`, `dynamic.rs`, `toml_file.rs`.

### `src/metrics.rs`
- [`src/metrics.rs`](src/metrics.rs) — **cohesion**: 977 lines mixing the metrics registry (`names` module, the eight `*Handles` structs, `Handles::build`, recorder install) with the admin HTTP server (`admin_server_spawn`/`run`, the `/status`, `/config` GET/PUT, `/config/reload`, `/healthz`/`readyz`/`metrics` handlers, `json_error`, `ConfigGetResponse`). The HTTP server is its own networking concern and is hard to find behind a filename advertising metrics. → Extract the admin HTTP server into `src/admin.rs`; leave `metrics.rs` as names + `Handles` + recorder install.

## Medium priority

### `src/cache/writer/cdc.rs`
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **cohesion**: the per-row PgEval + SQL-emission helpers (`pg_eval_matches/any/chunk_row`, `cache_predicate_into`, `cache_upsert_unconditional_into`, `cache_delete_into`, `cdc_on_conflict_tail_append`, `truncate_sql_build`) are a distinct SQL-building concern living ~2,500 lines from their batched counterparts, so the inline-vs-batched builders that must stay contract-compatible are far apart. → Extract into `writer/cdc/sql.rs`, or co-locate with the batched builders in `segment_eval.rs`.
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **misplaced**: an `impl WriterCore` block (`cache_table_invalidate`, `cache_query_evict`) is eviction/invalidation lifecycle for `WriterCore`, placed in the `WriterCdc` file only because CDC calls it; a reader looking for `cache_query_evict` won't find it where `WriterCore` lives. → Relocate to `core.rs` (or a `writer/eviction.rs`).
- [`src/cache/writer/cdc.rs`](src/cache/writer/cdc.rs) — **function_length**: `cdc_command_handle` (~280 lines) is a match-arm-per-command body inlining substantial logic — the `TableRegister` arm (~75 lines) handles intra-txn DDL detection, frame-recovery escalation, retention filtering, and prepared-statement invalidation; the `CommitMark` arm inlines a multi-condition flush decision. → Keep it a thin dispatcher and extract per-command bodies (`table_register_handle`, `commit_mark_handle`), ideally in `writer/cdc/dispatch.rs`.

### `src/cache/writer/core.rs`
- [`src/cache/writer/core.rs`](src/cache/writer/core.rs) — **misplaced**: the CDC frame-state vocabulary (`FrameRowEvent`, `ToastOverlayEntry`, `FrameState` enums + ~280 lines of frame-specific fields and the row/toast pool methods) lives in `core.rs` but is consumed almost entirely by the CDC apply path in `cdc.rs`, diluting the "shared core" framing. → Move into `writer/frame.rs`, leaving `WriterCore` holding only the fields.
- [`src/cache/writer/core.rs`](src/cache/writer/core.rs) — **function_length**: `status_respond` (~150–187 lines) is dominated by a 14-field tuple destructure of per-query metrics built inline with inline `LatencyStats` construction, easy to mis-order; `state_view_write` (~228 lines) similarly does several sub-tasks inline. → Extract the metrics-to-`QueryStatusData` mapping (and the `LatencyStats` build) into a helper, and break `state_view_write` into named sub-steps.

### `src/cache/writer/registration.rs`
- [`src/cache/writer/registration.rs`](src/cache/writer/registration.rs) — **cohesion**: ~1,683 production lines mixing three concerns — the `WriterRegistration` command impl, a self-contained subsumption sub-feature (`subsumption_check`/`query_subsume`, ~200 lines), and a cluster of pure WHERE/column classifiers (`update_eval_strategy_classify`, `pg_batchable_classify`, `limit_window_columns_collect`, `predicate_columns_collect`) that are query-shape analysis with their own `classify_tests`. → Extract the four classifiers (+ tests) into `writer/update_classify.rs` and subsumption into `writer/subsumption.rs`.

### `src/cache/query_cache.rs`
- [`src/cache/query_cache.rs`](src/cache/query_cache.rs) — **misplaced**: two self-contained support types sit in the dispatch file — `CoalesceQueue` (Mutex-wrapped queue with an enqueue/drain ordering invariant, ~60 lines) and `RegRateBucket` (token-bucket admission pacer with env-override parsing and refill math, ~60 lines). Neither is part of `CacheDispatch`; both are independently testable and bury the dispatch flow. → Move `CoalesceQueue` to `cache/coalesce_queue.rs` and `RegRateBucket` to `cache/reg_bucket.rs` (or the reg_gate module).

### `src/cache/messages.rs`
- [`src/cache/messages.rs`](src/cache/messages.rs) — **cohesion**: ~515 production lines mixing three message domains plus behavior that doesn't belong with message enums — proxy↔cache messages (with `CacheMessage::into_query_data` substitution logic), writer lifecycle commands (`QueryCommand` + a ~45-line hand-written Debug, `PopulationMerge`/`AdmitAction`/`MvBuildOutcome`/`SubsumptionResult`), and CDC commands/values (`CdcCommand`, `CdcValue`, `cdc_values_convert`); `QueryParameters`/`QueryParameter` is a fourth self-contained concern. → Split along command domains: writer commands into a `query_command` submodule (with `into_query_data`), CDC commands into a `cdc_command` submodule (with `cdc_values_convert`), keeping proxy↔cache in `messages.rs`; optionally move `QueryParameters` out.

### `src/cache/serve.rs`
- [`src/cache/serve.rs`](src/cache/serve.rs) — **cohesion**: ~1,010 production lines; the core serve flow (`handle_cached_query` + `Relay` + `relay_frame_apply` + `serve_query_send/finish`) is cohesive, but several independent helpers are bolted on — the wire-level SQLSTATE parser (`sqlstate_extract` + its own tests), the coalesce broadcast machinery (`BroadcastState`, `broadcast_setup/join/error_reply`, `push_and_broadcast`), the `ConnectionGuard` pool-return type, and `limit_bind_text`. → Extract broadcast/coalesce helpers into `serve/broadcast.rs` and the SQLSTATE parser into `serve/sqlstate.rs`; move `ConnectionGuard` next to the pool code.

### `src/query/resolve.rs`
- [`src/query/resolve.rs`](src/query/resolve.rs) — **cohesion**: ~1,369 production lines. The USING/NATURAL join resolution is a self-contained, densely special-cased cluster (`MergedJoinColumn`, `JoinScopeRanges`, `join_side_qualifier`, `join_natural_common_columns`, `join_side_column_resolve`, `join_using_resolve`, `join_using_or_cross`, plus merged-column handling threaded into column/scalar/select resolution) embedded inside the general resolver. → Extract into `query/resolve/join_using.rs`, exposing `merged_columns` via a small interface on the scope.

### `src/query/constraints.rs`
- [`src/query/constraints.rs`](src/query/constraints.rs) — **cohesion**: ~1,115 production lines mixing two vocabularies — pure value-domain range algebra/subsumption math (`RangeBound`, `ColumnRange`, range builders, `range_subsumes_range`, `column_range_subsumes`, `literal_value_order`) and AST-walking constraint extraction over `ResolvedWhereExpr` (`analyze_constraint_expr`, between/in/not_in/any_eq extractors, `collect_query_constraints`, `propagate_constraints`); the range algebra is also consumed by `constraint_index.rs`. → Split into `query/constraints/`: `range.rs`, `extract.rs`, public types + entry points in `mod.rs`.

### `src/query/constraint_index.rs`
- [`src/query/constraint_index.rs`](src/query/constraint_index.rs) — **cohesion**: ~1,050 production lines stacking four separable layers — the public `ConstraintIndex` + `ColumnSet` + classify/powerset/project helpers, the `ValueKey` key type + placement classification, the `ComplexIndex`/`ColumnIndex` containment data structures with their posting-list helpers, and the CDC row-coercion helper `row_value_forms`. → Split into `query/constraint_index/`: `value_key.rs`, `column_index.rs`, `row_forms.rs`, leaving `ConstraintIndex`/`ColumnSet`/classify in `mod.rs`.

### `src/query/decorrelate.rs`
- [`src/query/decorrelate.rs`](src/query/decorrelate.rs) — **function_length**: `select_node_decorrelate` (~210 lines) mixes orchestration with five near-identical match arms (EXISTS, NOT EXISTS, IN, NOT-IN/ALL, NOT(ANY)) that each repeat the same shape, plus deeply nested `matches!`-with-guard patterns for NOT-wrapped subqueries; the four join-builder functions share large verbatim ON/WHERE-merge and IS-NULL blocks. → Extract a helper classifying a conjunct into a join-decorrelation kind and funnel each arm through one shared apply path; factor the shared semi-join/anti-join assembly into private builders when the transform families move out.

### `src/query/transform/values.rs`
- [`src/query/transform/values.rs`](src/query/transform/values.rs) — **cohesion**: ~521 production lines mixing the three public table-source replacement transforms (`replace_with_values`/`_batch`/`_unnest`) and a large private alias-rewrite engine (`resolved_*_alias_update` family, ~200 lines) that fully recursively walks the resolved AST rewriting column table-aliases, obscuring the transform entry points and duplicating resolved-tree recursion found elsewhere. → Extract the alias-rewrite walk into `values/alias_rewrite.rs` (or route through a shared resolved-walk module), leaving `values.rs` as the source-replacement transforms.

### `src/query/resolved.rs`
- [`src/query/resolved.rs`](src/query/resolved.rs) — **duplication** (cross-cutting): see Cross-cutting section.

### `src/query/ast/convert_raw.rs`
- [`src/query/ast/convert_raw.rs`](src/query/ast/convert_raw.rs) — **cohesion**: ~1,686 production lines holding several internally-cohesive sub-converters plus distinct post-conversion passes — query/SELECT shape conversion, scalar-expr/function/window conversion, a self-contained WHERE-clause converter with its own error domain (`WhereParseError` rather than `AstError`), and a window-reference resolution pass. The two error-handling regimes are visually mixed mid-file. → Extract the WHERE-clause converter into `convert_raw/where.rs` (making the error boundary explicit) and window/window-ref resolution into `convert_raw/window.rs`.

### `src/proxy/connection.rs`
- [`src/proxy/connection.rs`](src/proxy/connection.rs) — **misplaced**: `OriginStream`/`OriginReadHalf`/`OriginWriteHalf` aliases, `origin_stream_from_tls`, and `origin_connect` are the origin-side analog of `client_stream.rs` but sit inline in `connection.rs`; the symmetry is broken and the connect/TLS plumbing is buried in the protocol file. → Add `proxy/origin_stream.rs` mirroring `client_stream.rs`.

### `src/proxy/search_path.rs`
- [`src/proxy/search_path.rs`](src/proxy/search_path.rs) — **cohesion**: one file holds two unrelated responsibilities sharing only the "search_path" topic — raw pg_query parse-tree classification of search_path-mutating statements (`MutationKind`, `SearchPathMutations`, `stmt_classify_raw`, `search_path_mutations_raw` — unsafe FFI) and parsing/resolving a search_path string value (`SearchPathEntry`, `SearchPath::parse/resolve` — pure safe string handling), with separate test groups and opposite safety profiles. → Split mutation-classification into `search_path/mutations.rs`, keeping the string parse/resolve half in `search_path/value.rs`.

### `src/pg/cache_connection.rs`
- [`src/pg/cache_connection.rs`](src/pg/cache_connection.rs) — **cohesion**: ~590 production lines mixing connection lifecycle/state (`CacheConnection`, `ParkedConnection`, `connect`/`startup_handshake`/`frame_next`), the prepared-statement FIFO registry (`PreparedStatements`, `PrepareOutcome`), and a cohesive group of frontend wire-encode free functions (`startup_message_build`, `frontend_msg_append`, `bind_text/value_write`, `extended_query_build`, ~190 lines). The encode helpers are pure byte-assembly paralleling the existing `protocol/encode.rs` split. → Move the frontend message-building free functions (+ constants) into `protocol/encode.rs` (or `protocol/frontend_encode.rs`); optionally lift `PreparedStatements`/`PrepareOutcome` to a sibling.

### `src/memory.rs`
- [`src/memory.rs`](src/memory.rs) — **cohesion**: ~444 production lines holding two separable concerns — pure, unit-testable budget/cap decision math (`throttle_evaluate`, `count_cap_evaluate`, `cap_rate_limit`, `disk_reserve_auto`, `disk_limit_resolve` + constants and Decision structs) versus OS-specific introspection (the three cfg-gated `sys` modules reading /proc, cgroup, sysctl + parsers). The platform layer buries under the algorithm docs. → Split platform introspection (sys modules + parsers + the wrappers) into `memory/sys.rs`, leaving the pure decision math in `memory.rs`.

### `src/query/resolve.rs`, `src/query/constraints.rs` cross-references
- See Cross-cutting for the resolved-AST traversal duplication that also touches these consumers.

## Low priority

### `src/cache/writer/registration.rs`
- [`src/cache/writer/registration.rs`](src/cache/writer/registration.rs) — **misplaced**: the file ends with a lone `impl WriterCore` block holding `cache_table_register` (CDC-driven table re-registration), unrelated to `WriterRegistration`'s query-registration path; its placement is incidental to the table-DDL methods already in `table.rs`. → Move `cache_table_register` into `table.rs` alongside the other cache-table DDL methods.

### `src/cache/query_cache.rs`
- [`src/cache/query_cache.rs`](src/cache/query_cache.rs) — **cohesion**: the arc-swap publish/subscribe plumbing (`CacheDispatchHandle`/`Publisher`/`Updater`/`Subscriber`, ~70 lines) and the wire-level request/serve message structs (`QueryRequest`, `ServeRequest`, `CoalescedClient`) are lifecycle wiring and message types unrelated to per-query dispatch. → Move the handle types to `cache/dispatch_handle.rs` and the request/serve structs to `cache/messages.rs` (or `types.rs`).

### `src/cache/serve.rs`
- [`src/cache/serve.rs`](src/cache/serve.rs) — **function_length**: `handle_cached_query` (~270 lines) builds the connection guard, fault-injects, sends the query, sets up broadcast, runs the big `select!` relay loop with inline timeout/desync/client-gone handling, then does residual-byte cleanup; each `select!` arm contains substantial inline failure logic. → Extract the residual-byte cleanup and per-arm failure handling into named helpers so the function reads as setup → loop → finish.

### `src/cache/cdc.rs`
- [`src/cache/cdc.rs`](src/cache/cdc.rs) — **cohesion**: ~710 production lines; `CdcProcessor`'s impl mixes the replication-stream state machine (run loop, LSN tracking, standby-status/keepalive) with pgoutput decoding helpers (`parse_relation_to_table_metadata`, `parse_insert/update/delete_row_data`, `tuple_data_parse` + layout assert). The decoders are a distinct wire-tuple → `TableMetadata`/`CdcValue` concern from the stream/ack protocol. → Extract decoders into `cache/cdc/decode.rs`, leaving `CdcProcessor` focused on the stream loop and LSN/standby protocol.

### `src/query/resolve.rs`
- [`src/query/resolve.rs`](src/query/resolve.rs) — **cohesion**: `ResolutionScope` and its helpers (`new`/`new_with_outer`, `scope_tables_snapshot`, `outer_table_scope_find`, table/derived-table add/find, `column_matches_find`, `merged_column_find`, `subquery_resolve`) plus `derived_table_columns_extract` form a cohesive "resolution scope" unit (~first 315 lines) conceptually separate from the per-node `resolve_*` free functions that consume it. → Move `ResolutionScope` (and `derived_table_columns_extract`) into `query/resolve/scope.rs`.

### `src/pg/protocol/extended.rs`
- [`src/pg/protocol/extended.rs`](src/pg/protocol/extended.rs) — **cohesion**: ~530 production lines mixing stateless extended-protocol message PARSERS (`parse_parse/bind/execute/describe/close_message`, `parse_parameter_description`, `read_cstring`, `count_to_usize`) with stateful proxy-session TYPES (`PreparedStatement` carrying origin_prepared/describe-cache/parse_bytes, `Portal`, `StatementType`, `ResultFormats`). → Keep the `Parsed*Message` DTOs and `parse_*` functions here; relocate the session-state types to a state/types module.

### `src/catalog.rs`
- [`src/catalog.rs`](src/catalog.rs) — **cohesion**: the `Oid` newtype with its `Display`, `ToSql`, and `FromSql` impls (~65 lines) is a standalone type-safety primitive sitting between the module doc and the unrelated `ColumnStore`/`TableMetadata` structs. The file is otherwise cohesive and under the size threshold. → Optional: move `Oid` + its SQL/Display impls to `oid.rs` (or alongside the related `Fingerprint`/`IdHasher` primitives); not urgent.

## Cross-cutting (duplication & misplaced code)

### Duplicated resolved-AST traversal
- [`src/query/resolved.rs`](src/query/resolved.rs) — **duplication** (medium): resolved-AST traversal is hand-rolled redundantly. `resolved.rs` reimplements three near-identical recursive walks per node type, and consumers each re-walk by matching on Join/SetOp/Subquery branches — [`src/query/transform/values.rs`](src/query/transform/values.rs) (`resolved_*_alias_update`), [`src/query/transform/pushdown.rs`](src/query/transform/pushdown.rs) (column-remap), `transform/parameters/resolved_parameterize.rs`, [`src/query/decorrelate.rs`](src/query/decorrelate.rs) (21 `ResolvedTableSource` arms), [`src/query/constraints.rs`](src/query/constraints.rs). `transform/walk.rs` provides a visitor for the UNRESOLVED AST but there is no resolved-AST equivalent, so every consumer re-derives the recursion and risks missing a branch. → Add a resolved-AST walker mirroring `transform/walk.rs` (a `ResolvedWalkerMut` + shared immutable `for_each`) and route the alias-update, column-remap, and parameterize passes through it.

### Duplicated boolean parsers
- [`src/query/evaluate.rs`](src/query/evaluate.rs) — **duplication** (low): two near-duplicate postgres-boolean text parsers with mirror-image names and divergent accept-sets — `pg_bool_parse` (evaluate.rs:242, accepts only t/f/true/false) and `parse_pg_bool` (cast.rs:181, accepts the full t/true/y/yes/on/1 set), both `pub`, both claiming to be the single source of truth; `literal_as_bool` already calls the cast.rs one, so the file depends on both. → Collapse to the canonical `cast.rs::parse_pg_bool`, delete `pg_bool_parse`, update `where_value_compare_string` to call the survivor.

### Duplicated Deparse for String/&str
- [`src/query/ast/mod.rs`](src/query/ast/mod.rs) — **duplication** (low): `impl Deparse for String` and `impl Deparse for &str` have byte-identical bodies (the `identifier_needs_quotes` quoting block); `EcoString` already delegates via `self.as_str().deparse(buf)` but `String` does not. → Make `impl Deparse for String` delegate to `self.as_str().deparse(buf)`, so the quoting logic lives only in the `&str` impl.

### Duplicated containment intersection
- [`src/query/constraint_index.rs`](src/query/constraint_index.rs) — **duplication** (low): `ComplexIndex::candidates` and `ComplexIndex::candidates_point` carry near-identical smallest-first per-column intersection logic, differing only in how each column's match set is produced (single range vs union over forms); a change to the intersection strategy must be made in two places. → Factor the smallest-first intersection of per-column match sets into one private helper both call after building their per-column sets.

### Duplicated TLS plumbing
- [`src/proxy/tls_stream.rs`](src/proxy/tls_stream.rs) — **duplication** (low): 598 lines, cohesive single responsibility, but the borrowed (`TlsReadHalf`/`TlsWriteHalf`) and owned (`OwnedTlsReadHalf`/`OwnedTlsWriteHalf`) variants duplicate near-identical AsyncRead/AsyncWrite/writable plumbing, and the two `TlsConnectionOps` impls for `ServerConnection`/`ClientConnection` are byte-for-byte identical — a fix must be made in 2–4 places. Not a split candidate. → Collapse the two identical `TlsConnectionOps` impls via a macro and share the borrowed/owned poll bodies through the existing free helpers.

### Repeated error-wrap closure in parameter decode
- [`src/query/transform/parameters/binary.rs`](src/query/transform/parameters/binary.rs) — **duplication** (low): production code is only ~697 lines (the bulk is ~1,580 test lines). `binary_parameter_to_literal_scalar` is a single ~350-line function combining a Kind-dispatch prelude with a 20+ arm per-OID match; each arm repeats the same `.map_err` error-wrap closure ~28 times, and the per-type text formatters (date/time/numeric/inet/interval/macaddr) are a distinct concern bolted on below the wire-decode dispatch. **A large share of the file is inline tests.** → Add a private `invalid_param(type_name, e)` helper for the repeated wrap and optionally split the canonical text formatters into `parameters/pg_text_format.rs`, leaving `binary.rs` as pure wire-decode dispatch.

## Reviewed and clean

`src/lib.rs`, `src/main.rs`, `src/result.rs`, `src/id_hash.rs`, `src/tls.rs`, `src/telemetry.rs`, `src/timing.rs`, `src/stream_utils.rs`, `src/tracing_utils.rs`

`src/query/mod.rs`, `src/query/update.rs`, `src/query/cast.rs`, `src/query/shape.rs`, `src/query/fingerprint.rs`

`src/query/ast/raw.rs`, `src/query/ast/mod.rs`

`src/query/transform/mod.rs`, `src/query/transform/walk.rs`, `src/query/transform/pushdown.rs`, `src/query/transform/constant_fold.rs`, `src/query/transform/select_node.rs`, `src/query/transform/parameters/mod.rs`, `src/query/transform/parameters/text.rs`, `src/query/transform/parameters/replace.rs`

`src/cache/mod.rs`, `src/cache/query.rs`, `src/cache/mv.rs`, `src/cache/memo.rs`, `src/cache/write_queue.rs`, `src/cache/reply.rs`, `src/cache/fast_path.rs`, `src/cache/status.rs`

`src/cache/writer/mod.rs`, `src/cache/writer/deadlock.rs`, `src/cache/writer/table.rs`, `src/cache/writer/staging.rs`, `src/cache/writer/population.rs`, `src/cache/writer/mv_build.rs`, `src/cache/writer/mv.rs`

`src/proxy/mod.rs`, `src/proxy/server.rs`, `src/proxy/egress.rs`, `src/proxy/query.rs`, `src/proxy/cache_sender.rs`, `src/proxy/client_stream.rs`

`src/pg/mod.rs`, `src/pg/cdc.rs`, `src/pg/connect.rs`, `src/pg/protocol/mod.rs`, `src/pg/protocol/backend.rs`, `src/pg/protocol/frontend.rs`, `src/pg/protocol/encode.rs`
