# ADR-028: Dynamic Runtime Configuration

## Status
Accepted

## Context
pgcache configuration was fully static — parsed once from TOML + CLI + env at startup and passed as `&Settings` throughout the codebase. Operators had to restart the process to change any setting, including cache size, eviction policy, admission threshold, table allowlist, or log level. This is incompatible with infrastructure-as-code workflows (Terraform, Ansible) that expect to read and modify service configuration without interrupting traffic.

A subset of settings can safely change at runtime (cache behavior, log level), while others are structurally bound to startup (listen sockets, worker thread count, DB connections, replication slots). The TOML file must remain the source of truth so that runtime changes survive restart and so that file-based IaC tools keep working.

## Decision
Split `Settings` into dynamic and static fields, expose the dynamic subset through an HTTP admin API, and keep the TOML file authoritative.

### Dynamic vs Static Split
Dynamic fields (runtime-adjustable): `cache_policy`, `admission_threshold`, `allowed_tables`, `excluded_tables`, `log_level`, `mv_size_ratio`, `mv_compute_min_rows`, `memo_cache_size`, `memory_limit`, `disk_limit` (and the deprecated, ignored `cache_size`). These are read where they are used, on query dispatch, registration or the writer's tick, and need no process-level resource rebuilt.

Static fields (restart-required): DB connections, CDC settings, listen/metrics addresses, worker count, TLS cert paths, pinned queries, telemetry flag. These are bound to process-level resources or startup-only registration.

### Lock-Free Config Sharing
Dynamic config is stored in an `Arc<ArcSwap<DynamicConfig>>` held by a cloneable `DynamicConfigHandle`. Readers (coordinator, writer, CDC handler) call `.load()` on every access — a single atomic load with no CAS. Writers call `.store()` with a new `Arc<DynamicConfig>`, which is infrequent.

### HTTP Admin API
Three endpoints added to the existing admin server:
- `GET /config` — returns current effective dynamic config and a `restart_required` flag indicating whether static fields in the file have drifted from the running process
- `PUT /config` — accepts a partial `DynamicConfigPatch`, writes it to the TOML file (format-preserving) when there is one, applies it to the running config, and swaps the result in
- `POST /config/reload` — re-reads the TOML file, resolves its dynamic fields under the startup flags and environment as startup did, and swaps them in (for file-based IaC that edits the TOML directly)

### TOML File as Source of Truth
All `PUT /config` changes go through the file. `toml_edit` performs a format-preserving read-modify-write on the TOML document, and the same patch is applied to the running `DynamicConfig`; a failed file write refuses the change. This keeps the file authoritative: a restart reads the same effective config, and version-controlled config files retain comments and ordering.

### Static Drift Detection
A `StaticConfigSnapshot` is captured at startup and stored on the handle with the flags it was resolved under. `GET /config` resolves the TOML file as it stands under those flags and compares; if any static field differs, the response sets `restart_required: true`. This tells operators when a file edit will only take effect after restart.

### Dynamic Log Level via Reload Layer
Logging uses `tracing_subscriber::reload::Layer` instead of a static `EnvFilter`. A type-erased reload function is stored on the handle; when `log_level` changes, the filter is swapped in-place.

## Rationale
- **ArcSwap on the hot path** — query dispatch and eviction read config on every call. Lock-free reads keep the common case as cheap as reading a static `&Settings`
- **TOML as source of truth** — restarts, file-based IaC, and human edits all converge on the same file. Alternatives (in-memory-only runtime changes, dual-source-of-truth with reconciliation) would create surprising restart behavior
- **Format-preserving writes** — config files live in version control; destroying comments and key ordering on every `PUT` would make the feature unusable for IaC
- **Partial `PUT` payload** — IaC tools commonly send only the fields they manage. Requiring a full config object would force clients to track fields they don't own
- **Split dynamic vs static at the type level** — encoding the split into `DynamicConfig` (behind the ArcSwap) vs the rest of `Settings` prevents accidental dynamic-mutation of fields that aren't actually reloadable
- **Restart-required flag** rather than silently ignoring static-field edits — operators get explicit feedback that a file change won't take effect

## Consequences

### Positive
- IaC tools (Terraform, Ansible) can manage pgcache config without restart loops
- Hot-path readers pay no synchronization cost for dynamic config access
- Runtime changes persist across restarts because the TOML file is updated in place
- Log level can be raised for debugging without process restart
- Static drift is surfaced explicitly, not silently ignored

### Negative
- Two paths to change config (TOML edit + reload, or direct `PUT`) means operators must pick one convention per environment to avoid confusion
- Format-preserving TOML edits add a dependency (`toml_edit`) and slightly more complex write logic
- Splitting fields into dynamic vs static is a manual classification — adding a new setting requires deciding which bucket it belongs in
- The admin API currently has no authentication; production deployments must rely on bind-address isolation

## Implementation Notes
- `DynamicConfig`, `DynamicConfigHandle`, `DynamicConfigPatch` and `StaticConfigSnapshot` live in `src/settings/dynamic.rs`, and the TOML read-modify-write functions in `src/settings/toml_file.rs`, as an extension of the settings module
- HTTP handlers (`config_get_handle`, `config_put_handle`, `config_reload_handle`) live in `src/admin.rs` with the other admin server endpoints
- `LogReloadHandle` in `src/settings/dynamic.rs` holds type-erased closures over the tracing reload handle, keeping the `tracing_subscriber` types out of the settings module's public API
