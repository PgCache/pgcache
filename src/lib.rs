// Query-analysis surface: compiled with and without the `proxy` feature.
pub mod cache;
pub mod catalog;
pub mod id_hash;
pub mod oid;
pub mod query;
pub mod result;
pub mod settings;

// Proxy runtime: wire protocol, connections, TLS, metrics, telemetry.
#[cfg(feature = "proxy")]
pub mod admin;
#[cfg(feature = "proxy")]
pub mod listener;
#[cfg(feature = "proxy")]
pub mod memory;
#[cfg(feature = "proxy")]
pub mod metrics;
pub mod pg;
#[cfg(feature = "proxy")]
pub mod proxy;
#[cfg(feature = "proxy")]
pub mod stream_utils;
#[cfg(feature = "proxy")]
pub mod telemetry;
#[cfg(feature = "proxy")]
pub mod timing;
#[cfg(feature = "proxy")]
pub mod tls;
#[cfg(feature = "proxy")]
pub mod tracing_utils;
