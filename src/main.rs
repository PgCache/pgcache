use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::thread::sleep;
use std::time::Duration;

use pgcache_lib::metrics::metrics_recorder_install;
use pgcache_lib::proxy::SharedProxyStatus;
use pgcache_lib::proxy::{ConnectionError, proxy_run};
use pgcache_lib::settings::Settings;
use rootcause::Report;
use tokio_util::sync::CancellationToken;

#[cfg(not(feature = "console"))]
use pgcache_lib::tracing_utils::SimpeFormatter;
use tokio::io;
use tracing::info;
#[cfg(not(feature = "console"))]
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

#[cfg(not(feature = "dhat-heap"))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[cfg(feature = "dhat-heap")]
#[global_allocator]
static ALLOC: dhat::Alloc = dhat::Alloc;

fn main() -> Result<(), Report> {
    #[cfg(feature = "dhat-heap")]
    let _dhat = dhat::Profiler::new_heap();

    // Install rustls crypto provider for TLS support
    rustls::crypto::aws_lc_rs::default_provider()
        .install_default()
        .map_err(|_| io::Error::other("crypto provider already installed"))?;

    #[cfg(feature = "hotpath")]
    let _guard = hotpath::HotpathGuardBuilder::new("pgcache")
        .percentiles(&[50.0, 95.0, 99.0])
        .format(hotpath::Format::Table)
        .build();

    let settings = Settings::from_args()?;
    let cancel = CancellationToken::new();
    let shared_proxy_status = SharedProxyStatus::new();

    let metrics_handle = metrics_recorder_install()?;

    if let Some(ref m) = settings.metrics {
        info!("Admin server will listen on http://{}", m.socket);
    }

    #[cfg(feature = "console")]
    console_subscriber::init();

    #[cfg(not(feature = "console"))]
    {
        // Log level precedence: CLI arg > config file > RUST_LOG env var > default (info)
        let filter = settings
            .dynamic
            .load()
            .log_level
            .as_deref()
            .map(EnvFilter::new)
            .or_else(|| EnvFilter::try_from_default_env().ok())
            .unwrap_or_else(|| EnvFilter::new("info"));

        let (filter_layer, reload_handle) = tracing_subscriber::reload::Layer::new(filter);
        tracing_subscriber::registry()
            .with(filter_layer)
            .with(tracing_subscriber::fmt::layer().event_format(SimpeFormatter))
            .init();

        let reload_handle_for_current = reload_handle.clone();
        settings
            .dynamic
            .log_reload_handle_set(pgcache_lib::settings::LogReloadHandle {
                reload: Box::new(move |level: &str| {
                    reload_handle
                        .reload(EnvFilter::new(level))
                        .map_err(|e| e.to_string())
                }),
                current: Box::new(move || {
                    reload_handle_for_current
                        .with_current(|filter| filter.to_string())
                        .ok()
                }),
            });
    }

    let sigint = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGINT, Arc::clone(&sigint))?;

    thread::scope(|scope| {
        let cancel_proxy = cancel.child_token();
        let shared_proxy_status = shared_proxy_status.clone();
        let metrics_handle = metrics_handle.clone();
        let proxy_handle = thread::Builder::new()
            .name("proxy".to_owned())
            .spawn_scoped(scope, || {
                proxy_run(&settings, cancel_proxy, shared_proxy_status, metrics_handle)
            })?;

        let sleep_duration = Duration::from_millis(500);

        // Wait for either SIGINT or proxy thread to finish
        while !sigint.load(Ordering::Relaxed) && !proxy_handle.is_finished() {
            sleep(sleep_duration);
        }

        // Signal all threads to shut down
        info!("shutting down");
        cancel.cancel();

        // Wait for proxy thread to finish
        let res = proxy_handle
            .join()
            .unwrap_or_else(|_panic| {
                Err(ConnectionError::IoError(io::Error::other("proxy thread panicked")).into())
            })
            .map_err(|e| e.into_dynamic());

        info!("process terminated {res:?}");

        // print the report.
        #[cfg(feature = "hotpath")]
        drop(_guard);

        res
    })
}
