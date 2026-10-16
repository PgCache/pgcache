//! Optional dial9 runtime telemetry (`dial9` feature). Configured entirely by
//! dial9's `DIAL9_*` environment variables and inert unless `DIAL9_ENABLED=true`;
//! without the feature every runtime is a plain tokio runtime.

use std::io;
#[cfg(feature = "dial9")]
use std::sync::OnceLock;
#[cfg(feature = "dial9")]
use std::time::Duration;

use tokio::runtime::{Builder, Runtime};

#[cfg(feature = "dial9")]
use dial9::{Dial9Handle, Dial9HandleTokioExt, Recorder, TokioAttachOptions};

#[cfg(feature = "dial9")]
static RECORDER_HANDLE: OnceLock<Dial9Handle> = OnceLock::new();

/// How long shutdown waits for the recorder to flush sealed trace segments.
#[cfg(feature = "dial9")]
const RECORDER_SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(5);

/// Keeps the recorder alive. Call [`ProfilingGuard::shutdown`] only after every
/// attached runtime has been dropped, so worker buffers are flushed first.
pub(crate) struct ProfilingGuard {
    #[cfg(feature = "dial9")]
    recorder: Recorder,
}

impl ProfilingGuard {
    pub(crate) fn shutdown(self) {
        #[cfg(feature = "dial9")]
        self.recorder.graceful_shutdown(RECORDER_SHUTDOWN_TIMEOUT);
    }
}

/// Build the shared multi-thread runtime and, under `dial9`, the process-wide
/// recorder that later [`runtime_build`] calls attach to.
#[cfg(feature = "dial9")]
pub(crate) fn runtime_shared_build(worker_threads: usize) -> io::Result<(ProfilingGuard, Runtime)> {
    let (recorder, runtime) = dial9::recorder_from_env_with(|builder| {
        builder.worker_threads(worker_threads);
    })?;
    let _ = RECORDER_HANDLE.set(recorder.handle().clone());
    Ok((ProfilingGuard { recorder }, runtime))
}

#[cfg(not(feature = "dial9"))]
pub(crate) fn runtime_shared_build(worker_threads: usize) -> io::Result<(ProfilingGuard, Runtime)> {
    let runtime = Builder::new_multi_thread()
        .worker_threads(worker_threads)
        .enable_all()
        .build()?;
    Ok((ProfilingGuard {}, runtime))
}

/// Build a dedicated runtime, attached to the recorder as `name` when one exists.
#[cfg(feature = "dial9")]
pub(crate) fn runtime_build(mut builder: Builder, name: &str) -> io::Result<Runtime> {
    match RECORDER_HANDLE.get() {
        Some(handle) => handle.attach_tokio_runtime(
            builder,
            TokioAttachOptions::builder().runtime_name(name).build(),
        ),
        None => builder.build(),
    }
}

#[cfg(not(feature = "dial9"))]
pub(crate) fn runtime_build(mut builder: Builder, _name: &str) -> io::Result<Runtime> {
    builder.build()
}
