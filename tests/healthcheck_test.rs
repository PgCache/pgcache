//! `pgcache --healthcheck`: the container image's `HEALTHCHECK` command.

use std::io::Error;
use std::process::Command;

use crate::util::TestContext;

mod util;

fn healthcheck(args: &[&str]) -> Result<std::process::Output, Error> {
    Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--healthcheck")
        .args(args)
        .output()
}

#[tokio::test]
async fn test_healthcheck_exits_zero_against_a_running_proxy() -> Result<(), Error> {
    let ctx = TestContext::setup().await?;
    let socket = format!("127.0.0.1:{}", ctx.metrics_port);
    let output = healthcheck(&["--metrics_socket", &socket])?;
    assert!(
        output.status.success(),
        "stderr: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

#[tokio::test]
async fn test_healthcheck_exits_nonzero_when_nothing_listens() -> Result<(), Error> {
    let port = util::find_available_port()?;
    let socket = format!("127.0.0.1:{port}");
    let output = healthcheck(&["--metrics_socket", &socket])?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains(&socket),
        "names the socket it could not reach"
    );
    Ok(())
}

#[tokio::test]
async fn test_healthcheck_requires_a_metrics_socket() -> Result<(), Error> {
    let output = healthcheck(&[])?;
    assert!(!output.status.success());
    assert!(
        String::from_utf8_lossy(&output.stderr).contains("metrics_socket"),
        "names the missing argument: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    Ok(())
}

/// `--version` prints the crate version and exits 0 before any settings are
/// resolved, so it needs no origin or cache.
#[test]
fn test_version_flag_prints_crate_version() {
    let output = Command::new(env!("CARGO_BIN_EXE_pgcache"))
        .arg("--version")
        .output()
        .expect("run pgcache --version");
    assert!(output.status.success(), "exit 0");
    assert_eq!(
        String::from_utf8_lossy(&output.stdout).trim(),
        format!("pgcache {}", env!("CARGO_PKG_VERSION"))
    );
}
