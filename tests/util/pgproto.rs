use std::process::Command;

/// Run a pgproto data file against the proxy and return the combined output.
/// pgproto prints protocol trace to stderr; we merge stdout and stderr.
/// Panics if pgproto is not found or exits with a non-zero status.
pub(crate) fn pgproto_run(port: u16, data_file: &str) -> String {
    let output = Command::new("/opt/local/bin/pgproto")
        .arg("-h")
        .arg("127.0.0.1")
        .arg("-p")
        .arg(port.to_string())
        .arg("-u")
        .arg("postgres")
        .arg("-d")
        .arg("origin_test")
        .arg("-f")
        .arg(data_file)
        .arg("-D")
        .output()
        .expect("pgproto execution");

    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).to_string();

    assert!(
        output.status.success(),
        "pgproto failed (exit {})\nstdout: {}\nstderr: {}",
        output.status,
        stdout,
        stderr,
    );

    format!("{stdout}{stderr}")
}
