//! Connection helper plus timeout-wrapped query/execute, so a stalled proxy
//! read fails the run with a pinpointing error instead of hanging it forever.

use std::future::poll_fn;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio::time::timeout;
use tokio_postgres::types::ToSql;
use tokio_postgres::{AsyncMessage, Client, NoTls, Row, ToStatement};

/// Cap on any single proxy operation. A served query returns in microseconds;
/// even a cold miss forwards to origin promptly. Exceeding this means pgcache
/// is stalled on that query (e.g. wedged under invalidation churn), which the
/// harness should surface, not wait out.
// Generous: this guards the hangs-forever class (orphaned coalesce waiters,
// PGC-253), not latency — transient reads in the tens of seconds are real
// under an unthrottled debug-build run saturating the box.
pub const OP_TIMEOUT: Duration = Duration::from_secs(60);

/// Set once the run starts killing the spawned stack: clients are still alive
/// at that point, so their connection drivers end with errors that are expected
/// teardown noise, not failures.
static TEARDOWN: AtomicBool = AtomicBool::new(false);

pub fn teardown_begin() {
    TEARDOWN.store(true, Ordering::Relaxed);
}

pub async fn connect(url: &str) -> Result<Client> {
    let (client, connection) = tokio_postgres::connect(url, NoTls)
        .await
        .with_context(|| format!("connecting to {url}"))?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            connection_end_log(&e);
        }
    });
    Ok(client)
}

/// [`connect`], delivering the server's notices on a channel instead of
/// dropping them in the connection driver.
pub async fn connect_with_notices(url: &str) -> Result<(Client, UnboundedReceiver<String>)> {
    let (client, mut connection) = tokio_postgres::connect(url, NoTls)
        .await
        .with_context(|| format!("connecting to {url}"))?;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
            match message {
                Ok(AsyncMessage::Notice(notice)) => {
                    let _ = tx.send(notice.message().to_owned());
                }
                Ok(_) => {}
                Err(e) => {
                    connection_end_log(&e);
                    break;
                }
            }
        }
    });
    Ok((client, rx))
}

fn connection_end_log(e: &tokio_postgres::Error) {
    if TEARDOWN.load(Ordering::Relaxed) {
        tracing::debug!(error = %e, "postgres connection ended during teardown");
    } else {
        tracing::error!(error = %e, "postgres connection ended");
    }
}

fn timed_out(what: &str) -> anyhow::Error {
    anyhow!("{what} exceeded {OP_TIMEOUT:?} — pgcache is stalled serving this query")
}

pub async fn query_timed<T>(
    client: &Client,
    stmt: &T,
    params: &[&(dyn ToSql + Sync)],
    what: &str,
) -> Result<Vec<Row>>
where
    T: ?Sized + ToStatement,
{
    timeout(OP_TIMEOUT, client.query(stmt, params))
        .await
        .map_err(|_| timed_out(what))?
        .with_context(|| what.to_owned())
}

pub async fn execute_timed<T>(
    client: &Client,
    stmt: &T,
    params: &[&(dyn ToSql + Sync)],
    what: &str,
) -> Result<u64>
where
    T: ?Sized + ToStatement,
{
    timeout(OP_TIMEOUT, client.execute(stmt, params))
        .await
        .map_err(|_| timed_out(what))?
        .with_context(|| what.to_owned())
}

pub async fn batch_timed(client: &Client, sql: &str, what: &str) -> Result<()> {
    timeout(OP_TIMEOUT, client.batch_execute(sql))
        .await
        .map_err(|_| timed_out(what))?
        .with_context(|| what.to_owned())
}
