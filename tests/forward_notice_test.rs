//! The forward tag (`PGCACHE_FAULT_FORWARD_NOTICE`): a cacheable read that
//! falls back to origin arrives preceded by a NOTICE, and a cache hit does not.
//! The consistency harness relies on it to tell origin-served reads from cache
//! serves.
#![cfg(feature = "fault-injection")]

use std::future::poll_fn;
use std::io::Error;
use std::time::Duration;

use tokio::sync::mpsc::{self, UnboundedReceiver};
use tokio_postgres::{AsyncMessage, Client, Config, NoTls};

use crate::util::TestContext;

mod util;

const FORWARD_NOTICE: &str = "pgcache: forwarded to origin";

/// A proxy connection whose notices are delivered on a channel instead of
/// being logged by the connection driver.
async fn notice_client_connect(port: u16) -> Result<(Client, UnboundedReceiver<String>), Error> {
    let (client, mut connection) = Config::new()
        .host("127.0.0.1")
        .port(port)
        .user("postgres")
        .dbname("origin_test")
        .connect(NoTls)
        .await
        .map_err(Error::other)?;
    let (tx, rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(message) = poll_fn(|cx| connection.poll_message(cx)).await {
            match message {
                Ok(AsyncMessage::Notice(notice)) => {
                    let _ = tx.send(notice.message().to_owned());
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("connection error: {e}");
                    break;
                }
            }
        }
    });
    Ok((client, rx))
}

/// Whether a forward tag arrived since the last call; drains the channel.
fn forward_tag_take(notices: &mut UnboundedReceiver<String>) -> bool {
    let mut tagged = false;
    while let Ok(message) = notices.try_recv() {
        tagged |= message == FORWARD_NOTICE;
    }
    tagged
}

#[tokio::test]
async fn test_forwarded_read_tagged_and_cache_hit_not() -> Result<(), Error> {
    let mut ctx = TestContext::setup_fault(&[("PGCACHE_FAULT_FORWARD_NOTICE", "1")]).await?;
    ctx.query(
        "create table fwd_tag (id integer primary key, val text)",
        &[],
    )
    .await?;
    ctx.query("insert into fwd_tag values (1, 'a')", &[])
        .await?;

    let (client, mut notices) = notice_client_connect(ctx.cache_port).await?;
    let sql = "select val from fwd_tag where id = 1";

    client.simple_query(sql).await.map_err(Error::other)?;
    assert!(
        forward_tag_take(&mut notices),
        "the first read misses and is forwarded"
    );

    let mut hit = false;
    for _ in 0..80 {
        let before = ctx.metrics().await?.queries_cache_hit;
        client.simple_query(sql).await.map_err(Error::other)?;
        let tagged = forward_tag_take(&mut notices);
        if ctx.metrics().await?.queries_cache_hit > before {
            assert!(!tagged, "a cache hit carries no forward tag");
            hit = true;
            break;
        }
        assert!(tagged, "a read that was not a cache hit is tagged");
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(hit, "the query was never served from cache");
    Ok(())
}
