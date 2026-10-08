//! A minimal raw wire-protocol client for assertions tokio-postgres hides —
//! the transaction-status byte of every `ReadyForQuery`, and pipelining
//! several simple `Query` messages before reading any response.

use std::io::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// One backend message: its tag byte and body (excluding the length field).
#[derive(Debug, Clone)]
pub(crate) struct WireMessage {
    pub tag: u8,
    pub body: Vec<u8>,
}

/// The responses to one simple query: every message up to and including its
/// `ReadyForQuery`.
#[derive(Debug, Clone)]
pub(crate) struct WireResponse {
    pub messages: Vec<WireMessage>,
}

impl WireResponse {
    /// The transaction status byte of the terminating `ReadyForQuery`.
    pub(crate) fn ready_status(&self) -> u8 {
        self.messages
            .last()
            .and_then(|m| m.body.first().copied())
            .unwrap_or(0)
    }

    pub(crate) fn data_row_count(&self) -> usize {
        self.messages.iter().filter(|m| m.tag == b'D').count()
    }

    /// The command tags of every `CommandComplete`, in order.
    pub(crate) fn command_tags(&self) -> Vec<String> {
        self.messages
            .iter()
            .filter(|m| m.tag == b'C')
            .map(|m| {
                String::from_utf8_lossy(m.body.strip_suffix(&[0]).unwrap_or(&m.body)).into_owned()
            })
            .collect()
    }

    /// The first column of the first `DataRow` as text, if any.
    pub(crate) fn first_value(&self) -> Option<String> {
        let row = self.messages.iter().find(|m| m.tag == b'D')?;
        // Body: column count (2) | per column: length (4) | bytes.
        let len = i32::from_be_bytes(row.body.get(2..6)?.try_into().ok()?);
        let len = usize::try_from(len).ok()?;
        let value = row.body.get(6..6 + len)?;
        Some(String::from_utf8_lossy(value).into_owned())
    }

    /// The SQLSTATE of the first `ErrorResponse`, if any.
    pub(crate) fn error_sqlstate(&self) -> Option<String> {
        let err = self.messages.iter().find(|m| m.tag == b'E')?;
        err.body
            .split(|b| *b == 0)
            .find(|field| field.first() == Some(&b'C'))
            .and_then(|field| field.get(1..))
            .map(|code| String::from_utf8_lossy(code).into_owned())
    }
}

pub(crate) struct WireClient {
    stream: TcpStream,
}

impl WireClient {
    /// Connect to the proxy on `port` as `postgres` / `origin_test` (trust
    /// auth) and consume the startup exchange through the first `ReadyForQuery`.
    pub(crate) async fn connect(port: u16) -> Result<Self, Error> {
        let (client, _) = Self::connect_pipelined(port, None).await?;
        Ok(client)
    }

    /// Connect like [`Self::connect`], but when `first_query` is given, write
    /// the `StartupMessage` and that simple `Query` in one flush, before
    /// reading anything back — a client that does not wait for the startup
    /// `ReadyForQuery` (legal under trust auth). Returns the client and, if a
    /// query was sent, its full response.
    pub(crate) async fn connect_pipelined(
        port: u16,
        first_query: Option<&str>,
    ) -> Result<(Self, Option<WireResponse>), Error> {
        let stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let mut client = Self { stream };
        let mut bytes = startup_message_build()?;
        if let Some(sql) = first_query {
            bytes.extend_from_slice(&query_message_build(sql)?);
        }
        client.stream.write_all(&bytes).await?;
        client.stream.flush().await?;
        let startup = client.response_read().await?;
        if let Some(auth) = startup.messages.iter().find(|m| m.tag == b'R')
            && auth.body.get(..4) != Some(&[0, 0, 0, 0])
        {
            return Err(Error::other("wire client needs trust authentication"));
        }
        let response = match first_query {
            Some(_) => Some(client.response_read().await?),
            None => None,
        };
        Ok((client, response))
    }

    /// Send one simple `Query` message without waiting for its response.
    pub(crate) async fn query_send(&mut self, sql: &str) -> Result<(), Error> {
        self.stream.write_all(&query_message_build(sql)?).await?;
        self.stream.flush().await
    }

    /// Read messages through the next `ReadyForQuery`.
    pub(crate) async fn response_read(&mut self) -> Result<WireResponse, Error> {
        let mut messages = Vec::new();
        loop {
            let mut header = [0u8; 5];
            self.stream.read_exact(&mut header).await?;
            let tag = header[0];
            let len = u32::from_be_bytes([header[1], header[2], header[3], header[4]]) as usize;
            let mut body = vec![0u8; len.saturating_sub(4)];
            self.stream.read_exact(&mut body).await?;
            messages.push(WireMessage { tag, body });
            if tag == b'Z' {
                return Ok(WireResponse { messages });
            }
        }
    }

    /// Send one simple query and read its full response.
    pub(crate) async fn query(&mut self, sql: &str) -> Result<WireResponse, Error> {
        self.query_send(sql).await?;
        self.response_read().await
    }
}

/// A `StartupMessage` for `postgres` / `origin_test` (protocol 3.0).
fn startup_message_build() -> Result<Vec<u8>, Error> {
    let mut body = Vec::new();
    body.extend_from_slice(&196_608u32.to_be_bytes());
    for (k, v) in [("user", "postgres"), ("database", "origin_test")] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let len = u32::try_from(body.len() + 4).map_err(Error::other)?;
    let mut out = len.to_be_bytes().to_vec();
    out.extend_from_slice(&body);
    Ok(out)
}

/// A simple `Query` message carrying `sql`.
fn query_message_build(sql: &str) -> Result<Vec<u8>, Error> {
    let len = u32::try_from(sql.len() + 5).map_err(Error::other)?;
    let mut out = Vec::with_capacity(sql.len() + 6);
    out.push(b'Q');
    out.extend_from_slice(&len.to_be_bytes());
    out.extend_from_slice(sql.as_bytes());
    out.push(0);
    Ok(out)
}
