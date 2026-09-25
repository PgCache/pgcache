//! A minimal raw wire-protocol client for assertions tokio-postgres hides —
//! the transaction-status byte of every `ReadyForQuery`, and pipelining
//! several simple `Query` messages before reading any response.

use std::io::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// One backend message: its tag byte and body (excluding the length field).
#[derive(Debug, Clone)]
pub struct WireMessage {
    pub tag: u8,
    pub body: Vec<u8>,
}

/// The responses to one simple query: every message up to and including its
/// `ReadyForQuery`.
#[derive(Debug, Clone)]
pub struct WireResponse {
    pub messages: Vec<WireMessage>,
}

impl WireResponse {
    /// The transaction status byte of the terminating `ReadyForQuery`.
    pub fn ready_status(&self) -> u8 {
        self.messages
            .last()
            .and_then(|m| m.body.first().copied())
            .unwrap_or(0)
    }

    pub fn data_row_count(&self) -> usize {
        self.messages.iter().filter(|m| m.tag == b'D').count()
    }

    /// The SQLSTATE of the first `ErrorResponse`, if any.
    pub fn error_sqlstate(&self) -> Option<String> {
        let err = self.messages.iter().find(|m| m.tag == b'E')?;
        err.body
            .split(|b| *b == 0)
            .find(|field| field.first() == Some(&b'C'))
            .and_then(|field| field.get(1..))
            .map(|code| String::from_utf8_lossy(code).into_owned())
    }
}

pub struct WireClient {
    stream: TcpStream,
}

impl WireClient {
    /// Connect to the proxy on `port` as `postgres` / `origin_test` (trust
    /// auth) and consume the startup exchange through the first `ReadyForQuery`.
    pub async fn connect(port: u16) -> Result<Self, Error> {
        let stream = TcpStream::connect(("127.0.0.1", port)).await?;
        let mut client = Self { stream };
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
        client.stream.write_all(&len.to_be_bytes()).await?;
        client.stream.write_all(&body).await?;
        client.stream.flush().await?;
        let startup = client.response_read().await?;
        if let Some(auth) = startup.messages.iter().find(|m| m.tag == b'R')
            && auth.body.get(..4) != Some(&[0, 0, 0, 0])
        {
            return Err(Error::other("wire client needs trust authentication"));
        }
        Ok(client)
    }

    /// Send one simple `Query` message without waiting for its response.
    pub async fn query_send(&mut self, sql: &str) -> Result<(), Error> {
        let len = u32::try_from(sql.len() + 5).map_err(Error::other)?;
        self.stream.write_all(b"Q").await?;
        self.stream.write_all(&len.to_be_bytes()).await?;
        self.stream.write_all(sql.as_bytes()).await?;
        self.stream.write_all(&[0]).await?;
        self.stream.flush().await
    }

    /// Read messages through the next `ReadyForQuery`.
    pub async fn response_read(&mut self) -> Result<WireResponse, Error> {
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
    pub async fn query(&mut self, sql: &str) -> Result<WireResponse, Error> {
        self.query_send(sql).await?;
        self.response_read().await
    }
}
