use std::io::Error;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Make a raw HTTP GET request and return (status_code, body).
pub(crate) async fn http_get(port: u16, path: &str) -> Result<(u16, String), Error> {
    http_request(port, "GET", path, "").await
}

/// Make a raw HTTP PUT request with a JSON body and return (status_code, body).
pub(crate) async fn http_put(
    port: u16,
    path: &str,
    json_body: &str,
) -> Result<(u16, String), Error> {
    http_request(port, "PUT", path, json_body).await
}

/// Make a raw HTTP POST request and return (status_code, body).
pub(crate) async fn http_post(port: u16, path: &str) -> Result<(u16, String), Error> {
    http_request(port, "POST", path, "").await
}

async fn http_request(
    port: u16,
    method: &str,
    path: &str,
    body: &str,
) -> Result<(u16, String), Error> {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}"))
        .await
        .map_err(Error::other)?;

    let mut request = format!(
        "{method} {path} HTTP/1.1\r\n\
         Host: localhost\r\n\
         Content-Type: application/json\r\n\
         Content-Length: {}\r\n\
         Connection: close\r\n\
         \r\n",
        body.len()
    );
    request.push_str(body);
    stream
        .write_all(request.as_bytes())
        .await
        .map_err(Error::other)?;

    let mut response = String::new();
    stream
        .read_to_string(&mut response)
        .await
        .map_err(Error::other)?;

    let status_code = response
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .unwrap_or(0);

    let resp_body = response
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();

    Ok((status_code, resp_body))
}
