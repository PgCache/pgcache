use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};

use socket2::{Domain, Protocol, Socket, Type};
use tokio::net::TcpListener;

/// Bind `addr`, widening an IPv4 wildcard (`0.0.0.0:port`) to a dual-stack
/// `[::]:port` socket so clients that resolve `localhost` to `::1` first
/// (Alpine's /etc/hosts with BusyBox wget) can connect. Falls back to the
/// plain IPv4 bind when the host has no IPv6.
pub async fn listener_bind(addr: SocketAddr) -> std::io::Result<TcpListener> {
    if addr.ip() != Ipv4Addr::UNSPECIFIED {
        return TcpListener::bind(addr).await;
    }
    match listener_bind_dual_stack(addr.port()) {
        Ok(listener) => Ok(listener),
        Err(e) => {
            tracing::debug!(
                "dual-stack bind on port {} unavailable ({e}); binding {addr}",
                addr.port()
            );
            TcpListener::bind(addr).await
        }
    }
}

fn listener_bind_dual_stack(port: u16) -> std::io::Result<TcpListener> {
    let socket = Socket::new(Domain::IPV6, Type::STREAM, Some(Protocol::TCP))?;
    socket.set_only_v6(false)?;
    #[cfg(not(windows))]
    socket.set_reuse_address(true)?;
    socket.set_nonblocking(true)?;
    socket.bind(&SocketAddr::from((Ipv6Addr::UNSPECIFIED, port)).into())?;
    socket.listen(1024)?;
    TcpListener::from_std(socket.into())
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv6Addr, SocketAddr};

    use tokio::net::TcpStream;

    use super::*;

    #[tokio::test]
    async fn test_ipv4_wildcard_accepts_both_loopback_families() {
        let listener = listener_bind("0.0.0.0:0".parse().expect("valid socket addr"))
            .await
            .expect("bind wildcard");
        let port = listener.local_addr().expect("local addr").port();

        TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect over IPv4 loopback");

        if listener.local_addr().expect("local addr").is_ipv6() {
            TcpStream::connect(SocketAddr::from((Ipv6Addr::LOCALHOST, port)))
                .await
                .expect("connect over IPv6 loopback");
        }
    }

    #[tokio::test]
    async fn test_explicit_ipv4_address_stays_ipv4() {
        let listener = listener_bind("127.0.0.1:0".parse().expect("valid socket addr"))
            .await
            .expect("bind loopback");
        let local = listener.local_addr().expect("local addr");
        assert!(local.is_ipv4());

        let port = local.port();
        TcpStream::connect(("127.0.0.1", port))
            .await
            .expect("connect over IPv4 loopback");
        assert!(
            TcpStream::connect(SocketAddr::from((Ipv6Addr::LOCALHOST, port)))
                .await
                .is_err()
        );
    }
}
