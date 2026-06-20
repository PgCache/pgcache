use std::net::SocketAddr;
use std::time::Duration;

use bytes::Bytes;
use http::{Method, Response};
use http_body_util::{BodyExt, Full};
use hyper::service::service_fn;
use hyper_util::rt::TokioIo;
use metrics_exporter_prometheus::PrometheusHandle;
use serde::Serialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;

use crate::cache::StatusRequest;
use crate::proxy::{SharedProxyStatus, StatusSender};
use crate::settings::{
    DynamicConfig, DynamicConfigHandle, DynamicConfigPatch, config_file_dynamic_extract,
    config_file_dynamic_update,
};

/// Spawn the admin HTTP server thread.
///
/// Serves `/metrics`, `/healthz`, `/readyz`, `/status`, and `/config` endpoints.
/// The `/status` endpoint sends a `StatusRequest` to the cache writer and
/// returns the JSON response.
pub fn admin_server_spawn(
    addr: SocketAddr,
    metrics: PrometheusHandle,
    cancel: CancellationToken,
    shared_proxy_status: SharedProxyStatus,
    status_tx: StatusSender,
    dynamic: DynamicConfigHandle,
) -> Result<(), std::io::Error> {
    std::thread::Builder::new()
        .name("http".to_owned())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("admin server tokio runtime");
            rt.block_on(admin_server_run(
                addr,
                metrics,
                cancel,
                shared_proxy_status,
                status_tx,
                dynamic,
            ));
        })?;
    Ok(())
}

/// Admin HTTP server that serves metrics, health, config, and status endpoints.
async fn admin_server_run(
    addr: SocketAddr,
    handle: PrometheusHandle,
    cancel: CancellationToken,
    shared_proxy_status: SharedProxyStatus,
    status_tx: StatusSender,
    dynamic: DynamicConfigHandle,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!("admin server bind failed on {addr}: {e}");
            return;
        }
    };

    loop {
        let stream = tokio::select! {
            _ = cancel.cancelled() => break,
            result = listener.accept() => {
                match result {
                    Ok((stream, _)) => stream,
                    Err(e) => {
                        tracing::warn!("admin server accept error: {e}");
                        continue;
                    }
                }
            }
        };

        let handle = handle.clone();
        let shared_proxy_status = shared_proxy_status.clone();
        let status_tx = status_tx.clone();
        let dynamic = dynamic.clone();
        tokio::spawn(async move {
            let service = service_fn(move |request: http::Request<hyper::body::Incoming>| {
                let h = handle.clone();
                let proxy_status = shared_proxy_status.clone();
                let status_tx = status_tx.clone();
                let dynamic = dynamic.clone();
                async move {
                    match (request.uri().path(), request.method()) {
                        ("/metrics", _) => {
                            let body = h.render();
                            Response::builder()
                                .header("Content-Type", "text/plain; charset=utf-8")
                                .header("Access-Control-Allow-Origin", "*")
                                .body(Full::new(Bytes::from(body)))
                        }
                        ("/healthz", _) => Response::builder()
                            .header("Content-Type", "text/plain")
                            .body(Full::new(Bytes::from("OK"))),
                        ("/readyz", _) => {
                            if proxy_status.is_ready() {
                                Response::builder()
                                    .header("Content-Type", "text/plain")
                                    .body(Full::new(Bytes::from("OK")))
                            } else {
                                Response::builder()
                                    .status(503)
                                    .header("Content-Type", "text/plain")
                                    .body(Full::new(Bytes::from("not ready")))
                            }
                        }
                        ("/status", _) => status_handle(status_tx).await,
                        ("/config", &Method::GET) => config_get_handle(&dynamic).await,
                        ("/config", &Method::PUT) => config_put_handle(request, &dynamic).await,
                        ("/config/reload", &Method::POST) => config_reload_handle(&dynamic).await,
                        _ => Response::builder()
                            .status(404)
                            .body(Full::new(Bytes::from("Not Found"))),
                    }
                }
            });

            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(TokioIo::new(stream), service)
                .await
            {
                tracing::debug!("admin connection error: {e}");
            }
        });
    }

    tracing::debug!("admin server shutting down");
}

/// Handle a `/status` request by querying the cache writer via the status channel.
async fn status_handle(status_tx: StatusSender) -> Result<Response<Full<Bytes>>, http::Error> {
    let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
    let req = StatusRequest { reply_tx };

    if status_tx.send(req).await.is_err() {
        return Response::builder()
            .status(503)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(r#"{"error":"cache unavailable"}"#)));
    }

    match tokio::time::timeout(Duration::from_secs(2), reply_rx).await {
        Ok(Ok(response)) => {
            let body = serde_json::to_string(&response)
                .unwrap_or_else(|e| format!(r#"{{"error":"serialization failed: {e}"}}"#));
            Response::builder()
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from(body)))
        }
        Ok(Err(_)) => Response::builder()
            .status(503)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"cache channel closed"}"#,
            ))),
        Err(_) => Response::builder()
            .status(503)
            .header("Content-Type", "application/json")
            .body(Full::new(Bytes::from(
                r#"{"error":"status request timed out"}"#,
            ))),
    }
}

/// Maximum request body size for config updates (64 KiB).
const CONFIG_BODY_LIMIT: usize = 64 * 1024;

fn json_error(status: u16, message: &str) -> Result<Response<Full<Bytes>>, http::Error> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(format!(
            r#"{{"error":"{message}"}}"#
        ))))
}

#[derive(Serialize)]
struct ConfigGetResponse<'a> {
    dynamic: &'a DynamicConfig,
    restart_required: bool,
    effective_log_level: Option<String>,
}

fn config_response(dynamic: &DynamicConfigHandle) -> Result<Response<Full<Bytes>>, http::Error> {
    let cfg = dynamic.load();
    let response = ConfigGetResponse {
        dynamic: &cfg,
        restart_required: dynamic.restart_required(),
        effective_log_level: dynamic.effective_log_level(),
    };
    let body = serde_json::to_string(&response)
        .unwrap_or_else(|e| format!(r#"{{"error":"serialization failed: {e}"}}"#));
    Response::builder()
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(body)))
}

async fn config_get_handle(
    dynamic: &DynamicConfigHandle,
) -> Result<Response<Full<Bytes>>, http::Error> {
    config_response(dynamic)
}

async fn config_put_handle(
    request: http::Request<hyper::body::Incoming>,
    dynamic: &DynamicConfigHandle,
) -> Result<Response<Full<Bytes>>, http::Error> {
    let body = match http_body_util::Limited::new(request, CONFIG_BODY_LIMIT)
        .collect()
        .await
    {
        Ok(collected) => collected.to_bytes(),
        Err(e) => return json_error(400, &format!("failed to read body: {e}")),
    };

    let patch: DynamicConfigPatch = match serde_json::from_slice(&body) {
        Ok(p) => p,
        Err(e) => return json_error(400, &format!("invalid JSON: {e}")),
    };

    if let Some(path) = dynamic.config_path()
        && let Err(e) = config_file_dynamic_update(path, &patch)
    {
        return json_error(500, &format!("failed to update config file: {e}"));
    }

    let current = dynamic.load();
    let new_config = patch.apply(&current);
    dynamic.update(new_config);

    config_response(dynamic)
}

async fn config_reload_handle(
    dynamic: &DynamicConfigHandle,
) -> Result<Response<Full<Bytes>>, http::Error> {
    let Some(path) = dynamic.config_path() else {
        return json_error(400, "no config file path available");
    };

    match config_file_dynamic_extract(path) {
        Ok(new_config) => {
            dynamic.update(new_config);
            config_response(dynamic)
        }
        Err(e) => json_error(500, &format!("failed to reload config: {e}")),
    }
}
