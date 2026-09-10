use super::server::FridayMcpServer;
use crate::app::events::EventBus;
use crate::exec::pool::ExecChannelPool;
use crate::mcp::session_mapper::SessionMapper;
use crate::tools::confirm::ConfirmRegistry;
use crate::tools::registry::ToolRegistry;
use rmcp::transport::streamable_http_server::session::local::LocalSessionManager;
use rmcp::transport::streamable_http_server::{StreamableHttpServerConfig, StreamableHttpService};
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

pub struct McpServerHandle {
    pub port: u16,
    pub cancel_token: CancellationToken,
    pub join_handle: tokio::task::JoinHandle<()>,
}

pub async fn start_mcp_server(
    tool_registry: Arc<ToolRegistry>,
    exec_pool: Arc<Mutex<ExecChannelPool>>,
    confirm_registry: Arc<Mutex<ConfirmRegistry>>,
    session_mapper: Arc<Mutex<SessionMapper>>,
    bus: EventBus,
    pool: sqlx::SqlitePool,
) -> Result<McpServerHandle, Box<dyn std::error::Error + Send + Sync>> {
    let cancel_token = CancellationToken::new();
    let server_cancel = cancel_token.clone();
    let loop_cancel = cancel_token.clone();

    let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
    let port = listener.local_addr()?.port();
    tracing::info!(port, "MCP server binding to 127.0.0.1");

    let config = StreamableHttpServerConfig::default()
        .with_sse_keep_alive(Some(std::time::Duration::from_secs(30)))
        .with_cancellation_token(server_cancel);

    let session_manager = Arc::new(LocalSessionManager::default());

    let service_factory = move || {
        Ok::<_, std::io::Error>(FridayMcpServer {
            tool_registry: tool_registry.clone(),
            exec_pool: exec_pool.clone(),
            confirm_registry: confirm_registry.clone(),
            session_mapper: session_mapper.clone(),
            bus: bus.clone(),
            pool: pool.clone(),
        })
    };

    let service = StreamableHttpService::new(service_factory, session_manager, config);

    listener.set_nonblocking(true)?;
    let tokio_listener = tokio::net::TcpListener::from_std(listener)?;

    let join_handle = tokio::spawn(async move {
        let service = Arc::new(service);

        loop {
            tokio::select! {
                accept_result = tokio_listener.accept() => {
                    match accept_result {
                        Ok((stream, addr)) => {
                            let service = service.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(stream, addr, service).await {
                                    // agent CLI（MCP 客户端）会话切换/中断时强断 HTTP 连接（TCP RST）
                                    // 属预期客户端行为，ERROR 级别会误导排障（issue #19 bug #4）——
                                    // 仅对 ConnectionReset 降级为 info，其他错误保持 ERROR。
                                    if is_client_disconnect_reset(&e) {
                                        tracing::info!(
                                            %addr,
                                            "mcp client disconnected (connection reset by peer, addr={addr}) — agent CLI 会话切换/重启的预期行为"
                                        );
                                    } else {
                                        tracing::error!(?e, %addr, "connection error");
                                    }
                                }
                            });
                        }
                        Err(e) => {
                            tracing::error!(?e, "accept error");
                        }
                    }
                }
                _ = loop_cancel.cancelled() => {
                    tracing::info!("MCP server listener shutting down");
                    break;
                }
            }
        }
    });

    Ok(McpServerHandle {
        port,
        cancel_token,
        join_handle,
    })
}

/// 错误链中查找 io::Error（hyper 1.x 未公开 as_io_error，走标准 source 链遍历）
fn find_io_error<'a>(e: &'a (dyn std::error::Error + 'static)) -> Option<&'a std::io::Error> {
    let mut cur: &(dyn std::error::Error + 'static) = e;
    loop {
        if let Some(io) = cur.downcast_ref::<std::io::Error>() {
            return Some(io);
        }
        match cur.source() {
            Some(s) => cur = s,
            None => return None,
        }
    }
}

/// hyper 连接错误是否为客户端单方面 RST 断开：
/// Windows WSAECONNRESET（os error 10054）映射到 `ErrorKind::ConnectionReset`，
/// 两者都查以防映射差异。agent CLI 会话切换/重启强断连接属预期行为。
fn is_client_disconnect_reset(e: &(dyn std::error::Error + 'static)) -> bool {
    find_io_error(e)
        .map(|io| io.kind() == std::io::ErrorKind::ConnectionReset || io.raw_os_error() == Some(10054))
        .unwrap_or(false)
}

async fn handle_connection(
    stream: tokio::net::TcpStream,
    addr: std::net::SocketAddr,
    service: Arc<StreamableHttpService<FridayMcpServer, LocalSessionManager>>,
) -> Result<(), hyper::Error> {
    tracing::info!(%addr, "MCP connection opened");
    let io = hyper_util::rt::TokioIo::new(stream);

    let service_clone = service.clone();
    let svc = hyper::service::service_fn(move |req| {
        let service = service_clone.clone();
        async move {
            let method = req.method().as_str().to_owned();
            let path = req.uri().path().to_owned();
            let user_agent = req
                .headers()
                .get(http::header::USER_AGENT)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("-")
                .to_owned();

            let response = service.handle(req).await;

            tracing::info!(
                method = %method,
                path = %path,
                user_agent = %user_agent,
                status = %response.status().as_u16(),
                "MCP HTTP request"
            );

            Ok::<_, std::convert::Infallible>(response)
        }
    });

    // 错误统一交回调用方按类别分级记录（ConnectionReset → info，其余 ERROR），
    // 避免同一错误在本层与外层重复打两条日志
    let result = hyper::server::conn::http1::Builder::new()
        .serve_connection(io, svc)
        .await;
    if result.is_ok() {
        tracing::info!(%addr, "MCP connection closed");
    }
    result
}

#[cfg(test)]
mod tests {
    use super::is_client_disconnect_reset;
    use std::error::Error as _;

    /// 模拟 hyper::Error 包裹 io::Error 的形态：source 链透出底层错误
    struct WrappedError {
        source: Box<dyn std::error::Error + Send + Sync>,
    }
    impl std::fmt::Debug for WrappedError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "WrappedError")
        }
    }
    impl std::fmt::Display for WrappedError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "wrapped: {}", self.source)
        }
    }
    impl std::error::Error for WrappedError {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            Some(self.source.as_ref())
        }
    }

    #[test]
    fn test_connection_reset_kind_is_client_disconnect() {
        let e = std::io::Error::from(std::io::ErrorKind::ConnectionReset);
        assert!(is_client_disconnect_reset(&e));
    }

    #[test]
    fn test_windows_10054_raw_os_error_is_client_disconnect() {
        let e = std::io::Error::from_raw_os_error(10054);
        assert!(is_client_disconnect_reset(&e));
    }

    #[test]
    fn test_other_io_errors_are_not_client_disconnect() {
        let e = std::io::Error::new(std::io::ErrorKind::Other, "boom");
        assert!(!is_client_disconnect_reset(&e));
    }

    #[test]
    fn test_non_io_error_chain_is_not_client_disconnect() {
        // 非 IO 类错误（如 hyper 的 H1 解析错误）链上无 io::Error → 保持 ERROR 级别
        #[derive(Debug)]
        struct ParseError;
        impl std::fmt::Display for ParseError {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "h1 parse error")
            }
        }
        impl std::error::Error for ParseError {}
        let e = WrappedError { source: Box::new(std::io::Error::new(std::io::ErrorKind::Unsupported, "h1 parse")) };
        // Unsupported kind 不是 ConnectionReset
        assert!(!is_client_disconnect_reset(&e));
        let plain = WrappedError { source: Box::new(ParseError) };
        assert!(!is_client_disconnect_reset(&plain));
    }

    #[test]
    fn test_wrapped_io_error_in_error_chain_is_detected() {
        // hyper::Error 包裹 io::Error 的真实形态：source 链穿透命中 10054
        let wrapped = WrappedError { source: Box::new(std::io::Error::from_raw_os_error(10054)) };
        assert!(is_client_disconnect_reset(&wrapped));
        let nested = WrappedError {
            source: Box::new(WrappedError { source: Box::new(std::io::Error::from_raw_os_error(10054)) }),
        };
        assert!(is_client_disconnect_reset(&nested), "多层包裹也要穿透");
    }
}
