use std::{
    collections::HashMap,
    pin::Pin,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream};
use tokio_stream::Stream;
use tonic::transport::server::{Connected, TcpConnectInfo};

#[derive(Clone, Debug)]
pub struct GrpcClientInfo {
    pub peer: String,
    pub connected_at: String,
    pub version: String,
    pub host: String,
}

struct RegistryInner {
    clients: HashMap<u64, GrpcClientInfo>,
}

#[derive(Clone, Default)]
pub struct GrpcClientRegistry {
    next_id: Arc<AtomicU64>,
    inner: Arc<Mutex<RegistryInner>>,
}

impl Default for RegistryInner {
    fn default() -> Self {
        Self {
            clients: HashMap::new(),
        }
    }
}

impl GrpcClientRegistry {
    pub fn insert(&self, peer: String) -> u64 {
        let id = self.next_id.fetch_add(1, Ordering::Relaxed) + 1;
        self.inner
            .lock()
            .expect("grpc registry")
            .clients
            .insert(
                id,
                GrpcClientInfo {
                    peer,
                    connected_at: chrono::Utc::now().to_rfc3339(),
                    version: String::new(),
                    host: String::new(),
                },
            );
        id
    }

    pub fn remove(&self, id: u64) {
        self.inner.lock().expect("grpc registry").clients.remove(&id);
    }

    pub fn touch(&self, peer: &str, version: &str, host: &str) {
        if version.is_empty() && host.is_empty() {
            return;
        }
        let mut inner = self.inner.lock().expect("grpc registry");
        for info in inner.clients.values_mut() {
            if info.peer != peer {
                continue;
            }
            if !version.is_empty() {
                info.version = version.to_string();
            }
            if !host.is_empty() {
                info.host = host.to_string();
            }
        }
    }

    pub fn clients(&self) -> Vec<GrpcClientInfo> {
        let mut clients: Vec<GrpcClientInfo> = self
            .inner
            .lock()
            .expect("grpc registry")
            .clients
            .values()
            .cloned()
            .collect();
        clients.sort_by(|left, right| {
            left.peer
                .cmp(&right.peer)
                .then(left.host.cmp(&right.host))
        });
        clients
    }
}

pub struct TrackingIncoming {
    listener: TcpListener,
    registry: GrpcClientRegistry,
}

impl TrackingIncoming {
    pub fn new(listener: TcpListener, registry: GrpcClientRegistry) -> Self {
        Self { listener, registry }
    }
}

impl Stream for TrackingIncoming {
    type Item = Result<TrackedConn, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.listener.poll_accept(cx) {
            Poll::Ready(Ok((stream, peer))) => {
                let _ = stream.set_nodelay(true);
                let local = stream.local_addr().ok();
                let id = this.registry.insert(peer.to_string());
                Poll::Ready(Some(Ok(TrackedConn {
                    inner: stream,
                    peer,
                    local,
                    id,
                    registry: this.registry.clone(),
                })))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Some(Err(error))),
            Poll::Pending => Poll::Pending,
        }
    }
}

pub struct TrackedConn {
    inner: TcpStream,
    peer: std::net::SocketAddr,
    local: Option<std::net::SocketAddr>,
    id: u64,
    registry: GrpcClientRegistry,
}

impl Drop for TrackedConn {
    fn drop(&mut self) {
        self.registry.remove(self.id);
    }
}

impl Connected for TrackedConn {
    type ConnectInfo = TcpConnectInfo;

    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: self.local,
            remote_addr: Some(self.peer),
        }
    }
}

impl AsyncRead for TrackedConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TrackedConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_tracks_and_updates_identity() {
        let registry = GrpcClientRegistry::default();
        let id = registry.insert("10.0.0.8:9".into());
        registry.touch("10.0.0.8:9", "python/0.1.34", "api");
        let clients = registry.clients();
        assert_eq!(clients.len(), 1);
        assert_eq!(clients[0].peer, "10.0.0.8:9");
        assert_eq!(clients[0].version, "python/0.1.34");
        assert_eq!(clients[0].host, "api");
        registry.remove(id);
        assert!(registry.clients().is_empty());
    }
}
