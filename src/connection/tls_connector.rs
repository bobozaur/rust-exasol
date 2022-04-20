#[cfg(any(feature = "native-tls", feature = "rustls"))]
use super::Ws;
use crate::error::ConnectionError;
#[cfg(feature = "native-tls")]
use __native_tls::TlsConnector as TlsCon;
#[cfg(feature = "rustls")]
use __rustls::ClientConfig;
#[cfg(all(feature = "rustls", feature = "tungstenite/rustls"))]
use __rustls::{ClientConnection, ServerName, StreamOwned};
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use std::net::TcpStream;
#[cfg(feature = "rustls")]
use std::sync::Arc;
#[cfg(any(feature = "native-tls", feature = "rustls"))]
use tungstenite::stream::MaybeTlsStream;

pub type ConResult<T> = std::result::Result<T, ConnectionError>;

#[derive(Clone)]
pub enum TlsConnector {
    #[cfg(feature = "rustls")]
    Rustls(Arc<ClientConfig>),
    #[cfg(feature = "native-tls")]
    NativeTls(TlsCon),
}

#[cfg(feature = "rustls")]
impl From<__rustls::ClientConfig> for TlsConnector {
    fn from(connector: ClientConfig) -> Self {
        Self::Rustls(Arc::new(connector))
    }
}

#[cfg(feature = "native-tls")]
impl From<__native_tls::TlsConnector> for TlsConnector {
    fn from(connector: TlsCon) -> Self {
        Self::NativeTls(connector)
    }
}

impl TlsConnector {
    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    pub(crate) fn connect(self, prefix: &str, addr: &str, port: u16) -> ConResult<Ws> {
        let socket = Self::connect_socket(addr, port)?;
        let stream = self.connect_tls(addr, socket)?;
        let ws_addr = format!("{}://{}:{}/", prefix, addr, port);
        let (ws, _) = tungstenite::client(ws_addr, stream)?;
        Ok(ws)
    }

    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    fn connect_tls(self, domain: &str, socket: TcpStream) -> ConResult<MaybeTlsStream<TcpStream>> {
        match self {
            #[cfg(all(feature = "rustls", feature = "tungstenite/rustls"))]
            Self::Rustls(c) => {
                let domain = ServerName::try_from(domain)?;
                let client = ClientConnection::new(c, domain)?;
                let stream = StreamOwned::new(client, socket);
                Ok(MaybeTlsStream::Rustls(stream))
            }
            #[cfg(feature = "native-tls")]
            Self::NativeTls(c) => {
                let stream = c.connect(domain, socket)?;
                Ok(MaybeTlsStream::NativeTls(stream))
            }
        }
    }

    #[cfg(any(feature = "native-tls", feature = "rustls"))]
    fn connect_socket(addr: &str, port: u16) -> std::io::Result<TcpStream> {
        TcpStream::connect(format!("{}:{}", addr, port))
    }
}
