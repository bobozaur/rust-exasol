mod export;
mod import;

use std::io::{Error as IoError, ErrorKind as IoErrorKind, Result as IoResult};
use std::iter;
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::task::{ready, Context, Poll};

use futures_io::AsyncRead;
use futures_util::future::try_join_all;
use futures_util::{io::BufReader, AsyncReadExt, AsyncWriteExt};
use sqlx_core::error::{BoxDynError, Error as SqlxError};

use crate::connection::{tls, websocket::socket::WithExaSocket};
use crate::options::ExaTlsOptionsRef;
use crate::{ExaDatabaseError, ExaSslMode};

use super::websocket::socket::ExaSocket;

pub use export::{ExaExport, ExportOptions, QueryOrTable};
pub use import::{ExaImport, ImportOptions, Trim};

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

const DOUBLE_CR_LF: &[u8; 4] = b"\r\n\r\n";

/// Packet that tells Exasol the transport was successful
const SUCCESS_HEADERS: &[u8; 66] = b"HTTP/1.1 200 OK\r\n\
                                     Connection: close\r\n\
                                     Transfer-Encoding: chunked\r\n\
                                     \r\n";

#[derive(Debug, Clone, Copy)]
pub enum RowSeparator {
    LF,
    CR,
    CRLF,
}

impl AsRef<str> for RowSeparator {
    fn as_ref(&self) -> &str {
        match self {
            RowSeparator::LF => "LF",
            RowSeparator::CR => "CR",
            RowSeparator::CRLF => "CRLF",
        }
    }
}

fn poll_read_byte(socket: Pin<&mut BufReader<ExaSocket>>, cx: &mut Context) -> Poll<IoResult<u8>> {
    let mut buffer = [0; 1];
    let n = ready!(socket.poll_read(cx, &mut buffer))?;

    if n != 1 {
        let msg = "cannot read chunk size";
        let err = IoError::new(IoErrorKind::InvalidInput, msg);
        Poll::Ready(Err(err))
    } else {
        Poll::Ready(Ok(buffer[0]))
    }
}

async fn start_jobs(
    num_jobs: usize,
    ips: Vec<IpAddr>,
    port: u16,
    encrypted: bool,
) -> Result<Vec<(ExaSocket, SocketAddr)>, SqlxError> {
    let num_jobs = if num_jobs > 0 { num_jobs } else { ips.len() };
    let futures = ips
        .into_iter()
        .take(num_jobs)
        .map(|ip| spawn_socket(ip, port));

    let socket_arr = try_join_all(futures)
        .await
        .map_err(|e| ExaDatabaseError::unknown(e.to_string()))?;

    let mut sockets = Vec::with_capacity(socket_arr.len());
    let mut addrs = Vec::with_capacity(socket_arr.len());
    let mut str_addrs = Vec::with_capacity(socket_arr.len());

    for (socket, addr) in socket_arr {
        str_addrs.push(addr.to_string());
        addrs.push(addr);
        sockets.push(socket);
    }

    let tls_opts = ExaTlsOptionsRef {
        ssl_mode: ExaSslMode::Preferred,
        ssl_ca: None,
        ssl_client_cert: None,
        ssl_client_key: None,
    };

    let sockets = if encrypted {
        let futures = iter::zip(sockets, &str_addrs).map(|(s, h)| upgrade_socket(s, h, tls_opts));

        try_join_all(futures)
            .await
            .map_err(|e| ExaDatabaseError::unknown(e.to_string()))?
    } else {
        sockets
    };

    Ok(iter::zip(sockets, addrs).collect())
}

async fn spawn_socket(ip: IpAddr, port: u16) -> Result<(ExaSocket, SocketAddr), BoxDynError> {
    let host = ip.to_string();
    let socket_addr = SocketAddr::new(ip, port);
    let with_socket = WithExaSocket(socket_addr);

    let mut socket = sqlx_core::net::connect_tcp(&host, port, with_socket).await?;
    socket.write_all(&SPECIAL_PACKET).await?;

    // Read response buffer.
    let mut buf = [0; 24];
    socket.read_exact(&mut buf).await?;

    let port = u16::from_le_bytes([buf[4], buf[5]]);
    let ip = IpAddr::from([buf[8], buf[10], buf[12], buf[14]]);
    let address = SocketAddr::new(ip, port);

    Ok((socket, address))
}

async fn upgrade_socket(
    socket: ExaSocket,
    host: &str,
    tls_opts: ExaTlsOptionsRef<'_>,
) -> Result<ExaSocket, SqlxError> {
    tls::maybe_upgrade(socket, host, tls_opts)
        .await
        .map(|(s, _)| s)
}

fn append_filenames(
    query: &mut String,
    addrs: Vec<SocketAddr>,
    is_encrypted: bool,
    is_compressed: bool,
) {
    let prefix = match is_encrypted {
        false => "http",
        true => "https",
    };

    let ext = match is_compressed {
        false => "csv",
        true => "gz",
    };

    for (idx, addr) in addrs.into_iter().enumerate() {
        let filename = format!("AT '{}://{}' FILE '{:0>5}.{}'", prefix, addr, idx, ext);
        query.push_str(&filename);
    }
}
