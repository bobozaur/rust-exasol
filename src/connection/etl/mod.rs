mod error;
mod export;
mod import;
mod non_tls;
#[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
mod tls;
mod traits;

use std::net::{IpAddr, SocketAddrV4};

use crate::ExaDatabaseError;
use arrayvec::ArrayString;
use futures_channel::oneshot::Receiver;
use futures_core::future::BoxFuture;
use sqlx_core::error::Error as SqlxError;
use sqlx_core::net::Socket;

use non_tls::spawn_non_tls_sockets;

use super::websocket::socket::ExaSocket;

pub use export::{ExaExport, ExportBuilder, QueryOrTable};
pub use import::{ExaImport, ImportBuilder, Trim};

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

const GZ_FILE_EXT: &str = "gz";
const CSV_FILE_EXT: &str = "csv";

const HTTP_SCHEME: &str = "http";
const HTTPS_SCHEME: &str = "https";

/// We do some implicit buffering as we have to parse
/// the incoming HTTP request and ignore the headers, read chunk sizes, etc.
///
/// We do that by reading one byte at a time and keeping track
/// of what we read to walk through states.
///
/// It would be higly inefficient to read a single byte from the
/// TCP stream every time, so we instead use a small [`futures_util::io::BufReader`].
const IMPLICIT_BUFFER_CAP: usize = 128;

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

async fn spawn_sockets(
    num: usize,
    ips: Vec<IpAddr>,
    port: u16,
    with_tls: bool,
) -> Result<
    Vec<(
        BoxFuture<'static, Result<ExaSocket, SqlxError>>,
        Receiver<SocketAddrV4>,
    )>,
    SqlxError,
> {
    let num_sockets = if num > 0 { num } else { ips.len() };

    #[cfg(any(feature = "etl_native_tls", feature = "etl_rustls"))]
    match with_tls {
        true => tls::spawn_tls_sockets(num_sockets, ips, port).await,
        false => spawn_non_tls_sockets(num_sockets, ips, port).await,
    }

    #[cfg(not(any(feature = "etl_native_tls", feature = "etl_rustls")))]
    match with_tls {
        true => Err(SqlxError::Tls("A TLS feature is required for ETL TLS")),
        false => spawn_non_tls_sockets(num_sockets, ips, port).await,
    }
}

fn append_filenames(
    query: &mut String,
    addrs: Vec<SocketAddrV4>,
    with_tls: bool,
    with_compression: bool,
) {
    let prefix = match with_tls {
        false => HTTP_SCHEME,
        true => HTTPS_SCHEME,
    };

    let ext = match with_compression {
        false => CSV_FILE_EXT,
        true => GZ_FILE_EXT,
    };

    for (idx, addr) in addrs.into_iter().enumerate() {
        let filename = format!("AT '{}://{}' FILE '{:0>5}.{}'\n", prefix, addr, idx, ext);
        query.push_str(&filename);
    }
}

/// Behind the scenes Exasol will import/export to a file located on the
/// one-shot HTTP server we will host on this socket.
///
/// The "file" will be defined something like "http://10.25.0.2/0001.csv".
///
/// While I don't know the exact implementation details, I assume Exasol
/// does port forwarding to/from the socket we connect (the one in this function)
/// and a local socket it opens (which has the address used in the file).
///
/// This function is used to retrieve the internal IP of that local socket,
/// so we can construct the file name.
async fn get_etl_addr<S>(mut socket: S) -> Result<(S, SocketAddrV4), SqlxError>
where
    S: Socket,
{
    // Write special packet
    let mut write_start = 0;

    while write_start < SPECIAL_PACKET.len() {
        let written = socket.write(&SPECIAL_PACKET[write_start..]).await?;
        write_start += written;
    }

    // Read response buffer.
    let mut buf = [0; 24];
    let mut read_start = 0;

    while read_start < buf.len() {
        let mut buf = &mut buf[read_start..];
        let read = socket.read(&mut buf).await?;
        read_start += read;
    }

    // Parse address
    let mut ip_buf = ArrayString::<16>::new_const();

    buf[8..]
        .iter()
        .take_while(|b| **b != b'\0')
        .for_each(|b| ip_buf.push(char::from(*b)));

    let port = u16::from_le_bytes([buf[4], buf[5]]);
    let ip = ip_buf.parse().map_err(ExaDatabaseError::unknown)?;
    let address = SocketAddrV4::new(ip, port);

    Ok((socket, address))
}
