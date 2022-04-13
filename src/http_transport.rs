use crate::error::HttpTransportError;
use crossbeam::queue::ArrayQueue;
use csv::{Reader, WriterBuilder};
#[cfg(feature = "native-tls")]
use native_tls::{Identity, TlsAcceptor, TlsStream};
use rcgen::{Certificate, CertificateParams, KeyPair, PKCS_RSA_SHA256};
use rsa::pkcs1::LineEnding;
use rsa::pkcs8::EncodePrivateKey;
use rsa::RsaPrivateKey;
#[cfg(feature = "rustls")]
use rustls::{ServerConfig, ServerConnection, StreamOwned};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs::OpenOptions;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::{Arc, Barrier};
use tungstenite::stream::NoDelay;

type HttpResult<T> = std::result::Result<T, HttpTransportError>;

/// Special Exasol packet that enables tunneling.
/// Exasol responds with an internal address that can be used in query.
const SPECIAL_PACKET: [u8; 12] = [2, 33, 33, 2, 1, 0, 0, 0, 1, 0, 0, 0];

/// Packet that ends tunnelling
const END_PACKET: &[u8; 5] = b"0\r\n\r\n";

/// Packet that tells Exasol the transport was successful
const SUCCESS_HEADERS: &[u8; 66] = b"HTTP/1.1 200 OK\r\n\
                                     Connection: close\r\n\
                                     Transfer-Encoding: chunked\r\n\
                                     \r\n";

/// Packet that tells Exasol the transport had an error
const ERROR_HEADERS: &[u8; 57] = b"HTTP/1.1 500 Internal Server Error\r\n\
                                   Connection: close\r\n\
                                   \r\n";

const WRITE_BUFFER_SIZE: usize = 65536;

/// A stream that might be protected with TLS.
pub enum MaybeTlsStream {
    /// Unencrypted socket stream.
    Plain(TcpStream),
    #[cfg(feature = "native-tls")]
    /// Encrypted socket stream using `native-tls`.
    NativeTls(TlsStream<TcpStream>),
    #[cfg(feature = "rustls")]
    /// Encrypted socket stream using `rustls`.
    Rustls(StreamOwned<rustls::ServerConnection, TcpStream>),
}

impl Read for MaybeTlsStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match *self {
            MaybeTlsStream::Plain(ref mut s) => s.read(buf),
            #[cfg(feature = "native-tls")]
            MaybeTlsStream::NativeTls(ref mut s) => s.read(buf),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.read(buf),
        }
    }
}

impl Write for MaybeTlsStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match *self {
            MaybeTlsStream::Plain(ref mut s) => s.write(buf),
            #[cfg(feature = "native-tls")]
            MaybeTlsStream::NativeTls(ref mut s) => s.write(buf),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match *self {
            MaybeTlsStream::Plain(ref mut s) => s.flush(),
            #[cfg(feature = "native-tls")]
            MaybeTlsStream::NativeTls(ref mut s) => s.flush(),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.flush(),
        }
    }
}

impl NoDelay for MaybeTlsStream {
    fn set_nodelay(&mut self, nodelay: bool) -> std::io::Result<()> {
        match *self {
            MaybeTlsStream::Plain(ref mut s) => s.set_nodelay(nodelay),
            #[cfg(feature = "native-tls")]
            MaybeTlsStream::NativeTls(ref mut s) => s.set_nodelay(nodelay),
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(ref mut s) => s.set_nodelay(nodelay),
        }
    }
}

pub(crate) struct HttpExportThread<T: DeserializeOwned>(Sender<HttpResult<T>>);

impl<T> HttpExportThread<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn new(data_sender: Sender<HttpResult<T>>) -> Self {
        Self(data_sender)
    }
    fn read_chunk_size<R>(reader: &mut R) -> HttpResult<usize>
    where
        R: BufRead,
    {
        let mut hex_len = String::new();
        reader.read_line(&mut hex_len)?;

        match hex_len.len() == 0 {
            true => Ok(0),
            false => usize::from_str_radix(hex_len.trim_end(), 16)
                .map_err(HttpTransportError::ChunkSizeError),
        }
    }

    fn read_chunk<R>(&mut self, reader: &mut R, size: usize) -> HttpResult<()>
    where
        R: BufRead,
    {
        let mut buf = vec![0; size];
        reader.read_exact(&mut buf)?;

        let mut csv_reader = Reader::from_reader(buf.as_slice());
        for result in csv_reader.deserialize() {
            let row = result?;
            self.0
                .send(Ok(row))
                .map_err(|_| HttpTransportError::SendError)?;
        }

        let mut end = [0; 2];
        reader.read_exact(&mut end)?;

        match end == [b'\r', b'\n'] {
            true => Ok(()),
            false => Err(HttpTransportError::DelimiterError(end)),
        }
    }
}

impl<T> HttpTransport for HttpExportThread<T>
where
    T: DeserializeOwned,
{
    fn process_data(
        &mut self,
        stream: &mut MaybeTlsStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        let mut reader = BufReader::new(stream);
        Self::skip_headers(&mut reader, run)?;

        while run.load(Ordering::Acquire) {
            let size = Self::read_chunk_size(&mut reader)?;
            println!("{:?}", &size);
            match size == 0 {
                true => break,
                false => self.read_chunk(&mut reader, size)?,
            }
        }

        let stream = reader.into_inner();
        stream.write_all(SUCCESS_HEADERS)?;
        stream.write_all(END_PACKET)?;
        Ok(())
    }
}

pub(crate) struct HttpImportThread<T: Serialize>(Arc<ArrayQueue<T>>);

impl<T> HttpImportThread<T>
where
    T: Serialize,
{
    pub(crate) fn new(data_queue: Arc<ArrayQueue<T>>) -> Self {
        Self(data_queue)
    }
}

impl<T> HttpTransport for HttpImportThread<T>
where
    T: Serialize,
{
    fn process_data(
        &mut self,
        mut stream: &mut MaybeTlsStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        let mut reader = BufReader::new(&mut stream);
        Self::skip_headers(&mut reader, run)?;

        stream.write_all(SUCCESS_HEADERS)?;
        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .buffer_capacity(WRITE_BUFFER_SIZE)
            .from_writer(stream);

        while run.load(Ordering::Acquire) && !self.0.is_empty() {
            match self.0.pop() {
                None => break,
                Some(r) => writer.serialize(r)?,
            }
        }

        writer.flush()?;
        let stream = writer
            .into_inner()
            .map_err(|_| HttpTransportError::CsvSocketError)?;
        stream.write_all(END_PACKET)?;
        Ok(())
    }
}

pub(crate) trait HttpTransport {
    fn start<A>(
        &mut self,
        server_addr: A,
        barrier: Arc<Barrier>,
        run: Arc<AtomicBool>,
        mut addr_sender: Sender<HttpResult<String>>,
    ) where
        A: ToSocketAddrs,
    {
        Self::initialize(server_addr, &mut addr_sender)
            .and_then(|stream| self.do_transport(stream, barrier, &run))
            .unwrap_or_else(|e| Self::report_error(&mut addr_sender, Err(e), &run));
    }

    fn do_transport(
        &mut self,
        stream: TcpStream,
        barrier: Arc<Barrier>,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        barrier.wait();

        if run.load(Ordering::Acquire) {
            let mut stream = Self::promote_stream(stream)?;
            self.process_data(&mut stream, run).map_err(|e| {
                stream.write_all(ERROR_HEADERS).ok();
                stream.write_all(END_PACKET).ok();
                e
            })?
        }
        Ok(())
    }

    fn process_data(
        &mut self,
        stream: &mut MaybeTlsStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()>;

    fn promote_stream(stream: TcpStream) -> HttpResult<MaybeTlsStream> {
        #[cfg(any(feature = "rustls", feature = "native-tls"))]
        let cert = make_cert()?;

        #[cfg(feature = "native-tls")]
        return Self::get_native_tls_stream(stream, cert);

        #[cfg(feature = "rustls")]
        return Self::get_rustls_stream(stream, cert);

        Ok(MaybeTlsStream::Plain(stream))
    }

    fn report_error(
        reporter: &mut Sender<HttpResult<String>>,
        msg: HttpResult<String>,
        run: &Arc<AtomicBool>,
    ) {
        reporter.send(msg).ok();
        run.store(false, Ordering::Release);
    }

    /// Connects a [TcpStream] to the Exasol server,
    /// gets an internal Exasol address for HTTP transport,
    /// sends the address back to the parent thread
    /// and returns the [TcpStream] for further use.
    fn initialize<A>(
        server_addr: A,
        addr_sender: &mut Sender<HttpResult<String>>,
    ) -> HttpResult<TcpStream>
    where
        A: ToSocketAddrs,
    {
        let mut stream = TcpStream::connect(server_addr)?;
        stream.write_all(&SPECIAL_PACKET)?;
        // println!("started_thread");

        let mut buf = [0; 24];
        stream.read_exact(&mut buf)?;

        let port_bytes = <[u8; 4]>::try_from(&buf[4..8])?;
        let port = u32::from_le_bytes(port_bytes);

        let mut ipaddr = String::with_capacity(16);
        buf[8..]
            .iter()
            .take_while(|b| **b != b'\0')
            .for_each(|b| ipaddr.push(char::from(*b)));

        addr_sender
            .send(Ok(format!("http://{}:{}", ipaddr, port)))
            .map_err(|_| HttpTransportError::SendError)?;
        Ok(stream)
    }

    fn skip_headers<R>(
        mut reader: R,
        run: &Arc<AtomicBool>,
    ) -> std::result::Result<(), std::io::Error>
    where
        R: BufRead,
    {
        let mut line = String::new();
        while run.load(Ordering::Acquire) && line != "\r\n" {
            line.clear();
            reader.read_line(&mut line)?;
        }
        Ok(())
    }
    #[cfg(feature = "native-tls")]
    fn get_native_tls_stream(socket: TcpStream, cert: Certificate) -> HttpResult<MaybeTlsStream> {
        let tls_cert = cert.serialize_pem()?;
        let key = cert.serialize_private_key_pem();

        let ident = Identity::from_pkcs8(tls_cert.as_bytes(), key.as_bytes())?;
        let mut connector = TlsAcceptor::new(ident)?;
        Ok(MaybeTlsStream::NativeTls(connector.accept(socket)?))
    }

    #[cfg(feature = "rustls")]
    fn get_rustls_stream(socket: TcpStream, cert: Certificate) -> HttpResult<MaybeTlsStream> {
        let tls_cert = rustls::Certificate(cert.serialize_der()?);
        let key = rustls::PrivateKey(cert.serialize_private_key_der());

        let config = {
            Arc::new(
                ServerConfig::builder()
                    .with_safe_defaults()
                    .with_no_client_auth()
                    .with_single_cert(vec![tls_cert], key)?,
            )
        };
        let client = ServerConnection::new(config)?;
        let stream = StreamOwned::new(client, socket);

        Ok(MaybeTlsStream::Rustls(stream))
    }
}

fn make_cert() -> HttpResult<Certificate> {
    let mut params = CertificateParams::default();
    params.alg = &PKCS_RSA_SHA256;
    params.key_pair = Some(make_rsa_keypair()?);
    Ok(Certificate::from_params(params)?)
}

fn make_rsa_keypair() -> HttpResult<KeyPair> {
    let mut rng = rand::thread_rng();
    let bits = 2048;
    let private_key = RsaPrivateKey::new(&mut rng, bits)?;
    let key = private_key.to_pkcs8_pem(LineEnding::CRLF)?;
    Ok(KeyPair::from_pem(&key)?)
}
