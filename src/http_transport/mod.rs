mod config;
mod stream;
mod writer;
mod reader;

use crate::error::HttpTransportError;
use crate::http_transport::stream::MaybeTlsStream;
pub(crate) use config::HttpTransportConfig;
pub use config::HttpTransportOpts;
use crossbeam::queue::{ArrayQueue, SegQueue};
use csv::{Reader, Terminator, Writer, WriterBuilder};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use stream::MaybeCompressedStream;
use writer::ExaRowWriter;
use reader::ExaRowReader;

/// Convenience alias
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

/// HTTP Transport export worker thread.
pub(crate) struct HttpExportThread<T: DeserializeOwned> {
    data_sender: Sender<T>,
}

impl<T> HttpExportThread<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn new(data_sender: Sender<T>) -> Self {
        Self {
            data_sender
        }
    }
}

impl<T> HttpTransport for HttpExportThread<T>
where
    T: DeserializeOwned,
{
    fn process_data(
        &mut self,
        stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        let mut reader = ExaRowReader::new(stream, run);

        // Read all data in buffer.
        Self::skip_headers(&mut reader, run)?;
        let csv_reader = Reader::from_reader(reader);

        for row in csv_reader.into_deserialize() {
            self.data_sender
                .send(row?)
                .map_err(|_| HttpTransportError::SendError)?;
        }

        Ok(())
    }

    fn success(stream: &mut MaybeCompressedStream) -> HttpResult<()> {
        stream
            .write_all(SUCCESS_HEADERS)
            .and(stream.write_all(END_PACKET))
            .map_err(HttpTransportError::IoError)
    }

    // When doing an EXPORT an error also has to be signaled
    // to Exasol by sending the error headers.
    fn error(
        stream: &mut MaybeCompressedStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        Self::stop(run);
        stream.write_all(ERROR_HEADERS).ok();
        Err(error)
    }
}

/// HTTP Transport import worker thread.
pub(crate) struct HttpImportThread<T: Serialize> {
    data_queue: Arc<SegQueue<T>>,
}

impl<T> HttpImportThread<T>
where
    T: Serialize,
{
    pub(crate) fn new(data_queue: Arc<SegQueue<T>>) -> Self {
        Self { data_queue }
    }
}

impl<T> HttpTransport for HttpImportThread<T>
where
    T: Serialize,
{
    fn process_data(
        &mut self,
        mut stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        let mut reader = BufReader::new(stream);
        Self::skip_headers(&mut reader, run)?;

        stream = reader.into_inner();
        stream.write_all(SUCCESS_HEADERS)?;

        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .buffer_capacity(WRITE_BUFFER_SIZE)
            .terminator(Terminator::CRLF)
            .from_writer(ExaRowWriter::new(stream, run));

        loop {
            match self.data_queue.pop() {
                Some(row) => writer.serialize(row)?,
                None => break
            }
        }

        writer.flush()?;
        Ok(())
    }

    fn success(stream: &mut MaybeCompressedStream) -> HttpResult<()> {
        stream
            .write_all(END_PACKET)
            .map_err(HttpTransportError::IoError)
    }

    // Errors will come from Exasol
    // So simply dropping the socket connection later will suffice.
    // No special action is required.
    fn error(
        _stream: &mut MaybeCompressedStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        Self::stop(run);
        Err(error)
    }
}

pub(crate) trait HttpTransport {
    /// Method to overwrite to IMPORT/EXPORT data.
    fn process_data(
        &mut self,
        stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()>;

    /// Defines success behaviour
    fn success(stream: &mut MaybeCompressedStream) -> HttpResult<()>;

    /// Defines error behaviour
    fn error(
        stream: &mut MaybeCompressedStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()>;

    /// Signals to stop running the whole HTTP transport if an error was encountered.
    fn stop(run: &Arc<AtomicBool>) {
        run.store(false, Ordering::Release);
    }

    /// Starts HTTP Transport
    fn start(&mut self, mut config: HttpTransportConfig) -> HttpResult<()> {
        // Initialize stream and send internal Exasol addresses to parent thread
        let res = Self::initialize(config.server_addr.as_str(), &mut config.addr_sender);

        // Wait for the parent thread to read all addresses, compose and execute query
        config.barrier.wait();

        // Do actual data processing.
        res.and_then(|stream| Self::promote(stream, config.encryption, config.compression))
            .and_then(|stream| self.transport(stream, config.run))
    }

    /// Connects a [TcpStream] to the Exasol server,
    /// gets an internal Exasol address for HTTP transport,
    /// sends the address back to the parent thread
    /// and returns the [TcpStream] for further use.
    fn initialize<A>(server_addr: A, addr_sender: &mut Sender<String>) -> HttpResult<TcpStream>
    where
        A: ToSocketAddrs,
    {
        // Connects stream and writes special packet to retrieve
        // the internal Exasol address to be used in the query.
        // This must always be done unencrypted.
        let mut stream = TcpStream::connect(server_addr)?;
        stream.write_all(&SPECIAL_PACKET)?;

        // Read response buffer.
        let mut buf = [0; 24];
        stream.read_exact(&mut buf)?;

        // Parse response and sends address over to parent thread
        // to generate and execute the query.
        addr_sender
            .send(Self::parse_address(buf)?)
            .map_err(|_| HttpTransportError::SendError)?;

        // Return created stream
        Ok(stream)
    }

    /// Performs the actual data transport.
    fn transport(
        &mut self,
        mut stream: MaybeCompressedStream,
        run: Arc<AtomicBool>,
    ) -> HttpResult<()> {
        let res = match run.load(Ordering::Acquire) {
            false => Err(HttpTransportError::ThreadError),
            true => self.process_data(&mut stream, &run),
        };

        match res {
            Ok(_) => Self::success(&mut stream),
            Err(e) => Self::error(&mut stream, e, &run),
        }
    }

    /// We don't do anything with the HTTP headers
    /// from Exasol, so we'll just read and discard them.
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

        /// Promotes stream to TLS and adds compression as needed
    fn promote(
        stream: TcpStream,
        encryption: bool,
        compression: bool,
    ) -> HttpResult<MaybeCompressedStream> {
        MaybeTlsStream::wrap(stream, encryption)
            .map(|tls_stream| MaybeCompressedStream::new(tls_stream, compression))
    }

    /// Parses response to return the internal Exasol address
    /// to be used in query.
    fn parse_address(buf: [u8; 24]) -> HttpResult<String> {
        let port_bytes = <[u8; 4]>::try_from(&buf[4..8])?;
        let port = u32::from_le_bytes(port_bytes);

        let mut ipaddr = String::with_capacity(16);
        buf[8..]
            .iter()
            .take_while(|b| **b != b'\0')
            .for_each(|b| ipaddr.push(char::from(*b)));

        Ok(format!("{}:{}", ipaddr, port))
    }
}
