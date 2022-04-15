mod config;
mod stream;

use crate::error::HttpTransportError;
use crate::http_transport::stream::MaybeTlsStream;
pub(crate) use config::HttpTransportConfig;
pub use config::HttpTransportOpts;
use crossbeam::queue::ArrayQueue;
use csv::{Reader, WriterBuilder};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufRead, BufReader, Cursor, Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::Sender;
use std::sync::Arc;
use stream::MaybeCompressedStream;

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

pub(crate) struct HttpExportThread<T: DeserializeOwned> {
    data_sender: Sender<T>,
    buf: Vec<u8>,
}

impl<T> HttpExportThread<T>
where
    T: DeserializeOwned,
{
    pub(crate) fn new(data_sender: Sender<T>) -> Self {
        Self {
            data_sender,
            buf: Vec::new(),
        }
    }

    fn read_chunk_size<R>(reader: &mut R) -> HttpResult<usize>
    where
        R: BufRead,
    {
        let mut hex_len = String::new();
        reader.read_line(&mut hex_len)?;

        match hex_len.is_empty() {
            true => Ok(0),
            false => usize::from_str_radix(hex_len.trim_end(), 16)
                .map_err(HttpTransportError::ChunkSizeError),
        }
    }

    fn read_chunk<R>(reader: &mut R, size: usize, buf: &mut Vec<u8>) -> HttpResult<()>
    where
        R: BufRead,
    {
        // Adding 2 to size to read the trailing \r\n that delimit the data chunk
        let mut tmp_buf = vec![0; size + 2];
        reader.read_exact(&mut tmp_buf)?;

        // Check chunk end for trailing delimiter
        // Note the reversed delimiter due to subsequent pop() calls
        match [tmp_buf.pop(), tmp_buf.pop()] == [Some(b'\n'), Some(b'\r')] {
            true => Ok(()),
            false => Err(HttpTransportError::DelimiterError),
        }?;

        buf.extend(tmp_buf);
        Ok(())
    }

    fn read_all<R>(run: &Arc<AtomicBool>, reader: &mut R, buf: &mut Vec<u8>) -> HttpResult<()>
    where
        R: BufRead,
    {
        let mut chunk_size = Self::read_chunk_size(reader)?;

        // Keep reading while not signaled to stop
        // and while there's still data.
        while run.load(Ordering::Acquire) && chunk_size > 0 {
            Self::read_chunk(reader, chunk_size, buf)
                .and(Self::read_chunk_size(reader))
                .map(|size| chunk_size = size)?
        }

        Ok(())
    }

    /// Deserialize and send rows over to parent thread
    fn deserialize(&mut self) -> HttpResult<()> {
        // Taking ownership of the data.
        let cursor = Cursor::new(std::mem::take(&mut self.buf));
        let csv_reader = Reader::from_reader(cursor);

        for row in csv_reader.into_deserialize() {
            self.data_sender
                .send(row?)
                .map_err(|_| HttpTransportError::SendError)?;
        }

        Ok(())
    }
}

impl<T> HttpTransport for HttpExportThread<T>
where
    T: DeserializeOwned,
{
    /// Overwritten to also deserialize rows and send them to parent thread.
    fn run_flow(&mut self, stream: MaybeCompressedStream, run: Arc<AtomicBool>) -> HttpResult<()> {
        self.do_transport(stream, run).and(self.deserialize())
    }

    fn process_data(
        &mut self,
        stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> HttpResult<()> {
        let mut reader = BufReader::new(stream);

        // Read all data in buffer.
        Self::skip_headers(&mut reader, run)
            .map_err(HttpTransportError::IoError)
            .and(Self::read_all(run, &mut reader, &mut self.buf))
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
            .from_writer(stream);

        while run.load(Ordering::Acquire) && !self.0.is_empty() {
            match self.0.pop() {
                None => break,
                Some(r) => writer.serialize(r)?,
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
    /// Runs the flow of the defined HTTP transport
    /// Added to be able to customize between IMPORT/EXPORT
    fn run_flow(&mut self, stream: MaybeCompressedStream, run: Arc<AtomicBool>) -> HttpResult<()> {
        self.do_transport(stream, run)
    }

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
        res.and_then(|stream| Self::promote(stream, config.use_encryption, config.use_compression))
            .and_then(|stream| self.run_flow(stream, config.run))
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
            .send(Self::parse_response(buf)?)
            .map_err(|_| HttpTransportError::SendError)?;

        // Return created stream
        Ok(stream)
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
    fn parse_response(buf: [u8; 24]) -> HttpResult<String> {
        let port_bytes = <[u8; 4]>::try_from(&buf[4..8])?;
        let port = u32::from_le_bytes(port_bytes);

        let mut ipaddr = String::with_capacity(16);
        buf[8..]
            .iter()
            .take_while(|b| **b != b'\0')
            .for_each(|b| ipaddr.push(char::from(*b)));

        Ok(format!("{}:{}", ipaddr, port))
    }

    /// Performs the actual data transport.
    fn do_transport(
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
}
