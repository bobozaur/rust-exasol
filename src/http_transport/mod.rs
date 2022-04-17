mod config;
mod reader;
mod stream;
mod writer;

use crate::error::{DriverError, HttpTransportError, Result};
use crate::http_transport::stream::MaybeTlsStream;
use crate::Connection;
pub(crate) use config::HttpTransportConfig;
pub use config::HttpTransportOpts;
use crossbeam::channel::{Receiver, SendError, Sender};
use crossbeam::thread::{Scope, ScopedJoinHandle};
use csv::{Reader, Terminator, Writer, WriterBuilder};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use reader::ExaRowReader;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufRead, BufReader, Cursor, Error, Read, Write};
use std::marker::PhantomData;
use std::net::{TcpStream, ToSocketAddrs};
use std::process::Output;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use stream::MaybeCompressedStream;
use writer::ExaRowWriter;

/// Convenience alias
pub(crate) type TransportResult<T> = std::result::Result<T, HttpTransportError>;

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

pub(crate) struct HttpExportJob<'a, Q: AsRef<str> + Serialize + Send, T: DeserializeOwned + Send> {
    con: &'a mut Connection,
    query_or_table: Q,
    opts: Option<HttpTransportOpts>,
    row_type: PhantomData<*const T>,
}

impl<'a, Q, T> HttpExportJob<'a, Q, T>
where
    Q: AsRef<str> + Serialize + Send,
    T: DeserializeOwned + Send,
{
    pub(crate) fn new(
        con: &'a mut Connection,
        query_or_table: Q,
        opts: Option<HttpTransportOpts>,
    ) -> Self {
        Self {
            con,
            query_or_table,
            opts,
            row_type: PhantomData,
        }
    }
}

impl<'a, Q, T> HttpTransportJob for HttpExportJob<'a, Q, T>
where
    Q: AsRef<str> + Serialize + Send,
    T: DeserializeOwned + Send,
{
    type Worker = HttpExportThread<T>;
    type DataHandler = Receiver<T>;
    type Output = Vec<T>;
    type Input = ();

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    ) {
        crossbeam::channel::unbounded()
    }

    fn generate_query(query_or_table: &str, hosts: String) -> String {
        format!("EXPORT {} INTO CSV\n{}", query_or_table, hosts)
    }

    fn handle_data(
        _input: Self::Input,
        handler: Self::DataHandler,
    ) -> TransportResult<Self::Output> {
        Ok(handler.into_iter().collect())
    }

    fn get_opts(&mut self) -> HttpTransportOpts {
        self.opts.take().unwrap_or(HttpTransportOpts::default())
    }

    fn get_parts(&mut self) -> (&str, &mut Connection, Self::Input) {
        (self.query_or_table.as_ref(), self.con, ())
    }
}

pub(crate) struct HttpImportJob<
    'a,
    Q: AsRef<str> + Serialize + Send,
    T: Serialize + Send,
    I: IntoIterator<Item = T>,
> {
    con: &'a mut Connection,
    table: Q,
    data: Option<I>,
    opts: Option<HttpTransportOpts>,
    row_type: PhantomData<*const T>,
}

impl<'a, Q, T, I> HttpImportJob<'a, Q, T, I>
where
    Q: AsRef<str> + Serialize + Send,
    T: Serialize + Send,
    I: IntoIterator<Item = T>,
{
    pub(crate) fn new(
        con: &'a mut Connection,
        table: Q,
        data: I,
        opts: Option<HttpTransportOpts>,
    ) -> Self {
        Self {
            con,
            table,
            opts,
            data: Some(data),
            row_type: PhantomData,
        }
    }
}

impl<'a, Q, T, I> HttpTransportJob for HttpImportJob<'a, Q, T, I>
where
    Q: AsRef<str> + Serialize + Send,
    T: Serialize + Send,
    I: IntoIterator<Item = T>,
{
    type Worker = HttpImportThread<T>;
    type DataHandler = Sender<T>;
    type Output = ();
    type Input = Option<I>;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    ) {
        let (s, r) = crossbeam::channel::unbounded();
        (r, s)
    }

    fn generate_query(table: &str, hosts: String) -> String {
        format!("IMPORT INTO {} FROM CSV\n{}", table, hosts)
    }

    fn handle_data(
        mut input: Self::Input,
        handler: Self::DataHandler,
    ) -> TransportResult<Self::Output> {
        for row in input.take().into_iter().flatten() {
            handler
                .send(row)
                .map_err(|_| HttpTransportError::SendError)?
        }
        Ok(())
    }

    fn get_opts(&mut self) -> HttpTransportOpts {
        self.opts.take().unwrap_or(HttpTransportOpts::default())
    }

    fn get_parts(&mut self) -> (&str, &mut Connection, Self::Input) {
        (self.table.as_ref(), self.con, self.data.take())
    }
}

pub(crate) trait HttpTransportJob {
    type Worker: HttpTransportWorker;
    type DataHandler: Clone + Send;
    type Output;
    type Input;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    );

    fn generate_query(query_or_table: &str, hosts: String) -> String;

    fn handle_data(input: Self::Input, handler: Self::DataHandler)
        -> TransportResult<Self::Output>;

    /// Accessor for the underlying HTTP Transport options.
    fn get_opts(&mut self) -> HttpTransportOpts;

    /// Accessor for the underlying fields.
    /// Done together to satisfy the borrow checker.
    fn get_parts(&mut self) -> (&str, &mut Connection, Self::Input);

    fn run(&mut self) -> Result<Self::Output> {
        let scope = crossbeam::scope(|s| {
            let (worker_channel, data_handler) = Self::generate_channel();
            let opts = self.get_opts();
            let (qot, con, input) = self.get_parts();
            let (hosts, num_threads) = Self::get_node_addresses(con, &opts)?;
            let main_barrier = Arc::new(Barrier::new(num_threads + 2));
            let barrier = main_barrier.clone();

            // Start orchestrator thread
            let handle = s.spawn(move |s| {
                Self::orchestrate(
                    s,
                    barrier,
                    worker_channel,
                    hosts,
                    num_threads,
                    opts,
                    qot,
                    con,
                )
            });

            // Aligning threads to the moment that all workers are
            // connected and the query is ready for execution.
            main_barrier.wait();
            let data_res = Self::map_result(Self::handle_data(input, data_handler));

            // We join on the orchestrator handle
            // to check for errors or get the data it retrieved.
            // Thread errors have higher priority than errors that occurred
            // in the query, because the query will generally error out if there
            // was a thread error, but the reverse is not true.
            let res = handle.join().map_err(|_| HttpTransportError::ThreadError);
            Self::map_result(res).and_then(|r| r).and(data_res)
        });

        let res = scope.map_err(|_| HttpTransportError::ThreadError);
        Self::map_result(res).and_then(|res| res)
    }

    /// Orchestrator that will spawn worker threads
    /// and execute the database query once all workers are connected.
    /// Returns the result generated by executing the query.
    fn orchestrate<'s, 'a>(
        s: &'s Scope<'a>,
        barrier: Arc<Barrier>,
        worker_channel: <Self::Worker as HttpTransportWorker>::Channel,
        hosts: Vec<String>,
        num_threads: usize,
        opts: HttpTransportOpts,
        qot: &'a str,
        con: &'a mut Connection,
    ) -> Result<()>
    where
        <Self::Worker as HttpTransportWorker>::Channel: 'a,
    {
        // Setup worker configs
        let run = Arc::new(AtomicBool::new(true));
        let (addr_sender, addr_receiver) = crossbeam::channel::bounded(num_threads);
        let configs = HttpTransportConfig::generate(
            hosts,
            barrier.clone(),
            run.clone(),
            addr_sender,
            opts.encryption(),
            opts.compression(),
        );

        // Start worker threads
        let mut handles = Self::start_workers(s, configs, num_threads, worker_channel);

        // Generate makeshift IMPORT/EXPORT filenames and locations
        let filenames = Self::generate_filenames(num_threads, &addr_receiver);

        // Signal threads to stop if there was an error
        // on the orchestrator thread side
        if filenames.is_err() {
            run.store(false, Ordering::Release);
        }

        let query = Self::generate_query(qot, filenames?);

        // Aligning threads to the moment that all workers are
        // connected and the query is ready for execution.
        barrier.wait();

        // Executing the query blocks until the transport is over
        let query_res = con.execute(query).map(|_| ());
        Self::join_handles(handles).and(query_res)
    }

    /// Joins on all worker handles
    fn join_handles(handles: Vec<ScopedJoinHandle<TransportResult<()>>>) -> Result<()> {
        let res = handles
            .into_iter()
            .map(Self::join_handle)
            .fold(Ok(()), |res, r| r);
        Self::map_result(res)
    }

    /// Joins on one worker handle
    fn join_handle(h: ScopedJoinHandle<TransportResult<()>>) -> TransportResult<()> {
        h.join()
            .map_err(|_| HttpTransportError::ThreadError)
            .and_then(|r| r)
    }

    /// Start a worker thread for each config
    /// and return their handles
    fn start_workers<'s, 'a>(
        s: &'s Scope<'a>,
        configs: Vec<HttpTransportConfig>,
        num_threads: usize,
        worker_channel: <Self::Worker as HttpTransportWorker>::Channel,
    ) -> Vec<ScopedJoinHandle<'s, TransportResult<()>>>
    where
        <Self::Worker as HttpTransportWorker>::Channel: 'a,
    {
        configs
            .into_iter()
            .fold(Vec::with_capacity(num_threads), |mut handles, config| {
                let worker_channel = worker_channel.clone();

                handles.push(s.spawn(move |_| {
                    let mut het = Self::Worker::new(worker_channel);
                    het.start(config)
                }));

                handles
            })
    }

    /// Gather Exasol cluster node hosts for HTTP Transport
    fn get_node_addresses(
        con: &mut Connection,
        opts: &HttpTransportOpts,
    ) -> Result<(Vec<String>, usize)> {
        // Get cluster hosts
        let mut hosts = con.get_nodes()?;

        // Calculate number of threads
        // The default is 0, which means a thread per cluster
        // will be used.
        // If a different value smaller than the number of nodes
        // in the cluster is provided, then that will be used.
        let hosts_len = hosts.len();
        let opts_len = opts.num_threads();
        let num_threads = match 0 < opts_len && opts_len < hosts_len {
            true => opts_len,
            false => hosts_len,
        };

        // Limit hosts to the number of threads we will spawn.
        // Randomizing them to balance load.
        hosts.shuffle(&mut thread_rng());
        let hosts = hosts.into_iter().take(num_threads).collect();
        Ok((hosts, num_threads))
    }

    /// Retrieves the internal addresses generated by
    /// each worker socket connection.
    /// These will be used in generating the filename part of the query.
    fn generate_filenames(num_threads: usize, addr_receiver: &Receiver<String>) -> Result<String> {
        let res = (0..num_threads)
            .into_iter()
            .map(|i| Self::recv_address(&addr_receiver, i))
            .collect::<TransportResult<Vec<String>>>()
            .map(|v| v.join("\n"));
        Self::map_result(res)
    }

    /// Wrapper that receives and parses an Exasol internal address
    #[inline]
    fn recv_address(receiver: &Receiver<String>, index: usize) -> TransportResult<String> {
        let addr = receiver
            .recv()
            .map(|s| format!("AT 'http://{}' FILE '{:0>3}.CSV'", s, index))?;
        Ok(addr)
    }

    /// Convenience function to map errors
    #[inline]
    fn map_result<T>(res: TransportResult<T>) -> Result<T> {
        res.map_err(DriverError::HttpTransportError)
            .map_err(From::from)
    }
}

/// HTTP Transport export worker thread.
pub(crate) struct HttpExportThread<T: DeserializeOwned> {
    data_handler: Sender<T>,
}

impl<T> HttpTransportWorker for HttpExportThread<T>
where
    T: DeserializeOwned + Send,
{
    type Channel = Sender<T>;

    fn new(channel: Self::Channel) -> Self {
        Self {
            data_handler: channel,
        }
    }

    fn process_data(
        &mut self,
        stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()> {
        let mut reader = ExaRowReader::new(stream, run);

        // Read all data in buffer.
        Self::skip_headers(&mut reader, run)?;
        let csv_reader = Reader::from_reader(reader);

        for row in csv_reader.into_deserialize() {
            println!("got row");
            self.data_handler
                .send(row?)
                .map_err(|_| HttpTransportError::SendError)?;
        }

        Ok(())
    }

    fn success(stream: &mut MaybeCompressedStream) -> TransportResult<()> {
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
    ) -> TransportResult<()> {
        Self::stop(run);
        stream.write_all(ERROR_HEADERS).ok();
        Err(error)
    }
}

/// HTTP Transport import worker thread.
pub(crate) struct HttpImportThread<T: Serialize> {
    data_handler: Receiver<T>,
}

impl<T> HttpTransportWorker for HttpImportThread<T>
where
    T: Serialize + Send,
{
    type Channel = Receiver<T>;
    fn new(channel: Self::Channel) -> Self {
        Self {
            data_handler: channel,
        }
    }

    fn process_data(
        &mut self,
        mut stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()> {
        let mut reader = BufReader::new(stream);
        Self::skip_headers(&mut reader, run)?;

        stream = reader.into_inner();
        stream.write_all(SUCCESS_HEADERS)?;

        let mut writer = WriterBuilder::new()
            .has_headers(false)
            .buffer_capacity(WRITE_BUFFER_SIZE)
            .terminator(Terminator::CRLF)
            .from_writer(ExaRowWriter::new(stream, run));

        for row in &self.data_handler {
            writer.serialize(row)?
        }

        writer.flush()?;
        Ok(())
    }

    fn success(stream: &mut MaybeCompressedStream) -> TransportResult<()> {
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
    ) -> TransportResult<()> {
        Self::stop(run);
        Err(error)
    }
}

/// Exasol HTTP Transport protocol implementation
pub(crate) trait HttpTransportWorker {
    /// Communication channel type.
    type Channel: Clone + Send;

    /// Method to generate a new worker instance
    fn new(channel: Self::Channel) -> Self;

    /// Method to overwrite to IMPORT/EXPORT data.
    fn process_data(
        &mut self,
        stream: &mut MaybeCompressedStream,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()>;

    /// Defines success behaviour
    fn success(stream: &mut MaybeCompressedStream) -> TransportResult<()>;

    /// Defines error behaviour
    fn error(
        stream: &mut MaybeCompressedStream,
        error: HttpTransportError,
        run: &Arc<AtomicBool>,
    ) -> TransportResult<()>;

    /// Signals to stop running the whole HTTP transport if an error was encountered.
    fn stop(run: &Arc<AtomicBool>) {
        run.store(false, Ordering::Release);
    }

    /// Starts HTTP Transport
    fn start(&mut self, mut config: HttpTransportConfig) -> TransportResult<()> {
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
    fn initialize<A>(server_addr: A, addr_sender: &mut Sender<String>) -> TransportResult<TcpStream>
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
    ) -> TransportResult<()> {
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
    ) -> TransportResult<MaybeCompressedStream> {
        MaybeTlsStream::wrap(stream, encryption)
            .map(|tls_stream| MaybeCompressedStream::new(tls_stream, compression))
    }

    /// Parses response to return the internal Exasol address
    /// to be used in query.
    fn parse_address(buf: [u8; 24]) -> TransportResult<String> {
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
