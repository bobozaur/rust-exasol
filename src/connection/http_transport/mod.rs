mod config;
mod stream;
mod worker;

use super::TRANSPORT_BUFFER_SIZE;
use crate::error::{DriverError, HttpTransportError, Result};
use crate::Connection;
use config::HttpTransportConfig;
pub use config::HttpTransportOpts;
use crossbeam::channel::{IntoIter, Receiver, Sender};
use crossbeam::thread::{Scope, ScopedJoinHandle};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use serde::Serialize;
use std::io::{Error, ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use worker::reader::ExaRowReader;
use worker::writer::ExaRowWriter;
use worker::{HttpExportThread, HttpImportThread, HttpTransportWorker};

pub struct ExaWriter {
    sender: Sender<Vec<u8>>,
    buf: Vec<u8>,
}

impl ExaWriter {
    pub fn new(sender: Sender<Vec<u8>>) -> Self {
        Self {
            sender,
            buf: Vec::with_capacity(TRANSPORT_BUFFER_SIZE),
        }
    }
}

impl Write for ExaWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.buf.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        let mut old_buf = Vec::with_capacity(TRANSPORT_BUFFER_SIZE);
        std::mem::swap(&mut self.buf, &mut old_buf);
        self.sender.send(old_buf).map_err(|_| {
            Error::new(
                ErrorKind::BrokenPipe,
                "Could not send data chunk to worker thread",
            )
        })
    }
}

pub struct ExaReader {
    receiver: IntoIter<Vec<u8>>,
    buf: Vec<u8>,
    pos: usize,
}

impl ExaReader {
    pub fn new(receiver: Receiver<Vec<u8>>) -> Self {
        Self {
            receiver: receiver.into_iter(),
            buf: Vec::new(),
            pos: 0,
        }
    }
}

impl Read for ExaReader {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut num_bytes = (&self.buf[self.pos..]).read(buf)?;

        if num_bytes == 0 {
            match self.receiver.next() {
                None => num_bytes = 0,
                Some(v) => {
                    self.buf = v;
                    self.pos = 0;
                    num_bytes = (&self.buf[self.pos..]).read(buf)?;
                }
            }
        }

        self.pos += num_bytes;
        Ok(num_bytes)
    }
}

/// Convenience alias
pub type TransportResult<T> = std::result::Result<T, HttpTransportError>;

pub struct HttpExportJob<'a, Q: AsRef<str> + Serialize + Send, T, F: FnOnce(ExaReader) -> T> {
    con: &'a mut Connection,
    query_or_table: Q,
    opts: Option<HttpTransportOpts>,
    callback: Option<F>,
}

impl<'a, Q, T, F> HttpExportJob<'a, Q, T, F>
where
    Q: AsRef<str> + Serialize + Send,
    F: FnOnce(ExaReader) -> T,
{
    pub fn new(
        con: &'a mut Connection,
        query_or_table: Q,
        callback: F,
        opts: Option<HttpTransportOpts>,
    ) -> Self {
        Self {
            con,
            query_or_table,
            opts,
            callback: Some(callback),
        }
    }
}

impl<'a, Q, T, F> HttpTransportJob for HttpExportJob<'a, Q, T, F>
where
    Q: AsRef<str> + Serialize + Send,
    F: FnOnce(ExaReader) -> T,
{
    type Worker = HttpExportThread;
    type DataHandler = Receiver<Vec<u8>>;
    type Callback = F;
    type Output = T;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    ) {
        crossbeam::channel::unbounded()
    }

    fn generate_query(query_or_table: &str, hosts: String) -> String {
        format!("EXPORT ({}) INTO CSV\n{}", query_or_table, hosts)
    }

    fn handle_data(handler: Self::DataHandler, callback: Self::Callback) -> Self::Output {
        let reader = ExaReader::new(handler);
        callback(reader)
    }

    fn get_opts(&mut self) -> HttpTransportOpts {
        self.opts.take().unwrap_or_default()
    }

    fn get_parts(&mut self) -> (&str, &mut Connection, Option<Self::Callback>) {
        (self.query_or_table.as_ref(), self.con, self.callback.take())
    }
}

pub struct HttpImportJob<'a, Q: AsRef<str> + Serialize + Send, T, F: FnOnce(ExaWriter) -> T> {
    con: &'a mut Connection,
    table: Q,
    opts: Option<HttpTransportOpts>,
    callback: Option<F>,
}

impl<'a, Q, T, F> HttpImportJob<'a, Q, T, F>
where
    Q: AsRef<str> + Serialize + Send,
    F: FnOnce(ExaWriter) -> T,
{
    pub fn new(
        con: &'a mut Connection,
        table: Q,
        callback: F,
        opts: Option<HttpTransportOpts>,
    ) -> Self {
        Self {
            con,
            table,
            opts,
            callback: Some(callback),
        }
    }
}

impl<'a, Q, T, F> HttpTransportJob for HttpImportJob<'a, Q, T, F>
where
    Q: AsRef<str> + Serialize + Send,
    F: FnOnce(ExaWriter) -> T,
{
    type Worker = HttpImportThread;
    type DataHandler = Sender<Vec<u8>>;
    type Callback = F;
    type Output = T;

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

    fn handle_data(handler: Self::DataHandler, callback: Self::Callback) -> Self::Output {
        let writer = ExaWriter::new(handler);
        callback(writer)
    }

    fn get_opts(&mut self) -> HttpTransportOpts {
        self.opts.take().unwrap_or_default()
    }

    fn get_parts(&mut self) -> (&str, &mut Connection, Option<Self::Callback>) {
        (self.table.as_ref(), self.con, self.callback.take())
    }
}

pub trait HttpTransportJob {
    type Worker: HttpTransportWorker;
    type DataHandler: Clone + Send;
    type Callback;
    type Output;

    fn generate_channel() -> (
        <Self::Worker as HttpTransportWorker>::Channel,
        Self::DataHandler,
    );

    fn generate_query(query_or_table: &str, hosts: String) -> String;

    fn handle_data(handler: Self::DataHandler, callback: Self::Callback) -> Self::Output;

    /// Accessor for the underlying HTTP Transport options.
    fn get_opts(&mut self) -> HttpTransportOpts;

    /// Accessor for the underlying fields.
    /// Retrieved together to satisfy the borrow checker.
    fn get_parts(&mut self) -> (&str, &mut Connection, Option<Self::Callback>);

    fn run(&mut self) -> Result<Self::Output> {
        let scope = crossbeam::scope(|s| {
            let (worker_channel, data_handler) = Self::generate_channel();
            let opts = self.get_opts();
            let (qot, con, cb) = self.get_parts();
            let (hosts, num_threads) = Self::get_node_addresses(con, &opts)?;

            // Start orchestrator thread
            let handle = s.spawn(move |s| {
                Self::orchestrate(s, worker_channel, hosts, num_threads, opts, qot, con)
            });

            let output = cb
                .map(|callback| Self::handle_data(data_handler, callback))
                .ok_or(HttpTransportError::ThreadError)
                .map_err(DriverError::HttpTransportError)
                .map_err(crate::error::Error::DriverError);

            // We join on the orchestrator handle
            // to check for errors or get the data it retrieved.
            // Thread errors have higher priority than errors that occurred
            // in the query, because the query will generally error out if there
            // was a thread error, but the reverse is not true.
            let res = handle.join().map_err(|_| HttpTransportError::ThreadError);
            map_transport_result(res).and_then(|r| r).and(output)
        });

        let res = scope.map_err(|_| HttpTransportError::ThreadError);
        map_transport_result(res).and_then(|res| res)
    }

    /// Orchestrator that will spawn worker threads
    /// and execute the database query once all workers are connected.
    /// Returns the result generated by executing the query.
    fn orchestrate<'s, 'a>(
        s: &'s Scope<'a>,
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
        let barrier = Arc::new(Barrier::new(num_threads + 1));
        let (addr_sender, addr_receiver) = crossbeam::channel::bounded(num_threads);
        let use_encryption = opts.encryption();
        let use_compression = opts.compression();
        let configs = HttpTransportConfig::generate(
            hosts,
            barrier.clone(),
            run.clone(),
            addr_sender,
            use_encryption,
            use_compression,
        );

        // Start worker threads
        let handles = Self::start_workers(s, configs, num_threads, worker_channel);

        // Generate makeshift IMPORT/EXPORT filenames and locations
        let filenames =
            Self::generate_filenames(use_encryption, use_compression, num_threads, &addr_receiver);

        // Signal threads to stop if there was an error
        // on the orchestrator thread side
        if filenames.is_err() {
            run.store(false, Ordering::Release);
        }

        // Aligning threads to the moment that all workers are
        // connected and the query is ready for execution.
        barrier.wait();

        // Executing the query blocks until the transport is over
        let query_res = filenames.and_then(|hosts| {
            let query = Self::generate_query(qot, hosts);
            con.execute(query)?;
            Ok(())
        });

        // Synchronize worker threads with the orchestrator
        // as the sockets must not close before the server
        // reads everything from them (query execution finishes)
        barrier.wait();

        Self::join_handles(handles).and(query_res)
    }

    /// Joins on all worker handles
    fn join_handles(handles: Vec<ScopedJoinHandle<TransportResult<()>>>) -> Result<()> {
        let res = handles
            .into_iter()
            .map(Self::join_handle)
            .fold(Ok(()), |_, r| r);
        map_transport_result(res)
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
    fn generate_filenames(
        use_encryption: bool,
        use_compression: bool,
        num_threads: usize,
        addr_receiver: &Receiver<String>,
    ) -> Result<String> {
        let prefix = match use_encryption {
            false => "http",
            true => "https",
        };

        let ext = match use_compression {
            false => "csv",
            true => "gz",
        };

        let res = (0..num_threads)
            .into_iter()
            .map(|i| Self::recv_address(prefix, ext, addr_receiver, i))
            .collect::<TransportResult<Vec<String>>>()
            .map(|v| v.join("\n"));
        map_transport_result(res)
    }

    /// Wrapper that receives and parses an Exasol internal address
    #[inline]
    fn recv_address(
        prefix: &str,
        ext: &str,
        receiver: &Receiver<String>,
        index: usize,
    ) -> TransportResult<String> {
        let addr = receiver
            .recv()
            .map(|s| format!("AT '{}://{}' FILE '{:0>3}.{}'", prefix, s, index, ext))?;
        Ok(addr)
    }
}

/// Convenience function for mapping a transport result to the crate's main result type
#[inline]
fn map_transport_result<T>(res: TransportResult<T>) -> Result<T> {
    res.map_err(DriverError::HttpTransportError)
        .map_err(From::from)
}
