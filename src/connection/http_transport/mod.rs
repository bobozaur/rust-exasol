mod config;
mod stream;
mod worker;

use crate::error::{DriverError, HttpTransportError, Result};
use crate::connection::http_transport::stream::MaybeTlsStream;
use crate::Connection;
pub use config::HttpTransportOpts;
use crossbeam::channel::{Receiver, Sender, SendError};
use crossbeam::thread::{Scope, ScopedJoinHandle};
use csv::{Reader, Terminator, Writer, WriterBuilder};
use rand::prelude::SliceRandom;
use rand::thread_rng;
use worker::reader::ExaRowReader;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::io::{BufRead, BufReader, Cursor, Error, Read, Write};
use std::marker::PhantomData;
use std::net::{TcpStream, ToSocketAddrs};
use std::process::Output;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use stream::MaybeCompressedStream;
use worker::writer::ExaRowWriter;
use config::HttpTransportConfig;
use worker::{HttpExportThread, HttpImportThread, HttpTransportWorker};


/// Convenience alias
pub type TransportResult<T> = std::result::Result<T, HttpTransportError>;

pub struct HttpExportJob<'a, Q: AsRef<str> + Serialize + Send, T: DeserializeOwned + Send> {
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
    pub fn new(
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

pub struct HttpImportJob<
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
    pub fn new(
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

pub trait HttpTransportJob {
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
    /// Retrieved together to satisfy the borrow checker.
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

    /// Convenience method to map errors
    #[inline]
    fn map_result<T>(res: TransportResult<T>) -> Result<T> {
        res.map_err(DriverError::HttpTransportError)
            .map_err(From::from)
    }
}