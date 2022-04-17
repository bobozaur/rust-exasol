use crossbeam::channel::Sender;
use rand::prelude::SliceRandom;
use rand::thread_rng;
use std::sync::atomic::AtomicBool;
use std::sync::{Arc, Barrier};

/// HTTP Transport options for IMPORT and EXPORT.
///
/// Defaults to 0 threads (meaning a thread will be created for all available Exasol nodes in the cluster),
/// no compression and encryption is conditioned by the `native-tls` and `rustls` feature flags.
#[derive(Clone, Debug)]
pub struct HttpTransportOpts {
    num_threads: usize,
    compression: bool,
    encryption: bool,
}

#[allow(clippy::derivable_impls)]
impl Default for HttpTransportOpts {
    fn default() -> Self {
        Self {
            num_threads: 0,
            compression: false,
            encryption: cfg!(any(feature = "native-tls", feature = "rustls")),
        }
    }
}

impl HttpTransportOpts {
    pub fn new(num_threads: usize, compression: bool, encryption: bool) -> Self {
        Self::validate_compression(compression);
        Self::validate_encryption(encryption);

        Self {
            num_threads,
            compression,
            encryption,
        }
    }

    pub fn encryption(&self) -> bool {
        self.encryption
    }

    pub fn compression(&self) -> bool {
        self.compression
    }

    pub fn num_threads(&self) -> usize {
        self.num_threads
    }

    fn validate_encryption(flag: bool) {
        if flag && cfg!(not(any(feature = "native-tls", feature = "rustls"))) {
            panic!("native-tls or rustls features must be enabled to use encryption")
        }
    }

    fn validate_compression(flag: bool) {
        if flag && cfg!(not(feature = "flate2")) {
            panic!("flate2 feature must be enabled to use compression")
        }
    }
}

/// Struct that holds utilities and parameters for
/// HTTP transport
#[derive(Clone, Debug)]
pub struct HttpTransportConfig {
    pub(crate) barrier: Arc<Barrier>,
    pub(crate) run: Arc<AtomicBool>,
    pub(crate) addr_sender: Sender<String>,
    pub(crate) server_addr: String,
    pub(crate) encryption: bool,
    pub(crate) compression: bool,
}

impl HttpTransportConfig {
    /// Generates a Vec of configs, one for each given address
    pub fn generate(
        mut hosts: Vec<String>,
        barrier: Arc<Barrier>,
        run: Arc<AtomicBool>,
        addr_sender: Sender<String>,
        use_encryption: bool,
        use_compression: bool,
    ) -> Vec<Self> {
        hosts
            .into_iter()
            .map(|server_addr| Self {
                server_addr,
                barrier: barrier.clone(),
                run: run.clone(),
                addr_sender: addr_sender.clone(),
                encryption: use_encryption,
                compression: use_compression,
            })
            .collect()
    }
}
