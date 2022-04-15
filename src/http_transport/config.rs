use std::sync::atomic::AtomicBool;
use std::sync::mpsc::Sender;
use std::sync::{Arc, Barrier};

/// HTTP Transport options for IMPORT and EXPORT.
///
/// Defaults to 0 threads (meaning a thread will be created for all available Exasol nodes in the cluster),
/// no compression and encryption is conditioned by the `native-tls` and `rustls` feature flags.
pub struct HttpTransportOpts {
    num_threads: u16, // let's, for some reason, not limit this to 255
    compression: bool,
    encryption: bool,
}

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
    pub fn new(num_threads: u16, compression: bool, encryption: bool) -> Self {
        Self::validate_compression(compression);
        Self::validate_encryption(encryption);

        Self {
            num_threads,
            compression,
            encryption,
        }
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
#[derive(Clone)]
pub(crate) struct HttpTransportConfig {
    pub(crate) barrier: Arc<Barrier>,
    pub(crate) run: Arc<AtomicBool>,
    pub(crate) addr_sender: Sender<String>,
    pub(crate) server_addr: Arc<String>,
    pub(crate) use_encryption: bool,
    pub(crate) use_compression: bool,
}

impl HttpTransportConfig {
    pub(crate) fn new(
        barrier: Arc<Barrier>,
        run: Arc<AtomicBool>,
        addr_sender: Sender<String>,
        server_addr: Arc<String>,
        use_encryption: bool,
        use_compression: bool,
    ) -> Self {
        Self {
            barrier,
            run,
            addr_sender,
            server_addr,
            use_encryption,
            use_compression,
        }
    }
}
