mod protocol_version;
use crate::error::ConnectionError;
use lazy_regex::regex;
pub use protocol_version::ProtocolVersion;
use rand::rngs::OsRng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use regex::Captures;
use rsa::{PaddingScheme, PublicKey, RsaPublicKey};
use serde_json::{json, Value};
use std::env;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::net::{SocketAddr, ToSocketAddrs};

// Convenience alias
type ConResult<T> = std::result::Result<T, ConnectionError>;

/// Will box this for efficiency
#[derive(Debug, Clone, Eq, PartialEq)]
struct InnerOpts {
    dsn: Option<String>,
    user: Option<String>,
    password: Option<String>,
    schema: Option<String>,
    port: u16,
    protocol_version: ProtocolVersion,
    client_name: String,
    client_version: String,
    client_os: String,
    fetch_size: usize,
    query_timeout: u64,
    use_encryption: bool,
    use_compression: bool,
    lowercase_columns: bool,
    autocommit: bool,
}

impl Display for InnerOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "DSN: {}\n\
             User: {}\n\
             Schema: {}\n\
             Port: {}\n\
             Protocol version: {}\n\
             Client name: {}\n\
             Client version: {}\n\
             Client OS: {}\n\
             Fetch size: {}\n\
             Query timeout: {}\n\
             Use encryption: {}\n\
             Use compression: {}\n\
             Lowercase columns: {}\n
             Autocommit: {}",
            self.dsn.as_deref().unwrap_or(""),
            self.user.as_deref().unwrap_or(""),
            self.schema.as_deref().unwrap_or(""),
            self.port,
            self.protocol_version,
            self.client_name,
            self.client_version,
            self.client_os,
            self.fetch_size,
            self.query_timeout,
            self.use_encryption,
            self.use_compression,
            self.lowercase_columns,
            self.autocommit
        )
    }
}

impl Default for InnerOpts {
    fn default() -> Self {
        let crate_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "UNKNOWN".to_owned());
        let use_encryption = cfg!(any(feature = "native-tls", feature = "rustls"));
        println!("{}", use_encryption);

        Self {
            dsn: None,
            user: None,
            password: None,
            schema: None,
            port: 8563,
            protocol_version: ProtocolVersion::V3,
            client_name: format!("{} {}", "Rust Exasol", crate_version),
            client_version: crate_version,
            client_os: env::consts::OS.to_owned(),
            fetch_size: 5 * 1024 * 1024,
            query_timeout: 0,
            use_encryption,
            use_compression: false,
            lowercase_columns: true,
            autocommit: true,
        }
    }
}

/// Connection options for [Connection](crate::Connection)
/// The DSN may or may not contain a port - if it does not,
/// the port field in this struct is used as a fallback.
///
/// Default is implemented for [ConOpts] so that most fields have fallback values.
/// DSN, user, password and schema fields are practically mandatory,
/// as they otherwise default to an empty string or JSON null.
/// ```
///  use exasol::ConOpts;
///
///  let mut opts = ConOpts::new(); // calls default() under the hood
///  opts.set_dsn("test_dsn");
///  opts.set_user("test_user");
///  opts.set_password("test_password");
///  opts.set_schema(Some("test_schema"));
///  opts.set_autocommit(false);
/// ```
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub struct ConOpts(Box<InnerOpts>);

impl Display for ConOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Connection options
impl ConOpts {
    /// Creates a default implementation of [ConOpts]
    pub fn new() -> Self {
        Self::default()
    }

    /// DSN (defaults to `None`)
    #[inline]
    pub fn dsn(&self) -> Option<&str> {
        self.0.dsn.as_deref()
    }

    #[inline]
    pub fn set_dsn<T>(&mut self, dsn: T)
    where
        T: Into<String>,
    {
        self.0.dsn = Some(dsn.into())
    }

    /// Port (defaults to `8563`).
    #[inline]
    pub fn port(&self) -> u16 {
        self.0.port
    }

    #[inline]
    pub fn set_port(&mut self, port: u16) {
        self.0.port = port
    }

    /// Schema (defaults to `None`).
    #[inline]
    pub fn schema(&self) -> Option<&str> {
        self.0.schema.as_deref()
    }

    #[inline]
    pub fn set_schema<T>(&mut self, schema: Option<T>)
    where
        T: Into<String>,
    {
        self.0.schema = schema.map(|s| s.into())
    }

    /// User (defaults to `None`).
    #[inline]
    pub fn user(&self) -> Option<&str> {
        self.0.user.as_deref()
    }

    #[inline]
    pub fn set_user<T>(&mut self, user: T)
    where
        T: Into<String>,
    {
        self.0.user = Some(user.into())
    }

    /// Password (defaults to `None`).
    #[inline]
    pub fn password(&self) -> Option<&str> {
        self.0.password.as_deref()
    }

    #[inline]
    pub fn set_password<T>(&mut self, password: T)
    where
        T: Into<String>,
    {
        self.0.password = Some(password.into())
    }

    /// Protocol Version (defaults to `ProtocolVersion::V3`).
    #[inline]
    pub fn protocol_version(&self) -> ProtocolVersion {
        self.0.protocol_version
    }

    #[inline]
    pub fn set_protocol_version(&mut self, pv: ProtocolVersion) {
        self.0.protocol_version = pv
    }

    /// Data fetch size in bytes (defaults to `5,242,880 = 5 * 1024 * 1024`).
    #[inline]
    pub fn fetch_size(&self) -> usize {
        self.0.fetch_size
    }

    #[inline]
    pub fn set_fetch_size(&mut self, fetch_size: usize) {
        self.0.fetch_size = fetch_size
    }

    /// Query timeout (defaults to `0`, which means it's disabled).
    #[inline]
    pub fn query_timeout(&self) -> u64 {
        self.0.query_timeout
    }

    #[inline]
    pub fn set_query_timeout(&mut self, timeout: u64) {
        self.0.query_timeout = timeout
    }

    /// Encryption flag (defaults to `false`).
    #[inline]
    pub fn encryption(&self) -> bool {
        self.0.use_encryption
    }

    /// Sets encryption by allowing the use of a secure websocket (WSS).
    #[inline]
    #[allow(unused)]
    pub fn set_encryption(&mut self, flag: bool) {
        if cfg!(any(feature = "native-tls", feature = "rustls")) {
            self.0.use_encryption = flag;
        } else {
            panic!("native-tls or rustls features must be enabled to set encryption")
        }
    }

    /// Compression flag (defaults to `false`).
    #[inline]
    pub fn compression(&self) -> bool {
        self.0.use_compression
    }

    /// Sets the compression flag.
    #[inline]
    #[allow(unused)]
    pub fn set_compression(&mut self, flag: bool) {
        if cfg!(feature = "flate2") {
            self.0.use_compression = flag;
        } else {
            panic!("flate2 feature must be enabled to set compression")
        }
    }

    /// Lowercase column names flag (defaults to `true`)
    #[inline]
    pub fn lowercase_columns(&self) -> bool {
        self.0.lowercase_columns
    }

    #[inline]
    pub fn set_lowercase_columns(&mut self, flag: bool) {
        self.0.lowercase_columns = flag;
    }

    /// Autocommit flag (defaults to `true`).
    #[inline]
    pub fn autocommit(&self) -> bool {
        self.0.autocommit
    }

    /// Sets the autocommit flag
    #[inline]
    pub fn set_autocommit(&mut self, flag: bool) {
        self.0.autocommit = flag
    }

    /// Convenience method for determining the websocket type.
    #[inline]
    pub(crate) fn ws_prefix(&self) -> &str {
        match self.encryption() {
            false => "ws",
            true => "wss",
        }
    }

    /// Parses the provided DSN to expand ranges and resolve IP addresses.
    /// Connection to all nodes will then be attempted in a random order
    /// until one is successful or all failed.
    pub(crate) fn parse_dsn(&self) -> ConResult<Vec<(String, u16)>> {
        let re = regex!(
            r"(?x)
                    ^(.+?)                     # Hostname prefix
                    (?:(\d+)\.\.(\d+)(.*?))?   # Optional range and hostname suffix (e.g. hostname1..4.com)
                    (?:/([0-9A-Fa-f]+))?       # Optional fingerprint (e.g. hostname1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
                    (?::(\d+)?)?$              # Optional port (e.g. hostname1..4.com:8564)
                    "
        );

        self.0
            .dsn
            .as_deref()
            .and_then(|dsn| re.captures(dsn))
            .ok_or(ConnectionError::InvalidDSN)
            .and_then(|cap| {
                // Parse capture groups from regex
                let hostname_prefix = &cap[1];
                let start_range = Self::parse_dsn_part(&cap, 2);
                let end_range = Self::parse_dsn_part(&cap, 3);
                let hostname_suffix = Self::parse_dsn_part(&cap, 4);
                let _fingerprint = Self::parse_dsn_part(&cap, 5);
                let port = Self::parse_dsn_part(&cap, 6)
                    .parse::<u16>()
                    .unwrap_or(self.0.port);

                // Create a new vec for storing hosts
                let mut hosts = Vec::new();

                // If there's no start range, then there's a single possible host
                if start_range.is_empty() {
                    hosts.push(format!("{}{}:{}", hostname_prefix, hostname_suffix, port));
                } else {
                    // If ranges were provided, then generate all possible hostnames
                    let start_range = start_range.parse::<u8>()?;
                    let end_range = end_range.parse::<u8>()?;

                    // The order does not even matter
                    // Rust's range will increment/decrement appropriately :]
                    for i in start_range..end_range {
                        hosts.push(format!(
                            "{}{}{}:{}",
                            hostname_prefix, i, hostname_suffix, port
                        ))
                    }
                }

                // Now we have to resolve hostnames to IPs
                let mut addresses = hosts
                    .into_iter()
                    .map(|h| Self::host_to_ip_list(h, port))
                    .collect::<ConResult<Vec<Vec<(String, u16)>>>>()?
                    .into_iter()
                    .flatten()
                    .collect::<Vec<(String, u16)>>();

                addresses.shuffle(&mut thread_rng());

                Ok(addresses)
            })
    }

    /// Encrypts the password with the provided key
    pub(crate) fn encrypt_password(&self, public_key: RsaPublicKey) -> ConResult<String> {
        let mut rng = OsRng;
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let pass_bytes = self.0.password.as_deref().unwrap_or("").as_bytes();
        let enc_pass = base64::encode(public_key.encrypt(&mut rng, padding, pass_bytes)?);
        Ok(enc_pass)
    }

    pub(crate) fn into_value(self, key: RsaPublicKey) -> ConResult<Value> {
        Ok(json!({
        "username": self.0.user,
        "password": self.encrypt_password(key)?,
        "driverName": &self.0.client_name,
        "clientName": &self.0.client_name,
        "clientVersion": self.0.client_version,
        "clientOs": self.0.client_os,
        "clientRuntime": "Rust",
        "useCompression": self.0.use_compression,
        "attributes": {
                    "currentSchema": self.0.schema,
                    "autocommit": self.0.autocommit,
                    "queryTimeout": self.0.query_timeout
                    }
        }))
    }

    /// Used for retrieving an optional DSN part, or an empty string if missing
    #[inline]
    fn parse_dsn_part<'a>(cap: &'a Captures, index: usize) -> &'a str {
        cap.get(index).map_or("", |s| s.as_str())
    }

    /// Parses a socket address to an IP
    #[inline]
    fn sock_addr_to_ip(sa: SocketAddr) -> String {
        sa.to_string().split(':').take(1).collect()
    }

    /// Resolves a hostname to a list of IP
    #[inline]
    fn host_to_ip_list(host: String, port: u16) -> ConResult<Vec<(String, u16)>> {
        Ok(host
            .to_socket_addrs()?
            .map(Self::sock_addr_to_ip)
            .map(|ip| (ip, port))
            .collect::<Vec<(String, u16)>>())
    }
}
