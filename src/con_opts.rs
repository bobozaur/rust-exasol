use crate::error::{ConnectionError, Error, Result};
use lazy_static::lazy_static;
use rand::rngs::OsRng;
use rand::seq::SliceRandom;
use rand::thread_rng;
use regex::{Captures, Regex};
use rsa::{PaddingScheme, PublicKey, RsaPublicKey};
use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{json, Value};
use std::env;
use std::fmt;
use std::fmt::{Display, Formatter};
use std::net::ToSocketAddrs;

static DEFAULT_PORT: u32 = 8563;
static DEFAULT_FETCH_SIZE: u32 = 5 * 1024 * 1024;
static DEFAULT_CLIENT_PREFIX: &str = "Rust Exasol";

/// Enum listing the protocol versions that can be used when
/// establishing a websocket connection to Exasol.
/// Defaults to the highest defined protocol version and
/// falls back to the highest protocol version supported by the server.
#[derive(Debug)]
pub enum ProtocolVersion {
    V1,
    V2,
    V3,
}

impl Display for ProtocolVersion {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::V1 => write!(f, "1"),
            ProtocolVersion::V2 => write!(f, "2"),
            ProtocolVersion::V3 => write!(f, "3"),
        }
    }
}

impl Serialize for ProtocolVersion {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match self {
            Self::V1 => serializer.serialize_u64(1),
            Self::V2 => serializer.serialize_u64(2),
            Self::V3 => serializer.serialize_u64(3),
        }
    }
}

impl<'de> Deserialize<'de> for ProtocolVersion {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        /// Visitor for deserializing JSON into ProtocolVersion values.
        struct ProtocolVersionVisitor;

        impl<'de> Visitor<'de> for ProtocolVersionVisitor {
            type Value = ProtocolVersion;

            fn expecting(&self, formatter: &mut Formatter) -> fmt::Result {
                formatter
                    .write_str("Expecting an u8 representing the websocket API protocol version.")
            }

            fn visit_u64<E>(self, v: u64) -> std::result::Result<Self::Value, E>
            where
                E: de::Error,
            {
                match v {
                    1 => Ok(ProtocolVersion::V1),
                    2 => Ok(ProtocolVersion::V2),
                    3 => Ok(ProtocolVersion::V3),
                    _ => Err(E::custom("Unknown protocol version!")),
                }
            }
        }

        deserializer.deserialize_u64(ProtocolVersionVisitor)
    }
}

/// Connection options for [Connection](crate::Connection)
/// The DSN may or may not contain a port - if it does not,
/// the port field in this struct is used as a fallback.
///
/// Default is implemented for [ConOpts] so that most fields have fallback values.
/// DSN, user, password and schema fields are practically mandatory,
/// as they otherwise default to an empty string.
/// ```
///  use exasol::ConOpts;
///
///  let opts = ConOpts {
///             dsn: "test_dsn".to_owned(),
///             user: "test_user".to_owned(),
///             password: "test_password".to_owned(),
///             schema: "test_schema".to_owned(),
///             autocommit: false,
///             .. ConOpts::default()
///             };
///
/// // Equivalent to the above
/// let mut opts = ConOpts::new(); // calls default() under the hood
///
/// opts.dsn = "test_dsn".to_owned();
/// opts.user = "test_user".to_owned();
/// opts.password = "test_password".to_owned();
/// opts.schema = "test_schema".to_owned();
/// opts.autocommit = false;
/// ```
#[derive(Debug)]
pub struct ConOpts {
    pub dsn: String,
    pub user: String,
    pub password: String,
    pub schema: String,
    pub port: u32,
    pub protocol_version: ProtocolVersion,
    pub client_name: String,
    pub client_version: String,
    pub client_os: String,
    pub fetch_size: u32,
    pub query_timeout: u32,
    pub use_compression: bool,
    pub autocommit: bool,
}

impl Default for ConOpts {
    fn default() -> Self {
        let crate_version = env::var("CARGO_PKG_VERSION").unwrap_or("UNKNOWN".to_owned());

        Self {
            dsn: "".to_owned(),
            user: "".to_owned(),
            password: "".to_owned(),
            schema: "".to_owned(),
            port: DEFAULT_PORT,
            protocol_version: ProtocolVersion::V3,
            client_name: format!("{} {}", DEFAULT_CLIENT_PREFIX, crate_version),
            client_version: crate_version,
            client_os: env::consts::OS.to_owned(),
            fetch_size: DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            use_compression: false,
            autocommit: true,
        }
    }
}

impl Display for ConOpts {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "DSN: {}\nUser: {}\n Fallback port: {}\n Client: {}\n Client version: {}\n Fetch size: {}\n Query timeout: {}\n Use compression: {}\n Autocommit: {}",
        self.dsn, self.user, self.port, self.client_name, self.client_version, self.fetch_size, self.query_timeout, self.use_compression, self.autocommit)
    }
}

/// Connection options
impl ConOpts {
    /// Creates a default implementation of [ConOpts]
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses the provided DSN to expand ranges and resolve IP addresses.
    /// Connection to all nodes will then be attempted in a random order
    /// until one is successful or all failed.
    pub(crate) fn parse_dsn(&self) -> Result<Vec<String>> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"(?x)
                    ^(.+?)                     # Hostname prefix
                    (?:(\d+)\.\.(\d+)(.*?))?   # Optional range and hostname suffix (e.g. hostname1..4.com)
                    (?:/([0-9A-Fa-f]+))?       # Optional fingerprint (e.g. hostname1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
                    (?::(\d+)?)?$              # Optional port (e.g. hostname1..4.com:8564)
                    ").unwrap();
        }

        RE.captures(&self.dsn)
            .ok_or::<Error>(ConnectionError::InvalidDSN.into())
            .and_then(|cap| {
                let hostname_prefix = &cap[1];
                let start_range = Self::get_dsn_part(&cap, 2);
                let end_range = Self::get_dsn_part(&cap, 3);
                let hostname_suffix = Self::get_dsn_part(&cap, 4);
                let _fingerprint = Self::get_dsn_part(&cap, 5);
                let port = Self::get_dsn_part(&cap, 6)
                    .parse::<u32>()
                    .map_or(self.port, |x| x);

                let mut hosts = vec![];

                if start_range.is_empty() {
                    hosts.push(format!("{}{}:{}", hostname_prefix, hostname_suffix, port));
                } else {
                    let start_range = start_range.parse::<u8>()?;
                    let end_range = end_range.parse::<u8>()?;

                    for i in start_range..end_range {
                        hosts.push(format!(
                            "{}{}{}:{}",
                            hostname_prefix, i, hostname_suffix, port
                        ))
                    }
                }

                let mut addresses = hosts
                    .into_iter()
                    .map(|h| {
                        Ok(h.to_socket_addrs()?
                            .map(|ip| ip.to_string().split(":").take(1).collect())
                            .collect::<Vec<String>>())
                    })
                    .collect::<Result<Vec<Vec<String>>>>()?
                    .into_iter()
                    .flatten()
                    .map(|addr| format!("{}:{}", addr, port))
                    .collect::<Vec<String>>();

                addresses.shuffle(&mut thread_rng());

                Ok(addresses)
            })
    }

    /// Encrypts the password with the provided key
    pub(crate) fn encrypt_password(&self, public_key: RsaPublicKey) -> Result<String> {
        let mut rng = OsRng;
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_password =
            base64::encode(public_key.encrypt(&mut rng, padding, &self.password.as_bytes())?);
        Ok(enc_password)
    }

    pub(crate) fn to_value(self, key: RsaPublicKey) -> Result<Value> {
        Ok(json!({
        "username": self.user,
        "password": self.encrypt_password(key)?,
        "driverName": &self.client_name,
        "clientName": &self.client_name,
        "clientVersion": self.client_version,
        "clientOs": self.client_os,
        "clientRuntime": "Rust",
        "useCompression": self.use_compression,
        "attributes": {
                    "currentSchema": self.schema,
                    "autocommit": self.autocommit,
                    "queryTimeout": self.query_timeout
                    }
        }))
    }

    /// Used for retrieving an optional DSN part, or an empty string if missing
    fn get_dsn_part<'a>(cap: &'a Captures, index: usize) -> &'a str {
        cap.get(index).map_or("", |s| s.as_str())
    }
}
