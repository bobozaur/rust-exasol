use std::env;
use regex::{Regex, Captures};
use lazy_static::lazy_static;
use crate::error::{Error, ConnectionError, Result};
use rand::{thread_rng};
use rand::rngs::OsRng;
use rand::seq::SliceRandom;
use std::net::ToSocketAddrs;
use rsa::{PaddingScheme, PublicKey, RsaPublicKey};

#[derive(Debug)]
pub struct ConOpts {
    pub dsn: String,
    pub user: String,
    pub password: String,
    pub schema: String,
    pub port: u32,
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
        let crate_version = env::var("CARGO_PKG_VERSION")
            .ok()
            .map_or("UNKNOWN".to_owned(), |x| x);

        Self {
            dsn: "".to_owned(),
            user: "".to_owned(),
            password: "".to_owned(),
            schema: "".to_owned(),
            port: 8563,
            client_name: format!("Rust Exasol {}", crate_version),
            client_version: crate_version,
            client_os: env::consts::OS.to_owned(),
            fetch_size: 5 * 1024 * 1024,
            query_timeout: 0,
            use_compression: false,
            autocommit: true,
        }
    }
}


/// Connection options
impl ConOpts {
    /// Parses the provided dsn to expand ranges and resolve IP addresses.
    /// Connection to all nodes will then be attempted in a random order
    /// until one is successful or all failed.
    pub(crate) fn parse_dsn(&self) -> Result<Vec<String>> {
        lazy_static! {
            static ref RE: Regex = Regex::new(r"(?x)
                    ^(.+?)                     # Hostname prefix
                    (?:(\d+)\.\.(\d+)(.*?))?   # Optional range and hostname suffix (e.g. myxasol1..4.com)
                    (?:/([0-9A-Fa-f]+))?       # Optional fingerprint (e.g. myexasol1..4.com/135a1d2dce102de866f58267521f4232153545a075dc85f8f7596f57e588a181)
                    (?::(\d+)?)?$              # Optional port (e.g. myexasol1..4.com:8564)
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
                    .map_or(8563, |x| x);

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

    /// Used for retrieving an optional DSN part, or an empty string if missing
    fn get_dsn_part<'a>(cap: &'a Captures, index: usize) -> &'a str {
        cap.get(index).map_or("", |s| s.as_str())
    }
}