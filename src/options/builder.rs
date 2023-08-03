use std::num::NonZeroUsize;

use super::{
    error::ExaConfigError,
    login::{AccessToken, RefreshToken},
    ssl_mode::ExaSslMode,
    Credentials, ExaConnectOptions, Login, ProtocolVersion, DEFAULT_CACHE_CAPACITY,
    DEFAULT_FETCH_SIZE, DEFAULT_PORT,
};
use sqlx_core::{connection::LogSettings, net::tls::CertificateInput, Error as SqlxError};

/// Builder for [`ExaConnectOptions`].
#[derive(Clone, Debug)]
pub struct ExaConnectOptionsBuilder<'a> {
    host: Option<&'a str>,
    port: u16,
    ssl_mode: ExaSslMode,
    ssl_ca: Option<CertificateInput>,
    ssl_client_cert: Option<CertificateInput>,
    ssl_client_key: Option<CertificateInput>,
    statement_cache_capacity: NonZeroUsize,
    username: Option<String>,
    password: Option<String>,
    access_token: Option<String>,
    refresh_token: Option<String>,
    schema: Option<String>,
    protocol_version: ProtocolVersion,
    fetch_size: usize,
    query_timeout: u64,
    compression: bool,
    feedback_interval: u8,
}

impl<'a> Default for ExaConnectOptionsBuilder<'a> {
    fn default() -> Self {
        Self {
            host: None,
            port: DEFAULT_PORT,
            ssl_mode: ExaSslMode::default(),
            ssl_ca: None,
            ssl_client_cert: None,
            ssl_client_key: None,
            statement_cache_capacity: NonZeroUsize::new(DEFAULT_CACHE_CAPACITY).unwrap(),
            username: None,
            password: None,
            access_token: None,
            refresh_token: None,
            schema: None,
            protocol_version: ProtocolVersion::V3,
            fetch_size: DEFAULT_FETCH_SIZE,
            query_timeout: 0,
            compression: false,
            feedback_interval: 1,
        }
    }
}

impl<'a> ExaConnectOptionsBuilder<'a> {
    pub fn build(self) -> Result<ExaConnectOptions, SqlxError> {
        let hostname = self.host.ok_or(ExaConfigError::MissingHost)?;
        let password = self.password.unwrap_or_default();

        // Only one authentication method can be used at once
        let login = match (self.username, self.access_token, self.refresh_token) {
            (Some(user), None, None) => Login::Credentials(Credentials::new(user, password)),
            (None, Some(token), None) => Login::AccessToken(AccessToken::new(token)),
            (None, None, Some(token)) => Login::RefreshToken(RefreshToken::new(token)),
            _ => return Err(ExaConfigError::MultipleAuthMethods.into()),
        };

        let opts = ExaConnectOptions {
            hosts: Self::generate_hosts(hostname),
            port: self.port,
            ssl_mode: self.ssl_mode,
            ssl_ca: self.ssl_ca,
            ssl_client_cert: self.ssl_client_cert,
            ssl_client_key: self.ssl_client_key,
            statement_cache_capacity: self.statement_cache_capacity,
            login,
            schema: self.schema,
            protocol_version: self.protocol_version,
            fetch_size: self.fetch_size,
            query_timeout: self.query_timeout,
            compression: self.compression,
            feedback_interval: self.feedback_interval,
            log_settings: LogSettings::default(),
        };

        Ok(opts)
    }

    pub fn host(&mut self, host: &'a str) -> &mut Self {
        self.host = Some(host);
        self
    }

    pub fn port(&mut self, port: u16) -> &mut Self {
        self.port = port;
        self
    }

    pub fn ssl_mode(&mut self, ssl_mode: ExaSslMode) -> &mut Self {
        self.ssl_mode = ssl_mode;
        self
    }

    pub fn ssl_ca(&mut self, ssl_ca: CertificateInput) -> &mut Self {
        self.ssl_ca = Some(ssl_ca);
        self
    }

    pub fn ssl_client_cert(&mut self, ssl_client_cert: CertificateInput) -> &mut Self {
        self.ssl_client_cert = Some(ssl_client_cert);
        self
    }

    pub fn ssl_client_key(&mut self, ssl_client_key: CertificateInput) -> &mut Self {
        self.ssl_client_key = Some(ssl_client_key);
        self
    }

    pub fn statement_cache_capacity(&mut self, capacity: NonZeroUsize) -> &mut Self {
        self.statement_cache_capacity = capacity;
        self
    }

    pub fn username(&mut self, username: String) -> &mut Self {
        self.username = Some(username);
        self
    }

    pub fn password(&mut self, password: String) -> &mut Self {
        self.password = Some(password);
        self
    }

    pub fn access_token(&mut self, access_token: String) -> &mut Self {
        self.access_token = Some(access_token);
        self
    }

    pub fn refresh_token(&mut self, refresh_token: String) -> &mut Self {
        self.refresh_token = Some(refresh_token);
        self
    }

    pub fn schema(&mut self, schema: String) -> &mut Self {
        self.schema = Some(schema);
        self
    }

    pub fn protocol_version(&mut self, protocol_version: ProtocolVersion) -> &mut Self {
        self.protocol_version = protocol_version;
        self
    }

    pub fn fetch_size(&mut self, fetch_size: usize) -> &mut Self {
        self.fetch_size = fetch_size;
        self
    }

    pub fn query_timeout(&mut self, query_timeout: u64) -> &mut Self {
        self.query_timeout = query_timeout;
        self
    }

    pub fn compression(&mut self, compression: bool) -> &mut Self {
        self.compression = compression;
        self
    }

    pub fn feedback_interval(&mut self, feedback_interval: u8) -> &mut Self {
        self.feedback_interval = feedback_interval;
        self
    }

    /// Exasol supports host ranges, e.g: hostname4..1.com.
    /// This method parses the provided host in the connection string
    /// and generates one for each possible entry in the range.
    ///
    /// We do expect the range to be in the ascending order though,
    /// so `hostname4..1.com` won't work.
    fn generate_hosts(hostname: &str) -> Vec<String> {
        let mut index_accum = 0;

        // We loop through occurences of ranges (..) and try to find one surrounded by digits.
        // If that happens, then we break out of the loop with the index of the range occurance.
        let range_idx = loop {
            let search_str = &hostname[index_accum..];

            // No range? No problem! Return the host as is.
            let Some(idx) = search_str.find("..") else {return vec![hostname.to_owned()]};

            // While if someone actually uses some "..thisismyhostname" host in the connection string
            // would be absolutely insane, it's still somewhat nicer not have this overflow.
            //
            // But really, if you read this and your host looks like that, you really should
            // re-evaluate your taste in domain names.
            //
            // In any case, the index points to the range dots.
            // We want to look before that, hence the substraction.
            let before_opt = idx
                .checked_sub(1)
                .and_then(|i| search_str.as_bytes().get(i));

            // Get the byte after the range dots.
            let after_opt = search_str.as_bytes().get(idx + 2);

            // Check if the range is surrounded by digits and if so, return its index.
            // Continue to the next range if not.
            break match (before_opt, after_opt) {
                (Some(b), Some(a)) if b.is_ascii_digit() || a.is_ascii_digit() => idx + index_accum,
                _ => {
                    index_accum += idx + 2;
                    continue;
                }
            };
        };

        let before_range = &hostname[..range_idx];
        let after_range = &hostname[range_idx + 2..];

        // We wanna find the last non-numeric character before the range index in the first
        // part of the hostname and the first non-numeric character right after the range dots,
        // in the second part of the hostname.
        let opt_start = before_range.rfind(|c: char| !c.is_numeric());
        let opt_end = after_range.find(|c: char| !c.is_numeric());

        // Return the hostname as is if we could not identify the range boundaries.
        let (Some(start_idx), Some(end_idx)) = (opt_start, opt_end) else {
            return vec![hostname.to_owned()];
        };

        // We split the hostname parts to isolate components.
        // The start is incremented as the index is for the last non-numeric character.
        let (prefix, start_range) = before_range.split_at(start_idx + 1);
        let (end_range, suffix) = after_range.split_at(end_idx);

        // Return the hostname as is if the range boundaries are not integers.
        let Ok(start) = start_range.parse::<usize>() else {return vec![hostname.to_owned()];};
        let Ok(end) = end_range.parse::<usize>() else {return vec![hostname.to_owned()];};

        (start..=end)
            .map(|i| format!("{prefix}{i}{suffix}"))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::ExaConnectOptionsBuilder;

    #[test]
    fn test_simple_hostname() {
        let hostname = "myhost.com";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_with_range() {
        let hostname = "myhost1..4.com";
        let expected = vec!["myhost1.com", "myhost2.com", "myhost3.com", "myhost4.com"];

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_hostname_with_big_range() {
        let hostname = "myhost125..127.com";
        let expected = vec!["myhost125.com", "myhost126.com", "myhost127.com"];

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_hostname_with_inverse_range() {
        let hostname = "myhost127..125.com";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert!(generated.is_empty())
    }

    #[test]
    fn test_hostname_with_numbers_no_range() {
        let hostname = "myhost1.4.com";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_with_range_one_numbers() {
        let hostname = "myhost1..b.com";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_with_range_no_numbers() {
        let hostname = "myhosta..b.com";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_starts_with_range() {
        let hostname = "..myhost.com";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_ends_with_range() {
        let hostname = "myhost.com..";

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, vec![hostname]);
    }

    #[test]
    fn test_hostname_real_and_fake_range() {
        let hostname = "myhosta..bcdef1..3.com";
        let expected = vec![
            "myhosta..bcdef1.com".to_owned(),
            "myhosta..bcdef2.com".to_owned(),
            "myhosta..bcdef3.com".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, expected);
    }

    #[test]
    fn test_hostname_two_valid_ranges() {
        let hostname = "myhost1..3cdef4..7.com";
        let expected = vec![
            "myhost1cdef4..7.com".to_owned(),
            "myhost2cdef4..7.com".to_owned(),
            "myhost3cdef4..7.com".to_owned(),
        ];

        let generated = ExaConnectOptionsBuilder::generate_hosts(hostname);
        assert_eq!(generated, expected);
    }
}
