//! Tests encrypted compression support with native-tls and flate2 features
//! We'll also include a fingerprint in the DSN

#[cfg(test)]
#[allow(unused)]
mod tests {
    use __native_tls::TlsConnector;
    use exasol::error::Result;
    use exasol::*;
    use std::env;

    #[test]
    #[allow(unused)]
    fn test_enc_compression() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut opts = ConOpts::new();
        opts.set_user(user);
        opts.set_password(password);
        opts.set_dsn(dsn);
        opts.set_schema(Some(schema));

        let mut config = TlsConnector::builder()
            .danger_accept_invalid_certs(true)
            .build()
            .unwrap();
        let connector = Connector::NativeTls(config);

        let mut exa_con = Connection::from_connector(connector, opts).unwrap();
        let mut result = exa_con
            .execute("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001;")
            .unwrap();

        let http_opts = HttpTransportOpts::new(1, true, true);
        let data: Vec<(String, String, u16)> = exa_con
            .export_to_vec(
                "SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001",
                Some(http_opts),
            )
            .unwrap();

        let http_opts = HttpTransportOpts::new(1, true, true);
        exa_con
            .import_from_iter("RUST.EXA_RUST_TEST", data, Some(http_opts))
            .unwrap();
    }
}
