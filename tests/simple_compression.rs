//! Tests unencrypted compression support

#[cfg(test)]
#[allow(unused)]
mod tests {
    use exasol::*;
    use std::env;

    #[test]
    #[allow(unused)]
    fn test_compression() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut opts = ConOpts::new();
        opts.set_user(user);
        opts.set_password(password);
        opts.set_dsn(dsn);
        opts.set_compression(true);

        let mut exa_con = Connection::new(opts).unwrap();
        let mut result = exa_con
            .execute("SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001;")
            .unwrap();

        let rows: Vec<(String, String, u16)> = exa_con.iter_result(&mut result).unwrap();

        let http_opts = HttpTransportOpts::new(1, true, false);
        let data: Vec<(String, String, u16)> = exa_con
            .export_to_vec(
                "SELECT * FROM RUST.EXA_RUST_TEST LIMIT 2001",
                Some(http_opts),
            )
            .unwrap();

        let http_opts = HttpTransportOpts::new(1, true, false);
        exa_con
            .import_from_iter("RUST.EXA_RUST_TEST", data, Some(http_opts))
            .unwrap();
    }
}
