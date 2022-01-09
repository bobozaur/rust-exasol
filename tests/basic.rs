//! Tests main crate functionalities, e.g: connecting, executing basic queries, etc.
#[cfg(test)]
mod tests {
    use rust_exasol::exasol::ExaConnection;
    use std::env;

    #[test]
    fn it_works() {
        let dsn = format!("ws://{}", std::env::var("EXA_DSN").unwrap());
        let schema = std::env::var("EXA_SCHEMA").unwrap();
        let user = std::env::var("EXA_USER").unwrap();
        let password = std::env::var("EXA_PASSWORD").unwrap();

        let mut exa_con = ExaConnection::new(&dsn, &schema, &user, &password).unwrap();
        println!("Connected!");

        let result = exa_con.execute("SELECT 1").unwrap();
        let result = exa_con.execute("SELECT 1 UNION ALL SELECT 2").unwrap();
    }
}