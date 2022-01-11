//! Tests main crate functionalities, e.g: connecting, executing basic queries, etc.
#[cfg(test)]
mod tests {
    use rust_exasol::exasol::ExaConnection;
    use serde_json::json;
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
        println!("{:?}", result);
        let result = exa_con.execute("SELECT '1' UNION ALL SELECT '2'").unwrap();
        println!("{:?}", result);
        let result = exa_con.execute("SELECT * FROM EXA_ALL_TABLES LIMIT 2000;").unwrap();
        println!("{:?}", result);
        let result = exa_con.execute("DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2").unwrap();
        println!("{:?}", result);
        let results = exa_con.execute_batch(vec!("SELECT 3", "SELECT 4"));
        println!("{:?}", results);

        exa_con.set_attributes(json!({"autocommit": false})).unwrap();
    }
}