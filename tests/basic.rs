//! Tests main crate functionalities, e.g: connecting, executing basic queries, etc.
#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::env;
    use std::option::IntoIter;
    use std::rc::Rc;

    use serde_json::json;

    use exasol::exasol::{Connection, QueryResult, ResultSet, Row};

    #[test]
    fn it_works() {
        let dsn = format!("ws://{}", std::env::var("EXA_DSN").unwrap());
        let schema = std::env::var("EXA_SCHEMA").unwrap();
        let user = std::env::var("EXA_USER").unwrap();
        let password = std::env::var("EXA_PASSWORD").unwrap();

        let mut exa_con = Connection::connect(&dsn, &schema, &user, &password).unwrap();
        println!("Connected!");

        let result = exa_con.execute("SELECT 1").unwrap();
        // println!("{:?}", result);
        let result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
        // println!("{:?}", result);
        let result = exa_con.execute("SELECT * FROM DIM_SIMPLE_DATE LIMIT 100000;").unwrap();
        // println!("{:?}", result);
        if let QueryResult::ResultSet(mut r) = result {
            for row in r {
                // println!("{:?}", row);
            }
        }
        let result = exa_con.execute("DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2").unwrap();
        // println!("{:?}", result);
        let results = exa_con.execute_batch(vec!("SELECT 3", "SELECT 4"));
        // println!("{:?}", results);

        let results = exa_con.execute_batch(vec!("SELECT 3", "DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2"));
        // println!("{:?}", results);

        exa_con.set_attributes(json!({"autocommit": false})).unwrap();
    }
}