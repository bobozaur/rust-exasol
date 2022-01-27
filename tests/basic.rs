//! Tests main crate functionalities, e.g: connecting, executing basic queries, etc.
#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::collections::HashMap;
    use std::env;

    use serde_json::json;

    use exasol::exasol::{Connection, QueryResult, Row, Result};

    #[test]
    #[allow(unused)]
    fn it_works() {
        let dsn = format!("{}", env::var("EXA_DSN").unwrap());
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut exa_con = Connection::new(&dsn, &schema, &user, &password).unwrap();

        let result = exa_con.execute("SELECT 1").unwrap();
        // println!("{:?}", result);
        let result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
        // println!("{:?}", result);
        // let result = exa_con.execute("SELECT * FROM DIM_SIMPLE_DATE WHERE CALENDARYEAR = 2022;").unwrap();
        // // println!("{:?}", result);
        // if let QueryResult::ResultSet(r) = result {
        //     let x = r.take(5000).collect::<Result<Vec<Row>>>();
        //     if let Ok(v) = x {
        //         for row in v.iter() {
        //             println!("{:?}", row);
        //         }
        //     }
        // }

        let result = exa_con.execute("DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2").unwrap();
        // println!("{:?}", result);
        let results = exa_con.execute_batch(vec!("SELECT 3", "SELECT 4"));
        // println!("{:?}", results);

        let results = exa_con.execute_batch(vec!("SELECT 3", "DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2"));
        // println!("{:?}", results);
    }
}