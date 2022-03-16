//! Tests main crate functionalities, e.g: connecting, executing basic queries, etc.
#[cfg(test)]
#[allow(unused)]
mod tests {
    use std::collections::HashMap;
    use std::env;

    use serde_json::json;

    use exasol::error::Result;
    use exasol::{connect, PreparedStatement, QueryResult, Row};

    #[test]
    #[allow(unused)]
    fn it_works() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();

        // let prepared = exa_con
        //     .prepare("Select 1 FROM (select 1) TMP WHERE 1 = ?;")
        //     .unwrap();
        // let result = prepared.execute(vec![vec![json!(1)]]).unwrap();
        //
        // println!("{:?}", result);
        //
        // if let QueryResult::ResultSet(r) = result {
        //     let x = r.take(50).collect::<Result<Vec<Row>>>();
        //     if let Ok(v) = x {
        //         for row in v.iter() {println!("{:?}", row)}
        //     }
        // }

        // let result = exa_con.execute("SELECT 1").unwrap();
        //
        // let result = exa_con
        //     .execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'")
        //     .unwrap();
        //
        use std::time::Instant;
        let now = Instant::now();

        let result = exa_con
            .execute("SELECT * FROM DIM_SIMPLE_DATE WHERE CALENDARYEAR = 2021;")
            .unwrap();

        if let QueryResult::ResultSet(r) = result {
            let x = r.take(50).collect::<Result<Vec<Row>>>();
            if let Ok(v) = x {
                for row in v.iter() {
                    println!("{:?}", row)
                }
            }
        }

        println!("{}", now.elapsed().as_millis());
        //
        // let query = "DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2".to_owned();
        // let result = exa_con.execute(&query).unwrap();
        //
        // // let result = exa_con.execute(query).unwrap();
        // let queries = vec!["SELECT 3".to_owned(), "SELECT 4".to_owned()];
        // let results = exa_con
        //     .execute_batch(&queries)
        //     .unwrap();
        //
        // let results = exa_con
        //     .execute_batch(vec!["SELECT 3".to_owned(), "DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2".to_owned()])
        //     .unwrap();
    }
}
