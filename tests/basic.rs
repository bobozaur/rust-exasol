//! Tests main crate functionalities, e.g: connecting, executing basic queries, etc.
#[cfg(test)]
#[allow(unused)]
mod tests {
    use serde::de::{
        DeserializeOwned, DeserializeSeed, EnumAccess, Error as SError, IntoDeserializer,
        MapAccess, SeqAccess, Unexpected, VariantAccess, Visitor,
    };
    use std::borrow::{Borrow, BorrowMut, Cow};
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::fmt::{Debug, Formatter};
    use std::iter::zip;
    use std::marker::PhantomData;
    use std::ops::Deref;
    use std::rc::Rc;
    use std::vec::IntoIter;
    use std::{env, vec};

    use serde::__private::de::Content;
    use serde::{
        forward_to_deserialize_any, serde_if_integer128, Deserialize, Deserializer, Serialize,
        Serializer,
    };
    use serde_json::ser::{CompactFormatter, Compound};
    use serde_json::{de, from_value, json, Error, Map, Value};

    use exasol::error::Result;
    use exasol::{connect, Column, PreparedStatement, QueryResult};

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

        let result = exa_con.execute("SELECT 1").unwrap();

        let result = exa_con.execute("SELECT * FROM EXA_RUST_TEST;").unwrap();

        if let QueryResult::ResultSet(mut r) = result {
            let x = r.next();
            println!("{:?}", x);
        }

        use std::time::Instant;
        let now = Instant::now();

        let result = exa_con
            .execute("SELECT 1 as col1, 2 as col2, 3 as col3 UNION ALL SELECT 4 as col1, 5 as col2, 6 as col3;")
            .unwrap();

        #[derive(Debug, Deserialize)]
        #[serde(rename_all = "UPPERCASE")]
        struct Test {
            col1: u8,
            col2: u8,
            col3: u8,
        }

        if let QueryResult::ResultSet(r) = result {
            for row in r.with_row_type::<Test>() {
                let x = row.unwrap();
                println!("{:?}", x);
            }
            // let x = r.take(50).collect::<Result<Vec<Vec<Value>>>>();
            // if let Ok(v) = x {
            //     for row in v.iter() {
            //         println!("{:?}", row)
            //     }
            // }
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
