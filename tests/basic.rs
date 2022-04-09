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
    use exasol::{connect, Column, PreparedStatement, QueryResult, ResultSet};

    #[test]
    #[allow(unused)]
    fn it_works() {
        let dsn = env::var("EXA_DSN").unwrap();
        let schema = env::var("EXA_SCHEMA").unwrap();
        let user = env::var("EXA_USER").unwrap();
        let password = env::var("EXA_PASSWORD").unwrap();

        let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
        let result = exa_con
            .execute("SELECT * FROM EXA_RUST_TEST LIMIT 2001;")
            .unwrap();

        let result_set = ResultSet::try_from(result)
            .unwrap()
            .with_row_type();
        for row in result_set {
            let record: (String, String, u16) = row.unwrap();
            println!("{:?}", row.unwrap());
        }

        use std::time::Instant;
        let now = Instant::now();

        let result = exa_con
            .execute("SELECT 1 as col1, 2 as col2, 3 as col3 UNION ALL SELECT 4 as col1, 5 as col2, 6 as col3;")
            .unwrap();

        #[derive(Debug, Deserialize)]
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
        }

        println!("{}", now.elapsed().as_millis());
    }
}
