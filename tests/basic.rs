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
    use exasol::{MapRow, Row, TryRowToType};

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
            for row in r.with_row_type::<MapRow>() {
                let x: std::result::Result<Vec<u8>, serde_json::Error> = row.unwrap().into_type();
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

    // #[test]
    // fn deser() {
    //     let data = [
    //         3, // single byte for length
    //         0, 0, 1, // first 3 byte int
    //         0, 0, 2, 0, 0, 3,
    //     ];
    //
    //     let mut deserializer = Deserializer::from_bytes(&data);
    //     let res = Vec::<i32>::deserialize(&mut deserializer).unwrap();
    //     println!("{:?}", res);
    //
    //     use std::io::Read;
    //
    //     use serde::de::{Error as _, SeqAccess, Visitor};
    //
    //     #[derive(Debug)]
    //     struct Error(String);
    //
    //     impl std::fmt::Display for Error {
    //         fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
    //             f.write_str(&self.0)
    //         }
    //     }
    //     impl std::error::Error for Error {}
    //     impl serde::de::Error for Error {
    //         fn custom<T>(msg: T) -> Self
    //         where
    //             T: std::fmt::Display,
    //         {
    //             Error(msg.to_string())
    //         }
    //     }
    //
    //     struct OurSeqAccess<'a, 'de> {
    //         inner: &'a mut Deserializer<'de>,
    //         remaining_length: usize,
    //     }
    //
    //     impl<'a, 'de> SeqAccess<'de> for OurSeqAccess<'a, 'de> {
    //         type Error = Error;
    //
    //         fn next_element_seed<T>(
    //             &mut self,
    //             seed: T,
    //         ) -> std::result::Result<Option<T::Value>, Self::Error>
    //         where
    //             T: serde::de::DeserializeSeed<'de>,
    //         {
    //             println!("In next_element_seed");
    //             if self.remaining_length > 0 {
    //                 self.remaining_length -= 1;
    //                 let el = seed.deserialize(&mut *self.inner)?;
    //                 Ok(Some(el))
    //             } else {
    //                 Ok(None)
    //             }
    //         }
    //     }
    //
    //     struct Deserializer<'de> {
    //         input: &'de [u8],
    //     }
    //
    //     impl<'de> Deserializer<'de> {
    //         pub fn from_bytes(input: &'de [u8]) -> Self {
    //             println!("In from_bytes");
    //             Self { input }
    //         }
    //     }
    //
    //     impl<'de, 'a> serde::de::Deserializer<'de> for &'a mut Deserializer<'de> {
    //         serde::forward_to_deserialize_any! {
    //             bool i8 i16 i64 u8 u16 u32 u64 f32 f64 char str string
    //             byte_buf option unit unit_struct newtype_struct tuple tuple_struct
    //             map struct enum identifier ignored_any bytes
    //         }
    //
    //         type Error = Error;
    //
    //         fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    //         where
    //             V: serde::de::Visitor<'de>,
    //         {
    //             println!("In deserialize_any");
    //             use serde::de::Error;
    //             Err(Self::Error::custom("expected sequence"))
    //         }
    //
    //         fn deserialize_i32<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    //         where
    //             V: Visitor<'de>,
    //         {
    //             println!("In deserialize_i32");
    //             let mut buf = [0; 3];
    //
    //             match self.input.read_exact(&mut buf) {
    //                 Ok(_) => {
    //                     // convert buffer to integer.
    //                     let mut i = (buf[0] as i32) << 16;
    //                     i += (buf[1] as i32) << 8;
    //                     i += buf[2] as i32;
    //
    //                     visitor.visit_i32(i)
    //                 }
    //                 Err(_) => Err(Error::custom("could not read next integer")),
    //             }
    //         }
    //
    //         fn deserialize_seq<V>(
    //             mut self,
    //             visitor: V,
    //         ) -> std::result::Result<V::Value, Self::Error>
    //         where
    //             V: Visitor<'de>,
    //         {
    //             println!("In deserialize_seq");
    //             let mut length = [0u8; 1];
    //             self.input
    //                 .read_exact(&mut length)
    //                 .map_err(|_| Error::custom("read error"))?;
    //
    //             let length = length[0] as usize;
    //
    //             visitor.visit_seq(OurSeqAccess {
    //                 inner: &mut self,
    //                 remaining_length: length,
    //             })
    //         }
    //     }
    // }

    #[test]
    fn row() {
        #[derive(Clone)]
        struct Row<'a> {
            columns: &'a Vec<String>,
            data: Vec<Value>,
        }

        impl<'a> From<Row<'a>> for Value {
            fn from(r: Row<'a>) -> Self {
                Value::Array(r.data)
            }
        }

        impl<'de> serde::de::Deserializer<'de> for Row<'de> {
            serde::forward_to_deserialize_any! {
                bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
                bytes byte_buf unit unit_struct enum option identifier
            }

            type Error = Error;
            fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
            where
                V: serde::de::Visitor<'de>,
            {
                visit_object(self, visitor)
            }

            fn deserialize_newtype_struct<V>(
                self,
                name: &'static str,
                visitor: V,
            ) -> std::result::Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                Value::from(self).deserialize_tuple_struct(name, 1, visitor)
            }

            fn deserialize_seq<V>(
                mut self,
                visitor: V,
            ) -> std::result::Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                Value::from(self).deserialize_seq(visitor)
            }

            fn deserialize_tuple<V>(
                self,
                len: usize,
                visitor: V,
            ) -> std::result::Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                Value::from(self).deserialize_tuple(len, visitor)
            }

            fn deserialize_tuple_struct<V>(
                self,
                name: &'static str,
                len: usize,
                visitor: V,
            ) -> std::result::Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                Value::from(self).deserialize_tuple_struct(name, len, visitor)
            }

            fn deserialize_map<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                visit_object(self, visitor)
            }

            fn deserialize_struct<V>(
                self,
                name: &'static str,
                fields: &'static [&'static str],
                visitor: V,
            ) -> std::result::Result<V::Value, Self::Error>
            where
                V: Visitor<'de>,
            {
                visit_object(self, visitor)
            }

            fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Error>
            where
                V: Visitor<'de>,
            {
                drop(self);
                visitor.visit_unit()
            }
        }

        struct RowMapDeserializer<'a> {
            columns: <&'a Vec<String> as IntoIterator>::IntoIter,
            data: <Vec<Value> as IntoIterator>::IntoIter,
        }

        impl<'a> RowMapDeserializer<'a> {
            fn new(row: Row<'a>) -> Self {
                Self {
                    columns: row.columns.iter(),
                    data: row.data.into_iter(),
                }
            }
        }

        impl<'de> MapAccess<'de> for RowMapDeserializer<'de> {
            type Error = Error;

            fn next_key_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Error>
            where
                T: DeserializeSeed<'de>,
            {
                match self.columns.next() {
                    Some(key) => {
                        let key_de = MapKeyDeserializer {
                            key: Cow::Borrowed(&**key),
                        };
                        seed.deserialize(key_de).map(Some)
                    }
                    None => Ok(None),
                }
            }

            fn next_value_seed<T>(&mut self, seed: T) -> std::result::Result<T::Value, Error>
            where
                T: DeserializeSeed<'de>,
            {
                match self.data.next() {
                    Some(value) => seed.deserialize(value),
                    None => Err(serde::de::Error::custom("value is missing")),
                }
            }

            fn size_hint(&self) -> Option<usize> {
                match self.columns.size_hint() {
                    (lower, Some(upper)) if lower == upper => Some(upper),
                    _ => None,
                }
            }
        }

        fn visit_object<'de, V>(row: Row<'de>, visitor: V) -> std::result::Result<V::Value, Error>
        where
            V: Visitor<'de>,
        {
            let len = row.columns.len();
            let mut deserializer = RowMapDeserializer::new(row);
            let map = visitor.visit_map(&mut deserializer)?;
            let remaining = deserializer.columns.len();
            if remaining == 0 {
                Ok(map)
            } else {
                Err(serde::de::Error::invalid_length(
                    len,
                    &"fewer elements in map",
                ))
            }
        }

        struct MapKeyDeserializer<'de> {
            key: Cow<'de, str>,
        }

        macro_rules! deserialize_integer_key {
            ($method:ident => $visit:ident) => {
                fn $method<V>(self, visitor: V) -> std::result::Result<V::Value, Error>
                where
                    V: Visitor<'de>,
                {
                    match (self.key.parse(), self.key) {
                        (Ok(integer), _) => visitor.$visit(integer),
                        (Err(_), Cow::Borrowed(s)) => visitor.visit_borrowed_str(s),
                        (Err(_), Cow::Owned(s)) => visitor.visit_string(s),
                    }
                }
            };
        }

        impl<'de> serde::Deserializer<'de> for MapKeyDeserializer<'de> {
            type Error = Error;

            forward_to_deserialize_any! {
                bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
                bytes byte_buf unit unit_struct option newtype_struct enum
                seq tuple tuple_struct map struct identifier ignored_any
            }

            fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Error>
            where
                V: Visitor<'de>,
            {
                BorrowedCowStrDeserializer::new(self.key).deserialize_any(visitor)
            }
        }

        struct BorrowedCowStrDeserializer<'de> {
            value: Cow<'de, str>,
        }

        impl<'de> BorrowedCowStrDeserializer<'de> {
            fn new(value: Cow<'de, str>) -> Self {
                BorrowedCowStrDeserializer { value }
            }
        }

        impl<'de> serde::de::Deserializer<'de> for BorrowedCowStrDeserializer<'de> {
            type Error = Error;

            fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Error>
            where
                V: serde::de::Visitor<'de>,
            {
                match self.value {
                    Cow::Borrowed(string) => visitor.visit_borrowed_str(string),
                    Cow::Owned(string) => visitor.visit_string(string),
                }
            }

            forward_to_deserialize_any! {
                bool i8 i16 i32 i64 i128 u8 u16 u32 u64 u128 f32 f64 char str string
                bytes byte_buf option unit unit_struct newtype_struct seq tuple
                tuple_struct map struct enum identifier ignored_any
            }
        }

        fn from_row<T>(row: Row) -> std::result::Result<T, Error>
        where
            T: DeserializeOwned,
        {
            T::deserialize(row)
        }

        fn deser<'de, D, F>(deserializer: D) -> std::result::Result<F, D::Error>
        where
            D: Deserializer<'de>,
            F: Deserialize<'de>,
        {
            struct TupleVariantVisitor<F>(PhantomData<F>);

            impl<'de, F> Visitor<'de> for TupleVariantVisitor<F>
            where
                F: Deserialize<'de>,
            {
                type Value = F;

                fn expecting(&self, formatter: &mut Formatter) -> std::fmt::Result {
                    write!(formatter, "A map")
                }

                fn visit_map<A>(
                    self,
                    mut map: A,
                ) -> std::result::Result<Self::Value, <A as MapAccess<'de>>::Error>
                where
                    A: MapAccess<'de>,
                {
                    let mut seq = vec![];
                    while let Ok(opt) = map.next_entry() {
                        if let Some(e) = opt {
                            let _: String = e.0;
                            seq.push(e.1)
                        } else {
                            break;
                        }
                    }
                    let val = Value::Array(seq);
                    Self::Value::deserialize(val.into_deserializer())
                        .map_err(|_| A::Error::custom("blabla"))
                }
            }
            deserializer.deserialize_map(TupleVariantVisitor(PhantomData))
        }

        use serde_json::Serializer;



        let columns = vec!["col1".to_owned(), "col2".to_owned()];
        let data = vec![
            Value::String("val1".to_owned()),
            Value::String("val2".to_owned()),
        ];

        let col2 = vec!["col1".to_owned()];
        let data2 = vec![Value::String("val1".to_owned())];

        let row10 = Row {
            columns: &col2,
            data: data2,
        };

        let row = Row {
            columns: &columns,
            data,
        };

        let row2 = row.clone();
        let row3 = row.clone();
        let row4 = row.clone();
        let row5 = row.clone();
        let row6 = row.clone();
        let row7 = row.clone();

        #[derive(Debug, Clone, Deserialize, Serialize)]
        struct A(String, String);

        #[derive(Debug, Deserialize, Serialize)]
        struct B {
            col1: String,
            col2: String,
        }

        #[derive(Debug, Deserialize, Serialize)]
        struct H {
            #[serde(flatten)]
            map: HashMap<String, Value>,
        }

        #[derive(Debug, Deserialize, Serialize)]
        #[serde(tag = "col1")]
        enum C {
            #[serde(rename = "val1")]
            Val1 { col2: String },
        }

        #[derive(Debug, Deserialize, Serialize)]
        #[serde(untagged)]
        enum D {
            #[serde(deserialize_with = "deser")]
            F(A),
            // #[serde(deserialize_with = "deser")]
            // A(Vec<String>),
            // B(B),
            // C { col1: String, col2: String },
        }

        #[derive(Debug, Deserialize, Serialize)]
        #[serde(tag = "col1", rename_all = "lowercase")]
        enum J {
            Val1,
            Val2,
        }

        let a: A = from_row(row).unwrap();
        let b: B = from_row(row2).unwrap();
        let c: C = from_row(row3).unwrap();
        let d: D = from_row(row4).unwrap();
        let e: (String, String) = from_row(row5).unwrap();
        let f: Vec<String> = from_row(row6).unwrap();
        let h: H = from_row(row7).unwrap();
        let j: J = from_row(row10).unwrap();

        println!(
            "{:?} {:?} {:?} {:?}, {:?}, {:?}, {:?}, {:?}",
            a, b, c, d, e, f, h, j
        );

        println!("{}", serde_json::to_string(&a).unwrap());
        println!("{}", serde_json::to_string(&b).unwrap());
        println!("{}", serde_json::to_string(&c).unwrap());
        println!("{}", serde_json::to_string(&d).unwrap());
        println!("{}", serde_json::to_string(&j).unwrap());
    }
}
