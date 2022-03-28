use crate::error::{Error, RequestError, Result};
use crate::response::Column;
use serde::de::{DeserializeSeed, Error as SError, IntoDeserializer, MapAccess, Visitor};
use serde::{forward_to_deserialize_any, Deserialize, Deserializer, Serialize};
use serde_json::{Error as SJError, Value};
use std::borrow::{Borrow, Cow};
use std::fmt::Formatter;
use std::hash::Hash;
use std::marker::PhantomData;
use std::vec::IntoIter;

#[derive(Clone, Debug)]
pub struct Row<'a> {
    columns: &'a Vec<Column>,
    data: Vec<Value>,
}

impl<'a> Row<'a> {
    pub(crate) fn new(data: Vec<Value>, columns: &'a Vec<Column>) -> Self {
        Self { columns, data }
    }
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

    type Error = SJError;
    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: serde::de::Visitor<'de>,
    {
        visit_map(self, visitor)
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

    fn deserialize_seq<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
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
        visit_map(self, visitor)
    }

    fn deserialize_struct<V>(
        self,
        _name: &'static str,
        _fields: &'static [&'static str],
        visitor: V,
    ) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        visit_map(self, visitor)
    }

    fn deserialize_ignored_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
    where
        V: Visitor<'de>,
    {
        drop(self);
        visitor.visit_unit()
    }
}

struct RowMapDeserializer<'a> {
    columns: <&'a Vec<Column> as IntoIterator>::IntoIter,
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
    type Error = SJError;

    fn next_key_seed<T>(&mut self, seed: T) -> std::result::Result<Option<T::Value>, Self::Error>
    where
        T: DeserializeSeed<'de>,
    {
        match self.columns.next() {
            Some(key) => {
                let key_de = MapKeyDeserializer {
                    key: Cow::Borrowed(&key.name),
                };
                seed.deserialize(key_de).map(Some)
            }
            None => Ok(None),
        }
    }

    fn next_value_seed<T>(&mut self, seed: T) -> std::result::Result<T::Value, Self::Error>
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

fn visit_map<'de, V>(row: Row<'de>, visitor: V) -> std::result::Result<V::Value, SJError>
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

impl<'de> serde::Deserializer<'de> for MapKeyDeserializer<'de> {
    type Error = SJError;

    forward_to_deserialize_any! {
        bool i8 i16 i32 i64 u8 u16 u32 u64 f32 f64 char str string
        bytes byte_buf unit unit_struct option newtype_struct enum
        seq tuple tuple_struct map struct identifier ignored_any
    }

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
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
    type Error = SJError;

    fn deserialize_any<V>(self, visitor: V) -> std::result::Result<V::Value, Self::Error>
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

pub fn deserialize_as_seq<'de, D, F>(deserializer: D) -> std::result::Result<F, D::Error>
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

/// Function used to transpose data from an iterator or serializable types,
/// which would represent a row-major data record, to column-major data.
/// This data is then ready to be JSON serialized and sent to Exasol.
pub(crate) fn transpose_data<T, C, S>(columns: &[&C], data: T) -> Result<Vec<Vec<Value>>>
where
    S: Serialize,
    C: ?Sized + Hash + Ord,
    String: Borrow<C>,
    T: IntoIterator<Item = S>,
{
    let iter_data = data
        .into_iter()
        .map(|s| to_seq_iter(s, columns).map(IntoIterator::into_iter))
        .collect::<Result<Vec<IntoIter<Value>>>>()?;
    Ok(ColumnMajorIterator(iter_data).collect::<Vec<Vec<Value>>>())
}

#[test]
fn col_major_seq_data() {
    #[derive(Clone, Serialize)]
    struct SomeRow(String, String);

    let row = SomeRow("val1".to_owned(), "val2".to_owned());
    let row_major_data = vec![
        row.clone(),
        row.clone(),
        row.clone(),
        row.clone(),
        row.clone(),
    ];

    let columns = &["col1", "col2"];
    let col_major_data = transpose_data(columns, row_major_data);
    println!("{:?}", col_major_data);
}

#[test]
fn col_major_map_data() {
    #[derive(Clone, Serialize)]
    struct SomeRow {
        col1: String,
        col2: String,
    }

    let row = SomeRow {
        col1: "val1".to_owned(),
        col2: "val2".to_owned(),
    };

    let row_major_data = vec![
        row.clone(),
        row.clone(),
        row.clone(),
        row.clone(),
        row.clone(),
    ];

    let columns = &["col1", "col2"];
    let col_major_data = transpose_data(columns, row_major_data);
    println!("{:?}", col_major_data);
}

fn to_seq_iter<C, S>(data: S, columns: &[&C]) -> Result<Vec<Value>>
where
    S: Serialize,
    C: ?Sized + Hash + Ord,
    String: Borrow<C>,
{
    let val = serde_json::to_value(data)?;

    match val {
        Value::Object(mut o) => columns
            .iter()
            .map(|c| {
                o.remove(c)
                    .ok_or(Error::BindError("Missing something".to_owned()))
            })
            .collect(),
        Value::Array(a) => {
            if columns.len() > a.len() {
                Err(Error::RequestError(RequestError::InvalidResponse(
                    "Not enough columns in data!",
                )))
            } else {
                Ok(a.into_iter().take(columns.len()).collect())
            }
        }
        _ => Err(Error::RequestError(RequestError::InvalidResponse("test"))),
    }
}

struct ColumnMajorIterator(Vec<IntoIter<Value>>);

impl Iterator for ColumnMajorIterator {
    type Item = Vec<Value>;

    fn next(&mut self) -> Option<Self::Item> {
        self.0
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Option<Vec<Value>>>()
            .and_then(|r| if r.is_empty() { None } else { Some(r) })
    }
}
