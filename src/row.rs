use crate::error::{Error, Result};
use crate::response::Column;
use serde::de::{DeserializeSeed, Error as SError, IntoDeserializer, MapAccess, Visitor};
use serde::{forward_to_deserialize_any, Deserialize, Deserializer, Serialize};
use serde_json::{Error as SJError, Value};
use std::borrow::{Borrow, Cow};
use std::fmt::{Debug, Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;
use std::vec::IntoIter;

#[test]
#[allow(dead_code)]
fn deser_row(){
    use serde_json::json;
    use serde::Deserialize;
    use std::collections::HashMap;

    let json_data = json!([
		{
		   "dataType":{
		      "size": 10,
		      "type":"VARCHAR"
		   },
		   "name":"col1"
		},
        {
		   "dataType":{
		      "size": 10,
		      "type":"VARCHAR"
		   },
		   "name":"col2"
		}
    ]
    );

    let columns = serde_json::from_value(json_data).unwrap();

    let data = json!(["val1", "val2"]);
    let data1 = data.as_array().unwrap().clone();
    let data2 = data.as_array().unwrap().clone();
    let data3 = data.as_array().unwrap().clone();
    let data4 = data.as_array().unwrap().clone();
    let data5 = data.as_array().unwrap().clone();

    let row1 = Row::new(data1, &columns);
    let row2 = Row::new(data2, &columns);
    let row3 = Row::new(data3, &columns);
    let row4 = Row::new(data4, &columns);
    let row5 = Row::new(data5, &columns);

    #[derive(Deserialize)]
    struct SomeRow1(String, String);

    #[derive(Deserialize)]
    struct SomeRow2{
        col1: String,
        col2: String
    }

    #[derive(Deserialize)]
    struct SomeRow3 {
        #[serde(flatten)]
        map: HashMap<String, Value>
    }

    // Row variant can be chosen through internal tagging
    #[derive(Deserialize)]
    #[serde(tag = "col1", rename_all = "camelCase")]
    enum SomeRow4 {
        Val2 { col2: String },
        Val1 { col2: String }
    }

    // Untagged deserialization can also be employed
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum SomeRow5 {
        // This variant won't be chosen due to data type,
        // but struct variants are perfectly fine, they just need
        // the custom deserialization mechanism
        #[serde(deserialize_with = "deserialize_as_seq")]
        SomeVar1(u8, u8),
        // commenting the line below results in an error when trying to
        // deserialize this variant, so the third variant will be attempted
        #[serde(deserialize_with = "deserialize_as_seq")]
        SomeVar2(SomeRow1),
        // Struct variants are map-like, so they can be deserialized right away
        SomeVar3{col1: String, col2: String},
        // And so can variants of a struct type, but the one above has order precedence
        SomeVar4(SomeRow2)
    }

    SomeRow1::deserialize(row1).unwrap();
    SomeRow2::deserialize(row2).unwrap();
    SomeRow3::deserialize(row3).unwrap();
    SomeRow4::deserialize(row4).unwrap();
    SomeRow5::deserialize(row5).unwrap();
}

/// Struct representing a result set row.
/// This is only used internally to further deserialize it into a given Rust type.
#[derive(Debug)]
pub(crate) struct Row<'a> {
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

/// Implements custom deserialization for [Row] because we own the data but borrow the columns.
/// Furthermore, we want to be able to deserialize it to whatever the user wants,
/// be it a sequence or map like type.
///
/// It can thus be said that we implement a non-descriptive deserialization mechanism.
/// Most of the implementation is a trimmed down version of the protocol in `serde_json`.
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

#[test]
fn col_major_seq_data() {
    #[derive(Clone, Serialize)]
    struct SomeRow(String, String);

    let row = SomeRow("val1".to_owned(), "val2".to_owned());
    let row_major_data = vec![row.clone(), row.clone()];

    let columns = &["col1", "col2"];
    let col_major_data = to_col_major(columns, row_major_data).unwrap();
    assert_eq!(
        col_major_data,
        vec![
            vec!["val1".to_owned(), "val1".to_owned()],
            vec!["val2".to_owned(), "val2".to_owned()]
        ]
    )
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

    let row_major_data = vec![row.clone(), row.clone()];

    let columns = &["col2", "col1"];
    let col_major_data = to_col_major(columns, row_major_data).unwrap();
    assert_eq!(
        col_major_data,
        vec![
            vec!["val2".to_owned(), "val2".to_owned()],
            vec!["val1".to_owned(), "val1".to_owned()]
        ]
    )
}

/// Function used to transpose data from an iterator or serializable types,
/// which would represent a row-major data record, to column-major data.
/// This data is then ready to be JSON serialized and sent to Exasol.
///
/// Sequence-like data is processed as such, and the columns are merely used to assert length.
/// Map-like data though requires columns so the data is processed in the expected order.
pub(crate) fn to_col_major<T, C, S>(columns: &[&C], data: T) -> Result<Vec<Vec<Value>>>
where
    S: Serialize,
    C: ?Sized + Hash + Ord + Display,
    String: Borrow<C>,
    T: IntoIterator<Item = S>,
{
    let iter_data = data
        .into_iter()
        .map(|s| to_seq_iter(s, columns).map(IntoIterator::into_iter))
        .collect::<Result<Vec<IntoIter<Value>>>>()?;
    Ok(ColumnMajorIterator(iter_data).collect::<Vec<Vec<Value>>>())
}

fn to_seq_iter<C, S>(data: S, columns: &[&C]) -> Result<Vec<Value>>
where
    S: Serialize,
    C: ?Sized + Hash + Ord + Display,
    String: Borrow<C>,
{
    let val = serde_json::to_value(data)?;

    match val {
        Value::Object(mut o) => columns
            .iter()
            .map(|c| {
                o.remove(c)
                    .ok_or(Error::DataError(format!("Data is missing column {}", c)))
            })
            .collect(),
        Value::Array(a) => {
            let num_cols = columns.len();

            if num_cols > a.len() {
                Err(Error::DataError("Not enough columns in data!".to_owned()))
            } else {
                Ok(a.into_iter().take(num_cols).collect())
            }
        }
        _ => Err(Error::DataError(
            "Data iterator items must deserialize to sequences or maps".to_owned(),
        )),
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

/// Deserialization function that can be used through the `serde(deserialize_with = "...")`
/// attribute to aid in deserialization of enum variants as sequences instead of maps.
///
/// Due to the non-descriptive nature of a Row, which can be deserialized into
/// either sequences or maps, the default behaviour is to deserialize them as maps.
///
/// In the case of enum variants, the default behaviour is always employed. Changing
/// this has to be done by changing the deserialization mechanism itself, which
/// this function does.
///
/// ```
/// # use serde_json::json;
/// # use exasol::*;
/// # use exasol::error::Result;
///
/// # let json_data = json!(
///		{
///		   "columns":[
///		      {
///		         "dataType":{
///		            "size":10,
///		            "type":"VARCHAR"
///		         },
///		         "name":"col1"
///		      },
///		      {
///		         "dataType":{
///		            "size":10,
///		            "type":"VARCHAR"
///		         },
///		         "name":"col2"
///		      }
///		   ],
///		   "data":[
///		      [
///		         "val1", "val3"
///		      ],
///		      [
///		         "val2", "val4"
///		      ]
///		   ],
///		   "numColumns":2,
///		   "numRows":2,
///		   "numRowsInMessage":2
///		}
/// #   );
/// #
/// #   let result_set1: ResultSetDe = serde_json::from_value(json_data.clone()).unwrap();
/// #   let result_set2: ResultSetDe = serde_json::from_value(json_data.clone()).unwrap();
/// #   let result_set3: ResultSetDe = serde_json::from_value(json_data).unwrap();
///
///
///    // Default serde behaviour, which is external tagging, is not supported
///    // and it doesn't even make sense in the context of a database row.
///    #[derive(Deserialize)]
///    #[serde(rename_all = "camelCase")]
///    enum Val1 {
///        Val2(String)
///    }
///
///    #[derive(Deserialize)]
///    struct SomeRow1(String, String);
///
///    // Row variant can be chosen through internal tagging
///    #[derive(Deserialize)]
///    #[serde(tag = "col1", rename_all = "camelCase")]
///    enum SomeRow2 {
///        Val2 { col2: String },
///        Val1 { col2: String }
///    }
///
///    // Untagged deserialization can also be employed
///    #[derive(Deserialize)]
///    #[serde(untagged)]
///    enum SomeRow3 {
///        // This variant won't be chosen due to data type,
///        // but struct variants are perfectly fine, they just need
///        // the custom deserialization mechanism
///        #[serde(deserialize_with = "deserialize_as_seq")]
///        SomeVar1(u8, u8),
///        // commenting the line below results in an error when trying to
///        // deserialize this variant, so the third variant will be attempted
///        #[serde(deserialize_with = "deserialize_as_seq")]
///        SomeVar2(SomeRow1),
///        // Struct variants are map-like, so they can be deserialized right away
///        SomeVar3{col1: String, col2: String},
///        // And so can variants of a struct type, but the one above has order precedence
///        SomeVar4(SomeRow2)
///    }
///
/// // We get some results from the database
/// // and set the row types we want to deserialize them to.
/// result_set1 = result_set1.with_row_type::<Val1>();
/// result_set2 = result_set2.with_row_type::<SomeRow2>();
/// result_set3 = result_set2.with_row_type::<SomeRow3>();
///
/// // Due to the unsupported external tagging deserialization
/// // this will error out
/// let rows1: Result<Vec<Val1>> = result_set1.collect();
/// // But internal tagging works
/// let rows2: Result<Vec<SomeRow2>> = result_set2.collect().unwrap();
/// // And so does untagged deserialization
/// let rows3: Result<Vec<SomeRow3>> = result_set3.collect().unwrap();
/// ```
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