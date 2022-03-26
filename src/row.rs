use crate::error::{Error, RequestError, Result};
use crate::response::Column;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::borrow::Borrow;
use std::hash::Hash;
use std::iter::zip;
use std::vec::IntoIter;

pub type Row = Vec<Value>;
pub type MapRow = Map<String, Value>;

/// Marker trait for supported row formats.
pub trait RowType: private::Sealed {
    fn build(data: Vec<Value>, columns: &Vec<Column>) -> Self;
}

impl RowType for Row {
    fn build(data: Vec<Value>, _: &Vec<Column>) -> Self {
        data
    }
}
impl RowType for MapRow {
    fn build(data: Vec<Value>, columns: &Vec<Column>) -> Self {
        let cols = columns.iter().map(|c| c.name.clone());
        Map::from_iter(zip(cols, data))
    }
}

/// Trait for converting an Exasol row into a specific type.
pub trait TryRowToType: private::Sealed {
    fn into_type<T>(self) -> std::result::Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>;
}

impl TryRowToType for Row {
    fn into_type<T>(self) -> std::result::Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_value(Value::Array(self))
    }
}

impl TryRowToType for MapRow {
    fn into_type<T>(self) -> std::result::Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_value(Value::Object(self))
    }
}

/// Used to restrict downstream implementations of the row related traits
mod private {
    use crate::{MapRow, Row};

    pub trait Sealed {}

    impl Sealed for Row {}
    impl Sealed for MapRow {}
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
