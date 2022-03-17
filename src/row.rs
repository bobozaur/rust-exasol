use crate::Column;
use serde::Deserialize;
use serde_json::{Map, Value};
use std::iter::zip;

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
pub trait TryIntoType: private::Sealed {
    fn into_type<T>(self) -> std::result::Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>;
}

impl TryIntoType for Row {
    fn into_type<T>(self) -> std::result::Result<T, serde_json::Error>
    where
        T: for<'de> Deserialize<'de>,
    {
        serde_json::from_value(Value::Array(self))
    }
}

impl TryIntoType for MapRow {
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
