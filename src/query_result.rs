use std::cell::{Ref, RefCell};
use std::cmp::Ordering;
use std::convert::Infallible;
use std::rc::Rc;
use std::vec::IntoIter;

use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::connection::ConnectionImpl;
use crate::connection::Result;
use crate::error::Error;

pub type Row = Vec<Value>;

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
/// ```
/// use serde_json::{json, Value};
/// use crate::query_result::Results;
///
/// fn deserialize_results() {
///     let result = json!({
///        "numResults":1,
///        "results":[
///           {
///      "resultSet":{
///         "columns":[
///            {
///               "dataType":{
///                  "precision":1,
///                  "scale":0,
///                  "type":"DECIMAL"
///               },
///               "name":"1"
///            }
///         ],
///         "data":[
///            [
///               1
///            ]
///         ],
///         "numColumns":1,
///         "numRows":1,
///         "numRowsInMessage":1
///      },
///      "resultType":"resultSet"
///           }
///        ]
///     });
///     let de: Results = serde_json::from_value(result).unwrap();
/// }
/// ```
#[derive(Deserialize)]
pub(crate) struct Results {
    #[serde(rename = "numResults")]
    num_results: u16,
    #[serde(rename = "results")]
    query_results: Vec<QueryResultImpl>,
}

impl Results {
    /// Consumes self, as it's useless after deserialization, to return a vector of QueryResults
    pub(crate) fn consume(mut self, con_rc: &Rc<RefCell<ConnectionImpl>>) -> Vec<QueryResult> {
        self.query_results
            .into_iter()
            .map(|q| match q {
                QueryResultImpl::ResultSet { resultSet } => {
                    let r = ResultSet {
                        num_columns: resultSet.num_columns,
                        total_rows_num: resultSet.total_rows_num,
                        total_rows_pos: 0,
                        chunk_rows_num: resultSet.chunk_rows_num,
                        chunk_rows_pos: 0,
                        statement_handle: resultSet.statement_handle,
                        columns: resultSet.columns,
                        iter: resultSet.data.into_iter().map(|v| v.into_iter()).collect(),
                        connection: Rc::clone(con_rc),
                    };
                    QueryResult::ResultSet(r)
                }
                QueryResultImpl::RowCount { rowCount } => QueryResult::RowCount(rowCount),
            })
            .collect()
    }
}

/// Enum containing the result of a query
/// `ResultSet` variant holds returned data
/// `RowCount` variant holds the affected row count
#[derive(Debug)]
pub enum QueryResult {
    ResultSet(ResultSet),
    RowCount(u32),
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
/// ```
/// use serde_json::{json, Value};
/// use super::QueryResultImpl;
///
/// fn deser_query_result1() {
///     let json_data = json!({
///     "resultSet":{
///        "columns":[
///           {
///              "dataType":{
///                 "precision":1,
///                 "scale":0,
///                 "type":"DECIMAL"
///              },
///              "name":"1"
///           }
///        ],
///        "data":[
///           [
///              1
///           ]
///        ],
///        "numColumns":1,
///        "numRows":1,
///        "numRowsInMessage":1
///     },
///     "resultType":"resultSet"
///          });
///
///     let de: QueryResultImpl = serde_json::from_value(json_data).unwrap();
/// }
/// ```
///
/// ```
/// use serde_json::{json, Value};
/// use super::QueryResultImpl;
///
/// fn deser_query_result2() {
///     let json_data = json!(
///     {
///         "resultType": "rowCount",
///         "rowCount": 0
///     });
///
///     let de: QueryResultImpl = serde_json::from_value(json_data).unwrap();
/// }
/// ```
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType")]
pub(crate) enum QueryResultImpl {
    #[serde(rename = "resultSet")]
    ResultSet { resultSet: ResultSetImpl },
    #[serde(rename = "rowCount")]
    RowCount { rowCount: u32 },
}

/// Iterator struct retaining the result set of a given query.
///
/// insert example
///
#[derive(Debug)]
pub struct ResultSet {
    num_columns: u8,
    total_rows_num: u32,
    total_rows_pos: u32,
    chunk_rows_num: usize,
    chunk_rows_pos: usize,
    statement_handle: Option<u16>,
    columns: Vec<Column>,
    iter: Vec<IntoIter<Value>>,
    connection: Rc<RefCell<ConnectionImpl>>,
}

impl ResultSet {
    /// Iterates over the iterator of iterators collecting the next() value of each of them
    /// and composing a row, which it then returns.
    /// If any iterator returns None, None gets returned.
    fn next_row(&mut self) -> Option<Row> {
        self.iter
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Option<Row>>()
            .and_then(|r| if r.is_empty() { None } else { Some(r) })
    }

    /// Gets the next chunk of the result set from Exasol
    fn fetch_chunk(&mut self) -> Option<Vec<Value>> {
        // If there's a statement handle
        self.statement_handle.and_then(|h| {
            // Compose the payload
            let payload = json!({
                "command": "fetch",
                "resultSetHandle": self.statement_handle,
                "startPosition": self.total_rows_pos,
                "numBytes": 5 * 1024 * 1024,
            });

            // Dereference connection and get data
            // Still pondering whether this should panic or not
            let fetched = (*self.connection)
                .borrow_mut()
                .get_data::<FetchedData>(payload)
                .ok();

            if let Some(f) = fetched {
                // Update iteration parameters and return next row
                self.chunk_rows_num = f.chunk_rows_num;
                self.chunk_rows_pos = 0;
                self.iter = f.data.into_iter().map(|v| v.into_iter()).collect();
                self.next_row()
            } else {
                None
            }
        })
    }
}

/// Making `ResultSet` an iterator
impl Iterator for ResultSet {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.next_row().or_else(|| {
            // All rows retrieved
            if self.total_rows_pos == self.total_rows_num {
                None
                // Whole chunk retrieved. Get new one.
            } else if self.chunk_rows_pos == self.chunk_rows_num {
                self.fetch_chunk()
                // If all else fails
            } else {
                None
            }
        });

        // Updating these indexes after retrieving the row
        // as fetching a new chunk also interferes with them
        self.total_rows_pos += 1;
        self.chunk_rows_pos += 1;

        row
    }
}

/// Struct used for deserialization of a ResultSet
/// ```
/// use serde_json::{json, Value};
/// use super::ResultSetImpl;
///
/// fn deser_result_set() {
///     let json_data = json!(
///        {
///        "columns":[
///           {
///              "dataType":{
///                 "precision":1,
///                 "scale":0,
///                 "type":"DECIMAL"
///              },
///              "name":"1"
///           }
///        ],
///        "data":[
///           [
///              1
///           ]
///        ],
///        "numColumns":1,
///        "numRows":1,
///        "numRowsInMessage":1
///     });
///
///     let de: ResultSetImpl = serde_json::from_value(json_data).unwrap();
/// }
///```
#[derive(Debug, Deserialize)]
pub(crate) struct ResultSetImpl {
    #[serde(rename = "numColumns")]
    num_columns: u8,
    #[serde(rename = "numRows")]
    total_rows_num: u32,
    #[serde(rename = "numRowsInMessage")]
    chunk_rows_num: usize,
    #[serde(rename = "resultSetHandle")]
    statement_handle: Option<u16>,
    columns: Vec<Column>,
    #[serde(default)]
    data: Vec<Vec<Value>>,
}

/// Struct used for deserialization of fetched data
/// from getting a result set given a statement handle
///
/// ```
/// use serde_json::{json, Value};
/// use super::FetchedData;
///
/// fn deser_fetched_data() {
///     let json_data = json!(
///         {
///             "numRows": 30,
///             "data": [[1, 2, 3], [4, 5, 6]]
///         }
///     );
///     
///     let de: FetchedData = serde_json::from_value(json_data).unwrap();
/// }
/// ```
#[derive(Deserialize)]
struct FetchedData {
    #[serde(rename = "numRows")]
    chunk_rows_num: usize,
    #[serde(default)]
    data: Vec<Row>,
}

/// A struct containing the name and datatype (as seen in Exasol) of a given column
/// ```
/// use serde_json::{json, Value};
/// use super::Column;
///
/// fn deser_column() {
///     let json_data = json!(
///     {
///           "dataType":{
///              "precision":1,
///              "scale":0,
///              "type":"DECIMAL"
///           },
///           "name":"1"
///     }
///     );
///
///     let de: Column = serde_json::from_value(json_data).unwrap();
/// }
/// ```
///
/// ```
/// use serde_json::{json, Value};
/// use super::Column;
///
/// fn deser_columns() {
///     let json_data = json!([
///     {
///           "dataType":{
///              "precision":1,
///              "scale":0,
///              "type":"DECIMAL"
///           },
///           "name":"1"
///     }]
///     );
///
///     let de: Vec<Column> = serde_json::from_value(json_data).unwrap();
/// }
/// ```
#[derive(Debug, Deserialize)]
pub struct Column {
    name: String,
    #[serde(rename = "dataType")]
    datatype: Value,
}
