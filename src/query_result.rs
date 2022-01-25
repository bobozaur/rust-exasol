use std::cell::RefCell;
use std::rc::Rc;
use std::vec::IntoIter;

use serde::Deserialize;
use serde_json::{json, Value};

use crate::connection::ConnectionImpl;
use crate::error::{Error, Result};

pub type Row = Vec<Value>;

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Deserialize)]
pub(crate) struct Results {
    #[serde(rename = "numResults")]
    num_results: u16,
    #[serde(rename = "results")]
    query_results: Vec<QueryResultImpl>,
}

impl Results {
    /// Consumes self, as it's useless after deserialization, to return a vector of QueryResults
    pub(crate) fn consume(self, con_rc: &Rc<RefCell<ConnectionImpl>>) -> Vec<QueryResult> {
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
                        is_closed: false,
                    };
                    QueryResult::ResultSet(r)
                }
                QueryResultImpl::RowCount { rowCount } => QueryResult::RowCount(rowCount),
            })
            .collect()
    }
}

#[test]
#[allow(unused)]
fn deserialize_results() {
    let result = json!({
       "numResults":1,
       "results":[
          {
     "resultSet":{
        "columns":[
           {
              "dataType":{
                 "precision":1,
                 "scale":0,
                 "type":"DECIMAL"
              },
              "name":"1"
           }
        ],
        "data":[
           [
              1
           ]
        ],
        "numColumns":1,
        "numRows":1,
        "numRowsInMessage":1
     },
     "resultType":"resultSet"
          }
       ]
    });
    let de: Results = serde_json::from_value(result).unwrap();
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
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType")]
pub(crate) enum QueryResultImpl {
    #[serde(rename = "resultSet")]
    ResultSet { resultSet: ResultSetImpl },
    #[serde(rename = "rowCount")]
    RowCount { rowCount: u32 },
}

#[test]
#[allow(unused)]
fn deser_query_result1() {
    let json_data = json!({
    "resultSet":{
       "columns":[
          {
             "dataType":{
                "precision":1,
                "scale":0,
                "type":"DECIMAL"
             },
             "name":"1"
          }
       ],
       "data":[
          [
             1
          ]
       ],
       "numColumns":1,
       "numRows":1,
       "numRowsInMessage":1
    },
    "resultType":"resultSet"
         });

    let de: QueryResultImpl = serde_json::from_value(json_data).unwrap();
}

#[test]
#[allow(unused)]
fn deser_query_result2() {
    let json_data = json!(
    {
        "resultType": "rowCount",
        "rowCount": 0
    });

    let de: QueryResultImpl = serde_json::from_value(json_data).unwrap();
}

/// Iterator struct retaining the result set of a given query.
///
/// insert example
///
#[allow(unused)]
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
    is_closed: bool,
}

impl ResultSet {
    /// Iterates over the iterator of iterators collecting the next() value of each of them
    /// and composing a row, which it then returns.
    /// If any iterator returns None, None gets returned.
    fn next_row(&mut self) -> Option<Result<Row>> {
        self.iter
            .iter_mut()
            .map(|iter| iter.next())
            .collect::<Option<Row>>()
            .and_then(|r| if r.is_empty() { None } else { Some(Ok(r)) })
    }

    /// Closes the result set if it wasn't already closed
    /// This method gets called when a `ResultSet` is fully iterated over
    /// but also when the `ResultSet` is dropped.
    fn close(&mut self) -> Result<()> {
        self.statement_handle.map_or(Ok(()), |h| {
            if !self.is_closed {
                self.is_closed = true;
                (*self.connection).borrow_mut().close_result_set(h)
            } else {
                Ok(())
            }
        })
    }

    /// Gets the next chunk of the result set from Exasol
    fn fetch_chunk(&mut self) -> Result<()> {
        // Check the statement handle
        self.statement_handle
            .ok_or(Error::InvalidResponse(
                "Cannot fetch rows chunk - missing statement handle".to_owned(),
            ))
            .and_then(|h| {
                // Compose the payload
                let payload = json!({
                    "command": "fetch",
                    "resultSetHandle": h,
                    "startPosition": self.total_rows_pos,
                    "numBytes": 5 * 1024 * 1024,
                });

                // Dereference connection and get data
                (*self.connection)
                    .borrow_mut()
                    .get_data::<FetchedData>(payload)
                    .and_then(|f| {
                        self.chunk_rows_num = f.chunk_rows_num;
                        self.chunk_rows_pos = 0;
                        self.iter = f.data.into_iter().map(|v| v.into_iter()).collect();
                        Ok(())
                    })
            })
    }
}

/// Making `ResultSet` an iterator
impl Iterator for ResultSet {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.next_row().or_else(|| {
            // All rows retrieved
            if self.total_rows_pos == self.total_rows_num {
                None
                // Whole chunk retrieved. Get new one.
            } else if self.chunk_rows_pos == self.chunk_rows_num {
                self.fetch_chunk()
                    .map_or_else(|e| Some(Err(e)), |_| self.next_row())
                // If all else fails
            } else {
                None
            }
        });

        // Updating positional counters
        self.total_rows_pos += 1;
        self.chunk_rows_pos += 1;

        // If row is None, all rows were iterated
        // so we're closing the result set, propagating the error, if any
        row.or_else(|| self.close().map_or_else(|e| Some(Err(e)), |_| None))
    }
}

impl Drop for ResultSet {
    fn drop(&mut self) {
        self.close().ok();
    }
}

/// Struct used for deserialization of a ResultSet
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
    data: Vec<Row>,
}

#[test]
#[allow(unused)]
fn deser_result_set() {
    let json_data = json!(
       {
       "columns":[
          {
             "dataType":{
                "precision":1,
                "scale":0,
                "type":"DECIMAL"
             },
             "name":"1"
          }
       ],
       "data":[
          [
             1
          ]
       ],
       "numColumns":1,
       "numRows":1,
       "numRowsInMessage":1
    });

    let de: ResultSetImpl = serde_json::from_value(json_data).unwrap();
}

/// Struct used for deserialization of fetched data
/// from getting a result set given a statement handle
#[derive(Deserialize)]
struct FetchedData {
    #[serde(rename = "numRows")]
    chunk_rows_num: usize,
    #[serde(default)]
    data: Vec<Row>,
}

#[test]
#[allow(unused)]
fn deser_fetched_data() {
    let json_data = json!(
        {
            "numRows": 30,
            "data": [[1, 2, 3], [4, 5, 6]]
        }
    );

    let de: FetchedData = serde_json::from_value(json_data).unwrap();
}

/// A struct containing the name and datatype (as seen in Exasol) of a given column
#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Column {
    name: String,
    #[serde(rename = "dataType")]
    datatype: Value,
}

#[test]
#[allow(unused)]
fn deser_column() {
    let json_data = json!(
    {
          "dataType":{
             "precision":1,
             "scale":0,
             "type":"DECIMAL"
          },
          "name":"1"
    }
    );

    let de: Column = serde_json::from_value(json_data).unwrap();
}

#[test]
#[allow(unused)]
fn deser_columns() {
    let json_data = json!([
    {
          "dataType":{
             "precision":1,
             "scale":0,
             "type":"DECIMAL"
          },
          "name":"1"
    }]
    );

    let de: Vec<Column> = serde_json::from_value(json_data).unwrap();
}
