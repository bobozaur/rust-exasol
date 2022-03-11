use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::rc::Rc;
use std::vec::IntoIter;

use crate::con_opts::ProtocolVersion;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::connection::ConnectionImpl;
use crate::error::{ExaError, RequestError, Result};

pub type Row = Vec<Value>;

/// Generic response received from the Exasol server
/// This is the first deserialization step
/// Used to determine whether the message
/// is a proper response, or an error
///
/// We're forced to use internal tagging as
/// ok/error responses have different adjacent fields
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "status")]
#[serde(rename_all = "snake_case")]
pub(crate) enum Response {
    #[serde(rename_all = "snake_case")]
    Ok {
        response_data: ResponseData,
        attributes: Option<Attributes>,
    },
    Error {
        exception: ExaError,
    },
}

/// This is the `responseData` field of the JSON response.
/// Because all `ok` responses are returned through this
/// with no specific identifier between them
/// we have to use untagged deserialization.
///
/// We'll set the most common one, the results,
/// as the first one, to speed up most deserializations.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ResponseData {
    Results(Results),
    Attributes(Attributes),
    Info(DatabaseInfo),
    PublicKey(PublicKey),
}

/// Generic struct containing the response fields
/// returned by Exasol in case of an error.
#[derive(Debug, Deserialize, Serialize)]
pub struct ExaError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String,
}

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Results {
    num_results: u16,
    results: Vec<QueryResultDe>,
}

impl Results {
    /// Consumes self, as it's useless after deserialization, to return a vector of QueryResults,
    /// each with a reference to a connection.
    ///
    /// The reference is needed for further row fetching.
    pub(crate) fn to_query_results(self, con_rc: &Rc<RefCell<ConnectionImpl>>) -> Vec<QueryResult> {
        self.results
            .into_iter()
            .map(|q| match q {
                QueryResultDe::ResultSet { result_set } => {
                    let r = ResultSet {
                        num_columns: result_set.num_columns,
                        total_rows_num: result_set.total_rows_num,
                        total_rows_pos: 0,
                        chunk_rows_num: result_set.chunk_rows_num,
                        chunk_rows_pos: 0,
                        statement_handle: result_set.statement_handle,
                        columns: result_set.columns,
                        iter: result_set.data.into_iter().map(|v| v.into_iter()).collect(),
                        connection: Rc::clone(con_rc),
                        is_closed: false,
                    };

                    QueryResult::ResultSet(r)
                }

                QueryResultDe::RowCount { row_count } => QueryResult::RowCount(rowCount),
            })
            .collect()
    }
}

/// Struct representing attributes returned from Exasol.
/// These can either be returned by an explicit `getAttributes` call
/// or as part of any response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct Attributes {
    #[serde(flatten)]
    map: HashMap<String, Value>,
}

/// Struct representing database information returned
/// after establishing a connection.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct DatabaseInfo {
    #[serde(flatten)]
    map: HashMap<String, Value>,
}

/// Struct representing public key information
/// returned as part of the login process.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) struct PublicKey {
    public_key_exponent: String,
    public_key_modulus: String,
    public_key_pem: String,
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "snake_case")]
enum QueryResultDe {
    ResultSet { result_set: ResultSetDe },
    RowCount { row_count: u32 },
}

/// Struct used for deserialization of a ResultSet
#[derive(Debug, Deserialize)]
struct ResultSetDe {
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

/// Struct containing the name and datatype (as seen in Exasol) of a given column.
#[allow(unused)]
#[derive(Debug, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "dataType")]
    pub datatype: Value,
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.datatype)
    }
}

/// Struct representing a datatype for a column in a result set.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct DataType {
    #[serde(rename = "type")]
    type_name: String,
    precision: Option<u8>,
    scale: Option<u8>,
    size: Option<usize>,
    character_set: Option<String>,
    with_local_time_zone: Option<bool>,
    fraction: Option<usize>,
    srid: Option<usize>,
}

/// Enum containing the result of a query
/// `ResultSet` variant holds a [ResultSet]
/// `RowCount` variant holds the affected row count
pub enum QueryResult {
    ResultSet(ResultSet),
    RowCount(u32),
}

impl Debug for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ResultSet(r) => write!(f, "{:?}", r),
            Self::RowCount(c) => write!(f, "Row count: {}", c),
        }
    }
}

/// Iterator struct over the result set of a given query.
/// The maximum data rows that are initially retrieved are 1000.
/// Further rows get fetched as needed
///
/// ```
/// # use exasol::{connect, Row, QueryResult};
/// # use exasol::error::Result;
/// # use std::env;
///
/// # let dsn = env::var("EXA_DSN").unwrap();
/// # let schema = env::var("EXA_SCHEMA").unwrap();
/// # let user = env::var("EXA_USER").unwrap();
/// # let password = env::var("EXA_PASSWORD").unwrap();
/// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
/// let result = exa_con.execute("SELECT * FROM EXA_ALL_OBJECTS LIMIT 2000;").unwrap();
///
/// if let QueryResult::ResultSet(r) = result {
///     let x = r.take(50).collect::<Result<Vec<Row>>>();
///         if let Ok(v) = x {
///             for row in v.iter() {
///                 // do stuff
///             }
///         }
///     }
/// ```
///
/// The iterator is lazy, and it will retrieve rows in chunks from the database
/// based on the `fetch_size` parameter set in the [ConOpts](crate::ConOpts) used when constructing
/// the [Connection](crate::Connection), until the result set is entirely read.
#[allow(unused)]
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

impl Debug for ResultSet {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Closed: {}\nHandle: {:?}\nColumns: {:?}\nRows: {}",
            self.is_closed, self.statement_handle, self.columns, self.total_rows_num
        )
    }
}

impl ResultSet {
    /// Returns a reference to a [Vec<Column>] of the result set columns
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    /// Returns the number of columns in the result set
    pub fn num_columns(&self) -> &u8 {
        &self.num_columns
    }

    /// Returns the number of rows in the result set
    pub fn num_rows(&self) -> &u32 {
        &self.total_rows_num
    }

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
    /// This method gets called when a [ResultSet] is fully iterated over
    /// but also when the [ResultSet] is dropped.
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
            .ok_or(RequestError::MissingHandleError.into())
            .and_then(|h| {
                // Dereference connection
                let mut con = (*self.connection).borrow_mut();

                // Safe to unwrap as this will always be present due to ConOpts
                let fetch_size = con.attr.get("fetch_size").unwrap();

                // Compose the payload
                let payload = json!({
                    "command": "fetch",
                    "resultSetHandle": h,
                    "startPosition": self.total_rows_pos,
                    "numBytes": fetch_size,
                });

                con.get_data::<FetchedData>(payload).and_then(|f| {
                    self.chunk_rows_num = f.chunk_rows_num;
                    self.chunk_rows_pos = 0;
                    self.iter = f.data.into_iter().map(|v| v.into_iter()).collect();
                    Ok(())
                })
            })
    }
}

/// Making [ResultSet] an iterator
impl Iterator for ResultSet {
    type Item = Result<Row>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = self.next_row().or_else(|| {
            // All rows retrieved
            if self.total_rows_pos >= self.total_rows_num {
                None
                // Whole chunk retrieved. Get new one.
            } else if self.chunk_rows_pos >= self.chunk_rows_num {
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

    let de: QueryResultDe = serde_json::from_value(json_data).unwrap();
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

#[test]
#[allow(unused)]
fn deser_query_result2() {
    let json_data = json!(
    {
        "resultType": "rowCount",
        "rowCount": 0
    });

    let de: QueryResultDe = serde_json::from_value(json_data).unwrap();
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

    let de: ResultSetDe = serde_json::from_value(json_data).unwrap();
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
