use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Display, Formatter};
use std::rc::Rc;

use crate::con_opts::ProtocolVersion;
use crate::constants::{MISSING_DATA, NO_RESPONSE_DATA};
use crate::error::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::connection::ConnectionImpl;
use crate::query_result::QueryResult;
use crate::PreparedStatement;

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
#[serde(tag = "status", rename_all = "camelCase")]
pub(crate) enum Response {
    #[serde(rename_all = "camelCase")]
    Ok {
        response_data: Option<ResponseData>,
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
/// As a result, the order of the enum variants matters,
/// as deserialization has to be non-overlapping yet exhaustive.
#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub(crate) enum ResponseData {
    PreparedStatement(PreparedStatementDe),
    Results(Results),
    FetchedData(FetchedData),
    PublicKey(PublicKey),
    LoginInfo(LoginInfo),
    Attributes(Attributes),
}

impl ResponseData {
    /// Attempts to convert this struct to a [Vec<QueryResult>].
    pub(crate) fn try_to_query_results(
        self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
    ) -> Result<Vec<QueryResult>> {
        match self {
            Self::Results(res) => Ok(res.to_query_results(con_impl)),
            _ => Err(NO_RESPONSE_DATA.into()),
        }
    }

    /// Attempts to convert this struct to a [PreparedStatement].
    pub(crate) fn try_to_prepared_stmt(
        self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
    ) -> Result<PreparedStatement> {
        match self {
            Self::PreparedStatement(res) => Ok(PreparedStatement::from_de(res, con_impl)),
            _ => Err(NO_RESPONSE_DATA.into()),
        }
    }

    /// Attempts to convert this struct to a [FetchedData].
    pub(crate) fn try_to_fetched_data(self) -> Result<FetchedData> {
        match self {
            Self::FetchedData(d) => Ok(d),
            _ => Err(MISSING_DATA.into()),
        }
    }

    /// Returns [Option<String>] with `Some(Key)` if conversion works
    /// or `None` if it does not.
    pub(crate) fn to_public_key(self) -> Option<String> {
        match self {
            Self::PublicKey(p) => Some(p.into_string_key()),
            _ => None,
        }
    }
}

/// Generic struct containing the response fields
/// returned by Exasol in case of an error.
#[derive(Debug, Deserialize, Serialize)]
pub struct ExaError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String,
}

impl Display for ExaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl std::error::Error for ExaError {}

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

/// Struct used for deserialization of the JSON
/// returned after executing one or more queries
/// Represents the collection of results from all queries.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Results {
    num_results: u8,
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
            .map(|q| QueryResult::from_de(q, con_rc))
            .collect()
    }
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

/// Struct used for deserialization of fetched data
/// from getting a result set given a statement handle
#[derive(Debug, Deserialize)]
pub(crate) struct FetchedData {
    #[serde(rename = "numRows")]
    pub(crate) chunk_rows_num: usize,
    #[serde(default)]
    pub(crate) data: Vec<Row>,
}

#[test]
#[allow(unused)]
fn deserialize_prepared() {
    let result = json!({
        "statementHandle": 1,
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
    let de: PreparedStatementDe = serde_json::from_value(result).unwrap();
}

/// Struct representing a prepared statement
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PreparedStatementDe {
    pub(crate) statement_handle: usize,
    pub(crate) parameter_data: Option<ParameterData>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ParameterData {
    pub num_columns: u8,
    pub columns: Vec<Column>,
}

/// Struct representing attributes returned from Exasol.
/// These can either be returned by an explicit `getAttributes` call
/// or as part of any response.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Attributes {
    #[serde(flatten)]
    pub(crate) map: HashMap<String, Value>,
}

/// Struct representing database information returned
/// after establishing a connection.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct LoginInfo {
    protocol_version: ProtocolVersion,
    #[serde(flatten)]
    map: HashMap<String, Value>,
}

/// Struct representing public key information
/// returned as part of the login process.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublicKey {
    public_key_exponent: String,
    public_key_modulus: String,
    public_key_pem: String,
}

impl PublicKey {
    pub(crate) fn into_string_key(self) -> String {
        self.public_key_pem
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
fn deser_query_result2() {
    let json_data = json!(
    {
        "resultType": "rowCount",
        "rowCount": 0
    });

    let de: QueryResultDe = serde_json::from_value(json_data).unwrap();
}

/// Struct used for deserialization of the JSON
/// returned sending queries to the database.
/// Represents the result of one query.
#[allow(non_snake_case)]
#[derive(Debug, Deserialize)]
#[serde(tag = "resultType", rename_all = "camelCase")]
pub(crate) enum QueryResultDe {
    #[serde(rename_all = "camelCase")]
    ResultSet { result_set: ResultSetDe },
    #[serde(rename_all = "camelCase")]
    RowCount { row_count: u32 },
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

/// Struct used for deserialization of a ResultSet
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct ResultSetDe {
    #[serde(rename = "numRows")]
    pub(crate) total_rows_num: u32,
    #[serde(rename = "numRowsInMessage")]
    pub(crate) chunk_rows_num: usize,
    pub(crate) num_columns: u8,
    pub(crate) result_set_handle: Option<u16>,
    pub(crate) columns: Vec<Column>,
    #[serde(default)]
    pub(crate) data: Vec<Row>,
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

/// Struct containing the name and datatype (as seen in Exasol) of a given column.
#[allow(unused)]
#[derive(Debug, Deserialize, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "dataType")]
    pub datatype: DataType,
}

impl Display for Column {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.name, self.datatype)
    }
}

/// Struct representing a datatype for a column in a result set.
#[allow(unused)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
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

impl Display for DataType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.type_name)
    }
}
