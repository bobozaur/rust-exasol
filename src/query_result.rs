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

#[derive(Debug, Deserialize)]
pub struct Column {
    name: String,
    #[serde(rename = "dataType")]
    datatype: Value,
}

#[derive(Debug, Deserialize)]
struct FetchedData {
    #[serde(rename = "numRows")]
    chunk_rows_num: usize,
    #[serde(default)]
    data: Vec<Vec<Value>>,
}


#[derive(Debug, Deserialize)]
pub struct ResultSetImpl {
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
    fn fetch(&mut self) -> Option<Vec<Value>> {
        self.statement_handle.and_then(|h| {
            let payload = json!({
            "command": "fetch",
            "resultSetHandle": self.statement_handle,
            "startPosition": self.total_rows_pos,
            "numBytes": 5 * 1024 * 1024,
        });

            let mut c = (*self.connection).borrow_mut();
            let f = c.get_data::<FetchedData>(payload).unwrap();

            self.chunk_rows_num = f.chunk_rows_num;
            self.chunk_rows_pos = 0;

            self.iter = f.data.into_iter().map(|v| v.into_iter()).collect();
            self.iter.iter_mut()
                .map(|iter| iter.next())
                .collect::<Option<Row>>()
                .and_then(|r| if r.is_empty() { None } else { Some(r) })
        })
    }
}

impl Iterator for ResultSet {
    type Item = Row;

    fn next(&mut self) -> Option<Self::Item> {
        let res = self.iter.iter_mut()
            .map(|iter| iter.next())
            .collect::<Option<Row>>()
            .and_then(|r| if r.is_empty() { None } else { Some(r) })
            .or_else(|| {
                if self.total_rows_pos == self.total_rows_num { None } else if self.chunk_rows_pos == self.chunk_rows_num { self.fetch() } else { None }
            });

        self.total_rows_pos += 1;
        self.chunk_rows_pos += 1;

        res
    }
}


#[derive(Debug, Deserialize)]
pub(crate) struct Results {
    #[serde(rename = "numResults")]
    num_results: u16,
    #[serde(rename = "results")]
    query_results: Vec<QueryResultImpl>,
}

impl Results {
    pub(crate) fn consume(mut self, con_rc: &Rc<RefCell<ConnectionImpl>>) -> Vec<QueryResult> {
        self.query_results
            .into_iter()
            .map(|q| {
                match q {
                    QueryResultImpl::ResultSet { resultSet } =>
                        {
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
                    QueryResultImpl::RowCount { rowCount } => QueryResult::RowCount(rowCount)
                }
            })
            .collect()
    }
}

#[derive(Debug)]
pub enum QueryResult {
    ResultSet(ResultSet),
    RowCount(u32),
}

#[derive(Debug, Deserialize)]
#[serde(tag = "resultType")]
pub(crate) enum QueryResultImpl {
    #[serde(rename = "resultSet")]
    ResultSet { resultSet: ResultSetImpl },
    #[serde(rename = "rowCount")]
    RowCount { rowCount: u32 },
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::query_result::{Column, QueryResult, Results, ResultSetImpl};

    #[test]
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

    #[test]
    fn deser_data() {
        let json_data = json!(
            [
               [
                  1
               ]
            ]);

        let de: Vec<Vec<Value>> = serde_json::from_value(json_data).unwrap();
    }

    #[test]
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

    #[test]
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
        use crate::query_result::QueryResultImpl;
        let de: QueryResultImpl = serde_json::from_value(json_data).unwrap();
    }

    #[test]
    fn deser_query_result2() {
        let json_data = json!(
            {
                "resultType": "rowCount",
                "rowCount": 0
            });
        use crate::query_result::QueryResultImpl;
        let de: QueryResultImpl = serde_json::from_value(json_data).unwrap();
    }

    #[test]
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
}