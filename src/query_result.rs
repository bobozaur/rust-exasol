use serde::{Deserialize, Serialize};
use serde_json::Value;
use crate::connection::Result;
use crate::error::Error;

#[derive(Debug, Deserialize)]
pub struct Column {
    name: String,
    #[serde(rename = "dataType")]
    datatype: Value,
}

#[derive(Debug, Deserialize)]
pub struct ResultSet {
    #[serde(rename = "numColumns")]
    num_columns: u8,
    #[serde(rename = "numRows")]
    total_num_rows: u32,
    #[serde(rename = "numRowsInMessage")]
    chunk_num_rows: u16,
    #[serde(rename = "resultSetHandle")]
    statement_handle: Option<u16>,
    columns: Vec<Column>,
    #[serde(default)]
    data: Vec<Vec<Value>>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Results {
    #[serde(rename = "numResults")]
    num_results: u16,
    #[serde(rename = "results")]
    #[serde(default)]
    query_results: Vec<QueryResult>,
}

impl Results {
    pub(crate) fn get_one(mut self) -> Result<QueryResult> {
        if let 1u16 = &self.num_results {
            Ok(self.query_results.remove(0))
        } else {
            Err(Error::InvalidResponse(format!("{} result sets returned. Expecting 1", &self.num_results)))
        }
    }

    pub(crate) fn get_all(mut self) -> Result<Vec<QueryResult>> {
        Ok(self.query_results)
    }
}

#[derive(Debug, Deserialize)]
#[serde(from = "QueryResultImpl")]
pub enum QueryResult {
    #[serde(rename = "resultSet")]
    ResultSet(ResultSet),
    #[serde(rename = "rowCount")]
    RowCount(u32),
}

impl From<QueryResultImpl> for QueryResult {
    fn from(q: QueryResultImpl) -> Self {
        match q {
            QueryResultImpl::ResultSet {resultSet} => Self::ResultSet(resultSet),
            QueryResultImpl::RowCount {rowCount} => Self::RowCount(rowCount)
        }
    }
}

#[derive(Debug, Deserialize)]
#[serde(tag = "resultType")]
pub(crate) enum QueryResultImpl {
    #[serde(rename = "resultSet")]
    ResultSet{resultSet: ResultSet},
    #[serde(rename = "rowCount")]
    RowCount {rowCount: u32}
}

#[cfg(test)]
mod tests {
    use serde_json::{json, Value};

    use crate::query_result::{Column, QueryResult, Results, ResultSet};

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

        let de : Vec<Vec<Value>> = serde_json::from_value(json_data).unwrap();
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

        let de: ResultSet = serde_json::from_value(json_data).unwrap();
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

        let de: QueryResult = serde_json::from_value(json_data).unwrap();
    }

        #[test]
    fn deser_query_result2() {
        let json_data = json!(
            {
                "resultType": "rowCount",
                "rowCount": 0
            });

        let de: QueryResult = serde_json::from_value(json_data).unwrap();
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