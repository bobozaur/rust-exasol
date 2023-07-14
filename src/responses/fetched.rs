use serde::Deserialize;
use serde_json::Value;

/// Struct used for deserialization of fetched data
/// from getting a result set given a statement handle
#[derive(Debug, Deserialize)]
pub struct DataChunk {
    #[serde(rename = "numRows")]
    pub chunk_rows_num: usize,
    pub data: Vec<Vec<Value>>,
}

impl DataChunk {
    pub fn len(&self) -> usize {
        self.data.len()
    }
}