use crate::error::{BindError, DriverError, Result};
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use serde::Serialize;
use serde_json::{Map, Value};
use std::collections::HashMap;

type BindResult = std::result::Result<String, BindError>;

/// Binds named or positional parameters from a type implementing [Serialize].
/// If the type is map-like, named parameters are needed.
/// For sequence-like types, positional parameters are needed.
///
/// Returns a Result containing the formatted string or an Error if any parameters are missing.
///
///
/// ```
/// use exasol::bind;
///
/// let params = vec!["VALUE1", "VALUE2"];
/// let query = "INSERT INTO MY_TABLE VALUES(:1, :0);";
/// let new_query = bind(query, params).unwrap();
///
/// assert_eq!("INSERT INTO MY_TABLE VALUES('VALUE2', 'VALUE1');", new_query);
/// ```
///
/// ```
/// use serde_json::json;
/// use exasol::bind;
///
/// let j = json!({
///     "COL1": "'TEST",
///     "COL2": 5
/// });
///
/// let params = j.as_object().unwrap();
///
/// let query = "INSERT INTO MY_TABLE VALUES(:COL1, :COL1, :COL2);";
/// let new_query = bind(query, params).unwrap();
///
/// assert_eq!("INSERT INTO MY_TABLE VALUES('''TEST', '''TEST', 5);", new_query);
/// ```
///
/// ```
/// use std::collections::HashMap;
/// use exasol::bind;
///
/// let params = HashMap::from([
///     ("COL".to_owned(), "VALUE1"),
///     ("COL2".to_owned(), "VALUE2")
/// ]);
///
/// let query = "INSERT INTO MY_TABLE VALUES(:COL, :COL2);";
/// let new_query = bind(query, params).unwrap();
///
/// assert_eq!("INSERT INTO MY_TABLE VALUES('VALUE1', 'VALUE2');", new_query);
/// ```
pub fn bind<T>(query: &str, params: T) -> Result<String>
where
    T: Serialize,
{
    Ok(serde_json::to_value(params)
        .map_err(BindError::DeserializeError)
        .and_then(|val| parametrize_query(query, val))
        .map_err(DriverError::BindError)?)
}

/// Processes input [Value] into parameters and binds them to the query.
#[inline]
fn parametrize_query(query: &str, val: Value) -> BindResult {
    match val {
        Value::Object(o) => bind_map_params(query, gen_map_params(o)),
        Value::Array(a) => bind_seq_params(query, gen_seq_params(a)),
        _ => Err(BindError::SerializeError),
    }
}

/// Bind map elements to the query
fn bind_map_params(query: &str, map: HashMap<String, String>) -> BindResult {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"[:\w\\]:\w+|:\w+:|(:(\w+))").unwrap();
    }

    let dummy = ""; // placeholder
    let mut result = Ok(dummy.to_owned()); // Will store errors here

    let q = RE.replace_all(query, |cap: &Captures| match map.get(&cap[2]) {
        Some(k) => k,
        None => {
            result = Err(BindError::MappingError(cap[1].to_owned()));
            dummy
        }
    });

    result.and(Ok(q.into_owned()))
}

/// Bind sequence elements to the query
fn bind_seq_params(query: &str, arr: Vec<String>) -> BindResult {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"[:\d\\]:\d+|:\d+:|(:(\d+))").unwrap();
    }

    let dummy = "".to_owned(); // placeholder
    let mut res1 = Ok(&dummy); // Will store errors here
    let mut res2 = Ok(0usize); // Will store errors here

    let q = RE.replace_all(query, |cap: &Captures| {
        let index = cap[2].parse();
        match index {
            Ok(i) => match arr.get(i) {
                Some(k) => k,
                None => {
                    res1 = Err(BindError::MappingError(cap[1].to_owned()));
                    &dummy
                }
            },
            Err(_) => {
                res2 = index.map_err(BindError::ParseIntError);
                &dummy
            }
        }
    });

    res1.and(res2).and(Ok(q.into_owned()))
}

/// Generates a `HashMap<String, String>` of the params SQL representation.
#[inline]
fn gen_map_params(params: Map<String, Value>) -> HashMap<String, String> {
    params
        .into_iter()
        .map(|(k, v)| (k, into_sql_param(v)))
        .collect()
}

/// Generates a `Vec<String>` of the params SQL representation.
#[inline]
fn gen_seq_params(params: Vec<Value>) -> Vec<String> {
    params.into_iter().map( into_sql_param).collect()
}

/// Transforms [Value] to it's SQL string representation
fn into_sql_param(val: Value) -> String {
    match val {
        Value::Null => "NULL".to_owned(),
        Value::String(s) => format!("'{}'", s.replace('\'', "''")),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => match b {
            true => "1".to_owned(),
            false => "0".to_owned(),
        },
        Value::Array(a) => {
            let vec = a
                .into_iter()
                .map(into_sql_param)
                .collect::<Vec<String>>()
                .join(", ");
            format!("({})", vec)
        }
        Value::Object(o) => {
            let vec = o
                .into_iter()
                .map(|(_, v)| into_sql_param(v))
                .collect::<Vec<String>>()
                .join(", ");
            format!("({})", vec)
        }
    }
}
