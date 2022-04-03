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
/// `:` chars in string literals must be escaped if they're not part of a parameter:
///
/// ```
/// use exasol::bind;
///
/// let params = vec!["VALUE1", "VALUE2"];
/// let query = "INSERT INTO MY_TABLE VALUES(:1, :0, 'str \\:str');";
/// let new_query = bind(query, params).unwrap();
///
/// assert_eq!("INSERT INTO MY_TABLE VALUES('VALUE2', 'VALUE1', 'str \\:str');", new_query);
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
/// use exasol::bind;
/// use serde::Serialize;
///
/// #[derive(Serialize)]
/// struct Parameters {
///     col1: String,
///     col2: u16,
///     col3: Vec<String>
/// }
///
/// let params = Parameters {
///     col1: "test".to_owned(),
///     col2: 10,
///     col3: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
/// };
///
/// let query = "\
///     SELECT * FROM TEST_TABLE \
///     WHERE NAME = :col1 \
///     AND ID = :col2 \
///     AND VALUE IN :col3;";
///
/// let new_query = bind(query, params).unwrap();
/// assert_eq!(new_query, "\
///     SELECT * FROM TEST_TABLE \
///     WHERE NAME = 'test' \
///     AND ID = 10 \
///     AND VALUE IN ('a', 'b', 'c');");
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
        Value::Object(o) => do_param_binding(query, gen_map_params(o)),
        Value::Array(a) => do_param_binding(query, gen_seq_params(a)),
        _ => Err(BindError::SerializeError),
    }
}

/// Bind map elements to the query
#[inline]
fn do_param_binding(query: &str, map: HashMap<String, String>) -> BindResult {
    lazy_static! {
        static ref RE: Regex = Regex::new(r"[:\w\\]:\w+|:\w+:|:(\w+)").unwrap();
    }

    let dummy = ""; // placeholder
    let mut result = Ok(dummy.to_owned()); // Will store errors here

    // If capture group 1 is None, then we only matched the regex expressions
    // that we purposely ignore, in which case we return a string slice from the query
    // representing th exact full match -> we replace a string with itself.
    //
    // If capture group 1 is Some, then we look in the map and replace
    // the parameter with it's corresponded value, storing errors if encountered.
    let q = RE.replace_all(query, |cap: &Captures| {
        cap.get(1)
            .map(|m| match map.get(m.as_str()) {
                Some(k) => k.as_str(),
                None => {
                    result = Err(BindError::MappingError(cap[0].to_owned()));
                    dummy
                }
            })
            .unwrap_or(&query[cap.get(0).unwrap().range()])
    });

    result.and(Ok(q.into_owned()))
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
fn gen_seq_params(params: Vec<Value>) -> HashMap<String, String> {
    params
        .into_iter()
        .enumerate()
        .map(|(i, v)| (i.to_string(), into_sql_param(v)))
        .collect()
}

/// Transforms [Value] to it's SQL string representation
#[inline]
fn into_sql_param(val: Value) -> String {
    match val {
        Value::Null => "NULL".to_owned(),
        Value::String(s) => ["'", &s.replace('\'', "''"), "'"].concat(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => match b {
            true => "1".to_owned(),
            false => "0".to_owned(),
        },
        Value::Array(a) => {
            let iter = a.into_iter().map(into_sql_param);
            build_param_list(iter)
        }
        Value::Object(o) => {
            let iter = o.into_iter().map(|(_, v)| into_sql_param(v));
            build_param_list(iter)
        }
    }
}

/// Concatenates an iterator of Strings into a parameter list
/// such as "(a, b, c)"
#[inline]
fn build_param_list<I>(iter: I) -> String
where
    I: Iterator<Item = String>,
{
    let mut str_params = "(".to_string();

    iter.for_each(|s| {
        str_params.push_str(&s);
        str_params.push_str(", ");
    });

    str_params.pop();
    str_params.pop();
    str_params.push(')');
    str_params
}
