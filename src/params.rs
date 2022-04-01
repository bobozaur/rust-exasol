use crate::error::{DriverError, Result};
use lazy_static::lazy_static;
use regex::{Captures, Regex};
use serde_json::{Map, Value};
use std::collections::btree_map::BTreeMap;
use std::collections::HashMap;

/// Binds named parameters from a construct implementing [SQLParamMap] to the given string.
/// Returns a Result containing the formatted string or an Error if any parameters are missing.
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
    T: SQLParamMap,
{
    lazy_static! {
        static ref RE: Regex = Regex::new(r"[:\w\\]:\w+|:\w+:|(:(\w+))").unwrap();
    }

    let map = params.into_params_map();
    let dummy = ""; // dummy placeholder for replace on missing map keys

    // Due to the fixed type of the RE.replace_all method
    // we have to use a placeholder result where we'll store
    // the last error that comes up from a missing key, if any.
    //
    // We'll also use the dummy as replace value in that case
    // as we need to return something and we'll error out anyway.
    let mut result = Ok(dummy.to_owned());
    let q = RE
        .replace_all(query, |cap: &Captures| match map.get(&cap[2]) {
            Some(k) => k,
            None => {
                result = Err(DriverError::BindError(cap[1].to_owned()).into());
                dummy
            }
        })
        .to_string();

    // If all went well, return the actual query instead of the dummy value.
    result.and(Ok(q))
}

#[test]
fn param_iter_test() {
    use super::params::param_iter;

    let a = vec![1, 2];
    assert_eq!("(1, 2)", param_iter(a, |v| v.into_sql_param()));
}

/// Used for stringifying an iterator
fn param_iter<T, I, F>(i: I, f: F) -> String
where
    I: IntoIterator<Item = T>,
    F: Fn(I::Item) -> String,
{
    format!(
        "({})",
        i.into_iter().map(f).collect::<Vec<String>>().join(", ")
    )
}

#[test]
fn param_array_test() {
    use super::params::param_array;

    let a = vec![1, 2];
    let mut b = vec![1, 2];
    assert_eq!("(1, 2)", param_array(&a));
    assert_eq!("(1, 2)", param_array(&mut b));
    assert_eq!("(1, 2)", param_array(a));
}

/// Used for stringifying an array-like referenced object
fn param_array<T, I>(a: I) -> String
where
    T: SQLParam,
    I: IntoIterator<Item = T>,
{
    param_iter(a, |val| val.into_sql_param())
}

#[test]
fn param_map_test() {
    use super::params::param_map;
    use std::collections::HashMap;

    let m = HashMap::from([("a", 1), ("b", 2)]);
    let mut n = HashMap::from([("a", 1), ("b", 2)]);
    let o = param_map(&m);
    let p = param_map(m);
    let q = param_map(&mut n);

    // As maps are unordered, we cannot test for an exact output
    // That does not matter in the context of an SQL IN clause, though
    assert!(p.contains("1"));
    assert!(p.contains("2"));
    assert!(o.contains("1"));
    assert!(o.contains("2"));
    assert!(q.contains("1"));
    assert!(q.contains("2"));
}
/// Used for stringifying a map-like referenced object
fn param_map<T, I, U>(m: I) -> String
where
    T: SQLParam,
    I: IntoIterator<Item = (U, T)>,
{
    param_iter(m, |(_, val)| val.into_sql_param())
}

/// Used in default implementations of [SQLParamMap] to provide a mechanism
/// for generating a String as a SQL parameter out of a type.
///
/// Can be implemented to other types for custom behaviour.
pub trait SQLParam {
    fn into_sql_param(self) -> String;
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("NULL", ().into_sql_param());
/// ```
impl SQLParam for () {
    fn into_sql_param(self) -> String {
        "NULL".to_owned()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", true.into_sql_param());
/// assert_eq!("0", false.into_sql_param());
/// ```
impl SQLParam for bool {
    fn into_sql_param(self) -> String {
        if self {
            "1".to_owned()
        } else {
            "0".to_owned()
        }
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("'1'", "1".into_sql_param());
/// ```
impl SQLParam for &str {
    fn into_sql_param(self) -> String {
        format!("'{}'", self.replace('\'', "''"))
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("'1'", String::from("1").into_sql_param());
/// ```
impl SQLParam for String {
    fn into_sql_param(self) -> String {
        format!("'{}'", self.replace('\'', "''"))
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1usize.into_sql_param());
/// ```
impl SQLParam for usize {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u8.into_sql_param());
/// ```
impl SQLParam for u8 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u16.into_sql_param());
/// ```
impl SQLParam for u16 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u32.into_sql_param());
/// ```
impl SQLParam for u32 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u64.into_sql_param());
/// ```
impl SQLParam for u64 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u128.into_sql_param());
/// ```
impl SQLParam for u128 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i8.into_sql_param());
/// ```
impl SQLParam for i8 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i16.into_sql_param());
/// ```
impl SQLParam for i16 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i32.into_sql_param());
/// ```
impl SQLParam for i32 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i64.into_sql_param());
/// ```
impl SQLParam for i64 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i128.into_sql_param());
/// ```
impl SQLParam for i128 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1f32.into_sql_param());
/// ```
impl SQLParam for f32 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1f64.into_sql_param());
/// ```
impl SQLParam for f64 {
    fn into_sql_param(self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// let a = Some(1);
/// assert_eq!("1", a.into_sql_param());
/// ```
/// ```
/// use exasol::SQLParam;
/// let a = Some("la");
/// assert_eq!("'la'", a.into_sql_param());
/// ```
/// ```
/// use exasol::SQLParam;
/// let a: Option<&str> = None;
/// assert_eq!("NULL", a.into_sql_param());
/// ```
impl<T> SQLParam for Option<T>
where
    T: SQLParam,
{
    fn into_sql_param(self) -> String {
        match self {
            Some(v) => v.into_sql_param(),
            None => ().into_sql_param(),
        }
    }
}

/// ```
/// use exasol::SQLParam;
///
/// assert_eq!("(1, 2, 3)", vec![1, 2, 3].into_sql_param());
/// assert_eq!("('1', '2', '3')", vec!["1", "2", "3"].into_sql_param());
/// ```
impl<T> SQLParam for Vec<T>
where
    T: SQLParam,
{
    fn into_sql_param(self) -> String {
        param_array(self)
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("('1', '2', '3')", ["1", "2", "3"].into_sql_param());
/// ```
impl<T, const S: usize> SQLParam for [T; S]
where
    T: SQLParam,
{
    fn into_sql_param(self) -> String {
        param_array(self)
    }
}

/// ```
/// use exasol::SQLParam;
/// let v = vec!["1", "2", "3"];
/// assert_eq!("('1', '2', '3')", v.as_slice().into_sql_param());
/// ```
impl<T> SQLParam for &[T]
where
    T: SQLParam + Sized + Clone,
{
    fn into_sql_param(self) -> String {
        param_array(self)
    }
}

/// ```
/// use exasol::SQLParam;
/// let mut v = vec!["1", "2", "3"];
/// assert_eq!("('1', '2', '3')", v.as_mut_slice().into_sql_param());
/// ```
impl<T> SQLParam for &mut [T]
where
    T: SQLParam + Sized + Clone,
{
    fn into_sql_param(self) -> String {
        param_array(self)
    }
}

/// ```
/// use std::collections::HashMap;
/// use exasol::SQLParam;
///
/// // Map values are returned just like vectors, but in no particular order
/// assert_eq!("(1)", HashMap::from([("a", 1)]).into_sql_param());
/// assert_eq!("('1')", HashMap::from([("a", "1")]).into_sql_param());
/// ```
impl<U, T> SQLParam for HashMap<U, T>
where
    T: SQLParam,
{
    fn into_sql_param(self) -> String {
        param_map(self)
    }
}

impl<U, T> SQLParam for BTreeMap<U, T>
where
    T: SQLParam,
{
    fn into_sql_param(self) -> String {
        param_map(self)
    }
}

impl SQLParam for Map<String, Value> {
    fn into_sql_param(self) -> String {
        param_map(self)
    }
}

impl SQLParam for Value {
    fn into_sql_param(self) -> String {
        match self {
            Value::Null => "NULL".to_owned(),
            Value::String(s) => s.into_sql_param(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.into_sql_param(),
            Value::Array(a) => a.into_sql_param(),
            Value::Object(o) => o.into_sql_param(),
        }
    }
}

/// ```
/// use exasol::SQLParam;
///
/// // References also work
/// let x = 5;
/// let y = &x;
/// assert_eq!("5", y.into_sql_param());
/// ```
impl<T> SQLParam for &T
where
    T: SQLParam + Clone,
{
    fn into_sql_param(self) -> String {
        self.clone().into_sql_param()
    }
}

/// ```
/// use exasol::SQLParam;
///
/// // Mutable references also work
/// let mut x = 5;
/// let y = &mut x;
/// assert_eq!("5", y.into_sql_param());
/// ```
impl<T> SQLParam for &mut T
where
    T: SQLParam + Clone,
{
    fn into_sql_param(self) -> String {
        self.clone().into_sql_param()
    }
}

/// Used to enable structs to be transposed as parameter maps for binding value to a SQL query.
/// Can generally be used along the [SQLParam] trait to properly generate parameters.
///
/// Generic consuming implementations are done for all iterator types where `Item: (String, T)`
/// or `Item: (&str, T)`. Non-consuming implementations are done only for `Item: (String, T)`.
/// ```
/// use std::collections::HashMap;
/// use serde_json::{Map, json, Value};
/// use exasol::{SQLParamMap, SQLParam};
///
/// let k1 = "a".to_owned();
/// let k2 = "b".to_owned();
///
/// let mut h = HashMap::from([("a".to_owned(), "1"), ("b".to_owned(), "2")]);
/// let mut j = HashMap::from([(&k1, "1".to_owned()), (&k2, "2".to_owned())]);
/// println!("{:?}", j.into_params_map());
/// println!("{:?}", (&h).into_params_map());
/// println!("{:?}", (&mut h).into_params_map());
/// println!("{:?}", h.into_params_map());
///
/// let j = json!({"a": 1, "b": 2, "c": [3, 4, 5]});
/// if let Some(m) = j.as_object() {
///     println!("{:?}", m.into_params_map());
/// }
///
/// #[derive(Debug)]
/// struct SomeStruct {
///     a: String,
///     b: i32,
///     c: Vec<i32>
/// }
///
/// impl SQLParamMap for SomeStruct {
///     fn into_params_map(self) -> HashMap<String, String> {
///        HashMap::from([
///             ("a".to_owned(), self.a.into_sql_param()),
///             ("b".to_owned(), self.b.into_sql_param()),
///             ("c".to_owned(), self.c.into_sql_param()),
///         ])
///    }
/// }
///
/// let some_struct = SomeStruct {a: "text".to_owned(), b: 50, c: vec![1, 2, 3]};
/// println!("{:?}", some_struct.into_params_map());
/// ```
pub trait SQLParamMap {
    fn into_params_map(self) -> HashMap<String, String>;
}

impl<I, K, V> SQLParamMap for I
where
    I: IntoIterator<Item = (K, V)>,
    V: SQLParam,
    String: From<K>
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (String::from(k), v.into_sql_param()))
            .collect()
    }
}