use crate::error::{Error, Result};
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
                result = Err(Error::BindError(format!("{} not found in map!", &cap[1])));
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
    assert_eq!("(1, 2)", param_iter(a, |v| v.to_sql_param()));
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
    param_iter(a, |val| val.to_sql_param())
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
    param_iter(m, |(_, val)| val.to_sql_param())
}

/// Used for transposing a string to an escaped SQL parameter
fn param_str(s: &str) -> String {
    format!("'{}'", s.replace("'", "''"))
}

/// Used for transposing a bool to an SQL parameter
fn param_bool(b: bool) -> String {
    if b {
        "1".to_owned()
    } else {
        "0".to_owned()
    }
}

/// Used in default implementations of [SQLParamMap] to provide a mechanism
/// for generating a String as a SQL parameter out of a type.
///
/// Can be implemented to other types for custom behaviour.
pub trait SQLParam {
    fn to_sql_param(&self) -> String;
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("NULL", ().to_sql_param());
/// ```
impl SQLParam for () {
    fn to_sql_param(&self) -> String {
        "NULL".to_owned()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", true.to_sql_param());
/// assert_eq!("0", false.to_sql_param());
/// ```
impl SQLParam for bool {
    fn to_sql_param(&self) -> String {
        param_bool(*self)
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("'1'", "1".to_sql_param());
/// ```
impl SQLParam for &str {
    fn to_sql_param(&self) -> String {
        param_str(self)
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("'1'", String::from("1").to_sql_param());
/// ```
impl SQLParam for String {
    fn to_sql_param(&self) -> String {
        param_str(&self)
    }
}
/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1usize.to_sql_param());
/// ```
impl SQLParam for usize {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u8.to_sql_param());
/// ```
impl SQLParam for u8 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u16.to_sql_param());
/// ```
impl SQLParam for u16 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u32.to_sql_param());
/// ```
impl SQLParam for u32 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u64.to_sql_param());
/// ```
impl SQLParam for u64 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1u128.to_sql_param());
/// ```
impl SQLParam for u128 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i8.to_sql_param());
/// ```
impl SQLParam for i8 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i16.to_sql_param());
/// ```
impl SQLParam for i16 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i32.to_sql_param());
/// ```
impl SQLParam for i32 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i64.to_sql_param());
/// ```
impl SQLParam for i64 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1i128.to_sql_param());
/// ```
impl SQLParam for i128 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1f32.to_sql_param());
/// ```
impl SQLParam for f32 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
/// assert_eq!("1", 1f64.to_sql_param());
/// ```
impl SQLParam for f64 {
    fn to_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::SQLParam;
///
/// assert_eq!("(1, 2, 3)", vec![1, 2, 3].to_sql_param());
/// assert_eq!("('1', '2', '3')", vec!["1", "2", "3"].to_sql_param());
/// ```
impl<T> SQLParam for Vec<T>
where
    T: SQLParam,
{
    fn to_sql_param(&self) -> String {
        param_array(self)
    }
}

/// ```
/// use std::collections::HashMap;
/// use exasol::SQLParam;
///
/// // Map values are returned just like vectors, but in no particular order
/// assert_eq!("(1)", HashMap::from([("a", 1)]).to_sql_param());
/// assert_eq!("('1')", HashMap::from([("a", "1")]).to_sql_param());
/// ```
impl<U, T> SQLParam for HashMap<U, T>
where
    T: SQLParam,
{
    fn to_sql_param(&self) -> String {
        param_map(self)
    }
}

impl<U, T> SQLParam for BTreeMap<U, T>
where
    T: SQLParam,
{
    fn to_sql_param(&self) -> String {
        param_map(self)
    }
}

impl SQLParam for Map<String, Value> {
    fn to_sql_param(&self) -> String {
        param_map(self)
    }
}

impl SQLParam for Value {
    fn to_sql_param(&self) -> String {
        match self {
            Value::Null => "NULL".to_owned(),
            Value::String(s) => param_str(&s),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => param_bool(*b),
            Value::Array(a) => param_array(a),
            Value::Object(o) => param_map(o),
        }
    }
}

/// ```
/// use exasol::SQLParam;
///
/// // References also work
/// let x = 5;
/// let y = &x;
/// assert_eq!("5", y.to_sql_param());
/// ```
impl<T> SQLParam for &T
where
    T: SQLParam,
{
    fn to_sql_param(&self) -> String {
        (*self).to_sql_param()
    }
}

/// ```
/// use exasol::SQLParam;
///
/// // Mutable references also work
/// let mut x = 5;
/// let y = &mut x;
/// assert_eq!("5", y.to_sql_param());
/// ```
impl<T> SQLParam for &mut T
where
    T: SQLParam,
{
    fn to_sql_param(&self) -> String {
        (&**self).to_sql_param()
    }
}

/// Used to enable structs to be transposed as parameter maps for binding value to a SQL query.
/// Can generally be used along the [SQLParam] trait to properly generate parameters.
/// ```
/// use std::collections::HashMap;
/// use serde_json::{Map, json, Value};
/// use exasol::{SQLParamMap, SQLParam};
///
///let h = HashMap::from([("a".to_owned(), "1"), ("b".to_owned(), "2")]);
///let mut j = HashMap::from([("a".to_owned(), "1".to_owned()), ("b".to_owned(), "2".to_owned())]);
/// println!("{:?}", (&mut j).into_params_map());
/// println!("{:?}", (&h).into_params_map());
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
///             ("a".to_owned(), self.a.to_sql_param()),
///             ("b".to_owned(), self.b.to_sql_param()),
///             ("c".to_owned(), self.c.to_sql_param()),
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

impl SQLParamMap for Map<String, Value> {
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.to_sql_param()))
            .collect()
    }
}

impl<T> SQLParamMap for HashMap<String, T>
where
    T: SQLParam,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.to_sql_param()))
            .collect()
    }
}

impl<T> SQLParamMap for BTreeMap<String, T>
where
    T: SQLParam,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.to_sql_param()))
            .collect()
    }
}

impl<I, T> SQLParamMap for &I
where
    for<'a> &'a I: IntoIterator<Item = (&'a String, &'a T)>,
    T: SQLParam,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k.clone(), v.to_sql_param()))
            .collect()
    }
}

impl<I, T> SQLParamMap for &mut I
where
    for<'a> &'a mut I: IntoIterator<Item = (&'a String, &'a mut T)>,
    T: SQLParam,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k.clone(), v.to_sql_param()))
            .collect()
    }
}
