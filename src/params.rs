use crate::error::{Error, Result};
use lazy_static::lazy_static;
use regex::Regex;
use serde_json::{Map, Value};
use std::collections::{BTreeMap, HashMap};

/// Binds named parameters from a construct implementing `ParameterMap` to the given string.
/// Returns a Result containing the formatted string or an Error if any parameters are missing.
///
/// ```
/// use serde_json::json;
/// use exasol::exasol::bind;
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
/// use exasol::exasol::bind;
///
/// let params = HashMap::from([
///     ("COL1".to_owned(), "VALUE1"),
///     ("COL2".to_owned(), "VALUE2")
/// ]);
///
/// let query = "INSERT INTO MY_TABLE VALUES(:COL1, :COL2);";
/// let new_query = bind(query, params).unwrap();
///
/// assert_eq!("INSERT INTO MY_TABLE VALUES('VALUE1', 'VALUE2');", new_query);
/// ```
pub fn bind<T>(query: &str, params: T) -> Result<String>
where
    T: ParameterMap,
{
    lazy_static! {
        static ref RE: Regex = Regex::new(r":(\w+)").unwrap();
    }

    let map = params.into_params_map();
    let mut final_query = query.to_owned();

    for cap in RE.captures_iter(query) {
        let sub = map.get(&cap[1]).ok_or(Error::InvalidResponse(format!(
            "{} not found in map!",
            &cap[1]
        )))?;

        final_query = final_query.replace(&cap[0], sub);
    }

    Ok(final_query)
}

#[test]
fn param_iter_test() {
    use super::params::param_iter;

    let a = vec![1, 2];
    assert_eq!("(1, 2)", param_iter(a, |v| v.as_sql_param()));
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
    T: SQLParameter,
    I: IntoIterator<Item = T>,
{
    param_iter(a, |val| val.as_sql_param())
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
    T: SQLParameter,
    I: IntoIterator<Item = (U, T)>,
{
    param_iter(m, |(_, val)| val.as_sql_param())
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

/// Used to provide a mechanism for generating a String as a SQL parameter out of self.
/// Can be implemented to custom types to generate values as needed.
pub trait SQLParameter {
    fn as_sql_param(&self) -> String;
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("NULL", ().as_sql_param());
/// ```
impl SQLParameter for () {
    fn as_sql_param(&self) -> String {
        "NULL".to_owned()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", true.as_sql_param());
/// assert_eq!("0", false.as_sql_param());
/// ```
impl SQLParameter for bool {
    fn as_sql_param(&self) -> String {
        param_bool(*self)
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("'1'", "1".as_sql_param());
/// ```
impl SQLParameter for &str {
    fn as_sql_param(&self) -> String {
        param_str(self)
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("'1'", String::from("1").as_sql_param());
/// ```
impl SQLParameter for String {
    fn as_sql_param(&self) -> String {
        param_str(&self)
    }
}
/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1usize.as_sql_param());
/// ```
impl SQLParameter for usize {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1u8.as_sql_param());
/// ```
impl SQLParameter for u8 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1u16.as_sql_param());
/// ```
impl SQLParameter for u16 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1u32.as_sql_param());
/// ```
impl SQLParameter for u32 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1u64.as_sql_param());
/// ```
impl SQLParameter for u64 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1u128.as_sql_param());
/// ```
impl SQLParameter for u128 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1i8.as_sql_param());
/// ```
impl SQLParameter for i8 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1i16.as_sql_param());
/// ```
impl SQLParameter for i16 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1i32.as_sql_param());
/// ```
impl SQLParameter for i32 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1i64.as_sql_param());
/// ```
impl SQLParameter for i64 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1i128.as_sql_param());
/// ```
impl SQLParameter for i128 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1f32.as_sql_param());
/// ```
impl SQLParameter for f32 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
/// assert_eq!("1", 1f64.as_sql_param());
/// ```
impl SQLParameter for f64 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
///
/// assert_eq!("(1, 2, 3)", vec![1, 2, 3].as_sql_param());
/// assert_eq!("('1', '2', '3')", vec!["1", "2", "3"].as_sql_param());
/// ```
impl<T> SQLParameter for Vec<T>
where
    T: SQLParameter,
{
    fn as_sql_param(&self) -> String {
        param_array(self)
    }
}

/// ```
/// use std::collections::HashMap;
/// use exasol::exasol::SQLParameter;
///
/// // Map values are returned just like vectors, but in no particular order
/// assert_eq!("(1)", HashMap::from([("a", 1)]).as_sql_param());
/// assert_eq!("('1')", HashMap::from([("a", "1")]).as_sql_param());
/// ```
impl<U, T> SQLParameter for HashMap<U, T>
where
    T: SQLParameter,
{
    fn as_sql_param(&self) -> String {
        param_map(self)
    }
}

impl<U, T> SQLParameter for BTreeMap<U, T>
where
    T: SQLParameter,
{
    fn as_sql_param(&self) -> String {
        param_map(self)
    }
}

impl SQLParameter for Map<String, Value> {
    fn as_sql_param(&self) -> String {
        param_map(self)
    }
}

impl SQLParameter for Value {
    fn as_sql_param(&self) -> String {
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
/// use exasol::exasol::SQLParameter;
///
/// // References also work
/// let x = 5;
/// let y = &x;
/// assert_eq!("5", y.as_sql_param());
/// ```
impl<T> SQLParameter for &T
where
    T: SQLParameter,
{
    fn as_sql_param(&self) -> String {
        (*self).as_sql_param()
    }
}

/// ```
/// use exasol::exasol::SQLParameter;
///
/// // Mutable references also work
/// let mut x = 5;
/// let y = &mut x;
/// assert_eq!("5", y.as_sql_param());
/// ```
impl<T> SQLParameter for &mut T
where
    T: SQLParameter,
{
    fn as_sql_param(&self) -> String {
        (&**self).as_sql_param()
    }
}

/// Used to enable structs to be transposed as parameter maps ready use bind in a SQL query
/// ```
/// use std::collections::HashMap;
/// use serde_json::{Map, json, Value};
/// use exasol::exasol::{ParameterMap, SQLParameter};
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
/// impl ParameterMap for SomeStruct {
///     fn into_params_map(self) -> HashMap<String, String> {
///        HashMap::from([
///             ("a".to_owned(), self.a.as_sql_param()),
///             ("b".to_owned(), self.b.as_sql_param()),
///             ("c".to_owned(), self.c.as_sql_param()),
///         ])
///    }
/// }
///
/// let some_struct = SomeStruct {a: "text".to_owned(), b: 50, c: vec![1, 2, 3]};
/// println!("{:?}", some_struct.into_params_map());
/// ```
pub trait ParameterMap {
    fn into_params_map(self) -> HashMap<String, String>;
}

impl ParameterMap for Map<String, Value> {
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.as_sql_param()))
            .collect()
    }
}

impl<T> ParameterMap for HashMap<String, T>
where
    T: SQLParameter,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.as_sql_param()))
            .collect()
    }
}

impl<T> ParameterMap for BTreeMap<String, T>
where
    T: SQLParameter,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.as_sql_param()))
            .collect()
    }
}

impl<I, T> ParameterMap for &I
where
    for<'a> &'a I: IntoIterator<Item = (&'a String, &'a T)>,
    T: SQLParameter,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k.clone(), v.as_sql_param()))
            .collect()
    }
}

impl<I, T> ParameterMap for &mut I
where
    for<'a> &'a mut I: IntoIterator<Item = (&'a String, &'a mut T)>,
    T: SQLParameter,
{
    fn into_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k.clone(), v.as_sql_param()))
            .collect()
    }
}
