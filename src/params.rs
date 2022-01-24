use serde_json::{json, Map, Value};
use std::collections::{BTreeMap, HashMap};
use std::fmt::Display;

#[test]
fn param_iter_test() {
    use super::params::param_iter;

    let a = vec![1, 2];
    assert_eq!("(1, 2)", param_iter(&a, |v| v.as_sql_param()));
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
    assert_eq!("(1, 2)", param_array(&a));
}

/// Used for stringifying an array-like referenced object
fn param_array<'a, T, I>(a: I) -> String
where
    T: 'a + SQLParameter,
    I: IntoIterator<Item = &'a T>,
{
    param_iter(a, |val| val.as_sql_param())
}

#[test]
fn param_map_test() {
    use super::params::param_map;
    use std::collections::HashMap;

    let m = HashMap::from([("a", 1), ("b", 2)]);
    let p = param_map(&m);

    // As maps are unordered, we cannot test for an exact output
    // That does not matter in the context of an SQL IN clause, though
    assert!(p.contains("1"));
    assert!(p.contains("2"));
}
/// Used for stringifying a map-like referenced object
fn param_map<'a, T, I, U>(m: I) -> String
where
    T: 'a + SQLParameter,
    I: IntoIterator<Item = (U, &'a T)>,
{
    param_iter(m, |(_, val)| val.as_sql_param())
}

/// Used to provide a mechanism for generating a String as a SQL parameter out of self.
/// Can be implemented to custom types to generate values as needed.
/// ```
/// use std::collections::HashMap;
/// use exasol::exasol::SQLParameter;
///
/// assert_eq!("1", 1u8.as_sql_param());
/// assert_eq!("1", 1u16.as_sql_param());
/// assert_eq!("1", 1u32.as_sql_param());
/// assert_eq!("1", 1u64.as_sql_param());
/// assert_eq!("1", 1u128.as_sql_param());
///
/// assert_eq!("1", 1i8.as_sql_param());
/// assert_eq!("1", 1i16.as_sql_param());
/// assert_eq!("1", 1i32.as_sql_param());
/// assert_eq!("1", 1i64.as_sql_param());
/// assert_eq!("1", 1i128.as_sql_param());
///
/// assert_eq!("1", 1usize.as_sql_param());
///
/// assert_eq!("NULL", ().as_sql_param());
///
/// assert_eq!("1", true.as_sql_param());
/// assert_eq!("0", false.as_sql_param());
///
/// assert_eq!("'1'", "1".as_sql_param());
/// assert_eq!("'1'", String::from("1").as_sql_param());
///
/// assert_eq!("(1, 2, 3)", vec![1, 2, 3].as_sql_param());
/// assert_eq!("('1', '2', '3')", vec!["1", "2", "3"].as_sql_param());
///
/// // Map values are returned just like vectors, but in no particular order
/// assert_eq!("(1)", HashMap::from([("a", 1)]).as_sql_param());
/// assert_eq!("('1')", HashMap::from([("a", "1")]).as_sql_param());
/// ```
pub trait SQLParameter {
    fn as_sql_param(&self) -> String;
}

impl SQLParameter for () {
    fn as_sql_param(&self) -> String {
        "NULL".to_owned()
    }
}

impl SQLParameter for bool {
    fn as_sql_param(&self) -> String {
        if *self {
            "1".to_owned()
        } else {
            "0".to_owned()
        }
    }
}

impl SQLParameter for &str {
    fn as_sql_param(&self) -> String {
        format!("'{}'", self)
    }
}

impl SQLParameter for String {
    fn as_sql_param(&self) -> String {
        format!("'{}'", self)
    }
}

impl SQLParameter for usize {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for u8 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for u16 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for u32 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for u64 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for u128 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for i8 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for i16 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for i32 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for i64 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for i128 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for f32 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl SQLParameter for f64 {
    fn as_sql_param(&self) -> String {
        self.to_string()
    }
}

impl<T> SQLParameter for Vec<T>
where
    T: SQLParameter,
{
    fn as_sql_param(&self) -> String {
        param_array(self)
    }
}

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
            Value::String(s) => format!("'{}'", s),
            Value::Number(n) => format!("{}", n),
            Value::Bool(b) => format!("{}", if *b { 1 } else { 0 }),
            Value::Array(a) => param_array(a),
            Value::Object(o) => param_map(o),
        }
    }
}

/// Used to enable structs to be transposed as parameter maps ready use bind in a SQL query
/// ```
/// use std::collections::HashMap;
/// use serde_json::{Map, json, Value};
/// use exasol::exasol::{ParameterMap, SQLParameter};
///
///let h = HashMap::from([("a".to_owned(), "1"), ("b".to_owned(), "2")]);
/// println!("{:?}", h.as_params_map());
///
/// let j = json!({"a": 1, "b": 2, "c": [3, 4, 5]});
/// if let Some(m) = j.as_object() {
///     println!("{:?}", m.as_params_map());
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
///     fn as_params_map(self) -> HashMap<String, String> {
///        HashMap::from([
///             ("a".to_owned(), self.a.as_sql_param()),
///             ("b".to_owned(), self.b.as_sql_param()),
///             ("c".to_owned(), self.c.as_sql_param()),
///         ])
///    }
/// }
///
/// let some_struct = SomeStruct {a: "text".to_owned(), b: 50, c: vec![1, 2, 3]};
/// println!("{:?}", some_struct.as_params_map());
/// ```
pub trait ParameterMap {
    fn as_params_map(self) -> HashMap<String, String>;
}

impl ParameterMap for Map<String, Value> {
    fn as_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.as_sql_param()))
            .collect()
    }
}

impl<T> ParameterMap for HashMap<String, T>
where
    T: SQLParameter,
{
    fn as_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.as_sql_param()))
            .collect()
    }
}

impl<T> ParameterMap for BTreeMap<String, T>
where
    T: SQLParameter,
{
    fn as_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k, v.as_sql_param()))
            .collect()
    }
}

impl<'a, T, I> ParameterMap for &'a I
where
    T: 'a + SQLParameter,
    I: Copy + IntoIterator<Item = (&'a String, &'a T)>,
{
    fn as_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k.clone(), v.as_sql_param()))
            .collect()
    }
}

impl<'a, T, I> ParameterMap for &'a mut I
where
    T: 'a + SQLParameter,
    I: Copy + IntoIterator<Item = (&'a String, &'a mut T)>,
{
    fn as_params_map(self) -> HashMap<String, String> {
        self.into_iter()
            .map(|(k, v)| (k.clone(), v.as_sql_param()))
            .collect()
    }
}
