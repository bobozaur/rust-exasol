use serde_json::{json, Map, Value};

/// Macro for creating query binding parameters
/// ```
/// #[macro_use] extern crate exasol;
///
/// #[should_panic]
/// fn test1() {
///     let value = params!("a");
/// }
/// ```
///
/// ```
/// #[macro_use] extern crate exasol;
///
/// fn test2() {
///     let value = params!({
///     "code": 200,
///     "success": true,
///     "features": ["serde", "shawerma"]
///     });
/// }
/// ```
#[macro_export(local_inner_macros)]
macro_rules! params {
    ($($json:tt)+) => {
        validate_params(json!($($json)+))
    };
}

fn validate_params(p: Value) -> Map<String, Value> {
    if let Value::Object(o) = p {
        for v in o.values() {
            match v {
                Value::String(_) | Value::Number(_)
                | Value::Bool(_) | Value::Null => {}
                Value::Array(x) => {
                    for y in x.iter() {
                        match y {
                            Value::String(_) | Value::Number(_)
                            | Value::Bool(_) | Value::Null => {}
                            _ => panic!("Invalid parameter {}", y)
                        }
                    }
                }
                _ => panic!("Invalid parameter {}", v)
            }
        };
        o
    } else {
        panic!("Invalid parameter {}", p)
    }
}