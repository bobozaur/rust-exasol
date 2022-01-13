use serde_json::{json, Map, Value};

/// Macro for creating and validating query binding parameters
#[macro_export]
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

#[test]
#[should_panic]
fn test1() {
    let value = params!({
    "code": 200,
    "success": true,
    "payload": {
        "features": [
            "serde",
            "json"
        ]
    }
});
}

#[test]
#[should_panic]
fn test2() {
    let value = params!("a");
}

#[test]
fn test3() {
    let value = params!({
    "code": 200,
    "success": true,
    "features": ["serde", "shawerma"]
    });
}