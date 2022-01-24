use serde_json::{json, Value};

/// Macro for creating query binding parameters
/// Practically just a wrapper over the `json` macro from `serde_json`
/// that checks that the returned `Value` is a JSON object.
///
/// A more performant solution would be to directly check that the provided input is map-like
/// before parsing it but that would sort-of be like re-inventing the wheel
/// since the `json` macro is around, so we're using this instead.
/// ```should_panic
/// #[macro_use] extern crate exasol;
/// fn main(){
/// let value = params!("a");
/// }
/// ```
///
/// ```
/// #[macro_use] extern crate exasol;
/// fn main() {
/// let value = params!({
/// "code": 200,
/// "success": true,
/// "features": ["serde", "shawerma"]
/// });
/// }
/// ```
#[macro_export(local_inner_macros)]
macro_rules! params {
    ($($json:tt)+) => {
        match serde_json::json!($($json)+) {
            serde_json::Value::Object(o) => o,
            serde_json::Value::Array(o) => std::panic!("Invalid format!. Parameter {:?}", o),
            serde_json::Value::String(o) => std::panic!("Invalid format!. Parameter {:?}", o),
            serde_json::Value::Number(o) => std::panic!("Invalid format!. Parameter {:?}", o),
            serde_json::Value::Bool(o) => std::panic!("Invalid format!. Parameter {:?}", o),
            _ => std::panic!("Invalid parameters provided!")
        }
    };
}
