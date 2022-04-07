//! # exasol
//!
//! Database connector for Exasol implemented using the Websocket protocol.
//! Messages are sent and received in the JSON format.
//!
//! # Errors
//!
//! Most, if not all, of the public API is implemented to return a [Result](crate::error::Result) due to
//! the nature of the library. Mentioning this here to avoid repeating the same note on all public functions.
//!
//! # Examples
//!
//! Using the [connect] function to conveniently create a connection.
//!
//! ```
//! use exasol::error::Result;
//! use exasol::{connect, bind, QueryResult};
//! use std::env;
//!
//! // Credentials can be retrieved however you like
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! // Quick and convenient way to create a connection
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//!
//! // Executing a statement
//! let result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
//!
//! // The ResultSet is a lazy iterator, fetching rows as needed.
//! // For that reason, it can only be iterated over when owned,
//! // as it gets consumed in the process.
//! if let QueryResult::ResultSet(r) = result {
//!     // Each row is a Result<Vec<serde_json::Value>>
//!     for row in r {
//!         for col_val in row.unwrap() {
//!             // Columns values are of type serde_json::Value
//!             println!("{}", col_val);
//!         }
//!     }
//! }
//! ```
//!
//! Keeping in mind that the result set is an iterator when owned,
//! the full iterator toolset can be used.
//!
//! Additionally, `TryFrom` is implemented for [QueryResult](crate::query::QueryResult) to make
//! [ResultSet](crate::query::ResultSet) retrieval more convenient, instead of having to match
//! against it (the same can be done for the `u32` representing the affected rows count).
//!
//! ```
//! use exasol::error::Result;
//! use exasol::{connect, bind, QueryResult, ResultSet};
//! use std::env;
//! use serde_json::Value;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//! let result = exa_con.execute("SELECT OBJECT_NAME FROM EXA_ALL_OBJECTS LIMIT 5000;").unwrap();
//!
//! ResultSet::try_from(result)
//!     .unwrap()
//!     .by_ref()
//!     .take(500)
//!     .collect::<Result<Vec<Vec<Value>>>>()
//!     .unwrap()
//!     .into_iter()
//!     .map(|row| row[0].as_str().unwrap().to_owned())
//!     .collect::<Vec<String>>();
//! ```
//!
//! In the example above, note the `collect::<Result<Vec<Row>>>()` call.
//! When iterating over a result set, because fetch requests may fail, every row
//! (which by default is a [Vec<serde_json::Value>]) is in the form of a [Result].
//!
//! `collect` can handle that for us and convert a [Vec<Result<Row>>] into a [Result<Vec<Row>>],
//! stopping on the first error encountered, if any.
//!
//! # Custom Row Type
//!
//! The crate implements a custom deserializer, which is a trimmed down specialized version
//! of the `serde_json` deserializer. Database row deserialization can be attempted to any
//! type implementing [Deserialize](serde::Deserialize).
//!
//! This is thanks to the magic of `serde`. In addition, regular `serde` features are supported,
//! such as flattening or renaming. Note that implicitly, column names are converted to lowercase
//! for easier deserialization. This can be changed through the [Connection::set_lowercase_columns]
//! method.
//!
//! Map-like and sequence-like types are natively deserialized. For `enum` sequence-like variants,
//! see [deserialize_as_seq].
//!
//! ```
//! use exasol::{connect, QueryResult, ResultSet};
//! use exasol::error::Result;
//! use serde_json::Value;
//! use serde::Deserialize;
//! use std::env;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//! let result = exa_con.execute("SELECT 1, 2 UNION ALL SELECT 1, 2;").unwrap();
//!
//! // Change the expected row type with the turbofish notation
//! let mut result_set = ResultSet::try_from(result).unwrap().with_row_type::<(String, String)>();
//! let row1 = result_set.next();
//!
//! // Nothing stops you from changing row types
//! // on the same result set, even while iterating through it
//! let mut result_set = result_set.with_row_type::<Vec<String>>();
//! let row2 = result_set.next();
//!
//! let result = exa_con.execute("SELECT 1 as col1, 2 as col2, 3 as col3 \
//!                               UNION ALL \
//!                               SELECT 4 as col1, 5 as col2, 6 as col3;").unwrap();
//!
//! #[derive(Debug, Deserialize)]
//! struct Test {
//!     col1: u8,
//!     col2: u8,
//!     col3: u8,
//! }
//!
//! let result_set = ResultSet::try_from(result).unwrap().with_row_type::<Test>();
//! for row in result_set.with_row_type::<Test>() {
//!     let ok_row = row.unwrap();
//!     // do stuff with row
//! }
//! ```
//!
//! # Parameter binding
//!
//! Queries can be composed by binding named or positional parameters to them through the [bind] function.
//! The function takes a string and a type implementing the [Serialize](serde::Serialize) trait.
//! The second argument must serialize to a sequence or map.
//!
//! Named parameter values behaviour:
//! * single value type get parsed to their SQL representation
//! * sequence-like types get parsed to a parenthesized list with elements in SQL representation
//! * map-like types get their values parsed to a parenthesized list with elements in SQL representation
//!
//! ```
//! use exasol::bind;
//! use serde::Serialize;
//!
//! #[derive(Serialize)]
//! struct Parameters {
//!     col1: String,
//!     col2: u16,
//!     col3: Vec<String>
//! }
//!
//! let params = Parameters {
//!     col1: "test".to_owned(),
//!     col2: 10,
//!     col3: vec!["a".to_owned(), "b".to_owned(), "c".to_owned()]
//! };
//!
//! let query = "\
//!     SELECT * FROM TEST_TABLE \
//!     WHERE NAME = :col1 \
//!     AND ID = :col2 \
//!     AND VALUE IN :col3;";
//!
//! let new_query = bind(query, params).unwrap();
//! assert_eq!(new_query, "\
//!     SELECT * FROM TEST_TABLE \
//!     WHERE NAME = 'test' \
//!     AND ID = 10 \
//!     AND VALUE IN ('a', 'b', 'c');");
//! ```
//!
//! # Custom Connection
//!
//! The [Connection] struct can be directly instantiated with the use of [ConOpts].
//!
//! ```
//! use exasol::{ConOpts, Connection};
//! use std::env;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! // Only providing fields for which we want custom values
//! let mut opts = ConOpts::new();
//! opts.set_dsn(dsn);
//! opts.set_user(user);
//! opts.set_password(password);
//! opts.set_schema(schema);
//!
//! let exa_con = Connection::new(opts).unwrap();
//! ```
//!
//! # Features
//!
//! - `native-tls` - (disabled by default) enables `tungstenite` WSS support through native-tls
//! - `rustls` - (disabled by default) enables `tungstenite` WSS support through rustls
//! - `flate2` - (disabled by default) enables support for requests and responses compression
//!
//! Enabling these features allows changing additional settings in [ConOpts] instances.
//!
//! # Panics
//! Attempting to use these methods without their respective features enabled results in panics.
//!
//! ``` should_panic
//! # use exasol::{ConOpts, Connection};
//! # use std::env;
//! #
//! # let dsn = env::var("EXA_DSN").unwrap();
//! # let schema = env::var("EXA_SCHEMA").unwrap();
//! # let user = env::var("EXA_USER").unwrap();
//! # let password = env::var("EXA_PASSWORD").unwrap();
//! #
//! let mut opts = ConOpts::new();
//!
//! opts.set_dsn(dsn);
//! opts.set_user(user);
//! opts.set_password(password);
//! opts.set_schema(schema);
//!
//! opts.set_encryption(true);
//! opts.set_compression(true);
//! ```
//!
//! # Batch Execution
//!
//! Faster query execution can be achieved through either
//! [Connection::execute_batch] or [PreparedStatement].

extern crate core;

pub mod con_opts;
pub mod connection;
pub mod error;
pub mod params;
pub mod query;
pub mod response;
pub mod row;

pub use crate::con_opts::{ConOpts, ProtocolVersion};
pub use crate::connection::{connect, Connection};
pub use crate::params::bind;
pub use crate::query::{PreparedStatement, QueryResult, ResultSet};
pub use crate::response::{Column, DataType};
pub use crate::row::deserialize_as_seq;
