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
//! Using the [connect] function to quickly create a connection.
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
//! // For that reason, it can only be iterated over when owned, as it gets consumed in the process.
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
//! ```
//! # use exasol::error::Result;
//! # use exasol::{connect, bind, QueryResult, Row};
//! # use std::env;
//! #
//! # let dsn = env::var("EXA_DSN").unwrap();
//! # let schema = env::var("EXA_SCHEMA").unwrap();
//! # let user = env::var("EXA_USER").unwrap();
//! # let password = env::var("EXA_PASSWORD").unwrap();
//!
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//! let result = exa_con.execute("SELECT OBJECT_NAME FROM EXA_ALL_OBJECTS LIMIT 5000;").unwrap();
//!
//! if let QueryResult::ResultSet(r) = result {
//!     let object_names_500: Vec<String> = r.take(500)
//!                                          .collect::<Result<Vec<Row>>>()
//!                                          .unwrap()
//!                                          .into_iter()
//!                                          .map(|row| row[0].as_str().unwrap().to_owned())
//!                                          .collect();
//! }
//! ```
//!
//! In the example above, note the `.collect::<Result<Vec<Row>>>()` call.
//! When iterating over a result set, because fetch requests may fail, every row
//! ([Row] is an alias for [Vec<serde_json::Value>]) is in the form of a [Result].
//!
//! `collect` can handle that for us and convert a [Vec<Result<Row>>] into a [Result<Vec<Row>>],
//! stopping on the first error encountered, if any.
//!
//! # Parameter binding
//!
//! Queries can be composed by binding named parameters to them through the [bind] function.
//! The function takes a string and a type implementing the [SQLParamMap] trait.
//!
//! ```
//! use serde_json::json;
//! use exasol::bind;
//!
//! let j = json!({
//!     "COL1": "'TEST",
//!     "COL2": 5
//! });
//!
//! let params = j.as_object().unwrap();
//!
//! let query = "INSERT INTO MY_TABLE VALUES(:COL1, :COL1, :COL2);";
//! let new_query = bind(query, params).unwrap();
//!
//! assert_eq!("INSERT INTO MY_TABLE VALUES('''TEST', '''TEST', 5);", new_query);
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
//! let opts = ConOpts {dsn, user, password, schema, autocommit: false, .. ConOpts::default()};
//!
//! let exa_con = Connection::new(opts).unwrap();
//! ```

pub mod con_opts;
pub mod connection;
mod constants;
pub mod error;
pub mod params;
pub mod query;
pub mod response;
pub mod row;

pub use crate::con_opts::ConOpts;
pub use crate::connection::{connect, Connection};
pub use crate::params::{bind, SQLParam, SQLParamMap};
pub use crate::query::{PreparedStatement, QueryResult, ResultSet};
pub use crate::response::{Column, DataType};
pub use crate::row::{MapRow, Row, TryRowToType};
