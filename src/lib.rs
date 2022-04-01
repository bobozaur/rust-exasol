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
//! Additionally, `TryFrom` is implemented for [QueryResult] to make [ResultSet] retrieval
//! more convenient (the same can be done for the u32 representing affected rows count).
//!
//! ```
//! # use exasol::error::Result;
//! # use exasol::{connect, bind, QueryResult, ResultSet};
//! # use std::env;
//! # use serde_json::Value;
//! #
//! # let dsn = env::var("EXA_DSN").unwrap();
//! # let schema = env::var("EXA_SCHEMA").unwrap();
//! # let user = env::var("EXA_USER").unwrap();
//! # let password = env::var("EXA_PASSWORD").unwrap();
//!
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//! let result = exa_con.execute("SELECT OBJECT_NAME FROM EXA_ALL_OBJECTS LIMIT 5000;").unwrap();
//!
//! ResultSet::try_from(result)
//!     .unwrap()
//!     .take(500)
//!     .collect::<Result<Vec<Vec<Value>>>>()
//!     .unwrap()
//!     .into_iter()
//!     .map(|row| row[0].as_str().unwrap().to_owned())
//!     .collect();
//! }
//! ```
//!
//! In the example above, note the `.collect::<Result<Vec<Row>>>()` call.
//! When iterating over a result set, because fetch requests may fail, every row
//! (which by default is a [Vec<serde_json::Value>]) is in the form of a [Result].
//!
//! `collect` can handle that for us and convert a [Vec<Result<Row>>] into a [Result<Vec<Row>>],
//! stopping on the first error encountered, if any.
//!
//! # Custom Row Type
//!
//! The crate implements a custom deserializer, which is a trimmed down specialized version
//! of the `serde_json` deserializer.
//! Database row deserialization can be attempted to any type implementing [Deserialize].
//! This is thanks to the magic of `serde`.
//!
//! Map-like types are natively deserialized.
//! For `enum` sequence-like variants, see [deserialize_as_seq].
//!
//! ```
//! # use exasol::{connect, QueryResult, ResultSet};
//! # use exasol::error::Result;
//! # use serde_json::Value;
//! # use std::env;
//!
//! # let dsn = env::var("EXA_DSN").unwrap();
//! # let schema = env::var("EXA_SCHEMA").unwrap();
//! # let user = env::var("EXA_USER").unwrap();
//! # let password = env::var("EXA_PASSWORD").unwrap();
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
//! }
//! ```
//!
//! # Parameter binding
//!
//! Queries can be composed by binding named or positional parameters to them through the [bind] function.
//! The function takes a string and a type implementing the [Serialize] trait.
//! The second argument must serialize to a sequence or map.
//!
//! ```
//! use serde_json::json;
//! use exasol::bind;
//!
//! let params = vec!["VALUE1", "VALUE2"];
//! let query = "INSERT INTO MY_TABLE VALUES(:1, :0);";
//! let new_query = bind(query, params).unwrap();
//!
//! assert_eq!("INSERT INTO MY_TABLE VALUES('VALUE2', 'VALUE1');", new_query);
//!
//! let j = json!({
//!     "COL1": "'TEST",
//!     "COL2": 5
//! });
//!
//! let params = j.as_object().unwrap();
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
//! let mut opts = ConOpts::new();
//! opts.set_dsn(dsn);
//! opts.set_user(user);
//! opts.set_password(password);
//! opts.set_schema(schema);
//!
//! let exa_con = Connection::new(opts).unwrap();
//! ```
//!
//! # Batch Execution
//!
//! Currently, the faster query processing functionalities are either
//! through [Connection::execute_batch] or [PreparedStatement].

extern crate core;

pub mod con_opts;
pub mod connection;
pub mod error;
pub mod params;
pub mod query;
pub mod response;
pub mod row;

pub use crate::con_opts::ConOpts;
pub use crate::connection::{connect, Connection};
pub use crate::params::bind;
pub use crate::query::{PreparedStatement, QueryResult, ResultSet};
pub use crate::response::{Column, DataType};
pub use crate::row::deserialize_as_seq;
