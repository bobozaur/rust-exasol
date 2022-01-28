//! # exasol
//!
//! Database connector for Exasol implemented using the Websocket protocol.
//! Messages are sent and received in the JSON format.
//!
//! ```
//! use exasol::error::Result;
//! use exasol::{connect, bind};
//! use std::env;
//!
//! let dsn = env::var("EXA_DSN").unwrap();
//! let schema = env::var("EXA_SCHEMA").unwrap();
//! let user = env::var("EXA_USER").unwrap();
//! let password = env::var("EXA_PASSWORD").unwrap();
//!
//! let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
//!
//! let result = exa_con.execute("SELECT 1").unwrap();
//!
//! let result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();

pub mod con_opts;
pub mod connection;
pub mod error;
pub mod params;
pub mod query_result;

pub use crate::con_opts::ConOpts;
pub use crate::connection::{connect, Connection};
pub use crate::params::{bind, ParameterMap, SQLParameter};
pub use crate::query_result::{Column, QueryResult, ResultSet, Row};
