//! # exasol
//!
//! Database connector for Exasol implemented using the Websocket protocol.
//! Messages are sent and received in the JSON format.

pub mod con_opts;
pub mod connection;
pub mod error;
pub mod params;
pub mod query_result;

pub use crate::con_opts::ConOpts;
pub use crate::connection::{connect, Connection};
pub use crate::params::{bind, ParameterMap, SQLParameter};
pub use crate::query_result::{Column, QueryResult, ResultSet, Row};
