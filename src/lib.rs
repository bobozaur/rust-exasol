//! # exasol
//!
//! Database connector for Exasol implemented using the Websocket protocol.
//! Messages are sent and received in the JSON format.

mod connection;
mod query_result;
mod params;
mod con_opts;
pub mod error;

pub use crate::connection::{Connection, connect};
pub use crate::query_result::{QueryResult, ResultSet, Row, Column};
pub use crate::params::{ParameterMap, SQLParameter, bind};
pub use crate::con_opts::ConOpts;


