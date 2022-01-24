//! # Rust-Exasol
//!
//! Database connector for Exasol implemented using the Websocket protocol.
//! Messages are sent and received in the JSON format.

mod error;
mod connection;
mod query_result;
mod macros;
mod params;

pub mod exasol {
    pub use crate::connection::Connection;
    pub use crate::query_result::{QueryResult, ResultSet, Row, Column};
    pub use crate::params::{ParameterMap, SQLParameter};
    pub use crate::error::{Result, Error};
}


