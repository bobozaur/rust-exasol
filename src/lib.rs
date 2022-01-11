//! # Rust-Exasol
//!
//! `Rust EXASOL` is a database connector for Exasol implemented using the Websocket protocol

mod error;
mod connection;
mod query_result;

pub mod exasol {
    pub use crate::connection::ExaConnection;
}


