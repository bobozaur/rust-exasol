use serde::Deserialize;

use crate::con_opts::ProtocolVersion;

/// Struct representing database information returned
/// after establishing a connection.
#[allow(unused)]
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LoginInfo {
    protocol_version: ProtocolVersion,
    session_id: u32,
    release_version: String,
    database_name: String,
    product_name: String,
    max_data_message_size: u32,
    max_identifier_length: u16,
    max_varchar_length: u16,
    identifier_quote_string: String,
    time_zone: String,
    time_zone_behavior: String,
}