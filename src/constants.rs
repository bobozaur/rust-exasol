use crate::Column;

pub(crate) static WS_STR: &str = "ws";
pub(crate) static WSS_STR: &str = "wss";

pub(crate) static DEFAULT_PORT: u16 = 8563;
pub(crate) static DEFAULT_FETCH_SIZE: u32 = 5 * 1024 * 1024;
pub(crate) static DEFAULT_CLIENT_PREFIX: &str = "Rust Exasol";

pub(crate) static NO_RESULT_SET: &str = "No result set found";
pub(crate) static NO_RESPONSE_DATA: &str = "No response data received";
pub(crate) static NOT_PONG: &str = "Received frame different from Pong";
pub(crate) static NO_PUBLIC_KEY: &str = "Public key not received";
pub(crate) static MISSING_DATA: &str = "Missing fetched data!";

pub(crate) static DUMMY_COLUMNS_NUM: u8 = 0;
pub(crate) static DUMMY_COLUMNS_VEC: Vec<Column> = vec![];