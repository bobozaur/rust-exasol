use crate::Column;

#[cfg(not(any(feature = "native-tls", feature = "rustls")))]
pub(crate) static WS_STR: &str = "ws";
#[cfg(any(feature = "native-tls", feature = "rustls"))]
pub(crate) static WS_STR: &str = "wss";

pub(crate) static NO_RESULT_SET: &str = "No result set found";
pub(crate) static NO_RESPONSE_DATA: &str = "No response data received";
pub(crate) static NOT_PONG: &str = "Received frame different from Pong";
pub(crate) static NO_PUBLIC_KEY: &str = "Public key not received";
pub(crate) static MISSING_DATA: &str = "Missing fetched data!";

pub(crate) static DUMMY_COLUMNS_NUM: u8 = 0;
pub(crate) static DUMMY_COLUMNS_VEC: Vec<Column> = vec![];