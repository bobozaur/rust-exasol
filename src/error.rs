use crate::response::{Attributes, ExaError, Response, ResponseData};
use rsa;
use serde_json;
use std::fmt::Debug;
use std::num::ParseIntError;
use thiserror::Error as ThisError;
use tungstenite;
use url;

/// Result implementation for the crate.;
pub type Result<T> = std::result::Result<T, Error>;

impl TryFrom<Response> for (Option<ResponseData>, Option<Attributes>) {
    type Error = Error;

    #[inline]
    fn try_from(resp: Response) -> Result<Self> {
        match resp {
            Response::Ok {
                response_data: data,
                attributes: attr,
            } => Ok((data, attr)),
            Response::Error { exception: e } => Err(Error::ExasolError(e)),
        }
    }
}

/// Error type for the crate.
#[derive(Debug, ThisError)]
pub enum Error {
    #[error(transparent)]
    DriverError(#[from] DriverError),
    #[error(transparent)]
    ExasolError(#[from] ExaError),
}

/// Driver related errors.
///
/// These errors have nothing to do with the Exasol database itself,
/// but rather the driver having issues with the underlying websocket connection,
/// making requests, processing responses, etc.
#[derive(Debug, ThisError)]
pub enum DriverError {
    #[error(transparent)]
    BindError(#[from] BindError),
    #[error(transparent)]
    DataError(#[from] DataError),
    #[error(transparent)]
    ConversionError(#[from] ConversionError),
    #[error(transparent)]
    RequestError(#[from] RequestError),
    #[error(transparent)]
    ConnectionError(#[from] ConnectionError),
}

#[derive(Debug, ThisError)]
pub enum BindError {
    #[error("Missing parameter to bind for {0}")]
    MappingError(String),
    #[error(transparent)]
    DeserError(#[from] serde_json::error::Error),
    #[error(transparent)]
    ParseIntError(#[from] ParseIntError)
}

/// Data processing related errors.
#[derive(Debug, ThisError)]
pub enum DataError {
    #[error("Missing data for column {0}")]
    MissingColumn(String),
    #[error("Expecting {0} data columns in array, found {1}")]
    InsufficientData(usize, usize),
    #[error("Data iterator items must deserialize to sequences or maps")]
    InvalidIterType,
    #[error(transparent)]
    TypeParseError(#[from] serde_json::error::Error),
}

/// Conversion errors from [QueryResult] to its variants.
#[derive(Debug, ThisError)]
pub enum ConversionError {
    #[error("Not a result set")]
    ResultSetError,
    #[error("Not a row count")]
    RowCountError,
}

/// Request related errors
#[derive(Debug, ThisError)]
pub enum RequestError {
    #[error(transparent)]
    MessageParseError(#[from] serde_json::error::Error),
    #[error(transparent)]
    WebsocketError(#[from] tungstenite::error::Error),
    #[error(transparent)]
    CompressionError(#[from] std::io::Error),
    #[error("Cannot fetch rows chunk - missing statement handle")]
    MissingHandleError,
    #[error("Response does not contain {0}")]
    InvalidResponse(&'static str),
}

/// Connection related errors
#[derive(Debug, ThisError)]
pub enum ConnectionError {
    #[error("Invalid DSN provided")]
    InvalidDSN,
    #[error(transparent)]
    DSNParseError(#[from] url::ParseError),
    #[error("Cannot resolve DSN hostnames")]
    HostnameResolutionError(#[from] std::io::Error),
    #[error("Cannot parse provided hostname range in DSN")]
    RangeParseError(#[from] ParseIntError),
    #[error(transparent)]
    CryptographyError(#[from] rsa::errors::Error),
    #[error(transparent)]
    PKCSError(#[from] rsa::pkcs1::Error),
    #[error(transparent)]
    WebsocketError(#[from] tungstenite::error::Error),
}
