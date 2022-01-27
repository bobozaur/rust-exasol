use rsa;
use serde::{Deserialize, Serialize};
use serde_json;
use serde_json::json;
use std::fmt::{Debug, Display, Formatter};
use std::num::ParseIntError;
use tungstenite;
use url;

/// Result implementation to return an exasol::error::Error;
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for Exasol
#[derive(Debug)]
pub enum Error {
    BindError(String),
    ConnectionError(ConnectionError),
    RequestError(RequestError),
    QueryError(ExaError),
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::BindError(e) => write!(f, "{}", e),
            Self::ConnectionError(e) => write!(f, "{}", e),
            Self::RequestError(e) => write!(f, "{}", e),
            Self::QueryError(e) => write!(f, "{}", e),
        }
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::BindError(e)
    }
}

impl From<ConnectionError> for Error {
    fn from(e: ConnectionError) -> Self {
        Self::ConnectionError(e)
    }
}

impl From<RequestError> for Error {
    fn from(e: RequestError) -> Self {
        Self::RequestError(e)
    }
}

impl From<ExaError> for Error {
    fn from(e: ExaError) -> Self {
        Self::QueryError(e)
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(e: serde_json::error::Error) -> Self {
        Self::RequestError(RequestError::ResponseParseError(e))
    }
}

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Self::ConnectionError(ConnectionError::DSNParseError(e))
    }
}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Self::ConnectionError(ConnectionError::HostnameResolutionError(e))
    }
}

impl From<ParseIntError> for Error {
    fn from(e: ParseIntError) -> Self {
        Self::ConnectionError(ConnectionError::RangeParseError(e))
    }
}

impl From<rsa::errors::Error> for Error {
    fn from(e: rsa::errors::Error) -> Self {
        Self::ConnectionError(ConnectionError::CryptographyError(e))
    }
}

impl From<rsa::pkcs1::Error> for Error {
    fn from(e: rsa::pkcs1::Error) -> Self {
        Self::ConnectionError(ConnectionError::PKCSError(e))
    }
}

impl From<tungstenite::error::Error> for Error {
    fn from(e: tungstenite::error::Error) -> Self {
        Self::ConnectionError(ConnectionError::WebsocketError(e))
    }
}

/// Request related errors
#[derive(Debug)]
pub enum RequestError {
    MissingHandleError,
    InvalidResponse(String),
    ResponseParseError(serde_json::error::Error),
    ExaError(ExaError),
}

impl std::error::Error for RequestError {}

impl Display for RequestError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingHandleError => {
                write!(f, "Cannot fetch rows chunk - missing statement handle")
            }
            Self::InvalidResponse(e) => write!(f, "{}", e),
            Self::ResponseParseError(e) => write!(f, "{}", e),
            Self::ExaError(e) => write!(f, "{}", e),
        }
    }
}

/// Connection related errors
#[derive(Debug)]
pub enum ConnectionError {
    InvalidDSN,
    DSNParseError(url::ParseError),
    HostnameResolutionError(std::io::Error),
    RangeParseError(ParseIntError),
    CryptographyError(rsa::errors::Error),
    PKCSError(rsa::pkcs1::Error),
    AuthError(ExaError),
    WebsocketError(tungstenite::error::Error),
}

impl std::error::Error for ConnectionError {}

impl Display for ConnectionError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::InvalidDSN => write!(f, "Invalid DSN provided"),
            Self::DSNParseError(e) => write!(f, "{}", e),
            Self::HostnameResolutionError(e) => write!(f, "{}", e),
            Self::RangeParseError(e) => write!(f, "{}", e),
            Self::CryptographyError(e) => write!(f, "{}", e),
            Self::PKCSError(e) => write!(f, "{}", e),
            Self::AuthError(e) => write!(f, "{}", e),
            Self::WebsocketError(e) => write!(f, "{:?}", e),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExaError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String,
}

impl Display for ExaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl std::error::Error for ExaError {}
