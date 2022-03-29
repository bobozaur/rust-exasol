use crate::response::{Attributes, ExaError, Response, ResponseData};
use rsa;
use serde_json;
use std::fmt::{Debug, Display, Formatter, write};
use std::num::ParseIntError;
use tungstenite;
use url;

/// Result implementation to return an exasol::error::Error;
pub type Result<T> = std::result::Result<T, Error>;

impl From<Response> for Result<(Option<ResponseData>, Option<Attributes>)> {
    fn from(resp: Response) -> Self {
        match resp {
            Response::Ok {
                response_data: data,
                attributes: attr,
            } => Ok((data, attr)),
            Response::Error { exception: e } => Err(e.into()),
        }
    }
}

/// Error type for Exasol
#[derive(Debug)]
pub enum Error {
    DataError(String),
    BindError(String),
    ConnectionError(ConnectionError),
    RequestError(RequestError),
    QueryError(ExaError),
}

impl Error {
    /// Converts this [Error] variant to a QueryError if the variant matches.
    /// Otherwise returns the initial error.
    pub(crate) fn conv_query_error(self) -> Self {
        match self {
            Self::RequestError(RequestError::ExaError(err)) => Self::QueryError(err),
            _ => self,
        }
    }
}

impl std::error::Error for Error {}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::DataError(e) => write!(f, "{}", e),
            Self::BindError(e) => write!(f, "{}", e),
            Self::ConnectionError(e) => write!(f, "{}", e),
            Self::RequestError(e) => write!(f, "{}", e),
            Self::QueryError(e) => write!(f, "{}", e),
        }
    }
}

impl From<&'static str> for Error {
    fn from(e: &'static str) -> Self {
        Self::RequestError(RequestError::InvalidResponse(e))
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::DataError(e)
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
    InvalidResponse(&'static str),
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
