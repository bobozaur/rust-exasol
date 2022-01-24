use std::error;
use std::fmt::{Debug, Display, Formatter};
use serde::{Serialize, Deserialize};
use serde_json;
use serde_json::json;
use tungstenite;
use rsa;
use url;

/// Result implementation to return an exasol::error::Error;
pub type Result<T> = std::result::Result<T, Error>;

#[derive(Debug)]
pub enum Error {
    InvalidResponse(String),
    DSNError(url::ParseError),
    ConnectionError(tungstenite::error::Error),
    ParsingError(serde_json::error::Error),
    CryptographyError(rsa::errors::Error),
    PKCSError(rsa::pkcs1::Error),
    RequestError(ExaError),
    AuthError(ExaError),
    QueryError(ExaError)
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self {
                Self::InvalidResponse(e) => write!(f, "{}", e),
                Self::DSNError(e) => write!(f, "{}", e),
                Self::ConnectionError(e) => write!(f, "{:?}", e),
                Self::ParsingError(e) => write!(f, "{}", e),
                Self::CryptographyError(e) => write!(f, "{}", e),
                Self::PKCSError(e) => write!(f, "{}", e),
                Self::RequestError(e) |
                Self::AuthError(e) |
                Self::QueryError(e) => write!(f, "{}", e)
            }
    }
}

impl From<tungstenite::error::Error> for Error {
    fn from(e: tungstenite::error::Error) -> Self {
        Self::ConnectionError(e)
    }
}

impl From<url::ParseError> for Error {
    fn from(e: url::ParseError) -> Self {
        Self::DSNError(e)
    }
}

impl From<serde_json::error::Error> for Error {
    fn from(e: serde_json::error::Error) -> Self {
        Self::ParsingError(e)
    }
}

impl From<String> for Error {
    fn from(e: String) -> Self {
        Self::InvalidResponse(e)
    }
}

impl From<rsa::errors::Error> for Error {
    fn from(e: rsa::errors::Error) -> Self {
        Self::CryptographyError(e)
    }
}

impl From<rsa::pkcs1::Error> for Error {
    fn from(e: rsa::pkcs1::Error) -> Self {
        Self::PKCSError(e)
    }
}

impl std::error::Error for Error {}

#[derive(Debug, Deserialize, Serialize)]
pub struct ExaError {
    text: String,
    #[serde(rename = "sqlCode")]
    code: String
}

impl Display for ExaError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", json!(self))
    }
}

impl std::error::Error for ExaError {}