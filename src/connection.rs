use std::collections::HashMap;
use std::net::TcpStream;

/// Main protagonist of this crate, the `ExaConnection` struct
/// will be what we use to interact with the database

use rand::rngs::OsRng;
use rsa::{PaddingScheme, pkcs1::FromRsaPublicKey, PublicKey, RsaPublicKey};
use serde_json::{json, Value};
use tungstenite::{connect, Message, stream::MaybeTlsStream, WebSocket};
use url::Url;

use crate::error::{Error, ExaError};
use crate::query_result::{QueryResult, Results, ResultSet};

pub type Result<T> = std::result::Result<T, Error>;

pub struct ExaConnection {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl Drop for ExaConnection {
    fn drop(&mut self) {
        // Closing session on Exasol side
        self.do_request(json!({"command": "disconnect"}));

        // Sending Message::Close frame
        self.ws.close(None);

        // Reading the Message::Close frame sent by server
        self.ws.read_message();

        // It is now safe to drop the socket
    }
}

impl ExaConnection {
    pub fn new(dsn: &str, schema: &str, user: &str, password: &str) -> Result<ExaConnection> {
        let (ws, _) = connect(Url::parse(dsn)?)?;
        let mut con = ExaConnection { ws };
        con.login(schema, user, password)?;
        con.get_attributes()?;
        Ok(con)
    }

    pub fn execute(&mut self, query: &str) -> Result<QueryResult> {
        let payload = json!({"command": "execute", "sqlText": query});
        serde_json::from_value::<Results>(self.do_request(payload)?
            .ok_or(Error::InvalidResponse("No response data received".to_owned()))?)?
            .get_one()
    }

    pub fn execute_batch(&mut self, queries: Vec<&str>) -> Result<Vec<QueryResult>> {
        let payload = json!({"command": "executeBatch", "sqlTexts": queries});
        serde_json::from_value::<Results>(self.do_request(payload)?
            .ok_or(Error::InvalidResponse("No response data received".to_owned()))?)?
            .get_all()
    }

    pub fn set_attributes(&mut self, attrs: Value) -> Result<()> {
        let mut payload = json!({"command": "setAttributes"});

        match &mut payload {
            Value::Object(m) => {
                let _ = &mut m.insert(String::from("attributes"), attrs);
            }
            _ => {}
        };

        self.do_request(payload)?;
        self.get_attributes()?;

        Ok(())
    }

    fn get_attributes(&mut self) -> Result<()> {
        let payload = json!({"command": "getAttributes"});
        self.do_request(payload)?;
        Ok(())
    }
    /// Converts JSON payload to string and writes message to websocket
    fn send(&mut self, payload: Value) -> Result<()> {
        Ok(self.ws.write_message(Message::Text(payload.to_string()))?)
    }

    /// We're only interested in getting Text or Binary messages
    /// The rest, such as Pong, can be discarded until we're replied with one of the above
    fn recv(&mut self) -> Result<Value> {
        loop {
            break match self.ws.read_message()? {
                Message::Text(resp) => Ok(serde_json::from_str(&resp)?),
                Message::Binary(resp) => Ok(serde_json::from_slice(&resp)?),
                _ => continue
            };
        }
    }

    /// Consumes the response JSON while checking the status
    /// Returns an Option with the responseData field, attributes field or None
    fn consume(mut json_data: Value) -> Result<Option<Value>> {
        let status = ExaConnection::extract_value(&mut json_data, "status")?
            .as_str()
            .ok_or(Error::InvalidResponse("Could not parse status field".to_owned()))?
            .to_owned();

        match status.as_str() {
            "ok" => {
                if let Ok(data) = ExaConnection::extract_value(&mut json_data, "responseData") {
                    Ok(Some(data))
                } else if let Ok(attr) = ExaConnection::extract_value(&mut json_data, "attributes") {
                    Ok(Some(attr))
                } else {
                    Ok(None)
                }
            }
            "error" => {
                let exception: Value = ExaConnection::extract_value(&mut json_data, "exception")?;
                let exc: ExaError = serde_json::from_value(exception)?;
                Err(Error::RequestError(exc))
            }
            _ => Err(Error::InvalidResponse("Unknown status value not in JSON response".to_owned()))
        }
    }

    fn extract_value(data: &mut Value, field: &str) -> Result<Value> {
        Ok(data.get_mut(field)
            .ok_or(Error::InvalidResponse(format!("Field '{}' not in JSON response", field)))?
            .take())
    }

    fn do_request(&mut self, payload: Value) -> Result<Option<Value>> {
        self.send(payload).and_then(|_| self.recv()).and_then(|data| ExaConnection::consume(data))
    }

    fn get_public_key(&mut self) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": 1});
        let pem = self.do_request(payload)?
            .ok_or(Error::InvalidResponse("No data received".to_owned()))
            .and_then(|mut data| ExaConnection::extract_value(&mut data, "publicKeyPem"))?
            .as_str()
            .ok_or(Error::InvalidResponse("Malformed public key".to_owned()))?
            .to_owned();

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)?)
    }

    fn encrypt_password(password: &str, public_key: RsaPublicKey) -> Result<String> {
        let mut rng = OsRng;
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_password = base64::encode(public_key.encrypt(&mut rng, padding, password.as_bytes())?);
        Ok(enc_password)
    }

    fn login(&mut self, schema: &str, user: &str, password: &str) -> Result<Option<Value>> {
        let public_key = self.get_public_key()?;
        let enc_password = ExaConnection::encrypt_password(password, public_key)?;

        let payload = json!({
                "username": user,
                "password": enc_password,
                "driverName": "RustEXASOL 0.01",
                "clientName": "RustEXASOL 0.01",
                "clientVersion": "0.0.1",
                "clientOs": std::env::consts::OS,
                "clientRuntime": "Rust",
                "useCompression": false,
                "attributes": {
                            "currentSchema": schema,
                            "autocommit": true,
                            "queryTimeout": 0
                            }
                });

        self.do_request(payload)
    }
}
