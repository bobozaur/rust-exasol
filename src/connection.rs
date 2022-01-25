use std::cell::RefCell;
use std::collections::HashMap;
use std::net::TcpStream;
use std::rc::Rc;

use rand::rngs::OsRng;
use rsa::{pkcs1::FromRsaPublicKey, PaddingScheme, PublicKey, RsaPublicKey};
use serde::Deserialize;
use serde_json::{json, Value};
use tungstenite::{connect, stream::MaybeTlsStream, Message, WebSocket};
use url::Url;

use crate::error::{Error, ExaError, Result};
use crate::query_result::{QueryResult, Results};

/// The `Connection` struct will be what we use to interact with the database
///
/// insert example
///
///
#[derive(Debug)]
pub struct Connection {
    con: Rc<RefCell<ConnectionImpl>>,
}

impl Connection {
    /// Creates the connection
    ///
    /// insert example
    ///
    ///
    pub fn connect(dsn: &str, schema: &str, user: &str, password: &str) -> Result<Connection> {
        let con_impl = ConnectionImpl::connect(&dsn, &schema, &user, &password)?;
        Ok(Connection {
            con: Rc::new(RefCell::new(con_impl)),
        })
    }

    /// Sends a query to the database and waits for the result.
    ///
    /// insert example
    ///
    ///
    pub fn execute(&mut self, query: &str) -> Result<QueryResult> {
        (*self.con).borrow_mut().execute(&self.con, query)
    }

    /// Sends multiple queries to the database and waits for the result.
    ///
    /// insert example
    ///
    ///
    pub fn execute_batch(&mut self, queries: Vec<&str>) -> Result<Vec<QueryResult>> {
        (*self.con).borrow_mut().execute_batch(&self.con, queries)
    }

    /// Ping the server and wait for Pong frame
    ///
    /// ```
    /// # use exasol::exasol::{Connection};
    /// # use std::env;
    /// #
    /// # let dsn = format!("ws://{}", env::var("EXA_DSN").unwrap());
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = Connection::connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.ping().unwrap();
    /// ```
    pub fn ping(&mut self) -> Result<()> {
        (*self.con).borrow_mut().ping()
    }

    /// Sets autocommit mode On or Off
    ///
    /// ```
    /// # use exasol::exasol::{Connection};
    /// # use std::env;
    /// #
    /// # let dsn = format!("ws://{}", env::var("EXA_DSN").unwrap());
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = Connection::connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_autocommit(false).unwrap();
    /// ```
    pub fn set_autocommit(&mut self, val: bool) -> Result<()> {
        let payload = json!({"autocommit": val});
        self.set_attributes(payload)
    }

    /// Sets the query timeout
    ///
    /// ```
    /// # use exasol::exasol::{Connection};
    /// # use std::env;
    /// #
    /// # let dsn = format!("ws://{}", env::var("EXA_DSN").unwrap());
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = Connection::connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_query_timeout(60).unwrap();
    /// ```
    pub fn set_query_timeout(&mut self, val: usize) -> Result<()> {
        let payload = json!({"queryTimeout": val});
        self.set_attributes(payload)
    }

    /// Sets the currently open schema
    ///
    /// ```
    /// # use exasol::exasol::{Connection};
    /// # use std::env;
    /// #
    /// # let dsn = format!("ws://{}", env::var("EXA_DSN").unwrap());
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = Connection::connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_schema(&schema).unwrap();
    /// ```
    pub fn set_schema(&mut self, schema: &str) -> Result<()> {
        let payload = json!({"currentSchema": schema});
        self.set_attributes(payload)
    }

    /// Sets connection attributes
    fn set_attributes(&mut self, attr: Value) -> Result<()> {
        (*self.con).borrow_mut().set_attributes(attr)
    }
}

/// Connection implementation.
/// This requires a wrapper so that the interior mutability pattern can be used
/// for sharing the connection in multiple ResultSet structs.
/// They need to own it so that they can use it to further fetch data when iterated.
#[doc(hidden)]
#[derive(Debug)]
pub(crate) struct ConnectionImpl {
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    attr: HashMap<String, Value>,
}

impl Drop for ConnectionImpl {
    /// Implementing drop to properly get rid of the connection and its components
    fn drop(&mut self) {
        // Closing session on Exasol side
        self.do_request(json!({"command": "disconnect"})).ok();

        // Sending Message::Close frame
        self.ws.close(None).ok();

        // Reading the Message::Close frame sent by server
        self.ws.read_message().ok();

        // It is now safe to drop the socket
    }
}

impl ConnectionImpl {
    /// Attempts to create the websocket, authenticate in Exasol
    /// and read the connection attributes afterwards.
    pub(crate) fn connect(dsn: &str, schema: &str, user: &str, password: &str) -> Result<ConnectionImpl> {
        let (ws, _) = connect(Url::parse(dsn)?)?;
        let attr = HashMap::new();
        let mut con = ConnectionImpl { ws, attr };

        con.login(schema, user, password)?;
        con.get_attributes()?;

        Ok(con)
    }

    /// Sends the setAttributes request
    /// And calls get_attributes for consistency
    pub(crate) fn set_attributes(&mut self, attrs: Value) -> Result<()> {
        let mut payload = json!({"command": "setAttributes"});

        // Safe to unwrap here. We've just built this payload.
        payload
            .as_object_mut()
            .unwrap()
            .insert(String::from("attributes"), attrs);
        self.do_request(payload)?;

        self.get_attributes()?;
        Ok(())
    }

    /// Sends the payload to Exasol to execute one query
    /// and retrieves the first element from the resulted Vec<QueryResult>
    pub(crate) fn execute(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        query: &str,
    ) -> Result<QueryResult> {
        let payload = json!({"command": "execute", "sqlText": query});
        self._execute(con_impl, payload)
            .and_then(|mut v: Vec<QueryResult>| {
                if 0 < v.len() {
                    Ok(v.swap_remove(0))
                } else {
                    Err(Error::InvalidResponse("No result set found".to_owned()))
                }
            })
    }

    /// Sends the payload to Exasol to execute multiple queries
    /// and retrieves the results as a vector of QueryResult enums
    pub(crate) fn execute_batch(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        queries: Vec<&str>,
    ) -> Result<Vec<QueryResult>> {
        let payload = json!({"command": "executeBatch", "sqlTexts": queries});
        self._execute(con_impl, payload)
    }

    /// Closes a result set
    pub(crate) fn close_result_set(&mut self, handle: u16) -> Result<()> {
        let payload = json!({"command": "closeResultSet", "resultSetHandles": [handle]});
        self.do_request(payload)?;
        Ok(())
    }

    /// Ping the server and waits for a Pong frame
    pub(crate) fn ping(&mut self) -> Result<()>{
        self.ws.write_message(Message::Ping(vec![]))?;
        match self.ws.read_message()? {
            Message::Pong(_) => Ok(()),
            _ => Err(Error::InvalidResponse("Received frame different from Pong".to_string()))
        }
    }

    /// Sends a request, deserializes it to the given generic and returns the result
    pub(crate) fn get_data<T>(&mut self, payload: Value) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(serde_json::from_value::<T>(
            self.do_request(payload)?.ok_or(Error::InvalidResponse(
                "No response data received".to_owned(),
            ))?,
        )?)
    }

    /// Sends a request and waits for its response
    pub(crate) fn do_request(&mut self, payload: Value) -> Result<Option<Value>> {
        self.send(payload)
            .and_then(|_| self.recv())
            .and_then(|data| self.validate(data))
    }

    /// Gets the database results as Results (that's what they deserialize into)
    /// and consumes them to return a usable vector of QueryResult enums.
    fn _execute(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        payload: Value,
    ) -> Result<Vec<QueryResult>> {
        Ok(self.get_data::<Results>(payload)?.consume(con_impl))
    }

    /// Gets connection attributes from Exasol
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
                _ => continue,
            };
        }
    }

    /// Consumes the response JSON while checking the status
    /// If no error occurred, returns an Option with the responseData field (if found) or None
    ///
    /// Additionally, attributes can be returned from any request
    /// so this checks if any were returned and updates the stored attributes
    fn validate(&mut self, mut json_data: Value) -> Result<Option<Value>> {
        let status = ConnectionImpl::extract_value(&mut json_data, "status")?
            .as_str()
            .ok_or(Error::InvalidResponse(
                "Could not parse status field".to_owned(),
            ))?
            .to_owned();

        match status.as_str() {
            "ok" => {
                // Extract attributes, if found, and update the attributes map
                ConnectionImpl::extract_value(&mut json_data, "attributes")
                    .and_then(|v| Ok(serde_json::from_value::<HashMap<String, Value>>(v)?))
                    .map(|m| self.attr.extend(m))
                    .ok();

                // Extract the responseData and return it
                // If not found, return None.
                ConnectionImpl::extract_value(&mut json_data, "responseData")
                    .map_or(Ok(None), |v| Ok(Some(v)))
            }
            "error" => {
                let exception: Value = ConnectionImpl::extract_value(&mut json_data, "exception")?;
                let exc: ExaError = serde_json::from_value(exception)?;
                Err(Error::RequestError(exc))
            }
            _ => Err(Error::InvalidResponse(
                "Unknown status value not in JSON response".to_owned(),
            )),
        }
    }

    /// Attempts to extract a Value from another Value
    fn extract_value(data: &mut Value, field: &str) -> Result<Value> {
        Ok(data
            .get_mut(field)
            .ok_or(Error::InvalidResponse(format!(
                "Field '{}' not in JSON response",
                field
            )))?
            .take())
    }

    /// Gets the public key from Exasol
    /// Used during authentication for encrypting the password
    fn get_public_key(&mut self) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": 1});
        let pem = self
            .do_request(payload)?
            .ok_or(Error::InvalidResponse("No data received".to_owned()))
            .and_then(|mut data| ConnectionImpl::extract_value(&mut data, "publicKeyPem"))?
            .as_str()
            .ok_or(Error::InvalidResponse("Malformed public key".to_owned()))?
            .to_owned();

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)?)
    }

    /// Encrypts the password with the provided key
    fn encrypt_password(password: &str, public_key: RsaPublicKey) -> Result<String> {
        let mut rng = OsRng;
        let padding = PaddingScheme::new_pkcs1v15_encrypt();
        let enc_password =
            base64::encode(public_key.encrypt(&mut rng, padding, password.as_bytes())?);
        Ok(enc_password)
    }

    /// Authenticates to Exasol
    /// Called after the websocket is established
    fn login(&mut self, schema: &str, user: &str, password: &str) -> Result<Option<Value>> {
        let public_key = self.get_public_key()?;
        let enc_password = ConnectionImpl::encrypt_password(password, public_key)?;

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
