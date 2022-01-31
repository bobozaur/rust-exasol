use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::TcpStream;
use std::rc::Rc;

use rsa::{pkcs1::FromRsaPublicKey, RsaPublicKey};
use serde::Deserialize;
use serde_json::{json, Value};
use tungstenite::{stream::MaybeTlsStream, Message, WebSocket};
use url::Url;

use crate::con_opts::{ConOpts, ProtocolVersion};
use crate::error::{ConnectionError, Error, ExaError, RequestError, Result};
use crate::query_result::{QueryResult, Results};

/// Convenience function to quickly connect using default options.
/// Returns a [Connection] set using the default [ConOpts]
/// ```
/// use exasol::connect;
/// use std::env;
///
/// let dsn = env::var("EXA_DSN").unwrap();
/// let schema = env::var("EXA_SCHEMA").unwrap();
/// let user = env::var("EXA_USER").unwrap();
/// let password = env::var("EXA_PASSWORD").unwrap();
///
/// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
/// ```
pub fn connect(dsn: &str, schema: &str, user: &str, password: &str) -> Result<Connection> {
    let opts = ConOpts {
        dsn: dsn.to_owned(),
        schema: schema.to_owned(),
        user: user.to_owned(),
        password: password.to_owned(),
        ..ConOpts::default()
    };

    Connection::new(opts)
}

/// The [Connection] struct will be what we use to interact with the database.
// A Rc<RefCell> is being used internally so that the connection can be shared
// with all the result sets retrieved (needed for fetching row chunks).
pub struct Connection {
    con: Rc<RefCell<ConnectionImpl>>,
}

impl Debug for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self.con.borrow())
    }
}

impl Connection {
    /// Creates the [Connection] using the provided [ConOpts].
    ///
    /// ```
    /// use std::env;
    /// use exasol::{ConOpts, Connection};
    ///
    /// let dsn = env::var("EXA_DSN").unwrap();
    /// let schema = env::var("EXA_SCHEMA").unwrap();
    /// let user = env::var("EXA_USER").unwrap();
    /// let password = env::var("EXA_PASSWORD").unwrap();
    ///
    /// let opts = ConOpts {dsn, user, password, schema, ..ConOpts::default()};
    /// let mut exa_con = Connection::new(opts).unwrap();
    /// ```
    pub fn new(opts: ConOpts) -> Result<Connection> {
        Ok(Connection {
            con: Rc::new(RefCell::new(ConnectionImpl::new(opts)?)),
        })
    }

    /// Sends a query to the database and waits for the result.
    /// Returns a [QueryResult]
    ///
    /// ```
    /// # use exasol::{connect, Row, QueryResult};
    /// # use exasol::error::Result;
    /// # use std::env;
    ///
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let result = exa_con.execute("SELECT * FROM EXA_ALL_OBJECTS LIMIT 2000;").unwrap();
    ///
    /// if let QueryResult::ResultSet(r) = result {
    ///     let x = r.take(50).collect::<Result<Vec<Row>>>();
    ///         if let Ok(v) = x {
    ///             for row in v.iter() {
    ///                 // do stuff
    ///             }
    ///         }
    ///     }
    /// ```
    pub fn execute(&mut self, query: &str) -> Result<QueryResult> {
        (*self.con).borrow_mut().execute(&self.con, query)
    }

    /// Sends multiple queries to the database and waits for the result.
    /// Returns a [Vec<QueryResult>].
    ///
    /// ```
    /// # use exasol::{connect, Row, QueryResult};
    /// # use exasol::error::Result;
    /// # use std::env;
    ///
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let results: Vec<QueryResult> = exa_con.execute_batch(vec!("SELECT 3", "SELECT 4")).unwrap();
    /// let results: Vec<QueryResult> = exa_con.execute_batch(vec!("SELECT 3", "DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2")).unwrap();
    /// ```
    pub fn execute_batch(&mut self, queries: Vec<&str>) -> Result<Vec<QueryResult>> {
        (*self.con).borrow_mut().execute_batch(&self.con, queries)
    }

    /// Ping the server and wait for Pong frame
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.ping().unwrap();
    /// ```
    pub fn ping(&mut self) -> Result<()> {
        (*self.con).borrow_mut().ping()
    }

    /// Sets autocommit mode On or Off
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_autocommit(false).unwrap();
    /// ```
    pub fn set_autocommit(&mut self, val: bool) -> Result<()> {
        let payload = json!({ "autocommit": val });
        self.set_attributes(payload)
    }

    /// Sets the query timeout
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_query_timeout(60).unwrap();
    /// ```
    pub fn set_query_timeout(&mut self, val: usize) -> Result<()> {
        let payload = json!({ "queryTimeout": val });
        self.set_attributes(payload)
    }

    /// Sets the fetch size in bytes when retrieving [crate::ResultSet] chunks
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// // Size is in bytes
    /// exa_con.set_fetch_size(2 * 1024 * 1024).unwrap();
    /// ```
    pub fn set_fetch_size(&mut self, val: usize) -> Result<()> {
        (*self.con)
            .borrow_mut()
            .attr
            .insert("fetch_size".to_owned(), json!(val));
        Ok(())
    }

    /// Sets the currently open schema
    ///
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// exa_con.set_schema(&schema).unwrap();
    /// ```
    pub fn set_schema(&mut self, schema: &str) -> Result<()> {
        let payload = json!({ "currentSchema": schema });
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
pub(crate) struct ConnectionImpl {
    pub(crate) attr: HashMap<String, Value>,
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
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

impl Debug for ConnectionImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_attr = self
            .attr
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<String>>()
            .join("\n");
        write!(f, "active: {}\n{}", self.ws.can_write(), str_attr)
    }
}

impl ConnectionImpl {
    /// Attempts to create the websocket, authenticate in Exasol
    /// and read the connection attributes afterwards.
    pub(crate) fn new(opts: ConOpts) -> Result<ConnectionImpl> {
        // Get list of IP addresses after parsing and resolving DSN
        let addresses = opts.parse_dsn()?;

        // Will loop through all of them until a connection is established
        let mut try_count = addresses.len();
        let mut addr_iter = addresses.into_iter();

        let ws = loop {
            try_count -= 1;

            if let Some(addr) = addr_iter.next() {
                let url = format!("ws://{}", addr);

                match Self::create_websocket(&url) {
                    Err(e) => {
                        if try_count <= 0 {
                            break Err(e);
                        }
                    }
                    Ok(ws) => {
                        break Ok(ws);
                    }
                }
            } else {
                // This is only reached if the iterator is empty, hence the DSN is invalid
                // as it could not be resolved to any IP
                break Err(Error::ConnectionError(ConnectionError::InvalidDSN));
            }
        }?;

        let attr = HashMap::new();
        let mut con = ConnectionImpl { ws, attr };

        con.login(opts)?;
        con.get_attributes()?;

        return Ok(con);
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
                    Err(RequestError::InvalidResponse(
                        "No result set found".to_owned(),
                    ))?
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
    pub(crate) fn ping(&mut self) -> Result<()> {
        self.ws.write_message(Message::Ping(vec![]))?;
        match self.ws.read_message()? {
            Message::Pong(_) => Ok(()),
            _ => Err(RequestError::InvalidResponse(
                "Received frame different from Pong".to_string(),
            ))?,
        }
    }

    /// Sends a request, deserializes it to the given generic and returns the result
    pub(crate) fn get_data<T>(&mut self, payload: Value) -> Result<T>
    where
        T: for<'de> Deserialize<'de>,
    {
        Ok(serde_json::from_value::<T>(
            self.do_request(payload)?
                .ok_or(RequestError::InvalidResponse(
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

    /// Attempts to create a websocket for the given URL
    fn create_websocket(url: &str) -> Result<WebSocket<MaybeTlsStream<TcpStream>>> {
        Ok(Url::parse(&url)
            .map_err(|e| ConnectionError::DSNParseError(e))
            .and_then(|u| tungstenite::connect(u).map_err(|e| ConnectionError::WebsocketError(e)))
            .and_then(|(ws, _)| Ok(ws))?)
    }

    /// Gets the database results as [crate::query_result::Results] (that's what they deserialize into)
    /// and consumes them to return a usable vector of QueryResult enums.
    fn _execute(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        payload: Value,
    ) -> Result<Vec<QueryResult>> {
        Ok(self
            .get_data::<Results>(payload)
            .map_err(|e| match e {
                Error::RequestError(RequestError::ExaError(err)) => Error::QueryError(err),
                _ => e,
            })?
            .parse(con_impl))
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
        let status = Self::extract_value(&mut json_data, "status")?
            .as_str()
            .ok_or(RequestError::InvalidResponse(
                "Returned status field is not a string".to_owned(),
            ))?
            .to_owned();

        match status.as_str() {
            "ok" => {
                // Extract attributes, if found, and update the attributes map
                Self::extract_value(&mut json_data, "attributes")
                    .and_then(|v| Ok(serde_json::from_value::<HashMap<String, Value>>(v)?))
                    .map(|m| self.attr.extend(m))
                    .ok();

                // Extract the responseData and return it
                // If not found, return None.
                Self::extract_value(&mut json_data, "responseData")
                    .map_or(Ok(None), |v| Ok(Some(v)))
            }
            "error" => {
                let exception: Value = Self::extract_value(&mut json_data, "exception")?;
                let exc: ExaError = serde_json::from_value(exception)?;
                Err(RequestError::ExaError(exc))?
            }
            _ => Err(RequestError::InvalidResponse(
                "Unknown status value in JSON response".to_owned(),
            ))?,
        }
    }

    /// Attempts to extract a Value from another Value
    fn extract_value(data: &mut Value, field: &str) -> Result<Value> {
        Ok(data
            .get_mut(field)
            .ok_or(RequestError::InvalidResponse(format!(
                "Field '{}' not in JSON response",
                field
            )))?
            .take())
    }

    /// Gets the public key from Exasol
    /// Used during authentication for encrypting the password
    fn get_public_key(&mut self, protocol_version: &ProtocolVersion) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": protocol_version});
        let pem = self
            .do_request(payload)?
            .ok_or(RequestError::InvalidResponse("No data received".to_owned()))
            .map_err(From::from)
            .and_then(|mut data| Self::extract_value(&mut data, "publicKeyPem"))?
            .as_str()
            .ok_or(RequestError::InvalidResponse(
                "Malformed public key".to_owned(),
            ))?
            .to_owned();

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)?)
    }

    /// Authenticates to Exasol
    /// Called after the websocket is established
    fn login(&mut self, opts: ConOpts) -> Result<Option<Value>> {
        // Encrypt password using server's public key
        let enc_password = self
            .get_public_key(&opts.protocol_version)
            .and_then(|k| opts.encrypt_password(k))?;

        // Retain the result set fetch size in bytes
        self.attr
            .insert("fetch_size".to_owned(), json!(opts.fetch_size));

        let payload = json!({
        "username": opts.user,
        "password": enc_password,
        "driverName": &opts.client_name,
        "clientName": &opts.client_name,
        "clientVersion": opts.client_version,
        "clientOs": opts.client_os,
        "clientRuntime": "Rust",
        "useCompression": opts.use_compression,
        "attributes": {
                    "currentSchema": opts.schema,
                    "autocommit": opts.autocommit,
                    "queryTimeout": opts.query_timeout
                    }
        });

        self.do_request(payload).map_err(|e| match e {
            Error::RequestError(RequestError::ExaError(err)) => {
                ConnectionError::AuthError(err).into()
            }
            _ => e,
        })
    }
}
