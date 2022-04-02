use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::TcpStream;
use std::rc::Rc;

use rsa::{pkcs1::FromRsaPublicKey, RsaPublicKey};
use serde::Serialize;
use serde_json::{json, Value};
use tungstenite::{stream::MaybeTlsStream, Message, WebSocket};
use url::Url;

use crate::con_opts::{ConOpts, ProtocolVersion};
use crate::error::{ConnectionError, DriverError, RequestError, Result};
use crate::query::{PreparedStatement, QueryResult};
use crate::response::{Attributes, Response, ResponseData};

#[cfg(feature = "flate2")]
use flate2::{read::ZlibDecoder, write::ZlibEncoder, Compression};
#[cfg(feature = "flate2")]
use std::io::Write;

// Convenience aliases
type ReqResult<T> = std::result::Result<T, RequestError>;
type ConResult = std::result::Result<WebSocket<MaybeTlsStream<TcpStream>>, ConnectionError>;

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
    let mut opts = ConOpts::new();
    opts.set_dsn(dsn);
    opts.set_user(user);
    opts.set_password(password);
    opts.set_schema(schema);

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
    /// let mut opts = ConOpts::new();
    /// opts.set_dsn(dsn);
    /// opts.set_user(user);
    /// opts.set_password(password);
    /// opts.set_schema(schema);
    ///
    /// let mut exa_con = Connection::new(opts).unwrap();
    /// ```
    #[inline]
    pub fn new(opts: ConOpts) -> Result<Connection> {
        Ok(Connection {
            con: Rc::new(RefCell::new(ConnectionImpl::new(opts)?)),
        })
    }

    /// Sends a query to the database and waits for the result.
    /// Returns a [QueryResult]
    ///
    /// ```
    /// # use exasol::{connect, QueryResult};
    /// # use exasol::error::Result;
    /// # use serde_json::Value;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let result = exa_con.execute("SELECT 1, 2 UNION ALL SELECT 1, 2;").unwrap();
    ///
    /// if let QueryResult::ResultSet(r) = result {
    ///     let x = r.collect::<Result<Vec<Vec<Value>>>>();
    ///         if let Ok(v) = x {
    ///             for row in v.iter() {
    ///                 // do stuff
    ///             }
    ///         }
    ///     }
    /// ```
    #[inline]
    pub fn execute<T>(&mut self, query: T) -> Result<QueryResult>
    where
        T: AsRef<str> + Serialize,
    {
        (*self.con).borrow_mut().execute(&self.con, &query)
    }

    /// Sends multiple queries to the database and waits for the result.
    /// Returns a [Vec<QueryResult>].
    ///
    /// ```
    /// # use exasol::{connect, QueryResult};
    /// # use exasol::error::Result;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let queries = vec!["SELECT 3", "SELECT 4"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// let queries = vec!["SELECT 3", "DELETE * FROM EXA_RUST_TEST WHERE 1=2"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// ```
    #[inline]
    pub fn execute_batch<T>(&mut self, queries: &[T]) -> Result<Vec<QueryResult>>
    where
        T: AsRef<str> + Serialize,
    {
        (*self.con).borrow_mut().execute_batch(&self.con, queries)
    }

    /// Creates a prepared statement of type [PreparedStatement].
    /// The prepared statement can then be executed with the provided data.
    /// ```
    /// # use exasol::{connect, QueryResult, PreparedStatement};
    /// # use exasol::error::Result;
    /// # use serde_json::json;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt: PreparedStatement = exa_con.prepare("SELECT 1 FROM (SELECT 1) TMP WHERE 1 = ?").unwrap();
    /// let data = vec![vec![json!(1)]];
    /// prepared_stmt.execute(&data).unwrap();
    /// ```
    #[inline]
    pub fn prepare<T>(&mut self, query: T) -> Result<PreparedStatement>
    where
        T: AsRef<str> + Serialize,
    {
        (*self.con).borrow_mut().prepare(&self.con, &query)
    }

    /// Ping the server and wait for a Pong frame
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
    #[inline]
    pub fn ping(&mut self) -> Result<()> {
        Ok((*self.con)
            .borrow_mut()
            .ping()
            .map_err(DriverError::RequestError)?)
    }

    /// Sets the fetch size in bytes when retrieving [ResultSet](crate::query::ResultSet) chunks
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
    /// exa_con.set_fetch_size(2 * 1024 * 1024);
    /// ```
    #[inline]
    pub fn set_fetch_size(&mut self, val: u32) {
        (*self.con).borrow_mut().driver_attr.fetch_size = val;
    }

    /// Sets whether the [ResultSet](crate::query::ResultSet) [Column](crate::response::Column)
    /// names will be implicitly cast to lowercase
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
    /// exa_con.set_lowercase_columns(false);
    /// ```
    #[inline]
    pub fn set_lowercase_columns(&mut self, flag: bool) {
        (*self.con).borrow_mut().driver_attr.lowercase_columns = flag;
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
    #[inline]
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
    #[inline]
    pub fn set_query_timeout(&mut self, val: usize) -> Result<()> {
        let payload = json!({ "queryTimeout": val });
        self.set_attributes(payload)
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
    #[inline]
    pub fn set_schema(&mut self, schema: &str) -> Result<()> {
        let payload = json!({ "currentSchema": schema });
        self.set_attributes(payload)
    }

    /// Sets connection attributes
    #[inline]
    fn set_attributes(&mut self, attr: Value) -> Result<()> {
        (*self.con).borrow_mut().set_attributes(attr)
    }
}

/// Connection implementation.
/// This requires a wrapper so that the interior mutability pattern can be used
/// for sharing the connection in multiple [ResultSet] and [PreparedStatement] structs.
/// They need to own it so that they can use it to further interact with the database
/// for row fetching or query execution.
#[doc(hidden)]
pub(crate) struct ConnectionImpl {
    pub(crate) driver_attr: DriverAttributes,
    exa_attr: HashMap<String, Value>,
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
    send: fn(&mut WebSocket<MaybeTlsStream<TcpStream>>, Value) -> ReqResult<()>,
    recv: fn(&mut WebSocket<MaybeTlsStream<TcpStream>>) -> ReqResult<Response>,
}

impl Drop for ConnectionImpl {
    /// Implementing drop to properly get rid of the connection and its components
    #[allow(unused)]
    fn drop(&mut self) {
        // Closing session on Exasol side
        self.do_request(json!({"command": "disconnect"}));

        // Sending Message::Close frame
        self.ws.close(None);

        // Reading the response sent by the server until Error:ConnectionClosed occurs.
        // We should typically get a Message::Close frame and then the error.
        while self.ws.read_message().is_ok() {}

        // It is now safe to drop the socket
    }
}

impl Debug for ConnectionImpl {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_attr = self
            .exa_attr
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
    ///
    /// We'll get the list of IP addresses resulted from parsing and resolving the DSN
    /// Then loop through all of them (max once each) until a connection is established.
    /// Login is attempted afterwards.
    ///
    /// If all options were exhausted and a connection could not be established
    /// an error is returned.
    pub(crate) fn new(opts: ConOpts) -> Result<ConnectionImpl> {
        let ws = Self::try_websocket_from_opts(&opts).map_err(DriverError::ConnectionError)?;
        let exa_attr = HashMap::new();

        let driver_attr = DriverAttributes {
            fetch_size: opts.get_fetch_size(),
            lowercase_columns: opts.get_lowercase_columns(),
        };

        let mut con = Self {
            driver_attr,
            exa_attr,
            ws,
            send,
            recv,
        };

        con.login(opts)?;
        con.get_attributes()?;
        Ok(con)
    }

    /// Sends an setAttributes request to Exasol
    /// And calls get_attributes for consistency
    pub(crate) fn set_attributes(&mut self, attrs: Value) -> Result<()> {
        let payload = json!({"command": "setAttributes", "attributes": attrs});
        self.do_request(payload)?;
        self.get_attributes()?;
        Ok(())
    }

    /// Sends the payload to Exasol to execute one query
    /// and retrieves the first element from the resulted Vec<QueryResult>
    pub(crate) fn execute<T>(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        query: &T,
    ) -> Result<QueryResult>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "execute", "sqlText": query});
        self.exec_and_get_first(con_impl, payload)
    }

    /// Sends the payload to Exasol to execute multiple queries
    /// and retrieves the results as a vector of QueryResult enums
    pub(crate) fn execute_batch<T>(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        queries: &[T],
    ) -> Result<Vec<QueryResult>>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "executeBatch", "sqlTexts": queries});
        self.exec_with_results(con_impl, payload)
    }

    /// Sends a request for execution and returns the first [QueryResult] received.
    pub(crate) fn exec_and_get_first(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        payload: Value,
    ) -> Result<QueryResult> {
        self.exec_with_results(con_impl, payload)
            .and_then(|mut v: Vec<QueryResult>| {
                if v.is_empty() {
                    Err(
                        DriverError::RequestError(RequestError::InvalidResponse("result sets"))
                            .into(),
                    )
                } else {
                    Ok(v.swap_remove(0))
                }
            })
    }

    /// Creates a prepared statement
    pub(crate) fn prepare<T>(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        query: &T,
    ) -> Result<PreparedStatement>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "createPreparedStatement", "sqlText": query});
        self.get_resp_data(payload)
            .and_then(|r| r.try_to_prepared_stmt(con_impl))
    }

    /// Closes a result set
    #[inline]
    pub(crate) fn close_result_set(&mut self, handle: u16) -> Result<()> {
        let payload = json!({"command": "closeResultSet", "resultSetHandles": [handle]});
        self.do_request(payload)?;
        Ok(())
    }

    /// Closes a prepared statement
    #[inline]
    pub(crate) fn close_prepared_stmt(&mut self, handle: usize) -> Result<()> {
        let payload = json!({"command": "closePreparedStatement", "statementHandle": handle});
        self.do_request(payload)?;
        Ok(())
    }

    /// Ping the server and waits for a Pong frame
    pub(crate) fn ping(&mut self) -> ReqResult<()> {
        self.ws.write_message(Message::Ping(vec![]))?;
        match self.ws.read_message()? {
            Message::Pong(_) => Ok(()),
            _ => Err(RequestError::InvalidResponse("pong frame")),
        }
    }

    /// Returns response data from a request
    pub(crate) fn get_resp_data(&mut self, payload: Value) -> Result<ResponseData> {
        self.do_request(payload)?.ok_or_else(|| {
            DriverError::RequestError(RequestError::InvalidResponse("response data")).into()
        })
    }

    /// Sends a request and waits for its response
    pub(crate) fn do_request(&mut self, payload: Value) -> Result<Option<ResponseData>> {
        let resp = self
            .send(payload)
            .and_then(|_| self.recv())
            .map_err(DriverError::RequestError)?;

        let (data, attr): (Option<ResponseData>, Option<Attributes>) = resp.try_into()?;
        if let Some(attributes) = attr {
            self.exa_attr.extend(attributes.map)
        }

        Ok(data)
    }

    /// Gets the database results as [crate::query_result::Results]
    /// (that's what they deserialize into) and consumes them
    /// to return a usable vector of [QueryResult] enums.
    pub(crate) fn exec_with_results(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        payload: Value,
    ) -> Result<Vec<QueryResult>> {
        let lc = self.driver_attr.lowercase_columns;
        self.get_resp_data(payload)
            .and_then(|r| r.try_to_query_results(con_impl, lc))
    }

    #[inline]
    fn send(&mut self, payload: Value) -> ReqResult<()> {
        (self.send)(&mut self.ws, payload)
    }

    #[inline]
    fn recv(&mut self) -> ReqResult<Response> {
        (self.recv)(&mut self.ws)
    }

    /// Attempts to create a Websocket for the given [ConOpts]
    fn try_websocket_from_opts(opts: &ConOpts) -> ConResult {
        let addresses = opts.parse_dsn()?;
        let ws_prefix = opts.get_ws_prefix();
        let mut try_count = addresses.len();
        let mut addr_iter = addresses.into_iter();

        loop {
            try_count -= 1;

            if let Some(addr) = addr_iter.next() {
                let url = format!("{}://{}", ws_prefix, addr);
                let res = Self::try_websocket_from_url(&url);

                match res {
                    Ok(ws) => break Ok(ws),
                    Err(e) => {
                        if try_count == 0 {
                            break Err(e);
                        }
                    }
                }
            } else {
                break Err(ConnectionError::InvalidDSN);
            }
        }
    }

    /// Attempts to create a websocket for the given URL
    fn try_websocket_from_url(url: &str) -> ConResult {
        let url = Url::parse(url)?;
        let (ws, _) = tungstenite::connect(url)?;
        Ok(ws)
    }

    /// Gets connection attributes from Exasol
    fn get_attributes(&mut self) -> Result<()> {
        let payload = json!({"command": "getAttributes"});
        self.do_request(payload)?;
        Ok(())
    }

    /// Gets the public key from Exasol
    /// Used during authentication for encrypting the password
    fn get_public_key(&mut self, protocol_version: ProtocolVersion) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": protocol_version});

        let pem = self
            .get_resp_data(payload)
            .and_then(|p| p.try_to_public_key_string())?;

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)
            .map_err(ConnectionError::PKCSError)
            .map_err(DriverError::ConnectionError)?)
    }

    /// Authenticates to Exasol
    /// Called after the websocket is established
    ///
    /// Login is always uncompressed. If compression is enabled, it is set afterwards.
    fn login(&mut self, opts: ConOpts) -> Result<()> {
        // Should we use compression?
        #[cfg(feature = "flate2")]
        let compress = opts.get_compression();

        // Encrypt password using server's public key
        let key = self.get_public_key(opts.get_protocol_version())?;
        let payload = opts.into_value(key).map_err(DriverError::ConnectionError)?;

        // Send login request
        self.do_request(payload)?;

        // Setting compression functions if needed
        #[cfg(feature = "flate2")]
        if compress {
            self.send = compressed_send;
            self.recv = compressed_recv;
        }

        Ok(())
    }
}

/// Struct holding driver related attributes
/// unrelated to the Exasol connection itself
pub(crate) struct DriverAttributes {
    pub(crate) fetch_size: u32,
    pub(crate) lowercase_columns: bool,
}

// Websocket communication functions, regular and compressed.
#[inline]
fn send(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>, payload: Value) -> ReqResult<()> {
    Ok(ws.write_message(Message::Text(payload.to_string()))?)
}

#[inline]
fn recv(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> ReqResult<Response> {
    loop {
        break match ws.read_message()? {
            Message::Text(resp) => Ok(serde_json::from_str::<Response>(&resp)?),
            Message::Binary(resp) => Ok(serde_json::from_slice::<Response>(&resp)?),
            _ => continue,
        };
    }
}

#[inline]
#[cfg(feature = "flate2")]
fn compressed_send(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>, payload: Value) -> ReqResult<()> {
    let mut enc = ZlibEncoder::new(Vec::new(), Compression::default());
    enc.write_all(payload.to_string().as_bytes())?;
    Ok(ws.write_message(Message::Binary(enc.finish()?))?)
}

#[inline]
#[cfg(feature = "flate2")]
fn compressed_recv(ws: &mut WebSocket<MaybeTlsStream<TcpStream>>) -> ReqResult<Response> {
    loop {
        break match ws.read_message()? {
            Message::Text(resp) => Ok(serde_json::from_reader(ZlibDecoder::new(resp.as_bytes()))?),
            Message::Binary(resp) => {
                Ok(serde_json::from_reader(ZlibDecoder::new(resp.as_slice()))?)
            }
            _ => continue,
        };
    }
}
