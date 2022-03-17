use std::cell::RefCell;
use std::collections::HashMap;
use std::fmt::{Debug, Formatter};
use std::net::TcpStream;
use std::rc::Rc;

use rsa::{pkcs1::FromRsaPublicKey, RsaPublicKey};
use serde_json::{json, Value};
use tungstenite::{stream::MaybeTlsStream, Message, WebSocket};
use url::Url;

use crate::con_opts::{ConOpts, ProtocolVersion};
use crate::constants::{NOT_PONG, NO_PUBLIC_KEY, NO_RESPONSE_DATA, NO_RESULT_SET, WS_STR};
use crate::error::{ConnectionError, Error, RequestError, Result};
use crate::query::{PreparedStatement, Query, QueryResult};
use crate::response::{Response, ResponseData};

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

    /// Sends a type implementing [Query] to the database and waits for the result.
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
    pub fn execute<T>(&mut self, query: T) -> Result<QueryResult>
    where
        T: Query,
    {
        (*self.con)
            .borrow_mut()
            .execute(&self.con, query.to_query())
    }

    /// Sends a vector of type implementing [Query] to the database and waits for the result.
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
    /// let queries = vec!["SELECT 3", "SELECT 4"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// let queries = vec!["SELECT 3", "DELETE * FROM DIM_SIMPLE_DATE WHERE 1=2"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// ```
    pub fn execute_batch<T>(&mut self, queries: &Vec<T>) -> Result<Vec<QueryResult>>
    where
        T: Query,
    {
        let queries = queries.iter().map(|q| q.to_query()).collect();
        (*self.con).borrow_mut().execute_batch(&self.con, &queries)
    }

    /// Creates a prepared statement of type [PreparedStatement].
    /// The prepared statement can then be executed with the provided data.
    /// ```
    /// # use exasol::{connect, Row, QueryResult, PreparedStatement};
    /// # use exasol::error::Result;
    /// # use serde_json::json;
    /// # use std::env;
    ///
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt: PreparedStatement = exa_con.prepare("SELECT 1 FROM (SELECT 1) TMP WHERE 1 = ?").unwrap();
    /// prepared_stmt.execute(vec![vec![json!(1)]]).unwrap();
    /// ```
    pub fn prepare<T>(&mut self, query: T) -> Result<PreparedStatement>
    where
        T: Query,
    {
        (*self.con)
            .borrow_mut()
            .prepare(&self.con, query.to_query())
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
        (*self.con).borrow_mut().set_con_attr(attr)
    }
}

/// Connection implementation.
/// This requires a wrapper so that the interior mutability pattern can be used
/// for sharing the connection in multiple ResultSet structs.
/// They need to own it so that they can use it to further fetch data when iterated.
#[doc(hidden)]
pub(crate) struct ConnectionImpl {
    attr: HashMap<String, Value>,
    ws: WebSocket<MaybeTlsStream<TcpStream>>,
}

impl Drop for ConnectionImpl {
    /// Implementing drop to properly get rid of the connection and its components
    fn drop(&mut self) {
        // Closing session on Exasol side
        self.do_request(json!({"command": "disconnect"})).ok();

        // Sending Message::Close frame
        self.ws.close(None).ok();

        // Reading the response sent by the server until Error:ConnectionClosed occurs.
        // We should typically get a Message::Close frame and then the error.
        loop {
            match self.ws.read_message().ok() {
                Some(_) => {}
                None => break,
            };
        }

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
    ///
    /// We'll get the list of IP addresses resulted from parsing and resolving the DSN
    /// Then loop through all of them (max once each) until a connection is established.
    /// Login is attempted afterwards.
    ///
    /// If all options were exhausted and a connection could not be established
    /// an error is returned.
    pub(crate) fn new(opts: ConOpts) -> Result<ConnectionImpl> {
        let addresses = opts.parse_dsn()?;
        let mut try_count = addresses.len();
        let mut addr_iter = addresses.into_iter();

        let ws = loop {
            try_count -= 1;

            if let Some(addr) = addr_iter.next() {
                let url = format!("{}://{}", WS_STR, addr);
                let res = Self::create_websocket(&url);

                match res {
                    Ok(ws) => break Ok(ws),
                    Err(e) => {
                        if try_count <= 0 {
                            break Err(e);
                        }
                    }
                }
            } else {
                break Err(Error::ConnectionError(ConnectionError::InvalidDSN));
            }
        }?;

        let mut con = ConnectionImpl {
            ws,
            attr: HashMap::new(),
        };

        con.login(opts)?;
        con.get_con_attr()?;

        return Ok(con);
    }

    /// Sends the setAttributes request
    /// And calls get_attributes for consistency
    pub(crate) fn set_con_attr(&mut self, attrs: Value) -> Result<()> {
        let payload = json!({"command": "setAttributes", "attributes": attrs});
        self.do_request(payload)?;
        self.get_con_attr()?;
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
        self.exec_and_get_first(con_impl, payload)
    }

    /// Sends the payload to Exasol to execute multiple queries
    /// and retrieves the results as a vector of QueryResult enums
    pub(crate) fn execute_batch(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        queries: &Vec<&str>,
    ) -> Result<Vec<QueryResult>> {
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
                    Err(NO_RESULT_SET.into())
                } else {
                    Ok(v.swap_remove(0))
                }
            })
    }

    /// Creates a prepared statement
    pub(crate) fn prepare(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        query: &str,
    ) -> Result<PreparedStatement> {
        let payload = json!({"command": "createPreparedStatement", "sqlText": query});
        self.get_resp_data(payload)
            .map_err(|e| e.conv_query_error())
            .and_then(|r| r.try_to_prepared_stmt(con_impl))
    }

    /// Closes a result set
    pub(crate) fn close_result_set(&mut self, handle: u16) -> Result<()> {
        let payload = json!({"command": "closeResultSet", "resultSetHandles": [handle]});
        self.do_request(payload)?;
        Ok(())
    }

    /// Closes a prepared statement
    pub(crate) fn close_prepared_stmt(&mut self, handle: usize) -> Result<()> {
        let payload = json!({"command": "closePreparedStatement", "statementHandle": handle});
        self.do_request(payload)?;
        Ok(())
    }

    /// Ping the server and waits for a Pong frame
    pub(crate) fn ping(&mut self) -> Result<()> {
        self.ws.write_message(Message::Ping(vec![]))?;
        match self.ws.read_message()? {
            Message::Pong(_) => Ok(()),
            _ => Err(NOT_PONG.into()),
        }
    }

    /// Returns response data from a request
    pub(crate) fn get_resp_data(&mut self, payload: Value) -> Result<ResponseData> {
        self.do_request(payload)?.ok_or(NO_RESPONSE_DATA.into())
    }

    /// Sends a request and waits for its response
    pub(crate) fn do_request(&mut self, payload: Value) -> Result<Option<ResponseData>> {
        let (data, attr) = Result::from(self.send(payload).and_then(|_| self.recv())?)?;
        if let Some(attributes) = attr {
            self.attr.extend(attributes.map)
        }
        Ok(data)
    }

    pub(crate) fn get_attr(&mut self, attr_name: &str) -> Option<&Value> {
        self.attr.get(attr_name)
    }

    pub(crate) fn set_attr(&mut self, attr_name: String, val: Value) {
        self.attr.insert(attr_name, val);
    }

    /// Attempts to create a websocket for the given URL
    fn create_websocket(url: &str) -> Result<WebSocket<MaybeTlsStream<TcpStream>>> {
        Url::parse(&url)
            .map_err(From::from)
            .and_then(|u| tungstenite::connect(u).map_err(From::from))
            .and_then(|(ws, _)| Ok(ws))
    }

    /// Gets the database results as [crate::query_result::Results]
    /// (that's what they deserialize into) and consumes them
    /// to return a usable vector of [QueryResult] enums.
    pub(crate) fn exec_with_results(
        &mut self,
        con_impl: &Rc<RefCell<ConnectionImpl>>,
        payload: Value,
    ) -> Result<Vec<QueryResult>> {
        self.get_resp_data(payload)
            .map_err(|e| e.conv_query_error())
            .and_then(|r| r.try_to_query_results(con_impl))
    }

    /// Gets connection attributes from Exasol
    fn get_con_attr(&mut self) -> Result<()> {
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
    fn recv(&mut self) -> Result<Response> {
        loop {
            break match self.ws.read_message()? {
                Message::Text(resp) => Ok(serde_json::from_str::<Response>(&resp)?),
                Message::Binary(resp) => Ok(serde_json::from_slice::<Response>(&resp)?),
                _ => continue,
            };
        }
    }

    /// Gets the public key from Exasol
    /// Used during authentication for encrypting the password
    fn get_public_key(&mut self, protocol_version: &ProtocolVersion) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": protocol_version});

        let pem = self
            .do_request(payload)?
            .and_then(|p| p.to_public_key())
            .ok_or(NO_PUBLIC_KEY)?;

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)?)
    }

    /// Authenticates to Exasol
    /// Called after the websocket is established
    fn login(&mut self, opts: ConOpts) -> Result<()> {
        // Retain the result set fetch size in bytes
        self.set_attr("fetch_size".to_owned(), json!(opts.fetch_size));

        // Encrypt password using server's public key
        let key = self.get_public_key(&opts.protocol_version)?;
        let payload = opts.to_value(key)?;

        self.do_request(payload).map_err(|e| match e {
            Error::RequestError(RequestError::ExaError(err)) => {
                ConnectionError::AuthError(err).into()
            }
            _ => e,
        })?;

        Ok(())
    }
}
