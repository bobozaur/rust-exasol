use crate::con_opts::ConOpts;
use crate::error::{ConnectionError, DriverError, RequestError, Result};
use compress::MaybeCompressedWs;
use driver_attr::DriverAttributes;
use exa_ws::ExaWebSocket;
pub use http_transport::HttpTransportOpts;
use http_transport::{HttpExportJob, HttpImportJob, HttpTransportJob};
use lazy_regex::regex;
use regex::Captures;
use response::{Attributes, FetchedData, QueryResultDe, Response, ResponseData, ResultSetDe};
pub use response::{Column, DataType, ExaError, PreparedStatement};
use result::ResultSetIter;
pub use result::{QueryResult, ResultSet};
pub use row::deserialize_as_seq;
use row::{to_col_major, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::net::TcpStream;
use std::ops::ControlFlow;
use tungstenite::{stream::MaybeTlsStream, Message, WebSocket};
use url::Url;

mod compress;
mod driver_attr;
mod exa_ws;
mod http_transport;
mod response;
mod result;
mod row;

// Convenience aliases
type ReqResult<T> = std::result::Result<T, RequestError>;
type WsAddr = (WebSocket<MaybeTlsStream<TcpStream>>, String, u16);

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
pub fn connect<T>(dsn: T, schema: T, user: T, password: T) -> Result<Connection>
where
    T: Into<String>,
{
    let mut opts = ConOpts::new();
    opts.set_dsn(dsn);
    opts.set_user(user);
    opts.set_password(password);
    opts.set_schema(Some(schema));

    Connection::new(opts)
}

/// The [Connection] struct will be what we use to interact with the database.
#[doc(hidden)]
pub struct Connection {
    driver_attr: DriverAttributes,
    ws: ExaWebSocket,
    rs_handles: HashSet<u16>,
    ps_handles: HashSet<u16>,
}

impl Drop for Connection {
    /// Implementing drop to properly get rid of the connection and its components
    fn drop(&mut self) {
        // Closes result sets and prepared statements
        let ps_handles = std::mem::take(&mut self.rs_handles);
        self.close_results_impl(ps_handles).ok();

        std::mem::take(&mut self.ps_handles)
            .into_iter()
            .for_each(|h| drop(self.close_prepared_stmt_impl(h)));

        // Closing session on Exasol side
        self.do_request(json!({"command": "disconnect"})).ok();

        // Sending Message::Close frame
        self.ws.close().ok();

        // Reading the response sent by the server until Error:ConnectionClosed occurs.
        // We should typically get a Message::Close frame and then the error.
        while self.ws.read_message().is_ok() {}

        // It is now safe to drop the socket
    }
}

impl Debug for Connection {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let str_attr = self
            .ws
            .exa_attr()
            .iter()
            .map(|(k, v)| format!("{}: {}", k, v))
            .collect::<Vec<String>>()
            .join("\n");
        write!(
            f,
            "active: {}\n{}\n{}",
            self.ws.can_write(),
            str_attr,
            self.driver_attr
        )
    }
}

impl Connection {
    /// Creates the [Connection] using the provided [ConOpts].
    /// If a range is provided as DSN, connection attempts are made for max each possible
    /// options, until one is successful.
    ///
    /// # Errors
    /// If all options were exhausted and a connection could not be established
    /// an error is returned.
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
    /// opts.set_schema(None);
    ///
    /// let mut exa_con = Connection::new(opts).unwrap();
    /// ```
    pub fn new(opts: ConOpts) -> Result<Connection> {
        let (ws, addr, port) =
            Self::try_websocket_from_opts(&opts).map_err(DriverError::ConnectionError)?;

        let driver_attr = DriverAttributes {
            port,
            server_ip: addr,
            fetch_size: opts.fetch_size(),
            lowercase_columns: opts.lowercase_columns(),
        };

        let ws = ExaWebSocket::new(ws, opts)?;

        let mut con = Self {
            rs_handles: HashSet::new(),
            ps_handles: HashSet::new(),
            driver_attr,
            ws,
        };

        // Get connection attributes from database
        con.get_attributes()?;
        Ok(con)
    }

    /// Returns the fetch size of [ResultSet] chunks
    /// ```
    /// # use exasol::connect;
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// # let mut exa_con = connect(dsn, schema, user, password).unwrap();
    /// // Size is in bytes
    /// assert_eq!(exa_con.fetch_size(), 2 * 1024 * 1024);
    /// ```
    #[inline]
    pub fn fetch_size(&self) -> usize {
        self.driver_attr.fetch_size
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
    pub fn set_fetch_size(&mut self, val: usize) {
        self.driver_attr.fetch_size = val;
    }

    /// Returns whether the result set columns are implicitly converted to lower case
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
    /// assert_eq!(exa_con.lowercase_columns(), true);
    /// ```
    #[inline]
    pub fn lowercase_columns(&self) -> bool {
        self.driver_attr.lowercase_columns
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
        self.driver_attr.lowercase_columns = flag;
    }

    /// Returns whether autocommit is enabled
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
    /// assert_eq!(exa_con.autocommit(), true);
    /// ```
    // This should not ever fail to unwrap but
    // we'll return the default set by the driver just in case
    // An alternative would maybe be setting it if it does not exist,
    // but there's clearly some problem if that happens as it's always
    // in the ConOpts.
    #[inline]
    pub fn autocommit(&self) -> bool {
        self.ws
            .exa_attr()
            .get("autocommit")
            .and_then(|v| v.as_bool())
            .unwrap_or(true)
    }

    /// Sets autocommit mode On or Off. The default is On.
    /// Turning it off means transaction mode is enabled and explicit `COMMIT` or `ROLLBACK`
    /// statements have to be executed.
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

    /// Returns the current query timeout
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
    /// assert_eq!(exa_con.query_timeout(), 0);
    /// ```
    // This should not ever fail to unwrap but
    // we'll return the default set by the driver just in case
    // An alternative would maybe be setting it if it does not exist,
    // but there's clearly some problem if that happens as it's always
    // in the ConOpts.
    #[inline]
    pub fn query_timeout(&self) -> u64 {
        self.ws
            .exa_attr()
            .get("queryTimeout")
            .and_then(|v| v.as_u64())
            .unwrap_or(0)
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
    pub fn set_query_timeout(&mut self, val: u64) -> Result<()> {
        let payload = json!({ "queryTimeout": val });
        self.set_attributes(payload)
    }

    /// Returns the currently open schema, if any
    /// This can be NULL, hence an Option is returned
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
    /// assert_eq!(exa_con.schema(), Some(schema));
    /// ```
    #[inline]
    pub fn schema(&self) -> Option<&str> {
        self.ws
            .exa_attr()
            .get("currentSchema")
            .and_then(|v| v.as_str())
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
    /// exa_con.set_schema(Some(schema)).unwrap();
    /// ```
    #[inline]
    pub fn set_schema<T>(&mut self, schema: Option<T>) -> Result<()>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({ "currentSchema": schema });
        self.set_attributes(payload)
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
    /// ```
    pub fn execute<T>(&mut self, query: T) -> Result<QueryResult>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "execute", "sqlText": query});
        self.exec_with_one_result(payload)
    }

    /// Sends multiple queries to the database and waits for their results.
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
    /// let queries = vec!["SELECT 3", "DELETE * FROM EXA_RUST_TEST WHERE 1 = 2"];
    /// let results: Vec<QueryResult> = exa_con.execute_batch(&queries).unwrap();
    /// ```
    pub fn execute_batch<T>(&mut self, queries: &[T]) -> Result<Vec<QueryResult>>
    where
        T: AsRef<str> + Serialize,
    {
        let payload = json!({"command": "executeBatch", "sqlTexts": queries});
        self.exec_with_results(payload)
    }

    /// For a given mutable reference of a [QueryResult],
    /// return all rows if the query produced a [ResultSet].
    /// Returns an empty `Vec` if there's no result set or it was all retrieved already.
    /// Automatically closes the result set if it was fully retrieved.
    /// ```
    /// # use exasol::error::Result;
    /// # use exasol::{connect, bind, QueryResult};
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
    /// let data: Vec<Vec<String>> = exa_con.retrieve_rows(&mut result).unwrap();
    /// assert_eq!(data.len(), 2);
    /// ```
    pub fn retrieve_rows<T: DeserializeOwned>(
        &mut self,
        result: &mut QueryResult,
    ) -> Result<Vec<T>> {
        result
            .result_set_mut()
            .map(|rs| ResultSetIter::new(rs, self).collect())
            .unwrap_or(Ok(Vec::new()))
    }

    /// For a given mutable reference of a [QueryResult],
    /// return at most n rows if the query produced a [ResultSet].
    /// Returns an empty `Vec` if there's no result set or it was all retrieved already.
    /// Automatically closes the result set if it was fully retrieved.
    /// ```
    /// # use exasol::error::Result;
    /// # use exasol::{connect, bind, QueryResult};
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
    /// let data: Vec<Vec<String>> = exa_con.retrieve_nrows(&mut result, 1).unwrap();
    /// assert_eq!(data.len(), 1);
    ///
    /// // We're closing the underlying result set because we don't need it anymore.
    /// exa_con.close_result(result).unwrap();
    /// ```
    pub fn retrieve_nrows<T: DeserializeOwned>(
        &mut self,
        result: &mut QueryResult,
        n: usize,
    ) -> Result<Vec<T>> {
        result
            .result_set_mut()
            .map(|rs| ResultSetIter::new(rs, self).take(n).collect())
            .unwrap_or(Ok(Vec::new()))
    }

    /// HTTP Transport export of a query or table with row deserialization into a given Rust type.
    /// The operation can be controlled by passing an [Option] with [HttpTransportOpts].
    /// By default, a thread is created for every node in the Exasol cluster, encryption is enabled
    /// and compression is disabled.
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
    /// let result = exa_con.export("SELECT * FROM EXA_RUST_TEST LIMIT 1000", None).unwrap();
    ///
    /// result.into_iter().take(5).for_each(|v: (String, String, u32)| println!("{:?}", v))
    /// ```
    pub fn export<Q, T>(
        &mut self,
        query_or_table: Q,
        opts: Option<HttpTransportOpts>,
    ) -> Result<Vec<T>>
    where
        Q: AsRef<str> + Serialize + Send,
        T: DeserializeOwned + Send,
    {
        HttpExportJob::new(self, query_or_table, opts).run()
    }

    /// HTTP Transport import into a table from a Rust type implementing [IntoIterator].
    /// The operation can be controlled by passing an [Option] with [HttpTransportOpts].
    /// By default, a thread is created for every node in the Exasol cluster, encryption is enabled
    /// and compression is disabled.
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
    /// let result: Vec<(String, String, u32)> = exa_con.export("SELECT * FROM EXA_RUST_TEST LIMIT 1000", None).unwrap();
    /// exa_con.import("EXA_RUST_TEST", result, None).unwrap();
    /// ```
    pub fn import<Q, T, I>(
        &mut self,
        table: Q,
        data: I,
        opts: Option<HttpTransportOpts>,
    ) -> Result<()>
    where
        Q: AsRef<str> + Serialize + Send,
        T: Serialize + Send,
        I: IntoIterator<Item = T>,
    {
        HttpImportJob::new(self, table, data, opts).run()
    }

    /// Creates a prepared statement of type [PreparedStatement].
    /// The prepared statement can then be executed with the provided data.
    ///
    /// Named parameters are supported to aid in using map-like types as data rows
    /// when executing the prepared statement. Using just `?` results in the parameter name
    /// being the empty string.
    ///
    /// Since a map-like type implies no duplicate keys, duplicate named parameters
    /// are not supported and will result in errors when the [PreparedStatement] is executed.
    ///
    /// For sequence-like types, parameter names are ignored and discarded.
    ///
    /// ```
    /// use exasol::{connect, QueryResult, PreparedStatement};
    /// use exasol::error::Result;
    /// use serde_json::json;
    /// use std::env;
    ///
    /// let dsn = env::var("EXA_DSN").unwrap();
    /// let schema = env::var("EXA_SCHEMA").unwrap();
    /// let user = env::var("EXA_USER").unwrap();
    /// let password = env::var("EXA_PASSWORD").unwrap();
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt = exa_con.prepare("SELECT 1 FROM (SELECT 1) TMP WHERE 1 = ?").unwrap();
    ///
    /// let data = vec![vec![json!(1)]];
    /// exa_con.execute_prepared(&prepared_stmt, &data).unwrap();
    ///
    /// // Prepared statements should be closed once you're done with them:
    /// exa_con.close_prepared_statement(prepared_stmt).unwrap();
    /// ```
    ///
    /// ```
    /// use exasol::{connect, QueryResult, PreparedStatement};
    /// use exasol::error::Result;
    /// use serde_json::json;
    /// use serde::Serialize;
    /// use std::env;
    ///
    /// let dsn = env::var("EXA_DSN").unwrap();
    /// let schema = env::var("EXA_SCHEMA").unwrap();
    /// let user = env::var("EXA_USER").unwrap();
    /// let password = env::var("EXA_PASSWORD").unwrap();
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prepared_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES(?col1, ?col2, ?col3)").unwrap();
    ///
    /// #[derive(Serialize, Clone)]
    /// struct Data {
    ///    col1: String,
    ///    col2: String,
    ///    col3: u8
    /// }
    ///
    /// let data_item = Data { col1: "t".to_owned(), col2: "y".to_owned(), col3: 10 };
    /// let vec_data = vec![data_item.clone(), data_item.clone(), data_item];
    ///
    /// exa_con.execute_prepared(&prepared_stmt, vec_data).unwrap();
    /// ```
    ///
    /// String literals resembling a parameter can be escaped, if needed:
    ///
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
    /// let prepared_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES('\\?col1', ?col2, ?col3)").unwrap();
    /// let data = vec![json!(["test", 1])];
    /// exa_con.execute_prepared(&prepared_stmt, &data).unwrap();
    /// ```
    pub fn prepare<T>(&mut self, query: T) -> Result<PreparedStatement>
    where
        T: Serialize + AsRef<str>,
    {
        let re = regex!(r"\\(\?\w*)|[?\w]\?\w*|\?\w*\?|\?(\w*)");
        let mut col_names = Vec::new();
        let str_query = query.as_ref();

        // This is similar to parameter binding, but uses ? instead of :
        //
        // Capture group 2 is Some when an actual parameter name is matched,
        // in which case it needs to be stored as a column name.
        // A simple question mark is returned to replace_all(),
        // as this is just the driver's mechanism for accepting named parameters
        // and Exasol has no clue about it.
        //
        // Capture group 1 is Some only when an escaped parameter construct
        // is matched(e.g: "\?PARAM"). Returning the group gets rid of the escape question mark.
        //
        // Otherwise, capture group 0, AKA the entire match, is returned as-is,
        // as it represents a regex match that we purposely ignore.
        // It's safe to unwrap it because it wouldn't be there if there is no match.
        let q = re.replace_all(str_query, |cap: &Captures| {
            cap.get(2)
                .map(|m| {
                    col_names.push(m.as_str().to_owned());
                    "?"
                })
                .or_else(|| cap.get(1).map(|m| &str_query[m.range()]))
                .unwrap_or(&str_query[cap.get(0).unwrap().range()])
        });

        let payload = json!({"command": "createPreparedStatement", "sqlText": q});
        let mut ps: PreparedStatement = self.get_resp_data(payload)?.try_into()?;
        ps.update_param_names(col_names);
        self.ps_handles.insert(ps.handle());
        Ok(ps)
    }

    /// Executes the prepared statement with the given data.
    /// Data must implement [IntoIterator] where `Item` implements [Serialize].
    /// Each `Item` of the iterator will represent a data row.
    ///
    /// If `Item` is map-like, the needed columns are retrieved and consumed,
    /// getting reordered based on the expected order given through the named parameters.
    ///
    /// If `Item` is sequence-like, the data is used as-is.
    /// Parameter names are ignored.
    ///
    /// # Errors
    ///
    /// Missing parameter names in map-like types (which can also be caused by duplication)
    /// or too few/many columns in sequence-like types results in errors.
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
    /// #
    /// use serde_json::json;
    /// use serde::Serialize;
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prep_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES(?col1, ?col2, ?col3)").unwrap();
    ///
    /// let json_data = json!(
    ///     [
    ///         ["a", "b", 1],
    ///         ["c", "d", 2],
    ///         ["e", "f", 3],
    ///         ["g", "h", 4]
    ///     ]
    /// );
    ///
    /// exa_con.execute_prepared(&prep_stmt, json_data.as_array().unwrap()).unwrap();
    ///
    /// #[derive(Serialize, Clone)]
    /// struct Data {
    ///    col1: String,
    ///    col2: String,
    ///    col3: u8
    /// }
    ///
    /// let data_item = Data { col1: "t".to_owned(), col2: "y".to_owned(), col3: 10 };
    /// let vec_data = vec![data_item.clone(), data_item.clone(), data_item];
    ///
    /// exa_con.execute_prepared(&prep_stmt, vec_data).unwrap();
    /// ```
    pub fn execute_prepared<T, S>(&mut self, ps: &PreparedStatement, data: T) -> Result<QueryResult>
    where
        S: Serialize,
        T: IntoIterator<Item = S>,
    {
        let (num_columns, columns) = ps
            .params()
            .map(|p| (p.num_columns(), p.columns()))
            .unwrap_or((0, [].as_slice()));

        let col_names = columns.iter().map(|c| c.name()).collect::<Vec<_>>();
        let col_major_data = to_col_major(&col_names, data).map_err(DriverError::DataError)?;
        let num_rows = col_major_data.num_rows();

        let payload = json!({
            "command": "executePreparedStatement",
            "statementHandle": ps.handle(),
            "numColumns": num_columns,
            "numRows": num_rows,
            "columns": columns,
            "data": col_major_data
        });

        self.exec_with_one_result(payload)
    }

    /// Consumes this [QueryResult], closing the inner [ResultSet], if there's one
    /// and it was not already closed.
    /// ```
    /// # use exasol::error::Result;
    /// # use exasol::{connect, bind, QueryResult};
    /// # use std::env;
    /// #
    /// # let dsn = env::var("EXA_DSN").unwrap();
    /// # let schema = env::var("EXA_SCHEMA").unwrap();
    /// # let user = env::var("EXA_USER").unwrap();
    /// # let password = env::var("EXA_PASSWORD").unwrap();
    /// #
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let mut result = exa_con.execute("SELECT '1', '2', '3' UNION ALL SELECT '4', '5', '6'").unwrap();
    /// exa_con.close_result(result);
    /// ```
    pub fn close_result(&mut self, qr: QueryResult) -> Result<()> {
        qr.result_set()
            .and_then(|rs| match rs.is_closed() {
                true => None,
                false => rs.handle(),
            })
            .map(|h| self.close_results_impl([h]))
            .unwrap_or(Ok(()))
    }

    /// Consumes this [PreparedStatement], closing it.
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
    /// #
    /// use serde_json::json;
    /// use serde::Serialize;
    ///
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let prep_stmt = exa_con.prepare("INSERT INTO EXA_RUST_TEST VALUES(?col1, ?col2, ?col3)").unwrap();
    /// exa_con.close_prepared_statement(prep_stmt);
    /// ```
    pub fn close_prepared_statement(&mut self, ps: PreparedStatement) -> Result<()> {
        self.close_prepared_stmt_impl(ps.handle())
    }

    /// Returns a Vec containing the addresses of
    /// all nodes in the Exasol cluster
    pub fn get_nodes(&mut self) -> Result<Vec<String>> {
        let addr = self.driver_attr.server_ip.as_str();
        let port = self.driver_attr.port;
        let payload = json!({"command": "getHosts", "hostIp": addr});
        let hosts: Vec<String> = self.get_resp_data(payload)?.try_into()?;

        Ok(hosts
            .into_iter()
            .map(|h| format!("{}:{}", h, port))
            .collect())
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
    pub fn ping(&mut self) -> Result<()> {
        self.ws
            .write_message(Message::Ping(vec![]))
            .and(self.ws.read_message())
            .map_err(DriverError::RequestError)?;
        Ok(())
    }

    /// Closes multiple results sets by going over the result set handles [Iterator].
    #[inline]
    pub(crate) fn close_results_impl<I>(&mut self, handles: I) -> Result<()>
    where
        I: IntoIterator<Item = u16> + Serialize,
    {
        let payload = json!({"command": "closeResultSet", "resultSetHandles": handles});
        self.do_request(payload).map(|_| ())
    }

    /// Returns response data from a request.
    #[inline]
    pub(crate) fn get_resp_data(&mut self, payload: Value) -> Result<ResponseData> {
        self.ws.get_resp_data(payload)
    }

    /// Sets connection attributes.
    #[inline]
    fn set_attributes(&mut self, attrs: Value) -> Result<()> {
        let payload = json!({"command": "setAttributes", "attributes": attrs});
        // Attributes have to be retrieved as well, for consistency.
        self.do_request(payload)
            .and_then(|_| self.get_attributes())
            .map(|_| ())
    }

    /// Closes a prepared statement through its handle.
    #[inline]
    fn close_prepared_stmt_impl(&mut self, h: u16) -> Result<()> {
        let payload = json!({"command": "closePreparedStatement", "statementHandle": h});
        self.do_request(payload).map(|_| ())
    }

    /// Sends a request for execution and returns the last [QueryResult] received.
    fn exec_with_one_result(&mut self, payload: Value) -> Result<QueryResult> {
        self.exec_with_results(payload)?
            .pop()
            .ok_or_else(|| DriverError::ResponseMismatch("result sets").into())
    }

    /// Gets the database results as [crate::query_result::Results]
    /// (that's what they deserialize into) and consumes them
    /// to return a usable vector of [QueryResult] enums.
    fn exec_with_results(&mut self, payload: Value) -> Result<Vec<QueryResult>> {
        let lc = self.driver_attr.lowercase_columns;
        let mut results: Vec<QueryResult> = self.get_resp_data(payload)?.try_into()?;

        results.iter_mut().for_each(|qr| {
            qr.lowercase_columns(lc);
            qr.result_set()
                .and_then(|rs| rs.handle())
                .map(|h| self.rs_handles.insert(h));
        });

        Ok(results)
    }

    /// This method attempts to create the websocket, authenticate in Exasol
    /// and read the connection attributes afterwards.
    ///
    /// We'll get the list of IP addresses resulted from parsing and resolving the DSN
    /// Then loop through all of them (max once each) until a connection is established.
    /// Login is attempted afterwards.
    fn try_websocket_from_opts(opts: &ConOpts) -> std::result::Result<WsAddr, ConnectionError> {
        let addresses = opts.parse_dsn()?;
        let ws_prefix = opts.ws_prefix();

        let cf = addresses
            .into_iter()
            .try_fold(ConnectionError::InvalidDSN, |_, (addr, port)| {
                Self::try_websocket_from_url(ws_prefix, addr, port)
            });

        match cf {
            ControlFlow::Break(ws) => Ok(ws),
            ControlFlow::Continue(e) => Err(e),
        }
    }

    /// Attempts to create a websocket for the given URL
    /// Issues a Break variant if the connection was established
    /// or the Continue variant if an error was encountered.
    fn try_websocket_from_url(
        prefix: &str,
        addr: String,
        port: u16,
    ) -> ControlFlow<WsAddr, ConnectionError> {
        let full_url = format!("{}://{}:{}", prefix, &addr, port);
        let res = Url::parse(&full_url)
            .map_err(ConnectionError::from)
            .and_then(|url| tungstenite::connect(url).map_err(ConnectionError::from));

        match res {
            Ok((ws, _)) => ControlFlow::Break((ws, addr, port)),
            Err(e) => ControlFlow::Continue(e),
        }
    }

    /// Gets connection attributes from Exasol
    #[inline]
    fn get_attributes(&mut self) -> Result<()> {
        let payload = json!({"command": "getAttributes"});
        self.do_request(payload)?;
        Ok(())
    }

    /// Sends a request and waits for its response.
    #[inline]
    fn do_request(&mut self, payload: Value) -> Result<Option<ResponseData>> {
        self.ws.do_request(payload)
    }
}
