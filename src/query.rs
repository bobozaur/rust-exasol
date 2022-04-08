use crate::connection::ConnectionImpl;
use crate::error::{ConversionError, DataError, DriverError, RequestError, Result};
use crate::response::{Column, QueryResultDe, ResultSetDe};
use crate::response::{ParameterData, PreparedStatementDe};
use crate::row::{to_col_major, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;

/// Enum containing the result of a query
/// `ResultSet` variant holds a [ResultSet]
/// `RowCount` variant holds the affected row count
pub enum QueryResult {
    ResultSet(ResultSet),
    RowCount(u32),
}

impl Debug for QueryResult {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ResultSet(r) => write!(f, "{:?}", r),
            Self::RowCount(c) => write!(f, "Row count: {}", c),
        }
    }
}

impl TryFrom<QueryResult> for ResultSet {
    type Error = ConversionError;

    #[inline]
    fn try_from(value: QueryResult) -> std::result::Result<Self, Self::Error> {
        match value {
            QueryResult::ResultSet(r) => Ok(r),
            _ => Err(ConversionError::ResultSetError),
        }
    }
}

impl TryFrom<QueryResult> for u32 {
    type Error = ConversionError;

    #[inline]
    fn try_from(value: QueryResult) -> std::result::Result<Self, Self::Error> {
        match value {
            QueryResult::RowCount(r) => Ok(r),
            _ => Err(ConversionError::RowCountError),
        }
    }
}

impl QueryResult {
    #[inline]
    pub(crate) fn from_de(
        qr: QueryResultDe,
        con_rc: &Rc<RefCell<ConnectionImpl>>,
        lc: bool,
    ) -> Self {
        match qr {
            QueryResultDe::ResultSet { result_set } => {
                QueryResult::ResultSet(ResultSet::from_de(result_set, con_rc, lc))
            }
            QueryResultDe::RowCount { row_count } => QueryResult::RowCount(row_count),
        }
    }
}

/// Iterator struct over the result set of a given query.
/// The maximum data rows that are initially retrieved are 1000.
/// Further rows get fetched as needed
///
/// ```
/// # use exasol::{connect, QueryResult};
/// # use serde_json::Value;
/// # use exasol::error::Result;
/// # use std::env;
/// #
/// # let dsn = env::var("EXA_DSN").unwrap();
/// # let schema = env::var("EXA_SCHEMA").unwrap();
/// # let user = env::var("EXA_USER").unwrap();
/// # let password = env::var("EXA_PASSWORD").unwrap();
/// #
/// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
/// let result = exa_con.execute("SELECT * FROM EXA_ALL_OBJECTS LIMIT 2000;").unwrap();
///
/// if let QueryResult::ResultSet(r) = result {
///     let x = r.take(50).collect::<Result<Vec<Vec<Value>>>>();
///         if let Ok(v) = x {
///             for row in v.iter() {
///                 // do stuff
///             }
///         }
///     }
/// ```
///
/// The iterator is lazy, and it will retrieve rows in chunks from the database
/// based on the `fetch_size` parameter set in the [ConOpts](crate::ConOpts) used when constructing
/// the [Connection](crate::Connection), until the result set is entirely read.
#[allow(unused)]
pub struct ResultSet<T: DeserializeOwned = Vec<Value>> {
    num_columns: u8,
    total_rows_num: u32,
    total_rows_pos: u32,
    chunk_rows_num: usize,
    chunk_rows_pos: usize,
    result_set_handle: Option<u16>,
    columns: Vec<Column>,
    flat_data: Vec<Value>,
    connection: Rc<RefCell<ConnectionImpl>>,
    is_closed: bool,
    row_type: PhantomData<*const T>,
}

impl<T> Debug for ResultSet<T>
where
    T: DeserializeOwned,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Closed: {}\n\
            Handle: {:?}\n\
            Columns: {:?}\n\
            Rows: {}",
            self.is_closed, self.result_set_handle, self.columns, self.total_rows_num
        )
    }
}

impl<T> ResultSet<T>
where
    T: DeserializeOwned,
{
    /// Returns a reference to a [Vec<Column>] of the result set columns.
    #[inline]
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    /// Returns the number of columns in the result set.
    #[inline]
    pub fn num_columns(&self) -> &u8 {
        &self.num_columns
    }

    /// Returns the number of rows in the result set.
    #[inline]
    pub fn num_rows(&self) -> &u32 {
        &self.total_rows_num
    }

    /// Method that consumes self to return a new [ResultSet]
    /// that deserializes rows to the given Rust type.
    ///
    /// This method does not take any arguments,
    /// it's merely a means for changing the generic row type.
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
    /// let mut exa_con = connect(&dsn, &schema, &user, &password).unwrap();
    /// let result = exa_con.execute("SELECT * FROM EXA_RUST_TEST LIMIT 1500;").unwrap();
    ///
    /// if let QueryResult::ResultSet(result_set) = result {
    ///     // Change the expected row type with the turbofish notation
    ///     let mut result_set = result_set.with_row_type::<(String, String)>();
    ///     let row1 = result_set.next();
    ///
    ///     // Nothing stops you from changing row types
    ///     // on the same result set, even while iterating through it
    ///     let mut result_set = result_set.with_row_type::<Vec<String>>();
    ///     let row2 = result_set.next();
    /// }
    /// ```
    pub fn with_row_type<R>(mut self) -> ResultSet<R>
    where
        R: DeserializeOwned,
    {
        ResultSet {
            num_columns: self.num_columns,
            total_rows_num: self.total_rows_num,
            total_rows_pos: self.total_rows_pos,
            chunk_rows_num: self.chunk_rows_num,
            chunk_rows_pos: self.chunk_rows_pos,
            result_set_handle: std::mem::take(&mut self.result_set_handle),
            columns: std::mem::take(&mut self.columns),
            flat_data: std::mem::take(&mut self.flat_data),
            connection: Rc::clone(&self.connection),
            is_closed: self.is_closed,
            row_type: PhantomData,
        }
    }

    /// Method that generates the [ResultSet] struct based on [ResultSetDe].
    /// It will also remap column names to their lowercase representation if needed.
    pub(crate) fn from_de(
        mut rs: ResultSetDe,
        con_rc: &Rc<RefCell<ConnectionImpl>>,
        lc: bool,
    ) -> Self {
        // Set column names as lowercase if needed
        match lc {
            false => (),
            true => rs
                .columns
                .iter_mut()
                .for_each(|c| c.name = c.name.to_lowercase()),
        }

        Self {
            num_columns: rs.num_columns,
            total_rows_num: rs.total_rows_num,
            total_rows_pos: 0,
            chunk_rows_num: rs.chunk_rows_num,
            chunk_rows_pos: 0,
            result_set_handle: rs.result_set_handle,
            columns: rs.columns,
            flat_data: rs.data,
            connection: Rc::clone(con_rc),
            is_closed: false,
            row_type: PhantomData,
        }
    }

    fn next_row(&mut self) -> Result<T> {
        let col_len = self.columns.len();

        // Get a row by indexing through the 0..col_len range
        // Then the index will be chunk_rows_pos + i * chunk_rows_num
        let mut row = Vec::with_capacity(col_len);
        (0..col_len)
            .into_iter()
            .map(|i| self.chunk_rows_pos + i * self.chunk_rows_num)
            .map(|i| self.flat_data.get_mut(i).map(|v| v.take()))
            .for_each(|o| row.extend(o));

        let row_len = row.len();

        // Ensure there was data for all columns
        match row_len == col_len {
            true => Ok(T::deserialize(Row::new(row, &self.columns))
                .map_err(DataError::TypeParseError)
                .map_err(DriverError::DataError)?),
            false => {
                Err(DataError::IncorrectLength(col_len, row_len)).map_err(DriverError::DataError)?
            }
        }
    }

    /// Closes the result set if it wasn't already closed
    /// This method gets called when a [ResultSet] is fully iterated over
    /// but also when the [ResultSet] is dropped.
    fn close(&mut self) -> Result<()> {
        self.result_set_handle.map_or(Ok(()), |h| {
            if !self.is_closed {
                self.is_closed = true;
                (*self.connection).borrow_mut().close_result_set(h)
            } else {
                Ok(())
            }
        })
    }

    /// Gets the next chunk of the result set from Exasol
    fn fetch_chunk(&mut self) -> Result<()> {
        // Check the statement handle
        self.result_set_handle
            .ok_or_else(|| DriverError::RequestError(RequestError::MissingHandleError).into())
            .and_then(|h| {
                // Dereference connection
                let mut con = (*self.connection).borrow_mut();
                let fetch_size = con.driver_attr.fetch_size;

                // Compose the payload
                let payload = json!({
                    "command": "fetch",
                    "resultSetHandle": h,
                    "startPosition": self.total_rows_pos,
                    "numBytes": fetch_size,
                });

                // Fetch and store data chunk
                con.get_resp_data(payload)?.try_to_fetched_data().map(|f| {
                    self.chunk_rows_num = f.chunk_rows_num;
                    self.chunk_rows_pos = 0;
                    self.flat_data = f.data;
                })
            })
    }
}

/// Making [ResultSet] an iterator
impl<T> Iterator for ResultSet<T>
where
    T: DeserializeOwned,
{
    type Item = Result<T>;

    fn next(&mut self) -> Option<Self::Item> {
        let row = if self.chunk_rows_pos < self.chunk_rows_num {
            // There still are rows in this chunk
            Some(self.next_row())
        } else if self.total_rows_pos >= self.total_rows_num {
            // All rows retrieved
            None
        } else {
            // Whole chunk retrieved. Get new one and get row.
            self.fetch_chunk()
                .map_or_else(|e| Some(Err(e)), |_| Some(self.next_row()))
        };

        // Updating positional counters
        self.total_rows_pos += 1;
        self.chunk_rows_pos += 1;

        // If row is None, all rows were iterated
        // so we're closing the result set, propagating the error, if any
        row.or_else(|| self.close().map_or_else(|e| Some(Err(e)), |_| None))
    }
}

impl<T> Drop for ResultSet<T>
where
    T: DeserializeOwned,
{
    fn drop(&mut self) {
        self.close().ok();
    }
}
/// Type that represents a prepared statement in Exasol.
#[derive(Debug)]
pub struct PreparedStatement {
    statement_handle: usize,
    parameter_data: Option<ParameterData>,
    connection: Rc<RefCell<ConnectionImpl>>,
    column_names: Vec<String>,
}

impl PreparedStatement {
    /// Method that generates the [PreparedStatement] struct based on [PreparedStatementDe].
    pub(crate) fn from_de(
        prep_stmt: PreparedStatementDe,
        con_rc: &Rc<RefCell<ConnectionImpl>>,
        col_names: Vec<String>,
    ) -> Self {
        Self {
            statement_handle: prep_stmt.statement_handle,
            parameter_data: prep_stmt.parameter_data,
            connection: Rc::clone(con_rc),
            column_names: col_names,
        }
    }

    /// Executes the prepared statement with the given data.
    /// Data must implement [IntoIterator] where `Item` implements [Serialize].
    /// Each `Item` of the iterator will represent a data row.
    ///
    /// If `Item` is map-like, only the needed columns are retrieved,
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
    /// prep_stmt.execute(json_data.as_array().unwrap()).unwrap();
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
    /// prep_stmt.execute(vec_data).unwrap();
    /// ```
    pub fn execute<T, S>(&self, data: T) -> Result<QueryResult>
    where
        S: Serialize,
        T: IntoIterator<Item = S>,
    {
        let (num_columns, columns) = match self.parameter_data.as_ref() {
            Some(p) => (&p.num_columns, p.columns.as_slice()),
            None => (&0, [].as_slice()),
        };

        let col_names = self
            .column_names
            .iter()
            .map(|c| c.as_str())
            .collect::<Vec<&str>>();

        let col_major_data = to_col_major(&col_names, data).map_err(DriverError::DataError)?;
        let num_rows = col_major_data.get_num_rows();

        let payload = json!({
            "command": "executePreparedStatement",
            "statementHandle": &self.statement_handle,
            "numColumns": num_columns,
            "numRows": num_rows,
            "columns": columns,
            "data": col_major_data
        });

        self.connection
            .borrow_mut()
            .exec_and_get_first(&self.connection, payload)
    }

    #[inline]
    pub fn close(&mut self) -> Result<()> {
        (*self.connection)
            .borrow_mut()
            .close_prepared_stmt(self.statement_handle)
    }
}

impl Drop for PreparedStatement {
    fn drop(&mut self) {
        self.close().ok();
    }
}
