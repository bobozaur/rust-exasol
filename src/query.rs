use crate::connection::ConnectionImpl;
use crate::constants::{DUMMY_COLUMNS_NUM, DUMMY_COLUMNS_VEC};
use crate::error::{RequestError, Result};
use crate::response::{Column, QueryResultDe, ResultSetDe};
use crate::response::{ParameterData, PreparedStatementDe};
use crate::row::{transpose_data, Row};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::{json, Value};
use std::cell::RefCell;
use std::fmt::{Debug, Formatter};
use std::marker::PhantomData;
use std::rc::Rc;
use std::vec::IntoIter;

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

impl QueryResult {
    pub(crate) fn from_de(
        query_result: QueryResultDe,
        con_rc: &Rc<RefCell<ConnectionImpl>>,
    ) -> Self {
        match query_result {
            QueryResultDe::ResultSet { result_set } => {
                QueryResult::ResultSet(ResultSet::from_de(result_set, con_rc))
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
/// # use exasol::{connect, QueryResult, Row};
/// # use serde_json::Value;
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
    data_iter: IntoIter<Vec<Value>>,
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
            "Closed: {}\nHandle: {:?}\nColumns: {:?}\nRows: {}",
            self.is_closed, self.result_set_handle, self.columns, self.total_rows_num
        )
    }
}

impl<T> ResultSet<T>
where
    T: DeserializeOwned,
{
    /// Returns a reference to a [Vec<Column>] of the result set columns.
    pub fn columns(&self) -> &Vec<Column> {
        &self.columns
    }

    /// Returns the number of columns in the result set.
    pub fn num_columns(&self) -> &u8 {
        &self.num_columns
    }

    /// Returns the number of rows in the result set.
    pub fn num_rows(&self) -> &u32 {
        &self.total_rows_num
    }

    pub fn row_type<R>(mut self) -> ResultSet<R>
    where
        R: DeserializeOwned,
    {
        ResultSet {
            num_columns: self.num_columns,
            total_rows_num: self.total_rows_num,
            total_rows_pos: self.total_rows_pos,
            chunk_rows_num: self.chunk_rows_num,
            chunk_rows_pos: self.chunk_rows_pos,
            result_set_handle: self.result_set_handle,
            columns: std::mem::take(&mut self.columns),
            data_iter: std::mem::replace(&mut self.data_iter, vec![].into_iter()),
            connection: Rc::clone(&self.connection),
            is_closed: self.is_closed,
            row_type: PhantomData,
        }
    }

    /// Method that generates the [ResultSet] struct based on [ResultSetDe].
    pub(crate) fn from_de(result_set: ResultSetDe, con_rc: &Rc<RefCell<ConnectionImpl>>) -> Self {
        Self {
            num_columns: result_set.num_columns,
            total_rows_num: result_set.total_rows_num,
            total_rows_pos: 0,
            chunk_rows_num: result_set.chunk_rows_num,
            chunk_rows_pos: 0,
            result_set_handle: result_set.result_set_handle,
            columns: result_set.columns,
            data_iter: result_set.data.into_iter(),
            connection: Rc::clone(con_rc),
            is_closed: false,
            row_type: PhantomData,
        }
    }

    fn next_row(&mut self) -> Option<Result<T>> {
        self.data_iter
            .next()
            .map(|r| Ok(T::deserialize(Row::new(r, &self.columns))?))
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
            .ok_or(RequestError::MissingHandleError.into())
            .and_then(|h| {
                // Dereference connection
                let mut con = (*self.connection).borrow_mut();

                // Safe to unwrap as this will always be present due to ConOpts
                let fetch_size = con.get_attr("fetch_size").unwrap();

                // Compose the payload
                let payload = json!({
                    "command": "fetch",
                    "resultSetHandle": h,
                    "startPosition": self.total_rows_pos,
                    "numBytes": fetch_size,
                });

                con.get_resp_data(payload)?
                    .try_to_fetched_data()
                    .and_then(|f| {
                        self.chunk_rows_num = f.chunk_rows_num;
                        self.chunk_rows_pos = 0;
                        self.data_iter = f.data.into_iter();
                        Ok(())
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
        let row = self.next_row().or_else(|| {
            // All rows retrieved
            if self.total_rows_pos >= self.total_rows_num {
                None
                // Whole chunk retrieved. Get new one.
            } else if self.chunk_rows_pos >= self.chunk_rows_num {
                self.fetch_chunk()
                    .map_or_else(|e| Some(Err(e)), |_| self.next_row())
                // If all else fails
            } else {
                None
            }
        });

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

#[derive(Debug)]
pub struct PreparedStatement {
    statement_handle: usize,
    parameter_data: Option<ParameterData>,
    connection: Rc<RefCell<ConnectionImpl>>,
}

impl PreparedStatement {
    /// Method that generates the [PreparedStatement] struct based on [PreparedStatementDe].
    pub(crate) fn from_de(
        prep_stmt: PreparedStatementDe,
        con_rc: &Rc<RefCell<ConnectionImpl>>,
    ) -> Self {
        Self {
            statement_handle: prep_stmt.statement_handle,
            parameter_data: prep_stmt.parameter_data,
            connection: Rc::clone(con_rc),
        }
    }

    pub fn execute<T, S>(&self, data: T) -> Result<QueryResult>
    where
        S: Serialize,
        T: IntoIterator<Item = S>,
    {
        let (num_columns, columns) = match self.parameter_data.as_ref() {
            Some(p) => (&p.num_columns, &p.columns),
            None => (&DUMMY_COLUMNS_NUM, &DUMMY_COLUMNS_VEC),
        };

        let col_names = columns
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<&str>>();
        let col_major_data = transpose_data(&col_names, data)?;

        let payload = json!({
            "command": "executePreparedStatement",
            "statementHandle": &self.statement_handle,
            "numColumns": num_columns,
            "numRows": col_major_data.len(),
            "columns": columns,
            "data": col_major_data
        });

        self.connection
            .borrow_mut()
            .exec_and_get_first(&self.connection, payload)
    }

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
