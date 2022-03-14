use crate::connection::ConnectionImpl;
use crate::error::Result;
use crate::response::{ParameterData, PreparedStatementDe};
use crate::{QueryResult, Row};
use serde_json::json;
use std::cell::RefCell;
use std::rc::Rc;

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

    pub fn execute(&self, data: Vec<Row>) -> Result<QueryResult> {
        let dummy_num_cols = 0u8;
        let dummy_cols_vec = vec![];

        let (num_columns, columns) = self
            .parameter_data
            .as_ref()
            .map_or((&dummy_num_cols, &dummy_cols_vec), |p| {
                (&p.num_columns, &p.columns)
            });

        let payload = json!({
            "command": "executePreparedStatement",
            "statementHandle": &self.statement_handle,
            "numColumns": num_columns,
            "numRows": data.len(),
            "columns": columns,
            "data": data
        });

        self.connection
            .borrow_mut()
            .exec_and_get_first(&self.connection, payload)
    }

    fn close(&mut self) -> Result<()> {
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
