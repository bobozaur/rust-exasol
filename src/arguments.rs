use sqlx::Arguments;

use crate::database::Exasol;

#[derive(Debug, Default, Clone)]
pub struct ExaArguments(pub Vec<String>);

impl<'q> Arguments<'q> for ExaArguments {
    type Database = Exasol;

    fn reserve(&mut self, additional: usize, size: usize) {
        todo!()
    }

    fn add<T>(&mut self, value: T)
    where
        T: 'q + Send + sqlx::Encode<'q, Self::Database> + sqlx::Type<Self::Database>,
    {
        todo!()
    }
}