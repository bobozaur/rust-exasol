# exasol
A synchronous database connector for Exasol written in Rust.

Please find the documentation [here](https://docs.rs/exasol/latest/exasol/).

Features available:
 - DSN parsing and resolving
 - Single and multi query execution
 - Named parameter binding
 - Autocommit is enabled by default. Disabling it results in transactional mode being enabled, which requires explicit "COMMIT" or "ROLLBACK" statements to be executed.

Features planned for future versions:
  - Prepared statements support
  - Performant IMPORT/EXPORT functionalities
  - Performant parallel IMPORT/EXPORT
  - WSS suppport through SSL
  - Compression support
