# exasol
A synchronous database connector for Exasol written in Rust, based on the Exasol [Websocket API](https://github.com/exasol/websocket-api).
Inspired by [Py-Exasol](https://github.com/exasol/pyexasol).

Please find the documentation [here](https://docs.rs/exasol/latest/exasol/).

Features available:
 - DSN parsing and resolving
 - Single and batch query execution
 - Prepared statements
 - WSS support through native-tls and rustls
 - Compression support
 - Row deserialization into Rust types
 - Positional parameter binding
 - Named parameter binding
 - Autocommit is enabled by default. Disabling it results in transactional mode being enabled, which requires explicit "COMMIT" or "ROLLBACK" statements to be executed.

Features planned for future versions:
  - Performant IMPORT/EXPORT functionalities
  - Performant parallel IMPORT/EXPORT

##Crate Features:
* native-tls - (disabled by default) enables `tungstenite`'s WSS support through `native-tls`
* rustls - (disabled by default) enables `tungstenite`'s WSS support through `rustls`
* flate2 - (disabled by default) enables support for requests and responses compression


##License
Licensed under either of

* Apache License, Version 2.0, (LICENSE-APACHE or https://www.apache.org/licenses/LICENSE-2.0)
* MIT license (LICENSE-MIT or https://opensource.org/licenses/MIT)

at your option.