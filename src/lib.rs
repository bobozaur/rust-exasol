pub mod exasol {
    use std::error::Error;
    use std::fmt::{Debug, Display, Formatter};
    use std::net::TcpStream;

    use rand::rngs::OsRng;
    use rsa::{PaddingScheme, pkcs1::FromRsaPublicKey, PublicKey, RsaPublicKey};
    use serde_json::{json, Value};
    use tungstenite::{connect, Message, stream::MaybeTlsStream, WebSocket};
    use url::Url;

    pub enum ExaError {
        ExaConnectionError,
    }

    impl Debug for ExaError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            todo!()
        }
    }

    impl Display for ExaError {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            todo!()
        }
    }

    impl std::error::Error for ExaError {}

    pub struct ExaConnection {
        ws: WebSocket<MaybeTlsStream<TcpStream>>,
    }

    impl Drop for ExaConnection {
        fn drop(&mut self) {
            self.do_request(json!({"command": "disconnect"}));
            self.ws.close(None);
        }
    }

    impl ExaConnection {
        pub fn new(dsn: &str, schema: &str, user: &str, password: &str) -> Result<ExaConnection, Box<dyn std::error::Error>> {
            let (ws, _) = connect(Url::parse(dsn)?)?;
            let mut con = ExaConnection { ws };
            con.login(schema, user, password)?;
            con.get_attributes();
            Ok(con)
        }

        pub fn execute(&mut self, query: &str) -> Result<Value, Box<dyn std::error::Error>>{
            let payload = json!({"command": "execute", "sqlText": query});
            self.do_request(payload)
        }

        fn get_attributes(&mut self) {
            let payload = json!({"command": "getAttributes"});
            self.do_request(payload);
        }

        fn do_request(&mut self, payload: Value) -> Result<Value, Box<dyn std::error::Error>>{
            self.ws.write_message(Message::Text(payload.to_string()))?;
            let msg = loop {
                break match self.ws.read_message()? {
                    Message::Text(resp) => resp,
                    Message::Binary(resp) => String::from_utf8(resp)?,
                    _ => continue
                };
            };
            println!("{}", &msg);
            Ok(serde_json::from_str(&msg)?)
        }

        fn get_public_key(&mut self) -> Result<RsaPublicKey, Box<dyn std::error::Error>> {
            let payload = json!({"command": "login", "protocolVersion": 1});
            let resp: Value = self.do_request(payload)?;
            let pem = resp["responseData"]["publicKeyPem"].as_str().ok_or(ExaError::ExaConnectionError)?;
            Ok(RsaPublicKey::from_pkcs1_pem(pem)?)
        }

        fn encrypt_password(password: &str, public_key: RsaPublicKey) -> Result<String, Box<dyn std::error::Error>> {
            let mut rng = OsRng;
            let padding = PaddingScheme::new_pkcs1v15_encrypt();
            let enc_password = base64::encode(public_key.encrypt(&mut rng, padding, password.as_bytes())?);
            Ok(enc_password)
        }

        fn login(&mut self, schema: &str, user: &str, password: &str) -> Result<Value, Box<dyn std::error::Error>> {
            let public_key = self.get_public_key()?;
            let enc_password = ExaConnection::encrypt_password(password, public_key)?;

            let payload = json!({
                "username": user,
                "password": enc_password,
                "driverName": "RustEXASOL 0.01",
                "clientName": "RustEXASOL 0.01",
                "clientVersion": "0.0.1",
                "clientOs": std::env::consts::OS,
                "clientRuntime": "Rust",
                "useCompression": false,
                "attributes": {
                            "currentSchema": schema,
                            "autocommit": true,
                            "queryTimeout": 0
                            }
                });

            self.do_request(payload)
        }
    }
}

