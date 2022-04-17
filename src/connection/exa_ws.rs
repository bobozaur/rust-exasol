use std::collections::HashMap;
use std::net::TcpStream;
use serde_json::{json, Value};
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};
use rsa::pkcs1::{DecodeRsaPublicKey};
use rsa::RsaPublicKey;
use crate::error::{Result, DriverError, ConnectionError};
use crate::{ConOpts, ProtocolVersion};
use super::{MaybeCompressedWs, ReqResult, ResponseData, Attributes};

pub struct ExaWebSocket {
    ws: MaybeCompressedWs,
    exa_attr: HashMap<String, Value>,
}

impl ExaWebSocket {
    pub fn new(ws: WebSocket<MaybeTlsStream<TcpStream>>, opts: ConOpts) -> Result<Self> {
        let mut exa_ws = Self {
            ws: MaybeCompressedWs::Plain(ws),
            exa_attr: HashMap::new(),
        };

        // Get compression flag before consuming opts
        let compression = opts.compression();

        // Login must always be uncompressed
        exa_ws.login(opts)?;

        // Enable compression if needed
        exa_ws.ws = exa_ws.ws.enable_compression(compression);

        Ok(exa_ws)
    }

    #[inline]
    pub fn read_message(&mut self) -> ReqResult<Message> {
        let msg = self.ws.as_inner_mut().read_message()?;
        Ok(msg)
    }

    #[inline]
    pub fn write_message(&mut self, msg: Message) -> ReqResult<()> {
        self.ws.as_inner_mut().write_message(msg)?;
        Ok(())
    }

    #[inline]
    pub fn close(&mut self) -> ReqResult<()> {
        self.ws.as_inner_mut().close(None)?;
        Ok(())
    }

    #[inline]
    pub fn can_write(&self) -> bool {
        self.ws.as_inner().can_write()
    }

    #[inline]
    pub fn exa_attr(&self) -> &HashMap<String, Value> {
        &self.exa_attr
    }

    /// Returns response data from a request.
    #[inline]
    pub fn get_resp_data(&mut self, payload: Value) -> Result<ResponseData> {
        self.do_request(payload)?
            .ok_or_else(|| DriverError::ResponseMismatch("response data").into())
    }

    /// Sends a request and waits for its response.
    pub fn do_request(&mut self, payload: Value) -> Result<Option<ResponseData>> {
        let resp = self
            .ws
            .send(payload)
            .and_then(|_| self.ws.recv())
            .map_err(DriverError::RequestError)?;

        let (data, attr): (Option<ResponseData>, Option<Attributes>) = resp.try_into()?;
        if let Some(attributes) = attr {
            self.exa_attr.extend(attributes.map)
        }

        Ok(data)
    }

    /// Gets the public key from Exasol
    /// Used during authentication for encrypting the password
    fn get_public_key(&mut self, protocol_version: ProtocolVersion) -> Result<RsaPublicKey> {
        let payload = json!({"command": "login", "protocolVersion": protocol_version});
        let pem: String = self.get_resp_data(payload).and_then(|p| p.try_into())?;

        Ok(RsaPublicKey::from_pkcs1_pem(&pem)
            .map_err(ConnectionError::PKCS1Error)
            .map_err(DriverError::ConnectionError)?)
    }

    /// Authenticates to Exasol
    /// Called after the websocket is established
    ///
    /// Login is always uncompressed. If compression is enabled, it is set afterwards.
    fn login(&mut self, opts: ConOpts) -> Result<()> {
        // Encrypt password using server's public key
        let key = self.get_public_key(opts.protocol_version())?;
        let payload = opts.into_value(key).map_err(DriverError::ConnectionError)?;

        // Send login request
        self.do_request(payload)?;

        Ok(())
    }
}
