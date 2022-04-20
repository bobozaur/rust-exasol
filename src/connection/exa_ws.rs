use super::{Attributes, MaybeCompressedWs, ReqResult, ResponseData};
use super::{Certificate, ConResult};
use crate::error::{ConnectionError, DriverError, Result};
use crate::{ConOpts, ProtocolVersion};
use rsa::pkcs1::DecodeRsaPublicKey;
use rsa::RsaPublicKey;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fmt::Write;
use std::net::TcpStream;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

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

    /// Validates the DSN fingerprint.
    pub fn validate_fingerprint(&mut self, fingerprint: String) -> ConResult<()> {
        // The fingerprint is already a HEX string, so we just print it.
        let fingerprint = fingerprint.to_uppercase();
        let server_fp = self
            .peer_certificate()
            .and_then(|cert| cert.as_bytes())
            .map(|v| {
                let mut hasher = Sha256::new();
                hasher.update(v.as_slice());
                let mut output = String::with_capacity(fingerprint.len());
                for byte in hasher.finalize() {
                    write!(&mut output, "{:02X}", byte).ok()?
                }
                Some(output)
            })
            .and_then(|s| s);

        // If encryption is not enabled the server won't return a certificate,
        // so we just return Ok(()) in that case.
        match server_fp {
            None => Ok(()),
            Some(fp) => match fingerprint == fp {
                true => Ok(()),
                false => Err(ConnectionError::FingerprintMismatch(fingerprint, fp)),
            },
        }
    }

    /// Gets the peer leaf certificate
    fn peer_certificate(&mut self) -> Option<Certificate> {
        let stream = self.ws.as_inner_mut().get_ref();
        match stream {
            #[cfg(feature = "rustls")]
            MaybeTlsStream::Rustls(s) => s
                .conn
                .peer_certificates()
                .map(|certs| certs.first())
                .flatten()
                .map(|cert| Certificate::RustlsCert(cert.clone())),
            #[cfg(feature = "native-tls-basic")]
            MaybeTlsStream::NativeTls(s) => s
                .peer_certificate()
                .ok()
                .flatten()
                .map(|cert| Certificate::NativeTlsCert(cert)),
            _ => None,
        }
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
