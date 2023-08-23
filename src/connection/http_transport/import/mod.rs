use std::{
    io::Result as IoResult,
    pin::Pin,
    task::{Context, Poll},
};

use async_compression::futures::write::GzipDecoder;
use futures_io::AsyncWrite;
use futures_util::AsyncWriteExt;
use pin_project::pin_project;

use self::writer::ImportWriter;

mod options;
mod writer;

pub use options::{ImportOptions, Trim};

#[pin_project(project = ExaImportProj)]
#[derive(Debug)]
pub enum ExaImport {
    #[cfg(feature = "compression")]
    Compressed(#[pin] GzipDecoder<ImportWriter>),
    Plain(#[pin] ImportWriter),
}

impl ExaImport {
    /// Ends the data import.
    /// *MUST* be called after no more data needs to be
    /// passed to this writer.
    pub async fn finish(mut self) -> IoResult<()> {
        self.flush().await?;
        self.into_inner().finish().await
    }

    pub(crate) fn new(writer: ImportWriter, compression: bool) -> Self {
        match compression {
            #[cfg(feature = "compression")]
            true => Self::Compressed(GzipDecoder::new(writer)),
            _ => Self::Plain(writer),
        }
    }

    fn into_inner(self) -> ImportWriter {
        match self {
            #[cfg(feature = "compression")]
            ExaImport::Compressed(s) => s.into_inner(),
            ExaImport::Plain(s) => s,
        }
    }
}

impl AsyncWrite for ExaImport {
    fn poll_write(self: Pin<&mut Self>, cx: &mut Context<'_>, buf: &[u8]) -> Poll<IoResult<usize>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaImportProj::Compressed(s) => s.poll_write(cx, buf),
            ExaImportProj::Plain(s) => s.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaImportProj::Compressed(s) => s.poll_flush(cx),
            ExaImportProj::Plain(s) => s.poll_flush(cx),
        }
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<IoResult<()>> {
        match self.project() {
            #[cfg(feature = "compression")]
            ExaImportProj::Compressed(s) => s.poll_close(cx),
            ExaImportProj::Plain(s) => s.poll_close(cx),
        }
    }
}
