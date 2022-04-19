use super::WRITE_BUFFER_SIZE;
#[cfg(feature = "flate2")]
use flate2::{write::GzEncoder, Compression};
use std::io::{ErrorKind, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Wrapper that will count the bytes written in the buffer
/// to also pass them to Exasol when sending a data chunk.
/// The CSV writer will take care of buffering so we don't have to.
pub struct ExaRowWriter<'a, W: Write> {
    run: &'a Arc<AtomicBool>,
    stream: W,
    compression: bool,
    buf: Vec<u8>,
}

impl<'a, W> ExaRowWriter<'a, W>
where
    W: Write,
{
    pub fn new(stream: W, run: &'a Arc<AtomicBool>, compression: bool) -> Self {
        Self {
            stream,
            run,
            compression,
            buf: Vec::with_capacity(WRITE_BUFFER_SIZE),
        }
    }

    /// Writes the data chunk to the internal buffer, passing it through
    /// the compressor if needed.
    /// If compression is enabled, it's very important that the number of input bytes
    /// written to the compressor are returned, not the output bytes after compression.
    #[inline]
    #[allow(unreachable_code)]
    fn write_chunk(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.compression {
            #[cfg(feature = "flate2")]
            return match buf.len() > 0 {
                false => Ok(0),
                true => {
                    let mut enc = GzEncoder::new(&mut self.buf, Compression::default());
                    let num_bytes = enc.write(buf)?;
                    enc.finish()?;
                    Ok(num_bytes)
                }
            };

            panic!("flate2 feature must be enabled to use compression");
        } else {
            self.buf.write(buf)
        }
    }
}

impl<'a, W> Write for ExaRowWriter<'a, W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.run.load(Ordering::Acquire) {
            // Number of bytes we wrote into the writer (input bytes, pre-compression).
            let num_bytes = self.write_chunk(buf)?;
            Ok(num_bytes)
        } else {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Stop signal encountered",
            ))
        }
    }

    /// Writing length and CRLF must be done with no compression
    /// hence we tap into the inner stream
    fn flush(&mut self) -> std::io::Result<()> {
        if self.run.load(Ordering::Acquire) {
            if !self.buf.is_empty() {
                let len = format!("{:X}", self.buf.len());
                self.stream.write_all(len.as_bytes())?;
                self.stream.write_all(b"\r\n")?;
                self.stream.write_all(self.buf.as_slice())?;
                self.stream.write_all(b"\r\n")?;
                self.buf.clear();
            }

            Ok(())
        } else {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Stop signal encountered",
            ))
        }
    }
}
