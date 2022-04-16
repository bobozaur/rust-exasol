use super::WRITE_BUFFER_SIZE;
use std::io::{ErrorKind, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

/// Wrapper that will count the bytes written in the buffer
/// to also pass them to Exasol when sending a data chunk.
/// The CSV writer will take care of buffering so we don't have to.
pub struct ExaRowWriter<'a, W: Write> {
    buf: Vec<u8>,
    run: &'a Arc<AtomicBool>,
    stream: W,
    len: usize,
}

impl<'a, W> ExaRowWriter<'a, W>
where
    W: Write,
{
    pub fn new(stream: W, run: &'a Arc<AtomicBool>) -> Self {
        Self {
            stream,
            run,
            len: 0,
            buf: Vec::with_capacity(WRITE_BUFFER_SIZE),
        }
    }
}

impl<'a, W> Write for ExaRowWriter<'a, W>
where
    W: Write,
{
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        if self.run.load(Ordering::Acquire) {
            let size = self.buf.write(buf)?;
            self.len += size;
            Ok(size)
        } else {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Stop signal encountered",
            ))
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        if self.run.load(Ordering::Acquire) {
            if self.len > 0 {
                self.stream
                    .write_all(format!("{:X}", self.len).as_bytes())?;
                self.stream.write_all(b"\r\n")?;
                self.stream.write_all(self.buf.as_slice())?;
                self.stream.write_all(b"\r\n")?;
                self.len = 0;
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
