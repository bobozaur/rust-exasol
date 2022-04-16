use std::io::{BufRead, BufReader, ErrorKind, Read};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const EXPECTED_DELIMITER: &[u8; 2] = b"\r\n";

/// Wrapper that will read the chunk size before reading the actual
/// data chunk.
/// We'll resort to a [BufReader] because the `read_line` method
/// comes in handy for reading the size as it ends in `\r\n`.
pub struct ExaRowReader<'a, R: Read> {
    buf_reader: BufReader<R>,
    run: &'a Arc<AtomicBool>,
}

impl<'a, R> ExaRowReader<'a, R>
where
    R: Read,
{
    pub fn new(stream: R, run: &'a Arc<AtomicBool>) -> Self {
        Self {
            buf_reader: BufReader::new(stream),
            run,
        }
    }

    fn read_chunk_size(&mut self) -> std::io::Result<u64> {
        let mut hex_len = String::new();
        self.buf_reader.read_line(&mut hex_len)?;

        match hex_len.is_empty() {
            true => Ok(0),
            false => u64::from_str_radix(hex_len.trim_end(), 16).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Could not parse chunk hex length")
            }),
        }
    }
}

impl<'a, R> Read for ExaRowReader<'a, R>
where
    R: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Read chunk
        if self.run.load(Ordering::Acquire) {
            let size = self.read_chunk_size()?;
            let num_bytes = self.buf_reader.by_ref().take(size).read(buf)?;

            // Check chunk end for trailing delimiter
            let mut delimiter = [0; 2];
            self.buf_reader.read_exact(&mut delimiter)?;

            // Check that the chunk was delimited as expected.
            // The delimiter does not make it into the buffer,
            // as it represents the ending of a chunk, not a row.
            match &delimiter == EXPECTED_DELIMITER {
                true => Ok(()),
                false => Err(std::io::Error::new(
                    ErrorKind::InvalidData,
                    format!("Invalid chunk delimiter: {:?}", delimiter),
                )),
            }?;

            Ok(num_bytes)
        } else {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Stop signal encountered",
            ))
        }
    }
}

impl<'a, R> BufRead for ExaRowReader<'a, R>
where
    R: Read,
{
    fn fill_buf(&mut self) -> std::io::Result<&[u8]> {
        self.buf_reader.fill_buf()
    }

    fn consume(&mut self, amt: usize) {
        self.buf_reader.consume(amt)
    }
}
