#[cfg(feature = "flate2")]
use flate2::read::GzDecoder;
use std::io::{BufRead, ErrorKind, Read, Write};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

const EXPECTED_DELIMITER: &[u8; 2] = b"\r\n";

/// Wrapper that will read the chunk size before reading the actual
/// data chunk.
/// We need a [BufRead] implementing type due to it's `read_line` method for reading the
/// data chunk length.
pub struct ExaRowReader<'a, R: Read + BufRead> {
    buf_reader: R,
    run: &'a Arc<AtomicBool>,
    compression: bool,
    buf: Vec<u8>,
}

impl<'a, R> ExaRowReader<'a, R>
where
    R: Read + BufRead,
{
    pub fn new(buf_reader: R, run: &'a Arc<AtomicBool>, compression: bool) -> Self {
        Self {
            buf_reader,
            run,
            compression,
            buf: Vec::new(),
        }
    }

    #[inline]
    fn read_chunk_size(&mut self) -> std::io::Result<u64> {
        let mut hex_len = String::new();
        self.buf_reader.read_line(&mut hex_len)?;
        let len_str = hex_len.trim_end();

        match len_str.is_empty() {
            true => Ok(0),
            false => u64::from_str_radix(len_str, 16).map_err(|_| {
                std::io::Error::new(ErrorKind::InvalidData, "Could not parse chunk hex length")
            }),
        }
    }

    /// Reads the data chunk from the underlying reader and expands it if needed.
    /// We need to store it in our own buffer because if compression is enabled we have no idea
    /// how much the chunk will decompress.
    #[inline]
    #[allow(unreachable_code)]
    fn store_chunk(&mut self, size: u64) -> std::io::Result<usize> {
        match self.compression {
            false => self
                .buf_reader
                .by_ref()
                .take(size)
                .read_to_end(&mut self.buf),
            true => {
                #[cfg(feature = "flate2")]
                return match size > 0 {
                    false => Ok(0),
                    true => GzDecoder::new(self.buf_reader.by_ref().take(size))
                        .read_to_end(&mut self.buf),
                };

                panic!("flate2 feature must be enabled to use compression");
            }
        }
    }

    /// Reads the length of the next chunk, the chunk itself, and the chunk delimiter.
    #[inline]
    fn read_chunk(&mut self) -> std::io::Result<usize> {
        let size = self.read_chunk_size()?;
        let num_bytes = self.store_chunk(size)?;
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
    }
}

impl<'a, R> Read for ExaRowReader<'a, R>
where
    R: Read + BufRead,
{
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        // Read chunk
        if self.run.load(Ordering::Acquire) {
            // Check if we still have data in our inner buffer
            if self.buf.len() == 0 {
                self.read_chunk()?;
            }

            // Read from inner buffer and consume bytes read.
            let num_bytes = self.buf.as_slice().read(buf)?;
            self.buf.drain(0..num_bytes);

            Ok(num_bytes)
        } else {
            Err(std::io::Error::new(
                ErrorKind::Other,
                "Stop signal encountered",
            ))
        }
    }
}
