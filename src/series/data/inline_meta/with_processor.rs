use core::fmt;
use std::io::{Read, Seek, SeekFrom};
use tracing::{instrument, warn};

use crate::Pos;

use super::{meta, FileWithInlineMeta, SetLen, Timestamp};

#[derive(Debug)]
pub(crate) enum Error<E> {
    Io(std::io::Error),
    Processor(E),
}

impl<E> From<std::io::Error> for Error<E> {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

impl<E: fmt::Debug> Error<E> {
    pub fn unwrap_io(self) -> std::io::Error {
        match self {
            Error::Io(e) => e,
            Error::Processor(e) => panic!(
                "Attempt to unwrap with_processor::Error as \
                Io error but was Processor error: {e:?}"
            ),
        }
    }
}

fn small_ts(line: &[u8]) -> u64 {
    let small_ts: [u8; 2] = line[0..2].try_into().expect("slice len is 2");
    u16::from_le_bytes(small_ts).into()
}

impl<F: fmt::Debug + Read + Seek + SetLen> FileWithInlineMeta<F> {
    #[instrument(level = "debug", skip(processor))]
    pub(crate) fn read_with_processor<E: std::fmt::Debug>(
        &mut self,
        seek: Pos,
        mut processor: impl FnMut(Timestamp, &[u8]) -> Result<(), E>,
    ) -> Result<(), Error<E>> {
        let mut to_read = seek.end - seek.start.raw_offset();
        let chunk_size = 16384usize.next_multiple_of(self.payload_size.line_size());
        // meta section decoding can read at most 3 lines, reading a 4th will always
        // conclude with a successful decode
        let max_needed_overlap = 3 * self.payload_size.line_size();
        let mut buf = vec![0; chunk_size + max_needed_overlap];

        self.file_handle
            .seek(SeekFrom::Start(seek.start.raw_offset()))?;

        let mut needed_overlap = 0;
        let mut full_ts = seek.first_full_ts;
        while to_read > 0 {
            let read_size =
                chunk_size.min(usize::try_from(to_read).unwrap_or(usize::MAX));
            self.file_handle
                .read_exact(&mut buf[needed_overlap..needed_overlap + read_size])?;
            to_read -= read_size as u64;
            let mut lines = buf[..needed_overlap + read_size]
                .chunks_exact(self.payload_size.line_size());

            needed_overlap = loop {
                let Some(line) = lines.next() else {
                    break 0;
                };

                if line[..2] != meta::PREAMBLE {
                    processor(small_ts(line) + full_ts, &line[2..])
                        .map_err(Error::Processor)?;
                    continue;
                }

                let Some(next_line) = lines.next() else {
                    if to_read == 0 {
                        // take care of the last item
                        processor(small_ts(line) + full_ts, &line[2..])
                            .map_err(Error::Processor)?;
                    }
                    break self.payload_size.line_size();
                };

                if next_line[..2] != meta::PREAMBLE {
                    processor(small_ts(line) + full_ts, &line[2..])
                        .map_err(Error::Processor)?;
                    processor(small_ts(next_line) + full_ts, &next_line[2..])
                        .map_err(Error::Processor)?;
                    continue;
                }

                match meta::read(lines.by_ref(), line, next_line) {
                    meta::Result::Meta {
                        meta,
                        line_after_meta,
                    } => {
                        full_ts = u64::from_le_bytes(meta);
                        processor(full_ts, &line_after_meta[2..])
                            .map_err(Error::Processor)?;
                    }
                    meta::Result::OutOfLines { consumed_lines } => {
                        break consumed_lines * self.payload_size.line_size();
                    }
                };
            };
        }
        Ok(())
    }
}
