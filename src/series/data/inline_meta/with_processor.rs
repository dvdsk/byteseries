use core::fmt;
use std::io::{Read, Seek, SeekFrom};
use tracing::{instrument, warn};

use crate::{CorruptionCallback, Pos};

use super::{meta, FileWithInlineMeta, SetLen, Timestamp};

// to make it easy for users writing Processors this does
// not implement std::core::Error
#[derive(Debug)]
pub(crate) enum Error<E> {
    Io(std::io::Error),
    Processor(E),
    CorruptMetaSection,
}

impl<E> From<std::io::Error> for Error<E> {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

fn ts_from(line: &[u8], full_ts: u64) -> u64 {
    let small_ts: [u8; 2] = line[0..2].try_into().expect("slice len is 2");
    let small_ts: u64 = u16::from_le_bytes(small_ts).into();

    full_ts + small_ts
}

impl<F: fmt::Debug + Read + Seek + SetLen> FileWithInlineMeta<F> {
    #[instrument(level = "debug", skip(processor, corruption_callback))]
    pub(crate) fn read_with_processor<E: std::fmt::Debug>(
        &mut self,
        seek: Pos,
        corruption_callback: &mut Option<CorruptionCallback>,
        mut processor: impl FnMut(Timestamp, &[u8]) -> Result<(), E>,
    ) -> Result<(), Error<E>> {
        let mut to_read = seek.end - seek.start.raw_offset();
        let chunk_size = 16384usize.next_multiple_of(self.payload_size.line_size());
        // meta section decoding can need at most 5 lines of overlap.
        let max_needed_overlap = (3 + 2) * self.payload_size.line_size();
        let mut buf = vec![0; chunk_size + max_needed_overlap];

        self.file_handle
            .seek(SeekFrom::Start(seek.start.raw_offset()))?;

        let mut skipping_over_corrupted_data = false;
        let mut needed_overlap = 0;
        let mut meta_ts = seek.first_full_ts;
        let mut read_size = 0;

        while to_read > 0 {
            // move needed overlap to start of next read
            let overlap = (read_size - needed_overlap)..read_size;
            buf.copy_within(overlap, 0);

            read_size = chunk_size.min(usize::try_from(to_read).unwrap_or(usize::MAX));
            to_read -= read_size as u64;
            self.file_handle
                .read_exact(&mut buf[needed_overlap..needed_overlap + read_size])?;
            let mut lines = buf[..needed_overlap + read_size]
                .chunks_exact(self.payload_size.line_size());

            needed_overlap = loop {
                let Some(line) = lines.next() else {
                    break 0;
                };

                if line[..2] != meta::PREAMBLE && !skipping_over_corrupted_data {
                    let debug_res = processor(ts_from(line, meta_ts), &line[2..])
                        .map_err(Error::Processor);
                    debug_res?;

                    continue;
                }

                let Some(next_line) = lines.next() else {
                    break self.payload_size.line_size();
                };

                // the break with needed_overlap ensures a new read always starts
                // before a meta section and never in between.
                if next_line[..2] != meta::PREAMBLE {
                    if let Some(corruption_accepted) = corruption_callback {
                        if corruption_accepted() {
                            continue;
                        } else {
                            return Err(Error::CorruptMetaSection);
                        }
                    } else {
                        return Err(Error::CorruptMetaSection);
                    }
                }

                skipping_over_corrupted_data = false;
                match meta::read(lines.by_ref(), line, next_line) {
                    meta::Result::Meta { meta } => {
                        meta_ts = u64::from_le_bytes(meta);
                    }
                    meta::Result::OutOfLines { consumed_lines } => {
                        break (2 + consumed_lines) * self.payload_size.line_size();
                    }
                };
            };
        }
        Ok(())
    }
}
