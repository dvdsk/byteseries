use crate::series::data::PayloadSize;
use std::io::Write;
use tracing::instrument;

pub(crate) const PREAMBLE: [u8; 2] = [0b1111_1111, 0b1111_1111];

pub(crate) fn lines_per_metainfo(payload_size: usize) -> usize {
    match payload_size {
        0 => 6,
        1 => 4,
        2 | 3 => 3,
        4.. => 2,
    }
}

// pub(crate) fn bytes_per_metainfo(payload_size: PayloadSize) -> usize {
//     lines_per_metainfo(payload_size.0) * payload_size.line_size()
// }

/// returns number of bytes written
#[instrument(level = "trace", skip(file_handle), ret)]
pub(crate) fn write(
    file_handle: &mut impl Write,
    meta: [u8; 8],
    payload_size: PayloadSize,
) -> std::io::Result<u64> {
    let t = meta;
    let lines = match payload_size.raw() {
        0 => {
            file_handle.write_all(&PREAMBLE)?;
            file_handle.write_all(&PREAMBLE)?;
            file_handle.write_all(&t[0..2])?;
            file_handle.write_all(&t[2..4])?;
            file_handle.write_all(&t[4..6])?;
            file_handle.write_all(&t[6..8])?;
            6
        }
        1 => {
            file_handle.write_all(&[PREAMBLE[0], PREAMBLE[1], t[0]])?;
            file_handle.write_all(&[PREAMBLE[0], PREAMBLE[1], t[1]])?;
            file_handle.write_all(&t[2..5])?;
            file_handle.write_all(&t[5..8])?;
            4
        }
        2 => {
            file_handle.write_all(&[PREAMBLE[0], PREAMBLE[1], t[0], t[1]])?;
            file_handle.write_all(&[PREAMBLE[0], PREAMBLE[1], t[2], t[3]])?;
            file_handle.write_all(&t[4..8])?;
            3
        }
        3 => {
            file_handle.write_all(&[PREAMBLE[0], PREAMBLE[1], t[0], t[1], t[2]])?;
            file_handle.write_all(&[PREAMBLE[0], PREAMBLE[1], t[3], t[4], t[5]])?;
            file_handle.write_all(&[t[6], t[7], 0, 0, 0])?;
            3
        }
        4.. => {
            let mut line = vec![0; payload_size.line_size()];
            line[0..2].copy_from_slice(&PREAMBLE);
            line[2..6].copy_from_slice(&[t[0], t[1], t[2], t[3]]);
            file_handle.write_all(&line)?;
            line[0..2].copy_from_slice(&PREAMBLE);
            line[2..6].copy_from_slice(&[t[4], t[5], t[6], t[7]]);
            file_handle.write_all(&line)?;
            2
        }
    };
    Ok(lines * (payload_size.line_size()) as u64)
}

#[derive(Debug)]
pub(crate) enum Result {
    OutOfLines { consumed_lines: usize },
    Meta { meta: [u8; 8] },
}
/// returns None if not enough data was left to decode a u64
#[instrument(level = "trace", skip(chunks))]
pub(crate) fn read<'a>(
    mut chunks: impl Iterator<Item = &'a [u8]>,
    first_chunk: &'a [u8],
    next_chunk: &'a [u8],
) -> Result {
    let mut result = [0u8; 8];
    let payload_size = first_chunk.len() - 2;
    match payload_size {
        0 => {
            result[0..2].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 0 },
                Some(chunk) => chunk,
            });
            result[2..4].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 1 },
                Some(chunk) => chunk,
            });
            result[4..6].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 2 },
                Some(chunk) => chunk,
            });
            result[6..8].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 3 },
                Some(chunk) => chunk,
            });
        }
        1 => {
            result[0] = first_chunk[2];
            result[1] = next_chunk[2];
            result[2..5].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 0 },
                Some(chunk) => chunk,
            });
            result[5..8].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 1 },
                Some(chunk) => chunk,
            });
        }
        2 => {
            result[0..2].copy_from_slice(&first_chunk[2..]);
            result[2..4].copy_from_slice(&next_chunk[2..]);
            result[4..8].copy_from_slice(match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 0 },
                Some(chunk) => chunk,
            });
        }
        3 => {
            result[0..3].copy_from_slice(&first_chunk[2..]);
            result[3..6].copy_from_slice(&next_chunk[2..]);
            let chunk = match chunks.next() {
                None => return Result::OutOfLines { consumed_lines: 0 },
                Some(chunk) => chunk,
            };
            result[6..8].copy_from_slice(&chunk[0..2]);
        }
        4.. => {
            result[0..4].copy_from_slice(&first_chunk[2..6]);
            result[4..8].copy_from_slice(&next_chunk[2..6]);
        }
    }

    Result::Meta { meta: result }
}
