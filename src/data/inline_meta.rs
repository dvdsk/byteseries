use core::fmt;
use std::io::{Read, Seek, SeekFrom, Write};
use std::iter;

use itertools::Itertools;
use tracing::instrument;

use crate::Error;

use super::{Decoder2, Timestamp};

#[derive(Debug)]
pub(crate) struct FileWithInlineMeta<F: fmt::Debug> {
    pub(crate) file_handle: F,
    pub(crate) full_line_size: usize,
}

pub(crate) fn meta_lines_indices(buf: &[u8], full_line_size: usize) -> Vec<usize> {
    buf.chunks_exact(full_line_size)
        .enumerate()
        .tuple_windows::<(_, _)>()
        .filter(|((_, line_a), _)| line_a[0..2] == [0, 0])
        .filter(|(_, (_, line_b))| line_b[0..2] == [0, 0])
        .map(|((line_numb, _), (_, _))| line_numb)
        .map(|line| line * full_line_size)
        .collect()
}

pub(crate) fn lines_per_metainfo(line_size: usize) -> usize {
    let base_lines = 2; // needed to recognise meta section
    let extra_lines_needed = match line_size {
        0 => 2,
        1 => 2,
        2 => 1,
        3 => 1,
        4.. => 0,
    };
    base_lines + extra_lines_needed
}

fn shift_over_meta_lines(buf: &mut [u8], meta_lines: Vec<usize>, full_line_size: usize) -> usize {
    let n_meta_lines = lines_per_metainfo(full_line_size - 2);
    let n_meta_bytes = n_meta_lines * full_line_size;
    let mut shifted = 0;
    let total_to_shift = meta_lines.len() * n_meta_bytes;
    for (start, end) in meta_lines
        .into_iter()
        .chain(iter::once(buf.len() - total_to_shift))
        .tuple_windows::<(_, _)>()
    {
        for i in start..end {
            buf[i] = buf[i + n_meta_bytes + shifted];
        }
        shifted += n_meta_bytes;
    }
    shifted
}

fn decode<D: Decoder2>(decoder: &mut D, line: &[u8]) -> (Timestamp, D::Item) {
    let small_ts: [u8; 2] = line[0..2].try_into().expect("line size is at least 2");
    let small_ts = u16::from_le_bytes(small_ts).into();
    let item = decoder.decode_line(&line[2..]);
    (small_ts, item)
}

impl<F: fmt::Debug + Read + Seek> FileWithInlineMeta<F> {
    pub(crate) fn inner_mut(&mut self) -> &mut F {
        &mut self.file_handle
    }

    pub(crate) fn read2<D: Decoder2>(
        &mut self,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
        start_byte: u64,
        stop_byte: u64,
        first_full_ts: Timestamp,
    ) -> Result<(), Error> {
        let n_lines = (stop_byte - start_byte) / self.full_line_size as u64;
        let mut buf = vec![0; n_lines as usize];
        self.file_handle.seek(SeekFrom::Start(start_byte))?;
        self.file_handle.read_exact(&mut buf)?;

        let mut full_ts = first_full_ts;
        let mut lines = buf.chunks_exact(self.full_line_size);
        loop {
            let Some(line) = lines.next() else {
                return Ok(());
            };
            if line[..2] != [0, 0] {
                let (small_ts, item) = decode(decoder, line);
                timestamps.push(small_ts + full_ts);
                data.push(item);
                continue;
            }

            let Some(next_line) = lines.next() else {
                return Ok(());
            };
            if next_line[..2] != [0, 0] {
                let (small_ts, item) = decode(decoder, next_line);
                timestamps.push(small_ts + full_ts);
                data.push(item);
                continue;
            }

            let Some(meta) = read_meta(lines.by_ref(), line, next_line) else {
                return Ok(());
            };
            full_ts = u64::from_le_bytes(meta);
        }
    }
}

impl<F: Read + fmt::Debug> Read for FileWithInlineMeta<F> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        let mut read_ignoring_meta = 0;

        while read_ignoring_meta == 0 {
            let n_read = self.file_handle.read(buf)?;
            let mut read = &mut buf[..n_read];
            if read.is_empty() {
                return Ok(0);
            }

            let meta_lines = meta_lines_indices(read, self.full_line_size);
            let shifted = shift_over_meta_lines(&mut read, meta_lines, self.full_line_size);

            read_ignoring_meta = n_read - shifted;
        }

        Ok(read_ignoring_meta)
    }
}

impl<F: Write + fmt::Debug> Write for FileWithInlineMeta<F> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.file_handle.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.file_handle.flush()
    }
}

impl<F: Seek + fmt::Debug> Seek for FileWithInlineMeta<F> {
    fn seek(&mut self, pos: SeekFrom) -> std::io::Result<u64> {
        self.file_handle.seek(pos)
    }
    fn stream_position(&mut self) -> std::io::Result<u64> {
        dbg!(self.file_handle.stream_position())
    }
}

/// returns number of bytes written
#[instrument(level = "trace", skip(file_handle), ret)]
pub(crate) fn write_meta(
    file_handle: &mut impl Write,
    meta: [u8; 8],
    line_size: usize,
) -> std::io::Result<u64> {
    tracing::info!("inserting full timestamp through meta lines");
    let t = meta;
    let lines = match line_size {
        0 => {
            file_handle.write_all(&[0, 0])?;
            file_handle.write_all(&[0, 0])?;
            file_handle.write_all(&t[0..2])?;
            file_handle.write_all(&t[2..4])?;
            file_handle.write_all(&t[4..6])?;
            file_handle.write_all(&t[6..8])?;
            6
        }
        1 => {
            file_handle.write_all(&[0, 0, t[0]])?;
            file_handle.write_all(&[0, 0, t[1]])?;
            file_handle.write_all(&t[2..5])?;
            file_handle.write_all(&t[5..8])?;
            4
        }
        2 => {
            file_handle.write_all(&[0, 0, t[0], t[1]])?;
            file_handle.write_all(&[0, 0, t[2], t[3]])?;
            file_handle.write_all(&t[4..8])?;
            3
        }
        3 => {
            file_handle.write_all(&[0, 0, t[0], t[1], t[2]])?;
            file_handle.write_all(&[0, 0, t[3], t[4], t[5]])?;
            file_handle.write_all(&[t[6], t[7], 0, 0, 0])?;
            3
        }
        4.. => {
            let mut line = vec![0u8; line_size + 2];
            line[2..6].copy_from_slice(&[t[0], t[1], t[2], t[3]]);
            file_handle.write_all(&line)?;
            line[2..6].copy_from_slice(&[t[4], t[5], t[6], t[7]]);
            file_handle.write_all(&line)?;
            2
        }
    };
    Ok(lines * (line_size + 2) as u64)
}

/// returns None if not enough data was left to decode a u64
#[instrument(level = "trace", skip(chunks), ret)]
pub(crate) fn read_meta<'a>(
    mut chunks: impl Iterator<Item = &'a [u8]>,
    first_chunk: &'a [u8],
    next_chunk: &'a [u8],
) -> Option<[u8; 8]> {
    let mut result = [0u8; 8];
    match first_chunk.len() - 2 {
        0 => {
            result[0..2].copy_from_slice(&chunks.next()?);
            result[2..4].copy_from_slice(&chunks.next()?);
            result[4..6].copy_from_slice(&chunks.next()?);
            result[6..8].copy_from_slice(&chunks.next()?);
        }
        1 => {
            result[0] = first_chunk[2];
            result[1] = next_chunk[2];
            result[2..5].copy_from_slice(&chunks.next()?);
            result[5..8].copy_from_slice(&chunks.next()?);
        }
        2 => {
            result[0..2].copy_from_slice(&first_chunk[2..]);
            result[2..4].copy_from_slice(&next_chunk[2..]);
            result[4..8].copy_from_slice(&chunks.next()?);
        }
        3 => {
            result[0..3].copy_from_slice(&first_chunk[2..]);
            result[3..6].copy_from_slice(&next_chunk[2..]);
            result[6..8].copy_from_slice(&chunks.next()?[0..2]);
        }
        4.. => {
            result[0..4].copy_from_slice(&first_chunk[2..6]);
            result[4..8].copy_from_slice(&next_chunk[2..6]);
        }
    }

    Some(result)
}

#[cfg(test)]
mod test {
    use std::io::Cursor;
    use std::io::Read;

    use super::*;
    const FULL_LINE_SIZE: usize = 6;
    const TWO_ZERO_LINES: [u8; 2 * FULL_LINE_SIZE] = [0u8; 2 * FULL_LINE_SIZE];

    fn data_lines<const N: usize>(n: usize) -> Vec<u8> {
        assert!(N >= 2);
        (1..(n + 1))
            .into_iter()
            .map(|i| {
                let mut line = [0u8; N];
                line[0] = (i % u8::MAX as usize) as u8;
                line[1] = (i / u8::MAX as usize) as u8;
                line
            })
            .flatten()
            .collect()
    }

    #[test]
    fn meta_section_at_start() {
        let n_data_lines = 5;
        let mut lines = TWO_ZERO_LINES.to_vec();
        lines.extend_from_slice(&data_lines::<FULL_LINE_SIZE>(n_data_lines));

        let file = Cursor::new(lines);
        let mut file = FileWithInlineMeta {
            file_handle: file,
            full_line_size: FULL_LINE_SIZE,
        };

        let mut buf = vec![0u8; 100];
        let n_read = file.read(&mut buf).unwrap();
        let read = &buf[0..n_read];
        assert_eq!(read, &data_lines::<FULL_LINE_SIZE>(n_data_lines))
    }

    #[test]
    fn meta_section_at_end() {
        let n_data_lines = 5;
        let mut lines = data_lines::<FULL_LINE_SIZE>(n_data_lines);
        lines.extend_from_slice(&TWO_ZERO_LINES);

        let file = Cursor::new(lines);
        let mut file = FileWithInlineMeta {
            file_handle: file,
            full_line_size: FULL_LINE_SIZE,
        };

        let mut buf = vec![0u8; 100];
        let n_read = file.read(&mut buf).unwrap();
        let read = &buf[0..n_read];
        assert_eq!(read, &data_lines::<FULL_LINE_SIZE>(n_data_lines))
    }

    #[test]
    fn meta_sections_around() {
        let n_data_lines = 2;
        let mut lines = TWO_ZERO_LINES.to_vec();
        lines.extend_from_slice(&data_lines::<FULL_LINE_SIZE>(n_data_lines));
        lines.extend_from_slice(&TWO_ZERO_LINES);

        let file = Cursor::new(lines);
        let mut file = FileWithInlineMeta {
            file_handle: file,
            full_line_size: FULL_LINE_SIZE,
        };

        let mut buf = vec![0u8; 100];
        let n_read = file.read(&mut buf).unwrap();
        let read = &buf[0..n_read];
        assert_eq!(read, &data_lines::<FULL_LINE_SIZE>(n_data_lines))
    }
}
