use core::fmt;
use std::io::{Read, Seek, SeekFrom, Write};
use tracing::instrument;

use crate::{Error, Resampler};

use super::{Decoder, Timestamp};

#[derive(Debug)]
pub(crate) struct FileWithInlineMeta<F: fmt::Debug> {
    pub(crate) file_handle: F,
    pub(crate) line_size: usize,
}

pub(crate) fn lines_per_metainfo(payload_size: usize) -> usize {
    let base_lines = 2; // needed to recognise meta section
    let extra_lines_needed = match payload_size {
        0 => 2,
        1 => 2,
        2 => 1,
        3 => 1,
        4.. => 0,
    };
    base_lines + extra_lines_needed
}

pub(crate) fn bytes_per_metainfo(payload_size: usize) -> usize {
    lines_per_metainfo(payload_size) * (payload_size + 2)
}

fn decode<D: Decoder>(decoder: &mut D, line: &[u8]) -> (Timestamp, D::Item) {
    let small_ts: [u8; 2] = line[0..2].try_into().expect("line size is at least 2");
    let small_ts = u16::from_le_bytes(small_ts).into();
    let item = decoder.decode_line(&line[2..]);
    (small_ts, item)
}

impl<F: fmt::Debug + Read + Seek> FileWithInlineMeta<F> {
    pub(crate) fn inner_mut(&mut self) -> &mut F {
        &mut self.file_handle
    }

    pub(crate) fn read2<D: Decoder>(
        &mut self,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
        start_byte: u64,
        stop_byte: u64,
        first_full_ts: Timestamp,
    ) -> Result<(), Error> {
        let to_read = stop_byte - start_byte;
        let mut buf = vec![0; to_read as usize];
        self.file_handle.seek(SeekFrom::Start(start_byte))?;
        self.file_handle.read_exact(&mut buf)?;

        let mut full_ts = first_full_ts;
        let mut lines = buf.chunks_exact(self.line_size);
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

    pub(crate) fn read2_resampling<R: crate::Resampler>(
        &mut self,
        resampler: &mut R,
        bucket_size: usize,
        timestamps: &mut Vec<u64>,
        data: &mut Vec<<R as Decoder>::Item>,
        start_byte: u64,
        stop_byte: u64,
        first_full_ts: u64,
    ) -> Result<(), Error> {
        let to_read = stop_byte - start_byte;
        let mut buf = vec![0; to_read as usize];
        self.file_handle.seek(SeekFrom::Start(start_byte))?;
        self.file_handle.read_exact(&mut buf)?;

        let mut sampler = Sampler::new(resampler, bucket_size, timestamps, data);

        let mut full_ts = first_full_ts;
        let mut lines = buf.chunks_exact(self.line_size);
        loop {
            let Some(line) = lines.next() else {
                return Ok(());
            };
            if line[..2] != [0, 0] {
                sampler.process(line, full_ts);
                continue;
            }

            let Some(next_line) = lines.next() else {
                return Ok(());
            };
            if next_line[..2] != [0, 0] {
                sampler.process(line, full_ts);
                continue;
            }

            let Some(meta) = read_meta(lines.by_ref(), line, next_line) else {
                return Ok(());
            };
            full_ts = u64::from_le_bytes(meta);
        }
    }
}

use crate::ResampleState;

struct Sampler<'a, R: Resampler> {
    resampler: &'a mut R,
    resample_state: <R as Resampler>::State,
    timestamp_sum: u64,
    sampled: usize,

    bucket_size: usize,
    timestamps: &'a mut Vec<u64>,
    data: &'a mut Vec<<R as Decoder>::Item>,
}

impl<'a, R: Resampler> Sampler<'a, R> {
    fn new(
        resampler: &'a mut R,
        bucket_size: usize,
        timestamps: &'a mut Vec<u64>,
        data: &'a mut Vec<<R as Decoder>::Item>,
    ) -> Self {
        Self {
            resample_state: resampler.state(),
            resampler,
            timestamp_sum: 0,
            sampled: 0,
            bucket_size,
            timestamps,
            data,
        }
    }

    fn process(&mut self, line: &[u8], full_ts: u64) {
        let (small_ts, item) = decode(self.resampler, line);
        self.timestamp_sum += full_ts + small_ts;
        self.resample_state.add(item);
        self.sampled += 1;
        if self.sampled >= self.bucket_size {
            self.timestamps
                .push(self.timestamp_sum / self.bucket_size as u64);
            self.data.push(self.resample_state.finish(self.bucket_size));
        }
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
}

/// returns number of bytes written
#[instrument(level = "trace", skip(file_handle), ret)]
pub(crate) fn write_meta(
    file_handle: &mut impl Write,
    meta: [u8; 8],
    payload_size: usize,
) -> std::io::Result<u64> {
    tracing::info!("inserting full timestamp through meta lines");
    let t = meta;
    let lines = match payload_size {
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
            let mut line = vec![0u8; payload_size + 2];
            line[2..6].copy_from_slice(&[t[0], t[1], t[2], t[3]]);
            file_handle.write_all(&line)?;
            line[2..6].copy_from_slice(&[t[4], t[5], t[6], t[7]]);
            file_handle.write_all(&line)?;
            2
        }
    };
    Ok(lines * (payload_size + 2) as u64)
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
            result[0..2].copy_from_slice(chunks.next()?);
            result[2..4].copy_from_slice(chunks.next()?);
            result[4..6].copy_from_slice(chunks.next()?);
            result[6..8].copy_from_slice(chunks.next()?);
        }
        1 => {
            result[0] = first_chunk[2];
            result[1] = next_chunk[2];
            result[2..5].copy_from_slice(chunks.next()?);
            result[5..8].copy_from_slice(chunks.next()?);
        }
        2 => {
            result[0..2].copy_from_slice(&first_chunk[2..]);
            result[2..4].copy_from_slice(&next_chunk[2..]);
            result[4..8].copy_from_slice(chunks.next()?);
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

// #[cfg(test)]
// mod test {
//     use std::io::Cursor;
//     use std::io::Read;
//
//     use super::*;
//     const LINE_SIZE: usize = 6;
//     const TWO_ZERO_LINES: [u8; 2 * LINE_SIZE] = [0u8; 2 * LINE_SIZE];
//
//     fn data_lines<const N: usize>(n: usize) -> Vec<u8> {
//         assert!(N >= 2);
//         (1..(n + 1))
//             .into_iter()
//             .map(|i| {
//                 let mut line = [0u8; N];
//                 line[0] = (i % u8::MAX as usize) as u8;
//                 line[1] = (i / u8::MAX as usize) as u8;
//                 line
//             })
//             .flatten()
//             .collect()
//     }
//
//     // #[test]
//     // fn meta_section_at_start() {
//     //     let n_data_lines = 5;
//     //     let mut lines = TWO_ZERO_LINES.to_vec();
//     //     lines.extend_from_slice(&data_lines::<LINE_SIZE>(n_data_lines));
//     //
//     //     let file = Cursor::new(lines);
//     //     let mut file = FileWithInlineMeta {
//     //         file_handle: file,
//     //         line_size: LINE_SIZE,
//     //     };
//     //
//     //     let mut buf = vec![0u8; 100];
//     //     let n_read = file.read(&mut buf).unwrap();
//     //     let read = &buf[0..n_read];
//     //     assert_eq!(read, &data_lines::<LINE_SIZE>(n_data_lines))
//     // }
//     //
//     // #[test]
//     // fn meta_section_at_end() {
//     //     let n_data_lines = 5;
//     //     let mut lines = data_lines::<LINE_SIZE>(n_data_lines);
//     //     lines.extend_from_slice(&TWO_ZERO_LINES);
//     //
//     //     let file = Cursor::new(lines);
//     //     let mut file = FileWithInlineMeta {
//     //         file_handle: file,
//     //         line_size: LINE_SIZE,
//     //     };
//     //
//     //     let mut buf = vec![0u8; 100];
//     //     let n_read = file.read(&mut buf).unwrap();
//     //     let read = &buf[0..n_read];
//     //     assert_eq!(read, &data_lines::<LINE_SIZE>(n_data_lines))
//     // }
//
//     // #[test]
//     // fn meta_sections_around() {
//     //     let n_data_lines = 2;
//     //     let mut lines = TWO_ZERO_LINES.to_vec();
//     //     lines.extend_from_slice(&data_lines::<LINE_SIZE>(n_data_lines));
//     //     lines.extend_from_slice(&TWO_ZERO_LINES);
//     //
//     //     let file = Cursor::new(lines);
//     //     let mut file = FileWithInlineMeta {
//     //         file_handle: file,
//     //         line_size: LINE_SIZE,
//     //     };
//     //
//     //     let mut buf = vec![0u8; 100];
//     //     let n_read = file.read(&mut buf).unwrap();
//     //     let read = &buf[0..n_read];
//     //     assert_eq!(read, &data_lines::<LINE_SIZE>(n_data_lines))
//     // }
// }
