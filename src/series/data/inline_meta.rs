use core::fmt;
use itertools::Itertools;
use std::io::{self, Read, Seek, SeekFrom, Write};
use tracing::{instrument, trace};

use crate::{Resampler, SeekPos};

use super::{Decoder, Timestamp};

#[derive(Debug)]
pub(crate) struct FileWithInlineMeta<F: fmt::Debug> {
    pub(crate) file_handle: F,
    pub(crate) line_size: usize,
}

pub(crate) fn lines_per_metainfo(payload_size: usize) -> usize {
    let base_lines = 2; // needed to recognise meta section
    let extra_lines_needed = match payload_size {
        0 | 1 => 2,
        2 | 3 => 1,
        4.. => 0,
    };
    base_lines + extra_lines_needed
}

pub(crate) fn bytes_per_metainfo(payload_size: usize) -> usize {
    lines_per_metainfo(payload_size) * (payload_size + 2)
}

pub(crate) trait SetLen {
    fn len(&self) -> Result<u64, std::io::Error>;
    fn set_len(&mut self, len: u64) -> Result<(), std::io::Error>;
}

impl<F: fmt::Debug + Read + Seek + SetLen> FileWithInlineMeta<F> {
    pub(crate) fn new(mut file: F, payload_size: usize) -> Result<Self, std::io::Error> {
        'check_and_repair: {
            if repaired_is_only_meta(&mut file, payload_size)? {
                break 'check_and_repair;
            }

            if removed_partial_meta_at_end(&mut file, payload_size)? {
                break 'check_and_repair;
            }

            if removed_start_of_meta_at_end(&mut file, payload_size)? {
                break 'check_and_repair;
            }
        }

        Ok(FileWithInlineMeta {
            file_handle: file,
            line_size: payload_size + 2,
        })
    }

    pub(crate) fn inner_mut(&mut self) -> &mut F {
        &mut self.file_handle
    }

    #[instrument(level = "debug", skip(self, decoder, timestamps, data))]
    pub(crate) fn read<D: Decoder>(
        &mut self,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
        seek: SeekPos,
    ) -> Result<(), std::io::Error> {
        trace!("reading data: {seek:?}");
        self.read_with_processor(seek, |ts, payload| {
            let item = decoder.decode_payload(payload);
            data.push(item);
            timestamps.push(ts);
        })
    }

    #[instrument(level = "debug", skip(self, resampler, timestamps, data))]
    pub(crate) fn read_resampling<R: crate::Resampler>(
        &mut self,
        resampler: &mut R,
        bucket_size: usize,
        timestamps: &mut Vec<u64>,
        data: &mut Vec<<R as Decoder>::Item>,
        seek: SeekPos,
    ) -> Result<(), std::io::Error> {
        trace!("reading data while resampling: {seek:?}");
        let mut sampler = Sampler::new(resampler, bucket_size, timestamps, data);
        self.read_with_processor(seek, |ts, payload| {
            sampler.process(ts, payload);
        })
    }

    #[instrument(level = "debug", skip(processor))]
    pub(crate) fn read_with_processor(
        &mut self,
        seek: SeekPos,
        mut processor: impl FnMut(Timestamp, &[u8]),
    ) -> Result<(), std::io::Error> {
        let mut to_read = seek.end - seek.start;
        let chunk_size = 16384usize.next_multiple_of(self.line_size);
        let mut buf = vec![0; chunk_size];

        self.file_handle.seek(SeekFrom::Start(seek.start))?;

        let mut needed_overlap = 0;
        let mut full_ts = seek.first_full_ts;
        while to_read > 0 {
            let read_size = chunk_size.min(usize::try_from(to_read).unwrap_or(usize::MAX));
            self.file_handle
                .read_exact(&mut buf[needed_overlap..needed_overlap + read_size])?;
            to_read -= read_size as u64;
            let mut lines = buf[..needed_overlap + read_size].chunks_exact(self.line_size);

            needed_overlap = loop {
                let Some(line) = lines.next() else {
                    break 0;
                };
                if line[..2] != [0, 0] {
                    let small_ts: [u8; 2] = line[0..2].try_into().expect("slice len is 2");
                    let small_ts: u64 = u16::from_le_bytes(small_ts).into();
                    processor(small_ts + full_ts, &line[2..]);
                    continue;
                }

                let Some(next_line) = lines.next() else {
                    if to_read == 0 {
                        // take care of the last item
                        let small_ts: [u8; 2] = line[0..2].try_into().expect("slice len is 2");
                        let small_ts: u64 = u16::from_le_bytes(small_ts).into();
                        processor(small_ts + full_ts, &line[2..]);
                    }
                    break 1;
                };
                if next_line[..2] != [0, 0] {
                    let small_ts: [u8; 2] = line[0..2].try_into().expect("len is 2");
                    let small_ts: u64 = u16::from_le_bytes(small_ts).into();
                    processor(small_ts + full_ts, &line[2..]);
                    let small_ts: [u8; 2] = next_line[0..2].try_into().expect("len is 2");
                    let small_ts: u64 = u16::from_le_bytes(small_ts).into();
                    processor(small_ts + full_ts, &next_line[2..]);
                    continue;
                }

                // mod so it returns needed overlap
                match read_meta(lines.by_ref(), line, next_line) {
                    MetaResult::Meta(meta) => full_ts = u64::from_le_bytes(meta),
                    MetaResult::OutOfLines { consumed } => break 2 + consumed,
                };
            };

            if to_read == 0 {}
        }
        Ok(())
    }
}

fn removed_start_of_meta_at_end<F: fmt::Debug + Read + Seek + SetLen>(
    file: &mut F,
    payload_size: usize,
) -> Result<bool, io::Error> {
    file.seek(SeekFrom::Start(
        file.len()? - bytes_per_metainfo(payload_size) as u64 - (payload_size + 2) as u64,
    ))?;
    let mut to_check = vec![0u8; 2 * (payload_size + 2)];
    file.read_exact(&mut to_check)?;
    let mut lines = to_check.chunks_exact(payload_size + 2);
    let last_line = lines.by_ref().last().expect("read multiple lines");
    let meta_start_before_last_line = lines.by_ref().take(2).all(|line| line[0..2] == [0, 0]);
    // unless there is a meta section directly before it time zero lines
    // are not allowed.
    if last_line[0..2] == [0, 0] && !meta_start_before_last_line {
        file.set_len(file.len()? - (payload_size + 2) as u64)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn removed_partial_meta_at_end<F: fmt::Debug + Read + Seek + SetLen>(
    file: &mut F,
    payload_size: usize,
) -> Result<bool, io::Error> {
    file.seek(SeekFrom::Start(
        file.len()? - bytes_per_metainfo(payload_size) as u64,
    ))?;
    let mut to_check = vec![0u8; bytes_per_metainfo(payload_size)];
    file.read_exact(&mut to_check)?;
    let lines = to_check.chunks_exact(payload_size + 2);
    let meta_section_start = lines
        .tuple_windows()
        .position(|(a, b)| (a[0..2] == [0; 0] || b[0..2] == [0; 0]));

    if let Some(pos) = meta_section_start {
        file.set_len(
            file.len()? - bytes_per_metainfo(payload_size) as u64
                + pos as u64 * (payload_size + 2) as u64,
        )?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn repaired_is_only_meta<F: fmt::Debug + Read + Seek + SetLen>(
    file: &mut F,
    payload_size: usize,
) -> Result<bool, io::Error> {
    Ok(if file.len()? <= bytes_per_metainfo(payload_size) as u64 {
        file.set_len(0)?;
        true
    } else {
        false
    })
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
        assert!(bucket_size > 0, "bucket_size should be > zero");
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

    fn process(&mut self, ts: Timestamp, payload: &[u8]) {
        let item = self.resampler.decode_payload(payload);
        self.timestamp_sum += ts;
        self.resample_state.add(item);
        self.sampled += 1;
        if self.sampled >= self.bucket_size {
            self.timestamps
                .push(self.timestamp_sum / self.bucket_size as u64);
            self.data.push(self.resample_state.finish(self.bucket_size));
            self.timestamp_sum = 0;
            self.sampled = 0;
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

#[derive(Debug)]
pub(crate) enum MetaResult {
    OutOfLines { consumed: usize },
    Meta([u8; 8]),
}
/// returns None if not enough data was left to decode a u64
#[instrument(level = "trace", skip(chunks), ret)]
pub(crate) fn read_meta<'a>(
    mut chunks: impl Iterator<Item = &'a [u8]>,
    first_chunk: &'a [u8],
    next_chunk: &'a [u8],
) -> MetaResult {
    let mut result = [0u8; 8];
    match first_chunk.len() - 2 {
        0 => {
            result[0..2].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 0 },
                Some(chunk) => chunk,
            });
            result[2..4].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 1 },
                Some(chunk) => chunk,
            });
            result[4..6].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 2 },
                Some(chunk) => chunk,
            });
            result[6..8].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 3 },
                Some(chunk) => chunk,
            });
        }
        1 => {
            result[0] = first_chunk[2];
            result[1] = next_chunk[2];
            result[2..5].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 0 },
                Some(chunk) => chunk,
            });
            result[5..8].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 1 },
                Some(chunk) => chunk,
            });
        }
        2 => {
            result[0..2].copy_from_slice(&first_chunk[2..]);
            result[2..4].copy_from_slice(&next_chunk[2..]);
            result[4..8].copy_from_slice(match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 0 },
                Some(chunk) => chunk,
            });
        }
        3 => {
            result[0..3].copy_from_slice(&first_chunk[2..]);
            result[3..6].copy_from_slice(&next_chunk[2..]);
            let chunk = match chunks.next() {
                None => return MetaResult::OutOfLines { consumed: 0 },
                Some(chunk) => chunk,
            };
            result[6..8].copy_from_slice(&chunk[0..2]);
        }
        4.. => {
            result[0..4].copy_from_slice(&first_chunk[2..6]);
            result[4..8].copy_from_slice(&next_chunk[2..6]);
        }
    }

    MetaResult::Meta(result)
}
