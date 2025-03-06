use crate::series::data::PayloadSize;
use core::fmt;
use itertools::Itertools;
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::iter;
use tracing::{instrument, warn};
use with_processor::Error;

use crate::{CorruptionCallback, Pos, Resampler};

use super::{Decoder, ReadError, Timestamp};
pub(crate) mod meta;
pub(crate) mod with_processor;

#[derive(Debug)]
pub(crate) struct FileWithInlineMeta<F: fmt::Debug> {
    pub(crate) file_handle: F,
    pub(crate) payload_size: PayloadSize,
}

pub(crate) trait SetLen {
    fn len(&self) -> Result<u64, std::io::Error>;
    fn set_len(&mut self, len: u64) -> Result<(), std::io::Error>;
}

impl<F: fmt::Debug + Read + Seek + SetLen> FileWithInlineMeta<F> {
    /// Will
    ///  - remove partial line write at the end of the file
    ///  - truncate the file if it contains only metadata
    ///  - remove a (partial) trailing metadata sections if there is one
    pub(crate) fn new(
        mut file: F,
        payload_size: PayloadSize,
    ) -> Result<Self, std::io::Error> {
        'check_and_repair: {
            if file.len()? == 0 {
                break 'check_and_repair;
            }

            repair_incomplete_last_write(&mut file, payload_size)?;
            if repaired_is_only_meta(&mut file, payload_size)? {
                warn!("repaired file only consisting of a meta section");
                break 'check_and_repair;
            }

            if removed_partial_meta_at_end(&mut file, payload_size)? {
                warn!("repaired incomplete written meta section at end");
                break 'check_and_repair;
            }

            if removed_start_of_meta_at_end(&mut file, payload_size)? {
                warn!("repaired one line of incomplete meta section at end");
                break 'check_and_repair;
            }
        }

        Ok(FileWithInlineMeta {
            file_handle: file,
            payload_size,
        })
    }

    pub(crate) fn inner_mut(&mut self) -> &mut F {
        &mut self.file_handle
    }

    #[instrument(
        level = "debug",
        skip(self, decoder, timestamps, data, corruption_callback)
    )]
    pub(crate) fn read<D: Decoder>(
        &mut self,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
        seek: Pos,
        corruption_callback: &mut Option<CorruptionCallback>,
    ) -> Result<(), ReadError> {
        let mut last = 0;
        self.read_with_processor::<()>(seek, corruption_callback, |ts, payload| {
            let item = decoder.decode_payload(payload);
            data.push(item);
            timestamps.push(ts);

            assert!(ts > last || ts == 0, "last: {last}, ts: {ts}");
            last = ts;
            Ok(())
        })
        .map_err(|e| match e {
            Error::Io(error) => ReadError::Io(error),
            Error::Processor(_) => {
                panic!("impossible, this processor never returns an error")
            }
            Error::CorruptMetaSection => ReadError::CorruptMetaSection,
        })
    }

    #[instrument(
        level = "debug",
        skip(self, decoder, timestamps, data, corruption_callback)
    )]
    pub(crate) fn read_first_n<D: Decoder>(
        &mut self,
        n: usize,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
        seek: Pos,
        corruption_callback: &mut Option<CorruptionCallback>,
    ) -> Result<(), ReadError> {
        #[derive(Debug)]
        struct ReachedN;

        let mut n_read = 0;
        let mut prev_ts = 0;
        let res = self.read_with_processor(seek, corruption_callback, |ts, payload| {
            prev_ts = ts;
            let item = decoder.decode_payload(payload);
            data.push(item);
            timestamps.push(ts);
            n_read += 1;

            if n_read >= n {
                Err(ReachedN)
            } else {
                Ok(())
            }
        });

        match res {
            Ok(()) | Err(Error::Processor(ReachedN)) => Ok(()),
            Err(Error::CorruptMetaSection) => Err(ReadError::CorruptMetaSection),
            Err(Error::Io(e)) => Err(ReadError::Io(e)),
        }
    }

    #[instrument(
        level = "debug",
        skip(self, resampler, timestamps, data, corruption_callback)
    )]
    pub(crate) fn read_resampling<R: crate::Resampler>(
        &mut self,
        resampler: &mut R,
        bucket_size: usize,
        timestamps: &mut Vec<u64>,
        data: &mut Vec<<R as Decoder>::Item>,
        seek: Pos,
        corruption_callback: &mut Option<CorruptionCallback>,
    ) -> Result<(), ReadError> {
        let mut sampler = Sampler::new(resampler, bucket_size, timestamps, data);
        self.read_with_processor::<()>(seek, corruption_callback, |ts, payload| {
            sampler.process(ts, payload);
            Ok(())
        })
        .map_err(|e| match e {
            Error::Io(error) => ReadError::Io(error),
            Error::Processor(_) => {
                panic!("impossible, this processor never returns an error")
            }
            Error::CorruptMetaSection => ReadError::CorruptMetaSection,
        })
    }
}

fn removed_start_of_meta_at_end<F: fmt::Debug + Read + Seek + SetLen>(
    file: &mut F,
    payload_size: PayloadSize,
) -> Result<bool, io::Error> {
    file.seek(SeekFrom::Start(
        file.len()? - payload_size.metainfo_size() as u64,
    ))?;
    let mut to_check = vec![1u8; 2 * payload_size.line_size()];
    file.read_exact(&mut to_check)?;
    let mut lines = to_check.chunks_exact(payload_size.line_size());
    let last_line = lines.by_ref().last().expect("read multiple lines");
    let meta_start_before_last_line = lines
        .by_ref()
        .take(2)
        .all(|line| line[0..2] == meta::PREAMBLE);
    // unless there is a meta section directly before it time zero lines
    // are not allowed.
    if last_line[0..2] == meta::PREAMBLE && !meta_start_before_last_line {
        file.set_len(file.len()? - payload_size.line_size() as u64)?;
        Ok(true)
    } else {
        Ok(false)
    }
}

fn removed_partial_meta_at_end<F: fmt::Debug + Read + Seek + SetLen>(
    file: &mut F,
    payload_size: PayloadSize,
) -> Result<bool, io::Error> {
    let check_start = file.len()? - payload_size.metainfo_size() as u64;
    file.seek(SeekFrom::Start(check_start))?;

    let mut to_check = vec![0u8; payload_size.metainfo_size()];
    file.read_exact(&mut to_check)?;

    // otherwise the check below does not match a partial meta section
    // that is only one line
    to_check.extend(meta::PREAMBLE);
    to_check.extend(iter::repeat_n(0, payload_size.raw()));

    let partial_meta_start = to_check
        .chunks_exact(payload_size.line_size())
        .tuple_windows()
        .position(|(a, b)| a[0..2] == meta::PREAMBLE && b[0..2] == meta::PREAMBLE)
        .map(|line| line * payload_size.line_size());

    if let Some(partial_meta_start) = partial_meta_start {
        file.set_len(check_start + partial_meta_start as u64)?;
        Ok(true)
    } else {
        Ok(false)
    }
}
fn repair_incomplete_last_write(
    file: &mut impl SetLen,
    payload_size: PayloadSize,
) -> Result<(), std::io::Error> {
    let rest = file.len()? % (payload_size.line_size() as u64);
    if rest > 0 {
        tracing::warn!(
            "Last write incomplete, truncating to largest multiple of the line size"
        );
        file.set_len(file.len()? - rest)?;
    }
    Ok(())
}

fn repaired_is_only_meta<F: fmt::Debug + Read + Seek + SetLen>(
    file: &mut F,
    payload_size: PayloadSize,
) -> Result<bool, io::Error> {
    Ok(if file.len()? <= payload_size.metainfo_size() as u64 {
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
