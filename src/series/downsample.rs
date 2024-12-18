mod repair;
pub(crate) mod resample;

use std::ffi::OsStr;
use std::io;
use std::ops::Bound;
use std::path::Path;

use tracing::instrument;

use super::data::index::{MetaPos, PayloadSize};
use super::data::{self, Data};
use super::DownSampled;
use crate::seek::RoughPos;
use crate::{file, Pos, ResampleState, Resampler, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    /// reject buckets that have a gap in time larger then this
    pub max_gap: Option<Timestamp>,
    /// number of items to average over
    pub bucket_size: usize,
}

impl Config {
    #[must_use]
    pub fn file_name_suffix(&self) -> String {
        format!("{:?}_{}", self.max_gap, self.bucket_size)
    }
    fn header(&self, name: &OsStr) -> String {
        let name = name.to_string_lossy();
        format!("This is a cache of averages from {name}. It contains no new data and can sefly be deleted. This config was used to sample the data: {self:?}")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            max_gap: None,
            bucket_size: 10,
        }
    }
}

#[derive(Debug)]
pub(crate) struct DownSampledData<R: Resampler> {
    data: Data,

    config: Config,
    samples_in_bin: usize,
    debug_tss: Vec<Timestamp>,

    resampler: R,
    ts_sum: Timestamp,
    resample_state: R::State,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("Failed to create data file for downsampled data")]
    CreateData(#[source] data::CreateError),
    #[error("Could not read existing data to downsample: {0}")]
    ReadSource(std::io::Error),
    #[error("Could not write out downsampled pre existing data: {0}")]
    WriteOut(#[source] data::PushError),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Failed to open data file")]
    Data(#[source] data::OpenError),
    #[error("Can not check last downsampled item by comparing to source, read error")]
    CanNotCompareToSource(#[source] data::ReadError),
    #[error(
        "There should not be a downsampled item since there are not \
        enough items in the source to form one"
    )]
    ShouldBeEmpty,
    #[error("Could not repair downsampled data cache")]
    Repair(#[source] repair::Error),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenOrCreateError {
    #[error("Could not open existing file")]
    Open(#[source] OpenError),
    #[error("Could not create new file")]
    Create(#[source] CreateError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not create new data file")]
    Creating(#[source] CreateError),
    #[error("While creating or opening")]
    OpenOrCreate(#[source] OpenOrCreateError),
}

impl<R> DownSampledData<R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    fn new(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: PayloadSize,
    ) -> Result<Self, data::CreateError> {
        let source_name = source_path.file_name().unwrap_or_default();
        let mut resampled_name = source_name.to_owned();
        resampled_name.push("_");
        resampled_name.push(config.file_name_suffix());
        let mut path = source_path.to_path_buf();
        path.set_file_name(resampled_name);
        dbg!(&path);
        Ok(Self {
            data: Data::new(path, payload_size, config.header(source_name).as_bytes())?,
            resample_state: resampler.state(),
            resampler,
            config,
            ts_sum: 0,
            samples_in_bin: 0,
            debug_tss: Vec::new(),
        })
    }

    #[instrument(level = "debug", skip(resampler))]
    pub(crate) fn open(
        mut resampler: R,
        config: Config,
        source_path: &Path,
        source: &mut Data,
        payload_size: PayloadSize,
    ) -> Result<Self, OpenError> {
        let source_name = source_path.file_name().unwrap_or_default();
        let mut resampled_name = source_name.to_owned();
        resampled_name.push("_");
        resampled_name.push(config.file_name_suffix());
        let mut path = source_path.to_path_buf();
        path.set_file_name(resampled_name);

        dbg!(&path);
        let file = file::FileWithHeader::open_existing(path.with_extension("byteseries"))
            .map_err(|source| data::OpenError::File {
                source,
                path: path.clone(),
            })
            .map_err(OpenError::Data)?;
        let (file, _) = file.split_off_header();
        let mut data =
            Data::open_existing(path, file, payload_size).map_err(OpenError::Data)?;

        repair::add_missing_data(source, &mut data, &config, &mut resampler)
            .map_err(OpenError::Repair)?;

        Ok(Self {
            data,
            resample_state: resampler.state(),
            resampler,
            config,
            ts_sum: 0,
            samples_in_bin: 0,
            debug_tss: Vec::new(),
        })
    }

    #[instrument(level = "debug", skip(source))]
    pub(crate) fn create(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: PayloadSize,
        source: &mut Data,
    ) -> Result<Self, CreateError> {
        let mut empty = Self::new(resampler, config, source_path, payload_size)
            .map_err(CreateError::CreateData)?;
        let Some(first_time) = source.first_time() else {
            return Ok(empty);
        };

        let seek = Pos {
            start: MetaPos::ZERO.line_start(payload_size),
            end: source.data_len,
            first_full_ts: first_time,
        };
        let res = source
            .file_handle
            .read_with_processor(seek, |ts, line| empty.process(ts, line));

        match res {
            Ok(()) => Ok(empty),
            Err(data::inline_meta::with_processor::Error::Io(e)) => {
                Err(CreateError::ReadSource(e))
            }
            Err(data::inline_meta::with_processor::Error::Processor(e)) => {
                Err(CreateError::WriteOut(e))
            }
        }
    }

    #[instrument(level = "debug", skip(source))]
    pub(crate) fn open_or_create(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: PayloadSize,
        source: &mut Data,
    ) -> Result<Self, OpenOrCreateError> {
        match dbg!(Self::open(
            resampler.clone(),
            config.clone(),
            source_path,
            source,
            payload_size,
        )) {
            Ok(downsampled) => return Ok(downsampled),
            Err(OpenError::Data(data::OpenError::File {
                source: file::OpenError::Io(io_error),
                ..
            })) if io_error.kind() == io::ErrorKind::NotFound => {
                tracing::info!("No downsampled data cache, creating one now");
            }
            Err(e) => return Err(OpenOrCreateError::Open(e)),
        }

        Self::create(resampler, config, source_path, payload_size, source)
            .map_err(OpenOrCreateError::Create)
    }
}

impl<R> DownSampled for DownSampledData<R>
where
    R: Resampler + Clone + Send + 'static,
    R::State: Send + 'static,
{
    #[instrument(level = "trace", skip(self, line))]
    fn process(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), data::PushError> {
        let data = self.resampler.decode_payload(line);
        self.resample_state.add(data);
        self.ts_sum += ts;
        self.debug_tss.push(ts);

        self.samples_in_bin += 1;
        if self.samples_in_bin >= self.config.bucket_size {
            let resampled_item = self.resample_state.finish(self.config.bucket_size);
            let resampled_line = self.resampler.encode_item(&resampled_item);
            let resampled_time = self.ts_sum / self.config.bucket_size as u64;
            assert!(
                resampled_time <= ts,
                "resampled_time should never be larger then last timestamp put into bin. \
                Info, samples_in_bin: {}, bucket_size: {}, last timestamp: {}, \
                resampled_time: {}, ts's in bin: {:?}", self.samples_in_bin, self.config.bucket_size, 
                ts, resampled_time, self.debug_tss
            );
            self.data.push_data(resampled_time, &resampled_line)?;
            self.samples_in_bin = 0;
            self.ts_sum = 0;
            self.debug_tss.clear();
        }

        Ok(())
    }

    /// returns an error if
    fn estimate_lines(
        &self,
        start: Bound<Timestamp>,
        end: Bound<Timestamp>,
    ) -> Option<crate::seek::Estimate> {
        let seek = RoughPos::new(&self.data, start, end).ok()?;
        Some(seek.estimate_lines(self.data.payload_size(), self.data.data_len))
    }

    fn data_mut(&mut self) -> &mut Data {
        &mut self.data
    }
    fn data(&self) -> &Data {
        &self.data
    }
}
