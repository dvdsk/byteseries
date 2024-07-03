pub(crate) mod resample;

use std::ffi::OsStr;
use std::ops::Bound;
use std::path::Path;

use super::data::{self, Data};
use super::DownSampled;
use crate::search::RoughSeekPos;
use crate::{util, ResampleState, Resampler, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    /// reject buckets that have a gap in time larger then this
    pub max_gap: Option<Timestamp>,
    /// number of items to average over
    pub bucket_size: usize,
}

impl Config {
    fn file_name_suffix(&self) -> String {
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

    resampler: R,
    ts_sum: Timestamp,
    resample_state: R::State,
}

#[derive(Debug, thiserror::Error)]
pub enum CreateError {
    #[error("{0}")]
    CreateData(data::CreateError),
    #[error("Could not read existing data to downsample: {0}")]
    ReadSource(std::io::Error),
    #[error("Could not write out downsampled pre existing data: {0}")]
    WriteOut(data::PushError),
}

#[derive(Debug, thiserror::Error)]
pub enum OpenError {
    #[error("Failed to open data file")]
    Data(data::OpenError),
    // TODO: recover/restore error
}

#[derive(Debug, thiserror::Error)]
pub enum OpenOrCreateError {
    #[error("Could not open existing file")]
    Open(OpenError),
    #[error("Could not create new file")]
    Create(CreateError),
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not create new data file: {0}")]
    Creating(CreateError),
    #[error("create or open: {0}")]
    OpenOrCreate(OpenOrCreateError),
}

impl<R: Resampler + Clone> DownSampledData<R> {
    fn new(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: usize,
    ) -> Result<Self, data::CreateError> {
        let source_name = source_path.file_name().unwrap_or_default();
        let mut resampled_name = source_name.to_owned();
        resampled_name.push("_");
        resampled_name.push(config.file_name_suffix());
        let mut path = source_path.to_path_buf();
        path.set_file_name(resampled_name);
        Ok(Self {
            data: Data::new(path, payload_size, config.header(source_name))?,
            resample_state: resampler.state(),
            resampler,
            config,
            ts_sum: 0,
            samples_in_bin: 0,
        })
    }

    pub(crate) fn open(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: usize,
    ) -> Result<Self, OpenError> {
        let source_name = source_path.file_name().unwrap_or_default();
        let mut resampled_name = source_name.to_owned();
        resampled_name.push("_");
        resampled_name.push(config.file_name_suffix());
        let mut path = source_path.to_path_buf();
        path.set_file_name(resampled_name);

        let (data, _): (_, String) =
            Data::open_existing(path, payload_size).map_err(OpenError::Data)?;
        // HARD to implement
        // TODO!("check if number last sample checks out with the data");

        Ok(Self {
            data,
            resample_state: resampler.state(),
            resampler,
            config,
            ts_sum: 0,
            samples_in_bin: 0,
        })
    }

    pub(crate) fn create(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: usize,
        source: &mut Data,
    ) -> Result<Self, CreateError> {
        let mut empty = Self::new(resampler, config, source_path, payload_size)
            .map_err(CreateError::CreateData)?;
        let Some(first_time) = source.first_time() else {
            return Ok(empty);
        };

        let mut process_res = Ok(());
        source
            .file_handle
            .read_with_processor(
                |ts, line| {
                    if let Err(e) = empty.process(ts, line) {
                        process_res = Err(e);
                    }
                },
                0,
                source.data_len,
                first_time,
            )
            .map_err(CreateError::ReadSource)?;
        process_res.map_err(CreateError::WriteOut)?;
        Ok(empty)
    }

    pub(crate) fn open_or_create(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: usize,
        source: &mut Data,
    ) -> Result<Self, OpenOrCreateError> {
        match Self::open(resampler.clone(), config.clone(), source_path, payload_size) {
            Ok(downsampled) => return Ok(downsampled),
            Err(OpenError::Data(data::OpenError::File(util::OpenError::AlreadyExists))) => (),
            Err(e) => return Err(OpenOrCreateError::Open(e)),
        }

        Self::create(resampler, config, source_path, payload_size, source)
            .map_err(OpenOrCreateError::Create)
    }
}

impl<R: Resampler> DownSampled for DownSampledData<R> {
    fn process(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), data::PushError> {
        let data = self.resampler.decode_payload(line);
        self.resample_state.add(data);
        self.ts_sum += ts;

        self.samples_in_bin += 1;
        if self.samples_in_bin >= self.config.bucket_size {
            let resampled_item = self.resample_state.finish(self.config.bucket_size);
            let resampled_line = self.resampler.encode_item(&resampled_item);
            let resampled_time = self.ts_sum / self.config.bucket_size as u64;
            self.data.push_data(resampled_time, &resampled_line)?;
            self.samples_in_bin = 0;
            self.ts_sum = 0;
        }

        Ok(())
    }

    fn estimate_lines(
        &self,
        start: Bound<Timestamp>,
        end: Bound<Timestamp>,
    ) -> crate::search::Estimate {
        dbg!(RoughSeekPos::new(&self.data, start, end))
            .estimate_lines(self.data.payload_size() + 2, self.data.data_len)
    }

    fn data_mut(&mut self) -> &mut Data {
        &mut self.data
    }
    fn data(&self) -> &Data {
        &self.data
    }
}
