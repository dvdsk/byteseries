pub(crate) mod resample;

use std::ffi::OsStr;
use std::ops::Bound;
use std::path::Path;

use super::data::Data;
use super::DownSampled;
use crate::search::RoughSeekPos;
use crate::{Error, ResampleState, Resampler, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    /// reject buckets that have a gap in time larger then this
    max_gap: Option<Timestamp>,
    /// number of items to average over
    bucket_size: usize,
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

impl<R: Resampler> DownSampledData<R> {
    pub(crate) fn new(
        resampler: R,
        config: Config,
        source_path: &Path,
        payload_size: usize,
    ) -> Self {
        let source_name = source_path.file_name().unwrap_or_default();
        let mut resampled_name = source_name.to_owned();
        resampled_name.push("_");
        resampled_name.push(config.file_name_suffix());
        let mut path = source_path.to_path_buf();
        path.set_file_name(resampled_name);
        Self {
            data: Data::new(path, payload_size, config.header(source_name)).unwrap(),
            resample_state: resampler.state(),
            resampler,
            config,
            ts_sum: 0,
            samples_in_bin: 0,
        }
    }
}

impl<R: Resampler> DownSampled for DownSampledData<R> {
    fn process(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), Error> {
        let data = self.resampler.decode_line(line);
        self.resample_state.add(data);
        self.ts_sum += ts;

        self.samples_in_bin += 1;
        if self.samples_in_bin >= self.config.bucket_size {
            let resampled_item = self.resample_state.finish(self.config.bucket_size);
            let resampled_line = self.resampler.encode_item(&resampled_item);
            let resampled_time = self.ts_sum / self.config.bucket_size as u64;
            self.data.push_data(resampled_time, &resampled_line)?;
        }

        Ok(())
    }

    fn estimate_lines(
        &self,
        start: Bound<Timestamp>,
        end: Bound<Timestamp>,
    ) -> crate::search::Estimate {
        RoughSeekPos::new(&self.data, start, end)
            .estimate_lines(self.data.payload_size() + 2, self.data.data_len)
    }

    fn data_mut(&mut self) -> &mut Data {
        &mut self.data
    }
    fn data(&self) -> &Data {
        &self.data
    }
}
