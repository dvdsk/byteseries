pub(crate) mod resample;

use std::ffi::OsStr;
use std::path::Path;

use super::data::Data;
use super::DownSampled;
use crate::{Error, ResampleState, Resampler, Timestamp};

#[derive(Debug, Clone)]
pub struct Config {
    sample_every: Timestamp,
    max_interval: Timestamp,
    bin_size: usize,
}
impl Config {
    fn file_name_suffix(&self) -> String {
        format!(
            "{}_{}_{}",
            self.sample_every, self.max_interval, self.bin_size
        )
    }
    fn header(&self, name: &OsStr) -> String {
        let name = name.to_string_lossy();
        format!("This is a cache of averages from {name}. It contains no new data and can sefly be deleted. This config was used to sample the data: {self:?}")
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            sample_every: todo!(),
            max_interval: todo!(),
            bin_size: todo!(),
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
        if self.samples_in_bin >= self.config.bin_size {
            let resampled_item = self.resample_state.finish(self.config.bin_size);
            let resampled_line = self.resampler.encode_item(&resampled_item);
            let resampled_time = self.ts_sum / self.config.bin_size as u64;
            self.data.push_data(resampled_time, &resampled_line)?;
        }

        Ok(())
    }
}
