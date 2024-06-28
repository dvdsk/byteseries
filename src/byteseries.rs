pub mod data;
mod downsample;
use core::fmt;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

use data::Data;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

use crate::search::SeekError;
use crate::{search, Decoder, Error, Resampler, Timestamp};

use self::downsample::DownSampledData;

trait DownSampled: fmt::Debug {
    fn process(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), Error>;
}

#[derive(Debug)]
pub struct ByteSeries {
    pub(crate) data: Data,
    downsampled: Vec<Box<dyn DownSampled>>,

    pub(crate) first_time_in_data: Option<Timestamp>,
    pub(crate) last_time_in_data: Option<Timestamp>,
}

impl ByteSeries {
    #[instrument]
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: H,
    ) -> Result<ByteSeries, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        Self::new_with_resamplers(
            name,
            payload_size,
            header,
            downsample::resample::EmptyResampler,
            Vec::new(),
        )
    }

    #[instrument]
    pub fn new_with_resamplers<H, R>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: H,
        resampler: R,
        resample_configs: Vec<downsample::Config>,
    ) -> Result<ByteSeries, Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
        R: Resampler + Clone + 'static,
    {
        Ok(ByteSeries {
            first_time_in_data: None,
            last_time_in_data: None,
            downsampled: resample_configs
                .into_iter()
                .map(|config| {
                    DownSampledData::new(resampler.clone(), config, name.as_ref(), payload_size)
                })
                .map(Box::new)
                .map(|boxed| boxed as Box<dyn DownSampled>)
                .collect(),
            data: Data::new(name, payload_size, header)?,
        })
    }

    /// line size in bytes, path is *without* any extension
    #[instrument]
    pub fn open_existing<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
    ) -> Result<(ByteSeries, H), Error>
    where
        H: DeserializeOwned + Serialize + Eq + fmt::Debug + 'static + Clone,
    {
        let (mut data, header) = Data::open_existing(name, payload_size)?;

        let bs = ByteSeries {
            first_time_in_data: data.first_time(),
            last_time_in_data: data.last_time(),
            downsampled: Vec::new(),
            data,
        };

        Ok((bs, header))
    }

    pub(crate) fn payload_size(&self) -> usize {
        self.data.payload_size()
    }

    pub fn push_line(&mut self, ts: Timestamp, line: impl AsRef<[u8]>) -> Result<(), Error> {
        //write 16 bit timestamp and then the line to file
        //for now no support for sign bit since data will always be after 0 (1970)
        if ts <= self.last_time_in_data.unwrap_or(0) {
            return Err(Error::NewLineBeforePrevious {
                new: ts,
                prev: self.last_time_in_data,
            });
        }
        self.data.push_data(ts, line.as_ref())?;
        self.last_time_in_data = Some(ts);
        self.first_time_in_data.get_or_insert(ts);

        for downsampled in self.downsampled.iter_mut() {
            downsampled.process(ts, line.as_ref())?;
        }
        Ok(())
    }

    pub fn read_all<D: Decoder>(
        &mut self,
        range: impl RangeBounds<Timestamp>,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        self.check_range(range.start_bound().cloned(), range.start_bound().cloned())?;
        let seek = search::RoughSeekPos::new(
            &self.data,
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )
        .refine(&mut self.data)?;
        self.data.read_all(seek, decoder, timestamps, data)
    }

    /// Will return between zero and two times `n` samples
    pub fn read_n<D: Decoder>(
        &mut self,
        _n: usize,
        range: impl RangeBounds<Timestamp>,
        _decoder: &mut D,
        _timestamps: &mut Vec<Timestamp>,
        _data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        self.check_range(range.start_bound().cloned(), range.start_bound().cloned())?;
        let rough_seek = search::RoughSeekPos::new(
            &self.data,
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        );
        // let seek = search::SeekPos::in_series(series, start, end)

        // check n points in self
        // estimate required resampling
        // check points in resampling
        // if bad check lower/higher res
        todo!()
    }

    fn check_range(&self, start: Bound<Timestamp>, end: Bound<Timestamp>) -> Result<(), SeekError> {
        if self.data.data_len == 0 {
            return Err(SeekError::EmptyFile);
        }

        match start {
            Bound::Included(ts) => {
                if ts >= self.last_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StartAfterData);
                }
            }
            Bound::Excluded(ts) => {
                if ts - 1 >= self.last_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StartAfterData);
                }
            }
            Bound::Unbounded => (),
        };

        match end {
            Bound::Included(ts) => {
                if ts <= self.first_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StopBeforeData);
                }
            }
            Bound::Excluded(ts) => {
                if ts - 1 <= self.first_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StopBeforeData);
                }
            }
            Bound::Unbounded => (),
        };

        Ok(())
    }
    pub fn flush_to_disk(&mut self) {
        self.data.flush_to_disk()
    }
}
