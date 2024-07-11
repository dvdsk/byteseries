pub mod data;
pub mod downsample;
use core::fmt;
use std::ops::{Bound, RangeBounds};
use std::path::Path;

use data::Data;
use itertools::Itertools;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tracing::instrument;

use crate::search::{Estimate, SeekError};
use crate::{search, Decoder, Resampler, Timestamp};

use self::data::inline_meta::bytes_per_metainfo;
use self::downsample::DownSampledData;

trait DownSampled: fmt::Debug + Send + 'static {
    fn process(&mut self, ts: Timestamp, line: &[u8]) -> Result<(), data::PushError>;
    fn estimate_lines(&self, start: Bound<Timestamp>, end: Bound<Timestamp>) -> Option<Estimate>;
    fn data_mut(&mut self) -> &mut Data;
    fn data(&self) -> &Data;
}

#[derive(Debug)]
pub struct ByteSeries {
    pub(crate) data: Data,
    downsampled: Vec<Box<dyn DownSampled>>,

    pub(crate) first_time_in_data: Option<Timestamp>,
    pub(crate) last_time_in_data: Option<Timestamp>,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Could not open byteseries: {0}")]
    Open(data::OpenError),
    #[error("Failed to create new byteseries: {0}")]
    Create(data::CreateError),
    #[error("Error caching downsampled data")]
    Downsampled(downsample::Error),
    #[error("Could not push, new timestamp: {new} lies before last in data: {prev}")]
    NewLineBeforePrevious { new: u64, prev: u64 },
    #[error("Could not push to data file: {0}")]
    Pushing(data::PushError),
    #[error("Could not updated downsampled data file's metadata: {0}")]
    Downampling(data::PushError),
    #[error("Timestamps do not exist in Data: {0}")]
    InvalidRange(SeekError),
    #[error("Error while finding start and end point in data: {0}")]
    Seeking(SeekError),
    #[error("Could not read data: {0}")]
    Reading(data::ReadError),
    #[error("Would need to collect more then usize::MAX samples to resample.")]
    TooMuchToResample,
}

impl ByteSeries {
    #[instrument]
    pub fn new<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        header: H,
    ) -> Result<ByteSeries, Error>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
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
        H: DeserializeOwned + Serialize + fmt::Debug + 'static + Clone,
        R: Resampler + Clone + Send + 'static,
        R::State: Send + 'static,
    {
        let mut data = Data::new(name.as_ref(), payload_size, header).map_err(Error::Create)?;
        Ok(ByteSeries {
            first_time_in_data: None,
            last_time_in_data: None,
            downsampled: resample_configs
                .into_iter()
                .map(|config| {
                    DownSampledData::create(
                        resampler.clone(),
                        config,
                        name.as_ref(),
                        payload_size,
                        &mut data,
                    )
                    .map_err(downsample::Error::Creating)
                })
                .map_ok(Box::new)
                .map_ok(|boxed| boxed as Box<dyn DownSampled>)
                .collect::<Result<Vec<_>, downsample::Error>>()
                .map_err(Error::Downsampled)?,
            data,
        })
    }

    /// line size in bytes, path is *without* any extension
    #[instrument]
    pub fn open_existing<H>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
    ) -> Result<(ByteSeries, H), Error>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + PartialEq + 'static + Clone,
    {
        let (mut data, header) = Data::open_existing(name, payload_size).map_err(Error::Open)?;

        let bs = ByteSeries {
            first_time_in_data: data.first_time(),
            last_time_in_data: data.last_time(),
            downsampled: Vec::new(),
            data,
        };

        Ok((bs, header))
    }

    /// line size in bytes, path is *without* any extension
    #[instrument]
    pub fn open_existing_with_resampler<H, R>(
        name: impl AsRef<Path> + fmt::Debug,
        payload_size: usize,
        resampler: R,
        resample_configs: Vec<downsample::Config>,
    ) -> Result<(ByteSeries, H), Error>
    where
        H: DeserializeOwned + Serialize + fmt::Debug + PartialEq + 'static + Clone,
        R: Resampler + Clone + Send + 'static,
        R::State: Send + 'static,
    {
        let (mut data, header) = Data::open_existing(&name, payload_size).map_err(Error::Open)?;

        Ok((
            ByteSeries {
                first_time_in_data: data.first_time(),
                last_time_in_data: data.last_time(),
                downsampled: resample_configs
                    .into_iter()
                    .map(|config| {
                        DownSampledData::open_or_create(
                            resampler.clone(),
                            config,
                            name.as_ref(),
                            payload_size,
                            &mut data,
                        )
                        .map_err(downsample::Error::OpenOrCreate)
                    })
                    .map_ok(Box::new)
                    .map_ok(|boxed| boxed as Box<dyn DownSampled>)
                    .collect::<Result<Vec<_>, downsample::Error>>()
                    .map_err(Error::Downsampled)?,
                data,
            },
            header,
        ))
    }

    #[instrument(skip(self, line), level = "trace")]
    pub fn push_line(&mut self, ts: Timestamp, line: impl AsRef<[u8]>) -> Result<(), Error> {
        //write 16 bit timestamp and then the line to file
        //for now no support for sign bit since data will always be after 0 (1970)
        match self.last_time_in_data {
            Some(last_in_data) if last_in_data >= ts => {
                return Err(Error::NewLineBeforePrevious {
                    new: ts,
                    prev: last_in_data,
                })
            }
            Some(_) | None => (),
        }

        self.data
            .push_data(ts, line.as_ref())
            .map_err(Error::Pushing)?;
        self.last_time_in_data = Some(ts);
        self.first_time_in_data.get_or_insert(ts);

        for downsampled in &mut self.downsampled {
            downsampled
                .process(ts, line.as_ref())
                .map_err(Error::Downampling)?;
        }
        Ok(())
    }

    /// # Errors
    ///
    /// See the [`Error`] docs for an exhaustive list of everything that can go wrong.
    pub fn read_all<D: Decoder>(
        &mut self,
        range: impl RangeBounds<Timestamp>,
        decoder: &mut D,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<D::Item>,
    ) -> Result<(), Error> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        self.check_range(start, end).map_err(Error::InvalidRange)?;
        let seek = search::RoughSeekPos::new(
            &self.data,
            range.start_bound().cloned(),
            range.end_bound().cloned(),
        )
        .expect("no data should be caught be check range")
        .refine(&mut self.data)
        .map_err(Error::Seeking)?;
        self.data
            .read_all(seek, decoder, timestamps, data)
            .map_err(Error::Reading)
    }

    /// Will return between zero and two times `n` samples
    ///
    /// This might read more but will resample down using averages.
    /// No interpolation is performed.
    ///
    /// # Errors
    ///
    /// See the [`Error`] docs for an exhaustive list of everything that can go wrong.
    /// Its mostly IO-issues.
    #[allow(clippy::missing_panics_doc)] // is bug if panic
    #[instrument(skip(self, resampler, timestamps, data),
        fields(range = format!("{:?}..{:?}", range.start_bound(), range.end_bound())))]
    pub fn read_n<R: Resampler>(
        &mut self,
        n: usize,
        range: impl RangeBounds<Timestamp>,
        resampler: &mut R,
        timestamps: &mut Vec<Timestamp>,
        data: &mut Vec<<R as Decoder>::Item>,
    ) -> Result<(), Error> {
        let start = range.start_bound().cloned();
        let end = range.end_bound().cloned();
        self.check_range(start, end).map_err(Error::InvalidRange)?;

        assert!(
            self.downsampled
                .windows(2)
                .all(|w| w[0].data().data_len >= w[1].data().data_len),
            "downsampled must be sorted in descending resolution/numb lines"
        );

        let mut optimal_data = &mut self.data;
        for downsampled in &mut self.downsampled {
            let Some(estimate) = downsampled.estimate_lines(start, end) else {
                break; // more downsampled files are empty
            };
            if estimate.max < n as u64 {
                tracing::debug!(
                    "not enough datapoints, not using next\
                    downsamled cache, estimate was: {estimate:?}"
                );
                break;
            }
            if estimate.min < n as u64 {
                tracing::debug!(
                    "possibly not enough datapoints, not using \
                    next level downsamled cache, estimate was: {estimate:?}"
                );
                break;
            }
            tracing::debug!("using downsampled data: {downsampled:?}");
            optimal_data = downsampled.data_mut();
        }

        let seek = search::RoughSeekPos::new(optimal_data, start, end)
            .expect(
                "check range catches the undownsampled file missing data \
                and downsampled are not selected if their `estimate_lines` is None ",
            )
            .refine(optimal_data)
            .map_err(Error::Seeking)?;
        let lines = seek.lines(optimal_data);
        let bucket_size = 1.max(lines / n as u64);
        let bucket_size = usize::try_from(bucket_size).map_err(|_| Error::TooMuchToResample)?;

        optimal_data
            .read_resampling(seek, resampler, bucket_size, timestamps, data)
            .map_err(Error::Reading)
    }

    fn check_range(&self, start: Bound<Timestamp>, end: Bound<Timestamp>) -> Result<(), SeekError> {
        let inline_meta_size = bytes_per_metainfo(self.data.payload_size());
        if self.data.data_len as usize <= inline_meta_size {
            return Err(SeekError::EmptyFile);
        }

        match start {
            Bound::Included(ts) => {
                if ts > self.last_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StartAfterData);
                }
            }
            Bound::Excluded(ts) => {
                if ts >= self.last_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StartAfterData);
                }
            }
            Bound::Unbounded => (),
        };

        match end {
            Bound::Included(ts) => {
                if ts < self.first_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StopBeforeData);
                }
            }
            Bound::Excluded(ts) => {
                if ts <= self.first_time_in_data.expect("data_len > 0") {
                    return Err(SeekError::StopBeforeData);
                }
            }
            Bound::Unbounded => (),
        };

        Ok(())
    }

    pub fn last_line<D>(
        &mut self,
        decoder: &mut D,
    ) -> Result<(u64, <D as Decoder>::Item), data::ReadError>
    where
        D: Decoder + Clone,
        <D as Decoder>::Item: Clone,
    {
        self.data.last_line(decoder)
    }

    pub fn flush_to_disk(&mut self) {
        self.data.flush_to_disk();
    }

    #[must_use]
    #[allow(clippy::missing_panics_doc)] // is bug if panic
    pub fn range(&self) -> Option<core::ops::RangeInclusive<Timestamp>> {
        self.first_time_in_data.map(|first| {
            first
                ..=self
                    .last_time_in_data
                    .expect("set at the same time as first_time_in_data")
        })
    }
}
